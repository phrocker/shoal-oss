// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tserver

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gozk "github.com/go-zookeeper/zk"
	"github.com/google/uuid"
)

// ServiceLock errors.
var (
	// ErrLockLost means the tablet-server ServiceLock is no longer held. It
	// wraps every way a lock ends, so a caller can treat it as one condition:
	// this process may no longer host anything under that generation.
	ErrLockLost = errors.New("tserver: service lock lost")
	// ErrNotHeld means the operation needs a held lock and there is none.
	ErrNotHeld = errors.New("tserver: service lock not held")
	// ErrLockInUse means the lock was already used for an acquisition. A
	// ServiceLock covers one generation; a new one needs a new ServiceLock.
	ErrLockInUse = errors.New("tserver: service lock already used")
	// ErrLockNodeMissing means the ephemeral node this process created is not
	// in the lock directory, so its claim cannot be established or proven.
	ErrLockNodeMissing = errors.New("tserver: lock node missing")
	// ErrLockReleased means an acquisition ended because Release was called
	// while it was still queued, so the lock was never held.
	ErrLockReleased = errors.New("tserver: service lock released before it was acquired")
	// ErrLockNodeOrphaned means a node this process created could not be
	// removed after an acquisition failed. The node keeps its sequence, and
	// so its place in line, for as long as the ZooKeeper session lives, and
	// can become the holder of a lock nobody is waiting on. Only closing the
	// session is guaranteed to remove it, and only the session's owner can do
	// that, which is why this is reported rather than swallowed.
	ErrLockNodeOrphaned = errors.New("tserver: lock node orphaned")
	// ErrInvalidLockPath means the configured lock directory cannot name a
	// ZooKeeper znode.
	ErrInvalidLockPath = errors.New("tserver: invalid lock path")
)

const (
	// zLockPrefix is Accumulo's ServiceLock.ZLOCK_PREFIX.
	zLockPrefix = "zlock#"
	// lockSequenceDigits is the width of the counter ZooKeeper appends to a
	// sequential node, and the width ServiceLock.validateAndSort requires.
	lockSequenceDigits = 10
	// zTabletServers is the znode root tablet servers register under.
	zTabletServers = "tservers"
)

// LockConn is the ZooKeeper surface the ServiceLock protocol needs.
// *github.com/go-zookeeper/zk.Conn satisfies it directly, so a caller passes
// the session it already authenticated with the instance secret; this package
// does not open or own ZooKeeper connections.
type LockConn interface {
	Create(path string, data []byte, flags int32, acl []gozk.ACL) (string, error)
	Children(path string) ([]string, *gozk.Stat, error)
	ExistsW(path string) (bool, *gozk.Stat, <-chan gozk.Event, error)
	Delete(path string, version int32) error
}

// A live ZooKeeper session is the intended implementation; this pins the
// interface to it so a signature drift is a compile error rather than a
// runtime surprise.
var _ LockConn = (*gozk.Conn)(nil)

// LossReason says how a held lock ended. It is diagnostic: every reason means
// the same thing to the fence, which is that this generation is over.
type LossReason int

const (
	// LossNone means the lock has not been lost.
	LossNone LossReason = iota
	// LossNodeDeleted means the lock znode is gone: the session that owned it
	// ended, or an operator deleted it.
	LossNodeDeleted
	// LossUnmonitorable means the lock can no longer be watched, so this
	// process cannot prove it still holds it. Accumulo's tablet server halts
	// on the equivalent LockWatcher.unableToMonitorLockNode; here the tablets
	// are dropped, which is the same refusal to serve what cannot be proven.
	LossUnmonitorable
	// LossSuperseded means another holder now owns the lock directory's
	// lowest sequence, so this process is no longer the holder.
	LossSuperseded
	// LossReleased means this process gave the lock up.
	LossReleased
)

// String renders the reason for logs and errors.
func (r LossReason) String() string {
	switch r {
	case LossNone:
		return "NONE"
	case LossNodeDeleted:
		return "NODE_DELETED"
	case LossUnmonitorable:
		return "UNMONITORABLE"
	case LossSuperseded:
		return "SUPERSEDED"
	case LossReleased:
		return "RELEASED"
	default:
		return fmt.Sprintf("LossReason(%d)", int(r))
	}
}

// PublicACL returns the ACL Accumulo applies to lock and server znodes
// (ZooUtil.PUBLIC): full control for the authenticated creator, read for
// anyone. The read entry is what lets unauthenticated clients enumerate live
// servers; the creator entry is why the session must carry the instance
// secret before any of this succeeds.
func PublicACL() []gozk.ACL {
	return append(gozk.AuthACL(gozk.PermAll), gozk.WorldACL(gozk.PermRead)...)
}

// TabletServerLockPath returns the lock directory a tablet server registers
// under: <instancePath>/tservers/<group>/<address>, the Accumulo 4 layout
// internal/zk walks when it enumerates live servers. An empty group means
// DefaultResourceGroup.
//
// A group outside Accumulo's resource-group grammar is refused rather than
// joined into a path. Segments are cleaned when they are joined, so a name
// like "../managers" would not register this server under tservers at all —
// it would put it in another role's subtree, where nothing looking for a
// tablet server would find it and something looking for a manager might.
func TabletServerLockPath(instancePath, group, address string) (string, error) {
	if group == "" {
		group = DefaultResourceGroup
	}
	if !validResourceGroup(group) {
		return "", fmt.Errorf("%w: resource group %q is not a name Accumulo reads (must match %s)",
			ErrInvalidLockData, group, resourceGroupPattern)
	}
	return path.Join(instancePath, zTabletServers, group, address), nil
}

// ParseLockNode maps a ZooKeeper lock child name onto the identity it names.
//
// The rules are ServiceLock.validateAndSort's: the name is
// "zlock#<uuid>#<10-digit sequence>", the UUID must be the dashed form
// Java's UUID.fromString accepts, and the sequence must fit the signed 32-bit
// counter Java reads with Integer.parseInt. The same rules are applied by
// internal/zk when it resolves a lock holder, so a node either package
// accepts is a node the other accepts.
func ParseLockNode(name string) (LockID, bool) {
	if !strings.HasPrefix(name, zLockPrefix) {
		return LockID{}, false
	}
	rest := strings.TrimPrefix(name, zLockPrefix)
	separator := strings.Index(rest, "#")
	if separator < 0 {
		return LockID{}, false
	}
	holder, digits := rest[:separator], rest[separator+1:]
	if len(digits) != lockSequenceDigits {
		return LockID{}, false
	}
	if !validAccumuloUUID(holder) {
		return LockID{}, false
	}
	sequence, err := strconv.ParseInt(digits, 10, 32)
	if err != nil {
		return LockID{}, false
	}
	id := LockID{UUID: holder, Sequence: sequence}
	if !id.Valid() {
		return LockID{}, false
	}
	return id, true
}

// sortLockNodes returns the child names that could name a lock, ordered by
// sequence with the holder first. Names that do not parse are dropped, exactly
// as ServiceLock.validateAndSort drops them, so a stray child in the directory
// cannot displace a real holder.
func sortLockNodes(children []string) []string {
	type candidate struct {
		name     string
		sequence int64
	}
	valid := make([]candidate, 0, len(children))
	for _, child := range children {
		id, ok := ParseLockNode(child)
		if !ok {
			continue
		}
		valid = append(valid, candidate{name: child, sequence: id.Sequence})
	}
	sort.Slice(valid, func(i, j int) bool { return valid[i].sequence < valid[j].sequence })
	names := make([]string, 0, len(valid))
	for _, entry := range valid {
		names = append(names, entry.name)
	}
	return names
}

// findLowestPrevPrefix returns the node a queued candidate at index must watch:
// the lowest-sequence node of the holder immediately ahead of it.
//
// Watching the immediate predecessor alone is not enough. A single process may
// leave several nodes behind — a create whose response was lost still created
// one — and they all carry that process's prefix. Watching its highest node
// would wake this one when a duplicate is cleaned up rather than when the
// holder actually leaves. Mirrors ServiceLock.findLowestPrevPrefix.
func findLowestPrevPrefix(sorted []string, index int) string {
	previous := sorted[index-1]
	prefixEnd := strings.LastIndex(previous, "#")
	prefix := previous[:prefixEnd]
	lowest := previous
	for i := index - 2; i >= 0; i-- {
		if !strings.HasPrefix(sorted[i], prefix) {
			break
		}
		lowest = sorted[i]
	}
	return lowest
}

// ServiceLockOptions configures one participation in the lock protocol.
type ServiceLockOptions struct {
	// Path is the lock directory — for a tablet server, the one
	// TabletServerLockPath builds. Required.
	Path string
	// UUID is this process's lock identity, the middle field of every node it
	// creates. A random one is minted when empty, which is what Accumulo does
	// per server process.
	UUID string
	// ACL is applied to the znodes this process creates. PublicACL is used
	// when empty, which is what Accumulo writes.
	ACL []gozk.ACL
	// VerifyInterval makes Maintain re-check the lock directory on a timer as
	// well as on watch events, catching a watch that was silently dropped.
	// Disabled when zero. Mirrors the tablet server's lock verification
	// thread.
	VerifyInterval time.Duration
}

// ServiceLock is one participation in Accumulo's ServiceLock protocol: it
// creates an ephemeral sequential node in a lock directory, waits its turn,
// and holds the lock until the node goes away.
//
// One ServiceLock covers one generation. It is deliberately not reusable: the
// generation is the fence, and re-acquiring through the same object would blur
// two generations into one identity. A process that loses its lock builds a
// new ServiceLock, whose node necessarily carries a higher sequence.
//
// ServiceLock is safe for concurrent use.
type ServiceLock struct {
	conn           LockConn
	dir            string
	uuid           string
	acl            []gozk.ACL
	verifyInterval time.Duration

	mu      sync.Mutex
	started bool
	node    string
	id      LockID
	held    bool
	reason  LossReason
	// released records that Release has been called, so an acquisition still
	// in flight cannot go on to hold a lock the caller has already given up.
	released bool

	// release is closed by Release to wake a queued acquisition. Waiting on
	// the lock directory alone would leave that acquisition asleep until an
	// unrelated holder happened to leave.
	release     chan struct{}
	releaseOnce sync.Once

	// createMu makes creating this process's node and sweeping it away
	// mutually exclusive. Without it a release can read an empty directory,
	// report success, and be followed by a create it never saw, leaving a
	// node in the queue that the caller was told did not exist.
	createMu sync.Mutex
}

// NewServiceLock returns a lock participant for one generation.
func NewServiceLock(conn LockConn, opts ServiceLockOptions) (*ServiceLock, error) {
	if conn == nil {
		return nil, errors.New("tserver: nil lock connection")
	}
	if err := validateLockPath(opts.Path); err != nil {
		return nil, err
	}
	holder := opts.UUID
	if holder == "" {
		generated, err := uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("tserver: mint lock uuid: %w", err)
		}
		holder = generated.String()
	}
	if !validAccumuloUUID(holder) {
		return nil, fmt.Errorf("%w: lock uuid %q is not the 36-character dashed form Accumulo reads",
			ErrInvalidLock, holder)
	}
	acl := opts.ACL
	if len(acl) == 0 {
		acl = PublicACL()
	}
	if opts.VerifyInterval < 0 {
		return nil, fmt.Errorf("tserver: negative verify interval %s", opts.VerifyInterval)
	}
	return &ServiceLock{
		conn:           conn,
		dir:            path.Clean(opts.Path),
		uuid:           holder,
		acl:            append([]gozk.ACL(nil), acl...),
		verifyInterval: opts.VerifyInterval,
		release:        make(chan struct{}),
	}, nil
}

func validateLockPath(lockPath string) error {
	if lockPath == "" {
		return fmt.Errorf("%w: empty", ErrInvalidLockPath)
	}
	if !strings.HasPrefix(lockPath, "/") {
		return fmt.Errorf("%w: %q is not absolute", ErrInvalidLockPath, lockPath)
	}
	if path.Clean(lockPath) == "/" {
		return fmt.Errorf("%w: %q is the ZooKeeper root", ErrInvalidLockPath, lockPath)
	}
	return nil
}

// Path returns the lock directory.
func (l *ServiceLock) Path() string { return l.dir }

// UUID returns this process's lock identity.
func (l *ServiceLock) UUID() string { return l.uuid }

// nodePrefix is the name prefix of every node this process creates in the
// lock directory. Because the UUID is this process's alone, a node carrying it
// is one of ours and no one else's.
func (l *ServiceLock) nodePrefix() string { return zLockPrefix + l.uuid + "#" }

// LockID returns the identity of the lock held, and whether it is still held.
// The identity survives the loss so a caller can fence the tablets it was
// hosting against exactly the generation that ended.
func (l *ServiceLock) LockID() (LockID, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.id, l.held
}

// LossReason reports how the lock ended, or LossNone while it is held.
func (l *ServiceLock) LossReason() LossReason {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reason
}

// Node returns the name of the ephemeral node this process holds the lock
// with, or "" when it holds none.
func (l *ServiceLock) Node() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.held {
		return ""
	}
	return l.node
}

// Acquire creates this process's lock node and waits until it holds the lock,
// returning the generation it acquired.
//
// The lock directory and its resource-group parent are created when missing,
// as ServiceLockSupport.createNonHaServiceLockPath does — a tablet server
// registering in a resource group nothing has used yet is normal.
//
// Waiting is cancellable: ctx ends the wait, and the node this process created
// is deleted before returning, so a cancelled acquisition leaves nothing queued
// behind. That matters because an abandoned ephemeral node would keep its place
// in line and block every candidate behind it until the session ended.
//
// When that cleanup itself fails, the returned error also wraps
// ErrLockNodeOrphaned: a node survived, and only closing the ZooKeeper session
// will remove it.
func (l *ServiceLock) Acquire(ctx context.Context, data ServiceLockData) (LockID, error) {
	payload, err := data.Encode()
	if err != nil {
		return LockID{}, err
	}
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return LockID{}, fmt.Errorf("%w: %s", ErrLockInUse, l.dir)
	}
	l.started = true
	l.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return LockID{}, err
	}
	if err := l.ensureLockDirectory(); err != nil {
		return LockID{}, err
	}
	if err := l.createNode(payload); err != nil {
		if errors.Is(err, ErrLockReleased) {
			// Released before anything was created: there is nothing to clean
			// up and nothing to wait for.
			return LockID{}, err
		}
		// The create may have taken effect even though the answer was lost,
		// so sweep before giving up rather than leaving a node in the queue.
		return LockID{}, l.withCleanup(err)
	}
	id, err := l.waitForOwnership(ctx)
	if err != nil {
		return LockID{}, l.withCleanup(err)
	}
	return id, nil
}

// createNode creates this process's queued lock node, refusing once Release
// has been called.
//
// The refusal and the create are held under createMu, which Release takes
// around its sweep. That is what makes a successful release mean what it
// says: either the create runs first and the sweep that follows finds its
// node, or the release runs first and there is no create at all. Checking a
// flag without the lock would leave the window between the check and the
// create, in which a release can look at an empty directory, report success,
// and leave the node that lands a moment later holding a place in the queue.
func (l *ServiceLock) createNode(payload []byte) error {
	l.createMu.Lock()
	defer l.createMu.Unlock()
	if l.isReleased() {
		return fmt.Errorf("%w: %s", ErrLockReleased, l.dir)
	}
	if _, err := l.conn.Create(l.dir+"/"+l.nodePrefix(), payload,
		gozk.FlagEphemeral|gozk.FlagSequence, l.acl); err != nil {
		return fmt.Errorf("create lock node in %s: %w", l.dir, err)
	}
	return nil
}

// withCleanup sweeps the nodes of an acquisition that did not finish and
// returns the failure that ended it, joined with an ErrLockNodeOrphaned when a
// node survived the sweep. The failure is returned unwrapped in the ordinary
// case so a caller can still compare a cancellation against context.Canceled.
func (l *ServiceLock) withCleanup(failure error) error {
	orphan := l.deleteOwnNodes()
	if orphan == nil {
		return failure
	}
	return errors.Join(failure, fmt.Errorf("%w in %s: %w", ErrLockNodeOrphaned, l.dir, orphan))
}

// isReleased reports whether Release has already been called.
func (l *ServiceLock) isReleased() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.released
}

// waitForOwnership collapses any duplicate nodes this process created, then
// waits until its node is the lowest in the directory.
//
// A release that lands mid-wait is reported as one however the wait actually
// ended. Release deletes this process's nodes, so the wait can just as easily
// trip over the missing node as be woken by the release itself; reporting the
// release is both deterministic and the more useful of the two facts, because
// it names the cause rather than the symptom.
func (l *ServiceLock) waitForOwnership(ctx context.Context) (LockID, error) {
	id, err := l.queueForOwnership(ctx)
	if err != nil && l.isReleased() {
		return LockID{}, fmt.Errorf("%w: %s", ErrLockReleased, l.dir)
	}
	return id, err
}

// queueForOwnership is waitForOwnership without the release reporting.
func (l *ServiceLock) queueForOwnership(ctx context.Context) (LockID, error) {
	node := ""
	for {
		children, _, err := l.conn.Children(l.dir)
		if err != nil {
			return LockID{}, fmt.Errorf("list lock nodes in %s: %w", l.dir, err)
		}
		sorted := sortLockNodes(children)
		if node == "" {
			// First pass: a create whose response was lost may have been
			// retried, leaving several nodes with this process's prefix. Keep
			// the one that entered the queue first and drop the rest, so this
			// process occupies one place in line. Mirrors ServiceLock.lock.
			ours := l.ownNodes(sorted)
			if len(ours) == 0 {
				return LockID{}, fmt.Errorf("%w: nothing with prefix %s in %s",
					ErrLockNodeMissing, l.nodePrefix(), l.dir)
			}
			node = ours[0]
			for _, duplicate := range ours[1:] {
				l.deleteNode(duplicate)
			}
			sorted = removeNodes(sorted, ours[1:])
		}
		index := indexOfNode(sorted, node)
		if index < 0 {
			return LockID{}, fmt.Errorf("%w: %s is gone from %s", ErrLockNodeMissing, node, l.dir)
		}
		if index == 0 {
			return l.acquired(node)
		}
		// Watch this process's own node as well as the one ahead of it. The
		// node ahead says when the turn arrives; the own node says when the
		// turn will never arrive. Watching only the node ahead leaves an
		// acquisition whose node was deleted — by an operator, or by the
		// session dropping just that node — asleep on an event about a queue
		// it is no longer in, until an unrelated holder happens to leave.
		mineExists, _, mineEvents, err := l.conn.ExistsW(path.Join(l.dir, node))
		if err != nil {
			return LockID{}, fmt.Errorf("watch lock node %s: %w", node, err)
		}
		if !mineExists {
			return LockID{}, fmt.Errorf("%w: %s is gone from %s", ErrLockNodeMissing, node, l.dir)
		}
		ahead := findLowestPrevPrefix(sorted, index)
		exists, _, aheadEvents, err := l.conn.ExistsW(path.Join(l.dir, ahead))
		if err != nil {
			return LockID{}, fmt.Errorf("watch lock node %s: %w", ahead, err)
		}
		if !exists {
			// It left between the listing and the watch; re-read rather than
			// wait for an event that will never come. The watch already left
			// on this process's own node costs one extra pass at worst,
			// because every pass re-reads the directory and decides again
			// from what it finds there.
			continue
		}
		select {
		case <-ctx.Done():
			return LockID{}, ctx.Err()
		case <-l.release:
			return LockID{}, fmt.Errorf("%w: %s", ErrLockReleased, l.dir)
		case <-mineEvents:
		case <-aheadEvents:
		}
	}
}

// acquired records the generation this process now holds. Callers must not
// hold l.mu.
func (l *ServiceLock) acquired(node string) (LockID, error) {
	id, ok := ParseLockNode(node)
	if !ok {
		// sortLockNodes only returns parseable names, so this cannot happen
		// through waitForOwnership; refuse rather than hold a lock whose
		// generation cannot be named, because an unnameable generation cannot
		// fence anything.
		return LockID{}, fmt.Errorf("%w: %q is not a lock node", ErrInvalidLock, node)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		// Release won the race with the last step of acquisition. Report a
		// lock that was never held rather than hold one the caller has
		// already been told is gone.
		return LockID{}, fmt.Errorf("%w: %s", ErrLockReleased, l.dir)
	}
	l.node = node
	l.id = id
	l.held = true
	l.reason = LossNone
	return id, nil
}

// Maintain watches the held lock and returns when it ends or ctx does.
//
// A lock loss returns an error wrapping ErrLockLost; the lock is not held
// afterwards and LockID still names the generation that ended. Cancellation
// returns ctx.Err() with the lock still held, because being told to stop
// watching is not being told to stop hosting — the caller decides whether to
// release.
//
// Anything that cannot be proven fails closed. If the watch cannot be
// established or is torn down — a session that expired, a watch invalidated by
// the client — the lock is treated as lost, because a lock this process cannot
// monitor is one it cannot prove it still holds, and the manager is free to
// place those tablets elsewhere the moment the session ends.
func (l *ServiceLock) Maintain(ctx context.Context) error {
	l.mu.Lock()
	held, node := l.held, l.node
	l.mu.Unlock()
	if !held {
		return fmt.Errorf("%w: %s", ErrNotHeld, l.dir)
	}
	nodePath := path.Join(l.dir, node)

	var ticks <-chan time.Time
	if l.verifyInterval > 0 {
		ticker := time.NewTicker(l.verifyInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	// One watch is outstanding at a time. A ZooKeeper watch is one-shot but
	// stays registered on both client and server until it fires, so arming a
	// new one on every pass would leave the old one behind: on a healthy
	// cluster the verify timer alone would accumulate a registration per
	// interval for the life of the lock, and they would all fire at once the
	// moment the node finally changed.
	var events <-chan gozk.Event
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if events == nil {
			exists, _, armed, err := l.conn.ExistsW(nodePath)
			if err != nil {
				return l.lose(LossUnmonitorable, fmt.Errorf("watch %s: %w", nodePath, err))
			}
			if !exists {
				return l.lose(LossNodeDeleted, nil)
			}
			events = armed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticks:
			// The watch armed above is still live and still the only one, so
			// it is left in place. Verify re-reads the directory itself, which
			// covers the node being gone without an event.
			if err := l.Verify(); err != nil {
				return err
			}
		case event := <-events:
			// The watch fired, so it is spent. Dropping it re-arms on the next
			// pass, which re-checks that the node is still there.
			events = nil
			if err := l.classifyEvent(event); err != nil {
				return err
			}
		}
	}
}

// classifyEvent turns a watch event into a loss, or nil to keep watching.
func (l *ServiceLock) classifyEvent(event gozk.Event) error {
	switch {
	case event.Type == gozk.EventNodeDeleted:
		return l.lose(LossNodeDeleted, nil)
	case event.Type == gozk.EventNotWatching:
		// The client gave up on this watch, which happens when the session
		// ends. The ephemeral node went with it.
		return l.lose(LossUnmonitorable, event.Err)
	case event.State == gozk.StateExpired:
		return l.lose(LossUnmonitorable, nil)
	default:
		return nil
	}
}

// Verify re-reads the lock directory and confirms this process is still the
// holder, returning an error wrapping ErrLockLost when it is not.
//
// The watch is the primary signal; this is the backstop for the cases a watch
// does not cover — one dropped without an event, or a lock directory that was
// deleted and recreated, which restarts ZooKeeper's sequence counter and lets
// a later arrival take a lower number than the one held here.
func (l *ServiceLock) Verify() error {
	l.mu.Lock()
	held, node := l.held, l.node
	l.mu.Unlock()
	if !held {
		return fmt.Errorf("%w: %s", ErrNotHeld, l.dir)
	}
	children, _, err := l.conn.Children(l.dir)
	if err != nil {
		return l.lose(LossUnmonitorable, fmt.Errorf("list lock nodes in %s: %w", l.dir, err))
	}
	sorted := sortLockNodes(children)
	if indexOfNode(sorted, node) < 0 {
		return l.lose(LossNodeDeleted, nil)
	}
	if sorted[0] != node {
		return l.lose(LossSuperseded, fmt.Errorf("%s now holds %s", sorted[0], l.dir))
	}
	return nil
}

// Release gives the lock up by deleting its node.
//
// It is safe to call after a loss and safe to call twice: the lock ends once,
// and the first ending is the one reported. A delete that finds nothing is
// success, because the node being gone is the outcome asked for.
//
// It is also safe to call against an acquisition that is still queued. Every
// node this process created is swept, not just the one it holds, so a release
// cannot leave a place in line for a process that is no longer waiting — and
// the acquisition is woken and refused, so it cannot go on to hold a lock the
// caller has already given up.
//
// A create that was already in flight is waited out before the sweep, so the
// success this reports covers that node too rather than racing past it.
func (l *ServiceLock) Release() error {
	l.mu.Lock()
	if !l.started {
		l.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrNotHeld, l.dir)
	}
	l.released = true
	l.mu.Unlock()
	l.releaseOnce.Do(func() { close(l.release) })
	// released is set above, so a create that has not started yet is already
	// refused; taking createMu here waits out one that has. Between them the
	// sweep below cannot miss a node this process made.
	l.createMu.Lock()
	err := l.deleteOwnNodes()
	l.createMu.Unlock()
	l.lose(LossReleased, nil)
	return err
}

// lose records the end of the generation, once. Returns an error wrapping
// ErrLockLost naming the reason the lock actually ended by, so a later cause
// cannot rewrite an earlier one.
func (l *ServiceLock) lose(reason LossReason, cause error) error {
	l.mu.Lock()
	if l.held {
		l.held = false
		l.reason = reason
	} else if l.reason != LossNone {
		reason = l.reason
		cause = nil
	} else {
		// The generation ended before it began — a release that arrived while
		// the acquisition was still queued. Record how it ended, so a caller
		// asking is not told it never did.
		l.reason = reason
	}
	id := l.id
	l.mu.Unlock()
	if cause != nil {
		return fmt.Errorf("%w: %s (%s): %w", ErrLockLost, id, reason, cause)
	}
	return fmt.Errorf("%w: %s (%s)", ErrLockLost, id, reason)
}

// ensureLockDirectory creates the lock directory and every missing ancestor.
func (l *ServiceLock) ensureLockDirectory() error {
	segments := strings.Split(strings.Trim(l.dir, "/"), "/")
	current := ""
	for _, segment := range segments {
		current += "/" + segment
		_, err := l.conn.Create(current, []byte{}, 0, l.acl)
		if err == nil || errors.Is(err, gozk.ErrNodeExists) {
			continue
		}
		if errors.Is(err, gozk.ErrNoAuth) {
			return fmt.Errorf("create %s: %w (the ZooKeeper session must carry the "+
				"instance secret before it can register a server)", current, err)
		}
		return fmt.Errorf("create %s: %w", current, err)
	}
	return nil
}

// ownNodes returns the nodes in sorted that this process created.
func (l *ServiceLock) ownNodes(sorted []string) []string {
	prefix := l.nodePrefix()
	ours := make([]string, 0, 1)
	for _, child := range sorted {
		if strings.HasPrefix(child, prefix) {
			ours = append(ours, child)
		}
	}
	return ours
}

// deleteOwnNodes removes every node this process created in the lock
// directory. It is the cleanup for an acquisition that did not finish, so a
// node cannot be left holding a place in the queue for a process that is no
// longer waiting.
//
// A failure is reported rather than swallowed. An abandoned ephemeral node
// keeps its sequence, and so its place in line, for as long as the session
// lives: it can become the holder of a lock nobody is waiting on, blocking
// every candidate behind it. The session dropping is the fallback that
// removes it, but only the session's owner can drop it, so the owner has to
// be told.
func (l *ServiceLock) deleteOwnNodes() error {
	children, _, err := l.conn.Children(l.dir)
	if err != nil {
		if errors.Is(err, gozk.ErrNoNode) {
			// There is no directory, so no node of this process is holding a
			// place in one. Nothing to clean up and nothing to report.
			return nil
		}
		return fmt.Errorf("list lock nodes in %s: %w", l.dir, err)
	}
	var failures []error
	for _, child := range l.ownNodes(sortLockNodes(children)) {
		if err := l.deleteNode(child); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// deleteNode removes one node from the lock directory, treating an already
// absent node as success.
func (l *ServiceLock) deleteNode(node string) error {
	nodePath := path.Join(l.dir, node)
	if err := l.conn.Delete(nodePath, -1); err != nil && !errors.Is(err, gozk.ErrNoNode) {
		return fmt.Errorf("delete %s: %w", nodePath, err)
	}
	return nil
}

func indexOfNode(nodes []string, node string) int {
	for i, candidate := range nodes {
		if candidate == node {
			return i
		}
	}
	return -1
}

// removeNodes returns nodes without the named ones, preserving order.
func removeNodes(nodes, remove []string) []string {
	if len(remove) == 0 {
		return nodes
	}
	dropped := make(map[string]struct{}, len(remove))
	for _, node := range remove {
		dropped[node] = struct{}{}
	}
	kept := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if _, skip := dropped[node]; skip {
			continue
		}
		kept = append(kept, node)
	}
	return kept
}
