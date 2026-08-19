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
	"slices"
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
//
// Nodes are watched by reading them, not by asking whether they exist.
// ZooKeeper sets an existence watch even when the node is missing — that is
// what makes it a creation watch — and every path watched here is a sequential
// node whose name is never handed out twice, so such a watch could only ever
// fire on a node that will never come back. It would sit on the client and the
// server until the session ended. A read sets its watch only when it succeeds,
// so a missing node costs nothing, and on a node that is there it fires on the
// delete this protocol is waiting for.
type LockConn interface {
	Create(path string, data []byte, flags int32, acl []gozk.ACL) (string, error)
	Children(path string) ([]string, *gozk.Stat, error)
	GetW(path string) ([]byte, *gozk.Stat, <-chan gozk.Event, error)
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
// Both variable segments are checked before they are joined, because segments
// are cleaned when they are joined: a group like "../managers", or an address
// like "../..:9997", would not register this server under tservers at all —
// it would put it in another role's subtree, where nothing looking for a
// tablet server would find it and something looking for a manager might. The
// group must match Accumulo's resource-group grammar, and the address must be
// one the manager could dial, which is the same check the descriptor written
// into the node gets: the directory name and the advertised address are the
// same address, and a reader that finds them disagreeing has no server.
func TabletServerLockPath(instancePath, group, address string) (string, error) {
	if group == "" {
		group = DefaultResourceGroup
	}
	if !validResourceGroup(group) {
		return "", fmt.Errorf("%w: resource group %q is not a name Accumulo reads (must match %s)",
			ErrInvalidLockData, group, resourceGroupPattern)
	}
	if err := validateAdvertiseAddress(address); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidLockData, err)
	}
	return path.Join(instancePath, zTabletServers, group, address), nil
}

// tabletServerLockIdentity returns the resource group and address a
// tablet-server lock directory names — the two variable segments of
// <instance>/tservers/<group>/<address>.
//
// A directory of another shape names no server: the manager's lock lives at
// <instance>/managers/lock, where nothing in the path identifies a process.
// Those report false rather than a guess, so a lock that is not a server
// registration is not held to a server registration's rules.
func tabletServerLockIdentity(dir string) (group, address string, ok bool) {
	segments := strings.Split(strings.Trim(dir, "/"), "/")
	if len(segments) < 3 {
		return "", "", false
	}
	tail := segments[len(segments)-3:]
	if tail[0] != zTabletServers {
		return "", "", false
	}
	return tail[1], tail[2], true
}

// ParseLockNode maps a ZooKeeper lock child name onto the identity it names.
//
// The rules are ServiceLock.validateAndSort's: the name is
// "zlock#<uuid>#<10-digit sequence>", the UUID must be the dashed form
// Java's UUID.fromString accepts, and the sequence must fit the signed 32-bit
// counter Java reads with Integer.parseInt. internal/zk, which resolves a lock
// holder from the same directories, is looser about the UUID — it takes any
// spelling uuid.Parse accepts — so a node this rejects is not always one it
// rejects. Accumulo's rules are the ones that decide what a lock node is, so
// they are the ones applied here.
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
	// claimed names the nodes this ServiceLock made or took over, and is what
	// the cleanup sweeps. The node prefix is not enough to decide that: it
	// carries the lock UUID, and nothing stops a later ServiceLock being built
	// with the same one, so a sweep by prefix would let a release arriving
	// after a rejoin delete the live node of the generation that replaced it.
	claimed []string
	// live records that this ServiceLock is still the process's participant in
	// the directory: its acquisition is in flight, or its generation is held.
	// While that is true every node carrying the prefix is this one's, because
	// a later ServiceLock is only built after this one is done. Once it is
	// false the prefix may be shared, and only the claim list is safe to
	// sweep.
	live bool

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

	// watchMu serializes arming the existence watch and guards watch, which
	// is the one registration outstanding on this generation's node.
	//
	// It lives here rather than inside Maintain because a cancelled Maintain
	// leaves the lock held and invites the caller to watch again: a watch
	// stays registered until it fires, so arming per call would accumulate
	// one per restart exactly as arming per pass accumulated one per verify
	// interval. Concurrent callers share it for the same reason.
	watchMu sync.Mutex
	watch   <-chan gozk.Event

	// done is closed when the generation ends. The event that ends it can be
	// received by only one watcher, so this is how the others find out.
	done     chan struct{}
	doneOnce sync.Once
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
		done:           make(chan struct{}),
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
//
// The data has to describe this process: every descriptor must carry the UUID
// the lock is held as. Accumulo's tablet server announces itself with one UUID
// in both places, and the two are read for different things — the node name is
// what fences a generation, the descriptor is what a client dials. Publishing
// one server's descriptors from another's lock node would split those apart,
// so it is refused here rather than left for the manager to find.
//
// The address and resource group are checked the same way, against the
// directory the lock is registered in. TabletServerLockPath and the descriptor
// carry the same two values in Accumulo — announceExistence builds the path
// from the address and group it advertises — but they arrive here separately,
// so nothing but this check keeps them together. They are read for different
// things as well: the directory is how a server is enumerated, the descriptor
// is what a client dials, and a lock that registers under one address while
// advertising another is a server the manager can see but nothing can reach.
// A lock path of another shape names no server and is not checked this way.
//
// On such a path the services are checked too: only the five a tablet server
// publishes are accepted. TabletServerLockData refuses the rest already, but
// ServiceLockData is an ordinary struct and can be built without it, and a
// descriptor for another role's service under a tablet server's path claims an
// endpoint that role owns while pointing at a process that does not implement
// it. Locks that name no server carry their own roles' services and are left
// alone.
//
// TSERV must be one of them. It is not one advertisement among five: the
// manager reads it off every held lock under the tablet-server tree, and
// LiveTServerSet.checkServer hands the result straight to a TServerInstance
// that dereferences it. A lock without a TSERV descriptor yields a null
// address there and a NullPointerException that ends the whole scan, not this
// server's part of it — one such registration stops the manager seeing any
// tablet server, including the healthy ones. A process that cannot serve TSERV
// yet belongs outside the tree, not in it without the descriptor.
func (l *ServiceLock) Acquire(ctx context.Context, data ServiceLockData) (LockID, error) {
	payload, err := data.Encode()
	if err != nil {
		return LockID{}, err
	}
	group, address, registersAServer := tabletServerLockIdentity(l.dir)
	publishesTabletServer := false
	for _, descriptor := range data.Descriptors {
		if descriptor.UUID != l.uuid {
			return LockID{}, fmt.Errorf("%w: %s descriptor names server %s, but this lock is held as %s",
				ErrInvalidLockData, descriptor.Service, descriptor.UUID, l.uuid)
		}
		if !registersAServer {
			continue
		}
		if descriptor.Service == ServiceTabletServer {
			publishesTabletServer = true
		}
		if _, ours := tabletServerServiceSet[descriptor.Service]; !ours {
			return LockID{}, fmt.Errorf("%w: %q is not a tablet-server service, but this lock registers a tablet server",
				ErrInvalidLockData, descriptor.Service)
		}
		if descriptor.Address != address {
			return LockID{}, fmt.Errorf("%w: %s descriptor advertises %s, but this lock registers %s",
				ErrInvalidLockData, descriptor.Service, descriptor.Address, address)
		}
		if descriptor.Group != group {
			return LockID{}, fmt.Errorf("%w: %s descriptor is in resource group %s, but this lock registers under %s",
				ErrInvalidLockData, descriptor.Service, descriptor.Group, group)
		}
	}
	if registersAServer && !publishesTabletServer {
		return LockID{}, fmt.Errorf("%w: this lock registers a tablet server but advertises no %s descriptor, which the manager reads off every lock in the tree",
			ErrInvalidLockData, ServiceTabletServer)
	}
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return LockID{}, fmt.Errorf("%w: %s", ErrLockInUse, l.dir)
	}
	l.started = true
	l.live = true
	l.mu.Unlock()

	if err := ctx.Err(); err != nil {
		// Nothing was created, so there is nothing to sweep — but this
		// ServiceLock has to stop looking live, or a Release that follows
		// would sweep the prefix, and by then the caller may have rejoined
		// under it. See sweep.
		l.notLive()
		return LockID{}, err
	}
	if err := l.ensureLockDirectory(); err != nil {
		l.notLive()
		return LockID{}, err
	}
	if err := l.createNode(payload); err != nil {
		if errors.Is(err, ErrLockReleased) {
			// Released before anything was created: there is nothing to clean
			// up and nothing to wait for. Release already recorded that this
			// ServiceLock is no longer live; saying so again keeps that true
			// of every failing return here rather than of another method's
			// field order.
			l.notLive()
			return LockID{}, err
		}
		// The create may have taken effect even though the answer was lost,
		// so sweep before giving up rather than leaving a node in the queue.
		// The name was never returned, so only the directory can say whether
		// there is one — which is why the sweep inside Acquire goes by prefix.
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
	created, err := l.conn.Create(l.dir+"/"+l.nodePrefix(), payload,
		gozk.FlagEphemeral|gozk.FlagSequence, l.acl)
	if err != nil {
		return fmt.Errorf("create lock node in %s: %w", l.dir, err)
	}
	// Claimed under createMu, so a release waiting on it sweeps this node
	// rather than reading a directory it is not in yet.
	l.claim(path.Base(created))
	return nil
}

// claim records nodes as this ServiceLock's, so the cleanup can sweep them
// without asking the directory which nodes carry its prefix.
func (l *ServiceLock) claim(nodes ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, node := range nodes {
		if slices.Contains(l.claimed, node) {
			continue
		}
		l.claimed = append(l.claimed, node)
	}
}

// claimedNodes returns the nodes this ServiceLock made or took over.
func (l *ServiceLock) claimedNodes() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.claimed)
}

// withCleanup sweeps the nodes of an acquisition that did not finish and
// returns the failure that ended it, joined with an ErrLockNodeOrphaned when a
// node survived the sweep. The failure is returned unwrapped in the ordinary
// case so a caller can still compare a cancellation against context.Canceled.
//
// The acquisition is still in flight here, so the prefix is this ServiceLock's
// and the sweep can use it. Nothing else in the directory can be carrying it:
// a later ServiceLock is built only after this call returns.
func (l *ServiceLock) withCleanup(failure error) error {
	orphan := l.sweep(true)
	l.notLive()
	if orphan == nil {
		return failure
	}
	return errors.Join(failure, fmt.Errorf("%w in %s: %w", ErrLockNodeOrphaned, l.dir, orphan))
}

// notLive records that this ServiceLock is no longer the process's
// participant in the lock directory, without ending a generation.
//
// Every path that leaves Acquire without a held lock goes through here, so
// what a later Release is allowed to sweep does not depend on how far the
// acquisition got. An acquisition that failed before creating anything has
// nothing to sweep, but one still looking live would have its prefix swept —
// and the prefix is shared with whatever the caller rejoined as. See sweep.
func (l *ServiceLock) notLive() {
	l.mu.Lock()
	l.live = false
	l.mu.Unlock()
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
	// The watch on this process's own node outlives a pass. A ZooKeeper watch
	// stays registered until it fires, so arming one per pass would leave a
	// registration behind for every place this process moves up the queue, and
	// a long queue would end with every one of them firing at once. It is
	// re-armed only after it has fired. The node ahead is a different node on
	// each pass, so its watch is not the same kind of accumulation: each one
	// belongs to the node it was armed on and is spent when that node goes.
	var mineEvents <-chan gozk.Event
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
			duplicates := ours[1:]
			// Everything carrying this prefix now is this acquisition's: it is
			// in flight, so no later ServiceLock can have created anything yet.
			// Claiming them here is what lets the cleanup sweep a node this
			// process created under a name it never learned, and what stops it
			// from sweeping a node it never made.
			l.claim(ours...)
			// A duplicate that cannot be dropped fails the acquisition. It is
			// this session's node, so it lives as long as the session, holding
			// a second place in line that no ServiceLock maintains: once the
			// node this process does adopt goes away, the duplicate becomes
			// the lowest in the directory and therefore the holder the manager
			// sees — a server that looks live and refuses everything, because
			// the generation this process was fenced to has ended. Failing
			// here hands it to Acquire's cleanup, which sweeps the prefix and
			// reports ErrLockNodeOrphaned when a node still survives, so the
			// session's owner is told that only closing the session clears it.
			var failures []error
			for _, duplicate := range duplicates {
				if err := l.deleteNode(duplicate); err != nil {
					failures = append(failures, err)
				}
			}
			if len(failures) > 0 {
				return LockID{}, fmt.Errorf("collapse duplicate lock nodes in %s: %w",
					l.dir, errors.Join(failures...))
			}
			sorted = removeNodes(sorted, duplicates)
		}
		index := indexOfNode(sorted, node)
		if index < 0 {
			return LockID{}, fmt.Errorf("%w: %s is gone from %s", ErrLockNodeMissing, node, l.dir)
		}
		if index == 0 {
			// First in line, but the caller stopped waiting. Committing here
			// would hand back a lock nobody is waiting for any more, while the
			// queued path below refuses the same cancellation — so the outcome
			// of a cancelled acquisition would depend on how far it happened
			// to get. Failing lets Acquire's cleanup take the node back out of
			// the directory instead of leaving this process holding a lock it
			// was never told to maintain.
			if err := ctx.Err(); err != nil {
				return LockID{}, err
			}
			return l.acquired(node)
		}
		// Watch this process's own node as well as the one ahead of it. The
		// node ahead says when the turn arrives; the own node says when the
		// turn will never arrive. Watching only the node ahead leaves an
		// acquisition whose node was deleted — by an operator, or by the
		// session dropping just that node — asleep on an event about a queue
		// it is no longer in, until an unrelated holder happens to leave.
		if mineEvents == nil {
			armed, mineExists, err := l.watchNode(path.Join(l.dir, node))
			if err != nil {
				return LockID{}, fmt.Errorf("watch lock node %s: %w", node, err)
			}
			if !mineExists {
				return LockID{}, fmt.Errorf("%w: %s is gone from %s", ErrLockNodeMissing, node, l.dir)
			}
			mineEvents = armed
		}
		ahead := findLowestPrevPrefix(sorted, index)
		aheadEvents, exists, err := l.watchNode(path.Join(l.dir, ahead))
		if err != nil {
			return LockID{}, fmt.Errorf("watch lock node %s: %w", ahead, err)
		}
		if !exists {
			// It left between the listing and the watch; re-read rather than
			// wait for an event that will never come. Nothing was registered
			// on it — a read only sets a watch when it succeeds — and the
			// watch already armed on this process's own node is kept and
			// reused by the next pass, so re-reading costs no registrations at
			// all.
			continue
		}
		select {
		case <-ctx.Done():
			return LockID{}, ctx.Err()
		case <-l.release:
			return LockID{}, fmt.Errorf("%w: %s", ErrLockReleased, l.dir)
		case <-mineEvents:
			// Spent. The next pass re-arms it, and re-reads the directory to
			// find out whether the node it watched is really gone.
			mineEvents = nil
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
//
// The watch belongs to the lock, not to the call. A Maintain that returns on
// cancellation leaves its registration in place and a later one reuses it, so
// a caller that stops and resumes watching does not leave a registration
// behind each time it stops. Watchers that are not the one to receive the
// event that ends the generation still return when it ends.
//
// A Maintain that arrives after the generation ended reports how it ended
// rather than that there is no lock, so what a watcher is told does not depend
// on whether it reached the watch before the loss landed.
func (l *ServiceLock) Maintain(ctx context.Context) error {
	l.mu.Lock()
	held, node := l.held, l.node
	l.mu.Unlock()
	if !held {
		return l.nothingToWatch()
	}
	nodePath := path.Join(l.dir, node)

	var ticks <-chan time.Time
	if l.verifyInterval > 0 {
		ticker := time.NewTicker(l.verifyInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		events, err := l.armWatch(nodePath)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.release:
			// The caller gave the lock up. Waiting for the node's deletion to
			// arrive as a watch event would keep watching a lock this process
			// has already released, and if that delete failed and left the
			// node in place no event is coming at all: with no verify
			// interval configured there is nothing else in this select to
			// wake it, and it would watch a released lock until ctx ended.
			return l.lose(LossReleased, nil)
		case <-l.done:
			// The generation ended somewhere else — another watcher, a
			// verification, a release. Report the ending that was recorded
			// rather than waiting for an event that has already been taken.
			return l.ended()
		case <-ticks:
			// The watch armed above is still live and still the only one, so
			// it is left in place. Verify re-reads the directory itself, which
			// covers the node being gone without an event.
			if err := l.Verify(); err != nil {
				return err
			}
		case event, open := <-events:
			// The watch fired, so it is spent. Dropping it re-arms on the next
			// pass, which re-checks that the node is still there.
			l.spendWatch(events)
			if !open {
				// The client closes a watch channel once it has delivered, so
				// a closed one means another watcher took the event. What it
				// said is not known here, and the zero value it reads as is
				// not an event to classify — the re-arm above is what finds
				// out, and it ends the generation if the node is gone.
				continue
			}
			if err := l.classifyEvent(event); err != nil {
				return err
			}
		}
	}
}

// armWatch returns the existence watch on this generation's node, arming one
// only when there is none outstanding.
//
// A ZooKeeper watch is one-shot but stays registered on both client and server
// until it fires, so arming a new one whenever a watcher happened to want one
// would leave the old one behind: on a healthy cluster the verify timer alone
// would accumulate a registration per interval for the life of the lock, and
// they would all fire at once the moment the node finally changed.
//
// A node that is already gone, or a watch that cannot be established, ends the
// generation: a lock this process cannot monitor is one it cannot prove it
// still holds.
func (l *ServiceLock) armWatch(nodePath string) (<-chan gozk.Event, error) {
	l.watchMu.Lock()
	defer l.watchMu.Unlock()
	if l.watch != nil {
		return l.watch, nil
	}
	armed, exists, err := l.watchNode(nodePath)
	if err != nil {
		return nil, l.lose(LossUnmonitorable, fmt.Errorf("watch %s: %w", nodePath, err))
	}
	if !exists {
		return nil, l.lose(LossNodeDeleted, nil)
	}
	l.watch = armed
	return armed, nil
}

// watchNode arms a watch that fires when a node goes away, reporting a node
// that is already gone without leaving a registration behind.
//
// Reading the node is what makes that possible: ZooKeeper sets a data watch
// only when the read succeeds, while an existence watch is set either way,
// because its purpose is to report a creation. Every path watched here is a
// sequential lock node, and ZooKeeper never hands out the same sequence twice,
// so an existence watch left on one could not fire before the session ended —
// and a process that keeps retrying an acquisition would leave one per
// attempt, on the client and on the server.
func (l *ServiceLock) watchNode(nodePath string) (<-chan gozk.Event, bool, error) {
	_, _, events, err := l.conn.GetW(nodePath)
	switch {
	case errors.Is(err, gozk.ErrNoNode):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	}
	return events, true, nil
}

// spendWatch drops a watch that has fired, so the next pass arms another. A
// watch armed since is left alone: it is the outstanding one now.
func (l *ServiceLock) spendWatch(spent <-chan gozk.Event) {
	l.watchMu.Lock()
	defer l.watchMu.Unlock()
	if l.watch == spent {
		l.watch = nil
	}
}

// nothingToWatch explains why there is no generation to watch: the ending
// already recorded for one that has ended, or ErrNotHeld for a lock that never
// held one.
//
// The distinction is what keeps a watcher's answer independent of when it
// arrived. A caller that starts watching just after the loss landed has the
// same interest in the loss as one that was already watching, and telling it
// the lock was never held would drop a generation's ending on the floor for no
// reason other than scheduling.
func (l *ServiceLock) nothingToWatch() error {
	if err := l.stillHeld(); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", ErrNotHeld, l.dir)
}

// stillHeld reports the ending of a generation that has ended, and nil while
// one is live. It is what keeps an answer from outliving the thing it is about:
// a check that read ZooKeeper before a release or a loss landed would otherwise
// return that reading afterwards, confirming a generation that is over.
func (l *ServiceLock) stillHeld() error {
	l.mu.Lock()
	ended := l.reason != LossNone
	l.mu.Unlock()
	if ended {
		return l.ended()
	}
	return nil
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
		// Anything else — a data change written to the node by something
		// outside this process, say — is not a loss. The watch is spent, and
		// the pass that re-arms it re-reads the directory.
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
//
// A verification made after the generation ended reports how it ended, the
// same as Maintain, rather than that there is no lock to verify.
//
// That includes an ending that lands while the directory is being read. The
// reading can be entirely before a release and the answer entirely after it,
// and a release whose delete fails leaves the node in place for the checks
// below to find, so the recorded ending is consulted again before this reports
// a lock still held.
func (l *ServiceLock) Verify() error {
	l.mu.Lock()
	held, node := l.held, l.node
	l.mu.Unlock()
	if !held {
		return l.nothingToWatch()
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
	return l.stillHeld()
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
//
// The ending is recorded before the delete, not after. The delete is what a
// watching Maintain sees, and it would report an external NODE_DELETED for a
// node this process took away on purpose — so whichever of them wins that race
// would decide how a deliberate release was remembered.
//
// A release that arrives after the generation is over sweeps only the nodes
// this ServiceLock recorded as its own. By then the process may have rejoined,
// and a rejoin is free to reuse the lock UUID: sweeping the prefix would take
// away the live node of the generation that replaced this one. See sweep.
func (l *ServiceLock) Release() error {
	l.mu.Lock()
	if !l.started {
		l.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrNotHeld, l.dir)
	}
	l.released = true
	// Read before lose records the ending. A first release on a live
	// generation is still the participant whose prefix it is; a second one, or
	// one arriving after the lock was lost and the process rejoined, is not.
	wasLive := l.live
	l.live = false
	l.mu.Unlock()
	l.releaseOnce.Do(func() { close(l.release) })
	l.lose(LossReleased, nil)
	// released is set above, so a create that has not started yet is already
	// refused; taking createMu here waits out one that has. Between them the
	// sweep below cannot miss a node this process made.
	l.createMu.Lock()
	err := l.sweep(wasLive)
	l.createMu.Unlock()
	return err
}

// lose records the end of the generation, once. Returns an error wrapping
// ErrLockLost naming the reason the lock actually ended by, so a later cause
// cannot rewrite an earlier one.
func (l *ServiceLock) lose(reason LossReason, cause error) error {
	l.mu.Lock()
	if reason != LossNone {
		// The generation is over, so this ServiceLock is no longer the
		// process's participant in the directory and the prefix is no longer
		// proof that a node is its own: the caller is free to rejoin with the
		// same lock UUID. Only ended() asks without ending anything, and it
		// passes LossNone. See sweep.
		l.live = false
	}
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
	// Closed under the lock, so a watcher that sees it closed and then reads
	// the ending sees the one recorded just above rather than a generation
	// that still looks held.
	l.doneOnce.Do(func() { close(l.done) })
	l.mu.Unlock()
	if cause != nil {
		return fmt.Errorf("%w: %s (%s): %w", ErrLockLost, id, reason, cause)
	}
	return fmt.Errorf("%w: %s (%s)", ErrLockLost, id, reason)
}

// ended returns the ending already recorded for this generation. lose reports
// the first ending recorded, so asking it again names that one rather than
// inventing another.
func (l *ServiceLock) ended() error { return l.lose(LossNone, nil) }

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

// sweep removes the nodes of this ServiceLock, so one cannot be left holding a
// place in the queue for a process that is no longer waiting.
//
// What counts as this ServiceLock's depends on whether it is still the
// process's participant in the directory. While it is — its acquisition in
// flight, or its generation held — every node carrying the prefix is its own,
// including one created by a create whose answer was lost and whose name it
// therefore never learned. A later ServiceLock is built only after this one is
// done, so nothing else can be carrying the prefix yet.
//
// Once it is done the prefix is no longer proof of anything. It carries the
// lock UUID, which identifies a lock rather than a process only by convention,
// and nothing stops the next ServiceLock being built with the same one: its
// node would carry the same prefix and a higher sequence. A sweep by prefix
// then lets a release arriving after a rejoin — a shutdown path that runs
// twice, or a goroutine that outlived the generation — delete the live node of
// the generation that replaced it, revoking a lock the manager had just seen
// taken. So a finished ServiceLock sweeps only the nodes it recorded as its
// own, which cannot include one it never made.
//
// A failure is reported rather than swallowed. An abandoned ephemeral node
// keeps its sequence, and so its place in line, for as long as the session
// lives: it can become the holder of a lock nobody is waiting on, blocking
// every candidate behind it. The session dropping is the fallback that
// removes it, but only the session's owner can drop it, so the owner has to
// be told.
func (l *ServiceLock) sweep(byPrefix bool) error {
	nodes, err := l.nodesToSweep(byPrefix)
	if err != nil {
		return err
	}
	var failures []error
	for _, node := range nodes {
		if err := l.deleteNode(node); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// nodesToSweep returns the nodes sweep should remove: everything under the
// prefix while this ServiceLock still owns it, and the recorded claims once it
// does not.
func (l *ServiceLock) nodesToSweep(byPrefix bool) ([]string, error) {
	if !byPrefix {
		return l.claimedNodes(), nil
	}
	children, _, err := l.conn.Children(l.dir)
	if err != nil {
		if errors.Is(err, gozk.ErrNoNode) {
			// There is no directory, so no node of this process is holding a
			// place in one. Nothing to clean up and nothing to report.
			return nil, nil
		}
		return nil, fmt.Errorf("list lock nodes in %s: %w", l.dir, err)
	}
	return l.ownNodes(sortLockNodes(children)), nil
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
