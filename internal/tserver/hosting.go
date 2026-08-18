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
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Lifecycle errors. Every one of them is a refusal to change hosting state,
// so callers can treat any non-nil error from a transition as "the tablet is
// exactly where it was".
var (
	// ErrInvalidExtent means the extent cannot name a tablet.
	ErrInvalidExtent = errors.New("tserver: invalid extent")
	// ErrNoLock means this process holds no tablet-server ServiceLock, so it
	// has no authority to host anything.
	ErrNoLock = errors.New("tserver: no tablet server lock held")
	// ErrInvalidLock means a lock identity was offered that names nothing.
	ErrInvalidLock = errors.New("tserver: invalid lock")
	// ErrStaleServerLock means the request was stamped with a tablet-server
	// lock generation other than the one currently held.
	ErrStaleServerLock = errors.New("tserver: stale tablet server lock")
	// ErrStaleManagerLock means the request came from a manager that has been
	// superseded by a later one.
	ErrStaleManagerLock = errors.New("tserver: stale manager lock")
	// ErrAlreadyAssigned means the extent is already assigned here — a
	// duplicate assignment.
	ErrAlreadyAssigned = errors.New("tserver: tablet already assigned")
	// ErrOverlapping means the extent shares rows with an assigned tablet,
	// which is how a stale pre-split or post-merge extent shows up.
	ErrOverlapping = errors.New("tserver: tablet overlaps an assigned tablet")
	// ErrNotAssigned means the extent is not tracked by this host.
	ErrNotAssigned = errors.New("tserver: tablet not assigned")
	// ErrWrongState means the tablet is tracked but not in the state the
	// transition requires.
	ErrWrongState = errors.New("tserver: tablet in wrong hosting state")
	// ErrLockNotNewer means a lock was offered that is not a later generation
	// than the newest one this host has already seen.
	ErrLockNotNewer = errors.New("tserver: lock is not a later generation")
	// ErrTabletsAssigned means a lock change was attempted while tablets are
	// still assigned.
	ErrTabletsAssigned = errors.New("tserver: tablets still assigned")
)

// HostingState is where a tablet sits in this host's lifecycle.
type HostingState int

const (
	// StateUnassigned means the host is not tracking the tablet at all. It is
	// the zero value, so an unknown extent reports as unassigned.
	StateUnassigned HostingState = iota
	// StateLoading means the manager assigned the tablet and the host is
	// bringing it online. It is not yet serving.
	StateLoading
	// StateHosted means the tablet is online and serving here.
	StateHosted
	// StateUnloading means the tablet is draining and will be released. It
	// still counts as assigned, so it cannot be re-assigned until released.
	StateUnloading
)

// String renders the state for logs and status output.
func (s HostingState) String() string {
	switch s {
	case StateUnassigned:
		return "UNASSIGNED"
	case StateLoading:
		return "LOADING"
	case StateHosted:
		return "HOSTED"
	case StateUnloading:
		return "UNLOADING"
	default:
		return fmt.Sprintf("HostingState(%d)", int(s))
	}
}

// UnloadMode selects how a manager-directed unassignment is carried out.
type UnloadMode int

const (
	// UnloadGraceful drains the tablet: it moves to StateUnloading and is
	// released once the host reports the drain finished. Used for migrations
	// and rolling replacement, where the manager waits for a clean handoff.
	UnloadGraceful UnloadMode = iota
	// UnloadImmediate releases the tablet at once, whatever it was doing.
	// Used when the manager needs the tablet back now — a forced unload or a
	// dead-server recovery handoff.
	UnloadImmediate
)

// String renders the mode for logs.
func (m UnloadMode) String() string {
	switch m {
	case UnloadGraceful:
		return "GRACEFUL"
	case UnloadImmediate:
		return "IMMEDIATE"
	default:
		return fmt.Sprintf("UnloadMode(%d)", int(m))
	}
}

// TabletStatus is one tracked tablet as the manager and operators see it.
type TabletStatus struct {
	Extent Extent
	State  HostingState
}

// Metrics is a snapshot of the host's operational counters.
//
// Loading, Hosted and Unloading are gauges derived from the tracked tablets
// at snapshot time. The rest are monotonic counters over the life of the
// process.
type Metrics struct {
	Loading   int
	Hosted    int
	Unloading int

	// Assignments counts accepted manager assignments.
	Assignments uint64
	// Loads counts tablets that finished loading and became hosted.
	Loads uint64
	// LoadFailures counts assignments abandoned because the load failed.
	LoadFailures uint64
	// Unloads counts tablets released back to the manager, gracefully or by
	// force.
	Unloads uint64
	// ForcedUnloads counts the subset of Unloads that skipped draining.
	ForcedUnloads uint64
	// RejectedStale counts transitions refused because their fence did not
	// match the current lock generation.
	RejectedStale uint64
	// RejectedDuplicate counts assignments refused because the tablet, or a
	// tablet overlapping it, was already assigned here.
	RejectedDuplicate uint64
	// LockLosses counts ServiceLock losses.
	LockLosses uint64
	// DroppedOnLockLoss counts tablets dropped because the lock was lost.
	DroppedOnLockLoss uint64
}

// Host tracks the tablets this process hosts on the manager's behalf.
//
// It is the fence, not the decision maker. Assign and Unassign apply what the
// manager asked for; nothing here initiates a hosting change. What Host adds
// is the guarantee that a change is only applied when it can be proven
// current: every manager-directed transition carries the ServiceLock
// generation it was minted under, and anything stale, duplicated, or
// overlapping an already-assigned tablet is refused rather than applied.
//
// Host is safe for concurrent use.
type Host struct {
	mu sync.Mutex

	// held is the ServiceLock currently held; invalid when none is.
	held LockID
	// newest is the newest server lock ever adopted. It outlives a lock loss
	// so a stale lock cannot be re-adopted after the session drops.
	newest LockID
	// manager is the newest manager lock observed. Requests from an older
	// manager are refused; a newer one is adopted on sight.
	manager LockID

	tablets map[string]*tabletEntry
	metrics Metrics
}

type tabletEntry struct {
	extent Extent
	state  HostingState
}

// NewHost returns a host that holds no lock. Until AdoptLock records an
// acquired ServiceLock, every transition fails closed with ErrNoLock.
func NewHost() *Host {
	return &Host{tablets: make(map[string]*tabletEntry)}
}

// AdoptLock records a newly acquired tablet-server ServiceLock.
//
// The lock must be a later generation than any this host has already used,
// including ones released by LoseLock — a ZooKeeper session that comes back
// always brings a higher sequence, so a lower one is proof the caller is
// replaying a lock it no longer holds. Tablets must all be released first: a
// host that still tracks tablets from an older generation cannot safely
// re-stamp them under a new one.
func (h *Host) AdoptLock(lock LockID) error {
	if !lock.Valid() {
		return fmt.Errorf("%w: %s", ErrInvalidLock, lock)
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.tablets) > 0 {
		return fmt.Errorf("%w: %d assigned, release them before adopting %s",
			ErrTabletsAssigned, len(h.tablets), lock)
	}
	if h.newest.Valid() && !lock.Supersedes(h.newest) {
		return fmt.Errorf("%w: offered %s, already saw %s", ErrLockNotNewer, lock, h.newest)
	}
	h.held = lock
	h.newest = lock
	return nil
}

// LoseLock records the loss of the tablet-server ServiceLock and drops every
// tablet the host was tracking, returning them so the caller can close them.
//
// This is what keeps a tablet from being multiply hosted across a session
// loss. Once the lock is gone the manager is free to give these tablets to
// somebody else, so the host stops claiming them immediately rather than
// waiting to find out; any in-flight transition stamped with the lost lock
// then fails closed.
func (h *Host) LoseLock() []Extent {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.held.Valid() {
		// Already released: nothing to hand back, and no new loss to record.
		return nil
	}
	h.held = LockID{}
	if len(h.tablets) == 0 {
		h.metrics.LockLosses++
		return nil
	}
	dropped := make([]Extent, 0, len(h.tablets))
	for _, entry := range h.tablets {
		dropped = append(dropped, entry.extent.clone())
	}
	sort.Slice(dropped, func(i, j int) bool { return compareExtents(dropped[i], dropped[j]) < 0 })
	h.tablets = make(map[string]*tabletEntry)
	h.metrics.LockLosses++
	h.metrics.DroppedOnLockLoss += uint64(len(dropped))
	return dropped
}

// Lock returns the ServiceLock currently held, and whether one is held.
func (h *Host) Lock() (LockID, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.held, h.held.Valid()
}

// ManagerLock returns the newest manager ServiceLock this host has accepted a
// request from, and whether it has accepted any.
func (h *Host) ManagerLock() (LockID, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.manager, h.manager.Valid()
}

// Assign records a manager-directed assignment and moves the tablet to
// StateLoading. The caller then brings the tablet online and reports the
// outcome with LoadComplete or LoadFailed.
//
// It fails closed — leaving hosting state untouched — when the extent is
// malformed, the fence is stale, the tablet is already assigned here, or the
// extent overlaps one that is. Overlap covers the stale-split case: a parent
// extent arriving after its children are assigned (or the reverse) would host
// the same rows twice, so it is refused.
func (h *Host) Assign(fence Fence, extent Extent) error {
	if err := extent.Validate(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.checkFence(fence); err != nil {
		return err
	}
	if existing, ok := h.tablets[extent.key()]; ok {
		h.metrics.RejectedDuplicate++
		return fmt.Errorf("%w: %s is %s", ErrAlreadyAssigned, existing.extent, existing.state)
	}
	for _, entry := range h.tablets {
		if entry.extent.Overlaps(extent) {
			h.metrics.RejectedDuplicate++
			return fmt.Errorf("%w: %s overlaps %s (%s)",
				ErrOverlapping, extent, entry.extent, entry.state)
		}
	}
	h.tablets[extent.key()] = &tabletEntry{extent: extent.clone(), state: StateLoading}
	h.metrics.Assignments++
	return nil
}

// LoadComplete reports that a loading tablet is online, moving it to
// StateHosted.
//
// server is the lock the load ran under. If it no longer matches, the load
// raced a lock loss and the tablet is not published — the manager has already
// been free to place it elsewhere. If the manager unassigned the tablet while
// it was loading it is in StateUnloading, and this returns ErrWrongState; the
// caller releases it with UnloadComplete instead of publishing it.
func (h *Host) LoadComplete(server LockID, extent Extent) error {
	return h.transition(server, extent, StateLoading, func(entry *tabletEntry) {
		entry.state = StateHosted
		h.metrics.Loads++
	})
}

// LoadFailed reports that a loading tablet could not be brought online and
// releases it. The caller logs why; the host only records that an assignment
// was abandoned so the manager can place the tablet elsewhere.
func (h *Host) LoadFailed(server LockID, extent Extent) error {
	return h.transition(server, extent, StateLoading, func(entry *tabletEntry) {
		delete(h.tablets, entry.extent.key())
		h.metrics.LoadFailures++
	})
}

// Unassign applies a manager-directed unassignment.
//
// UnloadGraceful moves the tablet to StateUnloading and waits for
// UnloadComplete, which is the migration and rolling-replacement path.
// UnloadImmediate releases it on the spot for a forced unload or a
// dead-server recovery handoff.
//
// It is idempotent by design: unassigning a tablet this host does not have —
// because it was never assigned, already released, or dropped on a lock loss
// — succeeds without changing anything. The manager asked for the tablet not
// to be hosted here, and it is not. The fence is still checked first, so a
// superseded manager cannot unassign anything.
func (h *Host) Unassign(fence Fence, extent Extent, mode UnloadMode) error {
	if err := extent.Validate(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.checkFence(fence); err != nil {
		return err
	}
	entry, ok := h.tablets[extent.key()]
	if !ok {
		return nil
	}
	if mode == UnloadImmediate {
		delete(h.tablets, extent.key())
		h.metrics.Unloads++
		h.metrics.ForcedUnloads++
		return nil
	}
	entry.state = StateUnloading
	return nil
}

// UnloadComplete reports that a draining tablet has finished and releases it.
// It is the last step of a graceful unload, and also how a load that was
// unassigned mid-flight gives the tablet back.
func (h *Host) UnloadComplete(server LockID, extent Extent) error {
	return h.transition(server, extent, StateUnloading, func(entry *tabletEntry) {
		delete(h.tablets, entry.extent.key())
		h.metrics.Unloads++
	})
}

// State reports where an extent sits in the lifecycle. An extent this host
// does not track is StateUnassigned.
func (h *Host) State(extent Extent) HostingState {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.tablets[extent.key()]
	if !ok {
		return StateUnassigned
	}
	return entry.state
}

// Hosted returns the tablets currently online here, in extent order. This is
// the set the manager can route reads and writes to.
func (h *Host) Hosted() []Extent {
	h.mu.Lock()
	defer h.mu.Unlock()
	hosted := make([]Extent, 0, len(h.tablets))
	for _, entry := range h.tablets {
		if entry.state == StateHosted {
			hosted = append(hosted, entry.extent.clone())
		}
	}
	sort.Slice(hosted, func(i, j int) bool { return compareExtents(hosted[i], hosted[j]) < 0 })
	return hosted
}

// Tablets returns every tracked tablet with its state, in extent order —
// including the ones still loading or draining, which the manager needs to
// see to tell a slow handoff from a stuck one.
func (h *Host) Tablets() []TabletStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	statuses := make([]TabletStatus, 0, len(h.tablets))
	for _, entry := range h.tablets {
		statuses = append(statuses, TabletStatus{Extent: entry.extent.clone(), State: entry.state})
	}
	sort.Slice(statuses, func(i, j int) bool {
		return compareExtents(statuses[i].Extent, statuses[j].Extent) < 0
	})
	return statuses
}

// Metrics returns a snapshot of the host's counters and gauges.
func (h *Host) Metrics() Metrics {
	h.mu.Lock()
	defer h.mu.Unlock()
	snapshot := h.metrics
	for _, entry := range h.tablets {
		switch entry.state {
		case StateLoading:
			snapshot.Loading++
		case StateHosted:
			snapshot.Hosted++
		case StateUnloading:
			snapshot.Unloading++
		}
	}
	return snapshot
}

// transition runs a local completion: it verifies the tablet is still ours
// under the given lock and is in the state the completion expects, then
// applies apply. Unlike the manager-facing calls it is strict about unknown
// extents — a completion for a tablet we never had is a bug in the caller,
// not a stale view of the cluster.
func (h *Host) transition(server LockID, extent Extent, want HostingState, apply func(*tabletEntry)) error {
	if err := extent.Validate(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.checkServerLock(server); err != nil {
		return err
	}
	entry, ok := h.tablets[extent.key()]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotAssigned, extent)
	}
	if entry.state != want {
		return fmt.Errorf("%w: %s is %s, want %s", ErrWrongState, extent, entry.state, want)
	}
	apply(entry)
	return nil
}

// checkFence validates a manager-directed request against both locks. Callers
// must hold h.mu.
func (h *Host) checkFence(fence Fence) error {
	if err := h.checkServerLock(fence.Server); err != nil {
		return err
	}
	if !fence.Manager.Valid() {
		h.metrics.RejectedStale++
		return fmt.Errorf("%w: request carries no manager lock", ErrStaleManagerLock)
	}
	if h.manager.Valid() && !fence.Manager.Equal(h.manager) && !fence.Manager.Supersedes(h.manager) {
		h.metrics.RejectedStale++
		return fmt.Errorf("%w: request from %s, following %s",
			ErrStaleManagerLock, fence.Manager, h.manager)
	}
	if !h.manager.Valid() || fence.Manager.Supersedes(h.manager) {
		h.manager = fence.Manager
	}
	return nil
}

// checkServerLock validates a request against the ServiceLock this process
// holds. Callers must hold h.mu.
func (h *Host) checkServerLock(server LockID) error {
	if !h.held.Valid() {
		h.metrics.RejectedStale++
		return fmt.Errorf("%w: request stamped %s", ErrNoLock, server)
	}
	if !server.Equal(h.held) {
		h.metrics.RejectedStale++
		return fmt.Errorf("%w: request stamped %s, holding %s", ErrStaleServerLock, server, h.held)
	}
	return nil
}
