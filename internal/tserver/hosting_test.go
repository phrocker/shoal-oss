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
	"sync"
	"testing"
)

func serverLock(sequence int64) LockID {
	return LockID{UUID: "6f1b2c8e-tserver", Sequence: sequence}
}

func managerLock(sequence int64) LockID {
	return LockID{UUID: "b91d4a70-manager", Sequence: sequence}
}

// newTestHost returns a host holding a lock, plus that lock and a fence
// stamped with it and a current manager.
func newTestHost(t *testing.T) (*Host, LockID, Fence) {
	t.Helper()
	host := NewHost()
	lock := serverLock(3)
	if err := host.AdoptLock(lock); err != nil {
		t.Fatalf("AdoptLock: %v", err)
	}
	return host, lock, Fence{Server: lock, Manager: managerLock(7)}
}

// hostTablet drives a tablet all the way to StateHosted.
func hostTablet(t *testing.T, host *Host, fence Fence, e Extent) {
	t.Helper()
	if err := host.Assign(fence, e); err != nil {
		t.Fatalf("Assign(%s): %v", e, err)
	}
	if err := host.LoadComplete(fence.Server, e); err != nil {
		t.Fatalf("LoadComplete(%s): %v", e, err)
	}
}

func wantState(t *testing.T, host *Host, e Extent, want HostingState) {
	t.Helper()
	if got := host.State(e); got != want {
		t.Fatalf("state of %s = %s, want %s", e, got, want)
	}
}

func wantHosted(t *testing.T, host *Host, want ...string) {
	t.Helper()
	hosted := host.Hosted()
	if len(hosted) != len(want) {
		t.Fatalf("hosted = %v, want %v", hosted, want)
	}
	for i := range want {
		if hosted[i].String() != want[i] {
			t.Fatalf("hosted = %v, want %v", hosted, want)
		}
	}
}

// TestHostWithoutLockRefusesEverything covers a process that has not yet won
// the ServiceLock, or has lost it: it has no authority to host, so every
// manager-directed call and every local completion fails closed.
func TestHostWithoutLockRefusesEverything(t *testing.T) {
	host := NewHost()
	lock := serverLock(3)
	fence := Fence{Server: lock, Manager: managerLock(7)}
	e := extent("2", "", "m")

	if err := host.Assign(fence, e); !errors.Is(err, ErrNoLock) {
		t.Fatalf("Assign: want ErrNoLock, got %v", err)
	}
	if err := host.Unassign(fence, e, UnloadGraceful); !errors.Is(err, ErrNoLock) {
		t.Fatalf("Unassign: want ErrNoLock, got %v", err)
	}
	if err := host.LoadComplete(lock, e); !errors.Is(err, ErrNoLock) {
		t.Fatalf("LoadComplete: want ErrNoLock, got %v", err)
	}
	if err := host.UnloadComplete(lock, e); !errors.Is(err, ErrNoLock) {
		t.Fatalf("UnloadComplete: want ErrNoLock, got %v", err)
	}
	if _, held := host.Lock(); held {
		t.Fatal("a fresh host must not report a held lock")
	}
	wantState(t, host, e, StateUnassigned)
}

// TestAssignThroughHostedLifecycle is the happy path: the manager assigns,
// the load finishes, and the tablet becomes visible as hosted.
func TestAssignThroughHostedLifecycle(t *testing.T) {
	host, lock, fence := newTestHost(t)
	e := extent("2", "", "m")

	if err := host.Assign(fence, e); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	wantState(t, host, e, StateLoading)
	wantHosted(t, host) // still loading: not routable yet

	if err := host.LoadComplete(lock, e); err != nil {
		t.Fatalf("LoadComplete: %v", err)
	}
	wantState(t, host, e, StateHosted)
	wantHosted(t, host, "2;m;<")

	metrics := host.Metrics()
	if metrics.Assignments != 1 || metrics.Loads != 1 || metrics.Hosted != 1 || metrics.Loading != 0 {
		t.Fatalf("metrics = %+v", metrics)
	}

	statuses := host.Tablets()
	if len(statuses) != 1 || statuses[0].State != StateHosted || !statuses[0].Extent.Equal(e) {
		t.Fatalf("tablets = %+v", statuses)
	}
}

// TestDuplicateAssignmentFailsClosed covers the acceptance criterion
// directly: a second assignment of a tablet already assigned here is refused
// and changes nothing.
func TestDuplicateAssignmentFailsClosed(t *testing.T) {
	host, lock, fence := newTestHost(t)
	e := extent("2", "", "m")

	for _, state := range []HostingState{StateLoading, StateHosted} {
		if state == StateHosted {
			if err := host.LoadComplete(lock, e); err != nil {
				t.Fatalf("LoadComplete: %v", err)
			}
		} else if err := host.Assign(fence, e); err != nil {
			t.Fatalf("Assign: %v", err)
		}

		err := host.Assign(fence, e)
		if !errors.Is(err, ErrAlreadyAssigned) {
			t.Fatalf("duplicate assign while %s: want ErrAlreadyAssigned, got %v", state, err)
		}
		wantState(t, host, e, state)
	}

	metrics := host.Metrics()
	if metrics.Assignments != 1 {
		t.Fatalf("a refused duplicate must not count as an assignment: %+v", metrics)
	}
	if metrics.RejectedDuplicate != 2 {
		t.Fatalf("RejectedDuplicate = %d, want 2", metrics.RejectedDuplicate)
	}
}

// TestOverlappingAssignmentFailsClosed covers stale split metadata: a parent
// extent arriving after its children are assigned would host the same rows
// twice, and the reverse direction is just as unsafe.
func TestOverlappingAssignmentFailsClosed(t *testing.T) {
	t.Run("parent after children", func(t *testing.T) {
		host, _, fence := newTestHost(t)
		left, right, parent := extent("2", "", "m"), extent("2", "m", ""), extent("2", "", "")

		if err := host.Assign(fence, left); err != nil {
			t.Fatalf("Assign left: %v", err)
		}
		if err := host.Assign(fence, right); err != nil {
			t.Fatalf("split children must both be assignable: %v", err)
		}
		if err := host.Assign(fence, parent); !errors.Is(err, ErrOverlapping) {
			t.Fatalf("want ErrOverlapping, got %v", err)
		}
		wantState(t, host, parent, StateUnassigned)
		wantState(t, host, left, StateLoading)
		wantState(t, host, right, StateLoading)
	})

	t.Run("child after parent", func(t *testing.T) {
		host, _, fence := newTestHost(t)
		parent, child := extent("2", "", ""), extent("2", "d", "m")

		if err := host.Assign(fence, parent); err != nil {
			t.Fatalf("Assign parent: %v", err)
		}
		if err := host.Assign(fence, child); !errors.Is(err, ErrOverlapping) {
			t.Fatalf("want ErrOverlapping, got %v", err)
		}
		wantState(t, host, child, StateUnassigned)
	})

	t.Run("other tables are unaffected", func(t *testing.T) {
		host, _, fence := newTestHost(t)
		if err := host.Assign(fence, extent("2", "", "")); err != nil {
			t.Fatalf("Assign: %v", err)
		}
		if err := host.Assign(fence, extent("3", "", "")); err != nil {
			t.Fatalf("the same range in another table must be assignable: %v", err)
		}
	})
}

func TestAssignRejectsMalformedExtent(t *testing.T) {
	host, _, fence := newTestHost(t)
	if err := host.Assign(fence, extent("2", "m", "d")); !errors.Is(err, ErrInvalidExtent) {
		t.Fatalf("want ErrInvalidExtent, got %v", err)
	}
	if len(host.Tablets()) != 0 {
		t.Fatalf("a malformed extent must not be tracked: %+v", host.Tablets())
	}
}

// TestStaleServerLockFailsClosed covers an assignment the manager minted
// against a lock generation this process no longer holds.
func TestStaleServerLockFailsClosed(t *testing.T) {
	host, lock, fence := newTestHost(t)
	e := extent("2", "", "m")

	stale := Fence{Server: serverLock(lock.Sequence - 1), Manager: fence.Manager}
	if err := host.Assign(stale, e); !errors.Is(err, ErrStaleServerLock) {
		t.Fatalf("older lock: want ErrStaleServerLock, got %v", err)
	}
	// A generation we do not hold yet is equally unusable.
	ahead := Fence{Server: serverLock(lock.Sequence + 1), Manager: fence.Manager}
	if err := host.Assign(ahead, e); !errors.Is(err, ErrStaleServerLock) {
		t.Fatalf("unheld lock: want ErrStaleServerLock, got %v", err)
	}
	// Same sequence, different holder: ambiguous, so refuse.
	other := Fence{Server: LockID{UUID: "someone-else", Sequence: lock.Sequence}, Manager: fence.Manager}
	if err := host.Assign(other, e); !errors.Is(err, ErrStaleServerLock) {
		t.Fatalf("other holder: want ErrStaleServerLock, got %v", err)
	}

	wantState(t, host, e, StateUnassigned)
	if metrics := host.Metrics(); metrics.RejectedStale != 3 || metrics.Assignments != 0 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

// TestSupersededManagerCannotCountermandTheLiveOne covers a manager failover:
// once a newer manager lock is seen it becomes the authority, and the old
// manager's requests are refused.
func TestSupersededManagerCannotCountermandTheLiveOne(t *testing.T) {
	host, lock, fence := newTestHost(t)
	old, current := extent("2", "", "m"), extent("2", "m", "")

	if err := host.Assign(fence, old); err != nil {
		t.Fatalf("Assign: %v", err)
	}

	newer := Fence{Server: lock, Manager: managerLock(fence.Manager.Sequence + 1)}
	if err := host.Assign(newer, current); err != nil {
		t.Fatalf("a newer manager is the authority: %v", err)
	}
	if got, ok := host.ManagerLock(); !ok || !got.Equal(newer.Manager) {
		t.Fatalf("manager lock = %s (held=%v), want %s", got, ok, newer.Manager)
	}

	if err := host.Unassign(fence, current, UnloadImmediate); !errors.Is(err, ErrStaleManagerLock) {
		t.Fatalf("superseded manager: want ErrStaleManagerLock, got %v", err)
	}
	wantState(t, host, current, StateLoading)

	unstamped := Fence{Server: lock}
	if err := host.Assign(unstamped, extent("2", "q", "z")); !errors.Is(err, ErrStaleManagerLock) {
		t.Fatalf("missing manager lock: want ErrStaleManagerLock, got %v", err)
	}
}

// TestGracefulUnloadDrainsThenReleases is the migration path: the tablet
// stops being routable as soon as the manager asks for it back, but is only
// released once the drain finishes.
func TestGracefulUnloadDrainsThenReleases(t *testing.T) {
	host, lock, fence := newTestHost(t)
	e := extent("2", "", "m")
	hostTablet(t, host, fence, e)

	if err := host.Unassign(fence, e, UnloadGraceful); err != nil {
		t.Fatalf("Unassign: %v", err)
	}
	wantState(t, host, e, StateUnloading)
	wantHosted(t, host) // no longer routable

	// Still assigned here, so it cannot be handed back to this host yet.
	if err := host.Assign(fence, e); !errors.Is(err, ErrAlreadyAssigned) {
		t.Fatalf("re-assign while draining: want ErrAlreadyAssigned, got %v", err)
	}

	if err := host.UnloadComplete(lock, e); err != nil {
		t.Fatalf("UnloadComplete: %v", err)
	}
	wantState(t, host, e, StateUnassigned)

	// Released: the manager can migrate it straight back.
	if err := host.Assign(fence, e); err != nil {
		t.Fatalf("re-assign after release: %v", err)
	}

	metrics := host.Metrics()
	if metrics.Unloads != 1 || metrics.ForcedUnloads != 0 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

// TestImmediateUnloadReleasesAtOnce covers forced unload and dead-server
// recovery handoff, where the manager needs the tablet back now.
func TestImmediateUnloadReleasesAtOnce(t *testing.T) {
	host, _, fence := newTestHost(t)
	hosted, loading := extent("2", "", "m"), extent("2", "m", "")
	hostTablet(t, host, fence, hosted)
	if err := host.Assign(fence, loading); err != nil {
		t.Fatalf("Assign: %v", err)
	}

	for _, e := range []Extent{hosted, loading} {
		if err := host.Unassign(fence, e, UnloadImmediate); err != nil {
			t.Fatalf("Unassign(%s): %v", e, err)
		}
		wantState(t, host, e, StateUnassigned)
	}

	metrics := host.Metrics()
	if metrics.Unloads != 2 || metrics.ForcedUnloads != 2 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

// TestUnassignIsIdempotent covers the manager retrying an unassignment, or
// unassigning a tablet this host never had: the requested end state already
// holds, so there is nothing to refuse.
func TestUnassignIsIdempotent(t *testing.T) {
	host, lock, fence := newTestHost(t)
	e := extent("2", "", "m")

	if err := host.Unassign(fence, e, UnloadGraceful); err != nil {
		t.Fatalf("unknown extent: %v", err)
	}
	hostTablet(t, host, fence, e)
	if err := host.Unassign(fence, e, UnloadGraceful); err != nil {
		t.Fatalf("first unassign: %v", err)
	}
	if err := host.Unassign(fence, e, UnloadGraceful); err != nil {
		t.Fatalf("repeated unassign: %v", err)
	}
	wantState(t, host, e, StateUnloading)

	if err := host.UnloadComplete(lock, e); err != nil {
		t.Fatalf("UnloadComplete: %v", err)
	}
	if err := host.Unassign(fence, e, UnloadImmediate); err != nil {
		t.Fatalf("unassign after release: %v", err)
	}
	if metrics := host.Metrics(); metrics.Unloads != 1 {
		t.Fatalf("idempotent unassigns must not double-count: %+v", metrics)
	}
}

func TestLoadFailedReleasesTheAssignment(t *testing.T) {
	host, lock, fence := newTestHost(t)
	e := extent("2", "", "m")
	if err := host.Assign(fence, e); err != nil {
		t.Fatalf("Assign: %v", err)
	}

	if err := host.LoadFailed(lock, e); err != nil {
		t.Fatalf("LoadFailed: %v", err)
	}
	wantState(t, host, e, StateUnassigned)

	if err := host.LoadFailed(lock, e); !errors.Is(err, ErrNotAssigned) {
		t.Fatalf("want ErrNotAssigned, got %v", err)
	}
	if metrics := host.Metrics(); metrics.LoadFailures != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

// TestLoadCompleteAfterUnassignDoesNotRepublish covers a load that finishes
// after the manager already took the tablet back: publishing it would host a
// tablet the manager has reassigned, so the load must release it instead.
func TestLoadCompleteAfterUnassignDoesNotRepublish(t *testing.T) {
	host, lock, fence := newTestHost(t)
	e := extent("2", "", "m")
	if err := host.Assign(fence, e); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	if err := host.Unassign(fence, e, UnloadGraceful); err != nil {
		t.Fatalf("Unassign: %v", err)
	}

	if err := host.LoadComplete(lock, e); !errors.Is(err, ErrWrongState) {
		t.Fatalf("want ErrWrongState, got %v", err)
	}
	wantState(t, host, e, StateUnloading)
	wantHosted(t, host)

	if err := host.UnloadComplete(lock, e); err != nil {
		t.Fatalf("UnloadComplete: %v", err)
	}
	wantState(t, host, e, StateUnassigned)
}

func TestCompletionsRejectWrongState(t *testing.T) {
	host, lock, fence := newTestHost(t)
	e := extent("2", "", "m")
	hostTablet(t, host, fence, e)

	if err := host.LoadComplete(lock, e); !errors.Is(err, ErrWrongState) {
		t.Fatalf("LoadComplete on a hosted tablet: want ErrWrongState, got %v", err)
	}
	if err := host.UnloadComplete(lock, e); !errors.Is(err, ErrWrongState) {
		t.Fatalf("UnloadComplete on a hosted tablet: want ErrWrongState, got %v", err)
	}
	wantState(t, host, e, StateHosted)
}

// TestLockLossDropsEverything covers the acceptance criterion that a
// ServiceLock loss cannot leave a tablet multiply hosted: the host stops
// claiming every tablet at once, and any work still in flight under the lost
// lock fails closed instead of publishing.
func TestLockLossDropsEverything(t *testing.T) {
	host, lock, fence := newTestHost(t)
	loading, hosted, draining := extent("2", "", "d"), extent("2", "d", "m"), extent("2", "m", "")

	if err := host.Assign(fence, loading); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	hostTablet(t, host, fence, hosted)
	hostTablet(t, host, fence, draining)
	if err := host.Unassign(fence, draining, UnloadGraceful); err != nil {
		t.Fatalf("Unassign: %v", err)
	}

	dropped := host.LoseLock()
	if len(dropped) != 3 {
		t.Fatalf("dropped = %v, want all three tablets", dropped)
	}
	for i, want := range []string{"2;d;<", "2;m;d", "2;<;m"} {
		if dropped[i].String() != want {
			t.Fatalf("dropped = %v, want %v in extent order", dropped, []string{"2;d;<", "2;m;d", "2;<;m"})
		}
	}
	wantHosted(t, host)
	if len(host.Tablets()) != 0 {
		t.Fatalf("no tablet may survive a lock loss: %+v", host.Tablets())
	}

	// In-flight work stamped with the lost lock must not publish anything.
	if err := host.LoadComplete(lock, loading); !errors.Is(err, ErrNoLock) {
		t.Fatalf("want ErrNoLock, got %v", err)
	}
	if err := host.Assign(fence, hosted); !errors.Is(err, ErrNoLock) {
		t.Fatalf("want ErrNoLock, got %v", err)
	}

	metrics := host.Metrics()
	if metrics.LockLosses != 1 || metrics.DroppedOnLockLoss != 3 {
		t.Fatalf("metrics = %+v", metrics)
	}

	// A second call for the same loss is a no-op.
	if dropped := host.LoseLock(); dropped != nil {
		t.Fatalf("repeat LoseLock returned %v", dropped)
	}
	if metrics := host.Metrics(); metrics.LockLosses != 1 {
		t.Fatalf("repeat LoseLock double-counted: %+v", metrics)
	}
}

// TestReacquiredLockStartsEmpty covers the restart path: after the lock comes
// back the host holds nothing until the manager assigns again, and requests
// stamped with the old generation stay refused.
func TestReacquiredLockStartsEmpty(t *testing.T) {
	host, lock, fence := newTestHost(t)
	e := extent("2", "", "m")
	hostTablet(t, host, fence, e)
	host.LoseLock()

	reacquired := serverLock(lock.Sequence + 5)
	if err := host.AdoptLock(reacquired); err != nil {
		t.Fatalf("AdoptLock: %v", err)
	}
	if got, ok := host.Lock(); !ok || !got.Equal(reacquired) {
		t.Fatalf("lock = %s (held=%v), want %s", got, ok, reacquired)
	}
	wantState(t, host, e, StateUnassigned)

	if err := host.Assign(fence, e); !errors.Is(err, ErrStaleServerLock) {
		t.Fatalf("old-generation assign: want ErrStaleServerLock, got %v", err)
	}
	if err := host.Assign(Fence{Server: reacquired, Manager: fence.Manager}, e); err != nil {
		t.Fatalf("assign under the new generation: %v", err)
	}
	wantState(t, host, e, StateLoading)
}

func TestAdoptLockGuards(t *testing.T) {
	host, lock, fence := newTestHost(t)

	if err := host.AdoptLock(LockID{}); !errors.Is(err, ErrInvalidLock) {
		t.Fatalf("invalid lock: want ErrInvalidLock, got %v", err)
	}
	if err := host.AdoptLock(lock); !errors.Is(err, ErrLockNotNewer) {
		t.Fatalf("same generation: want ErrLockNotNewer, got %v", err)
	}
	if err := host.AdoptLock(serverLock(lock.Sequence - 1)); !errors.Is(err, ErrLockNotNewer) {
		t.Fatalf("older generation: want ErrLockNotNewer, got %v", err)
	}

	if err := host.Assign(fence, extent("2", "", "m")); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	if err := host.AdoptLock(serverLock(lock.Sequence + 1)); !errors.Is(err, ErrTabletsAssigned) {
		t.Fatalf("adopting while hosting: want ErrTabletsAssigned, got %v", err)
	}

	// A generation already burned by a lock loss cannot come back.
	host.LoseLock()
	if err := host.AdoptLock(lock); !errors.Is(err, ErrLockNotNewer) {
		t.Fatalf("replayed lock after loss: want ErrLockNotNewer, got %v", err)
	}
}

// TestAssignCopiesExtentBounds makes sure a caller reusing its row buffers
// cannot rewrite what the host thinks it hosts.
func TestAssignCopiesExtentBounds(t *testing.T) {
	host, lock, fence := newTestHost(t)
	end := []byte("m")
	e := Extent{TableID: "2", EndRow: end}

	if err := host.Assign(fence, e); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	if err := host.LoadComplete(lock, e); err != nil {
		t.Fatalf("LoadComplete: %v", err)
	}
	end[0] = 'z'

	wantHosted(t, host, "2;m;<")
	hosted := host.Hosted()
	hosted[0].EndRow[0] = 'q'
	wantHosted(t, host, "2;m;<")
}

// TestConcurrentAssignHostsOnce is the race the fence exists for: several
// manager retries landing at once must produce exactly one assignment.
func TestConcurrentAssignHostsOnce(t *testing.T) {
	host, _, fence := newTestHost(t)
	e := extent("2", "", "m")

	const attempts = 16
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		accepted  int
		duplicate int
	)
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			err := host.Assign(fence, e)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrAlreadyAssigned):
				duplicate++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if accepted != 1 || duplicate != attempts-1 {
		t.Fatalf("accepted=%d duplicate=%d, want 1 and %d", accepted, duplicate, attempts-1)
	}
	wantState(t, host, e, StateLoading)
	if metrics := host.Metrics(); metrics.Assignments != 1 || metrics.Loading != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

// TestConcurrentAssignAndLockLoss checks that assignments racing a lock loss
// either land before it (and get dropped) or fail closed after it — never
// leaving a tablet claimed by a host that no longer holds the lock.
func TestConcurrentAssignAndLockLoss(t *testing.T) {
	host, _, fence := newTestHost(t)

	const attempts = 16
	var wg sync.WaitGroup
	wg.Add(attempts + 1)
	for i := 0; i < attempts; i++ {
		go func(i int) {
			defer wg.Done()
			e := Extent{TableID: "2", PrevEndRow: []byte{byte('a' + i)}, EndRow: []byte{byte('a' + i), 0}}
			if err := host.Assign(fence, e); err != nil && !errors.Is(err, ErrNoLock) {
				t.Errorf("assign %s: unexpected error: %v", e, err)
			}
		}(i)
	}
	go func() {
		defer wg.Done()
		host.LoseLock()
	}()
	wg.Wait()

	if tablets := host.Tablets(); len(tablets) != 0 {
		t.Fatalf("tablets survived the lock loss: %+v", tablets)
	}
	if _, held := host.Lock(); held {
		t.Fatal("lock still reported as held")
	}
}
