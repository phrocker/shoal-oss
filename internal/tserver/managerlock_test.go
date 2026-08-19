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
	"sync"
	"testing"
	"time"

	gozk "github.com/go-zookeeper/zk"
)

// fakeManagerReader stands in for the ZooKeeper reader that watches the
// manager lock directory. Readings are scripted so a test can walk a host
// through a manager failover, a transient read failure, and a manager going
// away entirely.
type fakeManagerReader struct {
	mu       sync.Mutex
	children []string
	err      error
	paths    []string
	reads    int
	// script replaces the reading after each call, so successive polls can
	// see different things.
	script []func(*fakeManagerReader)
}

func (r *fakeManagerReader) InstancePath() string { return testInstancePath }

func (r *fakeManagerReader) Children(ctx context.Context, path string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.paths = append(r.paths, path)
	r.reads++
	children, err := append([]string(nil), r.children...), r.err
	if len(r.script) > 0 {
		next := r.script[0]
		r.script = r.script[1:]
		next(r)
	}
	return children, err
}

func (r *fakeManagerReader) set(children []string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.children, r.err = children, err
}

func (r *fakeManagerReader) readCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

func (r *fakeManagerReader) readPaths() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.paths...)
}

func managerNode(holder string, sequence int) string {
	return fmt.Sprintf("zlock#%s#%010d", holder, sequence)
}

// TestReadManagerLockTakesTheLowestNode is the authority rule. Standby
// managers queue in the same directory as the live one, so reading anything
// but the lowest sequence would treat a manager that has not taken over as if
// it had.
func TestReadManagerLockTakesTheLowestNode(t *testing.T) {
	reader := &fakeManagerReader{children: []string{
		managerNode(otherUUID, 7),
		managerNode(managerUUID, 3),
		managerNode(serverUUID, 11),
	}}
	id, err := ReadManagerLock(context.Background(), reader)
	if err != nil {
		t.Fatalf("ReadManagerLock: %v", err)
	}
	want := LockID{UUID: managerUUID, Sequence: 3}
	if id != want {
		t.Fatalf("live manager = %s, want %s", id, want)
	}
	paths := reader.readPaths()
	if len(paths) != 1 || paths[0] != testInstancePath+"/managers/lock" {
		t.Fatalf("read %v, want the Accumulo manager lock directory", paths)
	}
}

func TestReadManagerLockIgnoresStrangers(t *testing.T) {
	reader := &fakeManagerReader{children: []string{
		"zlock#not-a-uuid#0000000000",
		"README",
		managerNode(managerUUID, 4),
	}}
	id, err := ReadManagerLock(context.Background(), reader)
	if err != nil {
		t.Fatalf("ReadManagerLock: %v", err)
	}
	if want := (LockID{UUID: managerUUID, Sequence: 4}); id != want {
		t.Fatalf("live manager = %s, want %s", id, want)
	}
}

// TestReadManagerLockReportsAnAbsentManager keeps "there is no manager" apart
// from "I could not tell", because only the first is grounds for withdrawing
// a manager's authority.
func TestReadManagerLockReportsAnAbsentManager(t *testing.T) {
	for _, tt := range []struct {
		name     string
		children []string
		err      error
	}{
		{"no lock directory", nil, gozk.ErrNoNode},
		{"empty directory", nil, nil},
		{"nothing that parses", []string{"README", "zlock#bad#0000000000"}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader := &fakeManagerReader{children: tt.children, err: tt.err}
			if _, err := ReadManagerLock(context.Background(), reader); !errors.Is(err, ErrNoManagerLock) {
				t.Fatalf("ReadManagerLock: want ErrNoManagerLock, got %v", err)
			}
		})
	}
}

func TestReadManagerLockPropagatesReadFailures(t *testing.T) {
	reader := &fakeManagerReader{err: gozk.ErrConnectionClosed}
	_, err := ReadManagerLock(context.Background(), reader)
	if !errors.Is(err, gozk.ErrConnectionClosed) {
		t.Fatalf("ReadManagerLock: want the ZooKeeper error, got %v", err)
	}
	if errors.Is(err, ErrNoManagerLock) {
		t.Fatal("an unreadable directory must not be reported as an absent manager")
	}
}

func TestReadManagerLockRefusesBadInput(t *testing.T) {
	if _, err := ReadManagerLock(context.Background(), nil); err == nil {
		t.Fatal("ReadManagerLock accepted a nil reader")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := &fakeManagerReader{children: []string{managerNode(managerUUID, 1)}}
	if _, err := ReadManagerLock(ctx, reader); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadManagerLock: want context.Canceled, got %v", err)
	}
	if reader.readCount() != 0 {
		t.Fatal("a cancelled read still went to ZooKeeper")
	}
}

// TestManagerAuthorityIsObservedNotAsserted is the whole reason this observer
// exists. A manager cannot talk its way into authority: until the lock has
// been read from ZooKeeper, every manager-directed request fails closed, and
// what makes them start working is the observation, not the request.
func TestManagerAuthorityIsObservedNotAsserted(t *testing.T) {
	host := NewHost()
	if err := host.AdoptLock(serverLock(1)); err != nil {
		t.Fatalf("AdoptLock: %v", err)
	}
	fence := Fence{Server: serverLock(1), Manager: managerLock(4)}
	extent := Extent{TableID: "2", EndRow: []byte("m")}

	if _, err := host.Assign(fence, extent); !errors.Is(err, ErrStaleManagerLock) {
		t.Fatalf("Assign before observing a manager: want ErrStaleManagerLock, got %v", err)
	}

	reader := &fakeManagerReader{children: []string{managerNode(managerUUID, 4)}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch := make(chan error, 1)
	go func() { watch <- WatchManagerLock(ctx, reader, host, time.Millisecond) }()

	waitFor(t, "the manager lock to be observed", func() bool {
		observed, ok := host.ManagerLock()
		return ok && observed == managerLock(4)
	})
	if _, err := host.Assign(fence, extent); err != nil {
		t.Fatalf("Assign after observing the live manager: %v", err)
	}

	cancel()
	if err := <-watch; !errors.Is(err, context.Canceled) {
		t.Fatalf("WatchManagerLock: want context.Canceled, got %v", err)
	}
}

// TestWatchManagerLockFollowsAFailover checks the observer moves authority
// forward when the live manager changes.
func TestWatchManagerLockFollowsAFailover(t *testing.T) {
	host := NewHost()
	reader := &fakeManagerReader{children: []string{managerNode(managerUUID, 4)}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = WatchManagerLock(ctx, reader, host, time.Millisecond) }()

	waitFor(t, "the first manager", func() bool {
		observed, ok := host.ManagerLock()
		return ok && observed == managerLock(4)
	})

	// The live manager died and the standby took over with a later node.
	reader.set([]string{managerNode(otherUUID, 9)}, nil)
	waitFor(t, "the failover", func() bool {
		observed, ok := host.ManagerLock()
		return ok && observed == LockID{UUID: otherUUID, Sequence: 9}
	})
}

// TestWatchManagerLockKeepsAuthorityThroughAReadFailure is the difference
// between not knowing and knowing there is nobody. Dropping the live manager
// because ZooKeeper was briefly unreachable would refuse that manager's
// assignments for no reason.
func TestWatchManagerLockKeepsAuthorityThroughAReadFailure(t *testing.T) {
	host := NewHost()
	reader := &fakeManagerReader{children: []string{managerNode(managerUUID, 4)}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = WatchManagerLock(ctx, reader, host, time.Millisecond) }()

	waitFor(t, "the manager lock to be observed", func() bool {
		_, ok := host.ManagerLock()
		return ok
	})

	reader.set(nil, gozk.ErrConnectionClosed)
	before := reader.readCount()
	waitFor(t, "several failed reads", func() bool { return reader.readCount() > before+3 })

	observed, ok := host.ManagerLock()
	if !ok || observed != managerLock(4) {
		t.Fatalf("manager authority = %s, %v after a read failure; want it retained", observed, ok)
	}
}

// TestWatchManagerLockClearsAuthorityWhenTheManagerIsGone is the other half:
// a directory that is readable and holds no lock is evidence there is no
// manager, and manager-directed work stops until one appears.
func TestWatchManagerLockClearsAuthorityWhenTheManagerIsGone(t *testing.T) {
	host := NewHost()
	if err := host.AdoptLock(serverLock(1)); err != nil {
		t.Fatalf("AdoptLock: %v", err)
	}
	reader := &fakeManagerReader{children: []string{managerNode(managerUUID, 4)}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = WatchManagerLock(ctx, reader, host, time.Millisecond) }()

	waitFor(t, "the manager lock to be observed", func() bool {
		_, ok := host.ManagerLock()
		return ok
	})

	reader.set(nil, nil)
	waitFor(t, "the manager to be forgotten", func() bool {
		_, ok := host.ManagerLock()
		return !ok
	})

	fence := Fence{Server: serverLock(1), Manager: managerLock(4)}
	if _, err := host.Assign(fence, Extent{TableID: "2", EndRow: []byte("m")}); !errors.Is(err, ErrStaleManagerLock) {
		t.Fatalf("Assign with no live manager: want ErrStaleManagerLock, got %v", err)
	}
}

// TestWatchManagerLockSurfacesARefusedEpoch covers the one reading a later
// poll cannot fix. ZooKeeper hands out sequence numbers from a counter on the
// parent, so a manager lock directory that was deleted and remade starts again
// at zero: the manager holding it is live, and its epoch is older than the one
// this host has already seen. Refusing it is right — authority does not move
// backwards — but polling on through it is not, because every later reading is
// the same refusal and the host answers to a manager that is gone in the
// meantime.
func TestWatchManagerLockSurfacesARefusedEpoch(t *testing.T) {
	host := NewHost()
	reader := &fakeManagerReader{children: []string{managerNode(otherUUID, 9)}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch := make(chan error, 1)
	go func() { watch <- WatchManagerLock(ctx, reader, host, time.Millisecond) }()

	waitFor(t, "the manager lock to be observed", func() bool {
		_, ok := host.ManagerLock()
		return ok
	})

	reader.set([]string{managerNode(managerUUID, 2)}, nil)
	select {
	case err := <-watch:
		if !errors.Is(err, ErrLockNotNewer) {
			t.Fatalf("WatchManagerLock: want ErrLockNotNewer, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WatchManagerLock kept polling a refusal no later reading can resolve")
	}

	// Nothing moved backwards on the way out.
	observed, ok := host.ManagerLock()
	if !ok || observed != (LockID{UUID: otherUUID, Sequence: 9}) {
		t.Fatalf("manager authority = %s, %v; want the newer epoch retained", observed, ok)
	}

	// And the documented recovery works: a host with no epoch history takes
	// the live manager on its first reading.
	fresh := NewHost()
	go func() { _ = WatchManagerLock(ctx, reader, fresh, time.Millisecond) }()
	waitFor(t, "a fresh host to take the live manager", func() bool {
		seen, ok := fresh.ManagerLock()
		return ok && seen == managerLock(2)
	})
}

// TestWatchManagerLockObservesBeforeItWaits matters at startup: a tablet
// server that waited a poll interval before its first reading would refuse the
// manager's first assignments after every restart.
func TestWatchManagerLockObservesBeforeItWaits(t *testing.T) {
	host := NewHost()
	reader := &fakeManagerReader{children: []string{managerNode(managerUUID, 4)}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = WatchManagerLock(ctx, reader, host, time.Hour) }()

	waitFor(t, "the first reading", func() bool {
		observed, ok := host.ManagerLock()
		return ok && observed == managerLock(4)
	})
}

func TestWatchManagerLockStopsWhenCancelled(t *testing.T) {
	host := NewHost()
	reader := &fakeManagerReader{children: []string{managerNode(managerUUID, 4)}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- WatchManagerLock(ctx, reader, host, time.Millisecond) }()

	waitFor(t, "the manager lock to be observed", func() bool {
		_, ok := host.ManagerLock()
		return ok
	})
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WatchManagerLock: want context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WatchManagerLock did not stop when cancelled")
	}

	settled := reader.readCount()
	time.Sleep(20 * time.Millisecond)
	if again := reader.readCount(); again != settled {
		t.Fatalf("the observer kept polling after it returned (%d then %d)", settled, again)
	}
}

func TestWatchManagerLockValidatesArguments(t *testing.T) {
	host := NewHost()
	reader := &fakeManagerReader{}
	if err := WatchManagerLock(context.Background(), nil, host, time.Second); err == nil {
		t.Fatal("WatchManagerLock accepted a nil reader")
	}
	if err := WatchManagerLock(context.Background(), reader, nil, time.Second); err == nil {
		t.Fatal("WatchManagerLock accepted a nil host")
	}
	for _, interval := range []time.Duration{0, -time.Second} {
		if err := WatchManagerLock(context.Background(), reader, host, interval); err == nil {
			t.Fatalf("WatchManagerLock accepted a %s poll interval", interval)
		}
	}
	if reader.readCount() != 0 {
		t.Fatal("a refused watch still read ZooKeeper")
	}
}
