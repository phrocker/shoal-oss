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
	"sync"
	"testing"
	"time"

	gozk "github.com/go-zookeeper/zk"
)

// lockNodePath is the node a process with this UUID gets in a fresh lock
// directory, which lets a test wait for the exact moment participation is
// established.
func lockNodePath(holder string, sequence int) string {
	return path.Join(testLockPath(), fmt.Sprintf("zlock#%s#%010d", holder, sequence))
}

// waitFor polls until cond holds, for tests where the moment of interest is
// reached by another goroutine.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestParticipateAdoptsTheGenerationItAcquired is the join: the fence the host
// stamps on every tablet is the ZooKeeper generation this process actually
// holds, not a number it chose.
func TestParticipateAdoptsTheGenerationItAcquired(t *testing.T) {
	f := newFakeZK()
	host := NewHost()
	lock := newTestLock(t, f, serverUUID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Participate(ctx, lock, host, testLockData(t), nil) }()

	waitArmed(t, f, lockNodePath(serverUUID, 0))
	held, ok := host.Lock()
	if !ok {
		t.Fatal("the host holds no lock while participating")
	}
	want := LockID{UUID: serverUUID, Sequence: 0}
	if held != want {
		t.Fatalf("host holds %s, want the acquired generation %s", held, want)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Participate: want context.Canceled, got %v", err)
	}
}

// TestParticipateDropsTabletsWhenTheLockIsLost is the fence doing its job
// against a real ZooKeeper event: the session ended, so this process stops
// claiming the tablets the manager is now free to place elsewhere.
func TestParticipateDropsTabletsWhenTheLockIsLost(t *testing.T) {
	f := newFakeZK()
	host := NewHost()
	lock := newTestLock(t, f, serverUUID)
	if err := host.ObserveManagerLock(managerLock(1)); err != nil {
		t.Fatalf("ObserveManagerLock: %v", err)
	}

	released := make(chan []Extent, 1)
	done := make(chan error, 1)
	go func() {
		done <- Participate(context.Background(), lock, host, testLockData(t),
			func(dropped []Extent) { released <- dropped })
	}()

	nodePath := lockNodePath(serverUUID, 0)
	waitArmed(t, f, nodePath)

	fence := Fence{Server: serverLock(0), Manager: managerLock(1)}
	first := Extent{TableID: "2", EndRow: []byte("m")}
	second := Extent{TableID: "2", PrevEndRow: []byte("m")}
	hostTablet(t, host, fence, first)
	hostTablet(t, host, fence, second)
	if len(host.Hosted()) != 2 {
		t.Fatalf("hosting %d tablets, want 2", len(host.Hosted()))
	}

	f.expire()

	if err := <-done; !errors.Is(err, ErrLockLost) {
		t.Fatalf("Participate: want ErrLockLost, got %v", err)
	}
	dropped := <-released
	if len(dropped) != 2 {
		t.Fatalf("released %v, want both tablets", dropped)
	}
	if !dropped[0].Equal(first) || !dropped[1].Equal(second) {
		t.Fatalf("released %v, want %v and %v", dropped, first, second)
	}
	if hosted := host.Hosted(); len(hosted) != 0 {
		t.Fatalf("still hosting %v after the lock was lost", hosted)
	}
	if _, ok := host.Lock(); ok {
		t.Fatal("the host still holds a lock after losing it")
	}
	if metrics := host.Metrics(); metrics.LockLosses != 1 || metrics.DroppedOnLockLoss != 2 {
		t.Fatalf("metrics = %+v, want one loss dropping two tablets", metrics)
	}
}

// TestParticipateStopsClaimingBeforeGivingUpTheLock pins the shutdown order.
// Deleting the lock node first would tell the manager these tablets may be
// placed elsewhere while this process still claimed them, which is the window
// where two servers host the same range.
func TestParticipateStopsClaimingBeforeGivingUpTheLock(t *testing.T) {
	f := newFakeZK()
	host := NewHost()
	lock := newTestLock(t, f, serverUUID)
	if err := host.ObserveManagerLock(managerLock(1)); err != nil {
		t.Fatalf("ObserveManagerLock: %v", err)
	}
	nodePath := lockNodePath(serverUUID, 0)

	var mu sync.Mutex
	hostedAtDelete := -1
	f.beforeDelete = func(deleted string) {
		if deleted != nodePath {
			return
		}
		mu.Lock()
		hostedAtDelete = len(host.Hosted())
		mu.Unlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Participate(ctx, lock, host, testLockData(t), nil) }()

	waitArmed(t, f, nodePath)
	hostTablet(t, host, Fence{Server: serverLock(0), Manager: managerLock(1)},
		Extent{TableID: "2", EndRow: []byte("m")})

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Participate: want context.Canceled, got %v", err)
	}

	mu.Lock()
	observed := hostedAtDelete
	mu.Unlock()
	if observed != 0 {
		t.Fatalf("the lock node was deleted while %d tablets were still claimed", observed)
	}
	if f.exists(nodePath) {
		t.Fatal("participation ended without giving the lock up")
	}
}

// TestParticipateGivesBackALockTheHostRefuses covers the generation that
// arrives out of order — a lock whose sequence is not newer than one this host
// already used. Holding it would leave a process registered as a live tablet
// server that refuses everything the manager asks of it, so the registration
// is withdrawn instead.
func TestParticipateGivesBackALockTheHostRefuses(t *testing.T) {
	f := newFakeZK()
	host := NewHost()
	if err := host.AdoptLock(serverLock(5)); err != nil {
		t.Fatalf("AdoptLock: %v", err)
	}
	lock := newTestLock(t, f, serverUUID)

	err := Participate(context.Background(), lock, host, testLockData(t), nil)
	if !errors.Is(err, ErrLockNotNewer) {
		t.Fatalf("Participate: want ErrLockNotNewer, got %v", err)
	}
	if nodes := f.lockNodes(testLockPath()); len(nodes) != 0 {
		t.Fatalf("a refused generation stayed registered in ZooKeeper: %v", nodes)
	}
	if held, _ := host.Lock(); held != serverLock(5) {
		t.Fatalf("the host's generation changed to %s", held)
	}
}

// TestParticipateReportsARefusedGenerationItCouldNotGiveBack is the worst
// case of the same path: the host will not use the lock and ZooKeeper will not
// take it back, which leaves a registration nothing is serving. Both facts
// have to reach the caller.
func TestParticipateReportsARefusedGenerationItCouldNotGiveBack(t *testing.T) {
	f := newFakeZK()
	host := NewHost()
	if err := host.AdoptLock(serverLock(5)); err != nil {
		t.Fatalf("AdoptLock: %v", err)
	}
	lock := newTestLock(t, f, serverUUID)
	f.failDelete(lockNodePath(serverUUID, 0), gozk.ErrConnectionClosed)

	err := Participate(context.Background(), lock, host, testLockData(t), nil)
	if !errors.Is(err, ErrLockNotNewer) {
		t.Fatalf("Participate: want ErrLockNotNewer, got %v", err)
	}
	if !errors.Is(err, gozk.ErrConnectionClosed) {
		t.Fatalf("Participate: the failed release is not reported: %v", err)
	}
}

// TestParticipateRejoinsWithALaterGeneration is the restart: the same process
// comes back, takes a higher sequence, hosts nothing until the manager says
// so, and cannot be tricked by a request stamped with the generation that
// died.
func TestParticipateRejoinsWithALaterGeneration(t *testing.T) {
	f := newFakeZK()
	host := NewHost()
	if err := host.ObserveManagerLock(managerLock(1)); err != nil {
		t.Fatalf("ObserveManagerLock: %v", err)
	}

	first := newTestLock(t, f, serverUUID)
	firstDone := make(chan error, 1)
	go func() { firstDone <- Participate(context.Background(), first, host, testLockData(t), nil) }()
	waitArmed(t, f, lockNodePath(serverUUID, 0))
	hostTablet(t, host, Fence{Server: serverLock(0), Manager: managerLock(1)},
		Extent{TableID: "2", EndRow: []byte("m")})
	f.expire()
	if err := <-firstDone; !errors.Is(err, ErrLockLost) {
		t.Fatalf("first participation: want ErrLockLost, got %v", err)
	}

	second := newTestLock(t, f, serverUUID)
	secondDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { secondDone <- Participate(ctx, second, host, testLockData(t), nil) }()
	waitArmed(t, f, lockNodePath(serverUUID, 1))

	rejoined, ok := host.Lock()
	if !ok || rejoined.Sequence != 1 {
		t.Fatalf("rejoined as %s, %v; want sequence 1", rejoined, ok)
	}
	if hosted := host.Hosted(); len(hosted) != 0 {
		t.Fatalf("a restarted server came back hosting %v", hosted)
	}

	// A request stamped with the generation that died is refused, even though
	// this is the same process at the same address.
	stale := Fence{Server: serverLock(0), Manager: managerLock(1)}
	if _, err := host.Assign(stale, Extent{TableID: "2", EndRow: []byte("m")}); !errors.Is(err, ErrStaleServerLock) {
		t.Fatalf("Assign with the dead generation: want ErrStaleServerLock, got %v", err)
	}
	current := Fence{Server: serverLock(1), Manager: managerLock(1)}
	if _, err := host.Assign(current, Extent{TableID: "2", EndRow: []byte("m")}); err != nil {
		t.Fatalf("Assign under the rejoined generation: %v", err)
	}

	cancel()
	if err := <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("second participation: want context.Canceled, got %v", err)
	}
}

// TestParticipateCancelledWhileQueuedNeverAdopts covers a shutdown that
// arrives before this process ever reached the front of the queue.
func TestParticipateCancelledWhileQueuedNeverAdopts(t *testing.T) {
	f := newFakeZK()
	holder := f.seedForeignLock(testLockPath(), otherUUID, 0)
	host := NewHost()
	lock := newTestLock(t, f, serverUUID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Participate(ctx, lock, host, testLockData(t), nil) }()

	waitArmed(t, f, path.Join(testLockPath(), holder))
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Participate: want context.Canceled, got %v", err)
	}
	if _, ok := host.Lock(); ok {
		t.Fatal("the host adopted a lock it never acquired")
	}
}

// TestParticipateWithoutTabletsSkipsTheReleaseCallback keeps the callback
// meaningful: it is called when there is something to close, not on every
// shutdown.
func TestParticipateWithoutTabletsSkipsTheReleaseCallback(t *testing.T) {
	f := newFakeZK()
	host := NewHost()
	lock := newTestLock(t, f, serverUUID)

	called := false
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Participate(ctx, lock, host, testLockData(t), func([]Extent) { called = true })
	}()
	waitArmed(t, f, lockNodePath(serverUUID, 0))
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Participate: want context.Canceled, got %v", err)
	}
	if called {
		t.Fatal("the release callback ran with no tablets to release")
	}
}

// TestParticipateReportsBothTheLossAndAFailedRelease keeps two independent
// facts visible: why this generation ended, and that its node may still be
// sitting in ZooKeeper claiming to be a live server.
func TestParticipateReportsBothTheLossAndAFailedRelease(t *testing.T) {
	f := newFakeZK()
	host := NewHost()
	lock := newTestLock(t, f, serverUUID)
	nodePath := lockNodePath(serverUUID, 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Participate(ctx, lock, host, testLockData(t), nil) }()

	waitArmed(t, f, nodePath)
	f.failDelete(nodePath, gozk.ErrConnectionClosed)
	cancel()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Participate: want the reason the generation ended, got %v", err)
	}
	if !errors.Is(err, gozk.ErrConnectionClosed) {
		t.Fatalf("Participate: the failed release is not reported: %v", err)
	}
	if _, ok := host.Lock(); ok {
		t.Fatal("the host still claims a generation it stopped participating in")
	}
}

func TestParticipateRefusesMissingArguments(t *testing.T) {
	f := newFakeZK()
	lock := newTestLock(t, f, serverUUID)
	if err := Participate(context.Background(), nil, NewHost(), testLockData(t), nil); err == nil {
		t.Fatal("Participate accepted a nil lock")
	}
	if err := Participate(context.Background(), lock, nil, testLockData(t), nil); err == nil {
		t.Fatal("Participate accepted a nil host")
	}
	if created := f.createdPaths(); len(created) != 0 {
		t.Fatalf("a refused participation wrote %v", created)
	}
}

// TestParticipateRefusesUnusableLockData keeps a process that cannot describe
// itself out of the live-server list entirely.
func TestParticipateRefusesUnusableLockData(t *testing.T) {
	f := newFakeZK()
	host := NewHost()
	lock := newTestLock(t, f, serverUUID)

	err := Participate(context.Background(), lock, host, ServiceLockData{}, nil)
	if !errors.Is(err, ErrInvalidLockData) {
		t.Fatalf("Participate: want ErrInvalidLockData, got %v", err)
	}
	if _, ok := host.Lock(); ok {
		t.Fatal("the host adopted a lock that was never published")
	}
}
