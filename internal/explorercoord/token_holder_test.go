/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package explorercoord

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/guard"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
)

// tokenHolderHarness opens a runtime whose stage hook can hold or fail the
// first publication right after its intent is durable — the holder of its
// token, with its transaction not yet run — and reports when a later
// publication has lost its token and is resolving the holder.
type tokenHolderHarness struct {
	runtime *Runtime
	config  Config
	// held is closed when the first publication reaches the hook.
	held chan struct{}
	// release lets a held first publication continue.
	release chan struct{}
	// resolving receives once per publication that lost its token.
	resolving chan struct{}
	// onRecover, when set, runs when a loser is about to recover a holder
	// it found not running, before it re-checks that.
	onRecover atomic.Pointer[func()]
}

const (
	holdTheHolder = iota
	failTheHolder
)

func newTokenHolderHarness(
	t *testing.T, contentionWait time.Duration, mode int,
) *tokenHolderHarness {
	t.Helper()
	h := &tokenHolderHarness{
		config:    testRuntimeConfig(t, testDirectory(t)),
		held:      make(chan struct{}),
		release:   make(chan struct{}),
		resolving: make(chan struct{}, 16),
	}
	h.config.ContentionWait = contentionWait
	var fired atomic.Bool
	h.config.testStageHook = func(stage recoveryStage) error {
		switch {
		case stage == recoveryStageTokenHeld:
			h.resolving <- struct{}{}
		case stage == recoveryStageTokenRecover:
			if run := h.onRecover.Load(); run != nil {
				(*run)()
			}
		case stage == recoveryStageIntent && fired.CompareAndSwap(false, true):
			close(h.held)
			if mode == failTheHolder {
				// The publication returns indeterminate with its intent
				// durable and unsettled, and nothing here will settle it.
				return context.Canceled
			}
			<-h.release
		}
		return nil
	}
	runtime, err := Open(h.config)
	if err != nil {
		t.Fatal(err)
	}
	h.runtime = runtime
	t.Cleanup(func() { _ = runtime.Close() })
	return h
}

func (h *tokenHolderHarness) intent(t *testing.T, publisher string) Intent {
	t.Helper()
	return testIntent(t, h.config.Domain, "transition", "one-token",
		"published-by-"+publisher, guard.ModeAbsentOrIdentical, 0,
		coordination.Digest{})
}

func (h *tokenHolderHarness) committed() bool {
	head, _, err := h.runtime.ReadEntity(context.Background(),
		guard.Entity{Kind: 'R', ID: coordination.EntityID("record/one")})
	return err == nil && head != nil
}

type publishOutcome struct {
	err error
	// committed is whether the holder's write was readable at the moment
	// this publication answered.
	committed bool
	elapsed   time.Duration
}

func (h *tokenHolderHarness) publish(
	ctx context.Context, t *testing.T, publisher string,
) <-chan publishOutcome {
	intent := h.intent(t, publisher)
	done := make(chan publishOutcome, 1)
	go func() {
		started := time.Now()
		_, err := h.runtime.Publish(ctx, Request{Intent: intent})
		done <- publishOutcome{
			err: err, committed: h.committed(), elapsed: time.Since(started),
		}
	}()
	return done
}

func await(t *testing.T, done <-chan publishOutcome, what string) publishOutcome {
	t.Helper()
	select {
	case outcome := <-done:
		return outcome
	case <-time.After(20 * time.Second):
		t.Fatalf("%s did not answer", what)
		return publishOutcome{}
	}
}

// TestALostTokenAnswersAfterItsHolderSettles: two publishers derive one
// transaction token from one logical transition and differ in content (who
// published it), so the second's intent conflicts with the first's. Its
// conflict must not be answered while the first is still in flight: a caller
// re-reading then sees the state from before the winner's write, and reports a
// conflict for a transition that is about to exist. That is how concurrent
// approval re-requests and concurrent sweeps came home with a bare "fleet
// registry conflict" under single-core scheduling (#729).
func TestALostTokenAnswersAfterItsHolderSettles(t *testing.T) {
	h := newTokenHolderHarness(t, time.Minute, holdTheHolder)
	holder := h.publish(context.Background(), t, "first")
	<-h.held

	loser := h.publish(context.Background(), t, "second")
	var lost publishOutcome
	select {
	case lost = <-loser:
		// Answered while the holder was still held: only correct if the
		// holder's write were already readable, which it cannot be.
		close(h.release)
	case <-time.After(500 * time.Millisecond):
		close(h.release)
		lost = await(t, loser, "the second publication")
	}
	if won := await(t, holder, "the holder"); won.err != nil {
		t.Fatalf("the holder failed: %v", won.err)
	}
	if !errors.Is(lost.err, transaction.ErrConflict) {
		t.Fatalf("the second publication = %v, want a conflict", lost.err)
	}
	if !lost.committed {
		t.Fatal("the conflict was answered before the token's holder had " +
			"committed, so a re-read could not see the winner")
	}
}

// TestALostTokenWaitIsBounded: a holder still running here that does not
// settle holds the loser no longer than the contention wait, and the answer
// is still the conflict.
func TestALostTokenWaitIsBounded(t *testing.T) {
	h := newTokenHolderHarness(t, 50*time.Millisecond, holdTheHolder)
	holder := h.publish(context.Background(), t, "first")
	<-h.held
	defer func() {
		close(h.release)
		if won := await(t, holder, "the holder"); won.err != nil {
			t.Errorf("the holder failed: %v", won.err)
		}
	}()
	lost := await(t, h.publish(context.Background(), t, "second"),
		"a publication waiting out the contention wait")
	if !errors.Is(lost.err, transaction.ErrConflict) {
		t.Fatalf("the second publication = %v, want a conflict", lost.err)
	}
}

// TestALostTokenWaitEndsWithItsContext: a context that ends while the loser
// waits ends the wait, and the caller is told so — a deadline or a
// cancellation, as the busy-guard wait answers — rather than a conflict.
func TestALostTokenWaitEndsWithItsContext(t *testing.T) {
	h := newTokenHolderHarness(t, time.Minute, holdTheHolder)
	holder := h.publish(context.Background(), t, "first")
	<-h.held
	defer func() {
		close(h.release)
		if won := await(t, holder, "the holder"); won.err != nil {
			t.Errorf("the holder failed: %v", won.err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loser := h.publish(ctx, t, "second")
	// Canceled only once it has lost its token and is waiting, so the
	// cancellation is the wait's to observe and not an earlier step's.
	select {
	case <-h.resolving:
	case <-time.After(20 * time.Second):
		t.Fatal("the second publication never reached the holder wait")
	}
	cancel()
	lost := await(t, loser, "a canceled waiting publication")
	if !errors.Is(lost.err, context.Canceled) {
		t.Fatalf("the canceled publication = %v, want context.Canceled", lost.err)
	}
}

// TestALostTokenToAnIndeterminateHolderResolvesIt: the holder returned
// indeterminate and left its intent unsettled, so nothing will settle it —
// there is no periodic recovery — and waiting would always run the full
// contention wait, for every loser and every one of a caller's retries. The
// loser resolves the holder instead, as recovery would, and answers at once
// with the holder's outcome readable; the next loser finds it settled.
func TestALostTokenToAnIndeterminateHolderResolvesIt(t *testing.T) {
	h := newTokenHolderHarness(t, time.Minute, failTheHolder)
	won := await(t, h.publish(context.Background(), t, "first"), "the holder")
	if !errors.Is(won.err, ErrIndeterminatePublication) {
		t.Fatalf("the failed holder = %v, want an indeterminate publication",
			won.err)
	}
	for _, publisher := range []string{"second", "third"} {
		lost := await(t, h.publish(context.Background(), t, publisher),
			"the "+publisher+" publication")
		if !errors.Is(lost.err, transaction.ErrConflict) {
			t.Fatalf("the %s publication = %v, want a conflict",
				publisher, lost.err)
		}
		if lost.elapsed > 10*time.Second {
			t.Fatalf("the %s publication took %v: it waited for a holder "+
				"that nothing would settle", publisher, lost.elapsed)
		}
		if !lost.committed {
			t.Fatalf("the %s publication answered with the indeterminate "+
				"holder still unresolved", publisher)
		}
	}
}

// TestALostTokenWaiterDoesNotHoldUpClose: the wait runs outside the runtime
// lock, so Close does not queue behind a waiter for its contention wait, and
// the waiter ends once the runtime is closed.
func TestALostTokenWaiterDoesNotHoldUpClose(t *testing.T) {
	h := newTokenHolderHarness(t, time.Minute, failTheHolder)
	if won := await(t, h.publish(context.Background(), t, "first"),
		"the holder"); !errors.Is(won.err, ErrIndeterminatePublication) {
		t.Fatalf("the failed holder = %v", won.err)
	}
	// Count the unsettled holder as running here, so the loser waits on it
	// rather than resolving it; that is the waiter Close must not queue
	// behind.
	txn, digest := h.holderKey(t)
	defer h.runtime.markInFlight(txn, digest)()

	loser := h.publish(context.Background(), t, "second")
	select {
	case <-h.resolving:
	case <-time.After(20 * time.Second):
		t.Fatal("the second publication never reached the holder wait")
	}
	closed := make(chan error, 1)
	go func() { closed <- h.runtime.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close waited behind a publication waiting for a token holder")
	}
	lost := await(t, loser, "a waiter on a closed runtime")
	if !errors.Is(lost.err, transaction.ErrUnavailable) {
		t.Fatalf("a waiter on a closed runtime = %v, want unavailable: "+
			"the runtime closed, which is not a conflict", lost.err)
	}
}

// holderKey is the in-flight key of the "first" publisher's intent.
func (h *tokenHolderHarness) holderKey(
	t *testing.T,
) (coordination.TXN, coordination.Digest) {
	t.Helper()
	normalized, _, digest, err := canonicalIntent(h.intent(t, "first"))
	if err != nil {
		t.Fatal(err)
	}
	txn, err := DeriveTXN(h.config.Domain, normalized.Operation, normalized.Token)
	if err != nil {
		t.Fatal(err)
	}
	return txn, digest
}

// TestALostTokenWaitsForAHolderRetryThatTookItBack: the loser found the
// holder not running and went to recover it, but by the time it held
// recoveryMu a retry of the holder's own intent was running again. Recovery
// is skipped, because that publication owns the transaction, and the loser
// must then wait for it like any running holder rather than answer its
// conflict while the retry is still deciding the outcome (#729's early
// answer).
func TestALostTokenWaitsForAHolderRetryThatTookItBack(t *testing.T) {
	h := newTokenHolderHarness(t, time.Minute, failTheHolder)
	if won := await(t, h.publish(context.Background(), t, "first"),
		"the holder"); !errors.Is(won.err, ErrIndeterminatePublication) {
		t.Fatalf("the failed holder = %v", won.err)
	}
	txn, digest := h.holderKey(t)
	retryRunning := make(chan struct{})
	var endRetry func()
	var once atomic.Bool
	takeBack := func() {
		if once.CompareAndSwap(false, true) {
			endRetry = h.runtime.markInFlight(txn, digest)
			close(retryRunning)
		}
	}
	h.onRecover.Store(&takeBack)

	loser := h.publish(context.Background(), t, "second")
	select {
	case <-retryRunning:
	case <-time.After(20 * time.Second):
		t.Fatal("the loser never went to recover the holder")
	}
	select {
	case lost := <-loser:
		t.Fatalf("the loser answered %v while the holder's retry was "+
			"running, before the outcome that decides it", lost.err)
	case <-time.After(300 * time.Millisecond):
	}
	// The retry returns without settling, as the holder did; the loser,
	// still polling, recovers it this time and answers with it resolved.
	endRetry()
	lost := await(t, loser, "the loser after the retry ended")
	if !errors.Is(lost.err, transaction.ErrConflict) {
		t.Fatalf("the loser = %v, want a conflict", lost.err)
	}
	if !lost.committed {
		t.Fatal("the loser answered with the holder still unresolved")
	}
}
