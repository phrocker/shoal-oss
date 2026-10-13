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

// holdFirstIntent opens a runtime whose first publication stops right after
// its intent is durable, until release is closed: the holder of its token,
// with its transaction not yet run.
func holdFirstIntent(
	t *testing.T, contentionWait time.Duration,
) (*Runtime, Config, <-chan struct{}, chan struct{}) {
	t.Helper()
	config := testRuntimeConfig(t, testDirectory(t))
	config.ContentionWait = contentionWait
	held := make(chan struct{})
	release := make(chan struct{})
	var fired atomic.Bool
	config.testStageHook = func(stage recoveryStage) error {
		if stage == recoveryStageIntent && fired.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
		return nil
	}
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	return runtime, config, held, release
}

type publishOutcome struct {
	result Result
	err    error
	// committed is whether the holder's write was readable at the moment
	// this publication answered.
	committed bool
}

// TestALostTokenAnswersAfterItsHolderSettles: two publishers derive one
// transaction token from one logical transition and differ in content (who
// published it), so the second's intent conflicts with the first's. Its
// conflict must not be answered while the first is still in flight: a caller
// re-reading then sees the state from before the winner's write, and reports a
// conflict for a transition that is about to exist. That is how concurrent
// approval re-requests and concurrent sweeps came home with a bare "fleet
// registry conflict" under single-core scheduling.
func TestALostTokenAnswersAfterItsHolderSettles(t *testing.T) {
	runtime, config, held, release := holdFirstIntent(t, time.Minute)
	entity := guard.Entity{Kind: 'R', ID: coordination.EntityID("record/one")}
	committed := func() bool {
		head, _, err := runtime.ReadEntity(context.Background(), entity)
		return err == nil && head != nil
	}
	holder := make(chan publishOutcome, 1)
	go func() {
		result, err := runtime.Publish(context.Background(), Request{
			Intent: testIntent(t, config.Domain, "transition", "one-token",
				"published-by-first", guard.ModeAbsentOrIdentical, 0,
				coordination.Digest{}),
		})
		holder <- publishOutcome{result: result, err: err}
	}()
	<-held

	loser := make(chan publishOutcome, 1)
	go func() {
		_, err := runtime.Publish(context.Background(), Request{
			Intent: testIntent(t, config.Domain, "transition", "one-token",
				"published-by-second", guard.ModeAbsentOrIdentical, 0,
				coordination.Digest{}),
		})
		loser <- publishOutcome{err: err, committed: committed()}
	}()

	var lost publishOutcome
	select {
	case lost = <-loser:
		// Answered while the holder was still held: only correct if the
		// holder's write were already readable, which it cannot be.
		close(release)
	case <-time.After(500 * time.Millisecond):
		close(release)
		lost = <-loser
	}
	won := <-holder
	if won.err != nil {
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

// TestALostTokenWaitIsBounded: a holder that never settles — a stalled or
// crashed publisher — holds the loser no longer than the contention wait,
// and the answer is still the conflict.
func TestALostTokenWaitIsBounded(t *testing.T) {
	runtime, config, held, release := holdFirstIntent(t, 50*time.Millisecond)
	holder := make(chan error, 1)
	go func() {
		_, err := runtime.Publish(context.Background(), Request{
			Intent: testIntent(t, config.Domain, "transition", "one-token",
				"published-by-first", guard.ModeAbsentOrIdentical, 0,
				coordination.Digest{}),
		})
		holder <- err
	}()
	<-held
	defer func() {
		close(release)
		if err := <-holder; err != nil {
			t.Errorf("the holder failed: %v", err)
		}
	}()

	done := make(chan error, 1)
	go func() {
		_, err := runtime.Publish(context.Background(), Request{
			Intent: testIntent(t, config.Domain, "transition", "one-token",
				"published-by-second", guard.ModeAbsentOrIdentical, 0,
				coordination.Digest{}),
		})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, transaction.ErrConflict) {
			t.Fatalf("the second publication = %v, want a conflict", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the wait for an unsettled holder outlasted the contention wait")
	}

	// And cancellation ends it sooner.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtime.Publish(ctx, Request{
		Intent: testIntent(t, config.Domain, "transition", "one-token",
			"published-by-third", guard.ModeAbsentOrIdentical, 0,
			coordination.Digest{}),
	}); err == nil {
		t.Fatal("a canceled publication of a held token succeeded")
	}
}
