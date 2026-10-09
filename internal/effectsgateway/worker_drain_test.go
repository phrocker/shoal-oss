// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package effectsgateway

import (
	"errors"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

func (h *workerHarness) runResult() error {
	h.t.Helper()
	select {
	case err := <-h.done:
		h.done <- err
		return err
	case <-time.After(10 * time.Second):
		h.t.Fatal("the worker did not stop")
	}
	return nil
}

func (h *workerHarness) abandonedCount() float64 {
	for _, line := range h.logs.lines() {
		if line["event"] == "abandoned" {
			count, _ := line["abandoned_runs"].(float64)
			return count
		}
	}
	return -1
}

// TestADrainThatRunsOutWritesWhatMayHaveHappened: the request in flight
// outlasts the grace period. When it ends, the run whose request may have
// reached the target is written to the unrecorded log as outcome_unknown
// before it is abandoned, the abandonment is logged with its count, nothing
// more is completed or reported for it, and Run says so.
func TestADrainThatRunsOutWritesWhatMayHaveHappened(t *testing.T) {
	h := newWorkerHarness(t, nil)
	h.explorer.enqueue("a1", "charge", chargeInput, workerEpoch.Add(time.Hour))
	h.startGated()
	h.cancel()
	eventually(t, "draining", func() bool { _, reason := h.worker.Ready(); return reason == NotReadyDraining })
	grace := GracePeriod(h.cfg.OperationTimeout, h.cfg.PlaneTimeout)
	h.clock.Advance(grace - time.Second)
	settle()
	if h.worker.UnrecordedEntries() != 0 {
		t.Fatal("abandoned before the grace period ended")
	}
	h.clock.Advance(time.Second)
	if err := h.runResult(); !errors.Is(err, ErrDrainAbandoned) {
		t.Fatalf("Run = %v, want ErrDrainAbandoned", err)
	}
	entries := h.log.Entries()
	if len(entries) != 1 || string(entries[0].ActionID) != "a1" || entries[0].Fence != 1 ||
		entries[0].Outcome != fleet.AmbiguityOutcomeUnknown || entries[0].RouteAction != "charge" ||
		string(entries[0].CorrelationID) != "trace-a1" {
		t.Fatalf("entries = %+v", entries)
	}
	if count := h.abandonedCount(); count != 1 {
		t.Fatalf("abandoned count = %v:\n%s", count, h.logs)
	}
	h.release()
	settle()
	if _, _, _, completions, reports := h.explorer.counts(); completions != 0 || reports != 0 {
		t.Fatalf("an abandoned run completed %d and reported %d times", completions, reports)
	}
	h.assertNoSecrets()
}

// TestADrainThatRunsOutWritesNothingForAnUnsentRun: a run abandoned before
// any request left is counted, and not written: nothing can have happened.
func TestADrainThatRunsOutWritesNothingForAnUnsentRun(t *testing.T) {
	hold, entered := make(chan struct{}), make(chan struct{}, 1)
	h := newWorkerHarness(t, nil)
	first := true
	h.credential = func() {
		if first {
			first = false
			entered <- struct{}{}
			<-hold
		}
	}
	t.Cleanup(func() { close(hold) })
	h.explorer.enqueue("a1", "charge", chargeInput, workerEpoch.Add(time.Hour))
	h.start()
	<-entered
	h.cancel()
	eventually(t, "draining", func() bool { _, reason := h.worker.Ready(); return reason == NotReadyDraining })
	h.clock.Advance(GracePeriod(h.cfg.OperationTimeout, h.cfg.PlaneTimeout))
	if err := h.runResult(); !errors.Is(err, ErrDrainAbandoned) {
		t.Fatalf("Run = %v", err)
	}
	if h.worker.UnrecordedEntries() != 0 {
		t.Fatalf("an unsent run was written: %+v", h.log.Entries())
	}
	if count := h.abandonedCount(); count != 1 {
		t.Fatalf("abandoned count = %v", count)
	}
}

// TestTheStartUpRetryStopsOnShutdown: SIGTERM during the retry of held
// reports stops it; the entries not reached stay on disk.
func TestTheStartUpRetryStopsOnShutdown(t *testing.T) {
	clock := newTimerClock()
	dir := t.TempDir()
	fillUnrecorded(t, dir, 10)
	h := &workerHarness{t: t, clock: clock, explorer: newFakeExplorer(t, clock),
		target: newFakeTarget(t), dir: dir, logs: &lockedBuffer{}}
	h.build(nil)
	h.explorer.onAmbiguity = func(n int, _ []byte, _ AmbiguityReport) (Action, error, bool) {
		if n == 1 {
			h.cancel()
		}
		return Action{}, &DispatchError{Op: "ambiguity", Kind: DispatchAmbiguityUnrecorded, Status: 404}, true
	}
	h.start()
	if err := h.runResult(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if reports := h.reportsMade(); len(reports) != 1 {
		t.Fatalf("%d retries after shutdown began, want 1", len(reports))
	}
	if h.log.Len() != 10 {
		t.Fatalf("%d entries left, want all 10", h.log.Len())
	}
	if pulls, _, _, _, _ := h.explorer.counts(); pulls != 0 {
		t.Fatalf("pulled %d times after shutdown", pulls)
	}
}
