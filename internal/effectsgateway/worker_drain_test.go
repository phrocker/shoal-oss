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
	"context"
	"errors"
	"net/http"
	"os"
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

// TestADrainThatRunsOutWritesWhatMayHaveHappened: the target answered, and
// the completion hangs, ignoring its deadline, as a misbehaving dependency
// might. The drain gives up at the grace period less the exit margin — not at
// the grace period, by which time the kubelet's SIGKILL may already have
// landed — and the run, whose request reached the target, is written to the
// unrecorded log as outcome_unknown in that same instant. The abandonment is
// logged with its count, nothing more is reported for the run, and Run says
// so.
func TestADrainThatRunsOutWritesWhatMayHaveHappened(t *testing.T) {
	h := newWorkerHarness(t, nil)
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	h.explorer.setGate(func(_ context.Context, op string) error {
		if op == "complete" {
			<-hang
		}
		return nil
	})
	h.explorer.enqueue("a1", "charge", chargeInput, workerEpoch.Add(time.Hour))
	h.startGated()
	sigterm := h.clock.Now()
	h.cancel()
	eventually(t, "draining", func() bool { _, reason := h.worker.Ready(); return reason == NotReadyDraining })
	h.release()
	settle()
	bound := GracePeriod(h.cfg.OperationTimeout, h.cfg.PlaneTimeout) - exitMargin
	// Stepped, so the renewal, which keeps the claim alive meanwhile, runs.
	for elapsed := time.Duration(0); elapsed < bound-time.Second; elapsed += time.Second {
		h.clock.Advance(time.Second)
		settle()
	}
	if h.worker.UnrecordedEntries() != 0 || h.abandonedCount() != -1 {
		t.Fatalf("abandoned a second before the drain bound (%s after SIGTERM)", bound)
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
	if written := entries[0].LastAt.Sub(sigterm); written > bound {
		t.Fatalf("the abandonment wrote at %s after SIGTERM, past the drain bound %s", written, bound)
	}
	if count := h.abandonedCount(); count != 1 {
		t.Fatalf("abandoned count = %v:\n%s", count, h.logs)
	}
	if _, _, _, _, reports := h.explorer.counts(); reports != 0 {
		t.Fatalf("an abandoned run reported %d times", reports)
	}
	h.assertNoSecrets()
}

// TestAbandonWritesOnce: every abandoned run goes into one durable rewrite of
// the log, not one per run.
func TestAbandonWritesOnce(t *testing.T) {
	h := newWorkerHarness(t, nil)
	for _, id := range []string{"a1", "a2", "a3"} {
		h.explorer.enqueue(id, "charge", chargeInput, workerEpoch.Add(time.Hour))
	}
	run := func(id string) *claimRun {
		route, _ := h.cfg.Routes.Lookup("charge")
		return &claimRun{action: Action{ID: []byte(id), ClaimFence: 1,
			CorrelationID: []byte("trace-" + id)}, route: route, attempted: true}
	}
	for _, id := range []string{"a1", "a2", "a3"} {
		h.worker.runs[run(id)] = struct{}{}
	}
	var synced int
	previous := fileSyncer
	fileSyncer = func(file *os.File) error { synced++; return file.Sync() }
	t.Cleanup(func() { fileSyncer = previous })
	if abandoned := h.worker.abandon(); abandoned != 3 {
		t.Fatalf("abandoned %d", abandoned)
	}
	// One rewrite: the temporary file and the directory.
	if synced != 2 || h.log.Len() != 3 {
		t.Fatalf("%d fsyncs for %d entries, want one rewrite (2 fsyncs)", synced, h.log.Len())
	}
	_ = h.log.Close()
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

// TestStoppingTheRenewalCancelsItsCallInFlight: the renewal's extension is in
// flight, blocked until its context ends, when a completion during the drain
// comes back indeterminate. The fallback report starts at once — on the
// worker's clock, with no time passing — because stopping the renewal cancels
// its call rather than waiting out the call's timeout, which the grace period
// does not budget for.
func TestStoppingTheRenewalCancelsItsCallInFlight(t *testing.T) {
	h := newWorkerHarness(t, nil)
	extending := make(chan struct{}, 1)
	h.explorer.setGate(func(ctx context.Context, op string) error {
		switch op {
		case "extend":
			extending <- struct{}{}
			<-ctx.Done()
			return &DispatchError{Op: "extend", Kind: DispatchIndeterminate}
		case "complete":
			return &DispatchError{Op: "complete", Kind: DispatchIndeterminate}
		}
		return nil
	})
	h.explorer.enqueue("a1", "charge", chargeInput, workerEpoch.Add(time.Hour))
	h.startGated()
	h.clock.Advance(30 * time.Second)
	select {
	case <-extending:
	case <-time.After(10 * time.Second):
		t.Fatal("no extension started")
	}
	h.cancel()
	eventually(t, "draining", func() bool { _, reason := h.worker.Ready(); return reason == NotReadyDraining })
	stopped := h.clock.Now()
	h.release()
	eventually(t, "the fallback report", func() bool { return len(h.explorer.reportTimes()) > 0 })
	if started := h.explorer.reportTimes()[0]; started.Sub(stopped) > 0 {
		t.Fatalf("the report started %s after the completion, waiting on the renewal", started.Sub(stopped))
	}
}

// TestTheWorstShutdownPathFitsTheDrainBound drives the real worker down the
// slowest path a drain can take, with every call blocking until its own
// timeout on the worker's clock: SIGTERM with a request in flight; the target
// never answers, so the request runs out its operation timeout; the
// completion runs out its whole budget and is indeterminate; the renewal, due
// in the middle of it, blocks too; each fallback report runs out its window;
// and the report lands in the unrecorded log. That last write, measured from
// SIGTERM on the same clock, must come no later than the grace period less
// the exit margin. The bound is measured here, not restated from the
// formula.
func TestTheWorstShutdownPathFitsTheDrainBound(t *testing.T) {
	const operation, plane = 25 * time.Second, time.Second
	h := newWorkerHarness(t, func(cfg *WorkerConfig) {
		cfg.OperationTimeout, cfg.PlaneTimeout, cfg.Renew = operation, plane, false
	})
	sent := make(chan struct{}, 1)
	h.target.script = func(_ int, _ http.ResponseWriter, r *http.Request) bool {
		sent <- struct{}{}
		<-r.Context().Done()
		return true
	}
	h.explorer.enqueue("a1", "charge", chargeInput, workerEpoch.Add(time.Hour))
	h.start()
	select {
	case <-sent:
	case <-time.After(10 * time.Second):
		t.Fatal("no request")
	}
	settle()
	h.explorer.setGate(func(ctx context.Context, op string) error {
		<-ctx.Done()
		if op == "ambiguity" {
			return &DispatchError{Op: op, Kind: DispatchIndeterminate}
		}
		return &DispatchError{Op: op, Kind: DispatchIndeterminate}
	})
	h.clock.Advance(2 * time.Second)
	settle()
	sigterm := h.clock.Now()
	h.cancel()
	bound := GracePeriod(operation, plane) - exitMargin
	for step := 0; h.worker.UnrecordedEntries() == 0 && step < 1000; step++ {
		h.clock.Advance(100 * time.Millisecond)
		time.Sleep(3 * time.Millisecond)
	}
	if err := h.runResult(); err != nil {
		t.Fatalf("Run = %v: the drain did not finish on its own", err)
	}
	entries := h.log.Entries()
	if len(entries) != 1 || entries[0].DispatchError != DispatchIndeterminate ||
		entries[0].Outcome != fleet.AmbiguityOutcomeUnknown {
		t.Fatalf("entries = %+v, want the fallback report's own entry", entries)
	}
	if reports := h.explorer.reportTimes(); len(reports) != FallbackReportAttempts {
		t.Fatalf("%d report attempts, want %d", len(reports), FallbackReportAttempts)
	}
	took := entries[0].LastAt.Sub(sigterm)
	if took > bound {
		t.Fatalf("SIGTERM to the last write took %s, past the drain bound %s", took, bound)
	}
	t.Logf("SIGTERM to the last write: %s of a %s bound", took, bound)
	// The path really is the worst one: it uses nearly all of the bound.
	if took < bound-3*time.Second {
		t.Fatalf("SIGTERM to the last write took %s; the path did not block as intended", took)
	}
}

// TestTheSettleWaitCoversAnExtensionOfUnknownOutcome: the renewal at L/2 is
// in flight when the completion comes back indeterminate. Stopping the
// renewal cancels it — and the explorer has applied it anyway, moving the
// lease to 30s + L = 90s, though the worker never reads that answer. The
// fallback report must wait until no renewal the worker sent can still hold
// the claim: not the old anchored end (60s), but the latest end the
// cancelled extension could have granted, sent + planeTimeout + L = 95s.
func TestTheSettleWaitCoversAnExtensionOfUnknownOutcome(t *testing.T) {
	h := newWorkerHarness(t, nil)
	h.explorer.loseExtendAnswer = true
	h.explorer.setGate(func(ctx context.Context, op string) error {
		switch op {
		case "extend":
			<-ctx.Done() // applied when the worker gives up on it
		case "complete":
			return &DispatchError{Op: "complete", Kind: DispatchIndeterminate}
		}
		return nil
	})
	h.explorer.enqueue("a1", "charge", chargeInput, workerEpoch.Add(time.Hour))
	h.startGated()
	h.clock.Advance(30 * time.Second)
	settle()
	h.release()
	eventually(t, "the cancelled extension to apply", func() bool {
		return h.explorer.record("a1").ClaimLeaseUntil.Equal(workerEpoch.Add(90 * time.Second))
	})
	settle()
	for _, at := range []time.Duration{61, 90, 94} {
		h.clock.Advance(at*time.Second - h.clock.Now().Sub(workerEpoch))
		settle()
		if reports := h.explorer.reportTimes(); len(reports) != 0 {
			t.Fatalf("reported at %ds, before the possibly extended end (95s)", at)
		}
	}
	h.clock.Advance(time.Second) // 95s
	eventually(t, "the report", func() bool { return len(h.explorer.reportTimes()) == 1 })
	if at := h.explorer.reportTimes()[0].Sub(workerEpoch); at != 95*time.Second {
		t.Fatalf("reported at %s", at)
	}
	if h.logs.has("dispatch_error") {
		// Under fence 1, the completion's own indeterminate answer is the
		// only one (a re-claim after the lapse has its own fence).
		count := 0
		for _, line := range h.logs.lines() {
			if line["event"] == "dispatch_error" && line["fence"] == float64(1) {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("%d dispatch_error events; the cancelled renewal logged one:\n%s", count, h.logs)
		}
	}
}

// TestPossibleLeaseEnd: Anchor's rule from the late side.
func TestPossibleLeaseEnd(t *testing.T) {
	sent := workerEpoch.Add(30 * time.Second)
	anchored := Anchored{LeaseLocal: workerEpoch.Add(60 * time.Second), DeadlineLocal: workerEpoch.Add(time.Hour)}
	if got := PossibleLeaseEnd(sent, 5*time.Second, time.Minute, anchored); !got.Equal(workerEpoch.Add(95 * time.Second)) {
		t.Fatalf("unclamped = %s", got.Sub(workerEpoch))
	}
	anchored.DeadlineLocal = workerEpoch.Add(70 * time.Second)
	if got := PossibleLeaseEnd(sent, 5*time.Second, time.Minute, anchored); !got.Equal(workerEpoch.Add(75 * time.Second)) {
		t.Fatalf("clamped = %s", got.Sub(workerEpoch))
	}
}
