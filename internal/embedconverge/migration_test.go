// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package embedconverge

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
)

func newTestMigration(t *testing.T, governor *Governor, clock *fakeClock) *Migration {
	t.Helper()
	m, err := NewMigration(testEpoch(t), governor, clock.Now)
	if err != nil {
		t.Fatalf("NewMigration: %v", err)
	}
	return m
}

func TestMigrationLeasesOnlyPendingFiles(t *testing.T) {
	t.Parallel()

	m := newTestMigration(t, nil, newFakeClock())
	leased := m.Lease(10)
	if len(leased) != 3 {
		t.Fatalf("leased %d files, want 3: the already-converged file must never be leased", len(leased))
	}
	for _, r := range leased {
		if r.Entry == "f-target.rf" {
			t.Fatal("a file already in the target space must not be re-embedded")
		}
	}
	// A leased file is in flight and must not be handed out twice.
	if again := m.Lease(10); len(again) != 0 {
		t.Fatalf("leased %d files again, want 0", len(again))
	}
	if got := m.Progress().InFlight; got != 3 {
		t.Fatalf("InFlight = %d, want 3", got)
	}
}

func TestMigrationCompleteConvergesAndIsIdempotent(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	m := newTestMigration(t, nil, clock)
	for _, r := range m.Lease(10) {
		if err := m.Complete(r, embeddingspace.Has("model-a")); err != nil {
			t.Fatalf("Complete(%s): %v", r.Entry, err)
		}
	}
	progress := m.Progress()
	if progress.Converged != 3 || progress.Skipped != 1 || progress.Total != 4 {
		t.Fatalf("progress = %+v, want 3 converged / 1 skipped / 4 total", progress)
	}
	if !progress.Done() {
		t.Fatal("every file reached the target, so the migration is done")
	}
	if progress.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0", progress.InFlight)
	}
	if len(progress.Spaces) != 1 || progress.Spaces[0].Files != 4 {
		t.Fatalf("spaces = %+v, want one space holding all four files", progress.Spaces)
	}

	// Re-reporting an already converged file is a no-op, not an error,
	// and must not re-open it.
	if err := m.Complete(ref("f-old.rf"), embeddingspace.Has("model-a")); err != nil {
		t.Fatalf("duplicate completion must be idempotent: %v", err)
	}
	if got := m.Progress().Converged; got != 3 {
		t.Fatalf("Converged = %d, want 3 after a duplicate completion", got)
	}
	if len(m.Lease(10)) != 0 {
		t.Fatal("a finished migration must lease nothing, so re-running costs no provider work")
	}
}

func TestMigrationRefusesNonMonotonicCompletion(t *testing.T) {
	t.Parallel()

	m := newTestMigration(t, nil, newFakeClock())
	m.Lease(10)
	// Landing in a third space is the silent-mislabelling case.
	err := m.Complete(ref("f-old.rf"), embeddingspace.Has("model-c"))
	if !errors.Is(err, embeddingspace.ErrNotMonotonic) {
		t.Fatalf("err = %v, want ErrNotMonotonic", err)
	}
	// Regressing a file that already reached the target is refused too.
	err = m.Complete(ref("f-target.rf"), embeddingspace.Has("model-b"))
	if !errors.Is(err, embeddingspace.ErrNotMonotonic) {
		t.Fatalf("err = %v, want ErrNotMonotonic for a regression", err)
	}
	if err := m.Complete(ref("f-old.rf"), embeddingspace.FileState{State: "bogus"}); !errors.Is(err, embeddingspace.ErrInvalidState) {
		t.Fatalf("err = %v, want ErrInvalidState", err)
	}
	if err := m.Complete(ref("nope.rf"), embeddingspace.Has("model-a")); !errors.Is(err, ErrUnknownFile) {
		t.Fatalf("err = %v, want ErrUnknownFile", err)
	}
}

func TestMigrationDefersAFileThatDidNotReachTheTarget(t *testing.T) {
	t.Parallel()

	m := newTestMigration(t, nil, newFakeClock())
	m.Lease(10)
	// A provider failure leaves the file in the space it already had.
	if err := m.Complete(ref("f-old.rf"), embeddingspace.Has("model-b")); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	progress := m.Progress()
	if progress.Deferred != 1 || progress.Converged != 0 {
		t.Fatalf("progress = %+v, want the file deferred rather than converged", progress)
	}
	if progress.Done() {
		t.Fatal("a deferred file means the migration is not done")
	}
}

func TestMigrationFailIsResumable(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	m := newTestMigration(t, nil, clock)
	leased := m.Lease(10)
	for _, r := range leased {
		if err := m.Fail(r, errors.New("provider 429")); err != nil {
			t.Fatalf("Fail(%s): %v", r.Entry, err)
		}
	}
	progress := m.Progress()
	if progress.Deferred != 3 || progress.InFlight != 0 {
		t.Fatalf("progress = %+v, want 3 deferred and nothing in flight", progress)
	}
	// Deferred is not eligible until a resume, so a failing provider is
	// not hammered inside one pass.
	if len(m.Lease(10)) != 0 {
		t.Fatal("deferred files must not be re-leased without a resume")
	}

	raw, err := Encode(m.Snapshot())
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	resumed, err := Resume(decoded, nil, clock.Now)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	again := resumed.Lease(10)
	if len(again) != 3 {
		t.Fatalf("resumed lease = %d files, want 3", len(again))
	}
	for _, r := range again {
		if r.Entry == "f-target.rf" {
			t.Fatal("resuming must not redo work that already reached the target")
		}
	}
	if got := resumed.Progress().Skipped; got != 1 {
		t.Fatalf("Skipped = %d, want 1 after a resume", got)
	}
}

func TestMigrationResumeKeepsConvergedWork(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	m := newTestMigration(t, nil, clock)
	leased := m.Lease(10)
	if err := m.Complete(leased[0], embeddingspace.Has("model-a")); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	raw, err := Encode(m.Snapshot())
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	resumed, err := Resume(decoded, nil, clock.Now)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	progress := resumed.Progress()
	if progress.Converged != 1 || progress.Skipped != 1 || progress.Pending != 2 {
		t.Fatalf("progress = %+v, want 1 converged / 1 skipped / 2 pending", progress)
	}
	for _, r := range resumed.Lease(10) {
		if r.Key() == leased[0].Key() {
			t.Fatal("an already converged file must never be leased again")
		}
	}
}

func TestMigrationAbandonLeavesAConsistentMixedCorpus(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	m := newTestMigration(t, nil, clock)
	leased := m.Lease(10)
	if err := m.Complete(leased[0], embeddingspace.Has("model-a")); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	m.Abandon()

	if len(m.Lease(10)) != 0 {
		t.Fatal("an abandoned migration must lease nothing")
	}
	if err := m.Complete(leased[1], embeddingspace.Has("model-a")); !errors.Is(err, ErrAbandoned) {
		t.Fatalf("err = %v, want ErrAbandoned", err)
	}
	progress := m.Progress()
	if !progress.Abandoned {
		t.Fatal("progress must report the abandonment")
	}
	if progress.Converged != 1 {
		t.Fatalf("Converged = %d, want 1: abandoning must not undo converged work", progress.Converged)
	}
	// Every file is still in exactly one recorded, valid space: the
	// corpus is mixed, not half-labelled.
	total := 0
	for _, space := range progress.Spaces {
		state := embeddingspace.FileState{State: space.State, Identity: space.Identity}
		if err := state.Validate(); err != nil {
			t.Fatalf("abandoned corpus holds an invalid state %+v: %v", state, err)
		}
		total += space.Files
	}
	if total != progress.Total {
		t.Fatalf("spaces account for %d files, want %d", total, progress.Total)
	}
	// Abandoning twice is harmless.
	m.Abandon()
	if !m.Progress().Abandoned {
		t.Fatal("abandon must stay abandoned")
	}
	if m.Stalled(time.Nanosecond) {
		t.Fatal("an abandoned migration is not stalled")
	}
}

func TestMigrationStalledOnlyWhenWorkRemains(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	m := newTestMigration(t, nil, clock)
	if m.Stalled(0) {
		t.Fatal("a non-positive idle window never reports stalled")
	}
	if m.Stalled(time.Minute) {
		t.Fatal("nothing has been idle for a minute yet")
	}
	clock.Advance(2 * time.Minute)
	if !m.Stalled(time.Minute) {
		t.Fatal("a migration with pending work and no progress is stalled")
	}
	for _, r := range m.Lease(10) {
		if err := m.Complete(r, embeddingspace.Has("model-a")); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}
	clock.Advance(time.Hour)
	if m.Stalled(time.Minute) {
		t.Fatal("a finished migration is not stalled")
	}
}

func TestMigrationLeaseSpendsTheGovernorBudget(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	g := NewGovernor(GovernorOptions{Budget: Budget{MaxFiles: 2}, Now: clock.Now})
	m, err := NewMigration(testEpoch(t), g, clock.Now)
	if err != nil {
		t.Fatalf("NewMigration: %v", err)
	}
	if got := len(m.Lease(10)); got != 2 {
		t.Fatalf("leased %d files, want 2: the budget bounds the migration", got)
	}
	if got := m.Progress().State; got != StateRunning {
		t.Fatalf("State = %q, want running", got)
	}
	g.Stop()
	if got := len(m.Lease(10)); got != 0 {
		t.Fatalf("leased %d files after the kill switch, want 0", got)
	}
	if got := m.Progress().State; got != StateStopped {
		t.Fatalf("State = %q, want stopped", got)
	}
}

func TestMigrationIsSafeUnderConcurrentUse(t *testing.T) {
	t.Parallel()

	observed := make([]Observation, 0, 64)
	for i := 0; i < 64; i++ {
		observed = append(observed, Observation{
			Ref:   ref("f" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".rf"),
			State: embeddingspace.Has("model-b"),
		})
	}
	epoch, err := Snapshot("e2", "t", "model-a", ModeForced, 0, observed)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	m, err := NewMigration(epoch, NewGovernor(GovernorOptions{Now: time.Now}), time.Now)
	if err != nil {
		t.Fatalf("NewMigration: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				batch := m.Lease(3)
				if len(batch) == 0 {
					return
				}
				for j, r := range batch {
					if (worker+j)%3 == 0 {
						_ = m.Fail(r, errors.New("provider unavailable"))
						continue
					}
					_ = m.Complete(r, embeddingspace.Has("model-a"))
				}
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = m.Progress()
			_ = m.Snapshot()
			_ = m.Stalled(time.Hour)
		}
	}()
	wg.Wait()

	progress := m.Progress()
	if progress.Total != 64 {
		t.Fatalf("Total = %d, want 64", progress.Total)
	}
	if progress.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0: every lease must be settled", progress.InFlight)
	}
	if progress.Converged+progress.Deferred != 64 {
		t.Fatalf("converged+deferred = %d, want 64", progress.Converged+progress.Deferred)
	}
	if err := m.Snapshot().Validate(); err != nil {
		t.Fatalf("the epoch must stay valid under concurrency: %v", err)
	}
}
