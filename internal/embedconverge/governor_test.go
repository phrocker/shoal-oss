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

// fakeClock is a manually advanced clock, so the token bucket's
// behaviour is exact rather than dependent on how long the test took.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1700000000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestGovernorThrottlesAndRefills(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	g := NewGovernor(GovernorOptions{FilesPerSecond: 2, Burst: 2, Now: clock.Now})

	for i := 0; i < 2; i++ {
		if err := g.AdmitFile(); err != nil {
			t.Fatalf("admission %d must come from the initial burst: %v", i, err)
		}
	}
	err := g.AdmitFile()
	if !errors.Is(err, ErrThrottled) {
		t.Fatalf("err = %v, want ErrThrottled", err)
	}
	if !errors.Is(err, embeddingspace.ErrConvergenceUnavailable) {
		t.Fatalf("throttling must read as convergence-unavailable, got %v", err)
	}

	clock.Advance(500 * time.Millisecond)
	if err := g.AdmitFile(); err != nil {
		t.Fatalf("one token must have refilled after 500ms at 2/s: %v", err)
	}
	if err := g.AdmitFile(); !errors.Is(err, ErrThrottled) {
		t.Fatalf("err = %v, want ErrThrottled after spending the refill", err)
	}

	clock.Advance(time.Hour)
	if err := g.AdmitFile(); err != nil {
		t.Fatalf("a long idle period must refill: %v", err)
	}
	// The bucket is capped at Burst, so an hour of idleness does not
	// bank an hour of permits.
	if err := g.AdmitFile(); err != nil {
		t.Fatalf("the second burst permit must be available: %v", err)
	}
	if err := g.AdmitFile(); !errors.Is(err, ErrThrottled) {
		t.Fatalf("err = %v, want ErrThrottled: the bucket must cap at Burst", err)
	}
}

func TestGovernorZeroRateIsUnlimited(t *testing.T) {
	t.Parallel()

	g := NewGovernor(GovernorOptions{Now: newFakeClock().Now})
	for i := 0; i < 100; i++ {
		if err := g.AdmitFile(); err != nil {
			t.Fatalf("admission %d: %v", i, err)
		}
	}
}

func TestGovernorFileBudgetStopsAdmitting(t *testing.T) {
	t.Parallel()

	g := NewGovernor(GovernorOptions{Budget: Budget{MaxFiles: 2}, Now: newFakeClock().Now})
	for i := 0; i < 2; i++ {
		if err := g.AdmitFile(); err != nil {
			t.Fatalf("admission %d: %v", i, err)
		}
		g.SettleFile(true, 10)
	}
	err := g.AdmitFile()
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("err = %v, want ErrBudgetExhausted", err)
	}
	if !errors.Is(err, embeddingspace.ErrConvergenceUnavailable) {
		t.Fatalf("an exhausted budget must read as convergence-unavailable, got %v", err)
	}
}

func TestGovernorRefundsTheFilePermitWhenConvergenceFailed(t *testing.T) {
	t.Parallel()

	g := NewGovernor(GovernorOptions{Budget: Budget{MaxFiles: 1}, Now: newFakeClock().Now})
	if err := g.AdmitFile(); err != nil {
		t.Fatalf("first admission: %v", err)
	}
	// A provider outage must not consume the entire file budget.
	g.SettleFile(false, 3)
	if err := g.AdmitFile(); err != nil {
		t.Fatalf("a failed file must be admissible again: %v", err)
	}
	g.SettleFile(true, 3)
	if err := g.AdmitFile(); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("err = %v, want ErrBudgetExhausted once the file actually converged", err)
	}
	if got := g.Stats().SpentCells; got != 6 {
		t.Fatalf("SpentCells = %d, want 6: cells stay charged even when the file failed", got)
	}
}

func TestGovernorCellBudgetStopsMidFile(t *testing.T) {
	t.Parallel()

	g := NewGovernor(GovernorOptions{Budget: Budget{MaxCells: 10}, Now: newFakeClock().Now})
	if err := g.CheckRunning(0); err != nil {
		t.Fatalf("CheckRunning at zero: %v", err)
	}
	if err := g.CheckRunning(9); err != nil {
		t.Fatalf("CheckRunning under budget: %v", err)
	}
	if err := g.CheckRunning(10); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("err = %v, want ErrBudgetExhausted at the cap", err)
	}
	g.SettleFile(true, 10)
	if err := g.CheckRunning(0); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("err = %v, want ErrBudgetExhausted once spent", err)
	}
}

func TestGovernorPauseIsReversibleAndStopIsNot(t *testing.T) {
	t.Parallel()

	g := NewGovernor(GovernorOptions{Now: newFakeClock().Now})
	g.Pause()
	if got := g.State(); got != StatePaused {
		t.Fatalf("state = %q, want %q", got, StatePaused)
	}
	if err := g.AdmitFile(); !errors.Is(err, ErrPaused) {
		t.Fatalf("err = %v, want ErrPaused", err)
	}
	if err := g.CheckRunning(0); !errors.Is(err, ErrPaused) {
		t.Fatalf("CheckRunning err = %v, want ErrPaused", err)
	}
	g.Resume()
	if err := g.AdmitFile(); err != nil {
		t.Fatalf("resume must re-admit: %v", err)
	}

	g.Stop()
	if err := g.AdmitFile(); !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v, want ErrStopped", err)
	}
	// The kill switch is terminal: neither resuming nor pausing may
	// take a stopped governor back into a state that admits work.
	g.Resume()
	g.Pause()
	if got := g.State(); got != StateStopped {
		t.Fatalf("state = %q, want %q: stop must be terminal", got, StateStopped)
	}
	if err := g.CheckRunning(0); !errors.Is(err, ErrStopped) {
		t.Fatalf("CheckRunning err = %v, want ErrStopped", err)
	}
}

func TestGovernorStatsCountAdmissionsAndRefusals(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	g := NewGovernor(GovernorOptions{FilesPerSecond: 1, Burst: 1, Now: clock.Now})
	if err := g.AdmitFile(); err != nil {
		t.Fatalf("AdmitFile: %v", err)
	}
	_ = g.AdmitFile()
	_ = g.AdmitFile()
	stats := g.Stats()
	if stats.Admitted != 1 {
		t.Fatalf("Admitted = %d, want 1", stats.Admitted)
	}
	if stats.Refused != 2 {
		t.Fatalf("Refused = %d, want 2", stats.Refused)
	}
	if stats.State != StateRunning {
		t.Fatalf("State = %q, want running", stats.State)
	}
}

func TestGovernorIsSafeUnderConcurrentUse(t *testing.T) {
	t.Parallel()

	g := NewGovernor(GovernorOptions{FilesPerSecond: 1000, Burst: 50, Now: time.Now})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if err := g.AdmitFile(); err == nil {
					_ = g.CheckRunning(int64(j))
					g.SettleFile(j%2 == 0, 1)
				}
				_ = g.Stats()
				_ = g.State()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			g.Pause()
			g.Resume()
		}
	}()
	wg.Wait()
	g.Stop()
	if err := g.AdmitFile(); !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v, want ErrStopped", err)
	}
}
