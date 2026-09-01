// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

// Package embedconverge orchestrates the migration of a table's corpus
// from one embedding space to another.
//
// It provides the three things a migration needs beyond the per-file
// space identity Phase 1 records:
//
//   - a Governor, which rate limits convergence, spends a declared
//     budget, and gives an operator a kill switch that takes effect on
//     the next cell rather than at the end of the job;
//   - an Epoch, which freezes the set of files a migration is
//     responsible for so progress is measured against a fixed
//     denominator instead of against a corpus that keeps growing under
//     it;
//   - a Migration, which tracks each file in the epoch through a
//     monotonic, idempotent, interruptible, resumable and abandonable
//     lifecycle.
//
// Two convergence modes share all of it. Lazy convergence attaches a
// Converger to ordinary compaction and converges files as compaction
// naturally touches them; forced convergence drives an Epoch to
// completion on demand. Both consult one Governor, so an operator's rate
// limit, budget and kill switch bound the whole migration rather than
// one mode of it.
package embedconverge

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
)

// RunState is the Governor's operator-visible state.
type RunState string

const (
	// StateRunning admits convergence subject to the rate limit and
	// budget.
	StateRunning RunState = "running"

	// StatePaused admits nothing but is reversible. Work already in
	// flight finishes unconverged rather than being torn up.
	StatePaused RunState = "paused"

	// StateStopped is the kill switch: terminal, irreversible, and
	// checked per cell so a migration stops without waiting for the
	// compaction it is inside to finish.
	StateStopped RunState = "stopped"
)

var (
	// ErrThrottled reports that the rate limit has no permit available
	// right now.
	ErrThrottled = errors.New("embedconverge: rate limited")

	// ErrBudgetExhausted reports that the migration has spent its
	// declared budget.
	ErrBudgetExhausted = errors.New("embedconverge: budget exhausted")

	// ErrPaused reports that an operator paused the migration.
	ErrPaused = errors.New("embedconverge: paused")

	// ErrStopped reports that an operator stopped the migration.
	ErrStopped = errors.New("embedconverge: stopped")
)

// Budget bounds what one migration may consume before it stops admitting
// work. A zero field means unlimited for that dimension.
//
// Files and cells are budgeted separately because neither bounds the
// other: a migration of a few enormous files and a migration of many
// tiny ones cost very different amounts at the provider, and only the
// cell count tracks what is actually being embedded.
type Budget struct {
	// MaxFiles caps how many files convergence may be admitted for.
	MaxFiles int64
	// MaxCells caps how many cells may be converted in total.
	MaxCells int64
}

// GovernorOptions configures a Governor.
type GovernorOptions struct {
	// FilesPerSecond is the sustained admission rate. Zero or negative
	// means unlimited, which is only appropriate for a local embedder; a
	// hosted provider must be given a real rate.
	FilesPerSecond float64

	// Burst is the most admissions that may happen back to back after an
	// idle period. Values below one are raised to one, because a bucket
	// that cannot hold a single permit never admits anything.
	Burst float64

	// Budget bounds total consumption. The zero value is unlimited.
	Budget Budget

	// Now injects the clock. Nil uses time.Now.
	//
	// The rate limiter is a token bucket over this clock rather than a
	// timer, so it holds no goroutine, admits without blocking, and is
	// exactly reproducible in tests.
	Now func() time.Time
}

// Governor rate limits and budgets convergence, and carries the kill
// switch. It is safe for concurrent use; one Governor is meant to be
// shared by every compaction and every migration worker on a node, so
// that the limits bound the node rather than each job independently.
type Governor struct {
	mu     sync.Mutex
	state  RunState
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
	budget Budget

	spentFiles int64
	spentCells int64
	admitted   int64
	refused    int64
}

// NewGovernor creates a Governor in the running state with a full token
// bucket, so the first admission is immediate rather than waiting out
// one interval.
func NewGovernor(opts GovernorOptions) *Governor {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	burst := opts.Burst
	if burst < 1 {
		burst = 1
	}
	rate := opts.FilesPerSecond
	if rate < 0 {
		rate = 0
	}
	return &Governor{
		state:  StateRunning,
		rate:   rate,
		burst:  burst,
		tokens: burst,
		last:   now(),
		now:    now,
		budget: opts.Budget,
	}
}

// State reports the current operator state.
func (g *Governor) State() RunState {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state
}

// Pause suspends admission reversibly. It is a no-op once stopped: the
// kill switch is terminal, and letting a pause "downgrade" a stop would
// make the stop reversible by accident.
func (g *Governor) Pause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == StateRunning {
		g.state = StatePaused
	}
}

// Resume returns a paused Governor to running. It is a no-op once
// stopped.
func (g *Governor) Resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == StatePaused {
		g.state = StateRunning
	}
}

// Stop is the kill switch. It is terminal: nothing re-enables a stopped
// Governor, so an operator who has stopped a runaway migration does not
// have to win a race against whatever might try to resume it.
func (g *Governor) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state = StateStopped
}

// AdmitFile asks for permission to converge one file. The returned error
// always wraps embeddingspace.ErrConvergenceUnavailable, so a caller
// that only needs to know "not now" can test for that, while an operator
// interface can still tell throttling from an exhausted budget.
//
// Admission never blocks. A compaction that is refused runs unconverged
// and preserves its inputs' space, which is strictly better than holding
// a compaction slot open waiting for a token.
func (g *Governor) AdmitFile() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.stateErrorLocked(); err != nil {
		g.refused++
		return err
	}
	if err := g.budgetErrorLocked(0); err != nil {
		g.refused++
		return err
	}
	if !g.takeTokenLocked() {
		g.refused++
		return fmt.Errorf("%w: %w: %.3f permits available at %.3f/s",
			embeddingspace.ErrConvergenceUnavailable, ErrThrottled, g.tokens, g.rate)
	}
	g.spentFiles++
	g.admitted++
	return nil
}

// CheckRunning is the per-cell kill-switch and budget probe. It is what
// makes a stop take effect immediately instead of at the end of whatever
// file is being rewritten.
func (g *Governor) CheckRunning(convertedSoFar int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.stateErrorLocked(); err != nil {
		return err
	}
	return g.budgetErrorLocked(convertedSoFar)
}

// SettleFile records the outcome of one admitted file. Cells are charged
// whether or not the file converged: they were embedded either way, and
// a budget that only counted successes would let a failing provider be
// retried without limit.
func (g *Governor) SettleFile(converged bool, cells int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if cells > 0 {
		g.spentCells += cells
	}
	if !converged && g.spentFiles > 0 {
		// The file did not converge, so it must be admissible again.
		// Refunding the file permit is what keeps a provider outage from
		// silently consuming the whole file budget; the cells it did
		// convert before failing stay charged.
		g.spentFiles--
	}
}

// Stats is a consistent snapshot of the Governor's counters.
type Stats struct {
	State      RunState
	SpentFiles int64
	SpentCells int64
	Admitted   int64
	Refused    int64
	Budget     Budget
}

// Stats returns a snapshot of admission accounting.
func (g *Governor) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Stats{
		State:      g.state,
		SpentFiles: g.spentFiles,
		SpentCells: g.spentCells,
		Admitted:   g.admitted,
		Refused:    g.refused,
		Budget:     g.budget,
	}
}

func (g *Governor) stateErrorLocked() error {
	switch g.state {
	case StateStopped:
		return fmt.Errorf("%w: %w", embeddingspace.ErrConvergenceUnavailable, ErrStopped)
	case StatePaused:
		return fmt.Errorf("%w: %w", embeddingspace.ErrConvergenceUnavailable, ErrPaused)
	default:
		return nil
	}
}

func (g *Governor) budgetErrorLocked(pendingCells int64) error {
	if g.budget.MaxFiles > 0 && g.spentFiles >= g.budget.MaxFiles {
		return fmt.Errorf("%w: %w: %d of %d files admitted",
			embeddingspace.ErrConvergenceUnavailable, ErrBudgetExhausted,
			g.spentFiles, g.budget.MaxFiles)
	}
	if g.budget.MaxCells > 0 && g.spentCells+pendingCells >= g.budget.MaxCells {
		return fmt.Errorf("%w: %w: %d of %d cells converted",
			embeddingspace.ErrConvergenceUnavailable, ErrBudgetExhausted,
			g.spentCells+pendingCells, g.budget.MaxCells)
	}
	return nil
}

func (g *Governor) takeTokenLocked() bool {
	if g.rate <= 0 {
		return true
	}
	now := g.now()
	if elapsed := now.Sub(g.last); elapsed > 0 {
		g.tokens += elapsed.Seconds() * g.rate
		if g.tokens > g.burst {
			g.tokens = g.burst
		}
		g.last = now
	}
	if g.tokens < 1 {
		return false
	}
	g.tokens--
	return true
}
