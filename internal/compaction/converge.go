// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package compaction

import (
	"context"
	"errors"
	"fmt"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/iterrt"
)

// Converger re-embeds a compaction's cells into the table's target
// embedding space. It is the seam between the composer, which knows
// nothing about vectors, and a migration engine, which owns the
// embedding provider, the rate limit, the budget and the kill switch.
//
// Lifecycle for one compaction: Begin once, Convert per cell, End once.
// End runs even when Begin refused, so budget accounting always settles.
//
// Implementations must be safe for concurrent use only if the same
// Converger is shared across compactions; a single compaction drives
// them from one goroutine.
type Converger interface {
	// Begin admits (or refuses) convergence of inputs currently in the
	// given states toward target.
	//
	// Returning an error wrapping
	// embeddingspace.ErrConvergenceUnavailable is the normal way to say
	// "not now": the provider is down, the migration is throttled, the
	// budget is spent, or an operator hit the kill switch. The
	// compaction then runs unconverged and preserves the inputs' space.
	// Any other error aborts the compaction.
	Begin(ctx context.Context, target string, inputs []embeddingspace.FileState) error

	// Convert rewrites one cell's value so that any vectors it carries
	// are in the target space. It must return a value that is either
	// fully in the target space or an error; a partial conversion would
	// mislabel the output file.
	//
	// Convert is called for every cell, including cells that carry no
	// vectors at all, which it should return unchanged.
	Convert(ctx context.Context, key *iterrt.Key, value []byte) ([]byte, error)

	// End settles one attempt. converted reports whether the output was
	// actually written in the target space; cells is how many cells were
	// converted before the attempt ended; err is the failure that ended
	// it, or nil.
	End(ctx context.Context, converted bool, cells int64, err error)
}

// ErrConvergenceAborted reports a convergence attempt that failed for a
// reason the composer must not paper over — a malformed cell, a bug in
// the rewriter. Distinct from embeddingspace.ErrConvergenceUnavailable,
// which is the expected "provider is unhappy, try later" signal.
var ErrConvergenceAborted = errors.New("compaction: convergence aborted")

// convergeAttempt is one compaction's decision about convergence.
type convergeAttempt struct {
	converger Converger
	target    string
	// active is true only when a Converger admitted the attempt, so the
	// output may be labelled with target.
	active bool
	// label is what the output file must claim.
	label embeddingspace.FileState
	// inputs are the per-input states, in Spec.Inputs order.
	inputs []embeddingspace.FileState
}

// planConvergence resolves what this compaction should do about the
// table's target space, calling Begin when a rewrite is both wanted and
// possible.
//
// Two properties are load bearing here:
//
//   - Idempotence. When every input already declares the target space,
//     no Converger is consulted at all, so repeatedly compacting a
//     converged tablet costs nothing and does not churn vectors.
//   - Never merging spaces under one label. Without an admitted
//     Converger the merged label comes from embeddingspace.Compatible,
//     which refuses to fold two different identities into one file. With
//     an admitted Converger every cell is rewritten into the target, so
//     mixed input is legitimate: the output really does contain only
//     target-space vectors.
func planConvergence(
	ctx context.Context, spec Spec, states []embeddingspace.FileState,
) (*convergeAttempt, error) {
	attempt := &convergeAttempt{
		converger: spec.Converger,
		inputs:    states,
	}
	target, err := embeddingspace.ParseTarget(spec.TargetEmbeddingSpace)
	if err != nil {
		return nil, fmt.Errorf("compaction: %w", err)
	}
	attempt.target = target

	wanted := false
	for _, state := range states {
		decision, err := embeddingspace.PlanConvergence(target, state)
		if err != nil {
			return nil, fmt.Errorf("compaction: %w", err)
		}
		if decision == embeddingspace.DecisionRewrite {
			wanted = true
		}
	}
	if !wanted || attempt.converger == nil {
		label, err := embeddingspace.Compatible("merge embedding spaces", states...)
		if err != nil {
			return nil, err
		}
		attempt.label = label
		return attempt, nil
	}

	if err := attempt.converger.Begin(ctx, target, states); err != nil {
		if !errors.Is(err, embeddingspace.ErrConvergenceUnavailable) {
			return nil, fmt.Errorf("compaction: begin convergence: %w", err)
		}
		attempt.converger.End(ctx, false, 0, err)
		attempt.converger = nil
		label, mergeErr := embeddingspace.Compatible("merge embedding spaces", states...)
		if mergeErr != nil {
			return nil, mergeErr
		}
		attempt.label = label
		return attempt, nil
	}
	attempt.active = true
	attempt.label = embeddingspace.Has(target)
	return attempt, nil
}

// preserved rebuilds the attempt as an unconverged one after a mid-flight
// provider failure. The output then claims exactly what the inputs
// claimed, which is the self-healing outcome: a later compaction, or a
// forced migration pass, picks the file up again.
func (a *convergeAttempt) preserved() (*convergeAttempt, error) {
	label, err := embeddingspace.Compatible("merge embedding spaces", a.inputs...)
	if err != nil {
		return nil, err
	}
	return &convergeAttempt{target: a.target, label: label, inputs: a.inputs}, nil
}

// verify checks the output label against every input state before the
// label is written into the file and the metadata entry.
func (a *convergeAttempt) verify() error {
	for _, state := range a.inputs {
		if err := embeddingspace.EnsureMonotonic(a.target, state, a.label); err != nil {
			return fmt.Errorf("compaction: %w", err)
		}
	}
	return a.label.Validate()
}

// convergingIterator rewrites each cell's value on its way to the
// writer. Conversion happens on positioning (Seek/Next) rather than in
// GetTopValue so that a provider failure has somewhere to go: the
// interface's readers cannot return an error, so an iterator that
// converted lazily would have to panic or silently emit the unconverted
// value, and the second of those is a mislabelled file.
type convergingIterator struct {
	source    iterrt.SortedKeyValueIterator
	converger Converger
	ctx       context.Context
	value     []byte
	err       error
	converted int64
}

func newConvergingIterator(
	ctx context.Context, source iterrt.SortedKeyValueIterator, converger Converger,
) *convergingIterator {
	return &convergingIterator{source: source, converger: converger, ctx: ctx}
}

func (c *convergingIterator) Init(
	source iterrt.SortedKeyValueIterator, options map[string]string, env iterrt.IteratorEnvironment,
) error {
	return c.source.Init(source, options, env)
}

func (c *convergingIterator) Seek(r iterrt.Range, columnFamilies [][]byte, inclusive bool) error {
	if err := c.source.Seek(r, columnFamilies, inclusive); err != nil {
		return err
	}
	c.convert()
	return c.err
}

func (c *convergingIterator) Next() error {
	if err := c.source.Next(); err != nil {
		return err
	}
	c.convert()
	return c.err
}

// prime converts the cell the wrapped source is already positioned on.
// buildSource seeks the stack before returning, so wrapping it afterwards
// would otherwise skip conversion of the very first cell.
func (c *convergingIterator) prime() error {
	c.convert()
	return c.err
}

func (c *convergingIterator) convert() {
	c.value = nil
	if c.err != nil || !c.source.HasTop() {
		return
	}
	converted, err := c.converger.Convert(c.ctx, c.source.GetTopKey(), c.source.GetTopValue())
	if err != nil {
		c.err = err
		return
	}
	c.value = converted
	c.converted++
}

// HasTop reports no cell once conversion has failed, which stops the
// drain. Callers must consult Err afterwards: a truncated stream that
// looked like a clean end-of-input would publish a partial file.
func (c *convergingIterator) HasTop() bool { return c.err == nil && c.source.HasTop() }

func (c *convergingIterator) GetTopKey() *iterrt.Key { return c.source.GetTopKey() }

func (c *convergingIterator) GetTopValue() []byte { return c.value }

func (c *convergingIterator) DeepCopy(env iterrt.IteratorEnvironment) iterrt.SortedKeyValueIterator {
	return newConvergingIterator(c.ctx, c.source.DeepCopy(env), c.converger)
}

func (c *convergingIterator) Err() error { return c.err }
