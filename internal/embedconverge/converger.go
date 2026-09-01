// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package embedconverge

import (
	"context"
	"fmt"
	"strings"

	"github.com/phrocker/shoal-oss/internal/compaction"
	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/iterrt"
)

// Rewriter re-embeds one cell's value into an embedding space.
//
// It is supplied by the layer that knows the value schema; this package
// deliberately knows nothing about it, so the throttle, budget, kill
// switch and epoch accounting are the same whatever the cells contain.
//
// The contract is strict on purpose. Rewrite must return a value whose
// vectors are entirely in target, or an error. There is no partial
// success, because the output file carries one label and a file labelled
// with a space it only partly contains is exactly the silent
// mislabelling the whole design exists to prevent. A cell carrying no
// vectors is returned unchanged and is not a failure.
//
// An error that wraps compaction.ErrConvergenceAborted fails the
// compaction outright; that is for genuine corruption. Every other error
// is read as a provider failure, which leaves the compaction to complete
// with the input's original space preserved.
type Rewriter interface {
	Rewrite(ctx context.Context, target string, key *iterrt.Key, value []byte) ([]byte, error)
}

// RewriterFunc adapts a function to Rewriter.
type RewriterFunc func(ctx context.Context, target string, key *iterrt.Key, value []byte) ([]byte, error)

// Rewrite implements Rewriter.
func (f RewriterFunc) Rewrite(
	ctx context.Context, target string, key *iterrt.Key, value []byte,
) ([]byte, error) {
	return f(ctx, target, key, value)
}

// Converger implements compaction.Converger on top of a Governor and a
// Rewriter. One Converger is meant to be shared by every compaction on a
// node: the Governor it holds is what makes the rate limit and budget
// apply to the node rather than to each job.
//
// It holds no per-attempt state, which is what makes sharing safe. The
// cell count a compaction converted comes back through End rather than
// being accumulated here, so two concurrent compactions cannot
// mis-attribute each other's work.
type Converger struct {
	target   string
	governor *Governor
	rewriter Rewriter
	observer func(Outcome)
}

// Outcome reports one compaction's convergence result to an observer.
type Outcome struct {
	Target    string
	Converged bool
	Cells     int64
	Err       error
}

// NewConverger builds a Converger for target.
//
// target is the identity this Converger's embedder actually produces —
// it must come from model.EmbeddingSpaceIdentity, never from
// CacheIdentity. CacheIdentity folds in the provider base URL, the HTTP
// client, credential identity and the maximum text size, none of which
// change a single vector: keying convergence on it would make moving an
// Ollama server to a different port invalidate every embedding in the
// corpus despite the vectors being bit-identical.
func NewConverger(
	target string, governor *Governor, rewriter Rewriter, observer func(Outcome),
) (*Converger, error) {
	normalized, err := embeddingspace.ParseTarget(target)
	if err != nil {
		return nil, err
	}
	if normalized == "" {
		return nil, fmt.Errorf("%w: converger needs a target embedding space",
			embeddingspace.ErrNoTarget)
	}
	if governor == nil {
		return nil, fmt.Errorf("embedconverge: converger needs a governor")
	}
	if rewriter == nil {
		return nil, fmt.Errorf("embedconverge: converger needs a rewriter")
	}
	return &Converger{
		target:   normalized,
		governor: governor,
		rewriter: rewriter,
		observer: observer,
	}, nil
}

// Target reports the space this Converger converges to.
func (c *Converger) Target() string { return c.target }

// Begin admits one compaction's convergence attempt.
func (c *Converger) Begin(
	ctx context.Context, target string, inputs []embeddingspace.FileState,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(target) != c.target {
		// The table wants a space this embedder cannot produce. Refusing
		// as "unavailable" rather than as an error is deliberate: the
		// compaction still runs and preserves its inputs, and a node
		// that *can* produce the target converges the file later. Failing
		// the compaction would let one misconfigured node block every
		// compaction on the table.
		return fmt.Errorf("%w: converger produces %q but the table targets %q",
			embeddingspace.ErrConvergenceUnavailable, c.target, strings.TrimSpace(target))
	}
	if len(inputs) == 0 {
		return fmt.Errorf("%w: nothing to converge", embeddingspace.ErrConvergenceUnavailable)
	}
	return c.governor.AdmitFile()
}

// Convert rewrites one cell.
//
// The kill switch is checked here, per cell, rather than only at Begin.
// A migration over a large corpus can sit inside a single compaction for
// a long time, and an operator who hits stop should not have to wait for
// it to finish.
func (c *Converger) Convert(
	ctx context.Context, key *iterrt.Key, value []byte,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.governor.CheckRunning(0); err != nil {
		return nil, err
	}
	converted, err := c.rewriter.Rewrite(ctx, c.target, key, value)
	if err != nil {
		return nil, err
	}
	return converted, nil
}

// End settles the attempt against the Governor's budget and reports it.
func (c *Converger) End(ctx context.Context, converted bool, cells int64, err error) {
	c.governor.SettleFile(converted, cells)
	if c.observer != nil {
		c.observer(Outcome{Target: c.target, Converged: converted, Cells: cells, Err: err})
	}
}

var _ compaction.Converger = (*Converger)(nil)
