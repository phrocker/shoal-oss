// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package embedconverge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/iterrt"
)

func passthrough() Rewriter {
	return RewriterFunc(func(
		_ context.Context, _ string, _ *iterrt.Key, value []byte,
	) ([]byte, error) {
		return value, nil
	})
}

func TestNewConvergerRequiresATargetAndItsCollaborators(t *testing.T) {
	t.Parallel()

	g := NewGovernor(GovernorOptions{Now: time.Now})
	if _, err := NewConverger("  ", g, passthrough(), nil); !errors.Is(err, embeddingspace.ErrNoTarget) {
		t.Fatalf("err = %v, want ErrNoTarget", err)
	}
	if _, err := NewConverger("model-a", nil, passthrough(), nil); err == nil {
		t.Fatal("a converger without a governor is unthrottled and must be refused")
	}
	if _, err := NewConverger("model-a", g, nil, nil); err == nil {
		t.Fatal("a converger without a rewriter must be refused")
	}
	c, err := NewConverger("  model-a  ", g, passthrough(), nil)
	if err != nil {
		t.Fatalf("NewConverger: %v", err)
	}
	if c.Target() != "model-a" {
		t.Fatalf("Target = %q, want %q", c.Target(), "model-a")
	}
}

func TestConvergerRefusesATargetItCannotProduce(t *testing.T) {
	t.Parallel()

	c, err := NewConverger("model-a", NewGovernor(GovernorOptions{Now: time.Now}), passthrough(), nil)
	if err != nil {
		t.Fatalf("NewConverger: %v", err)
	}
	err = c.Begin(context.Background(), "model-z", []embeddingspace.FileState{embeddingspace.NoEmbeddings()})
	if !errors.Is(err, embeddingspace.ErrConvergenceUnavailable) {
		t.Fatalf("err = %v, want ErrConvergenceUnavailable: a misconfigured node must not fail the compaction", err)
	}
	err = c.Begin(context.Background(), "model-a", nil)
	if !errors.Is(err, embeddingspace.ErrConvergenceUnavailable) {
		t.Fatalf("err = %v, want ErrConvergenceUnavailable with no inputs", err)
	}
	if err := c.Begin(context.Background(), " model-a ", []embeddingspace.FileState{embeddingspace.Unknown()}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
}

func TestConvergerBeginConsumesGovernorAdmission(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	g := NewGovernor(GovernorOptions{FilesPerSecond: 1, Burst: 1, Now: clock.Now})
	c, err := NewConverger("model-a", g, passthrough(), nil)
	if err != nil {
		t.Fatalf("NewConverger: %v", err)
	}
	inputs := []embeddingspace.FileState{embeddingspace.Has("model-b")}
	if err := c.Begin(context.Background(), "model-a", inputs); err != nil {
		t.Fatalf("first Begin: %v", err)
	}
	if err := c.Begin(context.Background(), "model-a", inputs); !errors.Is(err, ErrThrottled) {
		t.Fatalf("err = %v, want ErrThrottled", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Begin(ctx, "model-a", inputs); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestConvergerConvertHonoursTheKillSwitchPerCell(t *testing.T) {
	t.Parallel()

	g := NewGovernor(GovernorOptions{Now: time.Now})
	c, err := NewConverger("model-a", g, passthrough(), nil)
	if err != nil {
		t.Fatalf("NewConverger: %v", err)
	}
	value, err := c.Convert(context.Background(), nil, []byte("cell"))
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if string(value) != "cell" {
		t.Fatalf("value = %q, want %q", value, "cell")
	}

	g.Stop()
	if _, err := c.Convert(context.Background(), nil, []byte("cell")); !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v, want ErrStopped mid-stream", err)
	}
}

func TestConvergerConvertReportsRewriterFailure(t *testing.T) {
	t.Parallel()

	boom := errors.New("provider 503")
	rewriter := RewriterFunc(func(
		_ context.Context, target string, _ *iterrt.Key, _ []byte,
	) ([]byte, error) {
		if target != "model-a" {
			return nil, errors.New("rewriter was given the wrong target: " + target)
		}
		return nil, boom
	})
	c, err := NewConverger("model-a", NewGovernor(GovernorOptions{Now: time.Now}), rewriter, nil)
	if err != nil {
		t.Fatalf("NewConverger: %v", err)
	}
	if _, err := c.Convert(context.Background(), nil, []byte("cell")); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the rewriter's error", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Convert(ctx, nil, []byte("cell")); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestConvergerEndSettlesTheBudgetAndReports(t *testing.T) {
	t.Parallel()

	var seen []Outcome
	g := NewGovernor(GovernorOptions{Budget: Budget{MaxFiles: 1}, Now: time.Now})
	c, err := NewConverger("model-a", g, passthrough(), func(o Outcome) {
		seen = append(seen, o)
	})
	if err != nil {
		t.Fatalf("NewConverger: %v", err)
	}
	inputs := []embeddingspace.FileState{embeddingspace.Has("model-b")}
	if err := c.Begin(context.Background(), "model-a", inputs); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	// A failed attempt refunds the file permit, so a provider outage
	// cannot silently eat the whole budget.
	c.End(context.Background(), false, 7, errors.New("provider down"))
	if err := c.Begin(context.Background(), "model-a", inputs); err != nil {
		t.Fatalf("a failed attempt must leave the file admissible: %v", err)
	}
	c.End(context.Background(), true, 5, nil)
	if err := c.Begin(context.Background(), "model-a", inputs); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("err = %v, want ErrBudgetExhausted", err)
	}

	if len(seen) != 2 {
		t.Fatalf("observer saw %d outcomes, want 2", len(seen))
	}
	if seen[0].Converged || seen[0].Cells != 7 || seen[0].Err == nil {
		t.Fatalf("first outcome = %+v", seen[0])
	}
	if !seen[1].Converged || seen[1].Cells != 5 || seen[1].Target != "model-a" {
		t.Fatalf("second outcome = %+v", seen[1])
	}
	if got := g.Stats().SpentCells; got != 12 {
		t.Fatalf("SpentCells = %d, want 12", got)
	}
}
