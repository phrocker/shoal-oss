// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package compactexec

import (
	"context"
	"sync"
	"testing"

	"github.com/phrocker/shoal-oss/internal/compaction"
	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

// capturingComposer records the spec it was handed so the test can
// assert what the executor plumbed through, without needing a real
// embedding provider.
type capturingComposer struct {
	mu   sync.Mutex
	spec compaction.Spec
}

func (c *capturingComposer) Compact(
	_ context.Context, spec compaction.Spec, _ func(compaction.Progress),
) (*compaction.Result, error) {
	c.mu.Lock()
	c.spec = spec
	c.mu.Unlock()
	return &compaction.Result{Output: []byte("compacted"), EntriesWritten: 1}, nil
}

func (c *capturingComposer) captured() compaction.Spec {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.spec
}

type noopConverger struct{}

func (noopConverger) Begin(context.Context, string, []embeddingspace.FileState) error { return nil }

func (noopConverger) Convert(_ context.Context, _ *iterrt.Key, value []byte) ([]byte, error) {
	return value, nil
}

func (noopConverger) End(context.Context, bool, int64, error) {}

func TestExecuteCarriesEmbeddingStateAndConvergerToTheComposer(t *testing.T) {
	t.Parallel()

	image := makeRFile(t, cell{"a", "a", 1}, cell{"b", "b", 1})
	backend := memory.New()
	backend.Put("in.rf", image)

	composer := &capturingComposer{}
	converger := noopConverger{}
	exec, err := NewWithComposer(BackendStore{Backend: backend}, composer, Options{
		Converger: converger,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := testPlan(map[string][]byte{"in.rf": image}, "out.rf_tmp")
	plan.Inputs[0].Embedding = embeddingspace.Has("model-b")
	plan.TargetEmbeddingSpace = "model-a"

	if _, err := exec.Execute(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	spec := composer.captured()
	if len(spec.Inputs) != 1 {
		t.Fatalf("spec has %d inputs, want 1", len(spec.Inputs))
	}
	// Without this the external compaction path loses the per-file
	// state and every externally compacted file degrades to unknown.
	if spec.Inputs[0].MetadataEmbedding != embeddingspace.Has("model-b") {
		t.Fatalf("MetadataEmbedding = %+v, want has_embeddings model-b", spec.Inputs[0].MetadataEmbedding)
	}
	if spec.TargetEmbeddingSpace != "model-a" {
		t.Fatalf("TargetEmbeddingSpace = %q, want %q", spec.TargetEmbeddingSpace, "model-a")
	}
	if spec.Converger == nil {
		t.Fatal("the executor must pass its Converger to the composer")
	}
	if _, ok := spec.Converger.(noopConverger); !ok {
		t.Fatalf("spec.Converger = %T, want noopConverger", spec.Converger)
	}
}

func TestExecuteWithoutAConvergerLeavesConvergenceDisabled(t *testing.T) {
	t.Parallel()

	image := makeRFile(t, cell{"a", "a", 1})
	backend := memory.New()
	backend.Put("in.rf", image)

	composer := &capturingComposer{}
	exec, err := NewWithComposer(BackendStore{Backend: backend}, composer, Options{})
	if err != nil {
		t.Fatal(err)
	}
	plan := testPlan(map[string][]byte{"in.rf": image}, "out.rf_tmp")
	if _, err := exec.Execute(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if composer.captured().Converger != nil {
		t.Fatal("a compactor with no embedding provider must not converge")
	}
}
