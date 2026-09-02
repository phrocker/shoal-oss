// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package mincauthority

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/rfile"
	"github.com/phrocker/shoal-oss/internal/rfile/bcfile"
	"github.com/phrocker/shoal-oss/internal/rfile/bcfile/block"
)

// footerEmbedding reads back the space the coordinator actually stamped
// into the published RFile. Asserting on the returned DataFile alone
// would miss a divergence between the metadata claim and the durable
// evidence in the file itself.
func footerEmbedding(t *testing.T, f *fixture, path string) embeddingspace.FileState {
	t.Helper()
	raw, err := f.outputs.Read(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := bcfile.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	state, err := rfile.ReadEmbeddingSpaceMetadata(bc, block.Default())
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// TestRunUndeclaredSnapshotEmbeddingIsUnknown pins issue #274 on the
// worst site: a snapshot provider that declares nothing must not have
// "this file holds no vectors" invented for it and written into the
// RFile footer and the metadata column.
func TestRunUndeclaredSnapshotEmbeddingIsUnknown(t *testing.T) {
	f := newFixture(t)
	file, err := f.coordinator.Run(context.Background(), "op-1")
	if err != nil {
		t.Fatal(err)
	}
	if file.Embedding != embeddingspace.Unknown() {
		t.Fatalf("committed embedding = %+v, want unknown", file.Embedding)
	}
	if got := footerEmbedding(t, f, file.Path); got != embeddingspace.Unknown() {
		t.Fatalf("footer embedding = %+v, want unknown", got)
	}
}

// TestRunUsesConfiguredDefaultEmbedding covers the operator escape
// hatch: someone who knows their ingest pipeline emits no vectors may
// declare that, and the assertion is then recorded because they made it.
func TestRunUsesConfiguredDefaultEmbedding(t *testing.T) {
	f := newFixture(t)
	cfg := f.config
	cfg.DefaultEmbedding = embeddingspace.NoEmbeddings()
	coordinator, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	file, err := coordinator.Run(context.Background(), "op-1")
	if err != nil {
		t.Fatal(err)
	}
	if file.Embedding != embeddingspace.NoEmbeddings() {
		t.Fatalf("committed embedding = %+v, want no embeddings", file.Embedding)
	}
	if got := footerEmbedding(t, f, file.Path); got != embeddingspace.NoEmbeddings() {
		t.Fatalf("footer embedding = %+v, want no embeddings", got)
	}
}

// TestRunPrefersDeclaredSnapshotEmbedding: a provider that does declare
// a state wins over the configured default, which is only a fallback for
// silence.
func TestRunPrefersDeclaredSnapshotEmbedding(t *testing.T) {
	f := newFixture(t)
	f.snapshots.snapshot.Embedding = embeddingspace.Has("space-a")
	cfg := f.config
	cfg.DefaultEmbedding = embeddingspace.NoEmbeddings()
	coordinator, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	file, err := coordinator.Run(context.Background(), "op-1")
	if err != nil {
		t.Fatal(err)
	}
	if file.Embedding != embeddingspace.Has("space-a") {
		t.Fatalf("committed embedding = %+v, want has space-a", file.Embedding)
	}
	if got := footerEmbedding(t, f, file.Path); got != embeddingspace.Has("space-a") {
		t.Fatalf("footer embedding = %+v, want has space-a", got)
	}
}

// TestRunKeepsFooterAndMetadataAgreeing: the footer and the metadata
// column are cross-checked by VerifyMetadataMatchesFooter on the next
// compaction, so a configured WriterOptions.EmbeddingSpace must never
// end up saying something different from the state the coordinator
// records. A declared snapshot wins over both; with no declaration the
// writer option is read as the operator's default.
func TestRunKeepsFooterAndMetadataAgreeing(t *testing.T) {
	t.Run("declared snapshot overrides the writer option", func(t *testing.T) {
		f := newFixture(t)
		f.snapshots.snapshot.Embedding = embeddingspace.Has("space-a")
		cfg := f.config
		cfg.WriterOptions.EmbeddingSpace = embeddingspace.Has("space-b")
		coordinator, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		file, err := coordinator.Run(context.Background(), "op-1")
		if err != nil {
			t.Fatal(err)
		}
		if file.Embedding != embeddingspace.Has("space-a") {
			t.Fatalf("committed embedding = %+v", file.Embedding)
		}
		if got := footerEmbedding(t, f, file.Path); got != file.Embedding {
			t.Fatalf("footer %+v disagrees with metadata %+v", got, file.Embedding)
		}
	})

	t.Run("writer option is the default for an undeclared snapshot", func(t *testing.T) {
		f := newFixture(t)
		cfg := f.config
		cfg.WriterOptions.EmbeddingSpace = embeddingspace.Has("space-b")
		coordinator, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		file, err := coordinator.Run(context.Background(), "op-1")
		if err != nil {
			t.Fatal(err)
		}
		if file.Embedding != embeddingspace.Has("space-b") {
			t.Fatalf("committed embedding = %+v", file.Embedding)
		}
		if got := footerEmbedding(t, f, file.Path); got != file.Embedding {
			t.Fatalf("footer %+v disagrees with metadata %+v", got, file.Embedding)
		}
	})

	t.Run("DefaultEmbedding wins over the writer option", func(t *testing.T) {
		f := newFixture(t)
		cfg := f.config
		cfg.WriterOptions.EmbeddingSpace = embeddingspace.Has("space-b")
		cfg.DefaultEmbedding = embeddingspace.NoEmbeddings()
		coordinator, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		file, err := coordinator.Run(context.Background(), "op-1")
		if err != nil {
			t.Fatal(err)
		}
		if file.Embedding != embeddingspace.NoEmbeddings() {
			t.Fatalf("committed embedding = %+v", file.Embedding)
		}
		if got := footerEmbedding(t, f, file.Path); got != file.Embedding {
			t.Fatalf("footer %+v disagrees with metadata %+v", got, file.Embedding)
		}
	})
}

// TestNewRejectsInvalidDefaultEmbedding: the default is written into
// durable metadata, so an unencodable one must be refused at
// construction rather than at the first flush.
// TestNewRejectsPartialDefaultEmbedding: an identity with no state is
// not "unset", it is malformed. Testing only State != "" would let it
// through as if nothing had been configured, silently discarding an
// operator's intent instead of telling them their config is wrong.
func TestNewRejectsPartialDefaultEmbedding(t *testing.T) {
	f := newFixture(t)
	cfg := f.config
	cfg.DefaultEmbedding = embeddingspace.FileState{Identity: "space-a"}
	if _, err := New(cfg); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New err = %v, want ErrInvalidConfig", err)
	}
}

// TestRunRejectsPartialSnapshotEmbedding: the same distinction on the
// provider side. Only the exact zero value means "declared nothing"; a
// partial state must reach validateSnapshot and be refused, not be
// quietly overwritten by the default and published as durable evidence.
func TestRunRejectsPartialSnapshotEmbedding(t *testing.T) {
	f := newFixture(t)
	f.snapshots.snapshot.Embedding = embeddingspace.FileState{Identity: "space-a"}
	cfg := f.config
	cfg.DefaultEmbedding = embeddingspace.NoEmbeddings()
	coordinator, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Run(context.Background(), "op-1"); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("Run err = %v, want ErrInvalidSnapshot", err)
	}
}

func TestNewRejectsInvalidDefaultEmbedding(t *testing.T) {
	f := newFixture(t)
	for _, invalid := range []embeddingspace.FileState{
		{State: "definitely-not-a-state"},
		{State: embeddingspace.StateHasEmbeddings},
		{State: embeddingspace.StateNoEmbeddings, Identity: "space-a"},
	} {
		cfg := f.config
		cfg.DefaultEmbedding = invalid
		if _, err := New(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("New(%+v) error = %v, want ErrInvalidConfig", invalid, err)
		}
	}
}
