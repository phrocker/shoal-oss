// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package metadatacas

import (
	"context"
	"errors"
	"testing"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/metadata"
)

const backfillEntry = `{"path":"hdfs://nn/tables/5/A.rf","startRow":"","endRow":""}`

func backfillTarget() BackfillTarget {
	return BackfillTarget{
		TableID:       "5",
		FileQualifier: []byte(backfillEntry),
		FileValue:     []byte("100,10"),
	}
}

func newBackfillFixture(t *testing.T) (*fakeCluster, *BackfillWriter) {
	t.Helper()
	cluster := newFakeCluster()
	cluster.cells[cell(metadata.CFFile, backfillEntry)] = []byte("100,10")
	writer, err := NewBackfillWriter(cluster, cluster, cluster)
	if err != nil {
		t.Fatal(err)
	}
	return cluster, writer
}

// TestBackfillWriterWritesTheColumnOnce also pins idempotence at the CAS
// layer: the second write is rejected because the column is no longer
// absent, so a re-run cannot overwrite an established label.
func TestBackfillWriterWritesTheColumnOnce(t *testing.T) {
	cluster, writer := newBackfillFixture(t)
	applied, err := writer.WriteFileEmbedding(
		context.Background(), backfillTarget(), embeddingspace.Has("space-a"))
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("first write was not applied")
	}
	stored := cluster.cells[cell(metadata.CFFileEmbedding, backfillEntry)]
	state, err := embeddingspace.Decode(stored)
	if err != nil {
		t.Fatal(err)
	}
	if state != embeddingspace.Has("space-a") {
		t.Fatalf("stored column = %+v", state)
	}

	applied, err = writer.WriteFileEmbedding(
		context.Background(), backfillTarget(), embeddingspace.Has("space-b"))
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("second write must be rejected; the column is already present")
	}
	if string(cluster.cells[cell(metadata.CFFileEmbedding, backfillEntry)]) != string(stored) {
		t.Fatal("an established column was overwritten")
	}
}

// TestBackfillWriterRejectsAChangedFileEntry: a file replaced by a
// compaction mid-run is not the file whose footer was read, so the
// column must not land on it.
func TestBackfillWriterRejectsAChangedFileEntry(t *testing.T) {
	cluster, writer := newBackfillFixture(t)
	cluster.cells[cell(metadata.CFFile, backfillEntry)] = []byte("900,90")
	applied, err := writer.WriteFileEmbedding(
		context.Background(), backfillTarget(), embeddingspace.Has("space-a"))
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("write applied against a changed DataFileValue")
	}
	if _, ok := cluster.cells[cell(metadata.CFFileEmbedding, backfillEntry)]; ok {
		t.Fatal("a column was written for a file entry that had changed")
	}
}

// TestBackfillWriterRejectsAMissingFileEntry covers the orphan hazard:
// writing file.embedding for a file: entry that no longer exists would
// make metadata aggregation fail for the whole tablet.
func TestBackfillWriterRejectsAMissingFileEntry(t *testing.T) {
	cluster, writer := newBackfillFixture(t)
	delete(cluster.cells, cell(metadata.CFFile, backfillEntry))
	applied, err := writer.WriteFileEmbedding(
		context.Background(), backfillTarget(), embeddingspace.Has("space-a"))
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("write applied against a deleted file entry")
	}
	if _, ok := cluster.cells[cell(metadata.CFFileEmbedding, backfillEntry)]; ok {
		t.Fatal("an orphan file.embedding column was written")
	}
}

// TestBackfillWriterRefusesToRecordUnknown: the backfill exists to
// replace absence with a positive claim taken from a footer. Recording
// unknown would write a non-claim no footer produced.
func TestBackfillWriterRefusesToRecordUnknown(t *testing.T) {
	cluster, writer := newBackfillFixture(t)
	for _, state := range []embeddingspace.FileState{
		embeddingspace.Unknown(),
		{},
	} {
		if _, err := writer.WriteFileEmbedding(context.Background(), backfillTarget(), state); err == nil {
			t.Fatalf("WriteFileEmbedding(%+v) was accepted", state)
		}
	}
	if _, ok := cluster.cells[cell(metadata.CFFileEmbedding, backfillEntry)]; ok {
		t.Fatal("a column was written for a state with no basis")
	}
}

// TestBackfillWriterRefusesTheRootTablet: the root tablet's metadata
// lives in ZooKeeper and is owned by whichever server hosts it.
func TestBackfillWriterRefusesTheRootTablet(t *testing.T) {
	_, writer := newBackfillFixture(t)
	target := backfillTarget()
	target.TableID = metadata.RootTableID
	_, err := writer.WriteFileEmbedding(
		context.Background(), target, embeddingspace.NoEmbeddings())
	if !errors.Is(err, ErrRootBackfillUnsupported) {
		t.Fatalf("error = %v, want ErrRootBackfillUnsupported", err)
	}
}

// TestBackfillWriterReportsAnAmbiguousOutcome: an unknown conditional
// outcome may or may not have landed, so it must surface rather than be
// reported as a clean skip.
func TestBackfillWriterReportsAnAmbiguousOutcome(t *testing.T) {
	cluster, writer := newBackfillFixture(t)
	cluster.ambiguous = true
	applied, err := writer.WriteFileEmbedding(
		context.Background(), backfillTarget(), embeddingspace.Has("space-a"))
	if applied {
		t.Fatal("an ambiguous outcome must not be reported as applied")
	}
	if err == nil {
		t.Fatal("an ambiguous outcome must surface as an error")
	}
}

// TestUndeclaredCommitEncodesUnknown covers the other write path this
// package owns. encodeFileEmbedding turns a commit into the durable
// file.embedding column, so a commit that declared nothing must be
// persisted as unknown rather than as the no_embeddings claim that
// merges with every space.
func TestUndeclaredCommitEncodesUnknown(t *testing.T) {
	encoded, err := encodeFileEmbedding(embeddingspace.FileState{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := embeddingspace.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if state != embeddingspace.Unknown() {
		t.Fatalf("undeclared commit encoded as %+v, want unknown", state)
	}

	for _, declared := range []embeddingspace.FileState{
		embeddingspace.NoEmbeddings(),
		embeddingspace.Has("space-a"),
		embeddingspace.Unknown(),
	} {
		encoded, err := encodeFileEmbedding(declared)
		if err != nil {
			t.Fatal(err)
		}
		state, err := embeddingspace.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if state != declared {
			t.Fatalf("declared %+v encoded as %+v", declared, state)
		}
	}
}
