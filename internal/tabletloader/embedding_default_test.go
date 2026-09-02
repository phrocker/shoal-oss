// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package tabletloader

import (
	"testing"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
)

// TestValidateResolvedFilesAbsentEmbeddingIsUnknown pins issue #274 on
// the read path: a resolver that supplied no embedding state has told
// the loader nothing, and nothing is unknown, not no_embeddings.
func TestValidateResolvedFilesAbsentEmbeddingIsUnknown(t *testing.T) {
	files := []DataFile{{
		Path: "hdfs://nn/tables/2k/A.rf", Size: 10, NumEntries: 1,
		RawQualifier: []byte("A"),
	}}
	if err := validateResolvedFiles(files); err != nil {
		t.Fatal(err)
	}
	if got := files[0].Embedding; got != embeddingspace.Unknown() {
		t.Fatalf("absent embedding = %+v, want unknown", got)
	}
}

// TestValidateResolvedFilesKeepsDeclaredStates is the other half: an
// explicitly declared state, including no_embeddings, is left alone.
func TestValidateResolvedFilesKeepsDeclaredStates(t *testing.T) {
	files := []DataFile{
		{
			Path: "hdfs://nn/tables/2k/A.rf", Size: 10, NumEntries: 1,
			RawQualifier: []byte("A"), Embedding: embeddingspace.NoEmbeddings(),
		},
		{
			Path: "hdfs://nn/tables/2k/B.rf", Size: 10, NumEntries: 1,
			RawQualifier: []byte("B"), Embedding: embeddingspace.Has("space-a"),
		},
	}
	if err := validateResolvedFiles(files); err != nil {
		t.Fatal(err)
	}
	if got := files[0].Embedding; got != embeddingspace.NoEmbeddings() {
		t.Fatalf("file A = %+v, want no embeddings", got)
	}
	if got := files[1].Embedding; got != embeddingspace.Has("space-a") {
		t.Fatalf("file B = %+v, want has space-a", got)
	}
}
