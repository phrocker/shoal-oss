// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package compactjob

import (
	"context"
	"testing"

	"github.com/phrocker/shoal-oss/internal/compaction"
	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/iterrt"
)

func inputByEntry(t *testing.T, plan *Plan, entry string) InputFile {
	t.Helper()
	for _, in := range plan.Inputs {
		if in.Entry == entry {
			return in
		}
	}
	t.Fatalf("plan has no input for entry %q", entry)
	return InputFile{}
}

func TestTranslateCarriesThePerFileEmbeddingColumn(t *testing.T) {
	t.Parallel()

	job := validJob()
	first := job.Files[0].MetadataFileEntry
	second := job.Files[1].MetadataFileEntry
	states, err := embeddingspace.EncodeFileStates(map[string]embeddingspace.FileState{
		first:  embeddingspace.Has("model-b"),
		second: embeddingspace.NoEmbeddings(),
	})
	if err != nil {
		t.Fatalf("EncodeFileStates: %v", err)
	}
	job.Overrides[embeddingspace.JobFileStatesProperty] = states
	job.Overrides[embeddingspace.TableTargetProperty] = "model-a"

	plan := mustTranslate(t, job, Options{})
	if got := inputByEntry(t, plan, first).Embedding; got != embeddingspace.Has("model-b") {
		t.Fatalf("first input embedding = %+v, want has_embeddings model-b", got)
	}
	if got := inputByEntry(t, plan, second).Embedding; got != embeddingspace.NoEmbeddings() {
		t.Fatalf("second input embedding = %+v, want no_embeddings", got)
	}
	if plan.TargetEmbeddingSpace != "model-a" {
		t.Fatalf("TargetEmbeddingSpace = %q, want %q", plan.TargetEmbeddingSpace, "model-a")
	}
}

func TestTranslateLeavesUnmentionedInputsUnknown(t *testing.T) {
	t.Parallel()

	job := validJob()
	first := job.Files[0].MetadataFileEntry
	second := job.Files[1].MetadataFileEntry
	states, err := embeddingspace.EncodeFileStates(map[string]embeddingspace.FileState{
		first: embeddingspace.Has("model-a"),
	})
	if err != nil {
		t.Fatalf("EncodeFileStates: %v", err)
	}
	job.Overrides[embeddingspace.JobFileStatesProperty] = states

	plan := mustTranslate(t, job, Options{})
	// Fail closed: a file the job says nothing about is unknown, not
	// "has no embeddings". Treating silence as no_embeddings would let a
	// coordinator that dropped the column silently relabel embedded
	// files.
	if got := inputByEntry(t, plan, second).Embedding; got != embeddingspace.Unknown() {
		t.Fatalf("unmentioned input embedding = %+v, want unknown", got)
	}
	if got := inputByEntry(t, plan, first).Embedding; got != embeddingspace.Has("model-a") {
		t.Fatalf("first input embedding = %+v, want has_embeddings model-a", got)
	}
}

func TestTranslateWithoutTheColumnLeavesEveryInputUnknown(t *testing.T) {
	t.Parallel()

	plan := mustTranslate(t, validJob(), Options{})
	for _, in := range plan.Inputs {
		if in.Embedding != embeddingspace.Unknown() {
			t.Fatalf("input %q embedding = %+v, want unknown", in.Entry, in.Embedding)
		}
	}
	if plan.TargetEmbeddingSpace != "" {
		t.Fatalf("TargetEmbeddingSpace = %q, want empty", plan.TargetEmbeddingSpace)
	}
}

func TestTranslateRefusesAnEmbeddingColumnNamingAForeignFile(t *testing.T) {
	t.Parallel()

	job := validJob()
	states, err := embeddingspace.EncodeFileStates(map[string]embeddingspace.FileState{
		storedFile("hdfs://nn/accumulo/tables/2/t-0001/F9999.rf"): embeddingspace.Has("model-a"),
	})
	if err != nil {
		t.Fatalf("EncodeFileStates: %v", err)
	}
	job.Overrides[embeddingspace.JobFileStatesProperty] = states
	assertRefused(t, job, Options{}, ClassMalformedJob,
		"overrides["+embeddingspace.JobFileStatesProperty+"]")
}

func TestTranslateRefusesAMalformedEmbeddingColumn(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Overrides[embeddingspace.JobFileStatesProperty] = "{not json"
	assertRefused(t, job, Options{}, ClassMalformedJob,
		"overrides["+embeddingspace.JobFileStatesProperty+"]")

	job = validJob()
	job.Overrides[embeddingspace.JobFileStatesProperty] =
		`{"` + job.Files[0].MetadataFileEntry + `":{"state":"has_embeddings"}}`
	assertRefused(t, job, Options{}, ClassMalformedJob,
		"overrides["+embeddingspace.JobFileStatesProperty+"]")
}

func TestTranslateRefusesAnInvalidEmbeddingTarget(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Overrides[embeddingspace.TableTargetProperty] = string([]byte{0xff, 0xfe})
	assertRefused(t, job, Options{}, ClassUnsupportedProperty,
		"overrides["+embeddingspace.TableTargetProperty+"]")

	job = validJob()
	assertRefused(t, job, Options{TargetEmbeddingSpace: string([]byte{0xff, 0xfe})},
		ClassUnsupportedProperty, embeddingspace.TableTargetProperty)
}

func TestTranslateOverrideBeatsTheTableProperty(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Overrides[embeddingspace.TableTargetProperty] = " model-override "
	plan := mustTranslate(t, job, Options{TargetEmbeddingSpace: "model-table"})
	if plan.TargetEmbeddingSpace != "model-override" {
		t.Fatalf("TargetEmbeddingSpace = %q, want %q", plan.TargetEmbeddingSpace, "model-override")
	}
}

func TestTableOptionsReadTheEmbeddingTargetProperty(t *testing.T) {
	t.Parallel()

	opts, err := OptionsFromTableProperties(map[string]string{
		embeddingspace.TableTargetProperty: "  model-a  ",
	}, Limits{})
	if err != nil {
		t.Fatalf("TableOptions: %v", err)
	}
	if opts.TargetEmbeddingSpace != "model-a" {
		t.Fatalf("TargetEmbeddingSpace = %q, want %q", opts.TargetEmbeddingSpace, "model-a")
	}
}

func TestPlanSpecCarriesTheTargetAndConverger(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Overrides[embeddingspace.TableTargetProperty] = "model-a"
	plan := mustTranslate(t, job, Options{})

	spec := plan.Spec(nil)
	if spec.TargetEmbeddingSpace != "model-a" {
		t.Fatalf("spec target = %q, want %q", spec.TargetEmbeddingSpace, "model-a")
	}
	if spec.Converger != nil {
		t.Fatal("Spec must leave convergence disabled")
	}

	converger := planTestConverger{}
	spec = plan.SpecWithConverger(nil, converger)
	if spec.Converger == nil {
		t.Fatal("SpecWithConverger must attach the converger")
	}
	if _, ok := spec.Converger.(planTestConverger); !ok {
		t.Fatalf("spec.Converger = %T, want planTestConverger", spec.Converger)
	}
}

var _ compaction.Converger = planTestConverger{}

// planTestConverger is a stand-in used only to prove the plan carries a
// converger through to the spec.
type planTestConverger struct{}

func (planTestConverger) Begin(context.Context, string, []embeddingspace.FileState) error {
	return nil
}

func (planTestConverger) Convert(_ context.Context, _ *iterrt.Key, value []byte) ([]byte, error) {
	return value, nil
}

func (planTestConverger) End(context.Context, bool, int64, error) {}
