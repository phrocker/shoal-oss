// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package embedbackfill

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/metadata"
	"github.com/phrocker/shoal-oss/internal/rfile"
	"github.com/phrocker/shoal-oss/internal/rfile/wire"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
	"github.com/phrocker/shoal-oss/internal/thrift/gen/data"
)

// fakeMetadata is a tiny stand-in for the metadata table: a list of file
// entries whose embedding columns the writer mutates in place, so a
// second Run really does observe what the first one wrote.
type fakeMetadata struct {
	files    []File
	writes   int
	rejectOn map[string]bool
	failOn   map[string]error
}

func (f *fakeMetadata) List(context.Context) ([]File, error) {
	out := make([]File, len(f.files))
	copy(out, f.files)
	return out, nil
}

func (f *fakeMetadata) Write(
	_ context.Context, file File, state embeddingspace.FileState,
) (bool, error) {
	if err := f.failOn[file.Entry]; err != nil {
		return false, err
	}
	if f.rejectOn[file.Entry] {
		return false, nil
	}
	for i := range f.files {
		if f.files[i].Entry != file.Entry {
			continue
		}
		if f.files[i].Metadata.Known() {
			// Mirrors the CAS precondition: the column must be absent.
			return false, nil
		}
		f.files[i].Metadata = state
		f.writes++
		return true, nil
	}
	return false, nil
}

type fakeFooters struct {
	states map[string]embeddingspace.FileState
	errs   map[string]error
	reads  int
}

func (f *fakeFooters) FooterState(
	_ context.Context, path string,
) (embeddingspace.FileState, error) {
	f.reads++
	if err := f.errs[path]; err != nil {
		return embeddingspace.FileState{}, err
	}
	state, ok := f.states[path]
	if !ok {
		// No meta block. This is exactly the case the backfill must
		// refuse to guess at.
		return embeddingspace.Unknown(), nil
	}
	return state, nil
}

func unknownFile(entry, path string) File {
	return File{
		TableID: "5", EndRow: []byte("z"), Entry: entry, Path: path,
		Qualifier: []byte(entry), Value: []byte("100,10"),
		Metadata: embeddingspace.Unknown(),
	}
}

// TestRunResolvesFromTheFooter is the core claim: the footer is the
// authority, and a resolvable file gets exactly what the footer said.
func TestRunResolvesFromTheFooter(t *testing.T) {
	files := &fakeMetadata{files: []File{
		unknownFile("a", "/t/a.rf"),
		unknownFile("b", "/t/b.rf"),
	}}
	footers := &fakeFooters{states: map[string]embeddingspace.FileState{
		"/t/a.rf": embeddingspace.Has("space-a"),
		"/t/b.rf": embeddingspace.NoEmbeddings(),
	}}
	summary, err := Run(context.Background(), Config{Files: files, Footers: footers, Columns: files})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Resolved != 2 || summary.AlreadyLabelled != 0 || len(summary.Unresolved) != 0 {
		t.Fatalf("summary = %+v", summary)
	}
	if files.files[0].Metadata != embeddingspace.Has("space-a") {
		t.Fatalf("file a = %+v", files.files[0].Metadata)
	}
	if files.files[1].Metadata != embeddingspace.NoEmbeddings() {
		t.Fatalf("file b = %+v", files.files[1].Metadata)
	}
}

// TestRunIsIdempotent: a second pass over an already-backfilled table
// writes nothing and reports every file as already labelled. That is
// what makes the migration safe to re-run and safe to interrupt.
func TestRunIsIdempotent(t *testing.T) {
	files := &fakeMetadata{files: []File{
		unknownFile("a", "/t/a.rf"),
		unknownFile("b", "/t/b.rf"),
	}}
	footers := &fakeFooters{states: map[string]embeddingspace.FileState{
		"/t/a.rf": embeddingspace.Has("space-a"),
		"/t/b.rf": embeddingspace.NoEmbeddings(),
	}}
	cfg := Config{Files: files, Footers: footers, Columns: files}
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	writesAfterFirst := files.writes
	readsAfterFirst := footers.reads

	second, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if second.AlreadyLabelled != 2 || second.Resolved != 0 || len(second.Unresolved) != 0 {
		t.Fatalf("second run = %+v", second)
	}
	if files.writes != writesAfterFirst {
		t.Fatalf("second run wrote %d extra columns", files.writes-writesAfterFirst)
	}
	if footers.reads != readsAfterFirst {
		t.Fatalf("second run re-read %d footers; labelled files must not be reopened",
			footers.reads-readsAfterFirst)
	}
}

// TestRunLeavesFooterlessFilesUnresolved pins the deliberate refusal to
// guess. A file with no footer cannot be established from metadata, so
// it stays unknown and is reported by name with a reason.
func TestRunLeavesFooterlessFilesUnresolved(t *testing.T) {
	files := &fakeMetadata{files: []File{
		unknownFile("a", "/t/a.rf"),
		unknownFile("b", "/t/b.rf"),
	}}
	footers := &fakeFooters{states: map[string]embeddingspace.FileState{
		"/t/a.rf": embeddingspace.Has("space-a"),
	}}
	summary, err := Run(context.Background(), Config{Files: files, Footers: footers, Columns: files})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Resolved != 1 || len(summary.Unresolved) != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	if summary.Unresolved[0].Entry != "b" || summary.Unresolved[0].Reason != ReasonNoFooter {
		t.Fatalf("unresolved = %+v", summary.Unresolved[0])
	}
	if files.files[1].Metadata != embeddingspace.Unknown() {
		t.Fatalf("footerless file was relabelled to %+v", files.files[1].Metadata)
	}
	if summary.Complete() {
		t.Fatal("a run with outstanding files must not report complete")
	}
	if !strings.Contains(Report(summary), "b (/t/b.rf)") {
		t.Fatalf("report does not name the outstanding file:\n%s", Report(summary))
	}
}

// TestRunNeverRewritesADeclaredColumn: a file that already asserts
// no_embeddings is left exactly as it is, even when the footer would say
// something else. The backfill fills gaps; it does not arbitrate
// disagreements, which is what the integrity check is for.
func TestRunNeverRewritesADeclaredColumn(t *testing.T) {
	declared := unknownFile("a", "/t/a.rf")
	declared.Metadata = embeddingspace.NoEmbeddings()
	files := &fakeMetadata{files: []File{declared}}
	footers := &fakeFooters{states: map[string]embeddingspace.FileState{
		"/t/a.rf": embeddingspace.Has("space-a"),
	}}
	summary, err := Run(context.Background(), Config{Files: files, Footers: footers, Columns: files})
	if err != nil {
		t.Fatal(err)
	}
	if summary.AlreadyLabelled != 1 || summary.Resolved != 0 || files.writes != 0 {
		t.Fatalf("summary = %+v writes = %d", summary, files.writes)
	}
	if files.files[0].Metadata != embeddingspace.NoEmbeddings() {
		t.Fatalf("declared column was overwritten with %+v", files.files[0].Metadata)
	}
}

// TestRunReportsRacesSeparately: a rejected conditional write means the
// entry changed underneath the run. That is retryable and must not be
// counted as resolved.
func TestRunReportsRacesSeparately(t *testing.T) {
	files := &fakeMetadata{
		files:    []File{unknownFile("a", "/t/a.rf")},
		rejectOn: map[string]bool{"a": true},
	}
	footers := &fakeFooters{states: map[string]embeddingspace.FileState{
		"/t/a.rf": embeddingspace.Has("space-a"),
	}}
	summary, err := Run(context.Background(), Config{Files: files, Footers: footers, Columns: files})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Raced != 1 || summary.Resolved != 0 || summary.Complete() {
		t.Fatalf("summary = %+v", summary)
	}
}

// TestRunContinuesPastAFailure: one unreadable file must not strand a
// table's migration, and the operator needs the whole outstanding list.
func TestRunContinuesPastAFailure(t *testing.T) {
	files := &fakeMetadata{files: []File{
		unknownFile("a", "/t/a.rf"),
		unknownFile("b", "/t/b.rf"),
		unknownFile("c", "/t/c.rf"),
	}}
	footers := &fakeFooters{
		states: map[string]embeddingspace.FileState{
			"/t/a.rf": embeddingspace.Has("space-a"),
			"/t/c.rf": embeddingspace.Has("space-a"),
		},
		errs: map[string]error{"/t/b.rf": errors.New("object store timeout")},
	}
	summary, err := Run(context.Background(), Config{Files: files, Footers: footers, Columns: files})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Resolved != 2 || len(summary.Unresolved) != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	if !strings.Contains(summary.Unresolved[0].Reason, "object store timeout") {
		t.Fatalf("reason = %q", summary.Unresolved[0].Reason)
	}
}

// TestRunDryRunWritesNothing: the default CLI mode must be observably
// read-only.
func TestRunDryRunWritesNothing(t *testing.T) {
	files := &fakeMetadata{files: []File{unknownFile("a", "/t/a.rf")}}
	footers := &fakeFooters{states: map[string]embeddingspace.FileState{
		"/t/a.rf": embeddingspace.Has("space-a"),
	}}
	summary, err := Run(context.Background(), Config{Files: files, Footers: footers, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Resolved != 1 || files.writes != 0 {
		t.Fatalf("summary = %+v writes = %d", summary, files.writes)
	}
	if files.files[0].Metadata != embeddingspace.Unknown() {
		t.Fatal("dry run mutated metadata")
	}
	if !strings.Contains(Report(summary), "dry-run") {
		t.Fatalf("report hides the mode:\n%s", Report(summary))
	}
}

func TestRunRequiresAWriterWhenApplying(t *testing.T) {
	files := &fakeMetadata{}
	if _, err := Run(context.Background(), Config{Files: files, Footers: &fakeFooters{}}); err == nil {
		t.Fatal("apply mode without a column writer must be refused")
	}
}

// TestStorageFootersReadsRealFiles exercises the production footer
// reader against real RFile images: one carrying a meta block, one
// written before the block existed.
func TestStorageFootersReadsRealFiles(t *testing.T) {
	backend := memory.New()
	backend.Put("/t/labelled.rf", buildRFile(t, embeddingspace.Has("space-a")))
	backend.Put("/t/legacy.rf", buildRFile(t, embeddingspace.FileState{}))

	footers := StorageFooters{Backend: backend}
	got, err := footers.FooterState(context.Background(), "/t/labelled.rf")
	if err != nil {
		t.Fatal(err)
	}
	if got != embeddingspace.Has("space-a") {
		t.Fatalf("labelled footer = %+v", got)
	}
	got, err = footers.FooterState(context.Background(), "/t/legacy.rf")
	if err != nil {
		t.Fatal(err)
	}
	if got != embeddingspace.Unknown() {
		t.Fatalf("legacy footer = %+v, want unknown so the file is reported, not guessed at", got)
	}
}

// TestBackfillOverRealFilesLeavesLegacyFilesOutstanding is the end-to-end
// shape of a migration: the labelled file resolves, the footerless one is
// named for the operator.
func TestBackfillOverRealFilesLeavesLegacyFilesOutstanding(t *testing.T) {
	backend := memory.New()
	backend.Put("/t/labelled.rf", buildRFile(t, embeddingspace.NoEmbeddings()))
	backend.Put("/t/legacy.rf", buildRFile(t, embeddingspace.FileState{}))

	files := &fakeMetadata{files: []File{
		unknownFile("labelled", "/t/labelled.rf"),
		unknownFile("legacy", "/t/legacy.rf"),
	}}
	summary, err := Run(context.Background(), Config{
		Files: files, Footers: StorageFooters{Backend: backend}, Columns: files,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Resolved != 1 || len(summary.Unresolved) != 1 ||
		summary.Unresolved[0].Entry != "legacy" {
		t.Fatalf("summary = %+v", summary)
	}
	if files.files[0].Metadata != embeddingspace.NoEmbeddings() {
		t.Fatalf("labelled file = %+v", files.files[0].Metadata)
	}
	if files.files[1].Metadata != embeddingspace.Unknown() {
		t.Fatalf("legacy file = %+v", files.files[1].Metadata)
	}
}

// TestMetadataFilesCarriesTheRawEntryBytes: the CAS precondition is a
// byte comparison, so the enumerator must hand over the stored
// DataFileValue rather than a re-encoding of the parsed fields.
func TestMetadataFilesCarriesTheRawEntryBytes(t *testing.T) {
	row := "5;m"
	entry := `{"path":"hdfs://nn/tables/5/A.rf","startRow":"","endRow":""}`
	tablets, err := metadata.AggregateRows([]*data.TKeyValue{
		{
			Key: &data.TKey{
				Row: []byte(row), ColFamily: []byte(metadata.CFFile),
				ColQualifier: []byte(entry),
			},
			// The trailing ",-1" round-trips to "100,10" if re-encoded
			// from the decoded fields, which would break the CAS.
			Value: []byte("100,10,-1"),
		},
		{
			Key: &data.TKey{
				Row: []byte(row), ColFamily: []byte(metadata.CFTabletSection),
				ColQualifier: []byte(metadata.CQPrevRow),
			},
			Value: []byte("\x00"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := MetadataFiles{Reader: fakeTablets(tablets), TableID: "5"}.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 {
		t.Fatalf("listed = %+v", listed)
	}
	if string(listed[0].Value) != "100,10,-1" {
		t.Fatalf("Value = %q, want the stored bytes", listed[0].Value)
	}
	if listed[0].Metadata != embeddingspace.Unknown() {
		t.Fatalf("Metadata = %+v, want unknown", listed[0].Metadata)
	}
	if listed[0].Entry != entry {
		t.Fatalf("Entry = %q", listed[0].Entry)
	}
}

type fakeTablets []metadata.TabletInfo

func (f fakeTablets) LocateTable(context.Context, string) ([]metadata.TabletInfo, error) {
	return []metadata.TabletInfo(f), nil
}

func buildRFile(t *testing.T, state embeddingspace.FileState) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := rfile.NewWriter(&buf, rfile.WriterOptions{EmbeddingSpace: state})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(
		&wire.Key{Row: []byte("r"), ColumnFamily: []byte("cf"), Timestamp: 1}, []byte("v"),
	); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
