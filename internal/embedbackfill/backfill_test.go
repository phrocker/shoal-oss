// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package embedbackfill

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/metadata"
	"github.com/phrocker/shoal-oss/internal/metadatacas"
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
			// Mirrors the CAS precondition: an established column is
			// never replaced.
			return false, nil
		}
		f.files[i].Metadata = state
		f.files[i].ExistingColumn = nil
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
	if summary.Unresolved[0].Entry != "b" || summary.Unresolved[0].Reason != ReasonUnestablishedFooter {
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
	if len(summary.Raced) != 1 || summary.Resolved != 0 || summary.Complete() {
		t.Fatalf("summary = %+v", summary)
	}
	// A run reported as incomplete has to say which files, or the CLI's
	// non-zero exit points an operator at an empty list.
	if summary.Raced[0].Entry != "a" {
		t.Fatalf("raced = %+v", summary.Raced[0])
	}
	if !strings.Contains(Report(summary), "a (/t/a.rf)") {
		t.Fatalf("report does not name the raced file:\n%s", Report(summary))
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

// TestMetadataFilesDistinguishesAbsentFromExplicitUnknown: both decode
// to the same FileState, but a conditional write has to tell them apart
// — "must not exist" and "must still hold these bytes" are different
// preconditions, and getting it wrong makes an explicit-unknown column
// unrepairable.
func TestMetadataFilesDistinguishesAbsentFromExplicitUnknown(t *testing.T) {
	row := "5;m"
	absent := `{"path":"hdfs://nn/tables/5/A.rf","startRow":"","endRow":""}`
	explicit := `{"path":"hdfs://nn/tables/5/B.rf","startRow":"","endRow":""}`
	encoded, err := embeddingspace.Encode(embeddingspace.Unknown())
	if err != nil {
		t.Fatal(err)
	}
	tablets, err := metadata.AggregateRows([]*data.TKeyValue{
		metadataKV(row, metadata.CFFile, absent, "100,10"),
		metadataKV(row, metadata.CFFile, explicit, "200,20"),
		metadataKV(row, metadata.CFFileEmbedding, explicit, string(encoded)),
		metadataKV(row, metadata.CFTabletSection, metadata.CQPrevRow, "\x00"),
	})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := MetadataFiles{Reader: fakeTablets(tablets), TableID: "5"}.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	byEntry := map[string]File{}
	for _, file := range listed {
		byEntry[file.Entry] = file
	}
	if got := byEntry[absent]; got.Metadata != embeddingspace.Unknown() || len(got.ExistingColumn) != 0 {
		t.Fatalf("absent column = %+v, want unknown with no stored bytes", got)
	}
	got := byEntry[explicit]
	if got.Metadata != embeddingspace.Unknown() {
		t.Fatalf("explicit column = %+v", got.Metadata)
	}
	if string(got.ExistingColumn) != string(encoded) {
		t.Fatalf("ExistingColumn = %q, want the stored bytes", got.ExistingColumn)
	}
}

// TestMetadataFilesRejectsTargetsItCannotBackfill: a dry run has to
// validate the same target an apply would, or an operator gets a clean
// report for a migration that could never be performed. A typo'd table
// id locates nothing, and reporting that as a completed zero-file
// migration is the same failure in a different shape.
func TestMetadataFilesRejectsTargetsItCannotBackfill(t *testing.T) {
	root := metadata.TabletInfo{TableID: metadata.RootTableID}
	if _, err := (MetadataFiles{
		Reader: fakeTablets([]metadata.TabletInfo{root}), TableID: metadata.RootTableID,
	}).List(context.Background()); !errors.Is(err, metadatacas.ErrRootBackfillUnsupported) {
		t.Fatalf("root table error = %v, want ErrRootBackfillUnsupported", err)
	}
	if _, err := (MetadataFiles{
		Reader: fakeTablets(nil), TableID: "nosuchtable",
	}).List(context.Background()); err == nil {
		t.Fatal("a table that locates no tablets must not report a completed migration")
	}
}

// TestRunRepairsAnExplicitUnknownColumn: an explicit unknown column is
// exactly as uninformative as an absent one, and the compaction refusal
// tells operators a backfill repairs it. It has to actually do so.
func TestRunRepairsAnExplicitUnknownColumn(t *testing.T) {
	encoded, err := embeddingspace.Encode(embeddingspace.Unknown())
	if err != nil {
		t.Fatal(err)
	}
	file := unknownFile("a", "/t/a.rf")
	file.ExistingColumn = encoded
	files := &fakeMetadata{files: []File{file}}
	footers := &fakeFooters{states: map[string]embeddingspace.FileState{
		"/t/a.rf": embeddingspace.Has("space-a"),
	}}
	summary, err := Run(context.Background(), Config{Files: files, Footers: footers, Columns: files})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Resolved != 1 || len(summary.Raced) != 0 || !summary.Complete() {
		t.Fatalf("summary = %+v", summary)
	}
	if files.files[0].Metadata != embeddingspace.Has("space-a") {
		t.Fatalf("column = %+v, want the footer's state", files.files[0].Metadata)
	}
}

// TestCASColumnsForwardsEveryPrecondition: the writer's safety rests
// entirely on the target it is handed, so a dropped field is a silently
// unconditioned write. In particular the existing column bytes decide
// whether the write means "must be absent" or "must still be this
// unknown", and an explicit-unknown column is unrepairable without them.
func TestCASColumnsForwardsEveryPrecondition(t *testing.T) {
	encoded, err := embeddingspace.Encode(embeddingspace.Unknown())
	if err != nil {
		t.Fatal(err)
	}
	file := File{
		TableID: "5", PrevEndRow: []byte("a"), EndRow: []byte("z"),
		Entry: "a", Path: "/t/a.rf",
		Qualifier: []byte("qualifier"), Value: []byte("100,10,-1"),
		Metadata: embeddingspace.Unknown(), ExistingColumn: encoded,
	}
	recorder := &recordingWriter{}
	applied, err := CASColumns{Writer: recorder}.Write(
		context.Background(), file, embeddingspace.Has("space-a"))
	if err != nil || !applied {
		t.Fatalf("applied = %t, err = %v", applied, err)
	}
	want := metadatacas.BackfillTarget{
		TableID: "5", PrevEndRow: []byte("a"), EndRow: []byte("z"),
		FileQualifier: []byte("qualifier"), FileValue: []byte("100,10,-1"),
		ExistingEmbedding: encoded,
	}
	if !reflect.DeepEqual(recorder.target, want) {
		t.Fatalf("target = %+v, want %+v", recorder.target, want)
	}
	if recorder.state != embeddingspace.Has("space-a") {
		t.Fatalf("state = %+v", recorder.state)
	}
}

type recordingWriter struct {
	target metadatacas.BackfillTarget
	state  embeddingspace.FileState
}

func (r *recordingWriter) WriteFileEmbedding(
	_ context.Context, target metadatacas.BackfillTarget, state embeddingspace.FileState,
) (bool, error) {
	r.target = target
	r.state = state
	return true, nil
}

func metadataKV(row, cf, cq, value string) *data.TKeyValue {
	return &data.TKeyValue{
		Key: &data.TKey{
			Row: []byte(row), ColFamily: []byte(cf), ColQualifier: []byte(cq),
		},
		Value: []byte(value),
	}
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
