// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

// Package embedbackfill resolves tablet files whose embedding-space
// state was never recorded.
//
// # Why this exists
//
// Issue #274 stopped the metadata layers fabricating no_embeddings for a
// file that carries no file.embedding column. Absence of the column now
// reads as unknown, which is the honest answer and the fail-closed one.
// It is also, for an existing cluster, a large number of files that used
// to carry a (false) definite label and now carry none.
//
// The backfill is the migration path. It walks the files, establishes
// each one's real state, and writes the explicit column so unknown
// resolves to something definite.
//
// # What counts as truth
//
// The RFile (or Parquet) footer meta block written by whoever produced
// the file — embeddingspace.RFileMetaBlockName. It travels with the
// file, it was written by the producer, and it cannot be desynchronised
// from the bytes it describes. Nothing else is consulted, and in
// particular the backfill never inspects cell values to guess whether
// something looks like a vector: a guess written into durable metadata
// is exactly the failure this whole change set exists to remove.
//
// A file whose footer is *also* absent therefore cannot be resolved from
// metadata alone. Those files are left unknown and reported
// individually, with the reason, so an operator knows precisely which
// files need attention and why. Marking them no_embeddings to make the
// report look clean would reintroduce the bug.
//
// # Safety
//
// Every write is conditional on the file entry still being present and
// unchanged, and on the embedding column still being absent. A file that
// was compacted away mid-run is reported as raced, not written. A file
// that already carries a column is left alone. Re-running the backfill
// over an already-backfilled table therefore writes nothing and reports
// every file as already labelled, which is what makes it safe to run
// repeatedly and safe to interrupt.
package embedbackfill

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
)

// ReasonNoFooter is reported for a file that carries no embedding-space
// meta block. It is the one case the backfill deliberately refuses to
// resolve.
const ReasonNoFooter = "the file carries no embedding-space footer, so its state cannot be established from metadata"

// File is one metadata file entry the backfill may have to label.
type File struct {
	// TableID and the tablet bounds identify the metadata row the
	// column is written into.
	TableID    string
	PrevEndRow []byte
	EndRow     []byte

	// Entry is the metadata file: column qualifier as a string — the
	// name an operator sees and the name a compaction job uses.
	Entry string

	// Path is the storage path the footer is read from.
	Path string

	// Qualifier is the raw file: column qualifier.
	Qualifier []byte

	// Value is the raw DataFileValue currently in metadata. It is the
	// CAS precondition: if the entry changed, the file this backfill
	// examined is not the file it would be labelling.
	Value []byte

	// Metadata is the file's current embedding-space state as the
	// metadata layer reports it. Anything Known() is left alone.
	Metadata embeddingspace.FileState
}

// Files enumerates the candidate file entries.
type Files interface {
	List(ctx context.Context) ([]File, error)
}

// Footers resolves a file's authoritative embedding-space state from the
// file itself. It must return embeddingspace.Unknown() — not an error —
// when the file simply carries no embedding-space meta block, because
// that is a normal state for a file written before the block existed.
type Footers interface {
	FooterState(ctx context.Context, path string) (embeddingspace.FileState, error)
}

// Columns writes one explicit file.embedding column.
//
// It reports applied=false, with a nil error, when the conditional write
// was rejected because the file entry changed underneath the run. That
// is not a failure: the file is simply no longer the one that was
// examined, and a later run will pick up whatever replaced it.
type Columns interface {
	Write(ctx context.Context, file File, state embeddingspace.FileState) (applied bool, err error)
}

// Config is one backfill run.
type Config struct {
	Files   Files
	Footers Footers

	// Columns may be nil only when DryRun is set.
	Columns Columns

	// DryRun resolves and reports without writing anything. It is the
	// safe way to size a migration before committing to it.
	DryRun bool
}

// Unresolved is one file the backfill could not establish a state for.
type Unresolved struct {
	Entry  string
	Path   string
	Reason string
}

// Summary is what one run did. Every file in Files.List is accounted for
// in exactly one of the counters or in Unresolved.
type Summary struct {
	// DryRun repeats the run's mode so a caller printing a Summary
	// cannot claim writes that did not happen.
	DryRun bool

	// Scanned is every file the run considered.
	Scanned int

	// AlreadyLabelled carried a definite column before the run. On a
	// second run over the same table this is every file that the first
	// run resolved, which is what idempotence looks like from here.
	AlreadyLabelled int

	// Resolved had their state established from the footer and the
	// column written (or, under DryRun, would have been).
	Resolved int

	// Raced were resolvable but their metadata entry changed before the
	// conditional write landed. Re-running picks them up.
	Raced int

	// Unresolved could not be established from metadata alone, sorted by
	// entry. These are the files that need an operator.
	Unresolved []Unresolved
}

// Complete reports whether the run left nothing outstanding.
func (s Summary) Complete() bool { return len(s.Unresolved) == 0 && s.Raced == 0 }

// Run executes one backfill pass.
//
// It does not stop at the first file it cannot handle. A single
// unreadable file must not strand a whole table's migration, and the
// operator needs the complete list of what is outstanding, not the first
// entry of it. Only a failure to enumerate the files at all — where
// continuing would silently under-report — aborts the run.
func Run(ctx context.Context, cfg Config) (Summary, error) {
	if cfg.Files == nil || cfg.Footers == nil {
		return Summary{}, fmt.Errorf("embedbackfill: Files and Footers are required")
	}
	if cfg.Columns == nil && !cfg.DryRun {
		return Summary{}, fmt.Errorf("embedbackfill: Columns is required unless DryRun is set")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	files, err := cfg.Files.List(ctx)
	if err != nil {
		return Summary{}, fmt.Errorf("embedbackfill: enumerate files: %w", err)
	}

	summary := Summary{DryRun: cfg.DryRun, Scanned: len(files)}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		if file.Metadata.Known() {
			summary.AlreadyLabelled++
			continue
		}
		state, err := cfg.Footers.FooterState(ctx, file.Path)
		if err != nil {
			summary.Unresolved = append(summary.Unresolved, Unresolved{
				Entry: file.Entry, Path: file.Path,
				Reason: "read the file footer: " + err.Error(),
			})
			continue
		}
		if !state.Known() {
			summary.Unresolved = append(summary.Unresolved, Unresolved{
				Entry: file.Entry, Path: file.Path, Reason: ReasonNoFooter,
			})
			continue
		}
		if cfg.DryRun {
			summary.Resolved++
			continue
		}
		applied, err := cfg.Columns.Write(ctx, file, state)
		if err != nil {
			summary.Unresolved = append(summary.Unresolved, Unresolved{
				Entry: file.Entry, Path: file.Path,
				Reason: "write the file.embedding column: " + err.Error(),
			})
			continue
		}
		if !applied {
			summary.Raced++
			continue
		}
		summary.Resolved++
	}
	sort.Slice(summary.Unresolved, func(i, j int) bool {
		if summary.Unresolved[i].Entry != summary.Unresolved[j].Entry {
			return summary.Unresolved[i].Entry < summary.Unresolved[j].Entry
		}
		return summary.Unresolved[i].Path < summary.Unresolved[j].Path
	})
	return summary, nil
}

// Report renders a summary for an operator, one line per counter and one
// line per outstanding file.
func Report(summary Summary) string {
	var b strings.Builder
	if summary.DryRun {
		b.WriteString("mode: dry-run (no columns written)\n")
	} else {
		b.WriteString("mode: apply\n")
	}
	fmt.Fprintf(&b, "scanned: %d\n", summary.Scanned)
	fmt.Fprintf(&b, "already labelled: %d\n", summary.AlreadyLabelled)
	fmt.Fprintf(&b, "resolved: %d\n", summary.Resolved)
	fmt.Fprintf(&b, "raced (retry): %d\n", summary.Raced)
	fmt.Fprintf(&b, "unresolvable: %d\n", len(summary.Unresolved))
	for _, item := range summary.Unresolved {
		fmt.Fprintf(&b, "- %s (%s): %s\n", item.Entry, item.Path, item.Reason)
	}
	return b.String()
}
