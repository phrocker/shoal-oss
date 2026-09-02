// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package metadatacas

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/ingestclient"
	"github.com/phrocker/shoal-oss/internal/metadata"
	"github.com/phrocker/shoal-oss/internal/thrift/gen/data"
)

// ErrRootBackfillUnsupported reports an attempt to backfill the root
// tablet's own file entries.
//
// The root tablet's metadata lives in ZooKeeper rather than in a
// metadata row, so it is mutated through a completely different path
// that is owned by whichever tablet server holds the root tablet. A
// backfill tool that reached in there would be writing behind that
// owner's back. The root tablet holds a handful of files describing the
// metadata table, not user data with vectors, so refusing is a real
// answer rather than a gap.
var ErrRootBackfillUnsupported = errors.New(
	"metadatacas: the root tablet's file entries cannot be backfilled from outside its owner")

// BackfillTarget names one existing metadata file entry.
type BackfillTarget struct {
	// TableID is the table whose tablet holds the entry.
	TableID string

	// PrevEndRow and EndRow bound the tablet. EndRow selects the
	// metadata row; PrevEndRow is a precondition, so a tablet that split
	// underneath the run is refused rather than written to.
	PrevEndRow []byte
	EndRow     []byte

	// FileQualifier is the exact file: column qualifier bytes.
	FileQualifier []byte

	// FileValue is the exact DataFileValue bytes currently stored. It is
	// a precondition, not a payload: if the entry has changed, the file
	// being labelled is not the file that was examined.
	FileValue []byte
}

// BackfillWriter writes explicit file.embedding columns for file entries
// that carry none.
//
// It deliberately does not claim tablet ownership. The embedding column
// is a pure annotation: it neither adds nor removes a file, and its only
// legal value is one the file's own footer already asserts. What it does
// require is that the thing being annotated is still there and still the
// same, which is what the conditions below establish:
//
//   - the tablet's ~tab:~pr is unchanged, so the row still describes the
//     tablet that was examined and has not split;
//   - the file: entry still holds exactly the bytes that were read, so
//     the file has not been replaced by a compaction; and
//   - the file.embedding column is still absent, so a concurrent writer
//     that established the state first wins and this run reports a race
//     instead of overwriting it.
//
// Together those make the write idempotent: a second run finds the
// column present, the condition fails, and nothing is written.
type BackfillWriter struct {
	reader  TabletReader
	locator RootLocator
	writer  ConditionalWriter
	next    atomic.Int64
}

// NewBackfillWriter builds a writer over the same seams the tablet
// authority uses.
func NewBackfillWriter(
	reader TabletReader, locator RootLocator, writer ConditionalWriter,
) (*BackfillWriter, error) {
	if reader == nil || locator == nil || writer == nil {
		return nil, ErrInvalidConfig
	}
	w := &BackfillWriter{reader: reader, locator: locator, writer: writer}
	w.next.Store(1)
	return w, nil
}

// WriteFileEmbedding records state for one file entry.
//
// It reports applied=false with a nil error when the conditional write
// was rejected — the entry changed, the tablet split, or the column was
// written by someone else. That is an expected outcome of running a
// migration against a live cluster, not a failure, and re-running the
// backfill resolves whatever replaced the entry.
func (w *BackfillWriter) WriteFileEmbedding(
	ctx context.Context, target BackfillTarget, state embeddingspace.FileState,
) (bool, error) {
	if w == nil {
		return false, ErrInvalidConfig
	}
	if target.TableID == metadata.RootTableID {
		return false, ErrRootBackfillUnsupported
	}
	if target.TableID == "" || len(target.FileQualifier) == 0 || len(target.FileValue) == 0 {
		return false, fmt.Errorf("%w: incomplete backfill target", ErrInvalidConfig)
	}
	if !state.Known() {
		// The backfill only ever writes what a footer positively
		// asserted. Recording unknown would replace an absent column
		// with an explicit non-claim, which is no more informative and
		// which a later integrity check would have to reconcile against
		// the footer that could not produce it.
		return false, fmt.Errorf("%w: refusing to record %s", ErrInvalidConfig, state.String())
	}
	encoded, err := encodeFileEmbedding(state)
	if err != nil {
		return false, err
	}

	row, err := metadata.EncodeTabletRow(target.TableID, target.EndRow)
	if err != nil {
		return false, err
	}
	address, tableID, extent, err := locateMetadataTarget(ctx, w.reader, w.locator, target.TableID, row)
	if err != nil {
		return false, err
	}
	mutation, err := cclient.NewMutation(row)
	if err != nil {
		return false, err
	}
	mutation.PutLatest([]byte(metadata.CFFileEmbedding), target.FileQualifier, nil, encoded)
	wireMutation, err := mutation.ToThrift()
	if err != nil {
		return false, err
	}
	conditions := []*data.TCondition{
		{
			Cf:  []byte(metadata.CFTabletSection),
			Cq:  []byte(metadata.CQPrevRow),
			Cv:  []byte{},
			Val: metadata.EncodePrevEndRow(target.PrevEndRow),
		},
		{
			Cf:  []byte(metadata.CFFile),
			Cq:  append([]byte(nil), target.FileQualifier...),
			Cv:  []byte{},
			Val: append([]byte(nil), target.FileValue...),
		},
		{
			// Val nil means "this column must be absent".
			Cf: []byte(metadata.CFFileEmbedding),
			Cq: append([]byte(nil), target.FileQualifier...),
			Cv: []byte{},
		},
	}
	status, err := w.writer.ConditionalWrite(ctx, address, tableID, extent,
		&data.TConditionalMutation{
			Conditions: conditions, Mutation: wireMutation, ID: w.next.Add(1),
		})
	if err != nil {
		return false, err
	}
	switch status {
	case ingestclient.ConditionalAccepted:
		return true, nil
	case ingestclient.ConditionalRejected:
		return false, nil
	default:
		// An unknown outcome must not be reported as a clean skip: the
		// write may have landed. Reporting it surfaces the file to the
		// operator, and the next run's absent-column condition settles
		// which way it went.
		return false, fmt.Errorf("%w: conditional write outcome is unknown", ErrRejected)
	}
}
