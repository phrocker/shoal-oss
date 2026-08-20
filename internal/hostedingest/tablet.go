// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

// Package hostedingest joins fenced WAL, memtable, and minor-compaction
// authorities for one manager-assigned tablet.
package hostedingest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/phrocker/shoal/internal/ingestrouter"
	"github.com/phrocker/shoal/internal/mincauthority"
	"github.com/phrocker/shoal/internal/rfile"
	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/tabletloader"
	"github.com/phrocker/shoal/internal/tserver"
	"github.com/phrocker/shoal/internal/walauthority"
)

type MetadataAuthority interface {
	walauthority.MetadataAuthority
	mincauthority.MetadataAuthority
}

type MetadataFactory interface {
	Open(context.Context, tabletloader.Specification, ingestrouter.Fence) (MetadataAuthority, error)
}

type Config struct {
	Host           *tserver.Host
	ServerAddress  string
	WALRoot        string
	MincRoot       string
	StateRoot      string
	WALStore       walauthority.Store
	Outputs        storage.Backend
	Metadata       MetadataFactory
	FlushCells     int
	Now            func() time.Time
	NewOperationID func() string
}

type Factory struct{ cfg Config }

func NewFactory(cfg Config) (*Factory, error) {
	if cfg.Host == nil || cfg.ServerAddress == "" || cfg.WALRoot == "" ||
		cfg.MincRoot == "" || cfg.StateRoot == "" || cfg.WALStore == nil ||
		cfg.Outputs == nil || cfg.Metadata == nil {
		return nil, errors.New("hostedingest: incomplete factory configuration")
	}
	if cfg.FlushCells <= 0 {
		// Hosted scans currently consume immutable files, not a live memtable.
		// Flush each acknowledged batch by default so new scans observe it.
		cfg.FlushCells = 1
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewOperationID == nil {
		cfg.NewOperationID = uuid.NewString
	}
	return &Factory{cfg: cfg}, nil
}

func (f *Factory) Open(
	ctx context.Context,
	spec tabletloader.Specification,
	attempt tserver.Attempt,
) (ingestrouter.HostedTablet, error) {
	serverLock, serverOK := f.cfg.Host.Lock()
	managerLock, managerOK := f.cfg.Host.ManagerLock()
	if !serverOK || !managerOK || !attempt.Valid() {
		return nil, ingestrouter.ErrStaleFence
	}
	serverGeneration := string(spec.Generation)
	if serverGeneration == "" {
		serverGeneration = serverLock.String()
	}
	fence := ingestrouter.Fence{
		ServerGeneration: serverGeneration, ManagerGeneration: managerLock.String(),
		Assignment: attempt.Assignment(),
	}
	extent := ingestrouter.Extent{
		TableID: spec.Extent.TableID, PrevEndRow: spec.Extent.PrevEndRow, EndRow: spec.Extent.EndRow,
	}
	verifier := hostVerifier{
		host: f.cfg.Host, attempt: attempt,
		server: serverLock, manager: managerLock, fence: fence,
	}
	recoveryVerifier := verifier
	recoveryVerifier.allowLoading = true
	metadata, err := f.cfg.Metadata.Open(ctx, spec, fence)
	if err != nil {
		return nil, err
	}
	tablet := &Tablet{
		extent: extent, fence: fence, verifier: verifier, flushCells: f.cfg.FlushCells,
		metadata:  metadata,
		snapshots: make(map[string]mincauthority.Snapshot),
		applied:   make(map[string]struct{}), assigned: make(map[string][]ingestrouter.Mutation),
		nextTimestamp: f.cfg.Now().UnixMilli(), newOperationID: f.cfg.NewOperationID,
	}
	wal, report, err := walauthority.Open(ctx, walauthority.Config{
		Root: f.cfg.WALRoot, ServerAddress: f.cfg.ServerAddress,
		Extent: extent, Fence: fence, Metadata: metadata, Store: f.cfg.WALStore,
		Verifier: verifier, RecoveryVerifier: recoveryVerifier, Sink: tablet,
	})
	if err != nil {
		return nil, err
	}
	tablet.wal = wal
	tablet.recovery = report
	stateStore := &mincauthority.FileStateStore{Dir: filepath.Join(f.cfg.StateRoot, extentDigest(extent))}
	coordinator, err := mincauthority.New(mincauthority.Config{
		Root: f.cfg.MincRoot, Extent: extent, Fence: fence,
		Snapshots: tablet, Verifier: verifier, Metadata: metadata,
		Outputs: mincauthority.BackendOutputStore{Backend: f.cfg.Outputs},
		States:  stateStore,
	})
	if err != nil {
		_ = wal.Close(context.Background())
		return nil, err
	}
	tablet.minc = coordinator
	pending, err := stateStore.Pending(ctx, extent, fence)
	if err != nil {
		_ = wal.Close(context.Background())
		return nil, err
	}
	for _, state := range pending {
		tablet.resume = append(tablet.resume, state.OperationID)
		if state.Phase >= mincauthority.PhaseCommitted {
			tablet.snapshots[state.OperationID] = mincauthority.Snapshot{
				ID: state.SnapshotID, Extent: extent, Fence: fence,
				Boundary:    state.Boundary,
				CoveredWALs: append([]walauthority.Reference(nil), state.CoveredWALs...),
			}
		}
	}
	return tablet, nil
}

type hostVerifier struct {
	host            *tserver.Host
	attempt         tserver.Attempt
	server, manager tserver.LockID
	fence           ingestrouter.Fence
	allowLoading    bool
}

func (v hostVerifier) Verify(_ context.Context, fence ingestrouter.Fence) error {
	if fence != v.fence {
		return ingestrouter.ErrStaleFence
	}
	fenceValue := tserver.Fence{Server: v.server, Manager: v.manager}
	if v.allowLoading {
		return v.host.VerifyAssigned(fenceValue, v.attempt)
	}
	return v.host.VerifyHosted(fenceValue, v.attempt)
}

type Tablet struct {
	opMu sync.Mutex
	mu   sync.Mutex

	extent   ingestrouter.Extent
	fence    ingestrouter.Fence
	verifier hostVerifier
	metadata MetadataAuthority
	wal      *walauthority.Tablet
	minc     *mincauthority.Coordinator
	recovery walauthority.RecoveryReport

	active         []mincauthority.Cell
	activeSize     int
	snapshots      map[string]mincauthority.Snapshot
	applied        map[string]struct{}
	assigned       map[string][]ingestrouter.Mutation
	nextTimestamp  int64
	flushCells     int
	newOperationID func() string
	pendingFlush   string
	resume         []string
	files          []mincauthority.DataFile
	closed         bool
}

func (t *Tablet) Extent() ingestrouter.Extent { return cloneExtent(t.extent) }
func (t *Tablet) Fence() ingestrouter.Fence   { return t.fence }
func (t *Tablet) Authority() ingestrouter.CommitAuthority {
	return ingestrouter.AuthorityAccumuloWAL
}

func (t *Tablet) Commit(ctx context.Context, request ingestrouter.CommitRequest) error {
	t.opMu.Lock()
	defer t.opMu.Unlock()
	if err := t.resumePending(ctx); err != nil {
		return err
	}
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return walauthority.ErrClosed
	}
	request.Mutations = t.assignTimestamps(request.OperationID, request.Mutations)
	if err := t.wal.Commit(ctx, request); err != nil {
		return routeError(err)
	}
	t.mu.Lock()
	flush := !t.closed && len(t.active) >= t.flushCells
	t.mu.Unlock()
	if flush {
		if err := t.flush(ctx); err != nil {
			// WAL durability is already satisfied. Retain the exact compaction
			// operation ID so explicit flush, unload, or a later threshold
			// crossing resumes it instead of duplicating the snapshot.
			return nil
		}
	}
	return nil
}

func (t *Tablet) Apply(
	ctx context.Context,
	operationID string,
	_ int64,
	mutations []ingestrouter.Mutation,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return walauthority.ErrClosed
	}
	if _, ok := t.applied[operationID]; ok {
		return nil
	}
	t.assigned[operationID] = cloneMutations(mutations)
	for _, mutation := range mutations {
		for _, update := range mutation.Updates {
			if update.Timestamp.Set && update.Timestamp.Value >= t.nextTimestamp {
				t.nextTimestamp = update.Timestamp.Value + 1
			}
			t.active = append(t.active, mincauthority.Cell{
				Key: rfile.Key{
					Row:              append([]byte(nil), mutation.Row...),
					ColumnFamily:     append([]byte(nil), update.ColumnFamily...),
					ColumnQualifier:  append([]byte(nil), update.ColumnQualifier...),
					ColumnVisibility: append([]byte(nil), update.ColumnVisibility...),
					Timestamp:        update.Timestamp.Value, Deleted: update.Delete,
				},
				Value: append([]byte(nil), update.Value...),
			})
			t.activeSize += len(mutation.Row) + len(update.ColumnFamily) +
				len(update.ColumnQualifier) + len(update.ColumnVisibility) + len(update.Value) + 24
		}
	}
	t.applied[operationID] = struct{}{}
	return nil
}

func (t *Tablet) Prepare(
	ctx context.Context,
	operationID string,
	extent ingestrouter.Extent,
	fence ingestrouter.Fence,
) (mincauthority.Snapshot, error) {
	t.mu.Lock()
	if existing, ok := t.snapshots[operationID]; ok {
		t.mu.Unlock()
		return cloneSnapshot(existing), nil
	}
	t.mu.Unlock()

	var snapshot mincauthority.Snapshot
	boundary, err := t.wal.SealForMinorCompaction(ctx, func(boundary walauthority.MinorCompactionBoundary) error {
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.closed || len(t.active) == 0 {
			return mincauthority.ErrInvalidSnapshot
		}
		snapshot = mincauthority.Snapshot{
			ID:     operationID + ":" + fmt.Sprint(boundary.Sequence),
			Extent: cloneExtent(extent), Fence: fence, Boundary: boundary.Sequence,
			Cells:       append([]mincauthority.Cell(nil), t.active...),
			CoveredWALs: append([]walauthority.Reference(nil), boundary.References...),
		}
		t.active = nil
		t.activeSize = 0
		t.snapshots[operationID] = cloneSnapshot(snapshot)
		return nil
	})
	if err != nil {
		return mincauthority.Snapshot{}, err
	}
	if snapshot.Boundary != boundary.Sequence {
		return mincauthority.Snapshot{}, mincauthority.ErrInvalidSnapshot
	}
	return cloneSnapshot(snapshot), nil
}

func (t *Tablet) Complete(ctx context.Context, snapshotID string, _ mincauthority.DataFile) error {
	t.mu.Lock()
	var snapshot mincauthority.Snapshot
	var operationID string
	for id, candidate := range t.snapshots {
		if candidate.ID == snapshotID {
			operationID, snapshot = id, candidate
			break
		}
	}
	t.mu.Unlock()
	for _, ref := range snapshot.CoveredWALs {
		if err := t.wal.Retire(ctx, ref); err != nil {
			return err
		}
	}
	if operationID != "" {
		t.mu.Lock()
		delete(t.snapshots, operationID)
		t.mu.Unlock()
	}
	return nil
}

func (t *Tablet) Flush(ctx context.Context) error {
	t.opMu.Lock()
	defer t.opMu.Unlock()
	if err := t.resumePending(ctx); err != nil {
		return err
	}
	return t.flush(ctx)
}

func (t *Tablet) resumePending(ctx context.Context) error {
	for len(t.resume) > 0 {
		operationID := t.resume[0]
		file, err := t.minc.Run(ctx, operationID)
		if err != nil {
			return err
		}
		t.mu.Lock()
		found := false
		for _, existing := range t.files {
			if existing.Path == file.Path {
				found = true
				break
			}
		}
		if !found {
			t.files = append(t.files, file)
		}
		t.resume = t.resume[1:]
		t.mu.Unlock()
	}
	return nil
}

func (t *Tablet) flush(ctx context.Context) error {
	t.mu.Lock()
	empty := len(t.active) == 0 && t.pendingFlush == ""
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return walauthority.ErrClosed
	}
	if empty {
		return nil
	}
	t.mu.Lock()
	operationID := t.pendingFlush
	if operationID == "" {
		operationID = "minc-" + t.newOperationID()
		t.pendingFlush = operationID
	}
	t.mu.Unlock()
	file, err := t.minc.Run(ctx, operationID)
	if err == nil {
		t.mu.Lock()
		if t.pendingFlush == operationID {
			t.pendingFlush = ""
		}
		found := false
		for _, existing := range t.files {
			if existing.Path == file.Path {
				found = true
				break
			}
		}
		if !found {
			t.files = append(t.files, file)
		}
		t.mu.Unlock()
	}
	return err
}

func (t *Tablet) Close(ctx context.Context) error {
	t.opMu.Lock()
	defer t.opMu.Unlock()
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()
	if err := t.resumePending(ctx); err != nil {
		return err
	}
	if err := t.flush(ctx); err != nil {
		return err
	}
	if err := t.wal.Close(ctx); err != nil {
		return err
	}
	if releaser, ok := t.metadata.(interface {
		Release(context.Context, ingestrouter.Extent, ingestrouter.Fence) error
	}); ok {
		if err := releaser.Release(ctx, t.extent, t.fence); err != nil {
			return err
		}
	}
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}

func (t *Tablet) Recovery() walauthority.RecoveryReport { return t.recovery }

func (t *Tablet) DataFiles() []mincauthority.DataFile {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]mincauthority.DataFile, len(t.files))
	copy(out, t.files)
	for i := range out {
		out[i].StartRow = append([]byte(nil), t.files[i].StartRow...)
		out[i].EndRow = append([]byte(nil), t.files[i].EndRow...)
	}
	return out
}

func (t *Tablet) assignTimestamps(operationID string, mutations []ingestrouter.Mutation) []ingestrouter.Mutation {
	t.mu.Lock()
	defer t.mu.Unlock()
	if prior, ok := t.assigned[operationID]; ok {
		return cloneMutations(prior)
	}
	out := make([]ingestrouter.Mutation, len(mutations))
	for i, mutation := range mutations {
		out[i].Row = append([]byte(nil), mutation.Row...)
		out[i].Updates = make([]ingestrouter.Update, len(mutation.Updates))
		for j, update := range mutation.Updates {
			out[i].Updates[j] = update
			out[i].Updates[j].ColumnFamily = append([]byte(nil), update.ColumnFamily...)
			out[i].Updates[j].ColumnQualifier = append([]byte(nil), update.ColumnQualifier...)
			out[i].Updates[j].ColumnVisibility = append([]byte(nil), update.ColumnVisibility...)
			out[i].Updates[j].Value = append([]byte(nil), update.Value...)
			if !update.Timestamp.Set {
				out[i].Updates[j].Timestamp = ingestrouter.Timestamp{
					Set: true, Value: t.nextTimestamp,
				}
				t.nextTimestamp++
			} else if update.Timestamp.Value >= t.nextTimestamp {
				t.nextTimestamp = update.Timestamp.Value + 1
			}
		}
	}
	t.assigned[operationID] = cloneMutations(out)
	return out
}

func routeError(err error) error {
	switch {
	case errors.Is(err, walauthority.ErrStaleOwner), errors.Is(err, mincauthority.ErrStaleOwner):
		return errors.Join(ingestrouter.ErrStaleFence, err)
	case errors.Is(err, walauthority.ErrAmbiguous), errors.Is(err, mincauthority.ErrAmbiguous):
		return errors.Join(ingestrouter.ErrUnknownCommit, err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return errors.Join(ingestrouter.ErrRetryable, err)
	default:
		return err
	}
}

func cloneExtent(in ingestrouter.Extent) ingestrouter.Extent {
	return ingestrouter.Extent{
		TableID: in.TableID, PrevEndRow: append([]byte(nil), in.PrevEndRow...),
		EndRow: append([]byte(nil), in.EndRow...),
	}
}

func cloneSnapshot(in mincauthority.Snapshot) mincauthority.Snapshot {
	out := in
	out.Extent = cloneExtent(in.Extent)
	out.Cells = make([]mincauthority.Cell, len(in.Cells))
	for i, cell := range in.Cells {
		out.Cells[i] = mincauthority.Cell{Key: *cell.Key.Clone(), Value: append([]byte(nil), cell.Value...)}
	}
	out.CoveredWALs = append([]walauthority.Reference(nil), in.CoveredWALs...)
	return out
}

func cloneMutations(in []ingestrouter.Mutation) []ingestrouter.Mutation {
	out := make([]ingestrouter.Mutation, len(in))
	for i, mutation := range in {
		out[i].Row = append([]byte(nil), mutation.Row...)
		out[i].Updates = make([]ingestrouter.Update, len(mutation.Updates))
		for j, update := range mutation.Updates {
			out[i].Updates[j] = update
			out[i].Updates[j].ColumnFamily = append([]byte(nil), update.ColumnFamily...)
			out[i].Updates[j].ColumnQualifier = append([]byte(nil), update.ColumnQualifier...)
			out[i].Updates[j].ColumnVisibility = append([]byte(nil), update.ColumnVisibility...)
			out[i].Updates[j].Value = append([]byte(nil), update.Value...)
		}
	}
	return out
}

func extentDigest(extent ingestrouter.Extent) string {
	sum := sha256.Sum256([]byte(extent.Key()))
	return fmt.Sprintf("%x", sum[:16])
}
