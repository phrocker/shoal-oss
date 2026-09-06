/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package explorercoord

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/allocator"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/guard"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/recovery"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
)

const DefaultCoordinationTable = "_shoal_explorer_coordination"

var ErrIndeterminatePublication = errors.New("explorer coordination: publication outcome is indeterminate")

type Config struct {
	Directory             string
	Domain                coordination.DomainID
	Owner                 coordination.OwnerID
	Authority             transaction.Authority
	ControlVisibility     []byte
	CoordinationTable     string
	PhysicalTables        []string
	EngineOptions         engine.Options
	Pins                  transaction.PinValidator
	Lease                 time.Duration
	Clock                 func() time.Time
	MaxActiveReservations uint32
	MaxRetries            int
	RetryBackoff          time.Duration
	RecoveryLimit         int
	RecoveryConcurrency   int
	RecoveryRounds        int
	RecoveryBackoff       time.Duration
	RecoveryMaxPages      int
	DisableRecoveryOnOpen bool
	testStageHook         func(recoveryStage) error
}

type Request struct {
	Intent     Intent
	Owner      coordination.OwnerID
	LeaseUntil time.Time
}

// Result identifies the committed transaction and its durable logical digest.
type Result struct {
	TXN           coordination.TXN
	LogicalDigest coordination.Digest
	Epoch         coordination.Epoch
	Identities    []coordination.ResultIdentity
	Unchanged     bool
}

// Runtime owns one embedded engine, one process-exclusive directory lock, and
// the complete transaction composition for a domain. Multiple agents may call
// Publish concurrently through one Runtime. Multiple processes may not open
// the same directory.
type Runtime struct {
	mu             sync.RWMutex
	recoveryMu     sync.Mutex
	closed         bool
	engine         *engine.Engine
	lock           *os.File
	store          *EngineStore
	protocolStore  coordinationStore
	intents        *IntentStore
	physical       *Physical
	physicalTables map[string]struct{}
	allocator      *allocator.Client
	guards         *guard.Client
	coordinator    *transaction.Coordinator
	recovery       *recovery.Worker
	domain         coordination.DomainID
	owner          coordination.OwnerID
	authority      transaction.Authority
	lease          time.Duration
	clock          func() time.Time
	recoveryLimit  int
	recoveryPages  int
	intentCursor   []byte
	intentDone     bool
}

func Open(config Config) (*Runtime, error) {
	if strings.TrimSpace(config.Directory) == "" {
		return nil, errors.Join(transaction.ErrInvalid, errors.New("runtime directory is required"))
	}
	if err := config.Domain.Validate(); err != nil {
		return nil, errors.Join(transaction.ErrInvalid, err)
	}
	if err := config.Owner.Validate(); err != nil {
		return nil, errors.Join(transaction.ErrInvalid, err)
	}
	if err := validateRuntimeAuthority(config.Authority); err != nil {
		return nil, err
	}
	if config.Authority.Mode != coordination.WriterModeEmbeddedPrimary {
		return nil, errors.Join(transaction.ErrInvalid, errors.New("embedded runtime requires embedded-primary authority"))
	}
	if config.CoordinationTable == "" {
		config.CoordinationTable = DefaultCoordinationTable
	}
	if !validTableName(config.CoordinationTable) {
		return nil, errors.Join(transaction.ErrInvalid, errors.New("coordination table name is invalid"))
	}
	if len(config.ControlVisibility) > coordination.MaxCoordinateBytes {
		return nil, errors.Join(transaction.ErrInvalid, errors.New("control visibility exceeds its bound"))
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.Lease == 0 {
		config.Lease = time.Minute
	}
	if config.Lease < time.Second || config.Lease > 24*time.Hour {
		return nil, errors.Join(transaction.ErrInvalid, errors.New("runtime lease is outside its bound"))
	}
	if config.MaxActiveReservations == 0 {
		config.MaxActiveReservations = 1024
	}
	if config.MaxActiveReservations > coordination.MaxActiveReservations {
		return nil, errors.Join(transaction.ErrInvalid, errors.New("active reservation bound is invalid"))
	}
	if config.RecoveryLimit == 0 {
		config.RecoveryLimit = 256
	}
	if config.RecoveryMaxPages == 0 {
		config.RecoveryMaxPages = 4096
	}
	if config.RecoveryMaxPages < 1 {
		return nil, errors.Join(transaction.ErrInvalid, errors.New("recovery page bound is invalid"))
	}
	if err := os.MkdirAll(config.Directory, 0o755); err != nil {
		return nil, fmt.Errorf("explorer coordination: create runtime directory: %w", err)
	}
	lock, err := os.OpenFile(
		filepath.Join(config.Directory, ".shoal-explorer-runtime.lock"),
		os.O_CREATE|os.O_RDWR,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("explorer coordination: open runtime lock: %w", err)
	}
	if err := tryLockRuntimeFile(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("explorer coordination: runtime directory is already open: %w", err)
	}
	eng, err := engine.Open(config.Directory, config.EngineOptions)
	if err != nil {
		_ = unlockRuntimeFile(lock)
		_ = lock.Close()
		return nil, err
	}
	closeOnError := func(openErr error) (*Runtime, error) {
		_ = eng.Close()
		_ = unlockRuntimeFile(lock)
		_ = lock.Close()
		return nil, openErr
	}
	tables := append([]string{config.CoordinationTable}, config.PhysicalTables...)
	sort.Strings(tables)
	physicalTables := make(map[string]struct{}, len(config.PhysicalTables))
	for _, table := range config.PhysicalTables {
		physicalTables[table] = struct{}{}
	}
	for index, table := range tables {
		if !validTableName(table) {
			return closeOnError(errors.Join(transaction.ErrInvalid, errors.New("physical table name is invalid")))
		}
		if index > 0 && table == tables[index-1] {
			continue
		}
		if err := ensureTable(eng, table); err != nil {
			return closeOnError(err)
		}
	}
	store, err := NewEngineStore(eng, config.CoordinationTable)
	if err != nil {
		return closeOnError(err)
	}
	intents, err := NewIntentStore(config.Domain, config.ControlVisibility, store)
	if err != nil {
		return closeOnError(err)
	}
	physical, err := NewPhysical(eng)
	if err != nil {
		return closeOnError(err)
	}
	var protocolStore coordinationStore = store
	var physicalWriter transaction.PhysicalWriter = physical
	if config.testStageHook != nil {
		protocolStore = &stageStore{inner: store, hook: config.testStageHook}
		physicalWriter = &stageWriter{inner: physical, hook: config.testStageHook}
	}
	allocatorClient, err := allocator.New(allocator.Config{
		Domain: config.Domain, ControlVisibility: config.ControlVisibility,
		Store: protocolStore, Clock: config.Clock, MaxRetries: config.MaxRetries,
		RetryBackoff: config.RetryBackoff,
	})
	if err != nil {
		return closeOnError(err)
	}
	head, err := allocatorClient.CurrentHead(context.Background())
	if errors.Is(err, allocator.ErrNotFound) {
		head, err = allocatorClient.EnsureInitialized(context.Background(), allocator.InitializeOptions{
			HistoryFloor:        config.Authority.HistoryFloor,
			RetentionGeneration: config.Authority.RetentionGeneration,
			Authority: allocator.Authority{
				Generation: config.Authority.Generation, Mode: config.Authority.Mode,
				Holder: config.Authority.Holder, Fence: config.Authority.Fence,
			},
			MaxActiveReservations: config.MaxActiveReservations,
		})
	}
	if err != nil {
		return closeOnError(err)
	}
	if !headMatchesAuthority(head, config.Authority) {
		return closeOnError(errors.Join(transaction.ErrConflict, errors.New("configured authority does not match durable allocator state")))
	}
	runtime := &Runtime{
		engine: eng, lock: lock, store: store, intents: intents, physical: physical,
		protocolStore: protocolStore, allocator: allocatorClient,
		physicalTables: physicalTables,
		domain:         append(coordination.DomainID(nil), config.Domain...),
		owner:          append(coordination.OwnerID(nil), config.Owner...), authority: cloneAuthority(config.Authority),
		lease: config.Lease, clock: config.Clock, recoveryLimit: config.RecoveryLimit,
		recoveryPages: config.RecoveryMaxPages,
	}
	if err := runtime.compose(config, physicalWriter); err != nil {
		return closeOnError(err)
	}
	if !config.DisableRecoveryOnOpen {
		if err := runtime.Recover(context.Background()); err != nil {
			return closeOnError(err)
		}
	}
	return runtime, nil
}

func (r *Runtime) compose(config Config, physicalWriter transaction.PhysicalWriter) error {
	proxy := &txnStatusProxy{}
	guards, err := guard.New(guard.Config{
		Domain: r.domain, ControlVisibility: config.ControlVisibility, Store: r.protocolStore,
		Authority:  allocatorAuthoritySource{domain: r.domain, allocator: r.allocator},
		Retirement: activeRetirementSource{}, Transactions: proxy,
		Clock: r.clock, MaxRetries: config.MaxRetries, RetryBackoff: config.RetryBackoff,
	})
	if err != nil {
		return err
	}
	coordinator, err := transaction.New(transaction.Config{
		Domain: r.domain, ControlVisibility: config.ControlVisibility,
		Store: r.protocolStore, Allocator: r.allocator, Guards: guards,
		Materializer: r.intents, Writer: physicalWriter, Verifier: r.physical,
		Pins: config.Pins, Quarantine: r.intents, Clock: r.clock,
		MaxRetries: config.MaxRetries, RetryBackoff: config.RetryBackoff,
	})
	if err != nil {
		return err
	}
	proxy.coordinator = coordinator
	worker, err := recovery.New(recovery.Config{
		Domain: r.domain, Owner: r.owner, Authority: cloneAuthority(r.authority),
		Source: recovery.BandedSource{
			Scanner: r.protocolStore, ControlVisibility: append([]byte(nil), config.ControlVisibility...),
		},
		Coordinator: coordinator, Clock: r.clock, Lease: r.lease,
		Limit: config.RecoveryLimit, Concurrency: config.RecoveryConcurrency,
		MaxRounds: config.RecoveryRounds, Backoff: config.RecoveryBackoff,
	})
	if err != nil {
		return err
	}
	r.guards, r.coordinator, r.recovery = guards, coordinator, worker
	return nil
}

// Publish persists the canonical intent before entering the existing
// transaction protocol. Errors carrying ErrIndeterminatePublication must be
// retried with the same intent/token or resolved through recovery.
func (r *Runtime) Publish(ctx context.Context, request Request) (Result, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.publishLocked(ctx, request)
}

func (r *Runtime) publishLocked(
	ctx context.Context,
	request Request,
) (Result, error) {
	if r.closed {
		return Result{}, transaction.ErrUnavailable
	}
	head, err := r.allocator.CurrentHead(ctx)
	if err != nil {
		return Result{}, err
	}
	if !headMatchesAuthority(head, r.authority) {
		return Result{}, transaction.ErrConflict
	}
	owner := request.Owner
	if len(owner) == 0 {
		owner = r.owner
	}
	if err := owner.Validate(); err != nil {
		return Result{}, errors.Join(transaction.ErrInvalid, err)
	}
	leaseUntil := request.LeaseUntil
	if leaseUntil.IsZero() {
		leaseUntil = r.clock().UTC().Add(r.lease)
	}
	if leaseUntil.Location() != time.UTC || !leaseUntil.After(r.clock().UTC()) {
		return Result{}, errors.Join(transaction.ErrInvalid, errors.New("publication lease must be a future UTC time"))
	}
	record, _, err := r.intents.Put(ctx, request.Intent)
	if err != nil {
		return Result{}, err
	}
	if hook := runtimeStageHook(r.protocolStore); hook != nil {
		if err := hook(recoveryStageIntent); err != nil {
			return Result{}, errors.Join(ErrIndeterminatePublication, err)
		}
	}
	result, err := r.coordinator.Publish(ctx, transaction.Publication{
		TXN: record.TXN, Token: record.Intent.Token, LogicalDigest: record.LogicalDigest,
		Owner: owner, LeaseUntil: leaseUntil, Authority: cloneAuthority(r.authority),
	})
	if err != nil {
		return Result{}, r.classifyPublishFailure(ctx, record.TXN, err)
	}
	if err := r.intents.Complete(ctx, record.TXN, record.LogicalDigest, result.Epoch); err != nil {
		return Result{}, errors.Join(ErrIndeterminatePublication, err)
	}
	return Result{
		TXN: append(coordination.TXN(nil), record.TXN...), LogicalDigest: record.LogicalDigest,
		Epoch: result.Epoch, Identities: cloneResultIdentities(result.Identities),
		Unchanged: result.Unchanged,
	}, nil
}

func (r *Runtime) PublishRecord(
	ctx context.Context,
	request explorer.RecordPublication,
) (explorer.RecordPublicationResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return explorer.RecordPublicationResult{}, transaction.PublicError(transaction.ErrUnavailable)
	}
	lpart, err := Partition(r.domain, request.Partition)
	if err != nil {
		return explorer.RecordPublicationResult{}, transaction.PublicError(err)
	}
	intent, err := r.recordIntent(ctx, request, lpart)
	if err != nil {
		return explorer.RecordPublicationResult{}, transaction.PublicError(err)
	}
	result, err := r.publishLocked(ctx, Request{Intent: intent})
	if err != nil {
		public := transaction.PublicError(err)
		if errors.Is(err, ErrIndeterminatePublication) {
			public = explorer.MarkIndeterminateCommit(public)
		}
		return explorer.RecordPublicationResult{}, public
	}
	return explorer.RecordPublicationResult{Epoch: result.Epoch, Unchanged: result.Unchanged}, nil
}

func (r *Runtime) RecordCommitted(
	ctx context.Context,
	request explorer.RecordPublication,
) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false, transaction.ErrUnavailable
	}
	txn, err := DeriveTXN(r.domain, request.Operation, request.Token)
	if err != nil {
		return false, err
	}
	record, err := r.intents.Load(ctx, txn)
	if errors.Is(err, transaction.ErrNotFound) {
		// A record without a durable intent predates transactional publication.
		// New configured writes always persist intent first, so this cannot
		// silently admit a staged record produced by this runtime.
		return true, nil
	}
	if err != nil {
		return false, err
	}
	epoch, complete, err := r.intents.Completed(ctx, txn, record.LogicalDigest)
	if err != nil || !complete {
		return false, err
	}
	snapshot, err := r.coordinator.Inspect(ctx, txn)
	if err != nil {
		return false, err
	}
	if snapshot.Root.State != coordination.StateCommitted ||
		snapshot.Root.Epoch != epoch ||
		snapshot.Root.LogicalDigest != record.LogicalDigest {
		return false, nil
	}
	if !intentContainsRecordCell(record.Intent, request) {
		return false, fmt.Errorf("%w: committed record disagrees with its durable intent", transaction.ErrInternal)
	}
	return true, nil
}

func (r *Runtime) recordIntent(
	ctx context.Context,
	request explorer.RecordPublication,
	lpart coordination.LPART,
) (Intent, error) {
	txn, err := DeriveTXN(r.domain, request.Operation, request.Token)
	if err != nil {
		return Intent{}, err
	}
	if stored, loadErr := r.intents.Load(ctx, txn); loadErr == nil {
		if !r.intentMatchesRecord(stored.Intent, request) ||
			stored.Intent.Guards[0].ExpectedEpoch != request.ExpectedEpoch ||
			stored.Intent.Guards[0].ExpectedDigest != request.ExpectedDigest {
			return Intent{}, transaction.ErrConflict
		}
		return cloneIntent(stored.Intent), nil
	} else if !errors.Is(loadErr, transaction.ErrNotFound) {
		return Intent{}, loadErr
	}
	entity := guard.Entity{
		Kind: request.EntityKind,
		ID:   append(coordination.EntityID(nil), request.EntityID...),
	}
	head, _, err := r.guards.Read(ctx, entity)
	if err != nil {
		return Intent{}, err
	}
	mode := guard.ModeAbsentOrIdentical
	if head == nil {
		if request.ExpectedEpoch != 0 ||
			request.ExpectedDigest != (coordination.Digest{}) {
			return Intent{}, transaction.ErrConflict
		}
	} else {
		if request.ExpectedEpoch == 0 ||
			head.Epoch != request.ExpectedEpoch ||
			head.LogicalDigest != request.ExpectedDigest {
			return Intent{}, transaction.ErrConflict
		}
		mode = guard.ModeMutate
	}
	return Intent{
		Operation: append([]byte(nil), request.Operation...),
		Token:     append([]byte(nil), request.Token...),
		Cells: []Cell{{
			Table: request.Table, Row: append([]byte(nil), request.Row...),
			Family:     append([]byte(nil), request.Family...),
			Qualifier:  append([]byte(nil), request.Qualifier...),
			Visibility: append([]byte(nil), request.Visibility...),
			Value:      append([]byte(nil), request.Value...), EpochTimestamp: true,
			LPART: lpart, CopyGeneration: 1,
		}},
		Guards: []GuardIntent{{
			Entity: guard.Entity{
				Kind: entity.Kind,
				ID:   append(coordination.EntityID(nil), entity.ID...),
			},
			Mode: mode, ExpectedEpoch: request.ExpectedEpoch,
			ExpectedDigest:  request.ExpectedDigest,
			DesiredState:    guard.StateLive,
			DesiredWinnerID: append([]byte(nil), request.WinnerID...),
			LPART:           lpart, LogicalPolicyID: append([]byte(nil), request.LogicalPolicyID...),
			RetirementGeneration: 1,
		}},
		Results: []ResultIdentity{{
			Kind: append([]byte(nil), request.ResultKind...),
			ID:   append([]byte(nil), request.ResultID...),
		}},
	}, nil
}

func (r *Runtime) RecordHead(
	ctx context.Context,
	kind byte,
	id []byte,
) (*explorer.RecordPublicationHead, error) {
	head, _, err := r.ReadEntity(ctx, guard.Entity{
		Kind: kind, ID: append(coordination.EntityID(nil), id...),
	})
	if err != nil {
		return nil, err
	}
	if head == nil {
		return nil, nil
	}
	return &explorer.RecordPublicationHead{
		Epoch: head.Epoch, LogicalDigest: head.LogicalDigest,
		WinnerID: append([]byte(nil), head.WinnerID...),
	}, nil
}

func (r *Runtime) intentMatchesRecord(
	intent Intent,
	request explorer.RecordPublication,
) bool {
	if !bytes.Equal(intent.Operation, request.Operation) ||
		!bytes.Equal(intent.Token, request.Token) ||
		len(intent.Cells) != 1 || len(intent.Guards) != 1 || len(intent.Results) != 1 {
		return false
	}
	cell := intent.Cells[0]
	entity := intent.Guards[0]
	result := intent.Results[0]
	lpart, err := Partition(r.domain, request.Partition)
	if err != nil {
		return false
	}
	return cell.Table == request.Table &&
		bytes.Equal(cell.Row, request.Row) &&
		bytes.Equal(cell.Family, request.Family) &&
		bytes.Equal(cell.Qualifier, request.Qualifier) &&
		bytes.Equal(cell.Visibility, request.Visibility) &&
		bytes.Equal(cell.Value, request.Value) &&
		bytes.Equal(cell.LPART, lpart) &&
		entity.Entity.Kind == request.EntityKind &&
		bytes.Equal(entity.Entity.ID, request.EntityID) &&
		bytes.Equal(entity.DesiredWinnerID, request.WinnerID) &&
		bytes.Equal(entity.LPART, lpart) &&
		bytes.Equal(entity.LogicalPolicyID, request.LogicalPolicyID) &&
		bytes.Equal(result.Kind, request.ResultKind) &&
		bytes.Equal(result.ID, request.ResultID)
}

func intentContainsRecordCell(
	intent Intent,
	request explorer.RecordPublication,
) bool {
	for _, cell := range intent.Cells {
		if cell.Table == request.Table &&
			bytes.Equal(cell.Row, request.Row) &&
			bytes.Equal(cell.Family, request.Family) &&
			bytes.Equal(cell.Qualifier, request.Qualifier) &&
			bytes.Equal(cell.Visibility, request.Visibility) &&
			bytes.Equal(cell.Value, request.Value) {
			return true
		}
	}
	return false
}

// RecoverPage processes one bounded page of durable intents and transaction
// roots. Callers may resume by invoking it again while more is true.
func (r *Runtime) RecoverPage(ctx context.Context) (more bool, err error) {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false, transaction.ErrUnavailable
	}
	if !r.intentDone {
		candidates, next, err := r.intents.Candidates(ctx, r.intentCursor, r.recoveryLimit)
		if err != nil {
			return false, errors.Join(transaction.ErrUnavailable, err)
		}
		for _, txn := range candidates {
			if err := r.recoverIntent(ctx, txn); err != nil {
				return false, err
			}
		}
		r.intentCursor = append(r.intentCursor[:0], next...)
		if len(next) != 0 {
			return true, nil
		}
		r.intentDone = true
	}
	more, err = r.recovery.RunPage(ctx)
	if err != nil {
		return more, err
	}
	if !more {
		r.intentCursor = nil
		r.intentDone = false
		r.recovery.Reset()
	}
	return more, nil
}

func (r *Runtime) Recover(ctx context.Context) error {
	for page := 0; page < r.recoveryPages; page++ {
		more, err := r.RecoverPage(ctx)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
	return errors.Join(transaction.ErrUnavailable, errors.New("recovery page bound reached"))
}

func (r *Runtime) recoverIntent(ctx context.Context, txn coordination.TXN) error {
	record, err := r.intents.Load(ctx, txn)
	if err != nil {
		return err
	}
	if _, complete, err := r.intents.Completed(ctx, txn, record.LogicalDigest); err != nil {
		return err
	} else if complete {
		return nil
	}
	leaseUntil := r.clock().UTC().Add(r.lease)
	snapshot, inspectErr := r.coordinator.Inspect(ctx, txn)
	var result transaction.Result
	switch {
	case inspectErr == nil && snapshot.Root.State.Terminal() &&
		snapshot.Root.State != coordination.StateCommitted:
		return nil
	case inspectErr == nil && snapshot.Root.State.Nonterminal() &&
		snapshot.Lease.LeaseUntil.After(r.clock().UTC()) &&
		!bytes.Equal(snapshot.Root.Owner, r.owner):
		return nil
	case inspectErr == nil:
		result, err = r.coordinator.Recover(
			ctx, txn, r.owner, leaseUntil, cloneAuthority(r.authority),
		)
	case errors.Is(inspectErr, transaction.ErrNotFound):
		result, err = r.coordinator.Publish(ctx, transaction.Publication{
			TXN: record.TXN, Token: record.Intent.Token, LogicalDigest: record.LogicalDigest,
			Owner: r.owner, LeaseUntil: leaseUntil, Authority: cloneAuthority(r.authority),
		})
	default:
		return inspectErr
	}
	if err != nil {
		return err
	}
	return r.intents.Complete(ctx, record.TXN, record.LogicalDigest, result.Epoch)
}

func (r *Runtime) classifyPublishFailure(
	ctx context.Context,
	txn coordination.TXN,
	err error,
) error {
	if errors.Is(err, transaction.ErrInvalid) || errors.Is(err, transaction.ErrConflict) {
		return err
	}
	snapshot, inspectErr := r.coordinator.Inspect(ctx, txn)
	if inspectErr == nil && snapshot.Root.State.Terminal() &&
		snapshot.Root.State != coordination.StateCommitted {
		return err
	}
	return errors.Join(ErrIndeterminatePublication, err)
}

func (r *Runtime) Authority() transaction.Authority {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneAuthority(r.authority)
}

// CurrentHead returns the authoritative allocator state for diagnostics and
// dependency code that needs the current publication frontier.
func (r *Runtime) CurrentHead(
	ctx context.Context,
) (coordination.AllocatorHeadV1, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return coordination.AllocatorHeadV1{}, transaction.ErrUnavailable
	}
	return r.allocator.CurrentHead(ctx)
}

// ReadEntity returns the committed and pending guard records for one entity.
func (r *Runtime) ReadEntity(
	ctx context.Context,
	entity guard.Entity,
) (*guard.Head, *guard.Pending, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, nil, transaction.ErrUnavailable
	}
	return r.guards.Read(ctx, entity)
}

// Inspect returns the authoritative transaction root and lease.
func (r *Runtime) Inspect(
	ctx context.Context,
	txn coordination.TXN,
) (transaction.Snapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return transaction.Snapshot{}, transaction.ErrUnavailable
	}
	return r.coordinator.Inspect(ctx, txn)
}

func (r *Runtime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	engineErr := r.engine.Close()
	unlockErr := unlockRuntimeFile(r.lock)
	closeErr := r.lock.Close()
	return errors.Join(engineErr, unlockErr, closeErr)
}

type EmbeddedExplorer struct {
	Runtime  *Runtime
	Explorer *explorer.Explorer
}

// OpenExplorer opens the production embedded composition and wires
// deterministic document-revision record publication through it. Other
// Explorer record families retain their existing persistence path.
func OpenExplorer(
	config Config,
	options explorer.Options,
) (*EmbeddedExplorer, error) {
	config.PhysicalTables = append(
		append([]string(nil), config.PhysicalTables...),
		explorer.EmbeddedTableName,
	)
	runtime, err := Open(config)
	if err != nil {
		return nil, err
	}
	corpus, err := explorer.OpenWithEmbeddedEngine(runtime.engine, options, runtime)
	if err != nil {
		_ = runtime.Close()
		return nil, err
	}
	return &EmbeddedExplorer{Runtime: runtime, Explorer: corpus}, nil
}

func (e *EmbeddedExplorer) Close() error {
	if e == nil {
		return nil
	}
	var explorerErr error
	if e.Explorer != nil {
		explorerErr = e.Explorer.Close()
	}
	var runtimeErr error
	if e.Runtime != nil {
		runtimeErr = e.Runtime.Close()
	}
	return errors.Join(explorerErr, runtimeErr)
}

type allocatorAuthoritySource struct {
	domain    coordination.DomainID
	allocator *allocator.Client
}

func (s allocatorAuthoritySource) Current(
	ctx context.Context,
	domain coordination.DomainID,
) (guard.Authority, error) {
	if s.allocator == nil || !bytes.Equal(domain, s.domain) {
		return guard.Authority{}, guard.ErrUnavailable
	}
	head, err := s.allocator.CurrentHead(ctx)
	if err != nil {
		return guard.Authority{}, err
	}
	return guard.Authority{
		Generation: head.WriterAuthorityGeneration, Fence: head.WriterFence,
		RetentionGeneration: head.RetentionGeneration, HistoryFloor: head.HistoryFloor,
	}, nil
}

type activeRetirementSource struct{}

func (activeRetirementSource) Retired(
	context.Context,
	coordination.DomainID,
	guard.Entity,
) (bool, coordination.Generation, error) {
	return false, 1, nil
}

type txnStatusProxy struct {
	coordinator *transaction.Coordinator
}

func (p *txnStatusProxy) Status(
	ctx context.Context,
	domain coordination.DomainID,
	txn coordination.TXN,
) (guard.TxnDisposition, error) {
	if p.coordinator == nil {
		return 0, transaction.ErrUnavailable
	}
	return p.coordinator.Status(ctx, domain, txn)
}

func ensureTable(eng *engine.Engine, table string) error {
	for _, existing := range eng.TableNames() {
		if existing == table {
			return nil
		}
	}
	return eng.CreateTable(table, engine.TableOptions{})
}

func validateRuntimeAuthority(authority transaction.Authority) error {
	if err := authority.Generation.Validate(); err != nil {
		return errors.Join(transaction.ErrInvalid, err)
	}
	if err := authority.Fence.Validate(); err != nil {
		return errors.Join(transaction.ErrInvalid, err)
	}
	if err := authority.Holder.Validate(); err != nil {
		return errors.Join(transaction.ErrInvalid, err)
	}
	if err := authority.Mode.Validate(); err != nil {
		return errors.Join(transaction.ErrInvalid, err)
	}
	if err := authority.RetentionGeneration.Validate(); err != nil {
		return errors.Join(transaction.ErrInvalid, err)
	}
	if err := authority.HistoryFloor.Validate(); err != nil {
		return errors.Join(transaction.ErrInvalid, err)
	}
	return nil
}

func headMatchesAuthority(
	head coordination.AllocatorHeadV1,
	authority transaction.Authority,
) bool {
	return head.WriterAuthorityGeneration == authority.Generation &&
		head.WriterFence == authority.Fence &&
		head.WriterMode == authority.Mode &&
		bytes.Equal(head.WriterHolder, authority.Holder) &&
		head.RetentionGeneration == authority.RetentionGeneration &&
		head.HistoryFloor == authority.HistoryFloor
}

func cloneAuthority(value transaction.Authority) transaction.Authority {
	value.Holder = append(coordination.OwnerID(nil), value.Holder...)
	return value
}

func cloneResultIdentities(values []coordination.ResultIdentity) []coordination.ResultIdentity {
	result := make([]coordination.ResultIdentity, len(values))
	for index := range values {
		result[index].Kind = append([]byte(nil), values[index].Kind...)
		result[index].ID = append([]byte(nil), values[index].ID...)
	}
	return result
}

var _ explorer.RecordPublicationAdapter = (*Runtime)(nil)

type coordinationStore interface {
	allocator.Store
	ScanPrefix(context.Context, []byte, []byte, []byte, []byte, int) ([]allocator.Cell, error)
	ScanPrefixFrom(context.Context, []byte, []byte, []byte, []byte, []byte, int) ([]allocator.Cell, error)
}

type recoveryStage uint8

const (
	recoveryStageIntent recoveryStage = iota + 1
	recoveryStagePhysical
	recoveryStagePrepared
	recoveryStageCommitted
	recoveryStageCheckpoint
)

type stageStore struct {
	inner *EngineStore
	hook  func(recoveryStage) error
}

func (s *stageStore) ReadExact(
	ctx context.Context,
	coordinates []allocator.Coordinate,
) ([]allocator.Cell, error) {
	return s.inner.ReadExact(ctx, coordinates)
}

func (s *stageStore) ScanRowPrefix(
	ctx context.Context,
	row, family, qualifier, visibility []byte,
	limit int,
) ([]allocator.Cell, error) {
	return s.inner.ScanRowPrefix(ctx, row, family, qualifier, visibility, limit)
}

func (s *stageStore) ScanPrefix(
	ctx context.Context,
	row, family, qualifier, visibility []byte,
	limit int,
) ([]allocator.Cell, error) {
	return s.inner.ScanPrefix(ctx, row, family, qualifier, visibility, limit)
}

func (s *stageStore) ScanPrefixFrom(
	ctx context.Context,
	prefix, start, family, qualifier, visibility []byte,
	limit int,
) ([]allocator.Cell, error) {
	return s.inner.ScanPrefixFrom(ctx, prefix, start, family, qualifier, visibility, limit)
}

func (s *stageStore) CompareAndMutate(
	ctx context.Context,
	mutation allocator.Mutation,
) (allocator.Status, error) {
	status, err := s.inner.CompareAndMutate(ctx, mutation)
	if status != allocator.StatusAccepted || err != nil || s.hook == nil {
		return status, err
	}
	stage := recoveryStage(0)
	for _, update := range mutation.Updates {
		if root, decodeErr := coordination.UnmarshalTxnRootV3(update.Value); decodeErr == nil {
			switch root.State {
			case coordination.StatePrepared:
				stage = recoveryStagePrepared
			case coordination.StateCommitted:
				stage = recoveryStageCommitted
			}
		}
		if head, decodeErr := coordination.UnmarshalAllocatorHeadV1(update.Value); decodeErr == nil &&
			head.Frontier != 0 {
			stage = recoveryStageCheckpoint
		}
	}
	if stage != 0 {
		if hookErr := s.hook(stage); hookErr != nil {
			return allocator.StatusUnknown, hookErr
		}
	}
	return status, nil
}

type stageWriter struct {
	inner *Physical
	hook  func(recoveryStage) error
}

func (w *stageWriter) Write(
	ctx context.Context,
	epoch coordination.Epoch,
	cells []transaction.PhysicalCell,
) error {
	if err := w.inner.Write(ctx, epoch, cells); err != nil {
		return err
	}
	if w.hook != nil {
		return w.hook(recoveryStagePhysical)
	}
	return nil
}

func runtimeStageHook(store coordinationStore) func(recoveryStage) error {
	if value, ok := store.(*stageStore); ok {
		return value.hook
	}
	return nil
}
