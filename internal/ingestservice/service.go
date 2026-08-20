// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

// Package ingestservice implements Accumulo 4 TabletIngestClientService over
// the fenced ingest router.
package ingestservice

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/ingestrouter"
	clientgen "github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletingest"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletserver"
)

var (
	ErrDraining               = errors.New("ingestservice: draining")
	ErrUnsupportedDurability  = errors.New("ingestservice: durability mode is unsupported")
	ErrConditionalUnsupported = errors.New("ingestservice: conditional mutations are unsupported")
	ErrBackpressure           = errors.New("ingestservice: backpressure limit exceeded")
	ErrPermissionDenied       = errors.New("ingestservice: permission denied")
)

type Authenticator interface {
	Authenticate(context.Context, *security.TCredentials) error
	AuthorizeWrite(context.Context, *security.TCredentials, string) error
}

type Config struct {
	Router          *ingestrouter.Router
	Authenticator   Authenticator
	MaxSessions     int
	MaxSessionBytes int64
	SessionTTL      time.Duration
	Now             func() time.Time
}

type Metrics struct {
	ActiveSessions   int64
	Started          uint64
	AppliedBatches   uint64
	AppliedMutations uint64
	RejectedBatches  uint64
	RetriedBatches   uint64
	ExpiredSessions  uint64
	Backpressure     uint64
}

type Service struct {
	cfg Config

	mu        sync.Mutex
	drainDone chan struct{}
	sessions  map[data.UpdateID]*updateSession
	nextID    atomic.Int64
	accepting atomic.Bool

	started          atomic.Uint64
	appliedBatches   atomic.Uint64
	appliedMutations atomic.Uint64
	rejectedBatches  atomic.Uint64
	retriedBatches   atomic.Uint64
	expiredSessions  atomic.Uint64
	backpressure     atomic.Uint64
}

type updateSession struct {
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	credentials  *security.TCredentials
	durability   tabletingest.TDurability
	tables       map[string]*ingestrouter.Session
	failed       map[string]failedExtent
	committed    map[string]int64
	authFailures map[string]authFailure
	violations   map[string]*data.TConstraintViolationSummary
	bytes        int64
	request      uint64
	lastUsed     time.Time
	closed       bool
}

type failedExtent struct {
	extent    *data.TKeyExtent
	committed int64
}

type authFailure struct {
	extent *data.TKeyExtent
	code   clientgen.SecurityErrorCode
}

func New(cfg Config) (*Service, error) {
	if cfg.Router == nil || cfg.Authenticator == nil {
		return nil, errors.New("ingestservice: incomplete configuration")
	}
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = 1024
	}
	if cfg.MaxSessionBytes <= 0 {
		cfg.MaxSessionBytes = 64 << 20
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = 5 * time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("ingestservice: session id seed: %w", err)
	}
	s := &Service{
		cfg: cfg, sessions: make(map[data.UpdateID]*updateSession),
		drainDone: make(chan struct{}),
	}
	s.nextID.Store(int64(binary.BigEndian.Uint64(seed[:]) & ((1 << 62) - 1)))
	s.accepting.Store(true)
	return s, nil
}

func (s *Service) StartUpdate(
	ctx context.Context,
	_ *clientgen.TInfo,
	credentials *security.TCredentials,
	durability tabletingest.TDurability,
) (data.UpdateID, error) {
	if !s.accepting.Load() {
		return 0, securityError(credentials, clientgen.SecurityErrorCode_CONNECTION_ERROR)
	}
	if !supportedDurability(durability) {
		return 0, securityError(credentials, clientgen.SecurityErrorCode_UNSUPPORTED_OPERATION)
	}
	if err := s.cfg.Authenticator.Authenticate(ctx, credentials); err != nil {
		return 0, securityError(credentials, clientgen.SecurityErrorCode_BAD_CREDENTIALS)
	}
	now := s.cfg.Now()
	s.mu.Lock()
	s.expireLocked(now)
	if len(s.sessions) >= s.cfg.MaxSessions {
		s.mu.Unlock()
		s.backpressure.Add(1)
		return 0, securityError(credentials, clientgen.SecurityErrorCode_CONNECTION_ERROR)
	}
	id := data.UpdateID(s.nextID.Add(1))
	if id <= 0 {
		id = data.UpdateID(s.nextID.Add(1 << 30))
	}
	sessionCtx, cancelSession := context.WithCancel(context.Background())
	s.sessions[id] = &updateSession{
		ctx: sessionCtx, cancel: cancelSession,
		credentials: cloneCredentials(credentials), durability: durability,
		tables: make(map[string]*ingestrouter.Session),
		failed: make(map[string]failedExtent), committed: make(map[string]int64),
		authFailures: make(map[string]authFailure),
		violations:   make(map[string]*data.TConstraintViolationSummary),
		lastUsed:     now,
	}
	s.mu.Unlock()
	s.started.Add(1)
	return id, nil
}

func (s *Service) ApplyUpdates(
	ctx context.Context,
	_ *clientgen.TInfo,
	updateID data.UpdateID,
	keyExtent *data.TKeyExtent,
	wireMutations []*data.TMutation,
) error {
	session := s.session(updateID)
	if session == nil {
		return nil
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.ctx.Err() != nil {
		return nil
	}
	session.lastUsed = s.cfg.Now()

	extent, err := decodeExtent(keyExtent)
	if err != nil || len(wireMutations) == 0 {
		session.recordFailure(keyExtent, session.committedPrefix(keyExtent))
		s.rejectedBatches.Add(1)
		return nil
	}
	size := thriftBatchSize(keyExtent, wireMutations)
	if size > s.cfg.MaxSessionBytes-session.bytes {
		session.recordFailure(keyExtent, session.committedPrefix(keyExtent))
		s.backpressure.Add(1)
		s.rejectedBatches.Add(1)
		return nil
	}
	session.bytes += size
	if err := s.cfg.Authenticator.AuthorizeWrite(ctx, session.credentials, extent.TableID); err != nil {
		if errors.Is(err, ErrPermissionDenied) {
			session.recordAuthorizationFailure(keyExtent, clientgen.SecurityErrorCode_PERMISSION_DENIED)
			s.rejectedBatches.Add(1)
		} else {
			session.recordFailure(keyExtent, session.committedPrefix(keyExtent))
			s.retriedBatches.Add(1)
		}
		return nil
	}
	mutations := make([]ingestrouter.Mutation, len(wireMutations))
	for i, wireMutation := range wireMutations {
		decoded, decodeErr := cclient.FromThrift(wireMutation)
		if decodeErr != nil {
			session.recordViolation("org.apache.accumulo.core.constraints.DefaultKeySizeConstraint", -1,
				"malformed mutation encoding", 1)
			s.rejectedBatches.Add(1)
			return nil
		}
		mutations[i] = routerMutation(decoded)
	}
	routerSession := session.tables[extent.TableID]
	if routerSession == nil {
		routerSession, err = s.cfg.Router.Open(fmt.Sprintf("%d:%s", updateID, extent.TableID), extent.TableID)
		if err != nil {
			session.recordFailure(keyExtent, session.committedPrefix(keyExtent))
			s.rejectedBatches.Add(1)
			return nil
		}
		session.tables[extent.TableID] = routerSession
	}
	session.request++
	applyCtx, cancelApply := context.WithCancel(ctx)
	stop := context.AfterFunc(session.ctx, cancelApply)
	defer func() {
		stop()
		cancelApply()
	}()
	result, applyErr := routerSession.Apply(applyCtx, ingestrouter.Request{
		ID:      fmt.Sprintf("%d", session.request),
		Batches: []ingestrouter.Batch{{Extent: extent, Mutations: mutations}},
	})
	outcome := result.Outcomes[extent.Key()]
	switch {
	case applyErr == nil && outcome.Status == ingestrouter.OutcomeApplied:
		session.committed[thriftExtentKey(keyExtent)] += int64(len(mutations))
		s.appliedBatches.Add(1)
		s.appliedMutations.Add(uint64(len(mutations)))
	case outcome.Status == ingestrouter.OutcomeRetry:
		session.recordFailure(keyExtent, session.committedPrefix(keyExtent))
		s.retriedBatches.Add(1)
	default:
		session.recordFailure(keyExtent, session.committedPrefix(keyExtent))
		s.rejectedBatches.Add(1)
	}
	return nil
}

func (s *Service) CloseUpdate(
	_ context.Context,
	_ *clientgen.TInfo,
	updateID data.UpdateID,
) (*data.UpdateErrors, error) {
	session := s.removeSession(updateID)
	if session == nil {
		return nil, tabletserver.NewNoSuchScanIDException()
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	session.close()
	result := &data.UpdateErrors{
		FailedExtents:         make(map[*data.TKeyExtent]int64, len(session.failed)),
		ViolationSummaries:    make([]*data.TConstraintViolationSummary, 0, len(session.violations)),
		AuthorizationFailures: make(map[*data.TKeyExtent]clientgen.SecurityErrorCode, len(session.authFailures)),
	}
	for _, failure := range session.failed {
		result.FailedExtents[failure.extent] = failure.committed
	}
	for _, violation := range session.violations {
		copy := *violation
		result.ViolationSummaries = append(result.ViolationSummaries, &copy)
	}
	for _, failure := range session.authFailures {
		result.AuthorizationFailures[failure.extent] = failure.code
	}
	return result, nil
}

func (s *Service) CancelUpdate(
	_ context.Context,
	_ *clientgen.TInfo,
	updateID data.UpdateID,
) (bool, error) {
	session := s.removeSession(updateID)
	if session == nil {
		return false, nil
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	session.close()
	return true, nil
}

func (s *Service) StartConditionalUpdate(
	_ context.Context,
	_ *clientgen.TInfo,
	credentials *security.TCredentials,
	_ [][]byte,
	_ string,
	_ tabletingest.TDurability,
	_ string,
) (*data.TConditionalSession, error) {
	return nil, securityError(credentials, clientgen.SecurityErrorCode_UNSUPPORTED_OPERATION)
}

func (s *Service) ConditionalUpdate(
	_ context.Context,
	_ *clientgen.TInfo,
	_ data.UpdateID,
	_ data.CMBatch,
	_ []string,
) ([]*data.TCMResult_, error) {
	return nil, tabletserver.NewNoSuchScanIDException()
}

func (s *Service) InvalidateConditionalUpdate(context.Context, *clientgen.TInfo, data.UpdateID) error {
	return tabletserver.NewNoSuchScanIDException()
}

func (s *Service) CloseConditionalUpdate(context.Context, *clientgen.TInfo, data.UpdateID) error {
	return ErrConditionalUnsupported
}

func (s *Service) BeginDrain() {
	if !s.accepting.Swap(false) {
		return
	}
	s.mu.Lock()
	sessions := s.sessions
	s.sessions = make(map[data.UpdateID]*updateSession)
	s.mu.Unlock()
	for _, session := range sessions {
		session.cancel()
	}
	go func() {
		var wg sync.WaitGroup
		wg.Add(len(sessions))
		for _, session := range sessions {
			go func(session *updateSession) {
				defer wg.Done()
				session.mu.Lock()
				session.close()
				session.mu.Unlock()
			}(session)
		}
		wg.Wait()
		close(s.drainDone)
	}()
}

func (s *Service) Drain(ctx context.Context) error {
	s.BeginDrain()
	select {
	case <-s.drainDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) Accepting() bool { return s.accepting.Load() }

func (s *Service) Metrics() Metrics {
	s.mu.Lock()
	active := int64(len(s.sessions))
	s.mu.Unlock()
	return Metrics{
		ActiveSessions: active, Started: s.started.Load(),
		AppliedBatches: s.appliedBatches.Load(), AppliedMutations: s.appliedMutations.Load(),
		RejectedBatches: s.rejectedBatches.Load(), RetriedBatches: s.retriedBatches.Load(),
		ExpiredSessions: s.expiredSessions.Load(), Backpressure: s.backpressure.Load(),
	}
}

func (s *Service) session(id data.UpdateID) *updateSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(s.cfg.Now())
	return s.sessions[id]
}

func (s *Service) removeSession(id data.UpdateID) *updateSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.sessions[id]
	delete(s.sessions, id)
	return session
}

func (s *Service) expireLocked(now time.Time) {
	for id, session := range s.sessions {
		session.mu.Lock()
		expired := now.Sub(session.lastUsed) >= s.cfg.SessionTTL
		if expired {
			session.close()
		}
		session.mu.Unlock()
		if expired {
			delete(s.sessions, id)
			s.expiredSessions.Add(1)
		}
	}
}

func (s *updateSession) close() {
	if s.closed {
		return
	}
	s.closed = true
	s.cancel()
	for _, session := range s.tables {
		session.Close()
	}
	if s.credentials != nil {
		for i := range s.credentials.Token {
			s.credentials.Token[i] = 0
		}
		s.credentials.Token = nil
		s.credentials = nil
	}
}

func (s *updateSession) recordFailure(extent *data.TKeyExtent, committed int64) {
	key := thriftExtentKey(extent)
	failure := s.failed[key]
	if failure.extent == nil {
		failure.extent = cloneExtent(extent)
	}
	if committed > failure.committed {
		failure.committed = committed
	}
	s.failed[key] = failure
}

func (s *updateSession) committedPrefix(extent *data.TKeyExtent) int64 {
	return s.committed[thriftExtentKey(extent)]
}

func (s *updateSession) recordAuthorizationFailure(
	extent *data.TKeyExtent,
	code clientgen.SecurityErrorCode,
) {
	s.authFailures[thriftExtentKey(extent)] = authFailure{extent: cloneExtent(extent), code: code}
}

func (s *updateSession) recordViolation(class string, code int16, description string, count int64) {
	key := fmt.Sprintf("%s\x00%d\x00%s", class, code, description)
	summary := s.violations[key]
	if summary == nil {
		summary = &data.TConstraintViolationSummary{
			ConstrainClass: class, ViolationCode: code, ViolationDescription: description,
		}
		s.violations[key] = summary
	}
	summary.NumberOfViolatingMutations += count
}

func supportedDurability(d tabletingest.TDurability) bool {
	switch d {
	case tabletingest.TDurability_DEFAULT, tabletingest.TDurability_SYNC,
		tabletingest.TDurability_FLUSH, tabletingest.TDurability_LOG:
		return true
	default:
		return false
	}
}

func securityError(credentials *security.TCredentials, code clientgen.SecurityErrorCode) error {
	user := ""
	if credentials != nil {
		user = credentials.Principal
	}
	return &clientgen.ThriftSecurityException{User: user, Code: code}
}

func decodeExtent(in *data.TKeyExtent) (ingestrouter.Extent, error) {
	if in == nil {
		return ingestrouter.Extent{}, errors.New("nil extent")
	}
	extent := ingestrouter.Extent{
		TableID: string(in.Table), PrevEndRow: append([]byte(nil), in.PrevEndRow...),
		EndRow: append([]byte(nil), in.EndRow...),
	}
	return extent, extent.Validate()
}

func routerMutation(in *cclient.Mutation) ingestrouter.Mutation {
	out := ingestrouter.Mutation{Row: append([]byte(nil), in.Row()...)}
	for _, entry := range in.Entries() {
		timestamp := ingestrouter.Timestamp{
			Set: entry.HasTimestamp || entry.Timestamp != cclient.MutationLatestTimestamp,
		}
		if timestamp.Set {
			timestamp.Value = entry.Timestamp
		}
		out.Updates = append(out.Updates, ingestrouter.Update{
			ColumnFamily:     append([]byte(nil), entry.ColFamily...),
			ColumnQualifier:  append([]byte(nil), entry.ColQualifier...),
			ColumnVisibility: append([]byte(nil), entry.ColVisibility...),
			Timestamp:        timestamp, Value: append([]byte(nil), entry.Value...), Delete: entry.Deleted,
		})
	}
	return out
}

func thriftBatchSize(extent *data.TKeyExtent, mutations []*data.TMutation) int64 {
	var size int64
	if extent != nil {
		size += int64(len(extent.Table) + len(extent.PrevEndRow) + len(extent.EndRow))
	}
	for _, mutation := range mutations {
		if mutation == nil {
			continue
		}
		size += int64(len(mutation.Row) + len(mutation.Data))
		for _, value := range mutation.Values {
			size += int64(len(value))
		}
	}
	return size
}

func thriftExtentKey(extent *data.TKeyExtent) string {
	if extent == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d:%s/%x/%x", len(extent.Table), extent.Table, extent.PrevEndRow, extent.EndRow)
}

func cloneExtent(extent *data.TKeyExtent) *data.TKeyExtent {
	if extent == nil {
		return &data.TKeyExtent{}
	}
	return &data.TKeyExtent{
		Table: append([]byte(nil), extent.Table...), EndRow: append([]byte(nil), extent.EndRow...),
		PrevEndRow: append([]byte(nil), extent.PrevEndRow...),
	}
}

func cloneCredentials(in *security.TCredentials) *security.TCredentials {
	if in == nil {
		return nil
	}
	out := *in
	out.Token = append([]byte(nil), in.Token...)
	return &out
}

var _ tabletingest.TabletIngestClientService = (*Service)(nil)
