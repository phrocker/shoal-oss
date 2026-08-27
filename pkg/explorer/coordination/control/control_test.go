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
 */

package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/allocator"
)

type fault uint8

const (
	noFault fault = iota
	unknownBefore
	unknownAfter
	partial
)

type stored struct {
	cell    allocator.Cell
	deleted bool
}
type memoryStore struct {
	mu     sync.Mutex
	cells  map[string][]stored
	faults []fault
}

func newStore() *memoryStore { return &memoryStore{cells: map[string][]stored{}} }
func key(c allocator.Coordinate) string {
	return string(c.Row) + "\x00" + string(c.Family) + "\x00" + string(c.Qualifier) + "\x00" + string(c.Visibility)
}
func cloneCoordinate(c allocator.Coordinate) allocator.Coordinate {
	return allocator.Coordinate{Row: append([]byte(nil), c.Row...), Family: append([]byte(nil), c.Family...), Qualifier: append([]byte(nil), c.Qualifier...), Visibility: append([]byte(nil), c.Visibility...)}
}
func cloneCell(c allocator.Cell) allocator.Cell {
	return allocator.Cell{Coordinate: cloneCoordinate(c.Coordinate), Value: append([]byte(nil), c.Value...), Timestamp: c.Timestamp}
}
func (s *memoryStore) visible(c allocator.Coordinate) (allocator.Cell, bool) {
	values := s.cells[key(c)]
	if len(values) == 0 {
		return allocator.Cell{}, false
	}
	winner := values[0]
	for _, v := range values[1:] {
		if v.cell.Timestamp > winner.cell.Timestamp || v.cell.Timestamp == winner.cell.Timestamp && v.deleted {
			winner = v
		}
	}
	if winner.deleted {
		return allocator.Cell{}, false
	}
	return winner.cell, true
}
func (s *memoryStore) put(u allocator.Update) {
	values := s.cells[key(u.Coordinate)]
	entry := stored{cell: allocator.Cell{Coordinate: cloneCoordinate(u.Coordinate), Value: append([]byte(nil), u.Value...), Timestamp: u.Timestamp}, deleted: u.Delete}
	for i := range values {
		if values[i].cell.Timestamp == u.Timestamp {
			if !bytes.Equal(values[i].cell.Value, u.Value) || values[i].deleted != u.Delete {
				panic("same timestamp differing value")
			}
			return
		}
	}
	s.cells[key(u.Coordinate)] = append(values, entry)
}
func (s *memoryStore) ReadExact(ctx context.Context, coordinates []allocator.Coordinate) ([]allocator.Cell, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []allocator.Cell
	for _, c := range coordinates {
		if v, ok := s.visible(c); ok {
			result = append(result, cloneCell(v))
		}
	}
	return result, nil
}
func (s *memoryStore) ScanPrefix(ctx context.Context, prefix, family, qualifier, visibility []byte, limit int) ([]allocator.Cell, error) {
	return s.ScanPrefixFrom(ctx, prefix, prefix, family, qualifier, visibility, limit)
}
func (s *memoryStore) ScanPrefixFrom(ctx context.Context, prefix, start, family, qualifier, visibility []byte, limit int) ([]allocator.Cell, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []allocator.Cell
	for _, values := range s.cells {
		if len(values) == 0 {
			continue
		}
		c, ok := s.visible(values[0].cell.Coordinate)
		if !ok {
			continue
		}
		if bytes.HasPrefix(c.Coordinate.Row, prefix) && bytes.Compare(c.Coordinate.Row, start) >= 0 && bytes.Equal(c.Coordinate.Family, family) && bytes.Equal(c.Coordinate.Qualifier, qualifier) && bytes.Equal(c.Coordinate.Visibility, visibility) {
			result = append(result, cloneCell(c))
		}
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i].Coordinate.Row, result[j].Coordinate.Row) < 0 })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}
func (s *memoryStore) CompareAndMutate(ctx context.Context, m allocator.Mutation) (allocator.Status, error) {
	if err := ctx.Err(); err != nil {
		return allocator.StatusUnknown, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := noFault
	if len(s.faults) > 0 {
		f = s.faults[0]
		s.faults = s.faults[1:]
	}
	if f == unknownBefore {
		return allocator.StatusUnknown, allocator.ErrConditionalUnknown
	}
	for _, condition := range m.Conditions {
		cell, ok := s.visible(condition.Coordinate)
		if condition.Absent {
			if ok {
				return allocator.StatusRejected, nil
			}
		} else if !ok || !bytes.Equal(cell.Value, condition.Value) || condition.TimestampSet && cell.Timestamp != condition.Timestamp {
			return allocator.StatusRejected, nil
		}
	}
	updates := m.Updates
	if f == partial {
		updates = updates[:1]
	}
	for _, u := range updates {
		s.put(u)
	}
	if f == unknownAfter || f == partial {
		return allocator.StatusUnknown, allocator.ErrConditionalUnknown
	}
	return allocator.StatusAccepted, nil
}

type pins struct{ err error }

func (p pins) VerifySnapshotPins(context.Context, coordination.DomainID, coordination.SnapshotLeaseV2) error {
	return p.err
}

type leases struct{ block bool }

func (l leases) NoPinsBelow(context.Context, coordination.DomainID, coordination.Epoch, time.Time) error {
	if l.block {
		return ErrLeaseActive
	}
	return nil
}

type historyVerifier struct{}

func (historyVerifier) VerifyHistoryFloor(context.Context, coordination.DomainID, coordination.HistoryFloorV1, coordination.Epoch, coordination.Digest) error {
	return nil
}

func (l leases) SelectsObject(context.Context, coordination.DomainID, coordination.EntityKind, coordination.EntityID, coordination.Generation, time.Time) (bool, error) {
	return l.block, nil
}

type retireVerifier struct{}

func (retireVerifier) VerifyRetirement(context.Context, coordination.DomainID, coordination.RetirementDecisionV1) error {
	return nil
}

type fixedIDs struct {
	lease coordination.LeaseID
	term  coordination.AuthorityTerm
}

func (f fixedIDs) NewLeaseID(context.Context, coordination.DomainID, coordination.OwnerID) (coordination.LeaseID, error) {
	return append(coordination.LeaseID(nil), f.lease...), nil
}
func (f fixedIDs) NewAuthorityTerm(context.Context, coordination.DomainID, coordination.OwnerID) (coordination.AuthorityTerm, error) {
	return append(coordination.AuthorityTerm(nil), f.term...), nil
}

type route struct {
	mu         sync.Mutex
	open       bool
	mode       coordination.WriterMode
	generation coordination.Generation
	fence      coordination.Fence
}

func (r *route) Close(context.Context, coordination.DomainID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.open = false
	return nil
}
func (r *route) Open(_ context.Context, _ coordination.DomainID, m coordination.WriterMode, g coordination.Generation, f coordination.Fence) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mode = m
	r.generation = g
	r.fence = f
	r.open = true
	return nil
}
func (r *route) Current(context.Context, coordination.DomainID) (coordination.WriterMode, coordination.Generation, coordination.Fence, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mode, r.generation, r.fence, r.open, nil
}

type migration struct{}

func (migration) DrainAndVerify(context.Context, coordination.DomainID, coordination.WriterMode, coordination.Generation) error {
	return nil
}

func digest(s string) coordination.Digest { return coordination.Sum([]byte(s)) }
func baseHead(now time.Time) coordination.AllocatorHeadV1 {
	return coordination.AllocatorHeadV1{
		HeadGeneration: 1, NextEpoch: 11, RetiredThrough: 10, Frontier: 10, VisibleAt: now, CheckpointDigest: digest("checkpoint"),
		HistoryFloor: 1, RetentionGeneration: 1, WriterAuthorityGeneration: 1, WriterMode: coordination.WriterModeEmbeddedPrimary,
		WriterHolder: coordination.OwnerID("writer"), WriterFence: 1, MaxActiveReservations: 10,
	}
}
func seedHead(t *testing.T, s *memoryStore, head coordination.AllocatorHeadV1) {
	t.Helper()
	row, err := coordination.AllocatorRow(coordination.DomainID("domain"))
	if err != nil {
		t.Fatal(err)
	}
	value, err := coordination.MarshalAllocatorHeadV1(head)
	if err != nil {
		t.Fatal(err)
	}
	s.put(allocator.Update{Coordinate: allocator.Coordinate{Row: row, Family: familyHead, Qualifier: qualifierHead, Visibility: []byte("svc")}, Value: value, Timestamp: int64(head.HeadGeneration)})
}
func newClient(t *testing.T, s *memoryStore, now time.Time, l leases) (*Client, *route) {
	t.Helper()
	r := &route{}
	ids := fixedIDs{lease: coordination.LeaseID("lease-1"), term: coordination.AuthorityTerm("term-1")}
	c, err := New(Config{Domain: coordination.DomainID("domain"), ControlVisibility: []byte("svc"), Store: s, Pins: pins{}, Leases: l, History: historyVerifier{}, Retirements: retireVerifier{}, EmbeddedBackend: coordination.BackendID("embedded"), AccumuloBackend: coordination.BackendID("accumulo"), Clock: func() time.Time { return now }, LeaseIDs: ids, Terms: ids, Route: r, Migration: migration{}, RetryBackoff: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	return c, r
}
func initialize(t *testing.T, c *Client, now time.Time) Authority {
	t.Helper()
	a, err := c.InitializeAuthority(context.Background(), AuthorityRequest{Owner: coordination.OwnerID("writer"), Mode: coordination.WriterModeEmbeddedPrimary, LeaseUntil: now.Add(time.Hour), Now: now, Term: coordination.AuthorityTerm("term-1")})
	if err != nil {
		t.Fatal(err)
	}
	head, err := c.currentHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.InitializeHistoryFloor(context.Background(), head.HistoryFloor, now); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestLeaseLifecycleUnknownAndBounds(t *testing.T) {
	now := time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC)
	store := newStore()
	seedHead(t, store, baseHead(now))
	client, _ := newClient(t, store, now, leases{})
	initialize(t, client, now)
	request := CreateLeaseRequest{Owner: coordination.OwnerID("reader"), Fence: 1, Frontier: 10, AuthorityGeneration: 1, RetentionGeneration: 1, PolicyGeneration: 1, PolicyCopyPinDigest: digest("pins"), IndexPins: []coordination.IndexPin{{Family: coordination.Family("lexical"), IGEN: coordination.IGEN("i1")}}, Now: now, ExpiresAt: now.Add(time.Minute)}
	store.faults = []fault{unknownAfter}
	lease, err := client.CreateLease(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if lease.RecordGeneration != 1 || string(lease.Record.LeaseID) != "lease-1" {
		t.Fatalf("lease=%+v", lease)
	}
	if _, err = client.RenewLease(context.Background(), RenewLeaseRequest{LeaseID: lease.Record.LeaseID, Owner: lease.Record.Owner, Fence: 1, RecordGeneration: 1, AuthorityGeneration: 1, RetentionGeneration: 1, Now: now.Add(time.Second), ExpiresAt: now.Add(2 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RenewLease(context.Background(), RenewLeaseRequest{LeaseID: lease.Record.LeaseID, Owner: coordination.OwnerID("stale"), Fence: 1, RecordGeneration: 2, AuthorityGeneration: 1, RetentionGeneration: 1, Now: now.Add(2 * time.Second), ExpiresAt: now.Add(3 * time.Minute)}); !errors.Is(err, ErrStaleOwner) {
		t.Fatalf("stale renew=%v", err)
	}
	active, _, err := client.ListLeases(context.Background(), now, LeaseCursor{}, 10, true)
	if err != nil || len(active) != 1 {
		t.Fatalf("active=%d %v", len(active), err)
	}
	if _, err = client.ExpireLease(context.Background(), lease.Record.LeaseID, now.Add(time.Minute)); !errors.Is(err, ErrLeaseActive) {
		t.Fatalf("early expire=%v", err)
	}
	expired, err := client.ExpireLease(context.Background(), lease.Record.LeaseID, now.Add(3*time.Minute))
	if err != nil || expired.Record.State != coordination.LeaseStateExpired {
		t.Fatalf("expire=%+v %v", expired, err)
	}
	if _, err = client.ReleaseLease(context.Background(), ReleaseLeaseRequest{LeaseID: lease.Record.LeaseID, Owner: lease.Record.Owner, Fence: 1, RecordGeneration: expired.RecordGeneration, Now: now.Add(4 * time.Minute)}); !errors.Is(err, ErrExpired) {
		t.Fatalf("terminal release=%v", err)
	}
}

func TestExpiredLeaseTakeoverKeepsPins(t *testing.T) {
	now := time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC)
	store := newStore()
	seedHead(t, store, baseHead(now))
	client, _ := newClient(t, store, now, leases{})
	initialize(t, client, now)
	created, err := client.CreateLease(context.Background(), CreateLeaseRequest{
		Owner: coordination.OwnerID("old"), Fence: 1, Frontier: 10,
		AuthorityGeneration: 1, RetentionGeneration: 1, PolicyGeneration: 1,
		PolicyCopyPinDigest: digest("pins"), Now: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.TakeoverExpiredLease(context.Background(), TakeoverLeaseRequest{
		LeaseID: created.Record.LeaseID, PreviousGeneration: 1, Owner: coordination.OwnerID("new"),
		Fence: 2, AuthorityGeneration: 1, RetentionGeneration: 1,
		Now: now.Add(30 * time.Second), ExpiresAt: now.Add(2 * time.Minute),
	}); !errors.Is(err, ErrLeaseActive) {
		t.Fatalf("early takeover=%v", err)
	}
	taken, err := client.TakeoverExpiredLease(context.Background(), TakeoverLeaseRequest{
		LeaseID: created.Record.LeaseID, PreviousGeneration: 1, Owner: coordination.OwnerID("new"),
		Fence: 2, AuthorityGeneration: 1, RetentionGeneration: 1,
		Now: now.Add(time.Minute), ExpiresAt: now.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(taken.Record.Owner) != "new" || taken.Record.Frontier != created.Record.Frontier ||
		taken.Record.PolicyCopyPinDigest != created.Record.PolicyCopyPinDigest {
		t.Fatalf("takeover changed pins: %+v", taken)
	}
}

func TestHistoryFloorAtomicAdvanceAndCorruption(t *testing.T) {
	now := time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC)
	store := newStore()
	seedHead(t, store, baseHead(now))
	client, _ := newClient(t, store, now, leases{})
	initialize(t, client, now)
	floor, _, err := client.CurrentHistoryFloor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	store.faults = []fault{unknownAfter}
	next, err := client.AdvanceHistoryFloor(context.Background(), floor, 5, digest("proof"), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if next.Floor != 5 || next.RetentionGeneration != 2 {
		t.Fatalf("floor=%+v", next)
	}
	if _, err = client.AdvanceHistoryFloor(context.Background(), next, 11, digest("proof"), now.Add(2*time.Second)); !errors.Is(err, ErrBounds) {
		t.Fatalf("frontier bound=%v", err)
	}
	blocked, _ := newClient(t, store, now, leases{block: true})
	if _, err = blocked.AdvanceHistoryFloor(context.Background(), next, 6, digest("proof"), now.Add(2*time.Second)); !errors.Is(err, ErrLeaseActive) {
		t.Fatalf("lease block=%v", err)
	}
	store.faults = []fault{partial}
	if _, err = client.AdvanceHistoryFloor(context.Background(), next, 6, digest("proof"), now.Add(2*time.Second)); !errors.Is(err, ErrCorruption) {
		t.Fatalf("partial=%v", err)
	}
}

func TestAuthorityMirrorsAndBarrier(t *testing.T) {
	now := time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC)
	store := newStore()
	seedHead(t, store, baseHead(now))
	client, r := newClient(t, store, now, leases{})
	a := initialize(t, client, now)
	renewed, err := client.RenewAuthority(context.Background(), AuthorityTransition{Owner: a.Record.Owner, Term: a.Record.Term, Generation: a.Record.Generation, Fence: a.Record.Fence, Mode: a.Mode, LeaseUntil: now.Add(2 * time.Hour), Now: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if renewed.Record.Generation != 2 || renewed.Record.Fence != 1 || renewed.Record.PredecessorDigest != a.Record.Digest {
		t.Fatalf("renewed=%+v", renewed)
	}
	for _, backend := range []coordination.BackendID{coordination.BackendID("embedded"), coordination.BackendID("accumulo")} {
		state := coordination.BackendReplica
		if string(backend) == "embedded" {
			state = coordination.BackendPrimary
		}
		obs := coordination.BackendObservationV1{Backend: backend, AuthorityGeneration: 2, AuthorityFence: 1, ObservedFrontier: 10, State: state, ObservedDigest: renewed.Record.Digest, ObservedAt: now.Add(time.Second)}
		if _, err = client.PublishObservation(context.Background(), Observation{Record: obs, Mode: renewed.Mode}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = client.RoutingBarrier(context.Background(), now.Add(time.Second)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("closed route=%v", err)
	}
	if err = r.Open(context.Background(), coordination.DomainID("domain"), renewed.Mode, 2, 1); err != nil {
		t.Fatal(err)
	}
	decision, err := client.RoutingBarrier(context.Background(), now.Add(time.Second))
	if err != nil || !decision.Enabled {
		t.Fatalf("barrier=%+v %v", decision, err)
	}
	allocatorAuthority, err := (AuthoritySource{Client: client}).AllocatorAuthority(context.Background(), coordination.DomainID("domain"))
	if err != nil || allocatorAuthority.Generation != renewed.Record.Generation ||
		allocatorAuthority.Fence != renewed.Record.Fence {
		t.Fatalf("allocator authority=%+v %v", allocatorAuthority, err)
	}
	stale := decision.Accumulo
	stale.Record.AuthorityGeneration = 1
	stale.Record.ObservedDigest = digest("stale")
	stale.RecordGeneration++
	if _, err = client.PublishObservation(context.Background(), stale); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("stale mirror=%v", err)
	}
	revoked, err := client.RevokeAuthority(context.Background(), AuthorityTransition{Owner: renewed.Record.Owner, Term: renewed.Record.Term, Generation: renewed.Record.Generation, Fence: renewed.Record.Fence, Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if revoked.Record.Generation != 3 || revoked.Record.State != coordination.AuthorityRevoked {
		t.Fatalf("revoked=%+v", revoked)
	}
	client.terms = fixedIDs{term: coordination.AuthorityTerm("term-2")}
	acquired, err := client.AcquireAuthority(context.Background(), AuthorityRequest{Owner: coordination.OwnerID("next"), Mode: coordination.WriterModeAccumuloPrimary, LeaseUntil: now.Add(3 * time.Hour), Now: now.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if acquired.Record.Generation != 4 || acquired.Record.Fence != 2 || string(acquired.Record.Term) != "term-2" {
		t.Fatalf("acquired=%+v", acquired)
	}
}

func TestRetirementApprovalAndApplication(t *testing.T) {
	now := time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC)
	store := newStore()
	head := baseHead(now)
	head.HistoryFloor = 5
	seedHead(t, store, head)
	client, _ := newClient(t, store, now, leases{})
	initialize(t, client, now)
	decision := coordination.RetirementDecisionV1{ObjectKind: coordination.EntityKind("D"), ObjectID: coordination.EntityID("doc"), ObjectGeneration: 1, SafeAfterFrontier: 5, SafeAfterTime: now, HistoryFloor: 5, ProofDigest: digest("proof"), AuthorityGeneration: 1, State: coordination.RetirementCandidate}
	value, err := client.PublishRetirementCandidate(context.Background(), RetirementRequest{Decision: decision, Owner: coordination.OwnerID("gc"), Fence: 1, RetentionGeneration: 1, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	approved, err := client.ApproveRetirement(context.Background(), RetirementTransition{Kind: 'D', ID: decision.ObjectID, Owner: value.Owner, Fence: 1, RecordGeneration: 1, AuthorityGeneration: 1, RetentionGeneration: 1, HistoryFloor: 5, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	floor, _, _ := client.CurrentHistoryFloor(context.Background())
	if _, err = client.AdvanceHistoryFloor(context.Background(), floor, 6, digest("advance"), now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	applied, err := client.ApplyRetirement(context.Background(), RetirementTransition{Kind: 'D', ID: decision.ObjectID, Owner: value.Owner, Fence: 1, RecordGeneration: approved.RecordGeneration, AuthorityGeneration: 1, RetentionGeneration: 2, HistoryFloor: 6, Now: now.Add(3 * time.Second)}, false)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Decision.State != coordination.RetirementApplied || applied.RecordGeneration != 3 {
		t.Fatalf("applied=%+v", applied)
	}
}

func leaseRowsInBand(t *testing.T, count int) []coordination.LeaseID {
	t.Helper()
	domain := coordination.DomainID("domain")
	byBand := make(map[byte][]coordination.LeaseID)
	for index := 0; index < 100_000; index++ {
		id := coordination.LeaseID(fmt.Sprintf("lease-%06d", index))
		band := coordination.B8('L', domain, id)
		byBand[band] = append(byBand[band], id)
		if len(byBand[band]) == count {
			ids := byBand[band]
			sort.Slice(ids, func(i, j int) bool {
				left, _ := coordination.SnapshotLeaseRow(domain, ids[i])
				right, _ := coordination.SnapshotLeaseRow(domain, ids[j])
				return bytes.Compare(left, right) < 0
			})
			return ids
		}
	}
	t.Fatal("could not find enough lease IDs in one band")
	return nil
}

func retirementRowsInBand(t *testing.T, count int) []coordination.EntityID {
	t.Helper()
	domain := coordination.DomainID("domain")
	byBand := make(map[byte][]coordination.EntityID)
	for index := 0; index < 100_000; index++ {
		id := coordination.EntityID(fmt.Sprintf("object-%06d", index))
		band := coordination.B8('X', domain, []byte{'D'}, id)
		byBand[band] = append(byBand[band], id)
		if len(byBand[band]) == count {
			ids := byBand[band]
			sort.Slice(ids, func(i, j int) bool {
				left, _ := coordination.RetirementRow(domain, 'D', ids[i])
				right, _ := coordination.RetirementRow(domain, 'D', ids[j])
				return bytes.Compare(left, right) < 0
			})
			return ids
		}
	}
	t.Fatal("could not find enough retirement IDs in one band")
	return nil
}

func seedLease(t *testing.T, store *memoryStore, client *Client, now time.Time, id coordination.LeaseID, state coordination.LeaseState, pin coordination.Digest) {
	t.Helper()
	value := Lease{
		Record: coordination.SnapshotLeaseV2{
			LeaseID: id, Frontier: 10, Owner: coordination.OwnerID("reader"), Fence: 1,
			AuthorityGeneration: 1, RetentionGeneration: 1, PolicyGeneration: 1,
			PolicyCopyPinDigest: pin, CreatedAt: now, RenewedAt: now,
			ExpiresAt: now.Add(time.Hour), State: state,
		},
		RecordGeneration: 1, UpdatedAt: now,
	}
	encoded, err := marshalLease(value)
	if err != nil {
		t.Fatal(err)
	}
	coordinate, err := client.leaseCoordinate(id)
	if err != nil {
		t.Fatal(err)
	}
	store.put(allocator.Update{Coordinate: coordinate, Value: encoded, Timestamp: 1})
}

func seedRetirement(t *testing.T, store *memoryStore, client *Client, now time.Time, id coordination.EntityID, state coordination.RetirementState) {
	t.Helper()
	value := Retirement{
		Decision: coordination.RetirementDecisionV1{
			ObjectKind: coordination.EntityKind("D"), ObjectID: id, ObjectGeneration: 1,
			SafeAfterFrontier: 5, SafeAfterTime: now, HistoryFloor: 1,
			ProofDigest: digest("proof"), AuthorityGeneration: 1, State: state,
		},
		Owner: coordination.OwnerID("gc"), Fence: 1, RetentionGeneration: 1,
		RecordGeneration: 1, UpdatedAt: now,
	}
	encoded, err := marshalRetirement(value)
	if err != nil {
		t.Fatal(err)
	}
	coordinate, err := client.retirementCoordinate('D', id)
	if err != nil {
		t.Fatal(err)
	}
	store.put(allocator.Update{Coordinate: coordinate, Value: encoded, Timestamp: 1})
}

func TestLeasePaginationStaysInBandAcrossFilteredPages(t *testing.T) {
	now := time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC)
	store := newStore()
	seedHead(t, store, baseHead(now))
	client, _ := newClient(t, store, now, leases{})
	initialize(t, client, now)
	ids := leaseRowsInBand(t, 7)
	for index, id := range ids {
		state := coordination.LeaseStateReleased
		if index >= 4 {
			state = coordination.LeaseStateActive
		}
		seedLease(t, store, client, now, id, state, digest("pins"))
	}
	var cursor LeaseCursor
	seen := make(map[string]int)
	for {
		values, next, err := client.ListLeases(context.Background(), now, cursor, 2, true)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range values {
			seen[string(value.Record.LeaseID)]++
		}
		if len(next.Row) == 0 {
			break
		}
		cursor = next
	}
	if len(seen) != 3 {
		t.Fatalf("saw %d active leases, want 3: %#v", len(seen), seen)
	}
	for _, id := range ids[4:] {
		if seen[string(id)] != 1 {
			t.Fatalf("lease %q appeared %d times", id, seen[string(id)])
		}
	}
}

func TestRetirementPaginationStaysInBandAcrossFilteredPages(t *testing.T) {
	now := time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC)
	store := newStore()
	seedHead(t, store, baseHead(now))
	client, _ := newClient(t, store, now, leases{})
	initialize(t, client, now)
	ids := retirementRowsInBand(t, 7)
	for index, id := range ids {
		state := coordination.RetirementApplied
		if index >= 4 {
			state = coordination.RetirementCandidate
		}
		seedRetirement(t, store, client, now, id, state)
	}
	var cursor []byte
	seen := make(map[string]int)
	for {
		values, next, err := client.ListPendingRetirements(context.Background(), cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range values {
			seen[string(value.Decision.ObjectID)]++
		}
		if len(next) == 0 {
			break
		}
		cursor = next
	}
	if len(seen) != 3 {
		t.Fatalf("saw %d pending retirements, want 3: %#v", len(seen), seen)
	}
	for _, id := range ids[4:] {
		if seen[string(id)] != 1 {
			t.Fatalf("retirement %q appeared %d times", id, seen[string(id)])
		}
	}
}

func TestLeaseSafetyQueryFailsClosedAtScanBound(t *testing.T) {
	now := time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC)
	store := newStore()
	seedHead(t, store, baseHead(now))
	client, _ := newClient(t, store, now, leases{})
	initialize(t, client, now)
	client.maxScan = 3
	ids := leaseRowsInBand(t, 4)
	for index, id := range ids {
		state := coordination.LeaseStateActive
		pin := digest("other")
		if index == len(ids)-1 {
			pin = digest("target")
		}
		seedLease(t, store, client, now, id, state, pin)
	}
	selected, err := (LeaseSource{Client: client}).SelectsPolicyCopy(
		context.Background(), coordination.DomainID("domain"), coordination.LPART("part"), 1, digest("target"),
	)
	if selected || !errors.Is(err, ErrBounds) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("safety query = %v, %v; want fail-closed bounds", selected, err)
	}
	selected, err = (LeaseSource{Client: client}).SelectsIndexGeneration(
		context.Background(), coordination.DomainID("domain"), coordination.Family("lexical"), coordination.IGEN("target"),
	)
	if selected || !errors.Is(err, ErrBounds) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("index safety query = %v, %v; want fail-closed bounds", selected, err)
	}
}
