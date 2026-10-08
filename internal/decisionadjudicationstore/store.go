// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
// Package decisionadjudicationstore persists bounded, append-only adjudication
// histories. Its caller MUST derive Scope and Attribution from trusted identity
// and authorize the complete target history. This package supplies no authority.
package decisionadjudicationstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/internal/decisionstore"
	"github.com/phrocker/shoal-oss/pkg/decision"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/allocator"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const Table = "_shoal_decision_adjudications"
const MaxEntries = 128
const MaxStoredBytes = 8 << 20

var (
	ErrConflict      = errors.New("adjudication journal conflict")
	ErrIndeterminate = errors.New("adjudication append indeterminate")
	ErrCorrupt       = errors.New("invalid adjudication journal")
	ErrUnavailable   = errors.New("adjudication journal unavailable")
	ErrLimit         = errors.New("adjudication journal bound reached")
)

type Scope struct{ Domain []byte }
type Attribution struct {
	SubjectID, ActorID, ClientID shoal.ID
	OnBehalfOf                   []shoal.ID
	AuthorizationFingerprint     string
}
type Receipt struct {
	ID                                                shoal.ID
	Version                                           int64
	TargetID, TaskID, PictureID, PolicyID, ProposalID shoal.ID
	ProposalConfig                                    decision.AdjudicationProposalConfig
	Adjudicator                                       Attribution
	ReceivedAt                                        time.Time
}
type Config struct {
	Backend    decisionstore.CAS
	Clock      func() time.Time
	Visibility []byte
}
type Store struct{ config Config }
type entry struct {
	Receipt                   Receipt
	KeyDigest, IdentityDigest string
}
type journal struct {
	ScopeDigest string
	TargetID    shoal.ID
	Entries     []entry
}

func New(c Config) (*Store, error) {
	if absent(c.Backend) || c.Clock == nil || len(c.Visibility) > 4096 {
		return nil, invalid()
	}
	c.Visibility = append([]byte(nil), c.Visibility...)
	return &Store{c}, nil
}
func absent(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Map, reflect.Slice, reflect.Interface, reflect.Chan:
		return r.IsNil()
	}
	return false
}
func invalid() error {
	return shoal.NewError(shoal.ErrorInvalidArgument, "invalid adjudication journal request")
}
func hash(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func validHash(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func digestParts(parts ...string) string { b, _ := json.Marshal(parts); return hash(b) }
func scopeDigest(scope Scope) (string, error) {
	if len(scope.Domain) == 0 || len(scope.Domain) > auth.MaxPolicyComponentBytes {
		return "", invalid()
	}
	return hash(scope.Domain), nil
}
func validAttribution(a Attribution) bool {
	if shoal.ValidateRequiredID("subject", a.SubjectID) != nil || shoal.ValidateRequiredID("actor", a.ActorID) != nil || shoal.ValidateOptionalID("client", a.ClientID) != nil || len(a.OnBehalfOf) > auth.MaxOnBehalfOfEntries || !strings.HasPrefix(a.AuthorizationFingerprint, "auth-sha256:") || !validHash(strings.TrimPrefix(a.AuthorizationFingerprint, "auth-sha256:")) {
		return false
	}
	for _, id := range a.OnBehalfOf {
		if shoal.ValidateRequiredID("delegate", id) != nil {
			return false
		}
	}
	return true
}
func identityDigest(a Attribution) string {
	w := wireAttribution(a)
	w.AuthorizationFingerprint = ""
	if len(w.OnBehalfOf) == 0 {
		w.OnBehalfOf = nil
	}
	b, _ := json.Marshal(w)
	return hash(b)
}
func receiptID(scope string, target shoal.ID, identity, key string) shoal.ID {
	return shoal.ID("adjudication-receipt:" + digestParts("adjudication-receipt-v1", scope, string(target), identity, key))
}
func (s *Store) coordinate(scope string, target shoal.ID) allocator.Coordinate {
	return allocator.Coordinate{Row: []byte("adjudication-journal:" + digestParts("adjudication-journal-v1", scope, string(target))), Family: []byte("j"), Qualifier: []byte("history"), Visibility: append([]byte(nil), s.config.Visibility...)}
}
func equalCoordinate(a, b allocator.Coordinate) bool {
	return bytes.Equal(a.Row, b.Row) && bytes.Equal(a.Family, b.Family) && bytes.Equal(a.Qualifier, b.Qualifier) && bytes.Equal(a.Visibility, b.Visibility)
}
func validTime(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 && t == t.Round(0).UTC()
}
func (s *Store) now() (time.Time, error) {
	now := s.config.Clock().Round(0).UTC()
	if !validTime(now) {
		return time.Time{}, ErrUnavailable
	}
	return now, nil
}
func (s *Store) read(ctx context.Context, scope string, target shoal.ID) (journal, []byte, error) {
	empty := journal{ScopeDigest: scope, TargetID: target}
	if ctx.Err() != nil {
		return empty, nil, ErrUnavailable
	}
	coord := s.coordinate(scope, target)
	cells, e := s.config.Backend.ReadExact(ctx, []allocator.Coordinate{coord})
	if e != nil || ctx.Err() != nil {
		return empty, nil, ErrUnavailable
	}
	if len(cells) == 0 {
		return empty, nil, nil
	}
	if len(cells) != 1 || !equalCoordinate(cells[0].Coordinate, coord) || len(cells[0].Value) > MaxStoredBytes {
		return empty, nil, ErrCorrupt
	}
	raw := append([]byte(nil), cells[0].Value...)
	j, e := decode(raw)
	if e != nil || j.ScopeDigest != scope || j.TargetID != target || cells[0].Timestamp != int64(len(j.Entries)) {
		return empty, nil, ErrCorrupt
	}
	return j, raw, nil
}
func (s *Store) History(ctx context.Context, scope Scope, targetID shoal.ID) ([]Receipt, error) {
	sd, e := scopeDigest(scope)
	if e != nil {
		return nil, e
	}
	if !textID(targetID) {
		return nil, invalid()
	}
	j, _, e := s.read(ctx, sd, targetID)
	if e != nil {
		return nil, e
	}
	result := make([]Receipt, len(j.Entries))
	for i, v := range j.Entries {
		result[i] = v.Receipt
	}
	return result, nil
}
func lookup(j journal, id shoal.ID, proposal shoal.ID) (Receipt, bool, error) {
	for _, e := range j.Entries {
		if e.Receipt.ID == id {
			if e.Receipt.ProposalID != proposal {
				return Receipt{}, true, ErrConflict
			}
			return e.Receipt, true, nil
		}
	}
	return Receipt{}, false, nil
}

// Append attempts one CAS only. Exact retries replay the original server time
// and attribution even when later adjudications have advanced the target head.
func (s *Store) Append(ctx context.Context, scope Scope, key []byte, proposal decision.AdjudicationProposal, attribution Attribution) (Receipt, error) {
	if len(key) == 0 || len(key) > shoal.MaxIDBytes || proposal.Validate() != nil || !validAttribution(attribution) {
		return Receipt{}, invalid()
	}
	sd, e := scopeDigest(scope)
	if e != nil {
		return Receipt{}, e
	}
	identity := identityDigest(attribution)
	kd := hash(key)
	id := receiptID(sd, proposal.TargetID(), identity, kd)
	j, old, e := s.read(ctx, sd, proposal.TargetID())
	if e != nil {
		return Receipt{}, e
	}
	if r, found, e := lookup(j, id, proposal.ID()); found {
		return r, e
	}
	cfg := proposal.Config()
	var head shoal.ID
	if len(j.Entries) > 0 {
		head = j.Entries[len(j.Entries)-1].Receipt.ID
	}
	if cfg.ExpectedHeadID != head || cfg.ExpectedVersion != int64(len(j.Entries)) {
		return Receipt{}, ErrConflict
	}
	if len(j.Entries) >= MaxEntries {
		return Receipt{}, ErrLimit
	}
	now, e := s.now()
	if e != nil {
		return Receipt{}, e
	}
	if len(j.Entries) > 0 && now.Before(j.Entries[len(j.Entries)-1].Receipt.ReceivedAt) {
		return Receipt{}, ErrUnavailable
	}
	receipt := Receipt{ID: id, Version: int64(len(j.Entries) + 1), TargetID: proposal.TargetID(), TaskID: proposal.TaskID(), PictureID: proposal.PictureID(), PolicyID: proposal.PolicyID(), ProposalID: proposal.ID(), ProposalConfig: cfg, Adjudicator: attribution, ReceivedAt: now}
	j.Entries = append(j.Entries, entry{receipt, kd, identity})
	encoded, e := encode(j)
	if e != nil {
		return Receipt{}, e
	}
	if e = validateJournal(j); e != nil {
		return Receipt{}, ErrCorrupt
	}
	if ctx.Err() != nil {
		return Receipt{}, ErrUnavailable
	}
	coord := s.coordinate(sd, proposal.TargetID())
	condition := allocator.Condition{Coordinate: coord, Absent: old == nil}
	if old != nil {
		condition.Value = old
		condition.TimestampSet = true
		condition.Timestamp = receipt.Version - 1
	}
	status, writeErr := s.config.Backend.CompareAndMutate(ctx, allocator.Mutation{Row: coord.Row, Conditions: []allocator.Condition{condition}, Updates: []allocator.Update{{Coordinate: coord, Timestamp: receipt.Version, Value: encoded}}})
	current, _, e := s.read(ctx, sd, proposal.TargetID())
	if e != nil {
		return Receipt{}, errors.Join(ErrIndeterminate, e)
	}
	replay, found, lookupErr := lookup(current, id, proposal.ID())
	if found && lookupErr == nil {
		return replay, nil
	}
	if status == allocator.StatusRejected && writeErr == nil {
		return Receipt{}, ErrConflict
	}
	if lookupErr == nil {
		lookupErr = ErrConflict
	}
	return Receipt{}, errors.Join(ErrIndeterminate, lookupErr)
}
