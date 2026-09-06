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

// Package fleetevents defines durable, resumable product events for agent fleets.
package fleetevents

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	MaxIDBytes             = 256
	MaxKinds               = 64
	MaxKindBytes           = 128
	MaxEvidence            = 257
	MaxPageSize            = 256
	MaxSubscriptionTTL     = 30 * 24 * time.Hour
	DefaultSubscriptionTTL = 24 * time.Hour
	DefaultCursorTTL       = 15 * time.Minute
	// MaxMutationRetryWindow bounds durable publication and subscription
	// mutation receipts so their physical keyspaces remain finite.
	MaxMutationRetryWindow = 24 * time.Hour
	// MaxLongPollWait stays below the production HTTP server's 30-second
	// write timeout so an empty pull can complete before transport teardown.
	MaxLongPollWait = 25 * time.Second
)

var (
	ErrResyncRequired       = errors.New("fleet events: resync required")
	ErrCursorInvalid        = errors.New("fleet events: invalid cursor")
	ErrGenerationConflict   = errors.New("fleet events: subscription generation conflict")
	ErrActionCommitted      = errors.New("fleet events: action committed before authorization changed")
	ErrAuditOutcomeUnknown  = errors.New("fleet events: action committed but audit outcome is unknown")
	ErrPublicationUnknown   = errors.New("fleet events: publication outcome is unknown")
	ErrPublicationExpired   = errors.New("fleet events: publication idempotency window expired")
	ErrMutationExpired      = errors.New("fleet events: subscription mutation retry window expired")
	ErrRetentionCapacity    = errors.New("fleet events: retained retry window is at capacity")
	ErrSubscriptionNotFound = errors.New("fleet events: subscription not found")
)

// Evidence is the complete authorization join for an event. It contains only
// opaque identities; event payloads, credentials, callbacks, and egress
// destinations are deliberately absent from this API.
type Evidence struct {
	SourceID   []byte
	PolicyID   []byte
	ObjectID   shoal.ID
	NodeID     shoal.ID
	EdgeID     shoal.ID
	AnchorID   shoal.ID
	RevisionID shoal.ID
	Start      int64
	End        int64
	Visibility []string
}

// Event is one committed event envelope. Sequence is assigned by the durable
// backend. EventID is stable across retries of the same publication token.
type Event struct {
	Sequence           uint64
	EventID            []byte
	Kind               string
	ProducerID         []byte
	ProducerGeneration int64
	ActionID           []byte
	TransitionID       []byte
	CorrelationID      []byte
	Reason             interaction.Reason
	Evidence           []Evidence
	OccurredAt         time.Time
}

// PublishRequest contains the immutable event, an idempotency token, and the
// caller's bounded UTC deadline for retrying an ambiguous commit.
type PublishRequest struct {
	Token      []byte
	RetryUntil time.Time
	Event      Event
}

type PublishResult struct {
	EventID  []byte
	Sequence uint64
	Repeated bool
}

// Filter can only narrow delivery. Empty sets mean no additional narrowing.
type Filter struct {
	Kinds     []string
	SourceIDs [][]byte
	PolicyIDs [][]byte
}

type Subscription struct {
	ID                       []byte
	SubscriberID             shoal.ID
	AgentID                  shoal.ID
	AgentGeneration          int64
	AuthorizationFingerprint auth.Fingerprint
	PolicyGeneration         int64
	Filter                   Filter
	Generation               uint64
	CreatedAt                time.Time
	ExpiresAt                time.Time
	RevokedAt                time.Time
}

type CreateRequest struct {
	Token           []byte
	SubscriberID    shoal.ID
	AgentID         shoal.ID
	AgentGeneration int64
	Filter          Filter
	TTL             time.Duration
	RetryUntil      time.Time
}

type DeleteRequest struct {
	SubscriptionID     []byte
	ExpectedGeneration uint64
	RetryUntil         time.Time
}

type PullRequest struct {
	SubscriptionID []byte
	Cursor         string
	Limit          int
	Wait           time.Duration
}

type Page struct {
	Events      []Event
	NextCursor  string
	HighWater   uint64
	AtLeastOnce bool
}

// AuditRecord is a redacted privileged-action receipt. Implementations should
// use the existing durable interaction spine.
type AuditRecord struct {
	Operation                auth.Operation
	ActionID                 []byte
	RequestID                shoal.ID
	CorrelationID            []byte
	ObjectID                 []byte
	Evidence                 []Evidence
	AuthorizationFingerprint auth.Fingerprint
	AuthorizationExpiresAt   time.Time
	OccurredAt               time.Time
}

// LifecycleReceipt pins the authorization provenance of the durable action
// transition whose event is being published. Fresh authorization is still
// evaluated independently for every publication attempt.
type LifecycleReceipt struct {
	RequestID                shoal.ID
	CorrelationID            []byte
	AuthorizationFingerprint auth.Fingerprint
	AuthorizationExpiresAt   time.Time
}

type Auditor interface {
	RecordFleetAction(context.Context, AuditRecord) error
}

// LeaseValidator rechecks the target agent's durable lease and delegation
// immediately before delivery. Implementations must not cache a success.
type LeaseValidator interface {
	ValidateDelivery(context.Context, shoal.ID, int64) error
}

// Backend is the durable source of truth. Scan must return only committed,
// prefix-visible records and must return ErrResyncRequired below HistoryFloor.
type Backend interface {
	Create(context.Context, CreateRequest, auth.Fingerprint, int64, time.Time) (Subscription, bool, error)
	Subscription(context.Context, []byte) (Subscription, error)
	Delete(context.Context, []byte, shoal.ID, uint64, time.Time, time.Time) (Subscription, error)
	Append(context.Context, PublishRequest, time.Time) (PublishResult, error)
	Scan(context.Context, uint64, uint64, int) ([]Event, uint64, error)
}

func cloneEvent(event Event) Event {
	result := event
	result.EventID = cloneBytes(event.EventID)
	result.ProducerID = cloneBytes(event.ProducerID)
	result.ActionID = cloneBytes(event.ActionID)
	result.TransitionID = cloneBytes(event.TransitionID)
	result.CorrelationID = cloneBytes(event.CorrelationID)
	result.Evidence = cloneEvidence(event.Evidence)
	return result
}

func cloneEvidence(evidence []Evidence) []Evidence {
	result := make([]Evidence, len(evidence))
	for i := range evidence {
		result[i] = Evidence{
			SourceID: cloneBytes(evidence[i].SourceID),
			PolicyID: cloneBytes(evidence[i].PolicyID),
			ObjectID: evidence[i].ObjectID, NodeID: evidence[i].NodeID,
			EdgeID: evidence[i].EdgeID, AnchorID: evidence[i].AnchorID,
			RevisionID: evidence[i].RevisionID, Start: evidence[i].Start,
			End:        evidence[i].End,
			Visibility: append([]string(nil), evidence[i].Visibility...),
		}
	}
	return result
}

func cloneSubscription(subscription Subscription) Subscription {
	result := subscription
	result.ID = cloneBytes(subscription.ID)
	result.Filter = cloneFilter(subscription.Filter)
	return result
}

func cloneFilter(filter Filter) Filter {
	return Filter{
		Kinds:     append([]string(nil), filter.Kinds...),
		SourceIDs: cloneByteSlices(filter.SourceIDs),
		PolicyIDs: cloneByteSlices(filter.PolicyIDs),
	}
}

func normalizeFilter(filter Filter) (Filter, error) {
	if len(filter.Kinds) > MaxKinds || len(filter.SourceIDs) > auth.MaxDecisionGrantIDs ||
		len(filter.PolicyIDs) > auth.MaxDecisionGrantIDs {
		return Filter{}, shoal.NewError(shoal.ErrorInvalidArgument, "event filter exceeds its bound")
	}
	result := cloneFilter(filter)
	for _, kind := range result.Kinds {
		if err := validateKind(kind); err != nil {
			return Filter{}, err
		}
	}
	for _, values := range [][][]byte{result.SourceIDs, result.PolicyIDs} {
		for _, value := range values {
			if err := validateID("event filter identity", value, false); err != nil {
				return Filter{}, err
			}
		}
	}
	sort.Strings(result.Kinds)
	result.Kinds = uniqueStrings(result.Kinds)
	sort.Slice(result.SourceIDs, func(i, j int) bool { return bytes.Compare(result.SourceIDs[i], result.SourceIDs[j]) < 0 })
	sort.Slice(result.PolicyIDs, func(i, j int) bool { return bytes.Compare(result.PolicyIDs[i], result.PolicyIDs[j]) < 0 })
	result.SourceIDs = uniqueBytes(result.SourceIDs)
	result.PolicyIDs = uniqueBytes(result.PolicyIDs)
	return result, nil
}

func normalizeEvent(event Event, requireSequence bool) (Event, error) {
	result := cloneEvent(event)
	if requireSequence && result.Sequence == 0 {
		return Event{}, shoal.NewError(shoal.ErrorInvalidArgument, "event sequence is required")
	}
	if err := validateID("event ID", result.EventID, !requireSequence); err != nil {
		return Event{}, err
	}
	if err := validateKind(result.Kind); err != nil {
		return Event{}, err
	}
	if result.ProducerGeneration <= 0 {
		return Event{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "event producer generation is required")
	}
	for name, value := range map[string][]byte{
		"event producer ID":    result.ProducerID,
		"event action ID":      result.ActionID,
		"event transition ID":  result.TransitionID,
		"event correlation ID": result.CorrelationID,
	} {
		if err := validateID(name, value, name == "event correlation ID"); err != nil {
			return Event{}, err
		}
	}
	if err := result.Reason.Validate(); err != nil {
		return Event{}, err
	}
	if len(result.Evidence) == 0 || len(result.Evidence) > MaxEvidence {
		return Event{}, shoal.NewError(shoal.ErrorInvalidArgument, "event evidence is outside its bound")
	}
	for i := range result.Evidence {
		if err := validateID("event source ID", result.Evidence[i].SourceID, false); err != nil {
			return Event{}, err
		}
		if err := validateID("event policy ID", result.Evidence[i].PolicyID, false); err != nil {
			return Event{}, err
		}
		if err := shoal.ValidateRequiredID("event object ID", result.Evidence[i].ObjectID); err != nil {
			return Event{}, err
		}
		if err := validateEventEvidenceReference(result.Evidence[i]); err != nil {
			return Event{}, err
		}
	}
	sort.Slice(result.Evidence, func(i, j int) bool {
		return compareEvidence(result.Evidence[i], result.Evidence[j]) < 0
	})
	for i := 1; i < len(result.Evidence); i++ {
		if compareEvidence(result.Evidence[i-1], result.Evidence[i]) == 0 {
			return Event{}, shoal.NewError(shoal.ErrorInvalidArgument, "event evidence contains a duplicate")
		}
	}
	if result.OccurredAt.IsZero() || result.OccurredAt.Location() != time.UTC {
		return Event{}, shoal.NewError(shoal.ErrorInvalidArgument, "event occurrence time must be UTC")
	}
	return result, nil
}

func compareEvidence(left, right Evidence) int {
	for _, pair := range [][2][]byte{
		{left.SourceID, right.SourceID},
		{left.PolicyID, right.PolicyID},
		{[]byte(left.ObjectID), []byte(right.ObjectID)},
		{[]byte(left.NodeID), []byte(right.NodeID)},
		{[]byte(left.EdgeID), []byte(right.EdgeID)},
		{[]byte(left.AnchorID), []byte(right.AnchorID)},
		{[]byte(left.RevisionID), []byte(right.RevisionID)},
	} {
		if comparison := bytes.Compare(pair[0], pair[1]); comparison != 0 {
			return comparison
		}
	}
	if left.Start < right.Start {
		return -1
	}
	if left.Start > right.Start {
		return 1
	}
	if left.End < right.End {
		return -1
	}
	if left.End > right.End {
		return 1
	}
	for index := 0; index < len(left.Visibility) && index < len(right.Visibility); index++ {
		if left.Visibility[index] < right.Visibility[index] {
			return -1
		}
		if left.Visibility[index] > right.Visibility[index] {
			return 1
		}
	}
	if len(left.Visibility) < len(right.Visibility) {
		return -1
	}
	if len(left.Visibility) > len(right.Visibility) {
		return 1
	}
	return 0
}

func validateEventEvidenceReference(value Evidence) error {
	hasReference := value.NodeID != "" || value.EdgeID != "" ||
		value.AnchorID != "" || value.RevisionID != "" ||
		value.Start != 0 || value.End != 0 || len(value.Visibility) != 0
	if !hasReference {
		return nil
	}
	if value.NodeID == "" && value.EdgeID == "" {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "event evidence node or edge is required")
	}
	for name, id := range map[string]shoal.ID{
		"event evidence node": value.NodeID, "event evidence edge": value.EdgeID,
		"event evidence anchor":   value.AnchorID,
		"event evidence revision": value.RevisionID,
	} {
		if err := shoal.ValidateOptionalID(name, id); err != nil {
			return err
		}
	}
	if value.Start < 0 || value.End < value.Start {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "event evidence range is invalid")
	}
	if value.AnchorID == "" && (value.Start != 0 || value.End != 0) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "event evidence range requires an anchor")
	}
	if len(value.Visibility) == 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "event evidence visibility is required")
	}
	normalized, err := interaction.Conjoin(value.Visibility)
	if err != nil {
		return err
	}
	if len(normalized) != len(value.Visibility) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "event evidence visibility must be canonical")
	}
	for index := range normalized {
		if normalized[index] != value.Visibility[index] {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "event evidence visibility must be canonical")
		}
	}
	return nil
}

func validateID(name string, value []byte, optional bool) error {
	if len(value) == 0 && optional {
		return nil
	}
	if len(value) == 0 || len(value) > MaxIDBytes {
		return shoal.NewError(shoal.ErrorInvalidArgument, name+" is outside its byte bound")
	}
	return nil
}

func validateKind(kind string) error {
	if kind == "" || len(kind) > MaxKindBytes || !utf8.ValidString(kind) ||
		strings.TrimSpace(kind) != kind {
		return shoal.NewError(shoal.ErrorInvalidArgument, "event kind is invalid")
	}
	for _, r := range kind {
		if !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') &&
			r != '.' && r != '_' && r != '-' {
			return shoal.NewError(shoal.ErrorInvalidArgument, "event kind is invalid")
		}
	}
	return nil
}

func cloneBytes(value []byte) []byte { return append([]byte(nil), value...) }

func cloneByteSlices(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for i := range values {
		result[i] = cloneBytes(values[i])
	}
	return result
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func uniqueBytes(values [][]byte) [][]byte {
	if len(values) == 0 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if !bytes.Equal(value, out[len(out)-1]) {
			out = append(out, value)
		}
	}
	return out
}

func containsBytes(values [][]byte, wanted []byte) bool {
	index := sort.Search(len(values), func(i int) bool { return bytes.Compare(values[i], wanted) >= 0 })
	return index < len(values) && bytes.Equal(values[index], wanted)
}

func containsString(values []string, wanted string) bool {
	index := sort.SearchStrings(values, wanted)
	return index < len(values) && values[index] == wanted
}

func mapContextError(err error) error {
	if errors.Is(err, ErrResyncRequired) {
		return errors.Join(
			ErrResyncRequired,
			shoal.NewError(shoal.ErrorConflict, "event cursor requires resynchronization"),
		)
	}
	if errors.Is(err, ErrCursorInvalid) {
		return errors.Join(
			ErrCursorInvalid,
			shoal.NewError(shoal.ErrorInvalidArgument, "event cursor is invalid"),
		)
	}
	if errors.Is(err, ErrPublicationExpired) ||
		errors.Is(err, ErrMutationExpired) {
		return errors.Join(
			err,
			shoal.NewError(shoal.ErrorConflict, "fleet event retry window expired"),
		)
	}
	if errors.Is(err, ErrRetentionCapacity) {
		return errors.Join(
			ErrRetentionCapacity,
			shoal.NewError(
				shoal.ErrorUnavailable,
				"fleet event retry retention is temporarily at capacity",
			),
		)
	}
	if errors.Is(err, context.Canceled) {
		return shoal.WrapError(shoal.ErrorCanceled, "event operation canceled", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return shoal.WrapError(shoal.ErrorDeadline, "event operation deadline exceeded", err)
	}
	return err
}
