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

package explorerfleetevents

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"

	"github.com/phrocker/shoal-oss/internal/explorerfleetcap"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleetevents"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// ActionEventPublisher adapts durable dispatch lifecycle transitions to the
// fleet event log.
type ActionEventPublisher struct {
	service   *fleetevents.Service
	resolver  auth.Resolver
	reconcile explorerfleetcap.Capability
	now       func() time.Time
}

var _ fleet.ActionEventPublisher = (*ActionEventPublisher)(nil)

func NewActionEventPublisher(
	service *fleetevents.Service,
	resolver auth.Resolver,
	capability explorerfleetcap.Capability,
	clock func() time.Time,
) (*ActionEventPublisher, error) {
	if service == nil || resolver == nil || !capability.Valid() || clock == nil {
		return nil, errors.New("fleet events: action publisher dependencies are required")
	}
	return &ActionEventPublisher{
		service: service, resolver: resolver,
		reconcile: capability, now: clock,
	}, nil
}

func (p *ActionEventPublisher) PublishActionEvent(
	ctx context.Context, kind string, record fleet.ActionRecord,
) error {
	token, transitionID, err := actionEventIdentities(kind, record)
	if err != nil {
		return err
	}
	operation, fingerprint, expiresAt, err := actionEventAuthorization(kind, record)
	if err != nil {
		return err
	}
	provenance := record.EventProvenance()
	now := p.now().UTC()
	decision, err := p.resolver.Resolve(ctx)
	if err != nil {
		return err
	}
	if !publisherMatchesTransition(decision, kind, record) {
		return shoal.NewError(
			shoal.ErrorUnauthorized,
			"fleet action event authorization does not match durable transition",
		)
	}
	if err := decision.AuthorizeObject(operation, auth.ResourceRequest{
		AuthorizationDomain: decision.AuthorizationDomain(),
		SourceID:            record.SourceID, PolicyID: record.PolicyID,
		ObjectID: record.ObjectID,
	}, now); err != nil {
		return err
	}
	references := actionEvidenceReferences(record.Evidence)
	evidence := actionAuthorizationEvidence(record, references)
	event := fleetevents.Event{
		Kind:               kind,
		ProducerID:         []byte(record.AgentID),
		ProducerGeneration: record.AgentGeneration,
		ActionID:           append([]byte(nil), record.ID...),
		TransitionID:       transitionID,
		CorrelationID:      []byte(provenance.CorrelationID),
		Reason:             record.Reason,
		Evidence:           evidence,
		ConsumedEvidence:   references,
		OccurredAt:         record.UpdatedAt,
	}

	_, err = p.service.PublishLifecycle(
		ctx, p.reconcile, operation, fleetevents.PublishRequest{
			Token: token, RetryUntil: record.UpdatedAt.Add(fleetevents.MaxMutationRetryWindow),
			Event: event,
		}, fleetevents.LifecycleReceipt{
			RequestID:                provenance.RequestID,
			CorrelationID:            []byte(provenance.CorrelationID),
			AuthorizationFingerprint: fingerprint,
			AuthorizationExpiresAt:   expiresAt,
		})
	return err
}

func actionAuthorizationEvidence(
	record fleet.ActionRecord,
	references []interaction.EvidenceReference,
) []fleetevents.Evidence {
	result := make([]fleetevents.Evidence, 0, 1+len(references))
	covered := make(map[shoal.ID]struct{})
	appendID := func(id shoal.ID, reference *interaction.EvidenceReference) {
		result = append(result, fleetevents.Evidence{
			SourceID: append([]byte(nil), record.SourceID...),
			PolicyID: append([]byte(nil), record.PolicyID...),
			ObjectID: id, Reference: reference,
		})
		covered[id] = struct{}{}
		if reference != nil {
			for _, exactID := range fleetevents.ExactEvidenceReferenceIDs(*reference) {
				if exactID != "" {
					covered[exactID] = struct{}{}
				}
			}
		}
	}
	appendID(record.ObjectID, nil)
	for _, reference := range references {
		canonical := cloneActionEvidenceReference(reference)
		var representative shoal.ID
		for _, id := range fleetevents.ExactEvidenceReferenceIDs(canonical) {
			if id == "" {
				continue
			}
			if _, ok := covered[id]; !ok {
				representative = id
				break
			}
		}
		if representative != "" {
			appendID(representative, &canonical)
		}
	}
	return result
}

func cloneActionEvidenceReference(
	value interaction.EvidenceReference,
) interaction.EvidenceReference {
	value.NodeIDs = append([]shoal.ID(nil), value.NodeIDs...)
	value.EdgeIDs = append([]shoal.ID(nil), value.EdgeIDs...)
	value.Assertions = append(
		[]interaction.AssertionReference(nil), value.Assertions...)
	return value
}

func actionEvidenceReferences(
	values []fleet.EvidenceRef,
) []interaction.EvidenceReference {
	if len(values) == 0 {
		return nil
	}
	result := make([]interaction.EvidenceReference, len(values))
	for i, value := range values {
		result[i] = interaction.EvidenceReference{
			AnchorID: value.AnchorID, Kind: value.Kind, Citation: value.Citation,
			NodeIDs: append([]shoal.ID(nil), value.NodeIDs...),
			EdgeIDs: append([]shoal.ID(nil), value.EdgeIDs...),
			Assertions: append(
				[]interaction.AssertionReference(nil), value.Assertions...),
		}
	}
	return result
}

// publisherMatchesTransition reports whether the publishing decision is the
// principal that performed the transition being published.
//
// For an enqueue or a cancellation that is the record's own principal. For a
// claim or a completion it is the record's *claimant*, which since #437 need
// not be the same principal — applyClaim deliberately leaves the record's own
// identity alone, so a record claimed by a worker still names its enqueuer.
//
// Comparing against the enqueuer for every kind is what made #437's capability
// fail in the hosted build: a worker's claim and completion both failed to
// publish, and a failed publication is reported as ErrActionCommitted, so the
// write landed and the worker was told to reconcile an outcome that was
// actually recorded. Nothing in the fleet package's own tests could see it,
// because they bind a no-op event sink.
//
// A claimed or completed record with no claimant chain predates the field, and
// there the claimant was by construction the enqueuer.
func publisherMatchesTransition(
	decision auth.Decision, kind string, record fleet.ActionRecord,
) bool {
	switch kind {
	case "action.claimed", "action.completed", "action.failed":
		if record.ClaimantSubject != "" {
			return decision.Subject() == record.ClaimantSubject &&
				decision.Actor() == record.ClaimantActor &&
				decision.ClientID() == record.ClaimantClientID &&
				sameIDs(decision.OnBehalfOf(), record.ClaimantOnBehalfOf)
		}
	}
	return decision.Subject() == record.Subject &&
		decision.Actor() == record.Actor &&
		decision.ClientID() == record.ClientID &&
		sameIDs(decision.OnBehalfOf(), record.OnBehalfOf)
}

func actionEventAuthorization(
	kind string, record fleet.ActionRecord,
) (auth.Operation, auth.Fingerprint, time.Time, error) {
	var operation auth.Operation
	provenance := record.EventProvenance()
	switch kind {
	case "action.enqueued":
		if containsOperation(record.AuthorizedOperations, auth.OperationDispatch) {
			operation = auth.OperationDispatch
		} else {
			operation = auth.OperationInvoke
		}
	case "action.canceled":
		operation = auth.OperationDispatch
	case "action.claimed", "action.completed", "action.failed":
		// The operation the transition was actually authorized by, which since
		// #437 is not always invoke: a principal granted OperationExecute on
		// the descriptor may claim and complete work it did not enqueue, and
		// it need not hold invoke at all.
		//
		// Hardcoding invoke here did not merely mislabel the event. The
		// publishing decision is authorized against this operation below, so a
		// claim taken under execute failed to publish — and a failed
		// publication is returned as ErrActionCommitted, which means the
		// transition was written and the worker was told its outcome needs
		// reconciliation. The capability did not work end to end.
		//
		// Empty for a record written before the field existed, where the
		// claimant was by construction the enqueuer and invoke is what
		// authorized it.
		operation = record.TransitionOperation
		if operation == "" {
			operation = auth.OperationInvoke
		}
	default:
		return "", auth.Fingerprint{}, time.Time{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet action event kind is invalid")
	}
	if provenance.AuthorizationFingerprint == (auth.Fingerprint{}) ||
		provenance.AuthorizationExpiresAt.IsZero() ||
		provenance.AuthorizationExpiresAt.Location() != time.UTC ||
		!containsOperation(record.AuthorizedOperations, operation) {
		return "", auth.Fingerprint{}, time.Time{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action event authorization provenance is incomplete",
		)
	}
	return operation, provenance.AuthorizationFingerprint,
		provenance.AuthorizationExpiresAt, nil
}

func containsOperation(values []auth.Operation, wanted auth.Operation) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func sameIDs(left, right []shoal.ID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal([]byte(left[index]), []byte(right[index])) {
			return false
		}
	}
	return true
}

func actionEventToken(kind string, record fleet.ActionRecord) ([]byte, error) {
	token, _, err := actionEventIdentities(kind, record)
	return token, err
}

func actionEventIdentities(
	kind string, record fleet.ActionRecord,
) ([]byte, []byte, error) {
	transition, expectedState, err := actionEventTransition(kind, record)
	if err != nil {
		return nil, nil, err
	}
	if record.State != expectedState {
		return nil, nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action event kind does not match action state",
		)
	}
	if record.AgentID == "" || record.AgentGeneration <= 0 || record.Version == 0 {
		return nil, nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action event identity is incomplete",
		)
	}
	transitionHash := sha256.New()
	_, _ = transitionHash.Write([]byte("shoal-fleet-action-transition-id-v3"))
	writeTokenField(transitionHash, []byte(kind))
	writeTokenField(transitionHash, record.ID)
	writeTokenField(transitionHash, transition)
	transitionID := transitionHash.Sum(nil)

	hash := sha256.New()
	_, _ = hash.Write([]byte("shoal-fleet-action-event-publication-v5"))
	writeTokenField(hash, []byte(kind))
	writeTokenField(hash, record.ID)
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], record.Version)
	writeTokenField(hash, encoded[:])
	return hash.Sum(nil), transitionID, nil
}

func actionEventTransition(
	kind string, record fleet.ActionRecord,
) ([]byte, fleet.DispatchState, error) {
	var transition []byte
	var state fleet.DispatchState
	switch kind {
	case "action.enqueued":
		transition, state = record.IdempotencyKey, fleet.DispatchQueued
	case "action.claimed":
		transition, state = record.ClaimID, fleet.DispatchClaimed
	case "action.completed":
		transition, state = record.ExecutorKey, fleet.DispatchSucceeded
	case "action.failed":
		transition, state = record.ExecutorKey, fleet.DispatchFailed
	case "action.canceled":
		transition, state = record.CancelKey, fleet.DispatchCanceled
	default:
		return nil, "", shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet action event kind is invalid")
	}
	if len(transition) == 0 {
		return nil, "", shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action event transition identity is required",
		)
	}
	return transition, state, nil
}

type tokenWriter interface {
	Write([]byte) (int, error)
}

func writeTokenField(writer tokenWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}
