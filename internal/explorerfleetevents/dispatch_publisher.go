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

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleetevents"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// ActionEventPublisher adapts durable dispatch lifecycle transitions to the
// fleet event log.
type ActionEventPublisher struct {
	service  *fleetevents.Service
	resolver auth.Resolver
	now      func() time.Time
}

var _ fleet.ActionEventPublisher = (*ActionEventPublisher)(nil)

func NewActionEventPublisher(
	service *fleetevents.Service,
	resolver auth.Resolver,
	clock func() time.Time,
) (*ActionEventPublisher, error) {
	if service == nil || resolver == nil || clock == nil {
		return nil, errors.New("fleet events: action publisher dependencies are required")
	}
	return &ActionEventPublisher{
		service: service, resolver: resolver, now: clock,
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
	now := p.now().UTC()
	decision, err := p.resolver.Resolve(ctx)
	if err != nil {
		return err
	}
	if decision.Subject() != record.Subject || decision.Actor() != record.Actor ||
		decision.ClientID() != record.ClientID ||
		decision.RequestID() != record.RequestID ||
		decision.CorrelationID() != record.CorrelationID ||
		!sameIDs(decision.OnBehalfOf(), record.OnBehalfOf) {
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
	evidence := []fleetevents.Evidence{{
		SourceID: append([]byte(nil), record.SourceID...),
		PolicyID: append([]byte(nil), record.PolicyID...),
		ObjectID: record.ObjectID,
	}}
	for _, reference := range record.Evidence {
		if len(reference.Assertions) != 0 {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"fleet action event assertion evidence is not representable",
			)
		}
		common := fleetevents.Evidence{
			SourceID: append([]byte(nil), record.SourceID...),
			PolicyID: append([]byte(nil), record.PolicyID...),
			ObjectID: record.ObjectID, AnchorID: reference.AnchorID,
			Visibility: append([]string(nil), reference.Visibility...),
		}
		if reference.Citation.RevisionID != "" {
			common.ObjectID = reference.Citation.DocumentID
			common.RevisionID = reference.Citation.RevisionID
			common.Start = reference.Citation.Range.Start.Offset
			common.End = reference.Citation.Range.End.Offset
		}
		for _, nodeID := range reference.NodeIDs {
			item := common
			item.NodeID = nodeID
			evidence = append(evidence, item)
		}
		for _, edgeID := range reference.EdgeIDs {
			item := common
			item.EdgeID = edgeID
			evidence = append(evidence, item)
		}
	}
	if len(evidence) > fleetevents.MaxEvidence {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action event evidence exceeds its bound",
		)
	}
	event := fleetevents.Event{
		Kind:               kind,
		ProducerID:         []byte(record.AgentID),
		ProducerGeneration: record.AgentGeneration,
		ActionID:           append([]byte(nil), record.ID...),
		TransitionID:       transitionID,
		CorrelationID:      []byte(record.CorrelationID),
		Reason:             record.Reason,
		Evidence:           evidence,
		OccurredAt:         record.UpdatedAt,
	}
	_, err = p.service.PublishLifecycle(ctx, operation, fleetevents.PublishRequest{
		Token: token, RetryUntil: record.UpdatedAt.Add(fleetevents.MaxMutationRetryWindow),
		Event: event,
	}, fleetevents.LifecycleReceipt{
		RequestID: record.RequestID, CorrelationID: []byte(record.CorrelationID),
		AuthorizationFingerprint: fingerprint,
		AuthorizationExpiresAt:   expiresAt,
	})
	return err
}

func actionEventAuthorization(
	kind string, record fleet.ActionRecord,
) (auth.Operation, auth.Fingerprint, time.Time, error) {
	var operation auth.Operation
	var fingerprint auth.Fingerprint
	var expiresAt time.Time
	switch kind {
	case "action.enqueued", "action.canceled":
		operation = auth.OperationDispatch
		fingerprint = record.AuthorizationFingerprint
		expiresAt = record.AuthorizationExpiresAt
	case "action.claimed", "action.completed", "action.failed":
		operation = auth.OperationInvoke
		fingerprint = record.ExecutionFingerprint
		expiresAt = record.ExecutionExpiresAt
	default:
		return "", auth.Fingerprint{}, time.Time{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet action event kind is invalid")
	}
	if fingerprint == (auth.Fingerprint{}) || expiresAt.IsZero() ||
		expiresAt.Location() != time.UTC ||
		!containsOperation(record.AuthorizedOperations, operation) {
		return "", auth.Fingerprint{}, time.Time{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action event authorization provenance is incomplete",
		)
	}
	return operation, fingerprint, expiresAt, nil
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
