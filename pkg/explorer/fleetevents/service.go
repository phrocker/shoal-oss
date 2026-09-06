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

package fleetevents

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type Config struct {
	Backend          Backend
	Resolver         auth.Resolver
	GenerationReader auth.GenerationReader
	LeaseValidator   LeaseValidator
	Auditor          Auditor
	CursorKey        []byte
	CursorTTL        time.Duration
	Clock            func() time.Time
	PollInterval     time.Duration
	MaxWait          time.Duration
}

type Service struct {
	backend     Backend
	resolver    auth.Resolver
	generations auth.GenerationReader
	leases      LeaseValidator
	auditor     Auditor
	cursors     cursorCodec
	now         func() time.Time
	poll        time.Duration
	maxWait     time.Duration
}

func New(config Config) (*Service, error) {
	if config.Backend == nil || config.Resolver == nil || config.GenerationReader == nil ||
		config.LeaseValidator == nil || config.Auditor == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet event dependencies are required")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.CursorTTL == 0 {
		config.CursorTTL = DefaultCursorTTL
	}
	if config.PollInterval == 0 {
		config.PollInterval = 50 * time.Millisecond
	}
	if config.MaxWait == 0 {
		config.MaxWait = MaxLongPollWait
	}
	if config.PollInterval < time.Millisecond || config.MaxWait < config.PollInterval ||
		config.MaxWait > MaxLongPollWait {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet event wait bounds are invalid")
	}
	codec, err := newCursorCodec(config.CursorKey, config.CursorTTL)
	if err != nil {
		return nil, shoal.WrapError(shoal.ErrorInvalidArgument, "fleet event cursor configuration", err)
	}
	return &Service{
		backend: config.Backend, resolver: config.Resolver, generations: config.GenerationReader,
		leases: config.LeaseValidator, auditor: config.Auditor, cursors: codec,
		now: config.Clock, poll: config.PollInterval, maxWait: config.MaxWait,
	}, nil
}

func (s *Service) Create(ctx context.Context, request CreateRequest) (Subscription, error) {
	now := s.now().UTC()
	if err := validateRetryUntil(request.RetryUntil, now, ErrMutationExpired); err != nil {
		return Subscription{}, err
	}
	decision, guard, err := s.authorize(ctx, auth.OperationSubscriptionCreate, nil, now)
	if err != nil {
		return Subscription{}, err
	}
	if request.SubscriberID == "" {
		request.SubscriberID = decision.Subject()
	}
	if request.SubscriberID != decision.Subject() {
		return Subscription{}, auth.ObjectNotFound()
	}
	if err := validateID("subscription token", request.Token, false); err != nil {
		return Subscription{}, err
	}
	if err := shoal.ValidateRequiredID("subscription agent ID", request.AgentID); err != nil {
		return Subscription{}, err
	}
	if request.AgentGeneration <= 0 {
		return Subscription{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "subscription agent generation must be positive")
	}
	request.Filter, err = normalizeFilter(request.Filter)
	if err != nil {
		return Subscription{}, err
	}
	for _, source := range request.Filter.SourceIDs {
		if !containsBytes(decision.PermittedSourceIDs(), source) {
			return Subscription{}, shoal.NewError(shoal.ErrorUnauthorized, "subscription filter exceeds authorization")
		}
	}
	for _, policy := range request.Filter.PolicyIDs {
		if !containsBytes(decision.PermittedPolicyIDs(), policy) {
			return Subscription{}, shoal.NewError(shoal.ErrorUnauthorized, "subscription filter exceeds authorization")
		}
	}
	if request.TTL == 0 {
		request.TTL = DefaultSubscriptionTTL
	}
	if request.TTL <= 0 || request.TTL > MaxSubscriptionTTL {
		return Subscription{}, shoal.NewError(shoal.ErrorInvalidArgument, "subscription TTL is outside its bound")
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return Subscription{}, err
	}
	request.Token = deriveID(
		"fleet-subscription-create-token-v2",
		[]byte(decision.Subject()), fingerprint.Bytes(), request.Token,
	)
	if err := s.leases.ValidateDelivery(
		ctx, request.AgentID, request.AgentGeneration,
	); err != nil {
		return Subscription{}, err
	}
	if err := guard.Check(ctx); err != nil {
		return Subscription{}, err
	}
	subscription, _, err := s.backend.Create(ctx, request, fingerprint, decision.PolicyGeneration(), now)
	if err != nil {
		return Subscription{}, mapContextError(err)
	}
	if err := s.record(
		ctx, decision, auth.OperationSubscriptionCreate, request.Token,
		subscription.ID, nil, subscription.CreatedAt,
	); err != nil {
		return Subscription{}, classifyAuditError(err)
	}
	if err := guard.Check(ctx); err != nil {
		return Subscription{}, errors.Join(ErrActionCommitted, err)
	}
	return cloneSubscription(subscription), nil
}

func (s *Service) Delete(ctx context.Context, request DeleteRequest) error {
	now := s.now().UTC()
	if err := validateRetryUntil(request.RetryUntil, now, ErrMutationExpired); err != nil {
		return err
	}
	decision, guard, err := s.authorize(ctx, auth.OperationSubscriptionDelete, nil, now)
	if err != nil {
		return err
	}
	if err := validateID("subscription ID", request.SubscriptionID, false); err != nil ||
		request.ExpectedGeneration == 0 {
		if err != nil {
			return err
		}
		return shoal.NewError(shoal.ErrorInvalidArgument, "expected subscription generation is required")
	}
	if err := guard.Check(ctx); err != nil {
		return err
	}
	subscription, err := s.backend.Delete(
		ctx, request.SubscriptionID, decision.Subject(), request.ExpectedGeneration,
		request.RetryUntil, now,
	)
	if err != nil {
		return nonDisclosingSubscriptionError(err)
	}
	if err := s.record(
		ctx, decision, auth.OperationSubscriptionDelete, request.SubscriptionID,
		subscription.ID, nil, subscription.RevokedAt,
	); err != nil {
		return classifyAuditError(err)
	}
	if err := guard.Check(ctx); err != nil {
		return errors.Join(ErrActionCommitted, err)
	}
	return nil
}

func (s *Service) Publish(ctx context.Context, request PublishRequest) (PublishResult, error) {
	if _, reserved := lifecycleOperation(request.Event.Kind); reserved {
		return PublishResult{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet lifecycle event kinds require trusted publication",
		)
	}
	return s.publish(ctx, auth.OperationEventPublish, request, true, nil)
}

// PublishLifecycle is the trusted dispatch lifecycle publication path. It
// accepts only the narrow dispatch operations and preserves the caller's
// canonical durable token unchanged across authorization refreshes.
func (s *Service) PublishLifecycle(
	ctx context.Context, operation auth.Operation, request PublishRequest,
	receipt LifecycleReceipt,
) (PublishResult, error) {
	expected, trusted := lifecycleOperation(request.Event.Kind)
	if !trusted || operation != expected {
		return PublishResult{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet lifecycle publication operation is invalid")
	}
	if receipt.RequestID == "" ||
		receipt.AuthorizationFingerprint == (auth.Fingerprint{}) ||
		receipt.AuthorizationExpiresAt.IsZero() ||
		receipt.AuthorizationExpiresAt.Location() != time.UTC ||
		!bytes.Equal(receipt.CorrelationID, request.Event.CorrelationID) {
		return PublishResult{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet lifecycle receipt provenance is invalid")
	}
	return s.publish(ctx, operation, request, false, &receipt)
}

func lifecycleOperation(kind string) (auth.Operation, bool) {
	switch kind {
	case "action.enqueued", "action.canceled":
		return auth.OperationDispatch, true
	case "action.claimed", "action.completed", "action.failed":
		return auth.OperationInvoke, true
	default:
		return "", false
	}
}

func (s *Service) publish(
	ctx context.Context, operation auth.Operation, request PublishRequest,
	scopePublicToken bool, lifecycleReceipt *LifecycleReceipt,
) (PublishResult, error) {
	now := s.now().UTC()
	if err := validateRetryUntil(
		request.RetryUntil, now, ErrPublicationExpired,
	); err != nil {
		return PublishResult{}, err
	}
	decision, guard, err := s.authorize(ctx, operation, request.Event.Evidence, now)
	if err != nil {
		return PublishResult{}, err
	}
	if err := validateID("event publication token", request.Token, false); err != nil {
		return PublishResult{}, err
	}
	request.Event.OccurredAt = request.Event.OccurredAt.UTC()
	request.Event, err = normalizeEvent(request.Event, false)
	if err != nil {
		return PublishResult{}, err
	}
	if err := guard.Check(ctx); err != nil {
		return PublishResult{}, err
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return PublishResult{}, err
	}
	if scopePublicToken {
		request.Token = deriveID(
			"fleet-event-publication-token-v2",
			[]byte(decision.Subject()), fingerprint.Bytes(), request.Token,
		)
	}
	result, err := s.backend.Append(ctx, request, now)
	if err != nil {
		return PublishResult{}, mapContextError(err)
	}
	auditTime := now
	if !scopePublicToken {
		auditTime = request.Event.OccurredAt
	}
	var recordErr error
	if lifecycleReceipt == nil {
		recordErr = s.record(
			ctx, decision, operation, request.Event.ActionID,
			result.EventID, request.Event.Evidence, auditTime,
		)
	} else {
		recordErr = s.auditor.RecordFleetAction(ctx, AuditRecord{
			Operation: operation, ActionID: cloneBytes(request.Event.ActionID),
			RequestID:                lifecycleReceipt.RequestID,
			CorrelationID:            cloneBytes(lifecycleReceipt.CorrelationID),
			AuthorizationFingerprint: lifecycleReceipt.AuthorizationFingerprint,
			AuthorizationExpiresAt:   lifecycleReceipt.AuthorizationExpiresAt,
			ObjectID:                 cloneBytes(result.EventID),
			Evidence:                 cloneEvidence(request.Event.Evidence),
			OccurredAt:               auditTime,
		})
	}
	if recordErr != nil {
		return PublishResult{}, classifyAuditError(recordErr)
	}
	if err := guard.Check(ctx); err != nil {
		return PublishResult{}, errors.Join(ErrActionCommitted, err)
	}
	result.EventID = cloneBytes(result.EventID)
	// The stream position is internal resume state. Returning it would reveal
	// the number of otherwise hidden events to an authorized publisher.
	result.Sequence = 0
	return result, nil
}

func (s *Service) Pull(ctx context.Context, request PullRequest) (Page, error) {
	if request.Limit == 0 {
		request.Limit = MaxPageSize
	}
	if request.Limit < 1 || request.Limit > MaxPageSize || request.Wait < 0 || request.Wait > s.maxWait {
		return Page{}, shoal.NewError(shoal.ErrorInvalidArgument, "event pull bounds are invalid")
	}
	subscription, decision, fingerprint, next, frontier, err :=
		s.deliveryState(ctx, request, s.now().UTC())
	if err != nil {
		return Page{}, err
	}
	deadline := s.now().UTC().Add(request.Wait)
	for {
		events, pinnedFrontier, scanErr := s.backend.Scan(ctx, next, frontier, request.Limit)
		if scanErr != nil {
			return Page{}, mapContextError(scanErr)
		}
		frontier = pinnedFrontier
		page := make([]Event, 0, len(events))
		for _, event := range events {
			if event.Sequence < next {
				return Page{}, shoal.NewError(shoal.ErrorInternal, "event backend returned an invalid sequence")
			}
			allowed, authErr := s.authorizeDelivery(ctx, subscription, event, decision)
			if authErr != nil {
				return Page{}, authErr
			}
			if allowed {
				page = append(page, cloneEvent(event))
			}
			next = event.Sequence + 1
		}
		if len(page) > 0 || request.Wait == 0 || !s.now().UTC().Before(deadline) {
			freshSubscription, freshDecision, freshFingerprint, _, _, refreshErr :=
				s.deliveryState(ctx, PullRequest{SubscriptionID: request.SubscriptionID}, s.now().UTC())
			if refreshErr != nil {
				return Page{}, refreshErr
			}
			if freshSubscription.Generation != subscription.Generation ||
				freshDecision.Subject() != decision.Subject() ||
				freshFingerprint != fingerprint {
				return Page{}, shoal.NewError(shoal.ErrorUnavailable, "subscription authorization changed")
			}
			authorizedPage := page[:0]
			for _, event := range page {
				allowed, authErr := s.authorizeDelivery(
					ctx, freshSubscription, event, freshDecision)
				if authErr != nil {
					return Page{}, authErr
				}
				if allowed {
					authorizedPage = append(authorizedPage, event)
				}
			}
			page = authorizedPage
			// Global stream positions and the count of filtered events are
			// internal cursor state. Exposing them would reveal hidden objects.
			for i := range page {
				page[i].Sequence = 0
			}

			cursor, sealErr := s.cursors.seal(cursorState{
				SubscriptionID: subscription.ID, SubscriberID: string(subscription.SubscriberID),
				Fingerprint: fingerprint, Generation: subscription.Generation,
				NextSequence: next, Frontier: frontier,
				ExpiresAt: s.now().UTC().Add(s.cursors.ttl),
			})
			if sealErr != nil {
				return Page{}, sealErr
			}
			return Page{Events: page, NextCursor: cursor, HighWater: 0, AtLeastOnce: true}, nil
		}
		timer := time.NewTimer(s.poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Page{}, mapContextError(ctx.Err())
		case <-timer.C:
		}
		freshSubscription, freshDecision, freshFingerprint, _, _, refreshErr :=
			s.deliveryState(ctx, PullRequest{SubscriptionID: request.SubscriptionID}, s.now().UTC())
		if refreshErr != nil {
			return Page{}, refreshErr
		}
		if freshSubscription.Generation != subscription.Generation ||
			freshDecision.Subject() != decision.Subject() ||
			freshFingerprint != fingerprint {
			return Page{}, shoal.NewError(shoal.ErrorUnavailable, "subscription authorization changed")
		}
		subscription, decision, fingerprint =
			freshSubscription, freshDecision, freshFingerprint
	}
}

func classifyAuditError(err error) error {
	if err == nil || interaction.IsCommittedRecord(err) {
		return err
	}
	return errors.Join(ErrAuditOutcomeUnknown, err)
}

func validateRetryUntil(
	retryUntil, now time.Time, expired error,
) error {
	if retryUntil.IsZero() || retryUntil.Location() != time.UTC {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "mutation retry deadline must be UTC")
	}
	if !now.Before(retryUntil) {
		return mapContextError(expired)
	}
	if retryUntil.After(now.Add(MaxMutationRetryWindow)) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "mutation retry window exceeds its bound")
	}
	return nil
}

func (s *Service) deliveryState(
	ctx context.Context, request PullRequest, now time.Time,
) (Subscription, auth.Decision, auth.Fingerprint, uint64, uint64, error) {
	decision, guard, err := s.authorize(ctx, auth.OperationSubscriptionCreate, nil, now)
	if err != nil {
		return Subscription{}, auth.Decision{}, auth.Fingerprint{}, 0, 0, err
	}
	subscription, err := s.backend.Subscription(ctx, request.SubscriptionID)
	if err != nil {
		return Subscription{}, auth.Decision{}, auth.Fingerprint{}, 0, 0, nonDisclosingSubscriptionError(err)
	}
	if subscription.SubscriberID != decision.Subject() || !subscription.RevokedAt.IsZero() ||
		!now.Before(subscription.ExpiresAt) {
		return Subscription{}, auth.Decision{}, auth.Fingerprint{}, 0, 0, auth.ObjectNotFound()
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return Subscription{}, auth.Decision{}, auth.Fingerprint{}, 0, 0, err
	}
	if fingerprint != subscription.AuthorizationFingerprint {
		return Subscription{}, auth.Decision{}, auth.Fingerprint{}, 0, 0,
			shoal.NewError(shoal.ErrorUnavailable, "subscription authorization changed")
	}
	next := uint64(1)
	var frontier uint64
	if request.Cursor != "" {
		state, openErr := s.cursors.open(request.Cursor, now)
		if openErr != nil || !bytes.Equal(state.SubscriptionID, subscription.ID) ||
			state.SubscriberID != string(decision.Subject()) ||
			state.Fingerprint != fingerprint || state.Generation != subscription.Generation {
			return Subscription{}, auth.Decision{}, auth.Fingerprint{}, 0, 0,
				mapContextError(ErrCursorInvalid)
		}
		next = state.NextSequence
		frontier = state.Frontier
	}
	if err := s.leases.ValidateDelivery(
		ctx, subscription.AgentID, subscription.AgentGeneration,
	); err != nil {
		return Subscription{}, auth.Decision{}, auth.Fingerprint{}, 0, 0, err
	}
	if err := guard.Check(ctx); err != nil {
		return Subscription{}, auth.Decision{}, auth.Fingerprint{}, 0, 0, err
	}
	return subscription, decision, fingerprint, next, frontier, nil
}

func (s *Service) authorizeDelivery(
	ctx context.Context, subscription Subscription, event Event, decision auth.Decision,
) (bool, error) {
	now := s.now().UTC()
	fresh, guard, err := s.authorize(
		ctx, auth.OperationSubscriptionCreate, event.Evidence, now)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) || shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			return false, nil
		}
		return false, err
	}
	fingerprint, err := auth.AuthorizationFingerprint(fresh)
	if err != nil {
		return false, err
	}
	if fresh.Subject() != decision.Subject() ||
		fingerprint != subscription.AuthorizationFingerprint {
		return false, shoal.NewError(shoal.ErrorUnavailable, "subscription authorization changed")
	}
	if err := s.leases.ValidateDelivery(
		ctx, subscription.AgentID, subscription.AgentGeneration,
	); err != nil {
		return false, err
	}
	if !matchesFilter(subscription.Filter, event) {
		return false, nil
	}
	if err := guard.Check(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) authorize(
	ctx context.Context, operation auth.Operation, evidence []Evidence, now time.Time,
) (auth.Decision, auth.GenerationGuard, error) {
	decision, err := s.resolver.Resolve(ctx)
	if err != nil {
		return auth.Decision{}, auth.GenerationGuard{}, err
	}
	if len(evidence) == 0 {
		if err := decision.Authorize(operation, auth.ResourceRequest{
			AuthorizationDomain: decision.AuthorizationDomain(),
		}, now); err != nil {
			return auth.Decision{}, auth.GenerationGuard{}, err
		}
	} else {
		for _, item := range evidence {
			if err := decision.AuthorizeObject(operation, auth.ResourceRequest{
				AuthorizationDomain: decision.AuthorizationDomain(),
				SourceID:            item.SourceID, PolicyID: item.PolicyID, ObjectID: item.ObjectID,
			}, now); err != nil {
				return auth.Decision{}, auth.GenerationGuard{}, err
			}
		}
	}
	guard, err := auth.NewGenerationGuard(decision, s.generations)
	return decision, guard, err
}

func (s *Service) record(
	ctx context.Context, decision auth.Decision, operation auth.Operation,
	actionID, objectID []byte, evidence []Evidence, now time.Time,
) error {
	return s.auditor.RecordFleetAction(ctx, AuditRecord{
		Operation: operation, ActionID: cloneBytes(actionID),
		RequestID: decision.RequestID(), CorrelationID: []byte(decision.CorrelationID()),
		AuthorizationFingerprint: func() auth.Fingerprint {
			value, _ := auth.AuthorizationFingerprint(decision)
			return value
		}(),
		AuthorizationExpiresAt: decision.AuthenticationExpires(),
		ObjectID:               cloneBytes(objectID),
		Evidence:               cloneEvidence(evidence),
		OccurredAt:             now,
	})
}

func matchesFilter(filter Filter, event Event) bool {
	if len(filter.Kinds) > 0 && !containsString(filter.Kinds, event.Kind) {
		return false
	}
	for _, evidence := range event.Evidence {
		if len(filter.SourceIDs) > 0 && !containsBytes(filter.SourceIDs, evidence.SourceID) {
			return false
		}
		if len(filter.PolicyIDs) > 0 && !containsBytes(filter.PolicyIDs, evidence.PolicyID) {
			return false
		}
	}
	return true
}

func nonDisclosingSubscriptionError(err error) error {
	if errors.Is(err, ErrSubscriptionNotFound) {
		return auth.ObjectNotFound()
	}
	if errors.Is(err, ErrGenerationConflict) {
		return shoal.NewError(shoal.ErrorConflict, "subscription generation conflict")
	}
	return mapContextError(err)
}

func deriveID(tag string, parts ...[]byte) []byte {
	hash := sha256.New()
	hash.Write([]byte(tag))
	for _, part := range parts {
		hash.Write([]byte{0})
		hash.Write(part)
	}
	return hash.Sum(nil)
}
