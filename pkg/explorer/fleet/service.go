// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package fleet

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type Config struct {
	Store     Store
	Resolver  auth.Resolver
	Recorder  LifecycleRecorder
	Snapshots InteractionSnapshotProvider
	Executors ExecutorRegistry
	Clock     func() time.Time
}

type Service struct {
	store     Store
	resolver  auth.Resolver
	recorder  LifecycleRecorder
	snapshots InteractionSnapshotProvider
	executors ExecutorRegistry
	clock     func() time.Time
}

func NewService(config Config) (*Service, error) {
	if config.Store == nil || config.Resolver == nil || config.Recorder == nil ||
		config.Snapshots == nil ||
		config.Executors == nil || config.Clock == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet registry dependencies are required")
	}
	return &Service{
		store: config.Store, resolver: config.Resolver, recorder: config.Recorder,
		snapshots: config.Snapshots,
		executors: config.Executors, clock: config.Clock,
	}, nil
}

func (s *Service) Register(ctx context.Context, request RegisterRequest) (Descriptor, error) {
	ctx, cancel := s.withRequestDeadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.begin(ctx, auth.OperationAgentRegister, request.Context)
	if err != nil {
		return Descriptor{}, err
	}
	if err := validateGeneration(request.ExpectedGeneration); err != nil {
		return Descriptor{}, err
	}
	if err := shoal.ValidateRequiredID("registration key", request.RegistrationKey); err != nil {
		return Descriptor{}, err
	}
	spec, err := request.Spec.canonical(now)
	if err != nil {
		return Descriptor{}, err
	}
	if _, ok := s.executors.ResolveExecutor(spec.ExecutorRef); !ok {
		return Descriptor{}, shoal.NewError(shoal.ErrorInvalidArgument, "executor reference is not registered by the host")
	}
	if err := authorizeScopes(decision, auth.OperationAgentRegister, spec.ID,
		spec.AuthorizationDomain, spec.Scopes, now); err != nil {
		return Descriptor{}, err
	}
	if spec.ParentID != "" {
		if err := authorizeScopes(decision, auth.OperationDelegate, spec.ID,
			spec.AuthorizationDomain, spec.Scopes, now); err != nil {
			return Descriptor{}, err
		}
		parent, err := s.active(ctx, spec.ParentID, now, nil)
		if err != nil {
			return Descriptor{}, err
		}
		if parent.Subject != decision.Subject() ||
			!bytes.Equal(parent.AuthorizationDomain, spec.AuthorizationDomain) ||
			!scopesSubset(spec.Scopes, parent.Scopes) ||
			!capabilitiesSubset(spec.Capabilities, parent.Capabilities) ||
			spec.LeaseExpiresAt.After(parent.LeaseExpiresAt) {
			return Descriptor{}, shoal.NewError(shoal.ErrorUnauthorized, "delegated agent exceeds its parent")
		}
	}
	if request.ExpectedGeneration > 0 {
		current, err := s.store.Get(ctx, spec.ID)
		if err != nil {
			return Descriptor{}, concealStoreRead(err)
		}
		if current.Descriptor.Subject != decision.Subject() {
			return Descriptor{}, auth.ObjectNotFound()
		}
		if descriptorExpired(current.Descriptor, now) {
			return Descriptor{}, auth.ObjectNotFound()
		}
		if !bytes.Equal(current.Descriptor.AuthorizationDomain, spec.AuthorizationDomain) ||
			!scopesSubset(spec.Scopes, current.Descriptor.Scopes) ||
			!capabilitiesSubset(spec.Capabilities, current.Descriptor.Capabilities) {
			return Descriptor{}, shoal.NewError(shoal.ErrorUnauthorized, "agent update widens authorization")
		}
		if current.Descriptor.ParentID != spec.ParentID {
			return Descriptor{}, shoal.NewError(shoal.ErrorUnauthorized, "agent parent migration is denied")
		}
	}
	descriptor := Descriptor{
		ID: spec.ID, Generation: request.ExpectedGeneration + 1,
		Subject: decision.Subject(), Actor: decision.Actor(), ParentID: spec.ParentID,
		AuthorizationDomain: spec.AuthorizationDomain, Scopes: spec.Scopes,
		ExecutorRef: spec.ExecutorRef, Capabilities: spec.Capabilities,
		LeaseExpiresAt: spec.LeaseExpiresAt, UpdatedAt: now.UTC(),
	}
	if err := s.record(ctx, decision, request.Context, auth.OperationAgentRegister, descriptor.ID); err != nil {
		return Descriptor{}, err
	}
	stored, err := s.store.Apply(ctx, Mutation{
		RegistrationKey:    request.RegistrationKey,
		ExpectedGeneration: request.ExpectedGeneration, Descriptor: descriptor,
	})
	if err != nil {
		return Descriptor{}, err
	}
	return cloneDescriptor(stored.Descriptor), nil
}

func (s *Service) Heartbeat(ctx context.Context, request HeartbeatRequest) (Descriptor, error) {
	ctx, cancel := s.withRequestDeadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.begin(ctx, auth.OperationAgentHeartbeat, request.Context)
	if err != nil {
		return Descriptor{}, err
	}
	if err := validateMutationIdentity(request.ID, request.RegistrationKey, request.ExpectedGeneration); err != nil {
		return Descriptor{}, err
	}
	currentDescriptor, err := s.active(ctx, request.ID, now, nil)
	if err != nil {
		return Descriptor{}, concealStoreRead(err)
	}
	current := Stored{Descriptor: currentDescriptor}
	if err := authorizeDescriptor(decision, auth.OperationAgentHeartbeat, current.Descriptor, now); err != nil {
		return Descriptor{}, err
	}
	if current.Descriptor.Subject != decision.Subject() {
		return Descriptor{}, auth.ObjectNotFound()
	}
	if request.LeaseExpiresAt.Location() != time.UTC || !now.Before(request.LeaseExpiresAt) ||
		request.LeaseExpiresAt.Sub(now) > MaxLease {
		return Descriptor{}, shoal.NewError(shoal.ErrorInvalidArgument, "agent lease is outside its bound")
	}
	if current.Descriptor.ParentID != "" {
		parent, err := s.active(ctx, current.Descriptor.ParentID, now, nil)
		if err != nil {
			return Descriptor{}, err
		}
		if request.LeaseExpiresAt.After(parent.LeaseExpiresAt) {
			return Descriptor{}, shoal.NewError(shoal.ErrorUnauthorized, "delegated agent lease exceeds its parent")
		}
	}
	next := cloneDescriptor(current.Descriptor)
	next.Generation = request.ExpectedGeneration + 1
	next.Actor = decision.Actor()
	next.LeaseExpiresAt = request.LeaseExpiresAt
	next.UpdatedAt = now.UTC()
	if err := s.record(ctx, decision, request.Context, auth.OperationAgentHeartbeat, request.ID); err != nil {
		return Descriptor{}, err
	}
	stored, err := s.store.Apply(ctx, Mutation{
		RegistrationKey:    request.RegistrationKey,
		ExpectedGeneration: request.ExpectedGeneration, Descriptor: next,
	})
	if err != nil {
		return Descriptor{}, err
	}
	return cloneDescriptor(stored.Descriptor), nil
}

func (s *Service) Revoke(ctx context.Context, request RevokeRequest) (Descriptor, error) {
	ctx, cancel := s.withRequestDeadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.begin(ctx, auth.OperationAgentRevoke, request.Context)
	if err != nil {
		return Descriptor{}, err
	}
	if err := validateMutationIdentity(request.ID, request.RegistrationKey, request.ExpectedGeneration); err != nil {
		return Descriptor{}, err
	}
	current, err := s.store.Get(ctx, request.ID)
	if err != nil {
		return Descriptor{}, concealStoreRead(err)
	}
	if err := authorizeDescriptor(decision, auth.OperationAgentRevoke, current.Descriptor, now); err != nil {
		return Descriptor{}, err
	}
	next := cloneDescriptor(current.Descriptor)
	next.Generation = request.ExpectedGeneration + 1
	next.Actor = decision.Actor()
	next.RevokedAt = now.UTC()
	next.UpdatedAt = now.UTC()
	if err := s.record(ctx, decision, request.Context, auth.OperationAgentRevoke, request.ID); err != nil {
		return Descriptor{}, err
	}
	stored, err := s.store.Apply(ctx, Mutation{
		RegistrationKey:    request.RegistrationKey,
		ExpectedGeneration: request.ExpectedGeneration, Descriptor: next,
	})
	if err != nil {
		return Descriptor{}, err
	}
	return cloneDescriptor(stored.Descriptor), nil
}

func (s *Service) Resolve(ctx context.Context, request ResolveRequest) (Resolved, error) {
	ctx, cancel := s.withRequestDeadline(ctx, request.Context)
	defer cancel()
	if err := shoal.ValidateRequiredID("agent ID", request.ID); err != nil {
		return Resolved{}, err
	}
	decision, now, err := s.begin(ctx, auth.OperationAgentResolve, request.Context)
	if err != nil {
		return Resolved{}, err
	}
	descriptor, err := s.authorizedActive(ctx, decision, request.ID, now)
	if err != nil {
		return Resolved{}, err
	}
	if err := s.record(ctx, decision, request.Context, auth.OperationAgentResolve, request.ID); err != nil {
		return Resolved{}, err
	}
	executor, ok := s.executors.ResolveExecutor(descriptor.ExecutorRef)
	if !ok {
		return Resolved{}, shoal.NewError(shoal.ErrorUnavailable, "agent executor is unavailable")
	}
	return Resolved{Descriptor: cloneDescriptor(descriptor), Executor: executor}, nil
}

func (s *Service) List(ctx context.Context, request ListRequest) (ListPage, error) {
	ctx, cancel := s.withRequestDeadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.begin(ctx, auth.OperationAgentResolve, request.Context)
	if err != nil {
		return ListPage{}, err
	}
	if _, err := decision.IntersectSourceIDs(auth.OperationAgentResolve,
		decision.AuthorizationDomain(), request.SourceIDs, now); err != nil {
		return ListPage{}, err
	}
	if _, err := decision.IntersectPolicyIDs(auth.OperationAgentResolve,
		decision.AuthorizationDomain(), request.PolicyIDs, now); err != nil {
		return ListPage{}, err
	}
	if request.Limit == 0 {
		request.Limit = DefaultListPageSize
	}
	if request.Limit > MaxListPageSize ||
		len(request.Cursor) > MaxListCursorBytes {
		return ListPage{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet list pagination is outside its bound",
		)
	}
	if err := s.record(ctx, decision, request.Context, auth.OperationAgentResolve, "fleet-list"); err != nil {
		return ListPage{}, err
	}
	const scanMultiplier = 8
	maxScanned := int(request.Limit) * scanMultiplier
	cursor := request.Cursor
	result := make([]Descriptor, 0, request.Limit)
	for scanned, pages := 0, 0; scanned < maxScanned && pages < scanMultiplier; pages++ {
		pageCursor := cursor
		pageLimit := min(MaxListPageSize, uint32(maxScanned-scanned))
		page, err := s.store.ListPage(ctx, cursor, pageLimit)
		if err != nil {
			return ListPage{}, err
		}
		if len(page.Items) > int(pageLimit) ||
			(len(page.Items) == 0 && page.NextCursor != "" &&
				page.NextCursor == pageCursor) {
			return ListPage{}, shoal.NewError(
				shoal.ErrorInternal, "fleet store returned an invalid page")
		}
		for index, item := range page.Items {
			if item.Cursor == "" || item.Cursor == cursor {
				return ListPage{}, shoal.NewError(
					shoal.ErrorInternal,
					"fleet store returned an invalid item cursor",
				)
			}
			scanned++
			cursor = item.Cursor
			descriptor := item.Stored.Descriptor
			if !matchesFilter(descriptor, request) || descriptorExpired(descriptor, now) {
				continue
			}
			if authorizeDescriptor(decision, auth.OperationAgentResolve, descriptor, now) != nil {
				continue
			}
			if descriptor.ParentID != "" {
				if _, err := s.authorizedActive(ctx, decision, descriptor.ParentID, now); err != nil {
					continue
				}
			}
			result = append(result, cloneDescriptor(descriptor))
			if len(result) == int(request.Limit) {
				next := ""
				if index+1 < len(page.Items) || page.NextCursor != "" {
					next = cursor
				}
				return ListPage{
					Descriptors: result,
					NextCursor:  next,
				}, nil
			}
		}
		if page.NextCursor == "" {
			return ListPage{Descriptors: result}, nil
		}
		cursor = page.NextCursor
	}
	return ListPage{Descriptors: result, NextCursor: cursor}, nil
}

// ValidateDelivery is the narrow structural adapter consumed by durable event
// delivery. It resolves a fresh decision from the bound request context,
// rechecks agent_resolve against every descriptor scope, verifies the complete
// parent delegation chain is still active and narrowing, and pins the exact
// positive descriptor generation. It deliberately accepts no caller-supplied
// Decision and returns no executor or descriptor, preventing a dispatcher from
// transferring trusted session state or using stale authorization material.
func (s *Service) ValidateDelivery(
	ctx context.Context,
	agentID shoal.ID,
	expectedGeneration int64,
) error {
	return s.validateDelivery(ctx, agentID, expectedGeneration, true)
}

// ValidateCurrentDelivery is the non-generation-pinned counterpart used by
// integrations whose durable record intentionally follows the current agent
// generation. Every registry transition is CAS-protected and non-widening;
// this method still rechecks the current lease, revocation, authorization, and
// complete parent chain on every call.
func (s *Service) ValidateCurrentDelivery(
	ctx context.Context,
	agentID shoal.ID,
) error {
	return s.validateDelivery(ctx, agentID, 0, false)
}

func (s *Service) validateDelivery(
	ctx context.Context,
	agentID shoal.ID,
	expectedGeneration int64,
	pinned bool,
) error {
	if err := shoal.ValidateRequiredID("agent ID", agentID); err != nil {
		return err
	}
	if pinned && expectedGeneration <= 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"expected agent generation must be positive",
		)
	}
	decision, err := s.resolver.Resolve(ctx)
	if err != nil {
		return err
	}
	now := s.clock().UTC()
	descriptor, err := s.authorizedActive(ctx, decision, agentID, now)
	if err != nil {
		return err
	}
	if pinned && descriptor.Generation != expectedGeneration {
		return auth.ObjectNotFound()
	}
	return nil
}

func (s *Service) begin(ctx context.Context, operation auth.Operation, request RequestContext) (auth.Decision, time.Time, error) {
	now := s.clock().UTC()
	if err := request.validate(now); err != nil {
		return auth.Decision{}, time.Time{}, err
	}
	decision, err := s.resolver.Resolve(ctx)
	if err != nil {
		return auth.Decision{}, time.Time{}, err
	}
	if request.RequestID != decision.RequestID() ||
		request.CorrelationID != decision.CorrelationID() {
		return auth.Decision{}, time.Time{}, shoal.NewError(shoal.ErrorUnauthorized, "request identity does not match authentication")
	}
	if err := decision.Authorize(operation, auth.ResourceRequest{
		AuthorizationDomain: decision.AuthorizationDomain(),
	}, now); err != nil {
		return auth.Decision{}, time.Time{}, err
	}
	return decision, now, nil
}

func (s *Service) withRequestDeadline(
	ctx context.Context,
	request RequestContext,
) (context.Context, context.CancelFunc) {
	now := s.clock().UTC()
	if request.Deadline.IsZero() || !now.Before(request.Deadline) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		return canceled, func() {}
	}
	return context.WithTimeout(ctx, request.Deadline.Sub(now))
}

func (s *Service) authorizedActive(ctx context.Context, decision auth.Decision, id shoal.ID, now time.Time) (Descriptor, error) {
	descriptor, err := s.active(ctx, id, now, nil)
	if err != nil {
		return Descriptor{}, err
	}
	if err := authorizeDescriptor(decision, auth.OperationAgentResolve, descriptor, now); err != nil {
		return Descriptor{}, err
	}
	return descriptor, nil
}

// InteractionSnapshot returns a fresh authoritative corpus snapshot for
// downstream fleet lifecycle recording.
func (s *Service) InteractionSnapshot(
	ctx context.Context,
) (explorer.Snapshot, error) {
	if s == nil || s.snapshots == nil {
		return explorer.Snapshot{}, shoal.NewError(
			shoal.ErrorUnavailable,
			"fleet interaction snapshot provider is unavailable",
		)
	}
	return s.snapshots.InteractionSnapshot(ctx)
}

func (s *Service) active(
	ctx context.Context,
	id shoal.ID,
	now time.Time,
	seen map[shoal.ID]struct{},
) (Descriptor, error) {
	if seen == nil {
		seen = make(map[shoal.ID]struct{})
	}
	if _, exists := seen[id]; exists {
		return Descriptor{}, auth.ObjectNotFound()
	}
	seen[id] = struct{}{}
	stored, err := s.store.Get(ctx, id)
	if err != nil {
		return Descriptor{}, concealStoreRead(err)
	}
	descriptor := stored.Descriptor
	if descriptorExpired(descriptor, now) {
		return Descriptor{}, auth.ObjectNotFound()
	}
	if descriptor.ParentID != "" {
		parent, err := s.active(ctx, descriptor.ParentID, now, seen)
		if err != nil ||
			descriptor.Subject != parent.Subject ||
			!bytes.Equal(descriptor.AuthorizationDomain, parent.AuthorizationDomain) ||
			!scopesSubset(descriptor.Scopes, parent.Scopes) ||
			!capabilitiesSubset(descriptor.Capabilities, parent.Capabilities) ||
			descriptor.LeaseExpiresAt.After(parent.LeaseExpiresAt) {
			return Descriptor{}, auth.ObjectNotFound()
		}
	}
	return descriptor, nil
}

func (s *Service) record(
	ctx context.Context,
	decision auth.Decision,
	request RequestContext,
	operation auth.Operation,
	id shoal.ID,
) error {
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return err
	}
	snapshot, err := s.snapshots.InteractionSnapshot(ctx)
	if err != nil {
		return err
	}
	return s.recorder.RecordLifecycle(ctx, Lifecycle{
		Operation: operation, RequestID: decision.RequestID(),
		CorrelationID: decision.CorrelationID(), Subject: decision.Subject(),
		Actor: decision.Actor(), ClientID: decision.ClientID(),
		OnBehalfOf: decision.OnBehalfOf(), AgentID: id,
		ReasonCode: request.ReasonCode, ReasonDetail: request.ReasonDetail,
		Deadline:                 request.Deadline.UnixNano(),
		AuthorizationFingerprint: fingerprint,
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		AuditPurpose:             decision.AuditPurpose(),
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf.UTC(),
	})
}

func authorizeScopes(decision auth.Decision, operation auth.Operation, id shoal.ID, domain []byte, scopes []Scope, now time.Time) error {
	for _, scope := range scopes {
		if err := decision.AuthorizeObject(operation, auth.ResourceRequest{
			AuthorizationDomain: domain, SourceID: scope.SourceID,
			PolicyID: scope.PolicyID, ObjectID: id,
		}, now); err != nil {
			return err
		}
	}
	return nil
}

func authorizeDescriptor(decision auth.Decision, operation auth.Operation, descriptor Descriptor, now time.Time) error {
	return authorizeScopes(decision, operation, descriptor.ID,
		descriptor.AuthorizationDomain, descriptor.Scopes, now)
}

func validateGeneration(generation int64) error {
	if generation < 0 || generation == math.MaxInt64 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"expected generation is outside the incrementable range",
		)
	}
	return nil
}

func validateMutationIdentity(id, key shoal.ID, generation int64) error {
	if err := shoal.ValidateRequiredID("agent ID", id); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID("registration key", key); err != nil {
		return err
	}
	if generation <= 0 || generation == math.MaxInt64 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"expected generation must be positive and incrementable",
		)
	}
	return nil
}

func descriptorExpired(descriptor Descriptor, now time.Time) bool {
	return !descriptor.RevokedAt.IsZero() || !now.Before(descriptor.LeaseExpiresAt)
}

func scopesSubset(child, parent []Scope) bool {
	for _, wanted := range child {
		found := false
		for _, allowed := range parent {
			if bytes.Equal(wanted.SourceID, allowed.SourceID) &&
				bytes.Equal(wanted.PolicyID, allowed.PolicyID) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func capabilitiesSubset(child, parent []Capability) bool {
	for _, wantedCapability := range child {
		var allowed *Capability
		for i := range parent {
			if parent[i].Name == wantedCapability.Name {
				allowed = &parent[i]
				break
			}
		}
		if allowed == nil {
			return false
		}
		for _, wantedAction := range wantedCapability.Actions {
			found := false
			for _, allowedAction := range allowed.Actions {
				if wantedAction.Name == allowedAction.Name &&
					bytes.Equal(wantedAction.InputSchema, allowedAction.InputSchema) &&
					bytes.Equal(wantedAction.OutputSchema, allowedAction.OutputSchema) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func matchesFilter(descriptor Descriptor, request ListRequest) bool {
	if len(request.SourceIDs) == 0 && len(request.PolicyIDs) == 0 {
		return true
	}
	for _, scope := range descriptor.Scopes {
		if containsBytes(request.SourceIDs, scope.SourceID) &&
			containsBytes(request.PolicyIDs, scope.PolicyID) {
			return true
		}
	}
	return false
}

func containsBytes(values [][]byte, value []byte) bool {
	if len(values) == 0 {
		return true
	}
	for _, candidate := range values {
		if bytes.Equal(candidate, value) {
			return true
		}
	}
	return false
}

func concealStoreRead(err error) error {
	if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		return auth.ObjectNotFound()
	}
	return err
}
