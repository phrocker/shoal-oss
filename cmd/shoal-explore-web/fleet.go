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

package main

import (
	"context"
	"reflect"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type configuredFleetExecutor struct {
	reference string
}

type configuredFleetExecutors map[string]fleet.Executor

type fleetInteractionSink struct {
	durable    interaction.Sink
	authorized interaction.ResultSink
}

type boundFleetRegistry struct {
	service  *fleet.Service
	resolver auth.Resolver
}

type boundFleetDispatch struct {
	service  *fleet.DispatchService
	resolver auth.Resolver
}

func newConfiguredFleetExecutors(
	references []string,
) (configuredFleetExecutors, error) {
	result := make(configuredFleetExecutors, len(references))
	for _, reference := range references {
		if reference == "" || strings.TrimSpace(reference) != reference ||
			len(reference) > fleet.MaxExecutorRefBytes {
			return nil, shoal.NewError(
				shoal.ErrorInvalidArgument,
				"fleet executor reference is outside its bound",
			)
		}
		result[reference] = configuredFleetExecutor{reference: reference}
	}
	return result, nil
}

func (r configuredFleetExecutors) ResolveExecutor(
	reference string,
) (fleet.Executor, bool) {
	executor, ok := r[reference]
	return executor, ok
}

func (s fleetInteractionSink) EnsureInteractionSink(ctx context.Context) error {
	return s.durable.EnsureInteractionSink(ctx)
}

func (s fleetInteractionSink) RecordInteraction(
	ctx context.Context, session interaction.Session,
) error {
	_, err := s.authorized.RecordInteractionResult(ctx, session)
	return err
}

func (s fleetInteractionSink) RecordInteractionResult(
	ctx context.Context, session interaction.Session,
) (interaction.Session, error) {
	return s.authorized.RecordInteractionResult(ctx, session)
}

func (s fleetInteractionSink) Record(
	ctx context.Context, session interaction.Session,
) (interaction.Session, error) {
	return s.authorized.RecordInteractionResult(ctx, session)
}

func newBoundFleetRegistry(
	service *fleet.Service,
	resolver auth.Resolver,
) (*boundFleetRegistry, error) {
	if service == nil || isNilFleetDependency(resolver) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"bound fleet registry dependencies are required",
		)
	}
	return &boundFleetRegistry{service: service, resolver: resolver}, nil
}

func newBoundFleetDispatch(
	service *fleet.DispatchService,
	resolver auth.Resolver,
) (*boundFleetDispatch, error) {
	if service == nil || isNilFleetDependency(resolver) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"bound fleet dispatch dependencies are required",
		)
	}
	return &boundFleetDispatch{service: service, resolver: resolver}, nil
}

func isNilFleetDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (r *boundFleetRegistry) requestContext(
	ctx context.Context,
	request fleet.RequestContext,
) (fleet.RequestContext, error) {
	decision, err := r.resolver.Resolve(ctx)
	if err != nil {
		return fleet.RequestContext{}, err
	}
	request.RequestID = decision.RequestID()
	request.CorrelationID = decision.CorrelationID()
	return request, nil
}

func (r *boundFleetRegistry) Register(
	ctx context.Context,
	request fleet.RegisterRequest,
) (fleet.Descriptor, error) {
	bound, err := r.requestContext(ctx, request.Context)
	if err != nil {
		return fleet.Descriptor{}, err
	}
	request.Context = bound
	return r.service.Register(ctx, request)
}

func (r *boundFleetRegistry) Heartbeat(
	ctx context.Context,
	request fleet.HeartbeatRequest,
) (fleet.Descriptor, error) {
	bound, err := r.requestContext(ctx, request.Context)
	if err != nil {
		return fleet.Descriptor{}, err
	}
	request.Context = bound
	return r.service.Heartbeat(ctx, request)
}

func (r *boundFleetRegistry) Revoke(
	ctx context.Context,
	request fleet.RevokeRequest,
) (fleet.Descriptor, error) {
	bound, err := r.requestContext(ctx, request.Context)
	if err != nil {
		return fleet.Descriptor{}, err
	}
	request.Context = bound
	return r.service.Revoke(ctx, request)
}

func (r *boundFleetRegistry) Resolve(
	ctx context.Context,
	request fleet.ResolveRequest,
) (fleet.Resolved, error) {
	bound, err := r.requestContext(ctx, request.Context)
	if err != nil {
		return fleet.Resolved{}, err
	}
	request.Context = bound
	return r.service.Resolve(ctx, request)
}

func (r *boundFleetRegistry) List(
	ctx context.Context,
	request fleet.ListRequest,
) (fleet.ListPage, error) {
	bound, err := r.requestContext(ctx, request.Context)
	if err != nil {
		return fleet.ListPage{}, err
	}
	request.Context = bound
	return r.service.List(ctx, request)
}

func (r *boundFleetDispatch) requestContext(
	ctx context.Context,
	request fleet.RequestContext,
) (fleet.RequestContext, error) {
	decision, err := r.resolver.Resolve(ctx)
	if err != nil {
		return fleet.RequestContext{}, err
	}
	request.RequestID = decision.RequestID()
	request.CorrelationID = decision.CorrelationID()
	return request, nil
}

func (r *boundFleetDispatch) Enqueue(
	ctx context.Context,
	request fleet.EnqueueRequest,
) (fleet.ActionRecord, error) {
	bound, err := r.requestContext(ctx, request.Context)
	if err != nil {
		return fleet.ActionRecord{}, err
	}
	request.Context = bound
	return r.service.Enqueue(ctx, request)
}

func (r *boundFleetDispatch) Claim(
	ctx context.Context,
	request fleet.ClaimRequest,
) (fleet.ActionRecord, error) {
	bound, err := r.requestContext(ctx, request.Context)
	if err != nil {
		return fleet.ActionRecord{}, err
	}
	request.Context = bound
	return r.service.Claim(ctx, request)
}

func (r *boundFleetDispatch) Cancel(
	ctx context.Context,
	request fleet.CancelRequest,
) (fleet.ActionRecord, error) {
	bound, err := r.requestContext(ctx, request.Context)
	if err != nil {
		return fleet.ActionRecord{}, err
	}
	request.Context = bound
	return r.service.Cancel(ctx, request)
}

func (r *boundFleetDispatch) Status(
	ctx context.Context,
	request fleet.StatusRequest,
) (fleet.ActionRecord, error) {
	bound, err := r.requestContext(ctx, request.Context)
	if err != nil {
		return fleet.ActionRecord{}, err
	}
	request.Context = bound
	return r.service.Status(ctx, request)
}

func (r *boundFleetDispatch) Pull(
	ctx context.Context,
	request fleet.PullActionsRequest,
) (fleet.ActionPage, error) {
	bound, err := r.requestContext(ctx, request.Context)
	if err != nil {
		return fleet.ActionPage{}, err
	}
	request.Context = bound
	return r.service.Pull(ctx, request)
}

func (r *boundFleetDispatch) Invoke(
	ctx context.Context,
	request fleet.InvokeRequest,
) (fleet.ActionRecord, error) {
	bound, err := r.requestContext(ctx, request.Enqueue.Context)
	if err != nil {
		return fleet.ActionRecord{}, err
	}
	request.Enqueue.Context = bound
	return r.service.Invoke(ctx, request)
}
