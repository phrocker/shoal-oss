// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

// Package violating is a deliberately violating fixture for the authority
// check in authority_test.go: every function here reaches a forbidden
// operation a different way, and the check must flag each one. It is under
// testdata, so it is never built.
package violating

import (
	"context"
	"reflect"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

type enqueuer interface {
	Enqueue(context.Context, fleet.EnqueueRequest) (fleet.ActionRecord, error)
}

// Direct calls Enqueue on the dispatch service.
func Direct(ctx context.Context, s *fleet.DispatchService) {
	_, _ = s.Enqueue(ctx, fleet.EnqueueRequest{})
}

// MethodValue takes Invoke as a method value without calling it here.
func MethodValue(s *fleet.DispatchService) any {
	f := s.Invoke
	return f
}

// MethodExpression names Enqueue through the type.
func MethodExpression() any {
	return (*fleet.DispatchService).Enqueue
}

// ViaInterface reaches Enqueue through a locally declared interface.
func ViaInterface(ctx context.Context, e enqueuer) {
	_, _ = e.Enqueue(ctx, fleet.EnqueueRequest{})
}

// Registry calls Register on the fleet registry.
func Registry(ctx context.Context, s *fleet.Service) {
	_, _ = s.Register(ctx, fleet.RegisterRequest{})
}

// Reflective looks the method up by name.
func Reflective(s *fleet.DispatchService) {
	_ = reflect.ValueOf(s).MethodByName("Enqueue")
}
