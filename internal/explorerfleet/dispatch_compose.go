// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// ComposeDispatch constructs the production dispatch service over the shared
// embedded runtime. Recorder and events are explicit fail-closed host seams:
// production startup must provide the durable interaction and fleet-event
// adapters rather than silently substituting no-op implementations.
func ComposeDispatch(
	runtime *explorercoord.Runtime,
	registry *fleet.Service,
	resolver auth.Resolver,
	recorder fleet.ActionRecorder,
	events fleet.ActionEventPublisher,
	visibility []byte,
	clock func() time.Time,
) (*fleet.DispatchService, error) {
	return ComposeDispatchWithAttestations(runtime, registry, resolver,
		recorder, events, visibility, clock, nil)
}

// ComposeDispatchWithAttestations is ComposeDispatch with the executor
// attestation read every claim on an attestation-required action makes. Nil
// means none is ever current, so those claims are refused.
func ComposeDispatchWithAttestations(
	runtime *explorercoord.Runtime,
	registry *fleet.Service,
	resolver auth.Resolver,
	recorder fleet.ActionRecorder,
	events fleet.ActionEventPublisher,
	visibility []byte,
	clock func() time.Time,
	attestations fleet.ExecutorAttestations,
) (*fleet.DispatchService, error) {
	store, err := NewDispatchStore(runtime, visibility)
	if err != nil {
		return nil, err
	}
	return fleet.NewDispatchService(fleet.DispatchConfig{
		Store: store, Registry: registry, Resolver: resolver,
		Recorder: recorder, Events: events, Clock: clock,
		Attestations: attestations,
	})
}
