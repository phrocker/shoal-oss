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
	store, err := NewDispatchStore(runtime, visibility)
	if err != nil {
		return nil, err
	}
	return fleet.NewDispatchService(fleet.DispatchConfig{
		Store: store, Registry: registry, Resolver: resolver,
		Recorder: recorder, Events: events, Clock: clock,
	})
}
