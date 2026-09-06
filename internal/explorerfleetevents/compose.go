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
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleetevents"
	"github.com/phrocker/shoal-oss/pkg/interaction"
)

// ConfigureRuntime adds the fleet-event physical table before
// explorercoord.Open. Repeated calls are safe.
func ConfigureRuntime(config *explorercoord.Config) {
	if config == nil {
		return
	}
	for _, table := range config.PhysicalTables {
		if table == Table {
			return
		}
	}
	config.PhysicalTables = append(append([]string(nil), config.PhysicalTables...), Table)
}

// Compose constructs the production fleet-event service around the shared
// transaction runtime, authorization generation source, durable interaction
// recorder, and durable registry lease validator.
func Compose(
	runtime *explorercoord.Runtime,
	domain coordination.DomainID,
	resolver auth.Resolver,
	generations auth.GenerationReader,
	interactionRecorder *interaction.Recorder,
	leases fleetevents.LeaseValidator,
	cursorKey []byte,
	clock func() time.Time,
) (*fleetevents.Service, error) {
	service, _, err := ComposeWithPublisher(
		runtime, domain, resolver, generations, interactionRecorder,
		leases, cursorKey, clock,
	)
	return service, err
}

// ComposeWithPublisher constructs the public subscription service and the
// trusted dispatch-transition publisher over the same durable adapter.
func ComposeWithPublisher(
	runtime *explorercoord.Runtime,
	domain coordination.DomainID,
	resolver auth.Resolver,
	generations auth.GenerationReader,
	interactionRecorder *interaction.Recorder,
	leases fleetevents.LeaseValidator,
	cursorKey []byte,
	clock func() time.Time,
) (*fleetevents.Service, *ActionEventPublisher, error) {
	backend, err := New(runtime, domain)
	if err != nil {
		return nil, nil, err
	}
	auditor, err := fleetevents.NewInteractionAuditor(interactionRecorder)
	if err != nil {
		return nil, nil, err
	}
	service, err := fleetevents.New(fleetevents.Config{
		Backend: backend, Resolver: resolver, GenerationReader: generations,
		LeaseValidator: leases, Auditor: auditor, CursorKey: cursorKey,
		Clock: clock,
	})
	if err != nil {
		return nil, nil, err
	}
	publisher, err := NewActionEventPublisher(
		service, resolver, clock)
	if err != nil {
		return nil, nil, err
	}
	return service, publisher, nil
}
