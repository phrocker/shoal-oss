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

package explorerfleet

import (
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// ConfigureRuntime adds the fleet physical tables before explorercoord.Open.
func ConfigureRuntime(config explorercoord.Config) explorercoord.Config {
	for _, wanted := range []string{Table, DispatchTable} {
		found := false
		for _, table := range config.PhysicalTables {
			if table == wanted {
				found = true
				break
			}
		}
		if !found {
			config.PhysicalTables = append(config.PhysicalTables, wanted)
		}
	}
	return config
}

// Compose constructs the registry with a host-supplied lifecycle
// recorder. Production startup owns the authorized ResultSink adapter.
func Compose(
	runtime *explorercoord.Runtime,
	resolver auth.Resolver,
	recorder fleet.LifecycleRecorder,
	snapshots fleet.InteractionSnapshotProvider,
	executors fleet.ExecutorRegistry,
	visibility []byte,
	clock func() time.Time,
) (*fleet.Service, error) {
	store, err := NewStore(runtime, visibility)
	if err != nil {
		return nil, err
	}
	return fleet.NewService(fleet.Config{
		Store: store, Resolver: resolver, Recorder: recorder, Snapshots: snapshots,
		Executors: executors, Clock: clock,
	})
}
