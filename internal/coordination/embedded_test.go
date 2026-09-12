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

package coordination_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/coordination"
	"github.com/phrocker/shoal-oss/internal/coordination/coordtest"
	"github.com/phrocker/shoal-oss/internal/dirlock"
)

func TestEmbeddedCoordinatorConformance(t *testing.T) {
	directory := t.TempDir()
	coordtest.Run(t, func(owner string) (coordination.Coordinator, error) {
		return coordination.NewEmbeddedCoordinator(directory, owner)
	})
}

func TestEmbeddedCoordinatorRejectsConcurrentProcessAuthority(t *testing.T) {
	directory := t.TempDir()
	first, err := coordination.NewEmbeddedCoordinator(directory, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := first.Acquire(context.Background(), "table/graph")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })

	second, err := coordination.NewEmbeddedCoordinator(directory, "owner-b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Acquire(context.Background(), "table/graph"); !errors.Is(err, coordination.ErrLeaseHeld) ||
		!errors.Is(err, dirlock.ErrLocked) {
		t.Fatalf("second authority = %v, want lease and directory lock conflict", err)
	}
}

func TestEmbeddedCoordinatorLosesAuthorityWhenManifestChanges(t *testing.T) {
	directory := t.TempDir()
	first, err := coordination.NewEmbeddedCoordinator(directory, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := first.Acquire(context.Background(), "table/graph")
	if err != nil {
		t.Fatal(err)
	}
	token := lease.Token()
	manifestPath := filepath.Join(directory, ".shoal-authority.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), `"active": true`, `"active": false`, 1))
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := lease.Renew(context.Background()); !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("renew after manifest change = %v, want ErrLeaseLost", err)
	}
	select {
	case <-lease.Lost():
	default:
		t.Fatal("manifest change did not signal lease loss")
	}

	second, err := coordination.NewEmbeddedCoordinator(directory, "owner-b")
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := second.Acquire(context.Background(), "table/graph")
	if err != nil {
		t.Fatalf("replacement acquire: %v", err)
	}
	defer replacement.Release(context.Background())
	if replacement.Token().Generation <= token.Generation {
		t.Fatalf("replacement generation = %d, want > %d", replacement.Token().Generation, token.Generation)
	}
}

func TestEmbeddedCoordinatorWatchIsResourceScoped(t *testing.T) {
	directory := t.TempDir()
	coordinator, err := coordination.NewEmbeddedCoordinator(directory, "owner")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := coordinator.Watch(ctx, "table/other")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := coordinator.Acquire(context.Background(), "table/graph")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(context.Background())

	select {
	case event := <-events:
		t.Fatalf("unrelated event delivered: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestEmbeddedCoordinatorFailsClosedOnCorruptManifest(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(directory, ".shoal-authority.json"),
		[]byte(`{"version":1,"epoch":0}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	coordinator, err := coordination.NewEmbeddedCoordinator(directory, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Acquire(context.Background(), "table/graph"); err == nil {
		t.Fatal("acquire accepted corrupt authority manifest")
	}

	lock, err := dirlock.Acquire(directory, ".shoal-authority.lock")
	if err != nil {
		t.Fatalf("failed acquisition retained OS lock: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}
