/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
)

type fixedSource []coordination.TXN

func (s fixedSource) Candidates(context.Context, coordination.DomainID, int) ([]coordination.TXN, error) {
	return s, nil
}

type fakeCoordinator struct {
	snapshot  transaction.Snapshot
	recovered int
}

func (f *fakeCoordinator) Inspect(context.Context, coordination.TXN) (transaction.Snapshot, error) {
	return f.snapshot, nil
}

func (f *fakeCoordinator) Recover(context.Context, coordination.TXN, coordination.OwnerID, time.Time, transaction.Authority) (transaction.Result, error) {
	f.recovered++
	return transaction.Result{Epoch: f.snapshot.Root.Epoch}, nil
}

func TestWorkerBoundsAndAuthoritativeRecheck(t *testing.T) {
	now := time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC)
	coordinator := &fakeCoordinator{snapshot: transaction.Snapshot{
		Root: coordination.TxnRootV3{State: coordination.StateClaimed},
		Lease: coordination.TxnLeaseV1{
			Generation: 1, Owner: coordination.OwnerID("old"), Fence: 1, LeaseUntil: now.Add(-time.Minute),
		},
	}}
	worker, err := New(Config{
		Domain: coordination.DomainID("domain"), Owner: coordination.OwnerID("recovery"),
		Source:      fixedSource{coordination.TXN("b"), coordination.TXN("a")},
		Coordinator: coordinator, Clock: func() time.Time { return now },
		Authority: transaction.Authority{}, Limit: 2, Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOnce(context.Background()); err != nil || coordinator.recovered != 2 {
		t.Fatalf("RunOnce = recovered %d, %v", coordinator.recovered, err)
	}
	worker.config.Source = fixedSource{coordination.TXN("a"), coordination.TXN("b"), coordination.TXN("c")}
	if err := worker.RunOnce(context.Background()); !errors.Is(err, transaction.ErrUnavailable) {
		t.Fatalf("overflow queue = %v", err)
	}
}
