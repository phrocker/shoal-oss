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

// Package recovery provides bounded, non-authoritative transaction scanning.
package recovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/allocator"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
)

type CandidateSource interface {
	Candidates(context.Context, coordination.DomainID, int) ([]coordination.TXN, error)
}

type Coordinator interface {
	Inspect(context.Context, coordination.TXN) (transaction.Snapshot, error)
	Recover(context.Context, coordination.TXN, coordination.OwnerID, time.Time, transaction.Authority) (transaction.Result, error)
}

type Config struct {
	Domain      coordination.DomainID
	Owner       coordination.OwnerID
	Authority   transaction.Authority
	Source      CandidateSource
	Coordinator Coordinator
	Clock       func() time.Time
	Lease       time.Duration
	Limit       int
	Concurrency int
	MaxRounds   int
	Backoff     time.Duration
}

type Worker struct {
	config Config
}

func New(config Config) (*Worker, error) {
	if err := config.Domain.Validate(); err != nil {
		return nil, err
	}
	if err := config.Owner.Validate(); err != nil {
		return nil, err
	}
	if config.Source == nil || config.Coordinator == nil {
		return nil, errors.New("explorer recovery: source and coordinator are required")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.Lease == 0 {
		config.Lease = time.Minute
	}
	if config.Limit == 0 {
		config.Limit = 1024
	}
	if config.Concurrency == 0 {
		config.Concurrency = 4
	}
	if config.MaxRounds == 0 {
		config.MaxRounds = 3
	}
	if config.Backoff == 0 {
		config.Backoff = 25 * time.Millisecond
	}
	if config.Lease < time.Second || config.Lease > 24*time.Hour ||
		config.Limit < 1 || config.Limit > 10_000 ||
		config.Concurrency < 1 || config.Concurrency > 256 ||
		config.MaxRounds < 1 || config.MaxRounds > 100 ||
		config.Backoff < 0 || config.Backoff > time.Minute {
		return nil, errors.New("explorer recovery: configuration is outside its bound")
	}
	return &Worker{config: config}, nil
}

func (w *Worker) RunOnce(ctx context.Context) error {
	candidates, err := w.config.Source.Candidates(ctx, w.config.Domain, w.config.Limit+1)
	if err != nil {
		return errors.Join(transaction.ErrUnavailable, err)
	}
	if len(candidates) > w.config.Limit {
		return fmt.Errorf("%w: recovery queue exceeds its configured bound", transaction.ErrUnavailable)
	}
	sort.Slice(candidates, func(i, j int) bool { return bytes.Compare(candidates[i], candidates[j]) < 0 })
	sem := make(chan struct{}, w.config.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var combined error
	for _, txn := range candidates {
		txn := append(coordination.TXN(nil), txn...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				mu.Lock()
				combined = errors.Join(combined, ctx.Err())
				mu.Unlock()
				return
			}
			if err := w.recoverOne(ctx, txn); err != nil {
				mu.Lock()
				combined = errors.Join(combined, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return combined
}

func (w *Worker) recoverOne(ctx context.Context, txn coordination.TXN) error {
	for round := 0; round < w.config.MaxRounds; round++ {
		snapshot, err := w.config.Coordinator.Inspect(ctx, txn)
		if err != nil {
			if errors.Is(err, transaction.ErrNotFound) {
				return nil
			}
			return err
		}
		if snapshot.Root.State.Terminal() {
			if snapshot.Root.State != coordination.StateCommitted {
				return nil
			}
		}
		now := w.config.Clock().UTC()
		if snapshot.Root.State.Nonterminal() && snapshot.Lease.LeaseUntil.After(now) {
			return nil
		}
		_, err = w.config.Coordinator.Recover(
			ctx, txn, w.config.Owner, now.Add(w.config.Lease), w.config.Authority,
		)
		if err == nil || errors.Is(err, transaction.ErrConflict) ||
			errors.Is(err, transaction.ErrQuarantined) {
			return err
		}
		if !errors.Is(err, transaction.ErrUnavailable) {
			return err
		}
		timer := time.NewTimer(w.config.Backoff << min(round, 8))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return transaction.ErrUnavailable
}

type PrefixScanner interface {
	ScanPrefixFrom(context.Context, []byte, []byte, []byte, []byte, []byte, int) ([]allocator.Cell, error)
}

type BandedSource struct {
	Scanner           PrefixScanner
	ControlVisibility []byte
}

func (s BandedSource) Candidates(
	ctx context.Context,
	domain coordination.DomainID,
	limit int,
) ([]coordination.TXN, error) {
	if s.Scanner == nil || limit < 1 {
		return nil, errors.New("explorer recovery: invalid banded source")
	}
	result := make([]coordination.TXN, 0, limit)
	for band := 0; band < 256; band++ {
		prefix := []byte{1, byte(coordination.RowTxn), byte(band)}
		prefix = append(prefix, coordination.E(domain)...)
		cells, err := s.Scanner.ScanPrefixFrom(
			ctx, prefix, prefix, []byte("s"), []byte("root"), s.ControlVisibility, limit-len(result),
		)
		if err != nil {
			return nil, err
		}
		for _, cell := range cells {
			parsed, parseErr := coordination.ParseCoordinationRow(cell.Coordinate.Row)
			if parseErr != nil || parsed.Kind != coordination.RowTxn {
				return nil, errors.New("explorer recovery: invalid transaction scan row")
			}
			result = append(result, append(coordination.TXN(nil), parsed.TXN...))
			if len(result) == limit {
				return result, nil
			}
		}
	}
	return result, nil
}
