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

package transaction

import (
	"bytes"
	"context"
	"errors"

	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/allocator"
)

type AccumuloConfig = allocator.AccumuloConfig
type AccumuloStore = allocator.AccumuloStore

func NewAccumuloStore(config AccumuloConfig) (*AccumuloStore, error) {
	return allocator.NewAccumuloStore(config)
}

// TrustedCell is the complete physical mutation mapping produced only by a
// trusted Explorer adapter. Public APIs never accept it.
type TrustedCell struct {
	Table      []byte
	Row        []byte
	Family     []byte
	Qualifier  []byte
	Visibility []byte
	Timestamp  int64
	Value      []byte
}

type TrustedPhysicalSink interface {
	WriteTrusted(context.Context, []TrustedCell) error
	ReadTrusted(context.Context, []TrustedCell) ([]TrustedCell, error)
}

type AccumuloPhysicalAdapter struct {
	sink TrustedPhysicalSink
}

func NewAccumuloPhysicalAdapter(sink TrustedPhysicalSink) (*AccumuloPhysicalAdapter, error) {
	if sink == nil {
		return nil, errors.New("explorer transaction: trusted Accumulo physical sink is required")
	}
	return &AccumuloPhysicalAdapter{sink: sink}, nil
}

func mapTrusted(epoch coordination.Epoch, cells []PhysicalCell) []TrustedCell {
	result := make([]TrustedCell, len(cells))
	for i, cell := range cells {
		timestamp := int64(cell.Entry.ExplicitTimestamp)
		if cell.Entry.EpochSlot == coordination.EpochSlotContent {
			timestamp = int64(epoch)
		}
		result[i] = TrustedCell{
			Table: append([]byte(nil), cell.Entry.Table...), Row: append([]byte(nil), cell.Entry.Row...),
			Family:     append([]byte(nil), cell.Entry.ColumnFamily...),
			Qualifier:  append([]byte(nil), cell.Entry.ColumnQualifier...),
			Visibility: append([]byte(nil), cell.Visibility...), Timestamp: timestamp,
			Value: append([]byte(nil), cell.Value...),
		}
	}
	return result
}

func (a *AccumuloPhysicalAdapter) Write(ctx context.Context, epoch coordination.Epoch, cells []PhysicalCell) error {
	return a.sink.WriteTrusted(ctx, mapTrusted(epoch, cells))
}

func (a *AccumuloPhysicalAdapter) Verify(ctx context.Context, epoch coordination.Epoch, cells []PhysicalCell) error {
	wanted := mapTrusted(epoch, cells)
	got, err := a.sink.ReadTrusted(ctx, wanted)
	if err != nil {
		return err
	}
	if len(got) != len(wanted) {
		return ErrInternal
	}
	for i := range wanted {
		if !trustedCellEqual(wanted[i], got[i]) {
			return ErrInternal
		}
	}
	return nil
}

func trustedCellEqual(a, b TrustedCell) bool {
	return bytes.Equal(a.Table, b.Table) && bytes.Equal(a.Row, b.Row) &&
		bytes.Equal(a.Family, b.Family) && bytes.Equal(a.Qualifier, b.Qualifier) &&
		bytes.Equal(a.Visibility, b.Visibility) && a.Timestamp == b.Timestamp &&
		bytes.Equal(a.Value, b.Value)
}

var _ Store = (*AccumuloStore)(nil)
var _ PhysicalWriter = (*AccumuloPhysicalAdapter)(nil)
var _ PhysicalVerifier = (*AccumuloPhysicalAdapter)(nil)
