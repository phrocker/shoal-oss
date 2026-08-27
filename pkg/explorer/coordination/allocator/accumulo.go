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

package allocator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/phrocker/shoal-oss/accumulo"
)

type AccumuloConfig struct {
	Connector        *accumulo.Connector
	Table            accumulo.Table
	Authorizations   [][]byte
	WriterOptions    accumulo.ConditionalWriterOptions
	ScannerBatchSize int32
}

type accumuloScanner interface {
	Scan(context.Context, *accumulo.Range) ([]accumulo.KeyValue, error)
}

type accumuloStreamScanner interface {
	Stream(context.Context, *accumulo.Range) (*accumulo.ResultStream, error)
}

type accumuloConditionalWriter interface {
	Write(context.Context, *accumulo.ConditionalMutation) (accumulo.ConditionalStatus, error)
}

type AccumuloStore struct {
	connector      *accumulo.Connector
	table          accumulo.Table
	authorizations [][]byte
	batchSize      int32
	scanner        accumuloScanner
	writer         accumuloConditionalWriter
}

func NewAccumuloStore(config AccumuloConfig) (*AccumuloStore, error) {
	if config.Connector == nil {
		return nil, errors.New("allocator: Accumulo connector is required")
	}
	authorizations := make([][]byte, len(config.Authorizations))
	for i := range config.Authorizations {
		authorizations[i] = append([]byte(nil), config.Authorizations[i]...)
	}
	writer, err := config.Connector.NewConditionalWriter(config.Table, config.WriterOptions)
	if err != nil {
		return nil, err
	}
	return &AccumuloStore{
		connector: config.Connector, table: config.Table, authorizations: authorizations,
		batchSize: config.ScannerBatchSize, writer: writer,
	}, nil
}

func (s *AccumuloStore) ReadExact(ctx context.Context, coordinates []Coordinate) ([]Cell, error) {
	if len(coordinates) > coordinationReadBound {
		return nil, errors.New("allocator: exact read exceeds its bound")
	}
	groups := make(map[string][]Coordinate)
	rows := make(map[string][]byte)
	for _, coordinate := range coordinates {
		key := string(coordinate.Row)
		groups[key] = append(groups[key], coordinate.clone())
		rows[key] = append([]byte(nil), coordinate.Row...)
	}
	result := make([]Cell, 0, len(coordinates))
	for key, requested := range groups {
		columns := make([]accumulo.Column, len(requested))
		for i, coordinate := range requested {
			columns[i] = accumulo.NewColumnWithVisibility(
				coordinate.Family, coordinate.Qualifier, coordinate.Visibility,
			)
		}
		scanner, err := s.newScanner(columns)
		if err != nil {
			return nil, err
		}
		scanRange, err := accumulo.NewRangeRow(rows[key])
		if err != nil {
			return nil, err
		}
		values, err := scanBounded(ctx, scanner, scanRange, len(requested)+1)
		if err != nil {
			var cleanup *accumulo.CleanupError
			if !errors.As(err, &cleanup) {
				return nil, err
			}
		}
		for _, value := range values {
			cell := Cell{
				Coordinate: Coordinate{
					Row:        append([]byte(nil), value.Key.Row...),
					Family:     append([]byte(nil), value.Key.ColumnFamily...),
					Qualifier:  append([]byte(nil), value.Key.ColumnQualifier...),
					Visibility: append([]byte(nil), value.Key.ColumnVisibility...),
				},
				Value: append([]byte(nil), value.Value...),
			}
			for _, coordinate := range requested {
				if cell.Coordinate.equal(coordinate) {
					result = append(result, cell)
					break
				}
			}
		}
	}
	return result, nil
}

func scanBounded(ctx context.Context, scanner accumuloScanner, scanRange *accumulo.Range, limit int) ([]accumulo.KeyValue, error) {
	streaming, ok := scanner.(accumuloStreamScanner)
	if !ok {
		return scanner.Scan(ctx, scanRange)
	}
	stream, err := streaming.Stream(ctx, scanRange)
	if err != nil {
		return nil, err
	}
	values := make([]accumulo.KeyValue, 0, limit)
	for len(values) < limit && stream.Next() {
		values = append(values, stream.Entry())
	}
	streamErr := stream.Err()
	closeErr := stream.Close()
	if streamErr != nil {
		return nil, errors.Join(streamErr, closeErr)
	}
	var cleanup *accumulo.CleanupError
	if closeErr != nil && !errors.As(closeErr, &cleanup) {
		return nil, closeErr
	}
	return values, closeErr
}

const coordinationReadBound = 10_001

func (s *AccumuloStore) ScanRowPrefix(
	ctx context.Context,
	row, family, qualifierStart, visibility []byte,
	limit int,
) ([]Cell, error) {
	if limit < 1 || limit > coordinationReadBound {
		return nil, errors.New("allocator: scan limit is outside its bound")
	}
	start := &accumulo.Key{
		Row: row, ColumnFamily: family, ColumnQualifier: qualifierStart,
		ColumnVisibility: visibility, Timestamp: math.MaxInt64,
	}
	endFamily := append(append([]byte(nil), family...), 0)
	if len(family) == 1 && family[0] < 0xff {
		endFamily[0]++
		endFamily = endFamily[:1]
	}
	end := &accumulo.Key{Row: row, ColumnFamily: endFamily, Timestamp: math.MaxInt64}
	scanRange, err := accumulo.NewKeyRange(start, true, end, false)
	if err != nil {
		return nil, err
	}
	scanner, err := s.newScanner([]accumulo.Column{accumulo.NewColumnFamily(family)})
	if err != nil {
		return nil, err
	}
	values, err := scanBounded(ctx, scanner, scanRange, limit)
	if err != nil {
		var cleanup *accumulo.CleanupError
		if !errors.As(err, &cleanup) {
			return nil, err
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Key.Compare(values[j].Key) < 0 })
	result := make([]Cell, 0, limit)
	for _, value := range values {
		if !bytes.Equal(value.Key.Row, row) || !bytes.Equal(value.Key.ColumnFamily, family) ||
			!bytes.Equal(value.Key.ColumnVisibility, visibility) ||
			bytes.Compare(value.Key.ColumnQualifier, qualifierStart) < 0 {
			continue
		}
		result = append(result, Cell{
			Coordinate: Coordinate{
				Row: append([]byte(nil), value.Key.Row...), Family: append([]byte(nil), value.Key.ColumnFamily...),
				Qualifier:  append([]byte(nil), value.Key.ColumnQualifier...),
				Visibility: append([]byte(nil), value.Key.ColumnVisibility...),
			},
			Value: append([]byte(nil), value.Value...),
		})
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (s *AccumuloStore) newScanner(columns []accumulo.Column) (accumuloScanner, error) {
	if s.scanner != nil {
		return s.scanner, nil
	}
	return s.connector.NewScanner(s.table, accumulo.ScannerOptions{
		BatchSize: s.batchSize, Authorizations: s.authorizations, Columns: columns,
	})
}

func (s *AccumuloStore) CompareAndMutate(ctx context.Context, request Mutation) (Status, error) {
	if len(request.Row) == 0 || len(request.Conditions) == 0 || len(request.Updates) == 0 {
		return StatusUnknown, errors.New("allocator: malformed conditional mutation")
	}
	mutation, err := accumulo.NewMutation(request.Row)
	if err != nil {
		return StatusUnknown, err
	}
	for _, update := range request.Updates {
		if !bytes.Equal(update.Coordinate.Row, request.Row) {
			return StatusUnknown, errors.New("allocator: conditional update crosses rows")
		}
		if update.Delete {
			mutation.Delete(update.Coordinate.Family, update.Coordinate.Qualifier, update.Coordinate.Visibility, update.Timestamp)
		} else {
			mutation.Put(update.Coordinate.Family, update.Coordinate.Qualifier, update.Coordinate.Visibility, update.Timestamp, update.Value)
		}
	}
	conditions := make([]accumulo.Condition, len(request.Conditions))
	for i, condition := range request.Conditions {
		if !bytes.Equal(condition.Coordinate.Row, request.Row) {
			return StatusUnknown, errors.New("allocator: conditional predicate crosses rows")
		}
		if condition.Absent {
			conditions[i], err = accumulo.NewAbsentCondition(
				condition.Coordinate.Family, condition.Coordinate.Qualifier, condition.Coordinate.Visibility,
			)
		} else {
			conditions[i], err = accumulo.NewValueCondition(
				condition.Coordinate.Family, condition.Coordinate.Qualifier,
				condition.Coordinate.Visibility, condition.Value,
			)
		}
		if err != nil {
			return StatusUnknown, fmt.Errorf("allocator: invalid condition: %w", err)
		}
	}
	conditional, err := accumulo.NewConditionalMutation(mutation, conditions...)
	if err != nil {
		return StatusUnknown, err
	}
	status, err := s.writer.Write(ctx, conditional)
	switch status {
	case accumulo.ConditionalAccepted:
		return StatusAccepted, err
	case accumulo.ConditionalRejected:
		return StatusRejected, err
	default:
		if errors.Is(err, accumulo.ErrConditionalUnknown) {
			err = errors.Join(ErrConditionalUnknown, err)
		}
		return StatusUnknown, err
	}
}
