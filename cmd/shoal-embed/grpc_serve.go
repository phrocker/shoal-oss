// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"errors"

	"github.com/phrocker/shoal/internal/embedpb"
	"github.com/phrocker/shoal/internal/embedstore"
	"github.com/phrocker/shoal/internal/engine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultScanBatchSize = embedstore.DefaultScanBatchSize

// embedServer implements the ShoalEmbed gRPC service. All engine translation
// (mutation building, ScanRequest pushdowns, range derivation) is delegated to
// the shared embedstore.EngineStore so the wire server and in-process callers
// stay byte-for-byte identical.
type embedServer struct {
	embedpb.UnimplementedShoalEmbedServer
	eng   *engine.Engine
	store *embedstore.EngineStore
}

// newEmbedServer wires an embedServer over eng.
func newEmbedServer(eng *engine.Engine) *embedServer {
	return &embedServer{eng: eng, store: embedstore.New(eng)}
}

func (s *embedServer) CreateTable(ctx context.Context, req *embedpb.CreateTableRequest) (*embedpb.CreateTableResponse, error) {
	if req.Table == "" {
		return nil, status.Error(codes.InvalidArgument, "table is required")
	}
	if err := s.store.CreateTable(ctx, req.Table, req.Splits); err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "%v", err)
	}
	return &embedpb.CreateTableResponse{
		Table:   req.Table,
		Tablets: int32(len(req.Splits) + 1),
	}, nil
}

func (s *embedServer) Write(ctx context.Context, req *embedpb.WriteRequest) (*embedpb.WriteResponse, error) {
	if req.Table == "" {
		return nil, status.Error(codes.InvalidArgument, "table is required")
	}
	if err := s.store.Write(ctx, req.Table, req.Mutations); err != nil {
		return nil, status.Errorf(codes.Internal, "write: %v", err)
	}
	return &embedpb.WriteResponse{Written: int32(len(req.Mutations))}, nil
}

// scanStatusError maps embedstore's validation sentinels onto gRPC codes,
// defaulting any other error to Internal.
func scanStatusError(err error) error {
	switch {
	case errors.Is(err, embedstore.ErrMultiplePushdowns),
		errors.Is(err, embedstore.ErrVectorQueryRequired),
		errors.Is(err, embedstore.ErrNegativeMaxHops):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "scan: %v", err)
	}
}

func (s *embedServer) Scan(req *embedpb.ScanRequest, stream embedpb.ShoalEmbed_ScanServer) error {
	if req.Table == "" {
		return status.Error(codes.InvalidArgument, "table is required")
	}

	sc, err := s.store.Scanner(req.Table, req)
	if err != nil {
		return scanStatusError(err)
	}
	defer sc.Close()

	batchSize := int(req.BatchSize)
	if batchSize <= 0 {
		batchSize = defaultScanBatchSize
	}
	limit := int(req.Limit)

	batch := make([]*embedpb.Cell, 0, batchSize)
	total := 0

	for sc.Next() {
		k := sc.Key()
		batch = append(batch, &embedpb.Cell{
			Row:              k.Row,
			ColumnFamily:     k.ColumnFamily,
			ColumnQualifier:  k.ColumnQualifier,
			ColumnVisibility: k.ColumnVisibility,
			Timestamp:        k.Timestamp,
			Value:            sc.Value(),
		})

		if err := sc.Advance(); err != nil {
			return status.Errorf(codes.Internal, "scan advance: %v", err)
		}

		total++

		// Flush batch when full
		if len(batch) >= batchSize {
			if err := stream.Send(&embedpb.ScanResponse{Cells: batch}); err != nil {
				return err
			}
			batch = make([]*embedpb.Cell, 0, batchSize)
		}

		if limit > 0 && total >= limit {
			break
		}
	}

	// Send remaining cells
	if len(batch) > 0 {
		if err := stream.Send(&embedpb.ScanResponse{Cells: batch}); err != nil {
			return err
		}
	}

	return nil
}

func (s *embedServer) Flush(ctx context.Context, req *embedpb.FlushRequest) (*embedpb.FlushResponse, error) {
	if req.Table == "" {
		return nil, status.Error(codes.InvalidArgument, "table is required")
	}
	if err := s.store.Flush(ctx, req.Table); err != nil {
		return nil, status.Errorf(codes.Internal, "flush: %v", err)
	}
	return &embedpb.FlushResponse{}, nil
}

func (s *embedServer) Compact(ctx context.Context, req *embedpb.CompactRequest) (*embedpb.CompactResponse, error) {
	if req.Table == "" {
		return nil, status.Error(codes.InvalidArgument, "table is required")
	}
	if err := s.store.Compact(ctx, req.Table); err != nil {
		return nil, status.Errorf(codes.Internal, "compact: %v", err)
	}
	return &embedpb.CompactResponse{}, nil
}

func (s *embedServer) Status(_ context.Context, _ *embedpb.StatusRequest) (*embedpb.StatusResponse, error) {
	tables := s.eng.TableNames()
	return &embedpb.StatusResponse{Tables: tables}, nil
}

// Verify at compile time that embedServer implements the interface.
var _ embedpb.ShoalEmbedServer = (*embedServer)(nil)
