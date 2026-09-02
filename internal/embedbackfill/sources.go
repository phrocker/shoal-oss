// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package embedbackfill

import (
	"context"
	"fmt"
	"strings"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/metadata"
	"github.com/phrocker/shoal-oss/internal/metadatacas"
	"github.com/phrocker/shoal-oss/internal/parquetfile"
	"github.com/phrocker/shoal-oss/internal/rfile"
	"github.com/phrocker/shoal-oss/internal/rfile/bcfile"
	"github.com/phrocker/shoal-oss/internal/rfile/bcfile/block"
	"github.com/phrocker/shoal-oss/internal/storage"
)

// TabletReader is the metadata-walk seam. It matches the LocateTable
// method every metadata walker in the tree already exposes.
type TabletReader interface {
	LocateTable(ctx context.Context, tableID string) ([]metadata.TabletInfo, error)
}

// MetadataFiles enumerates one table's file entries from the metadata
// table.
type MetadataFiles struct {
	Reader  TabletReader
	TableID string
}

// List implements Files.
func (m MetadataFiles) List(ctx context.Context) ([]File, error) {
	if m.Reader == nil || strings.TrimSpace(m.TableID) == "" {
		return nil, fmt.Errorf("embedbackfill: MetadataFiles needs a reader and a table id")
	}
	tablets, err := m.Reader.LocateTable(ctx, m.TableID)
	if err != nil {
		return nil, err
	}
	out := make([]File, 0)
	for _, tablet := range tablets {
		for _, file := range tablet.Files {
			out = append(out, File{
				TableID:    tablet.TableID,
				PrevEndRow: append([]byte(nil), tablet.PrevRow...),
				EndRow:     append([]byte(nil), tablet.EndRow...),
				Entry:      string(file.RawQualifier),
				Path:       file.Path,
				Qualifier:  append([]byte(nil), file.RawQualifier...),
				Value:      append([]byte(nil), file.RawValue...),
				Metadata:   file.Embedding,
			})
		}
	}
	return out, nil
}

// CASColumns writes the file.embedding column through the metadata
// compare-and-set authority, which is the same path a minor compaction
// commit uses. Nothing about the backfill gets its own write route.
type CASColumns struct {
	Writer *metadatacas.BackfillWriter
}

// Write implements Columns.
func (c CASColumns) Write(
	ctx context.Context, file File, state embeddingspace.FileState,
) (bool, error) {
	if c.Writer == nil {
		return false, fmt.Errorf("embedbackfill: CASColumns needs a writer")
	}
	return c.Writer.WriteFileEmbedding(ctx, metadatacas.BackfillTarget{
		TableID:       file.TableID,
		PrevEndRow:    file.PrevEndRow,
		EndRow:        file.EndRow,
		FileQualifier: file.Qualifier,
		FileValue:     file.Value,
	}, state)
}

// StorageFooters reads embedding-space footers through a storage
// backend.
//
// It opens the file and reads only the trailer and the meta-block index,
// never the data blocks: establishing a file's embedding space must not
// cost a full read of the corpus, which is the same requirement issue
// #260 placed on scan planning.
type StorageFooters struct {
	Backend storage.Backend
}

// FooterState implements Footers. A file with no embedding-space meta
// block yields embeddingspace.Unknown() and no error — that is the
// normal shape of a file written before the block existed, and it is
// precisely the case the backfill must report rather than guess at.
func (s StorageFooters) FooterState(
	ctx context.Context, path string,
) (embeddingspace.FileState, error) {
	if s.Backend == nil {
		return embeddingspace.FileState{}, fmt.Errorf("embedbackfill: StorageFooters needs a backend")
	}
	file, err := s.Backend.Open(ctx, path)
	if err != nil {
		return embeddingspace.FileState{}, err
	}
	defer file.Close()
	if strings.HasSuffix(strings.ToLower(path), ".parquet") {
		return parquetfile.ReadEmbeddingSpaceMetadata(file, file.Size())
	}
	bc, err := bcfile.NewReader(file, file.Size())
	if err != nil {
		return embeddingspace.FileState{}, fmt.Errorf("open %s: %w", path, err)
	}
	state, err := rfile.ReadEmbeddingSpaceMetadata(bc, block.Default())
	if err != nil {
		return embeddingspace.FileState{}, fmt.Errorf("read %s footer: %w", path, err)
	}
	return state, nil
}
