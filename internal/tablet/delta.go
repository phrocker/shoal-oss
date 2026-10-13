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

package tablet

import (
	"bytes"
	"fmt"

	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/internal/parquetfile"
	"github.com/phrocker/shoal-oss/internal/rfile"
	"github.com/phrocker/shoal-oss/internal/rfile/bcfile"
	"github.com/phrocker/shoal-oss/internal/rfile/bcfile/block"
)

// SubtractImage returns an immutable file image holding the cells of image
// that are not in any of basis: a multiset difference over (key, value), so
// a cell present in image k times and in basis j times appears max(0, k-j)
// times, and the cells kept stay in image's order (identical keys newest
// first). The output has image's format (from name's extension) and
// embedding state.
//
// It is how an export splits a compaction output into the part a consumer
// already holds and the part it does not (#704): when the output's cells are
// the multiset union of its leaves' cells and basis holds exactly the
// already-shipped leaves' cells, the difference is exactly the cells of the
// leaves the consumer has not received. A compaction that dropped or
// rewrote cells (a tombstone hiding cells, an iterator stack) only drops
// cells from the difference or keeps rewritten ones, never invents a cell.
func SubtractImage(name string, image []byte, basis [][]byte) ([]byte, int64, error) {
	state, err := embeddingStateFromImage(name, image)
	if err != nil {
		return nil, 0, fmt.Errorf("tablet: subtract %s: embedding state: %w", name, err)
	}
	env := iterrt.IteratorEnvironment{Scope: iterrt.ScopeMajc}
	src, closeSrc, err := imageSource(name, image, env)
	if err != nil {
		return nil, 0, err
	}
	defer closeSrc()
	leaves := make([]iterrt.SortedKeyValueIterator, 0, len(basis))
	var closers []func()
	defer func() {
		for _, c := range closers {
			c()
		}
	}()
	for i, b := range basis {
		leaf, closeLeaf, err := imageSource(fmt.Sprintf("%s basis %d", name, i), b, env)
		if err != nil {
			return nil, 0, err
		}
		closers = append(closers, closeLeaf)
		leaves = append(leaves, leaf)
	}
	base := iterrt.NewMergingIterator(leaves...)
	if err := base.Init(nil, nil, env); err != nil {
		return nil, 0, err
	}
	if err := base.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		return nil, 0, err
	}
	if err := src.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		return nil, 0, err
	}
	var kept []iterrt.Cell
	for src.HasTop() {
		key := src.GetTopKey().Clone()
		for base.HasTop() && base.GetTopKey().Compare(key) < 0 {
			if err := base.Next(); err != nil {
				return nil, 0, err
			}
		}
		counts := make(map[string]int)
		for base.HasTop() && base.GetTopKey().Compare(key) == 0 {
			counts[string(base.GetTopValue())]++
			if err := base.Next(); err != nil {
				return nil, 0, err
			}
		}
		for src.HasTop() && src.GetTopKey().Compare(key) == 0 {
			value := src.GetTopValue()
			if counts[string(value)] > 0 {
				counts[string(value)]--
			} else {
				kept = append(kept, iterrt.Cell{
					Key:   src.GetTopKey().Clone(),
					Value: append([]byte(nil), value...),
				})
			}
			if err := src.Next(); err != nil {
				return nil, 0, err
			}
		}
	}
	out := iterrt.NewSliceSource(kept)
	if err := out.Init(nil, nil, env); err != nil {
		return nil, 0, err
	}
	if err := out.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		return nil, 0, err
	}
	if fileFormat(name) == FormatParquet {
		data, count, err := parquetfile.EncodeWithOptions(out, parquetfile.EncodeOptions{EmbeddingSpace: state})
		if err != nil {
			return nil, 0, fmt.Errorf("tablet: subtract %s: encode: %w", name, err)
		}
		return data, count, nil
	}
	var buf bytes.Buffer
	w, err := rfile.NewWriter(&buf, rfile.WriterOptions{Codec: block.CodecSnappy, EmbeddingSpace: state})
	if err != nil {
		return nil, 0, fmt.Errorf("tablet: subtract %s: new writer: %w", name, err)
	}
	var count int64
	for out.HasTop() {
		if err := w.Append(out.GetTopKey(), out.GetTopValue()); err != nil {
			return nil, 0, fmt.Errorf("tablet: subtract %s: append: %w", name, err)
		}
		count++
		if err := out.Next(); err != nil {
			return nil, 0, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, 0, fmt.Errorf("tablet: subtract %s: close writer: %w", name, err)
	}
	return buf.Bytes(), count, nil
}

// imageSource opens an in-memory immutable file image as an SKVI leaf.
func imageSource(name string, image []byte, env iterrt.IteratorEnvironment) (iterrt.SortedKeyValueIterator, func(), error) {
	if fileFormat(name) == FormatParquet {
		cells, err := parquetfile.Decode(image)
		if err != nil {
			return nil, nil, fmt.Errorf("tablet: open parquet %s: %w", name, err)
		}
		src := iterrt.NewSliceSource(cells)
		if err := src.Init(nil, nil, env); err != nil {
			return nil, nil, err
		}
		return src, func() {}, nil
	}
	open := func() (*rfile.Reader, error) {
		bc, err := bcfile.NewReader(bytes.NewReader(image), int64(len(image)))
		if err != nil {
			return nil, fmt.Errorf("tablet: bcfile open %s: %w", name, err)
		}
		return rfile.Open(bc, block.Default())
	}
	rdr, err := open()
	if err != nil {
		return nil, nil, err
	}
	src := iterrt.NewRFileSource(rdr, open)
	if err := src.Init(nil, nil, env); err != nil {
		_ = rdr.Close()
		return nil, nil, err
	}
	return src, func() { _ = rdr.Close() }, nil
}
