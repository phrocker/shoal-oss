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

package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

// TestIncrementalStateWithoutLineageGetsIt: a SyncState written by an older
// binary has no lineage; reusing a shipped entry gives it the file's
// current lineage, so the manifest names its leaves.
func TestIncrementalStateWithoutLineageGetsIt(t *testing.T) {
	ctx := context.Background()
	e, err := Open(filepath.Join(t.TempDir(), "p"), Options{Backend: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.CreateTable("t", TableOptions{}); err != nil {
		t.Fatal(err)
	}
	m, err := cclient.NewMutation([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	m.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("1"))
	if err := e.Write("t", []*cclient.Mutation{m}); err != nil {
		t.Fatal(err)
	}
	dst := memory.New()
	opts := RFileExportOptions{DestinationRoot: "/dst", EngineVersion: "test"}
	res, err := e.ExportRFilesIncremental(ctx, "t", dst, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	for rel, entry := range res.State.Shipped {
		entry.Lineage = nil
		res.State.Shipped[rel] = entry
	}
	res, err = e.ExportRFilesIncremental(ctx, "t", dst, opts, res.State)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Skipped) != 1 {
		t.Fatalf("skipped = %v, want the shipped file reused", res.Skipped)
	}
	for _, entry := range res.Manifest.RFiles {
		if entry.Lineage == nil || entry.Lineage.IsEmpty() {
			t.Fatal("reused entry carries no lineage")
		}
	}
}
