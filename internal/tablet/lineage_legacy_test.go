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
	"path/filepath"
	"testing"

	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

// TestLegacyImportIsNamedByItsSource: an import recorded by an older binary
// (a source, no lineage) is named by its producer identity, so a manifest
// naming that identity with leaves teaches the tablet those leaves.
func TestLegacyImportIsNamedByItsSource(t *testing.T) {
	backend := memory.New()
	dir := "/tablet"
	legacy := putFiles(backend, dir, filepath.Base(importPath('a', "1")))
	if err := persistImmutableManifest(backend, dir, immutableState{
		active: legacy, sources: map[string]string{filepath.Base(legacy[0]): "s-old"},
	}); err != nil {
		t.Fatal(err)
	}
	found, err := discoverImmutableFiles(backend, dir, false)
	if err != nil {
		t.Fatal(err)
	}
	tb := &Tablet{dir: dir, backend: backend, files: found.active, obsolete: found.obsolete,
		sources: found.sources, absorbed: found.absorbed, lineage: found.lineage}
	again := incoming(backend, "again.rf", "s-old", leaves("p", 1, 1), "")
	if _, err := tb.RegisterImmutableFiles([]ImportedFile{again}); err != nil {
		t.Fatal(err)
	}
	if l := tb.lineage[filepath.Base(legacy[0])]; !l.Covers(*leaves("p", 1, 1)) {
		t.Fatalf("legacy import did not learn its leaves: %+v", l)
	}
}

// TestRefreshKeepsUnsavedLineage: a lineage this session assigned but has
// not written yet survives RefreshFiles.
func TestRefreshKeepsUnsavedLineage(t *testing.T) {
	backend := memory.New()
	dir := t.TempDir()
	paths := putFiles(backend, dir, "F0000000000100.rf")
	if err := persistImmutableManifest(backend, dir, immutableState{active: paths}); err != nil {
		t.Fatal(err)
	}
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()
	tb.mu.Lock()
	tb.ensureLineageLocked()
	assigned := tb.lineage["F0000000000100.rf"]
	tb.mu.Unlock()
	if _, err := tb.RefreshFiles(); err != nil {
		t.Fatal(err)
	}
	files, err := tb.FilesWithLineage()
	if err != nil {
		t.Fatal(err)
	}
	if !files[0].Lineage.SameLeaves(assigned) {
		t.Fatalf("refresh replaced lineage %+v with %+v", assigned, files[0].Lineage)
	}
}
