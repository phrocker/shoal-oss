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
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/phrocker/shoal-oss/internal/storage"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

// TestMintedInstancePerProcessLifetime: each file records the instance it
// was minted under, and keeps it across a reopen; every Open mints new names
// under a fresh instance; a compaction's outputs take the current instance;
// a refresh keeps the in-memory instance.
func TestMintedInstancePerProcessLifetime(t *testing.T) {
	backend := memory.New()
	dir := t.TempDir()
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	writeAndFlush(t, tb, "a")
	first := tb.RFileLineage()
	if len(first) != 1 || first[0].Instance == "" {
		t.Fatalf("flush not minted: %+v", first)
	}
	if _, err := tb.RefreshFiles(); err != nil {
		t.Fatal(err)
	}
	writeAndFlush(t, tb, "b")
	same := tb.RFileLineage()
	if len(same) != 2 || same[1].Instance != first[0].Instance {
		t.Fatalf("a refresh changed the process's instance: %+v", same)
	}
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}

	tb, err = Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()
	if got := tb.RFileLineage(); got[0].Instance != first[0].Instance || got[1].Instance != first[0].Instance {
		t.Fatalf("reopen lost the files' instances: %+v", got)
	}
	writeAndFlush(t, tb, "c")
	reopened := tb.RFileLineage()
	if reopened[2].Instance == "" || reopened[2].Instance == first[0].Instance {
		t.Fatalf("a new Open minted under the old instance: %+v", reopened)
	}
	if err := tb.Compact(nil); err != nil {
		t.Fatal(err)
	}
	compacted := tb.RFileLineage()
	if len(compacted) != 1 || compacted[0].Instance != reopened[2].Instance {
		t.Fatalf("compaction output %+v, want this process's instance %s", compacted, reopened[2].Instance)
	}
}

// TestRestoredDirectoryReissuesNameUnderNewInstance: a directory restored
// in place from a backup (files.json and objects put back, same path)
// reissues a name it had already used once the clock is behind that name.
// The reissued name gets a different instance, so (name, instance) still
// names one content.
func TestRestoredDirectoryReissuesNameUnderNewInstance(t *testing.T) {
	backend := memory.New()
	dir := t.TempDir()
	withFrozenClock(t, 1000)
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	writeAndFlush(t, tb, "a")
	manifestPath := filepath.Join(dir, "files.json")
	backup, err := storage.ReadAll(context.Background(), backend, manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	writeAndFlush(t, tb, "b")
	original := tb.RFileLineage()[1]
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	backend.Put(manifestPath, backup)
	if err := backend.Remove(context.Background(), original.Path); err != nil {
		t.Fatal(err)
	}

	tb, err = Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()
	writeAndFlush(t, tb, "c")
	reissued := tb.RFileLineage()[1]
	if reissued.Path != original.Path {
		t.Fatalf("premise: restored directory should reissue %s, minted %s", original.Path, reissued.Path)
	}
	if reissued.Instance == "" || reissued.Instance == original.Instance {
		t.Fatalf("reissued name kept instance %s", original.Instance)
	}
}

// TestMintedOnlyForFilesTheTabletMinted: a stray adopted by a refresh has a
// local-looking name but was not minted here, and a files.json rewritten by
// an older binary (which drops "minted") leaves no instances behind.
func TestMintedOnlyForFilesTheTabletMinted(t *testing.T) {
	backend := memory.New()
	dir := t.TempDir()
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	writeAndFlush(t, tb, "a")
	stray := filepath.Join(dir, "F0000000000005.rf")
	data, err := storage.ReadAll(context.Background(), backend, tb.RFiles()[0])
	if err != nil {
		t.Fatal(err)
	}
	backend.Put(stray, data)
	if _, err := tb.RefreshFiles(); err != nil {
		t.Fatal(err)
	}
	files := tb.RFileLineage()
	if len(files) != 2 || files[0].Instance == "" || files[1].Path != stray || files[1].Instance != "" {
		t.Fatalf("refresh lineage %+v", files)
	}
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}

	manifestPath := filepath.Join(dir, "files.json")
	raw, err := storage.ReadAll(context.Background(), backend, manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["minted"] == nil {
		t.Fatalf("files.json lacks minted: %s", raw)
	}
	delete(m, "minted")
	out, _ := json.Marshal(m)
	backend.Put(manifestPath, out)
	tb, err = Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()
	for _, f := range tb.RFileLineage() {
		if f.Instance != "" {
			t.Fatalf("instance survived an older binary's rewrite: %+v", f)
		}
	}
}
