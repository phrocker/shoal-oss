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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
)

// Producer instance epochs (part of #701).
//
// A tablet's flush and compaction names ("F<ms>", "C<ms>-<n>") are unique
// within one continuous files.json history: each is above the persisted name
// high-water mark. A copy of that history is not continuous with it. A data
// directory recreated from empty, restored from a backup (even in place, onto
// the same path and inode), or cloned and run twice can issue a name again
// for different bytes. Nothing in the directory tells such a copy apart from
// the original, so the tablet does not try to: every process lifetime draws a
// fresh random INSTANCE the first time it mints a name (it is never persisted
// or reused across an Open), and files.json records, for each live file the
// tablet minted itself, the instance it was minted under (Minted). Files the
// tablet did not mint (imports, strays adopted by a refresh) have none.
//
// (name, instance) therefore names one immutable content: two writers never
// share an instance, so a reissued name always comes with a different
// instance. Exports carry it (RFileExportFile.ProducerEpoch) and incremental
// sync re-ships a file whose instance differs from the one it was shipped
// under, instead of reusing the shipped copy by path. The cost of a fresh
// instance per Open is nothing for files minted earlier (they keep the
// instance recorded for them); a file whose record was lost (an older binary
// rewrote files.json) is re-shipped once.

// MintedFile is a live immutable file and the instance this tablet minted
// it under; Instance is empty for a file the tablet did not mint.
type MintedFile struct {
	Path     string
	Instance string
}

// RFileLineage is RFiles plus, for each file, the instance this tablet
// minted it under (empty when it did not mint it), read under one lock so
// the list is one snapshot.
func (t *Tablet) RFileLineage() []MintedFile {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]MintedFile, len(t.files))
	for i, path := range t.files {
		out[i] = MintedFile{Path: path, Instance: t.minted[filepath.Base(path)]}
	}
	return out
}

// mintInstanceLocked returns this process lifetime's instance, drawing it on
// first use. Caller holds t.mu.
func (t *Tablet) mintInstanceLocked() (string, error) {
	if t.instance != "" {
		return t.instance, nil
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("tablet: draw name instance: %w", err)
	}
	t.instance = hex.EncodeToString(raw[:])
	return t.instance, nil
}

func cloneMinted(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for name, instance := range in {
		out[name] = instance
	}
	return out
}
