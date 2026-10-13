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
	"sort"
)

// Name lineage (#701).
//
// Producer side. A tablet's flush and compaction names ("F<ms>", "C<ms>-<n>")
// strictly increase: each is above the persisted name high-water mark. That
// holds for one continuous files.json history, not for a copy of it: a data
// directory restored from a backup, or cloned and run twice, can issue the
// same or lower names again. So a tablet draws a random INSTANCE (the name
// epoch) the first time it mints a name and persists it in files.json, and
// records, for each live file it minted itself, the instance it was minted
// under (Minted). Files it did not mint (imports, strays adopted by a
// refresh) carry none. Within one instance, names strictly increase with
// creation time. The instance is redrawn when files.json has none (a new
// directory, PublishImmutableFiles, or a rewrite by an older binary, which
// drops these fields) and when the tablet directory's identity (its absolute
// path and, where the platform has one, its device and inode) differs from
// the one recorded with the instance: a moved, copied or restored-elsewhere
// directory starts a new epoch for the names it mints from then on. Files
// minted before keep their old instance — their bytes are the original's.
//
// Consumer side. An imported file whose manifest entry carries the
// producer's epoch and a "<producer>~F<ms>"/"<producer>~C<ms>-<n>" name has a
// ProducerLineage. For each (producer, epoch) the tablet keeps a mark: the
// highest name of that lineage in any manifest it registered. Every manifest
// is a full snapshot of a producer tablet, so a lineage file named at or
// below the mark either was in a registered snapshot (the tablet holds it,
// live or absorbed) or had been retired by the producer before that snapshot
// was taken (its cells live on in the producer's newer files, which the
// snapshot listed). Either way an incoming file at or below the mark is
// held: a replay of an old manifest is a no-op (see RegisterImmutableFiles).
//
// That lets absorbed_sources shrink. An absorbed identity with a lineage is
// kept explicitly only until a registered snapshot of its lineage reaches a
// higher-or-equal name without listing it — proof the producer retired it.
// After that the mark covers it, and no snapshot taken later can list it
// again. Keeping it until then also means a producer that loses its epoch
// (rolled back to an older binary that drops the field) and exports a still
// live file without one is still matched by identity. absorbed_sources is
// therefore bounded by the producer's live files plus files absorbed since
// the last import, instead of growing with every compacted import.
//
// Explicit entries are kept for good — never pruned — when there is no
// lineage: imports without a ProducerID prefix, files the producer did not
// mint (chained imports), producers without an epoch (older binaries), and
// legacy "content:<digest>" entries.
//
// Limits. Holding by mark rests on two assumptions; where they fail, a new
// file can be taken as held and its cells are NOT imported:
//   - a manifest lists every live file of the producer tablet (exports and
//     incremental syncs always do; a hand-edited subset does not);
//   - one epoch has one writer. A directory restored over the same path
//     and inode with its old files.json, then run with a clock behind the
//     names it had issued, or two copies that keep the same path, device
//     and inode (a block-level clone mounted identically) and export under
//     the same ProducerID, can issue names at or below a mark. So can a
//     manifest that names files under another producer's ProducerID and
//     epoch (that already lets it overwrite that producer's shipped
//     objects).
// Marks themselves are never dropped: one per (producer tablet, epoch).

// ProducerLineage places an imported file in its producer's name lineage.
// The zero value means none.
type ProducerLineage struct {
	// Producer is the producer tablet: the file's relative path with the
	// file name replaced by its ProducerID ("graph/t-0000/agentA").
	Producer string `json:"producer"`
	// Epoch is the producer instance (and export transform) the name was
	// minted under.
	Epoch string `json:"epoch"`
	// Millis is the millisecond in the producer's file name.
	Millis int64 `json:"name_ms"`
}

func (l ProducerLineage) valid() bool {
	return l.Producer != "" && l.Epoch != "" && l.Millis > 0
}

func (l ProducerLineage) key() lineageKey {
	return lineageKey{producer: l.Producer, epoch: l.Epoch}
}

// lineageKey identifies one producer name lineage.
type lineageKey struct {
	producer, epoch string
}

// producerMark is a persisted lineage mark (files.json "producer_marks").
type producerMark struct {
	Producer  string `json:"producer"`
	Epoch     string `json:"epoch"`
	HighWater int64  `json:"high_water"`
}

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

// LocalFileNameMillis parses a flush ("F<ms><ext>") or compaction
// ("C<ms>-<n><ext>") file name, with ext ".rf" or ".parquet", and returns its
// millisecond. Any other name — an import, a prefixed name — is not one.
func LocalFileNameMillis(name string) (int64, bool) {
	if len(name) < 2 || (name[0] != 'F' && name[0] != 'C') {
		return 0, false
	}
	i := 1
	var ms int64
	for i < len(name) && name[i] >= '0' && name[i] <= '9' {
		if i > 18 {
			return 0, false
		}
		ms = ms*10 + int64(name[i]-'0')
		i++
	}
	if i-1 < 13 || ms <= 0 {
		return 0, false
	}
	rest := name[i:]
	if name[0] == 'C' {
		if len(rest) < 4 || rest[0] != '-' {
			return 0, false
		}
		j := 1
		for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
			j++
		}
		if j < 4 {
			return 0, false
		}
		rest = rest[j:]
	}
	if rest != ".rf" && rest != ".parquet" {
		return 0, false
	}
	return ms, true
}

// mintInstanceLocked returns the instance a name minted now belongs to,
// drawing a new one when the tablet has none or its directory identity
// changed since it was drawn. Caller holds t.mu.
func (t *Tablet) mintInstanceLocked() (string, error) {
	if t.instance != "" && t.instanceDir == t.dirIdentity {
		return t.instance, nil
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("tablet: draw name instance: %w", err)
	}
	t.instance = hex.EncodeToString(raw[:])
	t.instanceDir = t.dirIdentity
	return t.instance, nil
}

// tabletDirIdentity is the identity a name instance is bound to: the
// directory's absolute path plus, where available, its device and inode.
func tabletDirIdentity(dir string) string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		abs = dir
	}
	return abs + platformDirIdentity(abs)
}

func cloneLineage(in map[string]ProducerLineage) map[string]ProducerLineage {
	out := make(map[string]ProducerLineage, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneMarks(in map[lineageKey]int64) map[lineageKey]int64 {
	out := make(map[lineageKey]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// markHeld reports whether lineage is at or below its lineage's mark.
func markHeld(marks map[lineageKey]int64, lineage ProducerLineage) bool {
	if !lineage.valid() {
		return false
	}
	mark, ok := marks[lineage.key()]
	return ok && lineage.Millis <= mark
}

// advanceLineage applies a registered snapshot to the lineage state. For
// each lineage in files, top is its highest name; absorbed identities of
// that lineage named at or below top that the snapshot does not list were
// retired by the producer before the snapshot was taken, so they are
// dropped (the mark now covers them), and the mark rises to top.
func advanceLineage(
	files []ImportedFile,
	marks map[lineageKey]int64,
	absorbed map[string]struct{},
	absorbedLineage map[string]ProducerLineage,
) {
	tops := make(map[lineageKey]int64)
	listed := make(map[string]struct{}, len(files))
	for _, file := range files {
		if file.Source == "" || !file.Lineage.valid() {
			continue
		}
		listed[file.Source] = struct{}{}
		key := file.Lineage.key()
		if file.Lineage.Millis > tops[key] {
			tops[key] = file.Lineage.Millis
		}
	}
	if len(tops) == 0 {
		return
	}
	for identity, lineage := range absorbedLineage {
		top, ok := tops[lineage.key()]
		if !ok || lineage.Millis > top {
			continue
		}
		if _, still := listed[identity]; still {
			continue
		}
		delete(absorbed, identity)
		delete(absorbedLineage, identity)
	}
	for key, top := range tops {
		if top > marks[key] {
			marks[key] = top
		}
	}
}

func encodeMarks(marks map[lineageKey]int64) []producerMark {
	if len(marks) == 0 {
		return nil
	}
	out := make([]producerMark, 0, len(marks))
	for key, hw := range marks {
		out = append(out, producerMark{Producer: key.producer, Epoch: key.epoch, HighWater: hw})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Producer != out[j].Producer {
			return out[i].Producer < out[j].Producer
		}
		return out[i].Epoch < out[j].Epoch
	})
	return out
}

func decodeMarks(in []producerMark) map[lineageKey]int64 {
	out := make(map[lineageKey]int64, len(in))
	for _, mark := range in {
		if mark.Producer == "" || mark.Epoch == "" || mark.HighWater <= 0 {
			continue
		}
		key := lineageKey{producer: mark.Producer, epoch: mark.Epoch}
		if mark.HighWater > out[key] {
			out[key] = mark.HighWater
		}
	}
	return out
}
