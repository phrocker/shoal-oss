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
	"errors"
	"fmt"
	"path/filepath"

	"github.com/phrocker/shoal-oss/internal/storage"
)

// ImportedFile is one checksum-verified import object to register.
type ImportedFile struct {
	// Path is the object's (staged, engine-owned) path in this tablet.
	Path string
	// Source is the file's PRODUCER identity: a stable key for "this file
	// of that producer" (its producer-side path plus content digest).
	// Producer file names are never reissued, so two different producer
	// files never share a Source, even with identical bytes. Empty means
	// unknown; the content digest is then the identity.
	Source string
	// Lineage is the leaves the producer says the file holds (see Lineage).
	// Nil means the producer sent none (an older producer); the file is
	// then named by its Source alone.
	Lineage *Lineage
	// DeltaOf, when set, marks this file as a DELTA of the earlier file in
	// the same registration whose Source it names: the cells of that file
	// minus the cells of the leaves the producer had already shipped to
	// this destination (see RegisterImmutableFiles).
	DeltaOf string
}

// legacyContentIdentity is the identity of an imported file that carries no
// producer identity: its content digest, as recorded in the absorbed set
// once a compaction merges it. Local (non-import) files have none.
func legacyContentIdentity(path string) (string, bool) {
	identity := immutableFileIdentity(path)
	if identity == path {
		return "", false
	}
	return "content:" + identity, true
}

// registration is a tablet generation being extended by an import: the live
// files in recency order (oldest first) and what files.json records about
// them. planRegistration works on a private copy.
type registration struct {
	active   []string
	sources  map[string]string
	absorbed map[string]struct{}
	lineage  map[string]Lineage
	// adopted lists the incoming objects that became live; redundant, the
	// incoming objects the registration did not need, which the caller
	// removes once the new generation is recorded.
	adopted   []string
	redundant []string
}

// incomingLineage is the lineage an incoming file is registered with: the
// producer's, when it sent a well-formed one within MaxLineageEntries, or
// else the file's producer identity as an opaque leaf. A file with neither
// gets a fresh leaf.
func incomingLineage(file ImportedFile, newLeaf func() Lineage) Lineage {
	if file.Lineage != nil {
		if l, ok := file.Lineage.Normalize(); ok && !l.IsEmpty() && l.Size() <= MaxLineageEntries {
			return l
		}
	}
	if file.Source != "" {
		return OpaqueLineage(file.Source)
	}
	return newLeaf()
}

// planRegistration decides, for each incoming file in order (oldest first),
// whether it is new to the tablet and where it goes. See
// RegisterImmutableFiles for the rules.
func planRegistration(r registration, files []ImportedFile, newLeaf func() Lineage) registration {
	isActive := make(map[string]struct{}, len(r.active))
	for _, path := range r.active {
		isActive[path] = struct{}{}
	}
	heldIdentity := func(source string) bool {
		if _, ok := r.absorbed[source]; ok {
			return true
		}
		for _, s := range r.sources {
			if s == source {
				return true
			}
		}
		return false
	}
	// held is every leaf the tablet holds: the union of its live files'
	// lineages.
	var held Lineage
	for _, path := range r.active {
		held = held.Union(r.lineage[filepath.Base(path)])
	}
	// learn records, for a file the tablet already holds by producer
	// identity, the leaves its producer now names it with: a file imported
	// before lineage existed is named only by that identity (an opaque
	// leaf), so every live file holding it holds those leaves too. Union is
	// sound because both names describe the same cells.
	learn := func(file ImportedFile) {
		if file.Lineage == nil || file.Source == "" {
			return
		}
		leaves, ok := file.Lineage.Normalize()
		if !ok || leaves.IsEmpty() {
			return
		}
		opaque := OpaqueLineage(file.Source)
		for _, path := range r.active {
			name := filepath.Base(path)
			current := r.lineage[name]
			if !current.Covers(opaque) || current.Covers(leaves) {
				continue
			}
			if grown := current.Union(leaves); grown.Size() <= MaxLineageEntries {
				r.lineage[name] = grown
				held = held.Union(leaves)
			}
		}
	}
	// claimLegacy claims a legacy import with file's content for its
	// source (see RegisterImmutableFiles).
	claimLegacy := func(file ImportedFile) bool {
		identity := immutableFileIdentity(file.Path)
		for _, existing := range r.active {
			name := filepath.Base(existing)
			if immutableFileIdentity(existing) == identity && r.sources[name] == "" {
				r.sources[name] = file.Source
				// Same cells, so it holds the incoming file's leaves too.
				named := r.lineage[name].Union(incomingLineage(file, newLeaf))
				if named.Size() <= MaxLineageEntries {
					r.lineage[name] = named
					held = held.Union(named)
				}
				return true
			}
		}
		if digest, ok := legacyContentIdentity(file.Path); ok {
			if _, found := r.absorbed[digest]; found {
				delete(r.absorbed, digest)
				r.absorbed[file.Source] = struct{}{}
				return true
			}
		}
		return false
	}
	contentHeld := func(path string) bool {
		identity := immutableFileIdentity(path)
		for _, existing := range r.active {
			if immutableFileIdentity(existing) == identity {
				return true
			}
		}
		return false
	}
	// deltaFor maps a file's Source to the first later file that is a
	// delta of it.
	deltaFor := make(map[string]int)
	for i, file := range files {
		if file.DeltaOf != "" && file.Source != "" {
			if _, ok := deltaFor[file.DeltaOf]; !ok {
				deltaFor[file.DeltaOf] = i
			}
		}
	}
	consumed := make(map[int]bool)
	// floor is the first position the next file may take: files of one
	// registration keep their given (recency) order among themselves.
	floor := 0
	place := func(file ImportedFile, l Lineage, at int) {
		if at < floor {
			at = floor
		}
		if at > len(r.active) {
			at = len(r.active)
		}
		r.active = append(r.active, "")
		copy(r.active[at+1:], r.active[at:])
		r.active[at] = file.Path
		isActive[file.Path] = struct{}{}
		r.adopted = append(r.adopted, file.Path)
		name := filepath.Base(file.Path)
		if file.Source != "" {
			r.sources[name] = file.Source
		}
		r.lineage[name] = l
		held = held.Union(l)
		floor = at + 1
	}
	for i, file := range files {
		if consumed[i] {
			continue
		}
		if _, ok := isActive[file.Path]; ok {
			continue
		}
		var matched bool
		switch {
		case file.Source == "":
			matched = contentHeld(file.Path)
		case heldIdentity(file.Source):
			matched = true
			learn(file)
		case len(r.adopted) == 0:
			matched = claimLegacy(file)
		}
		if matched {
			r.redundant = append(r.redundant, file.Path)
			continue
		}
		l := incomingLineage(file, newLeaf)
		have := held.Intersect(l)
		switch {
		case have.IsEmpty():
			// None of its data has arrived before: newest.
			place(file, l, len(r.active))
		case have.SameLeaves(l):
			// Every leaf has arrived before (a producer's compaction of
			// files this tablet already imported, or an echo of its own
			// data): every cell is already here, at the position it
			// arrived at. Adding the file anywhere would move some of its
			// cells — above writes that came after them, or, below its
			// newest leaf, beneath their own later copies — so it is held,
			// like a re-import.
			r.absorbed[file.Source] = struct{}{}
			r.redundant = append(r.redundant, file.Path)
		default:
			// Some leaves are new. Their cells are new arrivals and belong
			// newest; the rest are already here and must not move. A file
			// cannot be in two places, so the producer may send a DELTA —
			// the file's cells minus those of the leaves it had already
			// shipped. When this tablet holds exactly those leaves and
			// none of the delta's, the delta is precisely the new cells:
			// it is appended as newest and the file itself is held.
			if j, ok := deltaFor[file.Source]; ok && j > i && !consumed[j] {
				delta := files[j]
				if dl, ok := deltaLineage(delta, l, have); ok {
					consumed[j] = true
					place(delta, dl, len(r.active))
					r.absorbed[file.Source] = struct{}{}
					r.redundant = append(r.redundant, file.Path)
					continue
				}
			}
			// No usable delta: rank the file just above the newest file
			// holding one of its leaves. That keeps the producer's own
			// order (its new cells above its older ones) and never lifts
			// an already-held cell above data that arrived after that
			// newest leaf; it can rank a new cell below such data, and a
			// held cell from an older leaf above data that arrived between
			// the two leaves. Both are documented limits (#704).
			newest := -1
			for at := len(r.active) - 1; at >= 0; at-- {
				if r.lineage[filepath.Base(r.active[at])].Intersects(l) {
					newest = at
					break
				}
			}
			place(file, l, newest+1)
		}
	}
	return r
}

// deltaLineage validates a delta of an incoming file with lineage l, of
// which the tablet holds have: the delta must name only leaves of l that
// the tablet does not hold, and exactly all of them.
func deltaLineage(delta ImportedFile, l, have Lineage) (Lineage, bool) {
	if delta.Lineage == nil {
		return Lineage{}, false
	}
	dl, ok := delta.Lineage.Normalize()
	if !ok || dl.IsEmpty() || dl.Size() > MaxLineageEntries {
		return Lineage{}, false
	}
	// l \ dl == have also rules out a delta naming a leaf the tablet holds.
	if !l.Covers(dl) || !l.Subtract(dl).SameLeaves(have) {
		return Lineage{}, false
	}
	return dl, true
}

// RegisterImmutableFiles adds only checksum-verified import objects to the
// authoritative generation. The returned paths became authoritative even when
// the returned error reports cleanup after a committed manifest write.
//
// files are given oldest first (an import passes its manifest's recency
// order). A file is identified by producer identity (ImportedFile.Source)
// and by the leaves it holds (ImportedFile.Lineage, #704):
//
//   - A file whose producer identity the tablet holds — live, or absorbed by
//     a local compaction — is not re-added and never moves. Re-importing an
//     unchanged manifest is a no-op.
//   - A file all of whose leaves the tablet already holds (a producer's
//     compaction of files it imported; its own data echoed back; the same
//     data through another engine) is held too: its cells are already here,
//     each at the position it arrived at.
//   - A file none of whose leaves the tablet holds is appended as newest, in
//     the given order. On, off, on is three producer files.
//   - A file holding some leaves the tablet has and some it has not is
//     replaced by its DELTA (a later file with DeltaOf naming it), appended
//     as newest, when the delta names exactly the leaves the tablet lacks.
//     Otherwise it is ranked just above the newest live file holding one of
//     its leaves (see planRegistration for what that can and cannot get
//     wrong). A delta not used this way is registered like any file, which
//     makes it held: its leaves are its base file's.
//   - Legacy imports (registered before producer identities were recorded)
//     are matched by content digest instead: a live one is claimed in place,
//     an absorbed one is claimed in the absorbed set — each only once, and
//     only by a file that comes BEFORE anything this call adds. A file
//     after an added one is newer than it, so it must not be ranked below
//     it by claiming an older position.
//   - A file with no Source is matched by content digest against live files
//     (the pre-#692 rule).
//
// Files of one call never take positions out of their given order. The
// incoming object of a file that was not added is removed.
func (t *Tablet) RegisterImmutableFiles(files []ImportedFile) ([]string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ensureLineageLocked()
	r := registration{
		active:   append([]string(nil), t.files...),
		sources:  make(map[string]string, len(t.sources)+len(files)),
		absorbed: make(map[string]struct{}, len(t.absorbed)),
		lineage:  cloneLineageMap(t.lineage),
	}
	for name, source := range t.sources {
		r.sources[name] = source
	}
	for identity := range t.absorbed {
		r.absorbed[identity] = struct{}{}
	}
	r = planRegistration(r, files, t.newLeafLocked)
	for _, path := range r.adopted {
		delete(t.obsolete, filepath.Base(path))
	}
	state := t.state(r.active, t.obsolete)
	state.sources, state.absorbed, state.lineage = r.sources, r.absorbed, r.lineage
	commit := func() {
		t.files = r.active
		t.sources = r.sources
		t.absorbed = r.absorbed
		t.lineage = r.lineage
	}
	if err := t.persistLocked(state); err != nil {
		if storage.IsCommittedWriteError(err) {
			commit()
			return r.adopted, err
		}
		return nil, err
	}
	commit()
	for _, path := range r.redundant {
		if err := removeObject(t.backend, path); err != nil {
			return r.adopted, fmt.Errorf("tablet: remove redundant immutable file %s: %w", path, err)
		}
	}
	return r.adopted, nil
}

// PublishImmutableFiles records the authoritative immutable generation for a
// tablet directory. Importers call it after checksum verification so stale
// objects already present under the destination prefix are not rediscovered.
// files must be in recency order, oldest first (see Tablet.files); they are
// planned like RegisterImmutableFiles against an empty tablet, so a delta
// whose base file is in the same list is held, and its object removed.
//
// It replaces the directory's generation wholesale: only the given files'
// sources are recorded, and any earlier sources / absorbed identities are
// discarded (only the name high-water mark carries over). It is meant for a
// directory no open tablet serves — the first import of a table; an open
// tablet merges imports through RegisterImmutableFiles.
func PublishImmutableFiles(b storage.Backend, dir string, files []ImportedFile) error {
	keys, err := listTabletObjects(b, dir)
	if err != nil {
		return err
	}
	state := immutableState{
		obsolete: make(map[string]struct{}),
	}
	// Every object present counts toward the name high-water mark: each is
	// listed below as active or obsolete, and persistImmutableManifest
	// covers both. So does a mark an earlier files.json recorded for names
	// whose objects are already gone.
	for _, key := range keys {
		if filepath.Base(key) != "files.json" {
			continue
		}
		data, err := storage.ReadAll(context.Background(), b, key)
		if err != nil {
			return fmt.Errorf("read immutable manifest: %w", err)
		}
		var previous immutableManifest
		if err := json.Unmarshal(data, &previous); err != nil {
			return fmt.Errorf("decode immutable manifest: %w", err)
		}
		if previous.NameHighWater > state.nameHighWater {
			state.nameHighWater = previous.NameHighWater
		}
	}
	origin := newOrigin()
	var seq uint64
	plan := planRegistration(registration{
		sources:  make(map[string]string),
		absorbed: make(map[string]struct{}),
		lineage:  make(map[string]Lineage),
	}, files, func() Lineage {
		seq++
		return singleLeaf(origin, seq)
	})
	state.active, state.sources, state.absorbed, state.lineage =
		plan.active, plan.sources, plan.absorbed, plan.lineage
	activeNames := make(map[string]struct{}, len(plan.active))
	for _, path := range plan.active {
		activeNames[filepath.Base(path)] = struct{}{}
	}
	for _, key := range keys {
		if ext := filepath.Ext(key); ext == ".rf" || ext == ".parquet" {
			name := filepath.Base(key)
			if _, ok := activeNames[name]; !ok {
				state.obsolete[name] = struct{}{}
			}
		}
	}
	if err := persistImmutableManifest(b, dir, state); err != nil {
		return err
	}
	var cleanupErr error
	for _, path := range plan.redundant {
		if err := removeObject(b, path); err != nil {
			cleanupErr = errors.Join(cleanupErr,
				fmt.Errorf("tablet: remove redundant immutable file %s: %w", path, err))
		}
	}
	if cleanupErr != nil {
		return storage.MarkCommittedWrite(cleanupErr)
	}
	return nil
}
