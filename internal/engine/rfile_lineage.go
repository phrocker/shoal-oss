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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/phrocker/shoal-oss/internal/rfile/bcfile"
	"github.com/phrocker/shoal-oss/internal/storage"
	"github.com/phrocker/shoal-oss/internal/tablet"
)

// Export lineage (#704).
//
// Every manifest entry carries the leaves its file holds (tablet.Lineage),
// so an importer can tell a producer's compaction output — a new file of
// data it has already received — from new data. When an output holds some
// leaves this destination was already sent and some it was not, the export
// also ships a DELTA (RFileExportManifest.Deltas): the output's cells minus
// the cells of the already-sent files that hold those leaves, i.e. exactly
// the cells of the leaves not yet sent. An importer that holds exactly the
// sent leaves appends the delta as newest and keeps the output's other
// cells where they already are; see tablet.RegisterImmutableFiles.
//
// "Already sent" is the destination's previous manifest: SyncState.Shipped
// for an incremental export, the manifest already at ManifestPath for a full
// one. Deltas are an optimisation an importer validates (it uses one only if
// its own holdings match), so a consumer that skipped a manifest, or holds
// data it got elsewhere, just does not use it. Older importers, and Accumulo
// promotion, ignore Deltas: they see only RFiles, as before.

// exportFiles lists the table's immutable files like rfiles, with lineage.
func (t *table) exportFiles() ([]tableRFile, error) {
	var out []tableRFile
	for i, tab := range t.tablets {
		files, err := tab.FilesWithLineage()
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			lineage := f.Lineage
			out = append(out, tableRFile{TabletIndex: i, Path: f.Path, Lineage: &lineage})
		}
	}
	return out, nil
}

// exportLineage is the lineage a manifest entry carries for f. A visibility
// stamp rewrites every cell, so a stamped export's leaves are derived from
// the source's under the stamp — the stamped cells are never mistaken for
// the unstamped ones they came from, and two stamped exports of the same
// data under the same stamp still agree.
func exportLineage(l *tablet.Lineage, opts RFileExportOptions) *tablet.Lineage {
	if l == nil || l.IsEmpty() {
		return nil
	}
	out := l.Clone()
	if opts.StampVisibilityLabel != "" {
		out = out.Derive("stamp\x00" + opts.StampVisibilityLabel + "\x00" + opts.StampMode)
	}
	return &out
}

// priorManifestEntries reads the manifest an earlier full export left at
// manifestPath on dst, for buildExportDeltas. Anything unreadable, or a
// manifest of another table, is no prior at all.
func priorManifestEntries(ctx context.Context, dst storage.Backend, manifestPath, table string) []RFileExportFile {
	data, err := storage.ReadAll(ctx, dst, manifestPath)
	if err != nil {
		return nil
	}
	var prior RFileExportManifest
	if err := json.Unmarshal(data, &prior); err != nil || prior.SourceTable != table {
		return nil
	}
	return prior.RFiles
}

func (e *Engine) exportLogger() *slog.Logger {
	if e.logger != nil {
		return e.logger
	}
	return slog.Default()
}

// buildExportDeltas returns the deltas for entries (this export's files, in
// manifest order) against prior (the files the destination's previous
// manifest listed). A delta that cannot be computed exactly is skipped: the
// importer then ranks the output as it would without one.
func (e *Engine) buildExportDeltas(
	ctx context.Context,
	dst storage.Backend,
	opts RFileExportOptions,
	entries []RFileExportFile,
	prior []RFileExportFile,
) []RFileExportFile {
	byTablet := make(map[int][]RFileExportFile)
	incomplete := make(map[int]bool)
	for _, p := range prior {
		if p.Lineage == nil || p.Lineage.IsEmpty() {
			// The tablet's sent leaves are not fully known.
			incomplete[p.TabletIndex] = true
			continue
		}
		byTablet[p.TabletIndex] = append(byTablet[p.TabletIndex], p)
	}
	var deltas []RFileExportFile
	for _, entry := range entries {
		if entry.Lineage == nil || entry.Lineage.Overlap || incomplete[entry.TabletIndex] {
			continue
		}
		delta, ok, err := e.exportDelta(ctx, dst, opts, entry, byTablet[entry.TabletIndex])
		if err != nil {
			e.exportLogger().Warn("export delta skipped",
				slog.String("file", entry.RelativePath), slog.String("err", err.Error()))
			continue
		}
		if ok {
			deltas = append(deltas, delta)
		}
	}
	return deltas
}

// exportDelta computes entry's delta against the tablet's previously sent
// files, if entry holds some sent leaves and some unsent ones.
func (e *Engine) exportDelta(
	ctx context.Context,
	dst storage.Backend,
	opts RFileExportOptions,
	entry RFileExportFile,
	sent []RFileExportFile,
) (RFileExportFile, bool, error) {
	lineage := *entry.Lineage
	var sentLeaves tablet.Lineage
	for _, s := range sent {
		sentLeaves = sentLeaves.Union(*s.Lineage)
	}
	have := lineage.Intersect(sentLeaves)
	if have.IsEmpty() || have.SameLeaves(lineage) {
		return RFileExportFile{}, false, nil
	}
	// The basis is the sent files holding entry's sent leaves. Their cells
	// must be exactly those leaves' cells, each once: every one wholly
	// inside entry's lineage, none holding a leaf twice, none sharing a
	// leaf with another.
	var basis []RFileExportFile
	var covered tablet.Lineage
	for _, s := range sent {
		if !s.Lineage.Intersects(lineage) {
			continue
		}
		if s.Lineage.Overlap || !lineage.Covers(*s.Lineage) || covered.Intersects(*s.Lineage) {
			return RFileExportFile{}, false, nil
		}
		covered = covered.Union(*s.Lineage)
		basis = append(basis, s)
	}
	if !covered.SameLeaves(have) {
		return RFileExportFile{}, false, nil
	}
	image, err := readVerified(ctx, dst, entry)
	if err != nil {
		return RFileExportFile{}, false, err
	}
	images := make([][]byte, len(basis))
	sums := make([]string, len(basis))
	for i, b := range basis {
		if images[i], err = readVerified(ctx, dst, b); err != nil {
			return RFileExportFile{}, false, err
		}
		sums[i] = b.SHA256
	}
	data, _, err := tablet.SubtractImage(entry.DestinationPath, image, images)
	if err != nil {
		return RFileExportFile{}, false, err
	}
	sort.Strings(sums)
	tag := sha256.Sum256([]byte(entry.SHA256 + "\x00" + strings.Join(sums, "\x00")))
	dir, base := path.Split(entry.RelativePath)
	ext := path.Ext(base)
	rel := dir + strings.TrimSuffix(base, ext) + "~d" + hex.EncodeToString(tag[:8]) + ext
	dstPath := joinBackendPath(dst, opts.DestinationRoot, filepath.FromSlash(rel))
	if err := storage.WriteAll(ctx, dst, dstPath, data); err != nil {
		return RFileExportFile{}, false, fmt.Errorf("write delta %s: %w", dstPath, err)
	}
	sum := sha256.Sum256(data)
	bcVersion := ""
	if footer, ferr := bcfile.ReadFooter(bytes.NewReader(data), int64(len(data))); ferr == nil {
		bcVersion = footer.Version.String()
	}
	deltaLineage := lineage.Subtract(sentLeaves)
	return RFileExportFile{
		TabletIndex:     entry.TabletIndex,
		SourcePath:      entry.SourcePath,
		DestinationPath: dstPath,
		RelativePath:    rel,
		Size:            int64(len(data)),
		SHA256:          hex.EncodeToString(sum[:]),
		BCFileVersion:   bcVersion,
		Format:          entry.Format,
		Role:            ExportRoleAuthoritative,
		Lineage:         &deltaLineage,
		DeltaOf:         entry.RelativePath,
	}, true, nil
}

// readVerified reads a manifest entry's object from b and checks it still
// holds the bytes the entry describes.
func readVerified(ctx context.Context, b storage.Backend, entry RFileExportFile) ([]byte, error) {
	data, err := storage.ReadAll(ctx, b, entry.DestinationPath)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != entry.SHA256 || int64(len(data)) != entry.Size {
		return nil, errors.New("object " + entry.DestinationPath + " no longer holds its manifest bytes")
	}
	return data, nil
}

// manifestDeltas returns, for each RFiles index, the deltas of that file in
// manifest.Deltas: those naming an entry of the same tablet by its
// relative path. Others are ignored — a delta is only ever an optional
// replacement for its base file.
func manifestDeltas(manifest *RFileExportManifest) ([]RFileExportFile, map[int][]int) {
	if len(manifest.Deltas) == 0 {
		return nil, nil
	}
	type key struct {
		tablet int
		rel    string
	}
	bases := make(map[key]int, len(manifest.RFiles))
	for i, f := range manifest.RFiles {
		if f.RelativePath != "" {
			bases[key{f.TabletIndex, f.RelativePath}] = i
		}
	}
	var deltas []RFileExportFile
	byBase := make(map[int][]int)
	for _, d := range manifest.Deltas {
		i, ok := bases[key{d.TabletIndex, d.DeltaOf}]
		if !ok || d.DeltaOf == "" || d.RelativePath == "" {
			continue
		}
		byBase[i] = append(byBase[i], len(deltas))
		deltas = append(deltas, d)
	}
	return deltas, byBase
}
