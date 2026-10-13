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

import "github.com/phrocker/shoal-oss/internal/tablet"

// Export lineage (#704): every manifest entry carries the leaves its file
// holds (tablet.Lineage), so an importer can tell a producer's compaction
// output — a new file of data it has already received — from new data (see
// tablet.RegisterImmutableFiles). Older importers, and Accumulo promotion,
// ignore it.

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
