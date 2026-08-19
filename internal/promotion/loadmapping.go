// Package promotion implements the client-side half of Accumulo's Bulk
// Import V2 protocol, used to promote an already-exported local Shoal
// table into an existing Accumulo cluster.
//
// Accumulo's manager is the sole authority over tablet metadata: promotion
// never edits accumulo.metadata or ZooKeeper directly. Instead this
// package:
//
//  1. Validates that a manifest is an unambiguous single-tablet export and
//     derives one fully unbounded destination KeyExtent from it.
//
//     Shoal's local tablets use [StartRow, EndRow) (inclusive start,
//     exclusive end — see engine/table.go's routeTablet), while
//     Accumulo's KeyExtent is (PrevEndRow, EndRow] (exclusive start,
//     inclusive end — see KeyExtent.java's contains()). Simply copying
//     split boundaries from a multi-tablet Shoal manifest into Accumulo
//     extents therefore makes rows whose value exactly equals a split
//     point invisible. Until a rewrite/materialization strategy exists for
//     split-bearing exports, BuildLoadMapping fails closed on any manifest
//     that declares multiple tablets or any non-nil tablet boundary.
//     Legacy manifests with no Tablets entries remain supported only when
//     every RFile uses TabletIndex 0.
//
//  2. Stages those RFiles flat into a bulk directory — Accumulo's bulk
//     import lists the bulk directory non-recursively, so files must sit
//     directly inside it, unlike ExportRFiles' per-tablet nesting — and
//     writes the load mapping as loadmap.json, in the exact
//     Gson-compatible JSON shape Accumulo's BulkSerialize /
//     LoadMappingIterator expect.
//
//  3. Submits the TABLE_BULK_IMPORT2 FATE operation via
//     accumulo.Connector.BulkImport — the only step that talks to
//     Accumulo, and it does so exclusively through the manager's FATE
//     machinery, preserving the manager as the sole authority over the
//     promoted table's resulting tablet layout and file set.
//
// See REFERENCES.md for the exact upstream Java sources this package
// mirrors, and docs/promotion.md for the protocol writeup, what this
// bounded slice covers, and what is intentionally deferred.
package promotion

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage"
)

// bulkLoadMappingFile is the fixed filename Accumulo's BulkSerialize reads
// the load mapping from (Constants.BULK_LOAD_MAPPING in Java).
const bulkLoadMappingFile = "loadmap.json"

// KeyExtent is a destination tablet range (PrevEndRow, EndRow]. A nil EndRow
// means "the last tablet" (positive infinity); a nil PrevEndRow means "the
// first tablet" (negative infinity) — matching Accumulo's own KeyExtent
// (core/.../dataImpl/KeyExtent.java) with the table ID omitted, since the
// FATE TABLE_BULK_IMPORT2 call already carries the destination table ID as
// its own argument.
type KeyExtent struct {
	EndRow     []byte
	PrevEndRow []byte
}

// FileEntry is one RFile contributed to a Mapping's KeyExtent. Name is the
// basename only, as the file will sit in the flat bulk directory.
// EstEntries is always 0: unlike Accumulo's own client, this package never
// opens RFiles to count entries, mirroring the estimate-free
// Bulk.FileInfo(Path, estSize) convenience constructor Accumulo itself
// falls back to when entry counts aren't available.
type FileEntry struct {
	Name       string
	EstSize    int64
	EstEntries int64
}

// Mapping is one destination KeyExtent's worth of files.
type Mapping struct {
	Tablet KeyExtent
	Files  []FileEntry
}

// LoadMapping is a complete Bulk Import V2 load mapping, ordered the way
// Accumulo's LoadMappingIterator requires: ascending by EndRow, with a nil
// (unbounded) EndRow sorting last.
type LoadMapping []Mapping

// BuildLoadMapping derives a Bulk Import V2 load mapping from an
// RFileExportManifest, but this safe first slice supports only
// unambiguous single-tablet exports. A manifest is accepted only when it
// either omits Tablets entirely and every RFile uses TabletIndex 0, or it
// declares exactly one tablet whose StartRow and EndRow are both nil.
//
// Any split-bearing or multi-tablet manifest is rejected before staging or
// submission: Shoal's [StartRow, EndRow) tablet boundaries cannot be
// copied safely into Accumulo's (PrevEndRow, EndRow] extents without a
// rewrite/materialization strategy for exact split-point rows.
//
// Accepted manifests yield a single, fully unbounded KeyExtent containing
// every exported file. Accumulo can legitimately load a file that spans
// many destination tablets under that one unbounded mapping entry, so the
// destination split layout does not have to mirror the source for this
// slice.
//
// RFiles sharing the same DestinationPath (e.g. a manifest listing one
// physical file more than once under the same tablet) contribute a single
// FileEntry: Accumulo's Bulk.Files is name-keyed and rejects duplicate
// filenames outright. A repeated DestinationPath under different
// TabletIndex values is rejected as ambiguous before any staging can omit
// it from loadmap.json.
//
// Legacy manifests with no Tablets entries (RFileExportManifest.Tablets
// nil/empty) map every RFile (TabletIndex 0) to a single unbounded tablet,
// mirroring RFileExportManifest.tabletCount()'s documented default.
// Manifests that omit Tablets but use any other TabletIndex are rejected as
// ambiguous legacy input.
func BuildLoadMapping(manifest *engine.RFileExportManifest) (LoadMapping, error) {
	if manifest == nil {
		return nil, fmt.Errorf("promotion: nil export manifest")
	}
	tablet, declared, err := resolveManifestTablet(manifest)
	if err != nil {
		return nil, err
	}

	files := make([]FileEntry, 0, len(manifest.RFiles))
	seen := make(map[string]int, len(manifest.RFiles))
	for _, rf := range manifest.RFiles {
		if _, ok := declared[rf.TabletIndex]; !ok {
			return nil, fmt.Errorf("promotion: rfile %q references undeclared tablet index %d", rf.DestinationPath, rf.TabletIndex)
		}
		if priorIndex, ok := seen[rf.DestinationPath]; ok {
			if priorIndex != rf.TabletIndex {
				return nil, fmt.Errorf(
					"promotion: rfile %q is declared under multiple tablet indexes (%d and %d)",
					rf.DestinationPath, priorIndex, rf.TabletIndex,
				)
			}
			continue
		}
		seen[rf.DestinationPath] = rf.TabletIndex
		files = append(files, FileEntry{
			Name:    filepath.Base(rf.DestinationPath),
			EstSize: rf.Size,
		})
	}
	if len(files) == 0 {
		return nil, nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	return LoadMapping{{
		Tablet: KeyExtent{
			EndRow:     rowBytes(tablet.EndRow),
			PrevEndRow: rowBytes(tablet.StartRow),
		},
		Files: files,
	}}, nil
}

func resolveManifestTablet(manifest *engine.RFileExportManifest) (engine.RFileExportTablet, map[int]struct{}, error) {
	if len(manifest.Tablets) == 0 {
		for _, rf := range manifest.RFiles {
			if rf.TabletIndex != 0 {
				return engine.RFileExportTablet{}, nil, fmt.Errorf(
					"promotion: legacy manifest without tablets is ambiguous: rfile %q references tablet index %d",
					rf.DestinationPath, rf.TabletIndex,
				)
			}
		}
		return engine.RFileExportTablet{Index: 0}, map[int]struct{}{0: {}}, nil
	}
	if len(manifest.Tablets) != 1 {
		return engine.RFileExportTablet{}, nil, fmt.Errorf(
			"promotion: split manifests unsupported in this slice: %d tablets declared",
			len(manifest.Tablets),
		)
	}
	tablet := manifest.Tablets[0]
	if tablet.StartRow != nil || tablet.EndRow != nil {
		return engine.RFileExportTablet{}, nil, fmt.Errorf(
			"promotion: split manifests unsupported in this slice: tablet index %d declares start/end boundaries",
			tablet.Index,
		)
	}
	return tablet, map[int]struct{}{tablet.Index: {}}, nil
}

func rowBytes(s *string) []byte {
	if s == nil {
		return nil
	}
	return []byte(*s)
}

// jsonTablet/jsonFileInfo/jsonMapping mirror Accumulo's
// clientImpl.bulk.Bulk.{Tablet,FileInfo,Mapping} Gson shape field-for-field
// (field names are case-sensitive and unmodified by any Gson naming
// policy). endRow/prevEndRow are base64url strings, omitted entirely when
// nil rather than serialized as null — Gson's ByteArrayToBase64TypeAdapter
// uses java.util.Base64.getUrlEncoder() (padded, URL-safe alphabet, i.e.
// Go's base64.URLEncoding) and Gson's default GsonBuilder suppresses null
// fields rather than emitting them.
type jsonTablet struct {
	EndRow     *string `json:"endRow,omitempty"`
	PrevEndRow *string `json:"prevEndRow,omitempty"`
}

type jsonFileInfo struct {
	Name       string `json:"name"`
	EstSize    int64  `json:"estSize"`
	EstEntries int64  `json:"estEntries"`
}

type jsonMapping struct {
	Tablet jsonTablet     `json:"tablet"`
	Files  []jsonFileInfo `json:"files"`
}

// WriteLoadMapping serializes mapping to <bulkDir>/loadmap.json on dst, as
// a top-level JSON array of {tablet:{endRow,prevEndRow},
// files:[{name,estSize,estEntries}]} objects — the exact shape Accumulo's
// clientImpl.bulk.LoadMappingIterator/BulkSerialize read.
func WriteLoadMapping(ctx context.Context, dst storage.Backend, bulkDir string, mapping LoadMapping) error {
	data, err := marshalLoadMapping(mapping)
	if err != nil {
		return err
	}
	path := joinBulkPath(dst, bulkDir, bulkLoadMappingFile)
	if err := storage.WriteAll(ctx, dst, path, data); err != nil {
		return fmt.Errorf("promotion: write load mapping %s: %w", path, err)
	}
	return nil
}

// ReadLoadMapping parses a loadmap.json previously written by
// WriteLoadMapping (or a real Accumulo client) at <bulkDir>/loadmap.json on
// src. Intended for tests and operator verification.
func ReadLoadMapping(ctx context.Context, src storage.Backend, bulkDir string) (LoadMapping, error) {
	path := joinBulkPath(src, bulkDir, bulkLoadMappingFile)
	data, err := storage.ReadAll(ctx, src, path)
	if err != nil {
		return nil, fmt.Errorf("promotion: read load mapping %s: %w", path, err)
	}
	return unmarshalLoadMapping(data)
}

func marshalLoadMapping(mapping LoadMapping) ([]byte, error) {
	out := make([]jsonMapping, len(mapping))
	for i, m := range mapping {
		files := make([]jsonFileInfo, len(m.Files))
		for j, f := range m.Files {
			files[j] = jsonFileInfo{Name: f.Name, EstSize: f.EstSize, EstEntries: f.EstEntries}
		}
		out[i] = jsonMapping{
			Tablet: jsonTablet{
				EndRow:     base64RowPtr(m.Tablet.EndRow),
				PrevEndRow: base64RowPtr(m.Tablet.PrevEndRow),
			},
			Files: files,
		}
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("promotion: marshal load mapping: %w", err)
	}
	return append(data, '\n'), nil
}

func unmarshalLoadMapping(data []byte) (LoadMapping, error) {
	var raw []jsonMapping
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("promotion: decode load mapping: %w", err)
	}
	mapping := make(LoadMapping, len(raw))
	for i, m := range raw {
		endRow, err := base64RowValue(m.Tablet.EndRow)
		if err != nil {
			return nil, fmt.Errorf("promotion: decode endRow: %w", err)
		}
		prevEndRow, err := base64RowValue(m.Tablet.PrevEndRow)
		if err != nil {
			return nil, fmt.Errorf("promotion: decode prevEndRow: %w", err)
		}
		files := make([]FileEntry, len(m.Files))
		for j, f := range m.Files {
			files[j] = FileEntry{Name: f.Name, EstSize: f.EstSize, EstEntries: f.EstEntries}
		}
		mapping[i] = Mapping{Tablet: KeyExtent{EndRow: endRow, PrevEndRow: prevEndRow}, Files: files}
	}
	return mapping, nil
}

func base64RowPtr(row []byte) *string {
	if row == nil {
		return nil
	}
	s := base64.URLEncoding.EncodeToString(row)
	return &s
}

func base64RowValue(s *string) ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	return base64.URLEncoding.DecodeString(*s)
}

// joinBulkPath mirrors engine's joinBackendPath: URL-style backend roots
// (scheme://..., plus HDFS's hdfs:/... authorityless form) join with a
// literal "/". Ambiguous one-character roots such as x://... are treated
// as backend URLs only when dst declares that scheme; otherwise they keep
// local Windows-drive semantics.
func joinBulkPath(dst storage.Backend, bulkDir, name string) string {
	if pathUsesBackendSeparatorJoinOnBackend(dst, bulkDir) {
		return strings.TrimRight(bulkDir, `/\`) + "/" + name
	}
	return filepath.Join(bulkDir, name)
}
