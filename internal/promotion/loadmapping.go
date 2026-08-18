// Package promotion implements the client-side half of Accumulo's Bulk
// Import V2 protocol, used to promote an already-exported local Shoal
// table into an existing Accumulo cluster.
//
// Accumulo's manager is the sole authority over tablet metadata: promotion
// never edits accumulo.metadata or ZooKeeper directly. Instead this
// package:
//
//  1. Computes a load mapping — which already-exported RFiles belong to
//     which destination KeyExtent — directly from the export manifest's
//     own tablet partitioning (engine.RFileExportManifest.Tablets). A
//     Shoal table already knows exactly which of its own splits each
//     RFile belongs to, so this is the same "caller-supplied partition"
//     style Accumulo's own client exposes as LoadPlan.RangeType.TABLE (see
//     BulkImport.java): there is no need to open RFiles and rediscover
//     tablet boundaries the way Accumulo's client does when importing
//     externally-produced files of unknown provenance.
//  2. Stages those RFiles flat into a bulk directory — Accumulo's bulk
//     import lists the bulk directory non-recursively, so files must sit
//     directly inside it, unlike ExportRFiles' per-tablet nesting — and
//     writes the load mapping as loadmap.json, in the exact
//     Gson-compatible JSON shape Accumulo's BulkSerialize /
//     LoadMappingIterator expect.
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

// BuildLoadMapping derives a Bulk Import V2 load mapping directly from an
// RFileExportManifest's own tablet partitioning: every RFile already
// records which of the source table's tablets it belongs to (TabletIndex),
// and every tablet already records its own (StartRow, EndRow) range, so the
// destination KeyExtent for each group of files is exactly the source
// tablet's range — no RFile-index scanning or destination-tablet lookup is
// required. Manifest.Tablets is already ordered ascending by Index with the
// unbounded (nil EndRow) tablet last, so the result preserves that order
// without re-sorting. Tablets with no files are omitted: there is nothing
// to import for that range.
//
// Legacy manifests with no Tablets entries (RFileExportManifest.Tablets
// nil/empty) map every RFile (TabletIndex 0) to a single unbounded tablet,
// mirroring RFileExportManifest.tabletCount()'s documented default.
func BuildLoadMapping(manifest *engine.RFileExportManifest) (LoadMapping, error) {
	if manifest == nil {
		return nil, fmt.Errorf("promotion: nil export manifest")
	}
	tablets := manifest.Tablets
	if len(tablets) == 0 {
		tablets = []engine.RFileExportTablet{{Index: 0}}
	}

	byIndex := make(map[int][]FileEntry, len(tablets))
	for _, rf := range manifest.RFiles {
		byIndex[rf.TabletIndex] = append(byIndex[rf.TabletIndex], FileEntry{
			Name:    filepath.Base(rf.DestinationPath),
			EstSize: rf.Size,
		})
	}

	mapping := make(LoadMapping, 0, len(tablets))
	for _, t := range tablets {
		files := byIndex[t.Index]
		if len(files) == 0 {
			continue
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
		mapping = append(mapping, Mapping{
			Tablet: KeyExtent{
				EndRow:     rowBytes(t.EndRow),
				PrevEndRow: rowBytes(t.StartRow),
			},
			Files: files,
		})
	}
	return mapping, nil
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
	path := joinBulkPath(bulkDir, bulkLoadMappingFile)
	if err := storage.WriteAll(ctx, dst, path, data); err != nil {
		return fmt.Errorf("promotion: write load mapping %s: %w", path, err)
	}
	return nil
}

// ReadLoadMapping parses a loadmap.json previously written by
// WriteLoadMapping (or a real Accumulo client) at <bulkDir>/loadmap.json on
// src. Intended for tests and operator verification.
func ReadLoadMapping(ctx context.Context, src storage.Backend, bulkDir string) (LoadMapping, error) {
	path := joinBulkPath(bulkDir, bulkLoadMappingFile)
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

// joinBulkPath mirrors engine's joinBackendPath: URL-style roots
// (scheme://...) join with a literal "/", local-style roots join with
// filepath.Join.
func joinBulkPath(bulkDir, name string) string {
	if strings.Contains(bulkDir, "://") {
		return strings.TrimRight(bulkDir, `/\`) + "/" + name
	}
	return filepath.Join(bulkDir, name)
}
