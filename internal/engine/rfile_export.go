package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/phrocker/shoal/internal/compaction"
	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/rfile/bcfile"
	"github.com/phrocker/shoal/internal/rfile/bcfile/block"
	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/tablet"
)

const (
	RFileExportManifestLegacyVersion = 1
	RFileExportManifestVersion       = 2
)

// producerIDRe constrains a fan-in producer id to characters that are safe in
// both object keys and local file names and that exclude the "~" namespacing
// separator used by exportRelPath.
var producerIDRe = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// validateProducerID rejects producer ids that would break destination naming
// (path separators, "~", or other unsafe characters). Empty is allowed and
// disables namespacing.
func validateProducerID(id string) error {
	if id == "" {
		return nil
	}
	if !producerIDRe.MatchString(id) {
		return fmt.Errorf("engine: invalid producer id %q (must match [A-Za-z0-9_.-]+)", id)
	}
	return nil
}

// RFileExportManifest describes a byte-for-byte RFile table export.
type RFileExportManifest struct {
	Version             int                 `json:"version"`
	CreatedAt           time.Time           `json:"created_at"`
	SourceTable         string              `json:"source_table"`
	EngineVersion       string              `json:"engine_version,omitempty"`
	RFileCompatibility  string              `json:"rfile_compatibility"`
	FileFormat          tablet.FileFormat   `json:"file_format,omitempty"`
	CFSchema            string              `json:"cf_schema,omitempty"`
	VisibilityStamp     string              `json:"visibility_stamp,omitempty"`
	AuthorizationsStamp string              `json:"authorizations_stamp,omitempty"`
	Tablets             []RFileExportTablet `json:"tablets"`
	RFiles              []RFileExportFile   `json:"rfiles"`
}

// RFileExportTablet describes one destination tablet's key-range
// boundaries in an RFile export manifest. StartRow/EndRow are nil for an
// unbounded side (matching Shoal's own [StartRow, EndRow) tablet
// convention; see engine/table.go's routeTablet), and are Go strings
// used purely as byte containers -- Accumulo row keys are arbitrary
// bytes, not necessarily valid UTF-8 text.
//
// MarshalJSON always writes StartRow/EndRow base64-encoded, under the
// dedicated start_row_b64/end_row_b64 JSON keys, instead of emitting them
// as ordinary JSON string literals under start_row/end_row: encoding/
// json's default string handling treats a Go string as UTF-8 text and
// silently replaces any invalid byte sequence with U+FFFD, which would
// irrecoverably corrupt a split row that is not valid UTF-8 the moment
// the manifest round-trips through JSON (see rfile_export_test.go's
// TestRFileExportTabletJSONRoundTripsNonUTF8Rows for a worked example,
// and REFERENCES.md/docs/promotion.md for why promotion depends on this
// byte-for-byte fidelity). This mirrors the same
// ByteArrayToBase64TypeAdapter-equivalent convention this repo already
// uses for Accumulo's own Bulk Import V2 loadmap.json wire format (see
// promotion.base64RowPtr/base64RowValue).
//
// UnmarshalJSON also still accepts the legacy plain start_row/end_row
// string keys, decoding them exactly as a raw string always was before
// this fix (unchanged, including its lossy-for-non-UTF-8-bytes
// behavior). exportTablets has, since long before this type gained
// custom JSON methods, been able to populate a non-nil StartRow/EndRow
// for any table with more than one tablet -- and cmd/shoal-embed's
// export/import commands persist and later re-read exactly this
// manifest shape through a storage backend, so a manifest already
// written to disk (or object storage) by an older build, and not yet
// imported, is a real artifact this package must still be able to read
// correctly, not merely a theoretical concern. Using a distinct JSON key
// for the base64 form (rather than sniffing whether a single field's
// contents happen to look like base64) is deliberate: a legacy raw
// string that happens to already be valid base64 would otherwise
// silently decode to different row bytes instead of failing loudly, the
// exact failure mode this package must not introduce. New manifests
// never round-trip through the legacy keys: MarshalJSON always emits
// only the *_b64 keys.
//
// Version 2 readers consume these byte-safe boundaries directly. Legacy
// version 1 remains accepted only for authoritative RFile manifests.
type RFileExportTablet struct {
	Index    int
	StartRow *string
	EndRow   *string
}

// rfileExportTabletJSON is RFileExportTablet's on-the-wire JSON shape.
// StartRowB64/EndRowB64 is the only form MarshalJSON ever writes;
// StartRow/EndRow (the pre-fix, plain-string keys) are accepted by
// UnmarshalJSON only, purely for reading an already-persisted legacy
// manifest -- see RFileExportTablet's doc comment.
type rfileExportTabletJSON struct {
	Index       int     `json:"index"`
	StartRowB64 *string `json:"start_row_b64,omitempty"`
	EndRowB64   *string `json:"end_row_b64,omitempty"`
	StartRow    *string `json:"start_row,omitempty"`
	EndRow      *string `json:"end_row,omitempty"`
}

// MarshalJSON implements json.Marshaler; see RFileExportTablet's doc
// comment for why StartRow/EndRow are base64-encoded under dedicated
// *_b64 keys.
func (t RFileExportTablet) MarshalJSON() ([]byte, error) {
	return json.Marshal(rfileExportTabletJSON{
		Index:       t.Index,
		StartRowB64: base64RowStringPtr(t.StartRow),
		EndRowB64:   base64RowStringPtr(t.EndRow),
	})
}

// UnmarshalJSON implements json.Unmarshaler; see RFileExportTablet's doc
// comment for the *_b64-preferred, legacy-plain-string-fallback decoding
// this performs.
func (t *RFileExportTablet) UnmarshalJSON(data []byte) error {
	var raw rfileExportTabletJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	startRow, err := decodeExportRow(raw.StartRowB64, raw.StartRow)
	if err != nil {
		return fmt.Errorf("engine: decode tablet %d start_row: %w", raw.Index, err)
	}
	endRow, err := decodeExportRow(raw.EndRowB64, raw.EndRow)
	if err != nil {
		return fmt.Errorf("engine: decode tablet %d end_row: %w", raw.Index, err)
	}
	t.Index = raw.Index
	t.StartRow = startRow
	t.EndRow = endRow
	return nil
}

// decodeExportRow resolves one row boundary from its two possible wire
// forms: b64 (the current, byte-safe representation this package always
// writes) takes priority when present; legacy (the pre-fix plain-string
// representation) is used as-is, unchanged from how every manifest ever
// written before this fix was already read, only when b64 is absent.
// Both being nil means an unbounded boundary; both being non-nil should
// never happen from any manifest this package itself writes, but is
// resolved deterministically (preferring b64) rather than rejected,
// since it is not itself unsafe -- merely redundant.
func decodeExportRow(b64, legacy *string) (*string, error) {
	if b64 != nil {
		return base64RowStringValue(b64)
	}
	if legacy != nil {
		v := *legacy
		return &v, nil
	}
	return nil, nil
}

// base64RowStringPtr base64-encodes row's raw bytes for the JSON wire
// form, or returns nil unchanged (an unbounded side stays absent from
// the JSON object via omitempty).
func base64RowStringPtr(row *string) *string {
	if row == nil {
		return nil
	}
	s := base64.URLEncoding.EncodeToString([]byte(*row))
	return &s
}

// base64RowStringValue reverses base64RowStringPtr.
func base64RowStringValue(encoded *string) (*string, error) {
	if encoded == nil {
		return nil, nil
	}
	decoded, err := base64.URLEncoding.DecodeString(*encoded)
	if err != nil {
		return nil, err
	}
	s := string(decoded)
	return &s, nil
}

type RFileExportFile struct {
	TabletIndex     int    `json:"tablet_index"`
	SourcePath      string `json:"source_path"`
	DestinationPath string `json:"destination_path"`
	RelativePath    string `json:"relative_path"`
	Size            int64  `json:"size"`
	SHA256          string `json:"sha256"`
	BCFileVersion   string `json:"bcfile_version,omitempty"`
	// Empty values are the legacy encoding of rfile/authoritative.
	Format string `json:"format,omitempty"`
	Role   string `json:"role,omitempty"`
}

const (
	ExportFormatRFile   = "rfile"
	ExportFormatParquet = "parquet"

	ExportRoleAuthoritative = "authoritative"
	ExportRoleDerived       = "derived"
)

type RFileExportOptions struct {
	DestinationRoot     string
	CFSchema            string
	VisibilityStamp     string
	AuthorizationsStamp string
	EngineVersion       string
	ManifestPath        string

	// StampVisibilityLabel, when non-empty, rewrites every exported cell's
	// ColumnVisibility to carry this tenant label (via the visibilityStamp
	// compaction iterator) instead of copying RFiles byte-for-byte. This is
	// what lets many independent producers fan their tables into one engine
	// while staying isolated: a scan only surfaces a producer's cells when
	// its Authorizations satisfy the stamped label. The label must be a bare
	// Accumulo CV label ([A-Za-z0-9_:./-]+). When set, it also defaults the
	// manifest's VisibilityStamp/AuthorizationsStamp metadata to the label.
	StampVisibilityLabel string
	// StampMode selects the stamping semantics: "and" (default) requires the
	// label on every cell; "whenEmpty" only stamps cells with no existing CV.
	StampMode string

	// ProducerID, when non-empty, namespaces every exported RFile's
	// destination object name with a "<producer>~" prefix on its base name
	// (e.g. graph/t-0000/agentA~F0001700000000000.rf). Local RFile names are
	// minted from a millisecond clock (F<ms>.rf / C<ms>.rf), so two producers
	// flushing into one shared destination at the same millisecond would
	// otherwise collide on an identical object key and clobber each other.
	// Prefixing with a stable per-producer id makes the destination names
	// globally unique, so many local agents can fan their RFiles into one
	// engine's tablet directories safely (the files still live directly in the
	// tablet dir and end in .rf, so import re-discovery finds them). Must match
	// [A-Za-z0-9_.-]+. It namespaces only the destination/manifest paths, never
	// the source RFiles.
	ProducerID string
}

// applyStampDefaults populates the manifest visibility metadata from the
// stamp label when the caller didn't set explicit values, so the manifest
// documents the enforced authorization.
func (o RFileExportOptions) applyStampDefaults(m *RFileExportManifest) {
	if o.StampVisibilityLabel == "" {
		return
	}
	if m.VisibilityStamp == "" {
		m.VisibilityStamp = o.StampVisibilityLabel
	}
	if m.AuthorizationsStamp == "" {
		m.AuthorizationsStamp = o.StampVisibilityLabel
	}
}

// ExportRFiles flushes table, copies its immutable RFiles to dst, and writes a manifest.
func (e *Engine) ExportRFiles(ctx context.Context, tableName string, dst storage.Backend, opts RFileExportOptions) (*RFileExportManifest, error) {
	if opts.DestinationRoot == "" {
		return nil, fmt.Errorf("engine: export destination root is required")
	}
	if err := validateProducerID(opts.ProducerID); err != nil {
		return nil, err
	}
	if err := e.Flush(tableName); err != nil {
		return nil, err
	}

	e.mu.RLock()
	tbl, ok := e.tables[tableName]
	var configuredFormat tablet.FileFormat
	if ok {
		configuredFormat = tbl.fileFormat()
	}
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("engine: table %q not found", tableName)
	}

	files := tbl.rfiles()
	compatibility := exportCompatibility(files)
	manifest := &RFileExportManifest{
		Version:             RFileExportManifestVersion,
		CreatedAt:           time.Now().UTC(),
		SourceTable:         tableName,
		EngineVersion:       opts.EngineVersion,
		RFileCompatibility:  compatibility,
		FileFormat:          configuredFormat,
		CFSchema:            opts.CFSchema,
		VisibilityStamp:     opts.VisibilityStamp,
		AuthorizationsStamp: opts.AuthorizationsStamp,
		Tablets:             tbl.exportTablets(),
		RFiles:              make([]RFileExportFile, 0, len(files)),
	}
	opts.applyStampDefaults(manifest)
	for _, f := range files {
		rel := e.exportRelPath(f, tableName, opts.ProducerID)
		manifestRel := rel
		dstPath := joinBackendPath(dst, opts.DestinationRoot, filepath.FromSlash(rel))
		size, sum, bcVersion, err := copyOrStampRFile(ctx, e.backend, f.Path, dst, dstPath, opts)
		if err != nil {
			return nil, err
		}
		manifest.RFiles = append(manifest.RFiles, RFileExportFile{
			TabletIndex:     f.TabletIndex,
			SourcePath:      f.Path,
			DestinationPath: dstPath,
			RelativePath:    manifestRel,
			Size:            size,
			SHA256:          sum,
			BCFileVersion:   bcVersion,
			Format:          string(fileFormatForPath(f.Path)),
			Role:            ExportRoleAuthoritative,
		})
	}
	sort.SliceStable(manifest.RFiles, func(i, j int) bool {
		if manifest.RFiles[i].TabletIndex != manifest.RFiles[j].TabletIndex {
			return manifest.RFiles[i].TabletIndex < manifest.RFiles[j].TabletIndex
		}
		return manifest.RFiles[i].DestinationPath < manifest.RFiles[j].DestinationPath
	})
	manifest.Version = exportManifestVersion(manifest)

	manifestPath := opts.ManifestPath
	if manifestPath == "" {
		manifestPath = joinBackendPath(dst, opts.DestinationRoot, "manifest.json")
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("engine: marshal manifest: %w", err)
	}
	if err := storage.WriteAll(ctx, dst, manifestPath, append(data, '\n')); err != nil {
		return nil, fmt.Errorf("engine: write manifest %s: %w", manifestPath, err)
	}
	return manifest, nil
}

// tabletCount is the number of tablet directories an import materializes for
// this manifest. A manifest with no explicit Tablets entries maps to the
// legacy single t-0000 tablet.
func (m *RFileExportManifest) tabletCount() int {
	if len(m.Tablets) == 0 {
		return 1
	}
	return len(m.Tablets)
}

func manifestSplits(manifest *RFileExportManifest) ([][]byte, error) {
	if len(manifest.Tablets) == 0 {
		return nil, nil
	}
	splits := make([][]byte, 0, len(manifest.Tablets)-1)
	var priorEnd *string
	for i, tb := range manifest.Tablets {
		if tb.Index != i {
			return nil, fmt.Errorf("engine: manifest tablet position %d declares index %d", i, tb.Index)
		}
		if !equalRowBoundary(tb.StartRow, priorEnd) {
			return nil, fmt.Errorf("engine: manifest tablet %d start row does not match prior end row", i)
		}
		if i+1 < len(manifest.Tablets) {
			if tb.EndRow == nil {
				return nil, fmt.Errorf("engine: manifest tablet %d has an unbounded end before the final tablet", i)
			}
			splits = append(splits, append([]byte(nil), []byte(*tb.EndRow)...))
		} else if tb.EndRow != nil {
			return nil, fmt.Errorf("engine: final manifest tablet %d must have an unbounded end", i)
		}
		priorEnd = tb.EndRow
	}
	return splits, nil
}

func equalRowBoundary(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func exportManifestVersion(manifest *RFileExportManifest) int {
	if manifest.FileFormat == tablet.FormatParquet ||
		(manifest.RFileCompatibility != "" && manifest.RFileCompatibility != "accumulo-rfile/shoal") {
		return RFileExportManifestVersion
	}
	for _, file := range manifest.RFiles {
		if (file.Format != "" && file.Format != ExportFormatRFile) ||
			(file.Role != "" && file.Role != ExportRoleAuthoritative) {
			return RFileExportManifestVersion
		}
	}
	return RFileExportManifestLegacyVersion
}

func joinBackendPath(dst storage.Backend, root, rel string) string {
	if usesBackendSeparatorJoinRoot(dst, root) {
		return strings.TrimRight(root, `/\`) + "/" + filepath.ToSlash(rel)
	}
	return filepath.Join(root, rel)
}

func usesBackendSeparatorJoinRoot(dst storage.Backend, root string) bool {
	return storage.UsesBackendPathJoin(dst, root)
}

// VerifyRFileExport verifies that every manifest object exists and matches size/hash.
func VerifyRFileExport(ctx context.Context, b storage.Backend, manifest *RFileExportManifest) error {
	if manifest == nil {
		return fmt.Errorf("engine: nil import manifest")
	}
	if manifest.Version != RFileExportManifestLegacyVersion && manifest.Version != RFileExportManifestVersion {
		return fmt.Errorf("engine: unsupported manifest version %d", manifest.Version)
	}
	if manifest.Version == RFileExportManifestLegacyVersion &&
		exportManifestVersion(manifest) != RFileExportManifestLegacyVersion {
		return fmt.Errorf("engine: manifest version %d is valid only for authoritative RFile exports", manifest.Version)
	}
	for _, rf := range manifest.RFiles {
		if rf.TabletIndex < 0 || rf.TabletIndex >= manifest.tabletCount() {
			return fmt.Errorf("engine: import file %q references undeclared tablet index %d", rf.DestinationPath, rf.TabletIndex)
		}
		role := rf.Role
		if role == "" {
			role = ExportRoleAuthoritative
		}
		if role != ExportRoleAuthoritative {
			return fmt.Errorf("engine: import file %q has role %q; only authoritative files are queryable", rf.DestinationPath, role)
		}
		size, sum, err := hashObject(ctx, b, rf.DestinationPath)
		if err != nil {
			return err
		}
		if size != rf.Size {
			return fmt.Errorf("engine: verify %s: size %d, want %d", rf.DestinationPath, size, rf.Size)
		}
		if sum != rf.SHA256 {
			return fmt.Errorf("engine: verify %s: sha256 %s, want %s", rf.DestinationPath, sum, rf.SHA256)
		}
	}
	return nil
}

// ImportRFileManifest verifies the manifest's RFiles and makes them queryable
// in this engine. The RFiles are expected to already be present at their
// DestinationPath on this engine's backend (export places them there) — import
// registers, it does not copy.
//
// Fan-in: a second import of a table this engine already serves MERGES the
// manifest's RFiles into the open table instead of dropping them. Because
// RFiles are immutable and uniquely named, merging is just a re-discovery of
// the tablet directories, which is idempotent and deduped — re-importing an
// unchanged manifest is a no-op, and importing a producer's freshly shipped
// RFiles makes them visible without a reopen. This is what lets many local
// agents export the same logical table into one cluster engine safely (with
// per-cell tenant visibility stamps providing isolation; see
// RFileExportOptions.StampVisibilityLabel).
func (e *Engine) ImportRFileManifest(ctx context.Context, manifest *RFileExportManifest) error {
	if err := VerifyRFileExport(ctx, e.backend, manifest); err != nil {
		return err
	}
	splits, err := manifestSplits(manifest)
	if err != nil {
		return err
	}
	tableDir := filepath.Join(e.dir, manifest.SourceTable)
	for _, tb := range manifest.Tablets {
		if err := os.MkdirAll(filepath.Join(tableDir, fmt.Sprintf("t-%04d", tb.Index)), 0o755); err != nil {
			return fmt.Errorf("engine: mkdir imported tablet: %w", err)
		}
	}
	if len(manifest.Tablets) == 0 {
		if err := os.MkdirAll(filepath.Join(tableDir, "t-0000"), 0o755); err != nil {
			return fmt.Errorf("engine: mkdir imported tablet: %w", err)
		}
	}
	filesByTablet := make(map[int][]string)
	for _, file := range manifest.RFiles {
		filesByTablet[file.TabletIndex] = append(filesByTablet[file.TabletIndex], file.DestinationPath)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if existing, exists := e.tables[manifest.SourceTable]; exists {
		if len(splits) != len(existing.splits) {
			return fmt.Errorf("engine: cannot merge import for table %q: manifest has %d tablet(s), open table has %d (divergent splits unsupported)",
				manifest.SourceTable, len(splits)+1, len(existing.tablets))
		}
		for i := range splits {
			if !bytes.Equal(splits[i], existing.splits[i]) {
				return fmt.Errorf("engine: cannot merge import for table %q: split %d is %x, open table has %x",
					manifest.SourceTable, i, splits[i], existing.splits[i])
			}
		}
		for i := range existing.tablets {
			if _, err := existing.tablets[i].RegisterImmutableFiles(filesByTablet[i]); err != nil {
				return fmt.Errorf("engine: merge import for table %q tablet %d: %w", manifest.SourceTable, i, err)
			}
		}
		return nil
	}
	format, err := tablet.ParseFileFormat(string(manifest.FileFormat))
	if err != nil {
		return err
	}
	if err := writeTableManifest(tableDir, tableManifest{
		Version:    tableManifestVersion,
		Splits:     splits,
		FileFormat: format,
	}); err != nil {
		return err
	}
	for i := 0; i < manifest.tabletCount(); i++ {
		tabletDir := filepath.Join(tableDir, fmt.Sprintf("t-%04d", i))
		if err := tablet.PublishImmutableFiles(e.backend, tabletDir, filesByTablet[i]); err != nil {
			return fmt.Errorf("engine: publish imported tablet %d manifest: %w", i, err)
		}
	}
	tbl, err := openTable(tableDir, manifest.SourceTable, e.logger, e.cache, e.walSyncMode, e.walSyncInterval, e.backend, e.publishRFile)
	if err != nil {
		return err
	}
	e.tables[manifest.SourceTable] = tbl
	return nil
}

// copyOrStampRFile copies an RFile byte-for-byte, or — when a tenant
// visibility label is configured — rewrites it through the visibilityStamp
// compaction iterator so every cell carries the label. Returns the written
// size, the destination SHA256, and the destination BCFile version.
func copyOrStampRFile(ctx context.Context, src storage.Backend, srcPath string, dst storage.Backend, dstPath string, opts RFileExportOptions) (int64, string, string, error) {
	if opts.StampVisibilityLabel == "" {
		return copyWithSHA256(ctx, src, srcPath, dst, dstPath)
	}
	return stampRFileObject(ctx, src, srcPath, dst, dstPath, opts.StampVisibilityLabel, opts.StampMode)
}

// stampRFileObject reads the whole source RFile, runs a single-input
// compaction that stamps every cell's ColumnVisibility with label, and
// writes the resulting RFile to dst. The rewrite necessarily changes the
// object bytes (and thus the SHA256), which is expected: the stamped copy is
// a distinct, tenant-scoped artifact, not a byte-identical mirror.
func stampRFileObject(ctx context.Context, src storage.Backend, srcPath string, dst storage.Backend, dstPath, label, mode string) (int64, string, string, error) {
	data, err := storage.ReadAll(ctx, src, srcPath)
	if err != nil {
		return 0, "", "", fmt.Errorf("engine: read export source %s: %w", srcPath, err)
	}
	stack := []iterrt.IterSpec{{
		Name: iterrt.IterVisibilityStamp,
		Options: map[string]string{
			iterrt.VisibilityStampLabelOption: label,
			iterrt.VisibilityStampModeOption:  mode,
		},
	}}
	res, err := compaction.Compact(compaction.Spec{
		Inputs:       []compaction.Input{{Name: srcPath, Bytes: data}},
		Stack:        stack,
		Scope:        iterrt.ScopeMajc,
		Codec:        block.CodecSnappy,
		OutputFormat: string(fileFormatForPath(srcPath)),
	})
	if err != nil {
		return 0, "", "", fmt.Errorf("engine: stamp export source %s: %w", srcPath, err)
	}
	if err := storage.WriteAll(ctx, dst, dstPath, res.Output); err != nil {
		return 0, "", "", fmt.Errorf("engine: write stamped export %s: %w", dstPath, err)
	}
	sum := sha256.Sum256(res.Output)
	bcVersion := ""
	if footer, ferr := bcfile.ReadFooter(bytes.NewReader(res.Output), int64(len(res.Output))); ferr == nil {
		bcVersion = footer.Version.String()
	}
	return int64(len(res.Output)), hex.EncodeToString(sum[:]), bcVersion, nil
}

func fileFormatForPath(path string) tablet.FileFormat {
	if filepath.Ext(path) == ".parquet" {
		return tablet.FormatParquet
	}
	return tablet.FormatRFile
}

func exportCompatibility(files []tableRFile) string {
	var rfile, parquet bool
	for _, file := range files {
		switch fileFormatForPath(file.Path) {
		case tablet.FormatParquet:
			parquet = true
		default:
			rfile = true
		}
	}
	switch {
	case rfile && parquet:
		return "mixed-rfile-parquet/shoal"
	case parquet:
		return "parquet/shoal"
	default:
		return "accumulo-rfile/shoal"
	}
}

func copyWithSHA256(ctx context.Context, src storage.Backend, srcPath string, dst storage.Backend, dstPath string) (written int64, sum string, bcVersion string, err error) {
	wb, ok := dst.(storage.WritableBackend)
	if !ok {
		return 0, "", "", storage.ErrReadOnly
	}
	in, err := src.Open(ctx, srcPath)
	if err != nil {
		return 0, "", "", fmt.Errorf("engine: open export source %s: %w", srcPath, err)
	}
	defer in.Close()
	footer, ferr := bcfile.ReadFooter(in, in.Size())
	if ferr == nil {
		bcVersion = footer.Version.String()
	}
	out, err := wb.Create(ctx, dstPath)
	if err != nil {
		return 0, "", "", fmt.Errorf("engine: create export destination %s: %w", dstPath, err)
	}
	var cleanupState storage.WriteCleanupState
	defer func() { storage.AbortOnError(&err, out, &cleanupState) }()
	h := sha256.New()
	buf := make([]byte, 256*1024)
	for written < in.Size() {
		want := int64(len(buf))
		if written+want > in.Size() {
			want = in.Size() - written
		}
		n, rerr := in.ReadAt(buf[:want], written)
		if n > 0 {
			chunk := buf[:n]
			wrote, werr := out.Write(chunk)
			if wrote > 0 {
				_, _ = h.Write(chunk[:wrote])
				written += int64(wrote)
			}
			if werr != nil {
				return written, "", "", fmt.Errorf("engine: write export %s: %w", dstPath, werr)
			}
			if wrote != len(chunk) {
				return written, "", "", fmt.Errorf("engine: write export %s: %w", dstPath, io.ErrShortWrite)
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				if written < in.Size() {
					return written, "", "", fmt.Errorf("engine: read export %s: %w", srcPath, io.ErrUnexpectedEOF)
				}
				break
			}
			return written, "", "", fmt.Errorf("engine: read export %s: %w", srcPath, rerr)
		}
	}
	if written != in.Size() {
		return written, "", "", fmt.Errorf("engine: read export %s: %w", srcPath, io.ErrUnexpectedEOF)
	}
	sum = hex.EncodeToString(h.Sum(nil))
	cleanupState.MarkCloseAttempted()
	if err = out.Close(); err != nil {
		return written, "", "", fmt.Errorf("engine: close export destination %s: %w", dstPath, err)
	}
	return written, sum, bcVersion, nil
}

func hashObject(ctx context.Context, b storage.Backend, path string) (int64, string, error) {
	f, err := b.Open(ctx, path)
	if err != nil {
		return 0, "", fmt.Errorf("engine: verify open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 256*1024)
	var off int64
	for off < f.Size() {
		want := int64(len(buf))
		if off+want > f.Size() {
			want = f.Size() - off
		}
		n, rerr := f.ReadAt(buf[:want], off)
		if n > 0 {
			_, _ = h.Write(buf[:n])
			off += int64(n)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return off, "", fmt.Errorf("engine: verify read %s: %w", path, rerr)
		}
	}
	return off, hex.EncodeToString(h.Sum(nil)), nil
}

type tableRFile struct {
	TabletIndex int
	Path        string
}

func (t *table) rfiles() []tableRFile {
	var out []tableRFile
	for i, tab := range t.tablets {
		tabFiles := tab.RFiles()
		for _, p := range tabFiles {
			out = append(out, tableRFile{TabletIndex: i, Path: p})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].TabletIndex != out[j].TabletIndex {
			return out[i].TabletIndex < out[j].TabletIndex
		}
		return out[i].Path < out[j].Path
	})
	return out
}

func (t *table) exportTablets() []RFileExportTablet {
	out := make([]RFileExportTablet, len(t.tablets))
	for i := range t.tablets {
		out[i] = RFileExportTablet{Index: i}
		if i > 0 && i-1 < len(t.splits) {
			s := string(t.splits[i-1])
			out[i].StartRow = &s
		}
		if i < len(t.splits) {
			s := string(t.splits[i])
			out[i].EndRow = &s
		}
	}
	return out
}
