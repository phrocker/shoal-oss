package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
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
)

const RFileExportManifestVersion = 1

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
	CFSchema            string              `json:"cf_schema,omitempty"`
	VisibilityStamp     string              `json:"visibility_stamp,omitempty"`
	AuthorizationsStamp string              `json:"authorizations_stamp,omitempty"`
	Tablets             []RFileExportTablet `json:"tablets"`
	RFiles              []RFileExportFile   `json:"rfiles"`
}

type RFileExportTablet struct {
	Index    int     `json:"index"`
	StartRow *string `json:"start_row,omitempty"`
	EndRow   *string `json:"end_row,omitempty"`
}

type RFileExportFile struct {
	TabletIndex     int    `json:"tablet_index"`
	SourcePath      string `json:"source_path"`
	DestinationPath string `json:"destination_path"`
	RelativePath    string `json:"relative_path"`
	Size            int64  `json:"size"`
	SHA256          string `json:"sha256"`
	BCFileVersion   string `json:"bcfile_version,omitempty"`
}

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
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("engine: table %q not found", tableName)
	}

	files := tbl.rfiles()
	manifest := &RFileExportManifest{
		Version:             RFileExportManifestVersion,
		CreatedAt:           time.Now().UTC(),
		SourceTable:         tableName,
		EngineVersion:       opts.EngineVersion,
		RFileCompatibility:  "accumulo-rfile/shoal",
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
		dstPath := joinBackendPath(opts.DestinationRoot, filepath.FromSlash(rel))
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
		})
	}
	sort.SliceStable(manifest.RFiles, func(i, j int) bool {
		if manifest.RFiles[i].TabletIndex != manifest.RFiles[j].TabletIndex {
			return manifest.RFiles[i].TabletIndex < manifest.RFiles[j].TabletIndex
		}
		return manifest.RFiles[i].DestinationPath < manifest.RFiles[j].DestinationPath
	})

	manifestPath := opts.ManifestPath
	if manifestPath == "" {
		manifestPath = joinBackendPath(opts.DestinationRoot, "manifest.json")
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

func joinBackendPath(root, rel string) string {
	if strings.Contains(root, "://") {
		return strings.TrimRight(root, `/\`) + "/" + filepath.ToSlash(rel)
	}
	return filepath.Join(root, rel)
}

// VerifyRFileExport verifies that every manifest object exists and matches size/hash.
func VerifyRFileExport(ctx context.Context, b storage.Backend, manifest *RFileExportManifest) error {
	if manifest == nil {
		return fmt.Errorf("engine: nil import manifest")
	}
	if manifest.Version != RFileExportManifestVersion {
		return fmt.Errorf("engine: unsupported manifest version %d", manifest.Version)
	}
	for _, rf := range manifest.RFiles {
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

	e.mu.Lock()
	defer e.mu.Unlock()
	if existing, exists := e.tables[manifest.SourceTable]; exists {
		// Guard the one merge case re-discovery can't reconcile: a manifest
		// that introduces more tablets (divergent splits) than the open table
		// has. Same-layout fan-in (the common case: same logical table from
		// many producers) refreshes cleanly.
		if manifest.tabletCount() > len(existing.tablets) {
			return fmt.Errorf("engine: cannot merge import for table %q: manifest has %d tablet(s), open table has %d (divergent splits unsupported)",
				manifest.SourceTable, manifest.tabletCount(), len(existing.tablets))
		}
		if _, err := existing.refreshFiles(); err != nil {
			return fmt.Errorf("engine: merge import for table %q: %w", manifest.SourceTable, err)
		}
		return nil
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
		Inputs: []compaction.Input{{Name: srcPath, Bytes: data}},
		Stack:  stack,
		Scope:  iterrt.ScopeMajc,
		Codec:  block.CodecSnappy,
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

func copyWithSHA256(ctx context.Context, src storage.Backend, srcPath string, dst storage.Backend, dstPath string) (int64, string, string, error) {
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
	bcVersion := ""
	if ferr == nil {
		bcVersion = footer.Version.String()
	}
	out, err := wb.Create(ctx, dstPath)
	if err != nil {
		return 0, "", "", fmt.Errorf("engine: create export destination %s: %w", dstPath, err)
	}
	defer out.Close()
	h := sha256.New()
	buf := make([]byte, 256*1024)
	var off int64
	for off < in.Size() {
		want := int64(len(buf))
		if off+want > in.Size() {
			want = in.Size() - off
		}
		n, rerr := in.ReadAt(buf[:want], off)
		if n > 0 {
			chunk := buf[:n]
			if _, err := out.Write(chunk); err != nil {
				return off, "", "", fmt.Errorf("engine: write export %s: %w", dstPath, err)
			}
			_, _ = h.Write(chunk)
			off += int64(n)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return off, "", "", fmt.Errorf("engine: read export %s: %w", srcPath, rerr)
		}
	}
	return off, hex.EncodeToString(h.Sum(nil)), bcVersion, nil
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
