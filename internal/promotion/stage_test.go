package promotion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/phrocker/shoal/accumulo"
	"github.com/phrocker/shoal/internal/engine"
	shstorage "github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/storage/azure"
	"github.com/phrocker/shoal/internal/storage/diskcache"
	"github.com/phrocker/shoal/internal/storage/gcs"
	"github.com/phrocker/shoal/internal/storage/hdfs"
	"github.com/phrocker/shoal/internal/storage/local"
	"github.com/phrocker/shoal/internal/storage/memory"
	"github.com/phrocker/shoal/internal/storage/s3"
)

type schemeAwareBackend struct {
	shstorage.Backend
	schemes []string
}

func (b schemeAwareBackend) BackendPathSchemes() []string {
	return b.schemes
}

func TestStageBulkDirFlattensCopiesAndWritesLoadMapping(t *testing.T) {
	src := memory.New()
	src.Put("export/events/t-0000/F0001.rf", []byte("tablet0-file1"))
	src.Put("export/events/t-0000/F0002.rf", []byte("tablet0-file2"))

	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 13, SHA256: "e47fb24ed70774dd8af7d59bf58fc740e126716aac3474bd262eb17f3e395e43"},
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0002.rf", Size: 13, SHA256: "066219c3f0f1a1bfccf12ef7e4d6957d102b3bf2e047fca44ba178a92e526cf0"},
		},
	}

	dst := memory.New()
	ctx := context.Background()
	mapping, err := StageBulkDir(ctx, src, manifest, dst, "hdfs://nn/bulk/events-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(mapping) != 1 {
		t.Fatalf("mapping entries = %d, want 1", len(mapping))
	}
	if mapping[0].Tablet.EndRow != nil || mapping[0].Tablet.PrevEndRow != nil {
		t.Fatalf("mapping tablet = %#v, want fully unbounded single tablet", mapping[0].Tablet)
	}

	for _, want := range []string{
		"hdfs://nn/bulk/events-1/F0001.rf",
		"hdfs://nn/bulk/events-1/F0002.rf",
		"hdfs://nn/bulk/events-1/loadmap.json",
	} {
		f, err := dst.Open(ctx, want)
		if err != nil {
			t.Fatalf("expected staged path %s: %v", want, err)
		}
		f.Close()
	}

	f, err := dst.Open(ctx, "hdfs://nn/bulk/events-1/F0001.rf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, f.Size())
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "tablet0-file1" {
		t.Fatalf("staged content = %q, want %q", buf, "tablet0-file1")
	}

	onDisk, err := ReadLoadMapping(ctx, dst, "hdfs://nn/bulk/events-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(onDisk) != len(mapping) {
		t.Fatalf("on-disk mapping entries = %d, want %d", len(onDisk), len(mapping))
	}
}

func TestStageBulkDirRejectsBasenameCollisionBeforeCopying(t *testing.T) {
	src := memory.New()
	src.Put("export/events/t-0000/part-a/F0001.rf", []byte("a"))
	src.Put("export/events/t-0000/part-b/F0001.rf", []byte("b"))

	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/part-a/F0001.rf", Size: 1},
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/part-b/F0001.rf", Size: 1},
		},
	}

	dst := memory.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, src, manifest, dst, "/bulk/events-1"); err == nil {
		t.Fatal("StageBulkDir with colliding basenames = nil error, want error")
	}
	if got := dst.Keys(); len(got) != 0 {
		t.Fatalf("StageBulkDir wrote %v on collision error, want no partial writes", got)
	}
}

func TestFlattenNamesAllowsSameDestinationPathTwice(t *testing.T) {
	rfiles := []engine.RFileExportFile{
		{DestinationPath: "export/events/t-0000/F0001.rf"},
		{DestinationPath: "export/events/t-0000/F0001.rf"},
	}
	names, err := flattenNames(rfiles)
	if err != nil {
		t.Fatalf("flattenNames with duplicate identical entries = %v, want nil error", err)
	}
	if names["export/events/t-0000/F0001.rf"] != "F0001.rf" {
		t.Fatalf("flattenNames = %v", names)
	}
}

func TestStageBulkDirRejectsCorruptSourceBeforeCopying(t *testing.T) {
	src := memory.New()
	src.Put("export/events/t-0000/F0001.rf", []byte("actual bytes on disk"))

	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 999, SHA256: "0000000000000000000000000000000000000000000000000000000000000000"},
		},
	}
	dst := memory.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, src, manifest, dst, "/bulk/events-1"); err == nil {
		t.Fatal("StageBulkDir with mismatched manifest size/sha256 = nil error, want error")
	}
	if got := dst.Keys(); len(got) != 0 {
		t.Fatalf("StageBulkDir wrote %v on verification failure, want no partial writes", got)
	}
}

func TestStageBulkDirDedupesRepeatedDestinationPathCopy(t *testing.T) {
	src := memory.New()
	src.Put("export/events/t-0000/F0001.rf", []byte("tablet0-file1"))

	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 13, SHA256: "e47fb24ed70774dd8af7d59bf58fc740e126716aac3474bd262eb17f3e395e43"},
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 13, SHA256: "e47fb24ed70774dd8af7d59bf58fc740e126716aac3474bd262eb17f3e395e43"},
		},
	}
	dst := memory.New()
	ctx := context.Background()
	mapping, err := StageBulkDir(ctx, src, manifest, dst, "/bulk/events-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(mapping) != 1 || len(mapping[0].Files) != 1 {
		t.Fatalf("mapping = %#v, want a single deduped FileEntry", mapping)
	}
	if got := dst.Keys(); len(got) != 2 {
		t.Fatalf("dst.Keys() = %v, want exactly 2 entries (one staged file + loadmap.json)", got)
	}
}

func TestStageBulkDirDedupesPhysicalSourceAliasesBeforeCopying(t *testing.T) {
	tests := []struct {
		name  string
		alias func(realPath string) (string, error)
	}{
		{
			name: "hardlink alias with same basename",
			alias: func(realPath string) (string, error) {
				aliasDir := filepath.Join(filepath.Dir(filepath.Dir(realPath)), "hardlink-alias")
				if err := os.MkdirAll(aliasDir, 0o755); err != nil {
					return "", err
				}
				aliasPath := filepath.Join(aliasDir, filepath.Base(realPath))
				return aliasPath, os.Link(realPath, aliasPath)
			},
		},
		{
			name: "symlinked parent alias with same basename",
			alias: func(realPath string) (string, error) {
				aliasDir := filepath.Join(filepath.Dir(filepath.Dir(realPath)), "symlink-alias")
				if err := os.Symlink(filepath.Dir(realPath), aliasDir); err != nil {
					return "", err
				}
				return filepath.Join(aliasDir, filepath.Base(realPath)), nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			realDir := filepath.Join(root, "real")
			if err := os.MkdirAll(realDir, 0o755); err != nil {
				t.Fatal(err)
			}
			realPath := filepath.Join(realDir, "F0001.rf")
			content := []byte("physical source bytes")
			if err := os.WriteFile(realPath, content, 0o644); err != nil {
				t.Fatal(err)
			}
			aliasPath, err := tt.alias(realPath)
			if err != nil {
				t.Skipf("alias setup not supported in this environment: %v", err)
			}

			manifest := localManifestFromFiles(t, realPath, aliasPath)
			dst := memory.New()
			mapping, err := StageBulkDir(context.Background(), local.New(), manifest, dst, "/bulk/events-1")
			if err != nil {
				t.Fatalf("StageBulkDir with physical source aliases: %v", err)
			}
			if len(mapping) != 1 || len(mapping[0].Files) != 1 {
				t.Fatalf("mapping = %#v, want one deduped staged file", mapping)
			}
			if got := dst.Keys(); len(got) != 2 {
				t.Fatalf("dst.Keys() = %v, want one staged file plus loadmap.json", got)
			}
		})
	}
}

func TestStageBulkDirRejectsPhysicalSourceAliasesWithDifferentBasenames(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	aPath := filepath.Join(exportDir, "A.rf")
	if err := os.WriteFile(aPath, []byte("same physical source"), 0o644); err != nil {
		t.Fatal(err)
	}
	bPath := filepath.Join(exportDir, "B.rf")
	if err := os.Link(aPath, bPath); err != nil {
		t.Skipf("hard links not supported in this environment: %v", err)
	}

	manifest := localManifestFromFiles(t, aPath, bPath)
	dst := memory.New()
	if _, err := StageBulkDir(context.Background(), local.New(), manifest, dst, "/bulk/events-1"); err == nil {
		t.Fatal("StageBulkDir with one physical source declared under two basenames = nil error, want error")
	}
	if got := dst.Keys(); len(got) != 0 {
		t.Fatalf("StageBulkDir wrote %v on ambiguous physical source alias, want no writes", got)
	}
}

func TestStageBulkDirRejectsInvalidBulkDirBeforeCopying(t *testing.T) {
	src := memory.New()
	src.Put("export/events/t-0000/F0001.rf", []byte("data"))
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 4, SHA256: "3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7"},
		},
	}

	tests := []struct {
		name    string
		bulkDir string
	}{
		{name: "empty", bulkDir: ""},
		{name: "whitespace padded", bulkDir: " /bulk/events-1 "},
		{name: "local root", bulkDir: "/"},
		{name: "url root", bulkDir: "hdfs://nn/"},
		{name: "uppercase authorityless hdfs root", bulkDir: "HDFS:/"},
		{name: "hdfs root alias via trailing parent dot segment", bulkDir: "hdfs://nn/tmp/.."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := memory.New()
			if _, err := StageBulkDir(context.Background(), src, manifest, dst, tt.bulkDir); !errors.Is(err, accumulo.ErrInvalidBulkDir) {
				t.Fatalf("StageBulkDir error = %v, want ErrInvalidBulkDir", err)
			}
			if got := dst.Keys(); len(got) != 0 {
				t.Fatalf("StageBulkDir wrote %v on invalid bulkDir, want no writes", got)
			}
		})
	}
}

func TestStageBulkDirRejectsUndeclaredTabletIndexBeforeCopying(t *testing.T) {
	src := memory.New()
	src.Put("export/events/t-0000/F0001.rf", []byte("data"))

	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 1, DestinationPath: "export/events/t-0000/F0001.rf", Size: 4, SHA256: "3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7"},
		},
	}

	dst := memory.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, src, manifest, dst, "/bulk/events-1"); err == nil {
		t.Fatal("StageBulkDir with undeclared tablet index = nil error, want error")
	}
	if got := dst.Keys(); len(got) != 0 {
		t.Fatalf("StageBulkDir wrote %v on undeclared tablet index, want no partial writes", got)
	}
}

func TestStageBulkDirRejectsSplitManifestBeforeCopying(t *testing.T) {
	src := memory.New()
	src.Put("events/t-0000/F0001.rf", []byte("a"))
	src.Put("events/t-0001/F0002.rf", []byte("b"))

	dst := memory.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, src, splitManifest(), dst, "/bulk/events-1"); err == nil {
		t.Fatal("StageBulkDir(split manifest) = nil error, want error")
	}
	if got := dst.Keys(); len(got) != 0 {
		t.Fatalf("StageBulkDir wrote %v on split-manifest rejection, want no partial writes", got)
	}
}

// TestStageBulkDirRejectsInPlaceBulkDirBeforeCopying uses the real local
// filesystem backend (not memory.Backend, which publishes writes
// atomically on Close and so cannot reproduce this hazard) to prove
// StageBulkDir refuses to stage into a bulkDir that aliases the source
// RFile's own path, and — the part that actually matters — that the
// source file on disk survives the rejected call untouched. Without the
// guard, storage.Copy would call local.Backend.Create(dstPath), which
// opens the aliased file with O_TRUNC and destroys it before the copy
// loop ever reads from the (separately opened) source handle.
func TestStageBulkDirRejectsInPlaceBulkDirBeforeCopying(t *testing.T) {
	tabletDir := filepath.Join(t.TempDir(), "export", "events", "t-0000")
	if err := os.MkdirAll(tabletDir, 0o755); err != nil {
		t.Fatal(err)
	}
	srcPath := filepath.Join(tabletDir, "F0001.rf")
	content := []byte("original rfile bytes that must survive a rejected in-place stage")
	if err := os.WriteFile(srcPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)

	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: srcPath, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])},
		},
	}

	be := local.New()
	ctx := context.Background()
	// bulkDir is the export's own tablet directory: flattening F0001.rf
	// into it resolves to the exact same path the manifest exports from.
	if _, err := StageBulkDir(ctx, be, manifest, be, tabletDir); err == nil {
		t.Fatal("StageBulkDir with bulkDir aliasing the source tablet dir = nil error, want error")
	}

	got, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("source file missing after rejected in-place stage: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("source file corrupted by rejected in-place stage: got %d bytes, want %d bytes intact", len(got), len(content))
	}
}

func TestStageBulkDirRejectsInPlaceBulkDirThroughDiskCacheWrappedLocalSource(t *testing.T) {
	root := t.TempDir()
	tabletDir := filepath.Join(root, "export", "events", "t-0000")
	if err := os.MkdirAll(tabletDir, 0o755); err != nil {
		t.Fatal(err)
	}
	srcPath := filepath.Join(tabletDir, "F0001.rf")
	content := []byte("original rfile bytes that must survive a rejected in-place stage through diskcache")
	if err := os.WriteFile(srcPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: srcPath, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])},
		},
	}

	srcBackend, err := diskcache.New(local.New(), filepath.Join(root, "cache"), 1<<20)
	if err != nil {
		t.Fatalf("diskcache.New: %v", err)
	}
	if _, err := StageBulkDir(context.Background(), srcBackend, manifest, local.New(), tabletDir); err == nil {
		t.Fatal("StageBulkDir with diskcache-wrapped local source aliasing bulkDir = nil error, want error")
	}

	got, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("source file missing after rejected diskcache-wrapped stage: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("source file corrupted by rejected diskcache-wrapped stage: got %d bytes, want %d bytes intact", len(got), len(content))
	}
}

func TestStagePathsAlias(t *testing.T) {
	tests := []struct {
		name string
		src  string
		dst  string
		want bool
	}{
		{name: "identical local paths", src: `C:\data\t-0000\F0001.rf`, dst: `C:\data\t-0000\F0001.rf`, want: true},
		{name: "windows drive path with redundant url-like separators", src: `C://data/F0001.rf`, dst: `C:\data\F0001.rf`, want: true},
		{name: "local paths differing only by separator style", src: `/data/t-0000/F0001.rf`, dst: `/data/t-0000//F0001.rf`, want: true},
		{name: "local paths differing only by unicode normalization", src: `/bulk/events-1/é.rf`, dst: "/bulk/events-1/e\u0301.rf", want: true},
		{name: "distinct local paths", src: `/data/t-0000/F0001.rf`, dst: `/bulk/events-1/F0001.rf`, want: false},
		{name: "identical relative paths", src: "export/events/t-0000/F0001.rf", dst: "export/events/t-0000/F0001.rf", want: true},
		{name: "local paths differing only by case", src: `/bulk/events-1/A.rf`, dst: `/bulk/events-1/a.rf`, want: true},
		{name: "local paths differing only by unicode normalization form", src: "/bulk/events-1/caf\u00e9.rf", dst: "/bulk/events-1/cafe\u0301.rf", want: true},
		{name: "identical url paths", src: "hdfs://nn/export/t-0000/F0001.rf", dst: "hdfs://nn/export/t-0000/F0001.rf", want: true},
		{name: "url paths differing only by trailing slash", src: "hdfs://nn/export/t-0000/F0001.rf", dst: "hdfs://nn/export/t-0000/F0001.rf/", want: true},
		{name: "distinct url paths", src: "hdfs://nn/export/t-0000/F0001.rf", dst: "hdfs://nn/bulk/events-1/F0001.rf", want: false},
		{name: "identical custom url paths", src: "custom+backend://bucket/path/F0001.rf", dst: "custom+backend://bucket/path/F0001.rf", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stagePathsAlias(tt.src, tt.dst); got != tt.want {
				t.Fatalf("stagePathsAlias(%q, %q) = %v, want %v", tt.src, tt.dst, got, tt.want)
			}
		})
	}
}

func TestPathUsesBackendSeparatorJoin(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "s3 qualified path", path: "s3://bucket/F0001.rf", want: true},
		{name: "gcs qualified path", path: "gs://bucket/F0001.rf", want: true},
		{name: "azure qualified path", path: "az://container/F0001.rf", want: true},
		{name: "custom scheme qualified path", path: "custom+backend://bucket/F0001.rf", want: true},
		{name: "hdfs qualified path", path: "hdfs://nn/tables/F0001.rf", want: true},
		{name: "hdfs authorityless path", path: "hdfs:/tables/F0001.rf", want: true},
		{name: "uppercase hdfs authorityless path", path: "HDFS:/tables/F0001.rf", want: true},
		{name: "opaque hdfs URI is not a joined backend path", path: "hdfs:tables/F0001.rf", want: false},
		{name: "windows drive path is local", path: `C://data/F0001.rf`, want: false},
		{name: "plain local path is local", path: `C:\data\F0001.rf`, want: false},
		{name: "generic non-builtin backend scheme", path: "memory://bucket/F0001.rf", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pathUsesBackendSeparatorJoin(tt.path); got != tt.want {
				t.Fatalf("pathUsesBackendSeparatorJoin(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestJoinBulkPathTreatsWindowsDrivePathAsLocal(t *testing.T) {
	got := joinBulkPath(local.New(), `C://data`, "F0001.rf")
	if got == `C://data/F0001.rf` {
		t.Fatalf("joinBulkPath treated C://data as a backend URL root: got %q", got)
	}
	if !localPathsLexicallyAlias(got, `C:\data\F0001.rf`) {
		t.Fatalf("joinBulkPath(%q, %q) = %q, want a local Windows-drive path aliasing %q", `C://data`, "F0001.rf", got, `C:\data\F0001.rf`)
	}
}

func TestJoinBulkPathTreatsAuthoritylessHDFSRootCaseInsensitively(t *testing.T) {
	got := joinBulkPath(memory.New(), "HDFS:/bulk", "F0001.rf")
	if got != "HDFS:/bulk/F0001.rf" {
		t.Fatalf("joinBulkPath(HDFS:/bulk) = %q, want %q", got, "HDFS:/bulk/F0001.rf")
	}
}

// TestJoinBulkPathUsesSeparatorJoinForGenericBackendScheme proves
// joinBulkPath joins with a literal "/" for a bulkDir using a backend
// URI scheme it does not know how to canonicalize (contrast the four
// built-in schemes s3/gs/az/hdfs), matching engine's own generic
// joinBackendPath. A filepath.Join fallback here would collapse the
// scheme's "//" (and use OS-native separators), producing a path the
// backend would not recognize as its own root.
func TestJoinBulkPathUsesSeparatorJoinForGenericBackendScheme(t *testing.T) {
	got := joinBulkPath(memory.New(), "memory://bucket", "F0001.rf")
	want := "memory://bucket/F0001.rf"
	if got != want {
		t.Fatalf("joinBulkPath(%q, %q) = %q, want %q", "memory://bucket", "F0001.rf", got, want)
	}
}

func TestJoinBulkPathPreservesCustomSchemeRoot(t *testing.T) {
	got := joinBulkPath(memory.New(), "custom+backend://bucket/prefix", "F0001.rf")
	if got != "custom+backend://bucket/prefix/F0001.rf" {
		t.Fatalf("joinBulkPath(custom scheme) = %q, want %q", got, "custom+backend://bucket/prefix/F0001.rf")
	}
}

func TestPathUsesBackendSeparatorJoinAcceptsDeclaredSingleCharacterScheme(t *testing.T) {
	backend := schemeAwareBackend{Backend: memory.New(), schemes: []string{"x"}}
	if !pathUsesBackendSeparatorJoinOnBackend(backend, "x://bucket/F0001.rf") {
		t.Fatalf("pathUsesBackendSeparatorJoinOnBackend(x scheme backend, x://bucket/F0001.rf) = false, want true")
	}
	if pathUsesBackendSeparatorJoinOnBackend(local.New(), "x://bucket/F0001.rf") {
		t.Fatalf("pathUsesBackendSeparatorJoinOnBackend(local backend, x://bucket/F0001.rf) = true, want false")
	}
}

func TestLocalBackendPathsOverrideBackendSchemeHeuristics(t *testing.T) {
	tests := []struct {
		name string
		be   shstorage.Backend
	}{
		{name: "local backend", be: local.New()},
		{
			name: "diskcache wrapped local backend",
			be: func() shstorage.Backend {
				backend, err := diskcache.New(local.New(), filepath.Join(t.TempDir(), "cache"), 1<<20)
				if err != nil {
					t.Fatalf("diskcache.New: %v", err)
				}
				return backend
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref := stagePathRef{backend: tt.be, path: "hdfs:/bulk/F0001.rf"}
			if !usesLocalFilesystemSemantics(ref) {
				t.Fatalf("usesLocalFilesystemSemantics(%s) = false, want true for a known local backend", ref.path)
			}
			if scheme := explicitBackendSchemeOnBackend(tt.be, ref.path); scheme != "" {
				t.Fatalf("explicitBackendSchemeOnBackend(%s) = %q, want empty for a known local backend", ref.path, scheme)
			}
			if _, ok := canonicalBackendPath(ref); ok {
				t.Fatalf("canonicalBackendPath(%s) unexpectedly treated a known local backend path as remote", ref.path)
			}
			if pathUsesBackendSeparatorJoinOnBackend(tt.be, "hdfs:/bulk") {
				t.Fatalf("pathUsesBackendSeparatorJoinOnBackend(hdfs:/bulk) = true, want false for a known local backend")
			}
		})
	}
}

func TestJoinBulkPathPreservesDeclaredSingleCharacterSchemeRoot(t *testing.T) {
	backend := schemeAwareBackend{Backend: memory.New(), schemes: []string{"x"}}
	got := joinBulkPath(backend, "x://bucket/prefix", "F0001.rf")
	if got != "x://bucket/prefix/F0001.rf" {
		t.Fatalf("joinBulkPath(single-char scheme) = %q, want %q", got, "x://bucket/prefix/F0001.rf")
	}
}

func TestIsBackendRootDistinguishesWindowsDrivePaths(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "backend root", path: "s3://bucket/", want: true},
		{name: "backend non-root", path: "s3://bucket/path", want: false},
		{name: "custom backend root", path: "custom+backend://bucket/", want: true},
		{name: "custom backend non-root", path: "custom+backend://bucket/path", want: false},
		{name: "hdfs root", path: "hdfs:/", want: true},
		{name: "uppercase hdfs root", path: "HDFS:/", want: true},
		{name: "hdfs bare authority root has no path segment", path: "hdfs://nn", want: true},
		{name: "hdfs root alias via trailing parent dot segment", path: "hdfs://nn/tmp/..", want: true},
		{name: "hdfs authorityless root alias via trailing parent dot segment", path: "hdfs:/tmp/..", want: true},
		{name: "hdfs non-root path containing a parent dot segment", path: "hdfs://nn/tmp/../keep", want: false},
		{name: "s3 dot segments are literal key characters, not a root alias", path: "s3://bucket/tmp/..", want: false},
		{name: "windows drive root with redundant separators", path: `C://`, want: true},
		{name: "windows drive non-root with redundant separators", path: `C://data`, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBackendRoot(tt.path); got != tt.want {
				t.Fatalf("isBackendRoot(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestIsBackendRootAcceptsDeclaredSingleCharacterScheme(t *testing.T) {
	backend := schemeAwareBackend{Backend: memory.New(), schemes: []string{"x"}}
	if !isBackendRootOnBackend(backend, "x://bucket/") {
		t.Fatalf("isBackendRootOnBackend(x scheme backend, x://bucket/) = false, want true")
	}
	if isBackendRootOnBackend(local.New(), "x://bucket/") {
		t.Fatalf("isBackendRootOnBackend(local backend, x://bucket/) = true, want false")
	}
}

func TestStagePathsAliasBackendCanonicalization(t *testing.T) {
	hdfsBackend := newTestHDFSBackend(t, "hdfs://nn:8020")

	tests := []struct {
		name       string
		srcBackend shstorage.Backend
		srcPath    string
		dstBackend shstorage.Backend
		dstPath    string
		want       bool
	}{
		{
			name:       "s3 scheme-less aliases qualified path",
			srcBackend: &s3.Backend{},
			srcPath:    "bucket/path/F0001.rf",
			dstBackend: &s3.Backend{},
			dstPath:    "s3://bucket/path/F0001.rf",
			want:       true,
		},
		{
			name:       "gcs scheme-less aliases qualified path",
			srcBackend: &gcs.Backend{},
			srcPath:    "bucket/path/F0001.rf",
			dstBackend: &gcs.Backend{},
			dstPath:    "gs://bucket/path/F0001.rf",
			want:       true,
		},
		{
			name:       "azure scheme-less aliases qualified path",
			srcBackend: &azure.Backend{},
			srcPath:    "container/path/F0001.rf",
			dstBackend: &azure.Backend{},
			dstPath:    "az://container/path/F0001.rf",
			want:       true,
		},
		{
			name:       "hdfs authorityless aliases qualified path on same backend",
			srcBackend: hdfsBackend,
			srcPath:    "/tables/1.rf",
			dstBackend: hdfsBackend,
			dstPath:    "hdfs://nn:8020/tables/1.rf",
			want:       true,
		},
		{
			name:       "uppercase hdfs authorityless aliases qualified path on same backend",
			srcBackend: hdfsBackend,
			srcPath:    "HDFS:/tables/1.rf",
			dstBackend: hdfsBackend,
			dstPath:    "hdfs://nn:8020/tables/1.rf",
			want:       true,
		},
		{
			name:       "local path does not alias s3 object spelling",
			srcBackend: local.New(),
			srcPath:    "bucket/path/F0001.rf",
			dstBackend: &s3.Backend{},
			dstPath:    "s3://bucket/path/F0001.rf",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stagePathsAliasOnBackends(tt.srcBackend, tt.srcPath, tt.dstBackend, tt.dstPath); got != tt.want {
				t.Fatalf(
					"stagePathsAliasOnBackends(%T, %q, %T, %q) = %v, want %v",
					tt.srcBackend, tt.srcPath, tt.dstBackend, tt.dstPath, got, tt.want,
				)
			}
		})
	}
}

// TestStagePathsAliasPhysicalIdentity covers the cases a purely lexical
// path comparison misses: an absolute path and an equivalent relative
// path (resolved against the process's working directory, exactly as
// os.Open/os.OpenFile would) naming the same file, and a symlink naming
// the same file as its target. Both must still be detected as aliasing
// so StageBulkDir doesn't truncate the source through a path form the
// string comparison alone doesn't recognize.
func TestStagePathsAliasPhysicalIdentity(t *testing.T) {
	t.Run("absolute path aliases equivalent relative path", func(t *testing.T) {
		dir := t.TempDir()
		absPath := filepath.Join(dir, "F0001.rf")
		if err := os.WriteFile(absPath, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Chdir(dir)
		relPath := "F0001.rf"

		if !stagePathsAlias(absPath, relPath) {
			t.Fatalf("stagePathsAlias(%q, %q) = false, want true (same physical file via absolute vs. relative path)", absPath, relPath)
		}
	})

	t.Run("symlink aliases its target", func(t *testing.T) {
		dir := t.TempDir()
		realPath := filepath.Join(dir, "F0001.rf")
		if err := os.WriteFile(realPath, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		linkPath := filepath.Join(dir, "F0001-link.rf")
		if err := os.Symlink(realPath, linkPath); err != nil {
			t.Skipf("symlink not supported in this environment: %v", err)
		}

		if !stagePathsAlias(realPath, linkPath) {
			t.Fatalf("stagePathsAlias(%q, %q) = false, want true (same physical file via symlink)", realPath, linkPath)
		}
	})

	t.Run("distinct files are not aliased", func(t *testing.T) {
		dir := t.TempDir()
		aPath := filepath.Join(dir, "a.rf")
		bPath := filepath.Join(dir, "b.rf")
		if err := os.WriteFile(aPath, []byte("a"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(bPath, []byte("b"), 0o644); err != nil {
			t.Fatal(err)
		}

		if stagePathsAlias(aPath, bPath) {
			t.Fatalf("stagePathsAlias(%q, %q) = true, want false (distinct files that happen to both exist)", aPath, bPath)
		}
	})

	t.Run("nonexistent destination is not aliased", func(t *testing.T) {
		dir := t.TempDir()
		srcPath := filepath.Join(dir, "F0001.rf")
		if err := os.WriteFile(srcPath, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		dstPath := filepath.Join(dir, "bulk", "F0001.rf")

		if stagePathsAlias(srcPath, dstPath) {
			t.Fatalf("stagePathsAlias(%q, %q) = true, want false (destination doesn't exist yet)", srcPath, dstPath)
		}
	})

	t.Run("symlink resolves through a symlinked parent directory to a not-yet-existing file", func(t *testing.T) {
		dir := t.TempDir()
		aliasDir := filepath.Join(dir, "alias")
		if err := os.Symlink(".", aliasDir); err != nil {
			t.Skipf("symlink not supported in this environment: %v", err)
		}
		linkPath := filepath.Join(dir, "A.rf")
		if err := os.Symlink(filepath.Join("alias", "B.rf"), linkPath); err != nil {
			t.Skipf("symlink not supported in this environment: %v", err)
		}
		bPath := filepath.Join(dir, "B.rf")
		// bPath does not exist yet: linkPath's target chain (A.rf ->
		// alias/B.rf, alias -> ".") only physically reaches bPath once
		// the symlinked "alias" parent directory component is followed
		// too, not just linkPath's own, single-hop symlink target.

		if !stagePathsAlias(linkPath, bPath) {
			t.Fatalf("stagePathsAlias(%q, %q) = false, want true (same not-yet-existing physical location via a symlinked parent directory)", linkPath, bPath)
		}
	})
}

func TestStagePathsAliasResolvesChainedParentSymlinksForNonexistentTargets(t *testing.T) {
	root := t.TempDir()
	realBulk := filepath.Join(root, "real-bulk")
	if err := os.MkdirAll(realBulk, 0o755); err != nil {
		t.Fatal(err)
	}

	secondHop := filepath.Join(root, "second-hop")
	if err := os.Symlink(realBulk, secondHop); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}
	firstHop := filepath.Join(root, "first-hop")
	if err := os.Symlink(secondHop, firstHop); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}

	left := filepath.Join(firstHop, "nested", "F0001.rf")
	right := filepath.Join(realBulk, "nested", "F0001.rf")
	if !stagePathsAlias(left, right) {
		t.Fatalf("stagePathsAlias(%q, %q) = false, want true after resolving chained parent symlinks for nonexistent targets", left, right)
	}
}

func TestStagePathsAliasResolvesDanglingRelativeSymlinkChains(t *testing.T) {
	root := t.TempDir()
	targetPath := filepath.Join(root, "target.rf")
	link2 := filepath.Join(root, "link2.rf")
	if err := os.Symlink("target.rf", link2); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}
	link1 := filepath.Join(root, "link1.rf")
	if err := os.Symlink("link2.rf", link1); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}

	if !stagePathsAlias(link1, targetPath) {
		t.Fatalf("stagePathsAlias(%q, %q) = false, want true after resolving a dangling relative symlink chain lexically", link1, targetPath)
	}
}

func TestStagePathsAliasResolvesDanglingTargetParentSymlinkComponents(t *testing.T) {
	root := t.TempDir()
	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(".", filepath.Join(bulkDir, "dirlink")); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}
	aPath := filepath.Join(bulkDir, "A.rf")
	if err := os.Symlink(filepath.Join("dirlink", "B.rf"), aPath); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}
	bPath := filepath.Join(bulkDir, "B.rf")

	if !stagePathsAlias(aPath, bPath) {
		t.Fatalf("stagePathsAlias(%q, %q) = false, want true after resolving parent symlink components in a dangling final target", aPath, bPath)
	}
}

// TestStageBulkDirRejectsInPlaceBulkDirViaRelativePath proves the alias
// guard still catches an aliased bulkDir when it's expressed as a path
// string that is lexically different from the manifest's absolute
// DestinationPath — here, bulkDir is passed as a path relative to the
// process's working directory that happens to resolve to the same
// physical tablet directory the RFile was exported to. A purely lexical
// comparison (the pre-fix behavior) would miss this and let
// storage.Copy truncate the source in place.
func TestStageBulkDirRejectsInPlaceBulkDirViaRelativePath(t *testing.T) {
	root := t.TempDir()
	tabletDir := filepath.Join(root, "export", "events", "t-0000")
	if err := os.MkdirAll(tabletDir, 0o755); err != nil {
		t.Fatal(err)
	}
	srcPath := filepath.Join(tabletDir, "F0001.rf")
	content := []byte("original rfile bytes that must survive a rejected in-place stage")
	if err := os.WriteFile(srcPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)

	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: srcPath, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])},
		},
	}

	// The manifest's DestinationPath is absolute; pass bulkDir as an
	// equivalent path relative to root instead, to prove the guard
	// doesn't depend on both sides sharing the same path form.
	t.Chdir(root)
	relBulkDir := filepath.Join("export", "events", "t-0000")

	be := local.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, be, manifest, be, relBulkDir); err == nil {
		t.Fatal("StageBulkDir with relative bulkDir aliasing the absolute source tablet dir = nil error, want error")
	}

	got, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("source file missing after rejected in-place stage: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("source file corrupted by rejected in-place stage: got %d bytes, want %d bytes intact", len(got), len(content))
	}
}

func TestStageBulkDirRejectsUnixRelativeCDriveHardlinkAliasBeforeCopying(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("C:/... is a Windows drive path on Windows, not a Unix-relative local path")
	}

	root := t.TempDir()
	t.Chdir(root)

	exportDir := filepath.Join(root, "C:", "export")
	bulkDirPath := filepath.Join(root, "C:", "bulk")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(bulkDirPath, 0o755); err != nil {
		t.Fatal(err)
	}

	srcPath := filepath.Join("C:", "export", "F0001.rf")
	content := []byte("original rfile bytes that must survive a rejected unix-relative C-drive hardlink alias stage")
	if err := os.WriteFile(srcPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	aliasPath := filepath.Join("C:", "bulk", "F0001.rf")
	if err := os.Link(srcPath, aliasPath); err != nil {
		t.Skipf("hard links not supported in this environment: %v", err)
	}

	manifest := localManifestFromFiles(t, srcPath)
	be := local.New()
	if _, err := StageBulkDir(context.Background(), be, manifest, be, filepath.Join("C:", "bulk")); err == nil {
		t.Fatal("StageBulkDir with unix-relative C:/bulk/F0001.rf hard-linked to source C:/export/F0001.rf = nil error, want error")
	}

	got, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("source file missing after rejected unix-relative C-drive stage: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("source file corrupted by rejected unix-relative C-drive stage: got %d bytes, want %d bytes intact", len(got), len(content))
	}
}

func TestStageBulkDirRejectsHDFSSpelledLocalBulkDirAliasBeforeCopying(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("paths containing ':' are not valid local relative paths on Windows")
	}

	root := t.TempDir()
	bulkDir := filepath.Join("hdfs:", "bulk")
	tabletDir := filepath.Join(root, bulkDir)
	if err := os.MkdirAll(tabletDir, 0o755); err != nil {
		t.Fatal(err)
	}
	srcPath := filepath.Join(tabletDir, "F0001.rf")
	content := []byte("original rfile bytes that must survive a rejected hdfs:-spelled local in-place stage")
	if err := os.WriteFile(srcPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: srcPath, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])},
		},
	}

	tests := []struct {
		name string
		src  func(t *testing.T) shstorage.Backend
	}{
		{name: "local source", src: func(t *testing.T) shstorage.Backend { return local.New() }},
		{
			name: "diskcache wrapped local source",
			src: func(t *testing.T) shstorage.Backend {
				backend, err := diskcache.New(local.New(), filepath.Join(root, "cache"), 1<<20)
				if err != nil {
					t.Fatalf("diskcache.New: %v", err)
				}
				return backend
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Chdir(root)
			be := local.New()
			if _, err := StageBulkDir(context.Background(), tt.src(t), manifest, be, bulkDir); err == nil {
				t.Fatal("StageBulkDir with bulkDir hdfs:/bulk aliasing the local source tablet dir = nil error, want error")
			}
			got, err := os.ReadFile(srcPath)
			if err != nil {
				t.Fatalf("source file missing after rejected hdfs:-spelled local stage: %v", err)
			}
			if string(got) != string(content) {
				t.Fatalf("source file corrupted by rejected hdfs:-spelled local stage: got %d bytes, want %d bytes intact", len(got), len(content))
			}
		})
	}
}

// TestStageBulkDirRejectsCrossFileAliasBeforeCopying proves the preflight
// check catches a destination that aliases a *different* manifest
// entry's source file, not just its own. Here bulkDir/A.rf is a symlink
// to B's source file: without checking every destination against every
// unique source, StageBulkDir would only compare A's destination to A's
// own source (finding no alias), start copying, and truncate B's source
// through the symlink before B is ever staged or re-verified.
func TestStageBulkDirRejectsCrossFileAliasBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	aPath := filepath.Join(exportDir, "A.rf")
	bPath := filepath.Join(exportDir, "B.rf")
	aContent := []byte("A file bytes")
	bContent := []byte("B file bytes that must survive a rejected stage")
	if err := os.WriteFile(aPath, aContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bPath, bContent, 0o644); err != nil {
		t.Fatal(err)
	}

	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A's computed destination (bulkDir/A.rf, since flattenNames uses the
	// basename) is a symlink to B's source -- a different RFile entirely.
	aDstPath := filepath.Join(bulkDir, "A.rf")
	if err := os.Symlink(bPath, aDstPath); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}

	aSum := sha256.Sum256(aContent)
	bSum := sha256.Sum256(bContent)
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: aPath, Size: int64(len(aContent)), SHA256: hex.EncodeToString(aSum[:])},
			{TabletIndex: 0, DestinationPath: bPath, Size: int64(len(bContent)), SHA256: hex.EncodeToString(bSum[:])},
		},
	}

	be := local.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with a destination aliasing a different RFile's source = nil error, want error")
	}

	got, err := os.ReadFile(bPath)
	if err != nil {
		t.Fatalf("source file B missing after rejected stage: %v", err)
	}
	if string(got) != string(bContent) {
		t.Fatalf("source file B corrupted by rejected stage: got %q, want %q", got, bContent)
	}
}

func TestStageBulkDirRejectsSymlinkedWriteTargetsBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	aPath := filepath.Join(exportDir, "A.rf")
	bPath := filepath.Join(exportDir, "B.rf")
	aContent := []byte("A source bytes that must survive a rejected stage")
	bContent := []byte("B source bytes that must survive a rejected stage")
	if err := os.WriteFile(aPath, aContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bPath, bContent, 0o644); err != nil {
		t.Fatal(err)
	}

	manifest := localManifestFromFiles(t, aPath, bPath)
	aDstPath := filepath.Join(bulkDir, "A.rf")
	bDstPath := filepath.Join(bulkDir, "B.rf")
	if err := os.Symlink(bDstPath, aDstPath); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}

	be := local.New()
	if _, err := StageBulkDir(context.Background(), be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with a symlinked write target = nil error, want error")
	}

	gotA, err := os.ReadFile(aPath)
	if err != nil {
		t.Fatalf("source file A missing after rejected stage: %v", err)
	}
	if string(gotA) != string(aContent) {
		t.Fatalf("source file A corrupted by rejected stage: got %q, want %q", gotA, aContent)
	}
	gotB, err := os.ReadFile(bPath)
	if err != nil {
		t.Fatalf("source file B missing after rejected stage: %v", err)
	}
	if string(gotB) != string(bContent) {
		t.Fatalf("source file B corrupted by rejected stage: got %q, want %q", gotB, bContent)
	}
	if _, err := os.Stat(bDstPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink target unexpectedly exists after rejected stage: err=%v", err)
	}
}

// TestStageBulkDirRejectsWriteTargetAliasedThroughSymlinkedParentBeforeCopying
// proves the write-target alias guard catches a case one hop deeper
// than TestStageBulkDirRejectsSymlinkedWriteTargetsBeforeCopying: here
// bulkDir/A.rf's own symlink target, "alias/B.rf", is lexically
// different from bulkDir/B.rf and neither exists yet, so a naive
// single-hop symlink resolution sees no alias at all. Only resolving
// the symlinked "alias" *parent* directory component -- which points
// back at bulkDir itself -- reveals that bulkDir/A.rf and bulkDir/B.rf
// both reach the same not-yet-created physical location.
func TestStageBulkDirRejectsWriteTargetAliasedThroughSymlinkedParentBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	aPath := filepath.Join(exportDir, "A.rf")
	bPath := filepath.Join(exportDir, "B.rf")
	aContent := []byte("A source bytes that must survive a rejected stage")
	bContent := []byte("B source bytes that must survive a rejected stage")
	if err := os.WriteFile(aPath, aContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bPath, bContent, 0o644); err != nil {
		t.Fatal(err)
	}
	manifest := localManifestFromFiles(t, aPath, bPath)

	aliasDir := filepath.Join(bulkDir, "alias")
	if err := os.Symlink(".", aliasDir); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}
	aDstPath := filepath.Join(bulkDir, "A.rf")
	if err := os.Symlink(filepath.Join("alias", "B.rf"), aDstPath); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}
	bDstPath := filepath.Join(bulkDir, "B.rf")

	be := local.New()
	if _, err := StageBulkDir(context.Background(), be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with a write target aliased through a symlinked parent directory = nil error, want error")
	}

	gotA, err := os.ReadFile(aPath)
	if err != nil {
		t.Fatalf("source file A missing after rejected stage: %v", err)
	}
	if string(gotA) != string(aContent) {
		t.Fatalf("source file A corrupted by rejected stage: got %q, want %q", gotA, aContent)
	}
	gotB, err := os.ReadFile(bPath)
	if err != nil {
		t.Fatalf("source file B missing after rejected stage: %v", err)
	}
	if string(gotB) != string(bContent) {
		t.Fatalf("source file B corrupted by rejected stage: got %q, want %q", gotB, bContent)
	}
	if _, err := os.Stat(bDstPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("write target aliased through symlinked parent unexpectedly exists after rejected stage: err=%v", err)
	}
}

// TestStageBulkDirRejectsDanglingFinalSymlinkChainAliasBeforeCopying proves
// the write-target alias guard follows a multi-hop *direct* symlink chain
// (bulkDir/A.rf -> mid.rf -> B.rf), not just a single symlink hop, even when
// every link in the chain is dangling (B.rf does not exist yet, since it is
// itself one of the write targets StageBulkDir is about to create).
func TestStageBulkDirRejectsDanglingFinalSymlinkChainAliasBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	aPath := filepath.Join(exportDir, "A.rf")
	bPath := filepath.Join(exportDir, "B.rf")
	aContent := []byte("A source bytes")
	bContent := []byte("B source bytes")
	if err := os.WriteFile(aPath, aContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bPath, bContent, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink("B.rf", filepath.Join(bulkDir, "mid.rf")); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}
	if err := os.Symlink("mid.rf", filepath.Join(bulkDir, "A.rf")); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}

	manifest := localManifestFromFiles(t, aPath, bPath)
	be := local.New()
	if _, err := StageBulkDir(context.Background(), be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with A.rf symlinked through a dangling chain to B.rf = nil error, want error")
	}
	if _, err := os.Stat(filepath.Join(bulkDir, "B.rf")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dangling symlink target unexpectedly exists after rejected stage: err=%v", err)
	}
}

func TestStageBulkDirRejectsDanglingFinalSymlinkTargetWithParentAliasBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	aPath := filepath.Join(exportDir, "A.rf")
	bPath := filepath.Join(exportDir, "B.rf")
	aContent := []byte("A source bytes")
	bContent := []byte("B source bytes")
	if err := os.WriteFile(aPath, aContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bPath, bContent, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(".", filepath.Join(bulkDir, "dirlink")); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}
	if err := os.Symlink(filepath.Join("dirlink", "B.rf"), filepath.Join(bulkDir, "A.rf")); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}

	manifest := localManifestFromFiles(t, aPath, bPath)
	be := local.New()
	if _, err := StageBulkDir(context.Background(), be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with A.rf symlinked to dirlink/B.rf where dirlink aliases bulkDir = nil error, want error")
	}
	if _, err := os.Stat(filepath.Join(bulkDir, "B.rf")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink target unexpectedly exists after rejected stage: err=%v", err)
	}
}

func TestStageBulkDirRejectsLoadMapAliasBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	srcPath := filepath.Join(exportDir, "F0001.rf")
	content := []byte("source bytes that must survive a rejected loadmap alias stage")
	if err := os.WriteFile(srcPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	manifest := localManifestFromFiles(t, srcPath)

	loadmapPath := filepath.Join(bulkDir, "loadmap.json")
	if err := os.Link(srcPath, loadmapPath); err != nil {
		t.Skipf("hard links not supported in this environment: %v", err)
	}

	be := local.New()
	if _, err := StageBulkDir(context.Background(), be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with loadmap.json aliasing the source = nil error, want error")
	}

	got, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("source file missing after rejected loadmap alias stage: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("source file corrupted by rejected loadmap alias stage: got %q, want %q", got, content)
	}
	gotLoadmap, err := os.ReadFile(loadmapPath)
	if err != nil {
		t.Fatalf("loadmap alias missing after rejected stage: %v", err)
	}
	if string(gotLoadmap) != string(content) {
		t.Fatalf("loadmap alias content changed after rejected stage: got %q, want %q", gotLoadmap, content)
	}
	if _, err := os.Stat(filepath.Join(bulkDir, "F0001.rf")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staged file exists after rejected loadmap alias stage: err=%v, want not-exist", err)
	}
}

// TestPathIdentityCacheMemoizesStatResults proves pathIdentityCache
// actually memoizes: checkNoStagingAliases's O(N^2) all-pairs comparison
// only stays cheap in filesystem-syscall terms if repeated stat calls for
// the same path are served from cache rather than re-stat'ing. This is
// verified indirectly (mutating the filesystem between two stat calls
// for the same path and confirming the second call still reflects the
// first, cached, result) since the cache sits directly in front of the
// os package and cannot be swapped for a counting fake.
func TestPathIdentityCacheMemoizesStatResults(t *testing.T) {
	t.Run("a successful stat is served from cache after the file is removed", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "f.rf")
		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		cache := newPathIdentityCache(1)
		first := cache.stat(path)
		if first == nil {
			t.Fatalf("stat(%q) = nil, want a FileInfo for an existing file", path)
		}

		if err := os.Remove(path); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		if _, err := os.Stat(path); err == nil {
			t.Fatalf("path %q still exists after Remove; test setup invalid", path)
		}

		second := cache.stat(path)
		if second == nil {
			t.Fatalf("stat(%q) after removal = nil, want the cached pre-removal FileInfo (a fresh os.Stat would fail here)", path)
		}
		if second != first {
			t.Fatalf("stat(%q) after removal returned a different FileInfo than the first call; cache was not reused", path)
		}
	})

	t.Run("a failed stat is cached too, not retried", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "does-not-exist-yet.rf")

		cache := newPathIdentityCache(1)
		first := cache.stat(path)
		if first != nil {
			t.Fatalf("stat(%q) = %v, want nil for a nonexistent file", path, first)
		}

		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		second := cache.stat(path)
		if second != nil {
			t.Fatalf("stat(%q) after later creating the file = %v, want the cached nil (a fresh os.Stat would now succeed)", path, second)
		}
	})
}

// TestStageBulkDirRejectsLoadMappingAliasBeforeCopying proves the
// preflight also protects bulkDir's own loadmap.json write target, not
// just the flattened RFile destinations. Here bulkDir/loadmap.json
// (created before StageBulkDir runs) is a symlink to the sole RFile's
// source: without including the load-mapping path in the checked target
// set, StageBulkDir would successfully copy the RFile, then destroy its
// own just-copied source when WriteLoadMapping opens the symlinked
// loadmap.json path for writing (WriteLoadMapping truncates exactly like
// storage.Copy does, and it runs after every RFile copy, so this would
// corrupt a source the rest of the stage already treated as verified and
// safely staged).
func TestStageBulkDirRejectsLoadMappingAliasBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	srcPath := filepath.Join(exportDir, "F0001.rf")
	content := []byte("rfile bytes that must survive a rejected stage")
	if err := os.WriteFile(srcPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// bulkDir/loadmap.json is a symlink to the RFile's own source.
	loadMapPath := filepath.Join(bulkDir, "loadmap.json")
	if err := os.Symlink(srcPath, loadMapPath); err != nil {
		t.Skipf("symlink not supported in this environment: %v", err)
	}

	sum := sha256.Sum256(content)
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: srcPath, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])},
		},
	}

	be := local.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with loadmap.json aliasing the sole RFile's source = nil error, want error")
	}

	got, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("source file missing after rejected stage: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("source file corrupted by rejected stage: got %q, want %q", got, content)
	}
}

// TestStageBulkDirRejectsHardLinkedWriteTargetsBeforeCopying proves the
// preflight also catches two different flattened destinations that are
// hard-linked to each other, even though neither aliases any manifest
// source. Here bulkDir/A.rf and bulkDir/B.rf (created before StageBulkDir
// runs) are hard-linked to the same physical file: without a
// target-vs-target check, both individually pass the target-vs-source
// check (A's and B's sources are genuinely distinct files), so copying A
// then B would silently overwrite A's just-staged bytes with B's, while
// the load mapping still records both names as independent files with
// independent sizes.
func TestStageBulkDirRejectsHardLinkedWriteTargetsBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	aPath := filepath.Join(exportDir, "A.rf")
	bPath := filepath.Join(exportDir, "B.rf")
	aContent := []byte("A file bytes")
	bContent := []byte("B file bytes that must survive a rejected stage")
	if err := os.WriteFile(aPath, aContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bPath, bContent, 0o644); err != nil {
		t.Fatal(err)
	}

	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// bulkDir/A.rf and bulkDir/B.rf pre-exist, hard-linked to the same
	// physical file -- distinct names, one underlying file.
	aDstPath := filepath.Join(bulkDir, "A.rf")
	bDstPath := filepath.Join(bulkDir, "B.rf")
	if err := os.WriteFile(aDstPath, []byte("stale placeholder"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(aDstPath, bDstPath); err != nil {
		t.Skipf("hard links not supported in this environment: %v", err)
	}

	aSum := sha256.Sum256(aContent)
	bSum := sha256.Sum256(bContent)
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: aPath, Size: int64(len(aContent)), SHA256: hex.EncodeToString(aSum[:])},
			{TabletIndex: 0, DestinationPath: bPath, Size: int64(len(bContent)), SHA256: hex.EncodeToString(bSum[:])},
		},
	}

	be := local.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with two hard-linked write targets = nil error, want error")
	}

	gotA, err := os.ReadFile(aPath)
	if err != nil {
		t.Fatalf("source file A missing after rejected stage: %v", err)
	}
	if string(gotA) != string(aContent) {
		t.Fatalf("source file A corrupted by rejected stage: got %q, want %q", gotA, aContent)
	}
	gotB, err := os.ReadFile(bPath)
	if err != nil {
		t.Fatalf("source file B missing after rejected stage: %v", err)
	}
	if string(gotB) != string(bContent) {
		t.Fatalf("source file B corrupted by rejected stage: got %q, want %q", gotB, bContent)
	}
}

// TestStageBulkDirRejectsHardLinkedSourcesBeforeCopying proves the
// preflight also catches two manifest entries whose DestinationPath
// values are different strings but physically the same file (for
// example a hard link from one per-tablet export directory into
// another). Each entry individually passes VerifyRFileExport and
// flattens to a distinct basename (A.rf, B.rf), so neither the
// target-vs-source nor the target-vs-target comparison has any reason
// to reject them: without a source-vs-source comparison, StageBulkDir
// would "succeed" while copying the same physical file's bytes twice
// under two independent bulk-import filenames, so Accumulo would
// import every row in that file twice.
func TestStageBulkDirRejectsHardLinkedSourcesBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir0 := filepath.Join(root, "export", "t-0000")
	exportDir1 := filepath.Join(root, "export", "t-0001")
	if err := os.MkdirAll(exportDir0, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(exportDir1, 0o755); err != nil {
		t.Fatal(err)
	}

	aPath := filepath.Join(exportDir0, "A.rf")
	bPath := filepath.Join(exportDir1, "B.rf")
	content := []byte("shared physical file bytes")
	if err := os.WriteFile(aPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(aPath, bPath); err != nil {
		t.Skipf("hard links not supported in this environment: %v", err)
	}

	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Neither bulkDir/A.rf nor bulkDir/B.rf exists yet: this is not a
	// write-target alias (they flatten to distinct names) but a
	// source-vs-source alias -- the same physical file is referenced by
	// two different manifest entries.

	sum := sha256.Sum256(content)
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: aPath, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])},
			{TabletIndex: 0, DestinationPath: bPath, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])},
		},
	}

	be := local.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with two hard-linked manifest sources = nil error, want error")
	}

	entries, err := os.ReadDir(bulkDir)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", bulkDir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("bulkDir has %d entries after rejected stage, want 0 (preflight must run before any copy): %v", len(entries), entries)
	}
}

// TestStageBulkDirRejectsCaseInsensitiveAliasBeforeCopying proves the
// preflight also catches two flattened destinations that differ only by
// case, even though *neither exists yet* when the check runs. Windows
// and macOS (by default) resolve filesystem paths case-insensitively, so
// bulkDir/A.rf and bulkDir/a.rf name the same physical location there --
// but an os.Stat-based physical-identity check cannot detect that: right
// up until the second Create silently resolves onto the first, both
// paths correctly stat as "doesn't exist yet." Without a case-
// insensitive lexical check, StageBulkDir would copy the first source,
// then silently overwrite it when copying the second, while the load
// mapping still records both names (A.rf and a.rf) as independent files
// with independent sizes.
func TestStageBulkDirRejectsCaseInsensitiveAliasBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir0 := filepath.Join(root, "export", "t-0000")
	exportDir1 := filepath.Join(root, "export", "t-0001")
	if err := os.MkdirAll(exportDir0, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(exportDir1, 0o755); err != nil {
		t.Fatal(err)
	}
	upperPath := filepath.Join(exportDir0, "A.rf")
	lowerPath := filepath.Join(exportDir1, "a.rf")
	upperContent := []byte("upper file bytes that must survive a rejected stage")
	lowerContent := []byte("lower file bytes")
	if err := os.WriteFile(upperPath, upperContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lowerPath, lowerContent, 0o644); err != nil {
		t.Fatal(err)
	}

	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Neither bulkDir/A.rf nor bulkDir/a.rf exists yet -- the scenario an
	// os.Stat-based physical-identity check cannot catch, since there's
	// nothing to stat until after the (unsafe) first copy.

	upperSum := sha256.Sum256(upperContent)
	lowerSum := sha256.Sum256(lowerContent)
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: upperPath, Size: int64(len(upperContent)), SHA256: hex.EncodeToString(upperSum[:])},
			{TabletIndex: 0, DestinationPath: lowerPath, Size: int64(len(lowerContent)), SHA256: hex.EncodeToString(lowerSum[:])},
		},
	}

	be := local.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with two case-insensitively-aliased write targets = nil error, want error")
	}

	gotUpper, err := os.ReadFile(upperPath)
	if err != nil {
		t.Fatalf("source file A.rf missing after rejected stage: %v", err)
	}
	if string(gotUpper) != string(upperContent) {
		t.Fatalf("source file A.rf corrupted by rejected stage: got %q, want %q", gotUpper, upperContent)
	}
	gotLower, err := os.ReadFile(lowerPath)
	if err != nil {
		t.Fatalf("source file a.rf missing after rejected stage: %v", err)
	}
	if string(gotLower) != string(lowerContent) {
		t.Fatalf("source file a.rf corrupted by rejected stage: got %q, want %q", gotLower, lowerContent)
	}
}

func TestStageBulkDirRejectsUnicodeNormalizedWriteTargetsBeforeCopying(t *testing.T) {
	src, manifest := memoryManifestFromBlobs(map[string][]byte{
		"export/events/t-0000/é.rf":  []byte("composed"),
		"export/events/t-0000/é.rf": []byte("decomposed"),
	})

	bulkDir := filepath.Join(t.TempDir(), "bulk")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if _, err := StageBulkDir(context.Background(), src, manifest, local.New(), bulkDir); err == nil {
		t.Fatal("StageBulkDir with Unicode-normalized write-target aliases = nil error, want error")
	}
}

func localManifestFromFiles(t *testing.T, paths ...string) *engine.RFileExportManifest {
	t.Helper()

	rfiles := make([]engine.RFileExportFile, 0, len(paths))
	for _, srcPath := range paths {
		data, err := os.ReadFile(srcPath)
		if err != nil {
			t.Fatalf("ReadFile(%q): %v", srcPath, err)
		}
		sum := sha256.Sum256(data)
		rfiles = append(rfiles, engine.RFileExportFile{
			TabletIndex:     0,
			DestinationPath: srcPath,
			Size:            int64(len(data)),
			SHA256:          hex.EncodeToString(sum[:]),
		})
	}

	return &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles:      rfiles,
	}
}

func memoryManifestFromBlobs(blobs map[string][]byte) (*memory.Backend, *engine.RFileExportManifest) {
	src := memory.New()
	paths := make([]string, 0, len(blobs))
	for path := range blobs {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	rfiles := make([]engine.RFileExportFile, 0, len(paths))
	for _, path := range paths {
		data := blobs[path]
		src.Put(path, data)
		sum := sha256.Sum256(data)
		rfiles = append(rfiles, engine.RFileExportFile{
			TabletIndex:     0,
			DestinationPath: path,
			Size:            int64(len(data)),
			SHA256:          hex.EncodeToString(sum[:]),
		})
	}

	return src, &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles:      rfiles,
	}
}

func newTestHDFSBackend(t *testing.T, address string) *hdfs.Backend {
	t.Helper()

	backend, err := hdfs.New(address, hdfs.WithClient(noopHDFSClient{}))
	if err != nil {
		t.Fatalf("hdfs.New(%q): %v", address, err)
	}
	return backend
}

type noopHDFSClient struct{}

func (noopHDFSClient) Open(string) (hdfs.Reader, error)        { return nil, errors.New("unused") }
func (noopHDFSClient) Create(string) (shstorage.Writer, error) { return nil, errors.New("unused") }
func (noopHDFSClient) MkdirAll(string, os.FileMode) error      { return errors.New("unused") }
func (noopHDFSClient) ReadDir(string) ([]os.FileInfo, error)   { return nil, errors.New("unused") }
func (noopHDFSClient) Remove(string) error                     { return errors.New("unused") }
func (noopHDFSClient) Rename(string, string) error             { return errors.New("unused") }
func (noopHDFSClient) Close() error                            { return nil }
