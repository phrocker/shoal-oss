package promotion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/phrocker/shoal/accumulo"
	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage/local"
	"github.com/phrocker/shoal/internal/storage/memory"
)

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

func TestStagePathsAlias(t *testing.T) {
	tests := []struct {
		name string
		src  string
		dst  string
		want bool
	}{
		{name: "identical local paths", src: `C:\data\t-0000\F0001.rf`, dst: `C:\data\t-0000\F0001.rf`, want: true},
		{name: "local paths differing only by separator style", src: `/data/t-0000/F0001.rf`, dst: `/data/t-0000//F0001.rf`, want: true},
		{name: "distinct local paths", src: `/data/t-0000/F0001.rf`, dst: `/bulk/events-1/F0001.rf`, want: false},
		{name: "identical relative paths", src: "export/events/t-0000/F0001.rf", dst: "export/events/t-0000/F0001.rf", want: true},
		{name: "identical url paths", src: "hdfs://nn/export/t-0000/F0001.rf", dst: "hdfs://nn/export/t-0000/F0001.rf", want: true},
		{name: "url paths differing only by trailing slash", src: "hdfs://nn/export/t-0000/F0001.rf", dst: "hdfs://nn/export/t-0000/F0001.rf/", want: true},
		{name: "distinct url paths", src: "hdfs://nn/export/t-0000/F0001.rf", dst: "hdfs://nn/bulk/events-1/F0001.rf", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stagePathsAlias(tt.src, tt.dst); got != tt.want {
				t.Fatalf("stagePathsAlias(%q, %q) = %v, want %v", tt.src, tt.dst, got, tt.want)
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
