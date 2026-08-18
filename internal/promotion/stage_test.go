package promotion

import (
	"context"
	"errors"
	"testing"

	"github.com/phrocker/shoal/accumulo"
	"github.com/phrocker/shoal/internal/engine"
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
