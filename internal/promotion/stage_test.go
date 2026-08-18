package promotion

import (
	"context"
	"testing"

	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage/memory"
)

func TestStageBulkDirFlattensCopiesAndWritesLoadMapping(t *testing.T) {
	src := memory.New()
	src.Put("export/events/t-0000/F0001.rf", []byte("tablet0-file1"))
	src.Put("export/events/t-0000/F0002.rf", []byte("tablet0-file2"))
	src.Put("export/events/t-0002/F0003.rf", []byte("tablet2-file1"))

	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets: []engine.RFileExportTablet{
			{Index: 0, EndRow: strPtr("g")},
			{Index: 1, StartRow: strPtr("g"), EndRow: strPtr("p")},
			{Index: 2, StartRow: strPtr("p")},
		},
		// SHA256/Size must match the Put content above exactly: StageBulkDir
		// now runs engine.VerifyRFileExport before copying anything.
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 13, SHA256: "e47fb24ed70774dd8af7d59bf58fc740e126716aac3474bd262eb17f3e395e43"},
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0002.rf", Size: 13, SHA256: "066219c3f0f1a1bfccf12ef7e4d6957d102b3bf2e047fca44ba178a92e526cf0"},
			{TabletIndex: 2, DestinationPath: "export/events/t-0002/F0003.rf", Size: 13, SHA256: "73595a5ac087b50583aaccc894558360559fa8406b808102cff9e4fe8816f466"},
		},
	}

	dst := memory.New()
	ctx := context.Background()
	// Use a URL-style bulk dir (as a real deployment would: an HDFS or
	// object-storage path) so the expected staged paths below are OS
	// independent - joinBulkPath always joins URL-style roots with a
	// literal "/", regardless of the host OS's path separator.
	mapping, err := StageBulkDir(ctx, src, manifest, dst, "hdfs://nn/bulk/events-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(mapping) != 2 {
		t.Fatalf("mapping entries = %d, want 2", len(mapping))
	}

	// Files must be flattened directly into the bulk dir, not nested.
	for _, want := range []string{
		"hdfs://nn/bulk/events-1/F0001.rf",
		"hdfs://nn/bulk/events-1/F0002.rf",
		"hdfs://nn/bulk/events-1/F0003.rf",
		"hdfs://nn/bulk/events-1/loadmap.json",
	} {
		f, err := dst.Open(ctx, want)
		if err != nil {
			t.Fatalf("expected staged path %s: %v", want, err)
		}
		f.Close()
	}

	// Content must match the source bytes exactly (a real copy, not a
	// zero-length placeholder).
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

	// Round-tripping the written load mapping must match what StageBulkDir
	// returned.
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
	src.Put("export/events/t-0000/F0001.rf", []byte("a"))
	src.Put("export/events/t-0002/F0001.rf", []byte("b")) // same basename, different tablet

	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets: []engine.RFileExportTablet{
			{Index: 0, EndRow: strPtr("g")},
			{Index: 2, StartRow: strPtr("p")},
		},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 1},
			{TabletIndex: 2, DestinationPath: "export/events/t-0002/F0001.rf", Size: 1},
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
			// Size/SHA256 deliberately don't match the real bytes above -
			// e.g. a stale manifest from before the export was re-run.
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
		// Same DestinationPath listed twice: flattenNames already allows
		// this (TestFlattenNamesAllowsSameDestinationPathTwice), and
		// BuildLoadMapping already dedupes the resulting FileEntry
		// (TestBuildLoadMappingDedupesRepeatedDestinationPath). StageBulkDir
		// must not copy the same source object twice either.
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
	// Only one staged copy of the file, not a numbered/renamed second copy.
	if got := dst.Keys(); len(got) != 2 { // F0001.rf + loadmap.json
		t.Fatalf("dst.Keys() = %v, want exactly 2 entries (one staged file + loadmap.json)", got)
	}
}
