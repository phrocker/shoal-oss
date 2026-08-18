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
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 13},
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0002.rf", Size: 13},
			{TabletIndex: 2, DestinationPath: "export/events/t-0002/F0003.rf", Size: 13},
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
