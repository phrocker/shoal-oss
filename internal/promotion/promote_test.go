package promotion

import (
	"context"
	"testing"

	"github.com/phrocker/shoal/accumulo"
	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage/memory"
)

// fakeBulkImporter records BulkImport calls instead of talking to Accumulo,
// so Promote's orchestration can be tested without any of accumulo's own
// discovery/manager fakes.
type fakeBulkImporter struct {
	calls   int
	table   string
	bulkDir string
	opts    accumulo.BulkImportOptions
	err     error
}

func (f *fakeBulkImporter) BulkImport(_ context.Context, tableName, bulkDir string, opts accumulo.BulkImportOptions) error {
	f.calls++
	f.table = tableName
	f.bulkDir = bulkDir
	f.opts = opts
	return f.err
}

func TestPromoteStagesThenSubmitsBulkImport(t *testing.T) {
	src := memory.New()
	src.Put("export/events/t-0000/F0001.rf", []byte("data"))
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 4},
		},
	}
	dst := memory.New()
	importer := &fakeBulkImporter{}

	// URL-style bulk dir keeps the expected staged path below OS
	// independent (joinBulkPath always uses "/" for URL-style roots).
	mapping, err := Promote(context.Background(), src, manifest, dst, "hdfs://nn/bulk/events-1", importer, "events", Options{SetTime: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(mapping) != 1 {
		t.Fatalf("mapping entries = %d, want 1", len(mapping))
	}
	if importer.calls != 1 {
		t.Fatalf("BulkImport calls = %d, want 1", importer.calls)
	}
	if importer.table != "events" {
		t.Fatalf("BulkImport table = %q, want %q", importer.table, "events")
	}
	if importer.bulkDir != "hdfs://nn/bulk/events-1" {
		t.Fatalf("BulkImport bulkDir = %q, want %q", importer.bulkDir, "hdfs://nn/bulk/events-1")
	}
	if !importer.opts.SetTime {
		t.Fatal("BulkImport opts.SetTime = false, want true (propagated from Options)")
	}

	// Staging must have actually happened: the file should exist flattened
	// in the bulk dir regardless of what the fake importer did with it.
	if _, err := dst.Open(context.Background(), "hdfs://nn/bulk/events-1/F0001.rf"); err != nil {
		t.Fatalf("expected staged file before BulkImport call: %v", err)
	}
}

func TestPromoteDoesNotSubmitWhenStagingFails(t *testing.T) {
	src := memory.New() // empty: referenced RFile is missing, Open will fail
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 4},
		},
	}
	dst := memory.New()
	importer := &fakeBulkImporter{}

	if _, err := Promote(context.Background(), src, manifest, dst, "/bulk/events-1", importer, "events", Options{}); err == nil {
		t.Fatal("Promote with missing source RFile = nil error, want error")
	}
	if importer.calls != 0 {
		t.Fatalf("BulkImport calls = %d, want 0 (staging failed before submission)", importer.calls)
	}
}

func TestPromotePropagatesBulkImportError(t *testing.T) {
	src := memory.New()
	src.Put("export/events/t-0000/F0001.rf", []byte("data"))
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 4},
		},
	}
	dst := memory.New()
	importer := &fakeBulkImporter{err: accumulo.ErrTableNotFound}

	if _, err := Promote(context.Background(), src, manifest, dst, "/bulk/events-1", importer, "events", Options{}); err != accumulo.ErrTableNotFound {
		t.Fatalf("Promote error = %v, want %v", err, accumulo.ErrTableNotFound)
	}
	if importer.calls != 1 {
		t.Fatalf("BulkImport calls = %d, want 1", importer.calls)
	}
}
