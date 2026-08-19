package promotion

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/phrocker/shoal/accumulo"
	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage/memory"
)

// fakePromoter records AddTableSplits and BulkImport calls instead of
// talking to Accumulo, so Promote's orchestration can be tested without
// any of accumulo's own discovery/manager fakes.
type fakePromoter struct {
	calls   int
	table   string
	bulkDir string
	opts    accumulo.BulkImportOptions
	err     error

	splitCalls int
	splitTable string
	splitRows  [][]byte
	splitErr   error
}

func (f *fakePromoter) AddTableSplits(_ context.Context, tableName string, splits [][]byte) error {
	f.splitCalls++
	f.splitTable = tableName
	f.splitRows = splits
	return f.splitErr
}

func (f *fakePromoter) BulkImport(_ context.Context, tableName, bulkDir string, opts accumulo.BulkImportOptions) error {
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
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 4, SHA256: "3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7"},
		},
	}
	dst := memory.New()
	importer := &fakePromoter{}

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
	if _, err := dst.Open(context.Background(), "hdfs://nn/bulk/events-1/F0001.rf"); err != nil {
		t.Fatalf("expected staged file before BulkImport call: %v", err)
	}
}

func TestPromoteDoesNotSubmitWhenStagingFails(t *testing.T) {
	src := memory.New()
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 4},
		},
	}
	dst := memory.New()
	importer := &fakePromoter{}

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
			{TabletIndex: 0, DestinationPath: "export/events/t-0000/F0001.rf", Size: 4, SHA256: "3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7"},
		},
	}
	dst := memory.New()
	importer := &fakePromoter{err: accumulo.ErrTableNotFound}

	if _, err := Promote(context.Background(), src, manifest, dst, "/bulk/events-1", importer, "events", Options{}); err != accumulo.ErrTableNotFound {
		t.Fatalf("Promote error = %v, want %v", err, accumulo.ErrTableNotFound)
	}
	if importer.calls != 1 {
		t.Fatalf("BulkImport calls = %d, want 1", importer.calls)
	}
}

func TestPromoteSkipsSubmissionForEmptyMapping(t *testing.T) {
	src := memory.New()
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles:      nil,
	}
	dst := memory.New()
	importer := &fakePromoter{}

	mapping, err := Promote(context.Background(), src, manifest, dst, "hdfs://nn/bulk/events-1", importer, "events", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(mapping) != 0 {
		t.Fatalf("mapping = %#v, want empty", mapping)
	}
	if importer.calls != 0 {
		t.Fatalf("BulkImport calls = %d, want 0 (empty mapping must not submit FATE)", importer.calls)
	}
	if _, err := dst.Open(context.Background(), "hdfs://nn/bulk/events-1/loadmap.json"); err != nil {
		t.Fatalf("expected loadmap.json to be written even for an empty mapping: %v", err)
	}
}

func TestPromoteRejectsInvalidDestinationInputsBeforeStagingOrSubmitting(t *testing.T) {
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
		name      string
		tableName string
		bulkDir   string
		want      error
	}{
		{name: "empty table", tableName: "", bulkDir: "/bulk/events-1", want: accumulo.ErrInvalidTableName},
		{name: "whitespace padded table", tableName: " events ", bulkDir: "/bulk/events-1", want: accumulo.ErrInvalidTableName},
		{name: "empty bulk dir", tableName: "events", bulkDir: "", want: accumulo.ErrInvalidBulkDir},
		{name: "backend root bulk dir", tableName: "events", bulkDir: "/", want: accumulo.ErrInvalidBulkDir},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := memory.New()
			importer := &fakePromoter{}
			if _, err := Promote(context.Background(), src, manifest, dst, tt.bulkDir, importer, tt.tableName, Options{}); !errors.Is(err, tt.want) {
				t.Fatalf("Promote error = %v, want %v", err, tt.want)
			}
			if importer.calls != 0 {
				t.Fatalf("BulkImport calls = %d, want 0 (invalid destination input must fail before submission)", importer.calls)
			}
			if got := dst.Keys(); len(got) != 0 {
				t.Fatalf("dst.Keys() = %v, want no staged files when destination validation fails first", got)
			}
		})
	}
}

func TestPromoteRejectsMalformedManifestBeforeStagingOrSubmitting(t *testing.T) {
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
	importer := &fakePromoter{}
	if _, err := Promote(context.Background(), src, manifest, dst, "/bulk/events-1", importer, "events", Options{}); err == nil {
		t.Fatal("Promote with undeclared tablet index = nil error, want error")
	}
	if importer.calls != 0 {
		t.Fatalf("BulkImport calls = %d, want 0 (malformed manifest must fail before submission)", importer.calls)
	}
	if got := dst.Keys(); len(got) != 0 {
		t.Fatalf("dst.Keys() = %v, want no staged files when manifest validation fails first", got)
	}
}

func TestPromoteReconcilesSplitsThenStagesThenSubmitsForMultiTabletManifest(t *testing.T) {
	src := memory.New()
	src.Put("events/t-0000/F0001.rf", []byte("a"))
	src.Put("events/t-0001/F0002.rf", []byte("b"))
	manifest := twoTabletManifest()
	manifest.RFiles[0].DestinationPath = "events/t-0000/F0001.rf"
	manifest.RFiles[0].Size = 1
	manifest.RFiles[0].SHA256 = "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
	manifest.RFiles[1].DestinationPath = "events/t-0001/F0002.rf"
	manifest.RFiles[1].Size = 1
	manifest.RFiles[1].SHA256 = "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d"

	dst := memory.New()
	importer := &fakePromoter{}
	mapping, err := Promote(context.Background(), src, manifest, dst, "hdfs://nn/bulk/events-1", importer, "events", Options{})
	if err != nil {
		t.Fatalf("Promote(two-tablet manifest) = %v, want success", err)
	}
	if len(mapping) != 2 {
		t.Fatalf("mapping entries = %d, want 2", len(mapping))
	}
	if importer.splitCalls != 1 {
		t.Fatalf("AddTableSplits calls = %d, want 1", importer.splitCalls)
	}
	if importer.splitTable != "events" {
		t.Fatalf("AddTableSplits table = %q, want %q", importer.splitTable, "events")
	}
	wantSplits := [][]byte{[]byte("g")}
	if !reflect.DeepEqual(importer.splitRows, wantSplits) {
		t.Fatalf("AddTableSplits rows = %#v, want %#v", importer.splitRows, wantSplits)
	}
	if importer.calls != 1 {
		t.Fatalf("BulkImport calls = %d, want 1", importer.calls)
	}
	for _, name := range []string{"F0001.rf", "F0002.rf"} {
		if _, err := dst.Open(context.Background(), "hdfs://nn/bulk/events-1/"+name); err != nil {
			t.Fatalf("expected staged file %s: %v", name, err)
		}
	}
}

func TestPromoteSkipsAddTableSplitsForSingleTabletManifest(t *testing.T) {
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
	dst := memory.New()
	importer := &fakePromoter{}
	if _, err := Promote(context.Background(), src, manifest, dst, "/bulk/events-1", importer, "events", Options{}); err != nil {
		t.Fatal(err)
	}
	if importer.splitCalls != 0 {
		t.Fatalf("AddTableSplits calls = %d, want 0 for a single-tablet manifest", importer.splitCalls)
	}
}

func TestPromoteAbortsBeforeStagingWhenAddTableSplitsFails(t *testing.T) {
	src := memory.New()
	src.Put("events/t-0000/F0001.rf", []byte("a"))
	src.Put("events/t-0001/F0002.rf", []byte("b"))
	manifest := twoTabletManifest()
	manifest.RFiles[0].DestinationPath = "events/t-0000/F0001.rf"
	manifest.RFiles[1].DestinationPath = "events/t-0001/F0002.rf"

	dst := memory.New()
	importer := &fakePromoter{splitErr: accumulo.ErrTableOffline}
	if _, err := Promote(context.Background(), src, manifest, dst, "/bulk/events-1", importer, "events", Options{}); !errors.Is(err, accumulo.ErrTableOffline) {
		t.Fatalf("Promote error = %v, want %v", err, accumulo.ErrTableOffline)
	}
	if importer.splitCalls != 1 {
		t.Fatalf("AddTableSplits calls = %d, want 1", importer.splitCalls)
	}
	if importer.calls != 0 {
		t.Fatalf("BulkImport calls = %d, want 0 (AddTableSplits failure must abort before submission)", importer.calls)
	}
	if got := dst.Keys(); len(got) != 0 {
		t.Fatalf("dst.Keys() = %v, want no staged files when AddTableSplits fails before staging", got)
	}
}

func TestPromoteEndToEndThreeTabletSuccess(t *testing.T) {
	src := memory.New()
	manifest := threeTabletManifest()
	wantHashes := []string{
		"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb", // sha256("a")
		"3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d", // sha256("b")
		"2e7d2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6", // sha256("c")
	}
	for i := range manifest.RFiles {
		src.Put(manifest.RFiles[i].DestinationPath, []byte{byte('a' + i)})
		manifest.RFiles[i].Size = 1
		manifest.RFiles[i].SHA256 = wantHashes[i]
	}

	dst := memory.New()
	importer := &fakePromoter{}
	mapping, err := Promote(context.Background(), src, manifest, dst, "hdfs://nn/bulk/events-1", importer, "events", Options{})
	if err != nil {
		t.Fatalf("Promote(three-tablet manifest) = %v, want success", err)
	}
	if len(mapping) != 3 {
		t.Fatalf("mapping entries = %d, want 3", len(mapping))
	}
	wantSplits := [][]byte{[]byte("d"), []byte("m")}
	if !reflect.DeepEqual(importer.splitRows, wantSplits) {
		t.Fatalf("AddTableSplits rows = %#v, want %#v", importer.splitRows, wantSplits)
	}
	if importer.calls != 1 {
		t.Fatalf("BulkImport calls = %d, want 1", importer.calls)
	}
	for _, name := range []string{"F0001.rf", "F0002.rf", "F0003.rf"} {
		if _, err := dst.Open(context.Background(), "hdfs://nn/bulk/events-1/"+name); err != nil {
			t.Fatalf("expected staged file %s: %v", name, err)
		}
	}
}
