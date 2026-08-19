package promotion

import (
	"context"
	"errors"
	"reflect"
	"strings"
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

	listSplitsCalls    int
	listSplitsTable    string
	listSplitsOverride [][]byte
	listSplitsSet      bool
	listSplitsErr      error
}

func (f *fakePromoter) AddTableSplits(_ context.Context, tableName string, splits [][]byte) error {
	f.splitCalls++
	f.splitTable = tableName
	f.splitRows = splits
	return f.splitErr
}

// ListTableSplits defaults to echoing back whatever AddTableSplits was
// last asked to add, i.e. it simulates a destination that now has
// exactly the required splits and nothing else -- the happy path every
// existing test other than the ones exercising this check explicitly
// relies on. Tests exercising verifyNoUnexpectedDestinationSplits set
// listSplitsOverride to simulate a destination with extra, unrelated
// splits.
func (f *fakePromoter) ListTableSplits(_ context.Context, tableName string) ([][]byte, error) {
	f.listSplitsCalls++
	f.listSplitsTable = tableName
	if f.listSplitsErr != nil {
		return nil, f.listSplitsErr
	}
	if f.listSplitsSet {
		return f.listSplitsOverride, nil
	}
	return f.splitRows, nil
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

// TestPromoteRejectsUndeclaredTabletIndexBeforeAddTableSplitsForMultiTabletManifest
// proves the gap this package's preflight BuildLoadMapping call in
// Promote closes: a manifest whose tablet *chain* is entirely
// well-formed (so RequiredDestinationSplits succeeds and would
// otherwise let AddTableSplits proceed) but whose RFiles reference an
// undeclared tablet index is rejected before AddTableSplits is ever
// called, not merely before BulkImport. Unlike
// TestPromoteRejectsMalformedManifestBeforeStagingOrSubmitting's
// single-tablet manifest (which requires zero splits and so never
// exercises this ordering at all), this manifest is a valid two-tablet
// chain that does require AddTableSplits, making the ordering gap
// observable.
func TestPromoteRejectsUndeclaredTabletIndexBeforeAddTableSplitsForMultiTabletManifest(t *testing.T) {
	src := memory.New()
	src.Put("events/t-0000/F0001.rf", []byte("a"))
	manifest := twoTabletManifest()
	manifest.RFiles[0].DestinationPath = "events/t-0000/F0001.rf"
	manifest.RFiles[0].Size = 1
	manifest.RFiles[0].SHA256 = "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
	// Tablet index 2 is not declared anywhere in manifest.Tablets (only
	// 0 and 1 exist), even though the chain those two tablets form is
	// itself perfectly valid.
	manifest.RFiles[1].TabletIndex = 2
	manifest.RFiles[1].DestinationPath = "events/t-0002/F0002.rf"

	dst := memory.New()
	importer := &fakePromoter{}
	if _, err := Promote(context.Background(), src, manifest, dst, "/bulk/events-1", importer, "events", Options{}); err == nil {
		t.Fatal("Promote with an RFile referencing an undeclared tablet index = nil error, want error")
	}
	if importer.splitCalls != 0 {
		t.Fatalf("AddTableSplits calls = %d, want 0 (an RFile-level manifest error must fail before any split reconciliation)", importer.splitCalls)
	}
	if importer.calls != 0 {
		t.Fatalf("BulkImport calls = %d, want 0", importer.calls)
	}
	if got := dst.Keys(); len(got) != 0 {
		t.Fatalf("dst.Keys() = %v, want no staged files when manifest validation fails first", got)
	}
}

// TestPromoteRejectsDestinationPathDeclaredUnderConflictingTabletIndexes
// covers the other RFile-level error BuildLoadMapping's preflight now
// catches before AddTableSplits: the same DestinationPath appearing
// twice under two different TabletIndex values, which is ambiguous
// about which destination tablet the physical file actually belongs to.
func TestPromoteRejectsDestinationPathDeclaredUnderConflictingTabletIndexes(t *testing.T) {
	src := memory.New()
	src.Put("events/t-0000/F0001.rf", []byte("a"))
	manifest := twoTabletManifest()
	manifest.RFiles[0].DestinationPath = "events/t-0000/F0001.rf"
	manifest.RFiles[0].Size = 1
	manifest.RFiles[0].SHA256 = "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
	// The same DestinationPath as RFiles[0], but declared under tablet
	// index 1 instead of 0 -- an unresolvable conflict about which
	// tablet the file belongs to, not a legitimate duplicate.
	manifest.RFiles[1].TabletIndex = 1
	manifest.RFiles[1].DestinationPath = "events/t-0000/F0001.rf"

	dst := memory.New()
	importer := &fakePromoter{}
	if _, err := Promote(context.Background(), src, manifest, dst, "/bulk/events-1", importer, "events", Options{}); err == nil {
		t.Fatal("Promote with a DestinationPath declared under conflicting tablet indexes = nil error, want error")
	}
	if importer.splitCalls != 0 {
		t.Fatalf("AddTableSplits calls = %d, want 0 (an RFile-level manifest error must fail before any split reconciliation)", importer.splitCalls)
	}
	if importer.calls != 0 {
		t.Fatalf("BulkImport calls = %d, want 0", importer.calls)
	}
}

// TestPromoteRejectsUnsupportedManifestVersionBeforeAddTableSplits covers
// the third RFile-level check BuildLoadMapping's preflight now catches
// before AddTableSplits: a manifest whose Version does not match
// engine.RFileExportManifestVersion, even when the declared Tablets chain
// and every RFile are otherwise perfectly valid. Without this check,
// engine.VerifyRFileExport (called only later, from inside StageBulkDir)
// would have been the sole place a version mismatch was ever caught --
// after AddTableSplits had already reconciled the destination's splits.
func TestPromoteRejectsUnsupportedManifestVersionBeforeAddTableSplits(t *testing.T) {
	src := memory.New()
	src.Put("events/t-0000/F0001.rf", []byte("a"))
	manifest := twoTabletManifest()
	manifest.RFiles[0].DestinationPath = "events/t-0000/F0001.rf"
	manifest.RFiles[0].Size = 1
	manifest.RFiles[0].SHA256 = "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
	manifest.Version = engine.RFileExportManifestVersion + 1

	dst := memory.New()
	importer := &fakePromoter{}
	if _, err := Promote(context.Background(), src, manifest, dst, "/bulk/events-1", importer, "events", Options{}); err == nil {
		t.Fatal("Promote with an unsupported manifest version = nil error, want error")
	}
	if importer.splitCalls != 0 {
		t.Fatalf("AddTableSplits calls = %d, want 0 (an unsupported manifest version must fail before any split reconciliation)", importer.splitCalls)
	}
	if importer.calls != 0 {
		t.Fatalf("BulkImport calls = %d, want 0", importer.calls)
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
	if importer.listSplitsCalls != 1 {
		t.Fatalf("ListTableSplits calls = %d, want 1 (Promote must verify reconciled splits before staging)", importer.listSplitsCalls)
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
	if importer.listSplitsCalls != 0 {
		t.Fatalf("ListTableSplits calls = %d, want 0 for a single-tablet manifest (no splits required, nothing to verify)", importer.listSplitsCalls)
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

// populatedThreeTabletManifest returns threeTabletManifest with every
// RFile's size/hash filled in and staged into src, so a test can call
// Promote against it directly without repeating the fixture setup
// TestPromoteEndToEndThreeTabletSuccess already establishes.
func populatedThreeTabletManifest(src *memory.Backend) *engine.RFileExportManifest {
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
	return manifest
}

// TestPromoteRejectsExtraDestinationSplitBeforeLastRequiredRow is the
// regression test for the exact counter-example that broke
// BuildLoadMapping's widening rule (see docs/promotion.md §3.2/§5): a
// destination that already has an extra, unrelated split strictly
// before the manifest's required splits. threeTabletManifest requires
// splits at "d" and "m"; a pre-existing split at "c" (< "d") forces
// Accumulo's real PrepBulkImport.validateLoadMapping walk past the one
// tablet in the whole table whose real prevEndRow is nil after
// resolving the first entry, so the second entry's widened
// prevEndRow=nil can never re-match — a spurious "concurrent merge"
// rejection that has nothing to do with an actual concurrent merge.
// Promote must catch this itself and fail closed before ever staging or
// submitting anything.
func TestPromoteRejectsExtraDestinationSplitBeforeLastRequiredRow(t *testing.T) {
	src := memory.New()
	manifest := populatedThreeTabletManifest(src)
	dst := memory.New()
	importer := &fakePromoter{
		listSplitsSet:      true,
		listSplitsOverride: [][]byte{[]byte("c"), []byte("d"), []byte("m")},
	}
	_, err := Promote(context.Background(), src, manifest, dst, "hdfs://nn/bulk/events-1", importer, "events", Options{})
	if err == nil {
		t.Fatal("Promote with an unexpected extra destination split before the last required row = nil error, want error")
	}
	if !strings.Contains(err.Error(), `"c"`) {
		t.Fatalf("error %q does not name the offending split row", err.Error())
	}
	if importer.splitCalls != 1 {
		t.Fatalf("AddTableSplits calls = %d, want 1 (reconciliation still runs before verification)", importer.splitCalls)
	}
	if importer.listSplitsCalls != 1 {
		t.Fatalf("ListTableSplits calls = %d, want 1", importer.listSplitsCalls)
	}
	if importer.calls != 0 {
		t.Fatalf("BulkImport calls = %d, want 0 (must fail closed before submission)", importer.calls)
	}
	if got := dst.Keys(); len(got) != 0 {
		t.Fatalf("dst.Keys() = %v, want no staged files when the extra-split check fails before staging", got)
	}
}

// TestPromoteAllowsTrailingDestinationSplitAfterLastRequiredRow proves
// the check above is not over-conservative: a trailing extra split
// strictly after the last required row falls entirely inside the
// manifest's final, always-unbounded entry, which can legitimately
// absorb any number of additional real trailing tablets, so it must not
// be rejected.
func TestPromoteAllowsTrailingDestinationSplitAfterLastRequiredRow(t *testing.T) {
	src := memory.New()
	manifest := populatedThreeTabletManifest(src)
	dst := memory.New()
	importer := &fakePromoter{
		listSplitsSet:      true,
		listSplitsOverride: [][]byte{[]byte("d"), []byte("m"), []byte("z")},
	}
	mapping, err := Promote(context.Background(), src, manifest, dst, "hdfs://nn/bulk/events-1", importer, "events", Options{})
	if err != nil {
		t.Fatalf("Promote with a trailing extra split after the last required row = %v, want success", err)
	}
	if len(mapping) != 3 {
		t.Fatalf("mapping entries = %d, want 3", len(mapping))
	}
	if importer.calls != 1 {
		t.Fatalf("BulkImport calls = %d, want 1", importer.calls)
	}
}

// TestPromoteRejectsWhenDestinationIsMissingARequiredSplit exercises the
// defensive "too few" side of the same check: it should never happen in
// practice (a successful AddTableSplits is supposed to guarantee the
// required rows exist), but if a concurrent merge removes a
// just-reconciled split in the narrow window before ListTableSplits
// observes it, Promote must fail closed rather than proceed with a
// mapping that no longer matches the destination's real splits.
func TestPromoteRejectsWhenDestinationIsMissingARequiredSplit(t *testing.T) {
	src := memory.New()
	manifest := populatedThreeTabletManifest(src)
	dst := memory.New()
	importer := &fakePromoter{
		listSplitsSet:      true,
		listSplitsOverride: [][]byte{[]byte("d")},
	}
	_, err := Promote(context.Background(), src, manifest, dst, "hdfs://nn/bulk/events-1", importer, "events", Options{})
	if err == nil {
		t.Fatal("Promote with a destination missing a required split = nil error, want error")
	}
	if importer.calls != 0 {
		t.Fatalf("BulkImport calls = %d, want 0 (must fail closed before submission)", importer.calls)
	}
	if got := dst.Keys(); len(got) != 0 {
		t.Fatalf("dst.Keys() = %v, want no staged files when the extra-split check fails before staging", got)
	}
}
