package promotion

import (
	"context"
	"encoding/base64"
	"reflect"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage/memory"
)

func strPtr(s string) *string { return &s }

func singleTabletManifest() *engine.RFileExportManifest {
	return &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0001.rf", Size: 100},
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0002.rf", Size: 50},
		},
	}
}

func splitManifest() *engine.RFileExportManifest {
	return &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets: []engine.RFileExportTablet{
			{Index: 0, EndRow: strPtr("g")},
			{Index: 1, StartRow: strPtr("g")},
		},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0001.rf", Size: 100},
			{TabletIndex: 1, DestinationPath: "events/t-0001/F0002.rf", Size: 50},
		},
	}
}

func TestBuildLoadMappingSingleTabletUsesUnboundedExtent(t *testing.T) {
	mapping, err := BuildLoadMapping(singleTabletManifest())
	if err != nil {
		t.Fatal(err)
	}
	if len(mapping) != 1 {
		t.Fatalf("mapping entries = %d, want 1", len(mapping))
	}
	if mapping[0].Tablet.EndRow != nil || mapping[0].Tablet.PrevEndRow != nil {
		t.Fatalf("single-tablet extent = %#v, want fully unbounded", mapping[0].Tablet)
	}
	if len(mapping[0].Files) != 2 {
		t.Fatalf("single-tablet files = %#v, want 2", mapping[0].Files)
	}
	if mapping[0].Files[0].Name != "F0001.rf" || mapping[0].Files[1].Name != "F0002.rf" {
		t.Fatalf("single-tablet files not name-sorted: %#v", mapping[0].Files)
	}
	if mapping[0].Files[0].EstSize != 100 || mapping[0].Files[1].EstSize != 50 {
		t.Fatalf("single-tablet file sizes = %#v", mapping[0].Files)
	}
}

func TestBuildLoadMappingDefaultsSingleTabletForLegacyManifest(t *testing.T) {
	manifest := &engine.RFileExportManifest{
		SourceTable: "events",
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0001.rf", Size: 10},
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0002.rf", Size: 20},
		},
	}
	mapping, err := BuildLoadMapping(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(mapping) != 1 {
		t.Fatalf("mapping entries = %d, want 1", len(mapping))
	}
	if mapping[0].Tablet.EndRow != nil || mapping[0].Tablet.PrevEndRow != nil {
		t.Fatalf("legacy default tablet = %#v, want fully unbounded", mapping[0].Tablet)
	}
	if len(mapping[0].Files) != 2 {
		t.Fatalf("legacy default files = %#v, want 2", mapping[0].Files)
	}
}

func TestBuildLoadMappingRejectsNilManifest(t *testing.T) {
	if _, err := BuildLoadMapping(nil); err == nil {
		t.Fatal("BuildLoadMapping(nil) = nil error, want error")
	}
}

func TestBuildLoadMappingDedupesRepeatedDestinationPath(t *testing.T) {
	manifest := &engine.RFileExportManifest{
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0001.rf", Size: 10},
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0001.rf", Size: 10},
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0002.rf", Size: 20},
		},
	}
	mapping, err := BuildLoadMapping(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(mapping) != 1 {
		t.Fatalf("mapping entries = %d, want 1", len(mapping))
	}
	if len(mapping[0].Files) != 2 {
		t.Fatalf("files = %#v, want 2 (F0001.rf deduped, F0002.rf kept)", mapping[0].Files)
	}
}

func TestBuildLoadMappingRejectsSplitManifest(t *testing.T) {
	if _, err := BuildLoadMapping(splitManifest()); err == nil {
		t.Fatal("BuildLoadMapping(split manifest) = nil error, want error")
	}
}

func TestBuildLoadMappingRejectsSingleTabletBoundaryManifest(t *testing.T) {
	manifest := &engine.RFileExportManifest{
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0, EndRow: strPtr("g")}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0001.rf", Size: 10},
		},
	}
	if _, err := BuildLoadMapping(manifest); err == nil {
		t.Fatal("BuildLoadMapping(single tablet with boundary) = nil error, want error")
	}
}

func TestBuildLoadMappingRejectsUndeclaredTabletIndex(t *testing.T) {
	manifest := &engine.RFileExportManifest{
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 1, DestinationPath: "events/t-0001/F0001.rf", Size: 10},
		},
	}
	if _, err := BuildLoadMapping(manifest); err == nil {
		t.Fatal("BuildLoadMapping with undeclared tablet index = nil error, want error")
	}
}

func TestBuildLoadMappingRejectsAmbiguousLegacyManifest(t *testing.T) {
	manifest := &engine.RFileExportManifest{
		SourceTable: "events",
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 1, DestinationPath: "events/t-0001/F0001.rf", Size: 10},
		},
	}
	if _, err := BuildLoadMapping(manifest); err == nil {
		t.Fatal("BuildLoadMapping with legacy manifest using non-zero tablet index = nil error, want error")
	}
}

func TestWriteLoadMappingMatchesAccumuloJSONShape(t *testing.T) {
	rowNeedingURLSafeEncoding := []byte{0xfb, 0xef, 0xbe}
	mapping := LoadMapping{
		{
			Tablet: KeyExtent{EndRow: rowNeedingURLSafeEncoding},
			Files:  []FileEntry{{Name: "F0001.rf", EstSize: 10, EstEntries: 0}},
		},
		{
			Tablet: KeyExtent{PrevEndRow: rowNeedingURLSafeEncoding},
			Files:  []FileEntry{{Name: "F0002.rf", EstSize: 20, EstEntries: 5}},
		},
	}

	dst := memory.New()
	ctx := context.Background()
	if err := WriteLoadMapping(ctx, dst, "hdfs://nn/bulk/events-1", mapping); err != nil {
		t.Fatal(err)
	}

	raw, err := dst.Open(ctx, "hdfs://nn/bulk/events-1/loadmap.json")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, raw.Size())
	if _, err := raw.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	body := string(buf)

	wantURLSafe := base64.URLEncoding.EncodeToString(rowNeedingURLSafeEncoding)
	wantStd := base64.StdEncoding.EncodeToString(rowNeedingURLSafeEncoding)
	if wantURLSafe == wantStd {
		t.Fatal("test fixture bytes do not actually distinguish URL-safe from standard base64")
	}
	if !strings.Contains(body, wantURLSafe) {
		t.Fatalf("loadmap.json missing expected base64url row %q:\n%s", wantURLSafe, body)
	}
	if strings.Contains(body, wantStd) {
		t.Fatalf("loadmap.json used standard base64 %q instead of URL-safe:\n%s", wantStd, body)
	}
	if strings.Contains(body, `"prevEndRow":null`) || strings.Contains(body, `"endRow":null`) {
		t.Fatalf("loadmap.json serialized a null row instead of omitting the key:\n%s", body)
	}
	for _, want := range []string{`"tablet"`, `"endRow"`, `"prevEndRow"`, `"files"`, `"name"`, `"estSize"`, `"estEntries"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("loadmap.json missing expected field %s:\n%s", want, body)
		}
	}
}

func TestReadLoadMappingRoundTripsSingleTabletWriteLoadMapping(t *testing.T) {
	mapping, err := BuildLoadMapping(singleTabletManifest())
	if err != nil {
		t.Fatal(err)
	}
	dst := memory.New()
	ctx := context.Background()
	if err := WriteLoadMapping(ctx, dst, "/bulk/events-1", mapping); err != nil {
		t.Fatal(err)
	}
	got, err := ReadLoadMapping(ctx, dst, "/bulk/events-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, mapping) {
		t.Fatalf("round-tripped mapping = %#v, want %#v", got, mapping)
	}
}
