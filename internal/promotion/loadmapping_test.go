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

func threeTabletManifest() *engine.RFileExportManifest {
	return &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets: []engine.RFileExportTablet{
			{Index: 0, EndRow: strPtr("g")},
			{Index: 1, StartRow: strPtr("g"), EndRow: strPtr("p")},
			{Index: 2, StartRow: strPtr("p")},
		},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0001.rf", Size: 100},
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0002.rf", Size: 50},
			{TabletIndex: 2, DestinationPath: "events/t-0002/F0003.rf", Size: 75},
			// TabletIndex 1 intentionally has no files: an empty tablet.
		},
	}
}

func TestBuildLoadMappingGroupsByTabletRangeAndOmitsEmptyTablets(t *testing.T) {
	mapping, err := BuildLoadMapping(threeTabletManifest())
	if err != nil {
		t.Fatal(err)
	}
	if len(mapping) != 2 {
		t.Fatalf("mapping entries = %d, want 2 (tablet 1 has no files)", len(mapping))
	}

	first := mapping[0]
	if first.Tablet.PrevEndRow != nil {
		t.Fatalf("first tablet PrevEndRow = %q, want nil", first.Tablet.PrevEndRow)
	}
	if string(first.Tablet.EndRow) != "g" {
		t.Fatalf("first tablet EndRow = %q, want %q", first.Tablet.EndRow, "g")
	}
	if len(first.Files) != 2 {
		t.Fatalf("first tablet files = %#v, want 2 entries", first.Files)
	}
	if first.Files[0].Name != "F0001.rf" || first.Files[1].Name != "F0002.rf" {
		t.Fatalf("first tablet files not name-sorted: %#v", first.Files)
	}
	if first.Files[0].EstSize != 100 || first.Files[1].EstSize != 50 {
		t.Fatalf("first tablet file sizes = %#v", first.Files)
	}

	last := mapping[1]
	if string(last.Tablet.PrevEndRow) != "p" {
		t.Fatalf("last tablet PrevEndRow = %q, want %q", last.Tablet.PrevEndRow, "p")
	}
	if last.Tablet.EndRow != nil {
		t.Fatalf("last tablet EndRow = %q, want nil (unbounded)", last.Tablet.EndRow)
	}
	if len(last.Files) != 1 || last.Files[0].Name != "F0003.rf" {
		t.Fatalf("last tablet files = %#v", last.Files)
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
	// Same physical file listed twice under the same tablet (a legitimate,
	// already-tested manifest shape per flattenNames'
	// TestFlattenNamesAllowsSameDestinationPathTwice) must contribute one
	// FileEntry, not two: Accumulo's Bulk.Files is name-keyed and rejects
	// duplicate filenames outright.
	manifest := &engine.RFileExportManifest{
		SourceTable: "events",
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

func destinationTablets(bounds ...string) []KeyExtent {
	// bounds is a sequence of split points; destinationTablets builds the
	// resulting (n+1) contiguous tablets, unbounded at both ends.
	tablets := make([]KeyExtent, 0, len(bounds)+1)
	var prev []byte
	for _, b := range bounds {
		tablets = append(tablets, KeyExtent{PrevEndRow: prev, EndRow: []byte(b)})
		prev = []byte(b)
	}
	tablets = append(tablets, KeyExtent{PrevEndRow: prev, EndRow: nil})
	return tablets
}

func TestValidateAgainstDestinationAcceptsMatchingSplits(t *testing.T) {
	mapping, err := BuildLoadMapping(threeTabletManifest())
	if err != nil {
		t.Fatal(err)
	}
	// threeTabletManifest's tablets split at "g" and "p" - a destination
	// with the exact same splits (including the empty middle tablet, which
	// BuildLoadMapping omitted from mapping since it has no files) must
	// validate cleanly: mapping's entries only need their PrevEndRow/EndRow
	// to each match *some* destination boundary, not a 1:1 tablet count.
	destination := destinationTablets("g", "p")
	if err := ValidateAgainstDestination(mapping, destination); err != nil {
		t.Fatalf("ValidateAgainstDestination = %v, want nil", err)
	}
}

func TestValidateAgainstDestinationAllowsFileSpanningMultipleDestinationTablets(t *testing.T) {
	// A mapping entry covering (nil, "p"] is valid against a destination
	// that is split more finely (nil,"g"],("g","p"]: PrepBulkImport's own
	// validateLoadMapping walks forward from the entry's PrevEndRow across
	// as many real destination tablets as needed until it finds one whose
	// EndRow matches - it does not require a single destination tablet to
	// span the whole mapping entry.
	mapping := LoadMapping{{Tablet: KeyExtent{EndRow: []byte("p")}, Files: []FileEntry{{Name: "F0001.rf"}}}}
	destination := destinationTablets("g", "p")
	if err := ValidateAgainstDestination(mapping, destination); err != nil {
		t.Fatalf("ValidateAgainstDestination = %v, want nil (file may span multiple destination tablets)", err)
	}
}

func TestValidateAgainstDestinationRejectsMismatchedPrevEndRow(t *testing.T) {
	mapping := LoadMapping{{Tablet: KeyExtent{PrevEndRow: []byte("m"), EndRow: []byte("p")}}}
	destination := destinationTablets("g", "p") // no split at "m"
	err := ValidateAgainstDestination(mapping, destination)
	if err == nil {
		t.Fatal("ValidateAgainstDestination = nil, want error (no destination split at prevEndRow)")
	}
}

func TestValidateAgainstDestinationRejectsMismatchedEndRow(t *testing.T) {
	mapping := LoadMapping{{Tablet: KeyExtent{EndRow: []byte("m")}}} // destination has no split at "m"
	destination := destinationTablets("g", "p")
	err := ValidateAgainstDestination(mapping, destination)
	if err == nil {
		t.Fatal("ValidateAgainstDestination = nil, want error (no destination split at endRow)")
	}
}

func TestValidateAgainstDestinationRejectsEmptyDestination(t *testing.T) {
	mapping := LoadMapping{{Tablet: KeyExtent{}}}
	if err := ValidateAgainstDestination(mapping, nil); err == nil {
		t.Fatal("ValidateAgainstDestination with no destination tablets = nil, want error")
	}
}

func TestValidateAgainstDestinationDistinguishesNilFromEmptyRow(t *testing.T) {
	// A destination whose first tablet has PrevEndRow nil (unbounded) must
	// not be confused with a tablet whose PrevEndRow is the zero-length
	// (but non-nil) row "": these mean different things (negative infinity
	// vs. an actual split at the empty row) and must not collide in
	// boundaryKey's map encoding.
	mapping := LoadMapping{{Tablet: KeyExtent{PrevEndRow: []byte(""), EndRow: []byte("g")}}}
	destination := []KeyExtent{
		{PrevEndRow: nil, EndRow: []byte("g")}, // first tablet: unbounded start, not ""
	}
	err := ValidateAgainstDestination(mapping, destination)
	if err == nil {
		t.Fatal("ValidateAgainstDestination = nil, want error (mapping's \"\" PrevEndRow must not match destination's nil/unbounded start)")
	}
}

func TestValidateAgainstDestinationAcceptsFullyUnboundedSingleTablet(t *testing.T) {
	mapping, err := BuildLoadMapping(&engine.RFileExportManifest{
		SourceTable: "events",
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: "events/t-0000/F0001.rf", Size: 10},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	destination := []KeyExtent{{}} // single tablet, fully unbounded both ends
	if err := ValidateAgainstDestination(mapping, destination); err != nil {
		t.Fatalf("ValidateAgainstDestination = %v, want nil", err)
	}
}

func TestWriteLoadMappingMatchesAccumuloJSONShape(t *testing.T) {
	// Row bytes chosen so standard base64 and URL-safe base64 diverge
	// (0xFB 0xEF produces '+'/'/' in standard alphabet, '-'/'_' in URL-safe),
	// so a passing test proves base64.URLEncoding was actually used and not
	// just any base64 variant.
	rowNeedingURLSafeEncoding := []byte{0xfb, 0xef, 0xbe}
	mapping := LoadMapping{
		{
			Tablet: KeyExtent{EndRow: rowNeedingURLSafeEncoding}, // PrevEndRow nil: first tablet
			Files:  []FileEntry{{Name: "F0001.rf", EstSize: 10, EstEntries: 0}},
		},
		{
			Tablet: KeyExtent{PrevEndRow: rowNeedingURLSafeEncoding}, // EndRow nil: last tablet
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

	// First tablet has no prevEndRow: the key must be entirely absent, not
	// present as JSON null (matches Gson's default null-suppression).
	if strings.Contains(body, `"prevEndRow":null`) || strings.Contains(body, `"endRow":null`) {
		t.Fatalf("loadmap.json serialized a null row instead of omitting the key:\n%s", body)
	}
	for _, want := range []string{`"tablet"`, `"endRow"`, `"prevEndRow"`, `"files"`, `"name"`, `"estSize"`, `"estEntries"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("loadmap.json missing expected field %s:\n%s", want, body)
		}
	}
}

func TestReadLoadMappingRoundTripsWriteLoadMapping(t *testing.T) {
	mapping, err := BuildLoadMapping(threeTabletManifest())
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
