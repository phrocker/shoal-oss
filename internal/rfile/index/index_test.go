package index

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/phrocker/shoal/internal/rfile/wire"
)

func sampleKey(rowSuffix string) *wire.Key {
	return &wire.Key{
		Row:              []byte("row-" + rowSuffix),
		ColumnFamily:     []byte("cf"),
		ColumnQualifier:  []byte("cq"),
		ColumnVisibility: []byte(""),
		Timestamp:        12345,
		Deleted:          false,
	}
}

func TestIndexEntryRoundtrip(t *testing.T) {
	want := &IndexEntry{
		Key:            sampleKey("aaa"),
		NumEntries:     7,
		Offset:         1024,
		CompressedSize: 256,
		RawSize:        2048,
	}
	var buf bytes.Buffer
	if err := WriteIndexEntry(&buf, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadIndexEntry(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got.NumEntries != want.NumEntries ||
		got.Offset != want.Offset ||
		got.CompressedSize != want.CompressedSize ||
		got.RawSize != want.RawSize {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if !got.Key.Equal(want.Key) {
		t.Errorf("Key mismatch: got %+v, want %+v", got.Key, want.Key)
	}
}

func TestIndexEntryTruncated(t *testing.T) {
	want := &IndexEntry{Key: sampleKey("a"), NumEntries: 1, Offset: 1, CompressedSize: 1, RawSize: 1}
	var buf bytes.Buffer
	if err := WriteIndexEntry(&buf, want); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()
	for i := 0; i < len(full); i++ {
		_, err := ReadIndexEntry(bytes.NewReader(full[:i]))
		if err == nil {
			t.Errorf("ReadIndexEntry on %d-byte truncation succeeded, expected error", i)
		}
	}
}

func TestIndexBlockMultiLevelRoundtrip(t *testing.T) {
	// Build a block with 3 IndexEntries.
	entries := []*IndexEntry{
		{Key: sampleKey("a"), NumEntries: 100, Offset: 1024, CompressedSize: 200, RawSize: 800},
		{Key: sampleKey("m"), NumEntries: 50, Offset: 2048, CompressedSize: 150, RawSize: 600},
		{Key: sampleKey("z"), NumEntries: 25, Offset: 3072, CompressedSize: 80, RawSize: 300},
	}
	// Encode entries into Data + record offsets.
	var data bytes.Buffer
	offsets := make([]int32, 0, len(entries))
	for _, e := range entries {
		offsets = append(offsets, int32(data.Len()))
		if err := WriteIndexEntry(&data, e); err != nil {
			t.Fatal(err)
		}
	}
	want := &IndexBlock{
		Level:   1,
		Offset:  1000,
		HasNext: true,
		Offsets: offsets,
		Data:    data.Bytes(),
	}
	var buf bytes.Buffer
	if err := WriteIndexBlock(&buf, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadIndexBlock(bytes.NewReader(buf.Bytes()), V8)
	if err != nil {
		t.Fatal(err)
	}
	if got.Level != want.Level || got.Offset != want.Offset || got.HasNext != want.HasNext {
		t.Errorf("header mismatch: got %+v, want %+v", got, want)
	}
	if got.NumEntries() != len(entries) {
		t.Errorf("NumEntries = %d, want %d", got.NumEntries(), len(entries))
	}
	if !bytes.Equal(got.Data, want.Data) {
		t.Errorf("Data mismatch: got len %d, want %d", len(got.Data), len(want.Data))
	}
	// Decode entries from the preserved Data using the offsets.
	for i, e := range entries {
		readBack, err := ReadIndexEntry(bytes.NewReader(got.Data[got.Offsets[i]:]))
		if err != nil {
			t.Fatalf("entry %d: %v", i, err)
		}
		if readBack.Offset != e.Offset || !readBack.Key.Equal(e.Key) {
			t.Errorf("entry %d: got %+v, want %+v", i, readBack, e)
		}
	}
}

func TestIndexBlockFlatV3(t *testing.T) {
	// v3/v4 layout: int32 size + size × IndexEntry (no level/offset/hasNext header).
	// Build that wire form by hand, then verify ReadIndexBlock(..., V3) reads it
	// and re-presents it as a level-0 block with explicit offsets.
	entries := []*IndexEntry{
		{Key: sampleKey("aa"), NumEntries: 10, Offset: 100, CompressedSize: 50, RawSize: 200},
		{Key: sampleKey("bb"), NumEntries: 20, Offset: 200, CompressedSize: 100, RawSize: 400},
	}
	var buf bytes.Buffer
	if err := wire.WriteInt32(&buf, int32(len(entries))); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := WriteIndexEntry(&buf, e); err != nil {
			t.Fatal(err)
		}
	}
	got, err := ReadIndexBlock(bytes.NewReader(buf.Bytes()), V3)
	if err != nil {
		t.Fatal(err)
	}
	if got.Level != 0 || got.Offset != 0 || got.HasNext {
		t.Errorf("flat: header should be zero/false, got %+v", got)
	}
	if got.NumEntries() != len(entries) {
		t.Errorf("NumEntries = %d, want %d", got.NumEntries(), len(entries))
	}
	for i, e := range entries {
		readBack, err := ReadIndexEntry(bytes.NewReader(got.Data[got.Offsets[i]:]))
		if err != nil {
			t.Fatalf("entry %d: %v", i, err)
		}
		if readBack.Offset != e.Offset || !readBack.Key.Equal(e.Key) {
			t.Errorf("entry %d roundtrip mismatch", i)
		}
	}
}

func TestIndexBlockUnsupportedVersion(t *testing.T) {
	_, err := ReadIndexBlock(bytes.NewReader(nil), 99)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Errorf("err = %v, want ErrUnsupportedVersion", err)
	}
}

func makeMinimalIndexBlock(t *testing.T) *IndexBlock {
	t.Helper()
	// One leaf entry pointing at a hypothetical data block.
	entry := &IndexEntry{
		Key:            sampleKey("zzz"),
		NumEntries:     1,
		Offset:         1024,
		CompressedSize: 64,
		RawSize:        128,
	}
	var data bytes.Buffer
	if err := WriteIndexEntry(&data, entry); err != nil {
		t.Fatal(err)
	}
	return &IndexBlock{
		Level:   0,
		Offset:  0,
		HasNext: false,
		Offsets: []int32{0},
		Data:    data.Bytes(),
	}
}

func TestLocalityGroupRoundtrip_V8Default(t *testing.T) {
	want := &LocalityGroup{
		IsDefault:       true,
		Name:            "",
		ColumnFamilies:  map[string]int64{"family-a": 100, "family-b": 50},
		FirstKey:        sampleKey("first"),
		NumTotalEntries: 150,
		RootIndex:       makeMinimalIndexBlock(t),
	}
	var buf bytes.Buffer
	if err := WriteLocalityGroup(&buf, want, V8); err != nil {
		t.Fatal(err)
	}
	got, err := ReadLocalityGroup(bytes.NewReader(buf.Bytes()), V8)
	if err != nil {
		t.Fatal(err)
	}
	if got.IsDefault != want.IsDefault {
		t.Errorf("IsDefault = %v", got.IsDefault)
	}
	if got.Name != want.Name {
		t.Errorf("Name = %q, want %q", got.Name, want.Name)
	}
	if len(got.ColumnFamilies) != len(want.ColumnFamilies) {
		t.Errorf("CF count = %d, want %d", len(got.ColumnFamilies), len(want.ColumnFamilies))
	}
	for cf, c := range want.ColumnFamilies {
		if got.ColumnFamilies[cf] != c {
			t.Errorf("CF[%q] = %d, want %d", cf, got.ColumnFamilies[cf], c)
		}
	}
	if !got.FirstKey.Equal(want.FirstKey) {
		t.Errorf("FirstKey mismatch")
	}
	if got.NumTotalEntries != want.NumTotalEntries {
		t.Errorf("NumTotalEntries = %d, want %d", got.NumTotalEntries, want.NumTotalEntries)
	}
}

func TestLocalityGroupRoundtrip_V8Named(t *testing.T) {
	want := &LocalityGroup{
		IsDefault:       false,
		Name:            "secondary-lg",
		ColumnFamilies:  map[string]int64{"only-cf": 42},
		FirstKey:        nil, // empty group
		NumTotalEntries: 0,
		RootIndex:       &IndexBlock{Level: 0, Offset: 0, Offsets: []int32{}, Data: []byte{}},
	}
	var buf bytes.Buffer
	if err := WriteLocalityGroup(&buf, want, V8); err != nil {
		t.Fatal(err)
	}
	got, err := ReadLocalityGroup(bytes.NewReader(buf.Bytes()), V8)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "secondary-lg" {
		t.Errorf("Name = %q", got.Name)
	}
	if got.FirstKey != nil {
		t.Errorf("FirstKey should be nil, got %+v", got.FirstKey)
	}
}

func TestLocalityGroupRoundtrip_V7WithStartBlock(t *testing.T) {
	// v7 stores startBlock; verify the writer/reader honor it.
	want := &LocalityGroup{
		IsDefault:       true,
		Name:            "",
		StartBlock:      17,
		ColumnFamilies:  nil, // -1 sentinel: too many CFs to track
		FirstKey:        sampleKey("first"),
		NumTotalEntries: 1,
		RootIndex:       makeMinimalIndexBlock(t),
	}
	var buf bytes.Buffer
	if err := WriteLocalityGroup(&buf, want, V7); err != nil {
		t.Fatal(err)
	}
	got, err := ReadLocalityGroup(bytes.NewReader(buf.Bytes()), V7)
	if err != nil {
		t.Fatal(err)
	}
	if got.StartBlock != 17 {
		t.Errorf("StartBlock = %d, want 17", got.StartBlock)
	}
	if got.ColumnFamilies != nil {
		t.Errorf("ColumnFamilies should be nil (cfCount=-1 sentinel), got %v", got.ColumnFamilies)
	}
}

func TestLocalityGroup_RejectsCFCountMinusOneOnNonDefault(t *testing.T) {
	// Hand-craft a wire byte sequence that violates the invariant: a
	// non-default LG with cfCount=-1.
	var buf bytes.Buffer
	wire.WriteBool(&buf, false) // !isDefault
	wire.WriteUTF(&buf, "named")
	// no startBlock (V8)
	wire.WriteInt32(&buf, -1) // cfCount = -1, illegal here

	_, err := ReadLocalityGroup(bytes.NewReader(buf.Bytes()), V8)
	if !errors.Is(err, ErrCorruptColumnFamilies) {
		t.Errorf("err = %v, want ErrCorruptColumnFamilies chain", err)
	}
}

func TestParse_V8MinimalSingleGroup(t *testing.T) {
	// Build the smallest possible RFile.index meta block: magic + V8 + 1
	// default LG with no CFs, no firstKey, empty index.
	var buf bytes.Buffer
	if err := wire.WriteInt32(&buf, RIndexMagic); err != nil {
		t.Fatal(err)
	}
	if err := wire.WriteInt32(&buf, V8); err != nil {
		t.Fatal(err)
	}
	if err := wire.WriteInt32(&buf, 1); err != nil { // 1 group
		t.Fatal(err)
	}
	lg := &LocalityGroup{
		IsDefault: true,
		Name:      "",
		ColumnFamilies: map[string]int64{}, // empty (not nil — we WANT cfCount=0)
		FirstKey:        nil,
		NumTotalEntries: 0,
		RootIndex:       &IndexBlock{Level: 0, Offsets: []int32{}, Data: []byte{}},
	}
	if err := WriteLocalityGroup(&buf, lg, V8); err != nil {
		t.Fatal(err)
	}

	r, err := Parse(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if r.Version != V8 {
		t.Errorf("Version = %d, want %d", r.Version, V8)
	}
	if len(r.Groups) != 1 {
		t.Fatalf("Groups len = %d, want 1", len(r.Groups))
	}
	if !r.Groups[0].IsDefault {
		t.Errorf("group should be default")
	}
}

func TestParse_BadMagic(t *testing.T) {
	var buf bytes.Buffer
	var bad uint32 = 0xdeadbeef
	wire.WriteInt32(&buf, int32(bad)) // wrong magic (truncate via variable)
	wire.WriteInt32(&buf, V8)

	_, err := Parse(buf.Bytes())
	if !errors.Is(err, ErrBadMagic) {
		t.Errorf("err = %v, want ErrBadMagic", err)
	}
}

func TestParse_UnsupportedVersion(t *testing.T) {
	var buf bytes.Buffer
	wire.WriteInt32(&buf, RIndexMagic)
	wire.WriteInt32(&buf, 99) // unsupported

	_, err := Parse(buf.Bytes())
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Errorf("err = %v, want ErrUnsupportedVersion", err)
	}
}

func TestParse_NegativeGroupCount(t *testing.T) {
	var buf bytes.Buffer
	wire.WriteInt32(&buf, RIndexMagic)
	wire.WriteInt32(&buf, V8)
	wire.WriteInt32(&buf, -5)

	_, err := Parse(buf.Bytes())
	if err == nil {
		t.Errorf("expected error for negative group count")
	}
}

func TestParse_TruncatedAfterHeader(t *testing.T) {
	var buf bytes.Buffer
	wire.WriteInt32(&buf, RIndexMagic)
	wire.WriteInt32(&buf, V8)
	wire.WriteInt32(&buf, 1) // promised 1 group but no bytes follow

	_, err := Parse(buf.Bytes())
	if err == nil {
		t.Errorf("expected truncation error")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		// wrapped EOF — fine, just confirm an error
		t.Logf("got error: %v (acceptable)", err)
	}
}

func TestParse_V8MultipleGroups(t *testing.T) {
	// One default LG (empty CF table) + one named LG with two CFs.
	var buf bytes.Buffer
	wire.WriteInt32(&buf, RIndexMagic)
	wire.WriteInt32(&buf, V8)
	wire.WriteInt32(&buf, 2)

	defaultLG := &LocalityGroup{
		IsDefault:       true,
		ColumnFamilies:  map[string]int64{},
		NumTotalEntries: 5,
		RootIndex:       makeMinimalIndexBlock(t),
	}
	if err := WriteLocalityGroup(&buf, defaultLG, V8); err != nil {
		t.Fatal(err)
	}
	namedLG := &LocalityGroup{
		IsDefault:       false,
		Name:            "vector-lg",
		ColumnFamilies:  map[string]int64{"vec_quant": 10000, "vec_centroid": 16},
		FirstKey:        sampleKey("vec-first"),
		NumTotalEntries: 10016,
		RootIndex:       makeMinimalIndexBlock(t),
	}
	if err := WriteLocalityGroup(&buf, namedLG, V8); err != nil {
		t.Fatal(err)
	}

	r, err := Parse(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Groups) != 2 {
		t.Fatalf("len = %d", len(r.Groups))
	}
	if !r.Groups[0].IsDefault {
		t.Errorf("group 0 should be default")
	}
	if r.Groups[1].Name != "vector-lg" {
		t.Errorf("group 1 name = %q, want vector-lg", r.Groups[1].Name)
	}
	if r.Groups[1].ColumnFamilies["vec_quant"] != 10000 {
		t.Errorf("CF count mismatch: %v", r.Groups[1].ColumnFamilies)
	}
}

func TestParse_V8WithoutOptionalTrailers(t *testing.T) {
	// v8 file written without the optional sample/vector trailer flags.
	// Java handles this via mb.available()>0 checks. Verify Parse doesn't
	// error on a clean exact-fit input.
	var buf bytes.Buffer
	wire.WriteInt32(&buf, RIndexMagic)
	wire.WriteInt32(&buf, V8)
	wire.WriteInt32(&buf, 1)
	if err := WriteLocalityGroup(&buf, &LocalityGroup{
		IsDefault:       true,
		ColumnFamilies:  map[string]int64{},
		NumTotalEntries: 0,
		RootIndex:       &IndexBlock{Offsets: []int32{}, Data: []byte{}},
	}, V8); err != nil {
		t.Fatal(err)
	}
	// No trailing bytes — buf ends exactly here.
	r, err := Parse(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if r.SampleGroups != nil || r.VectorIndexRaw != nil {
		t.Errorf("optional trailers should be nil")
	}
}

func TestParse_V8WithVectorTrailer(t *testing.T) {
	var buf bytes.Buffer
	wire.WriteInt32(&buf, RIndexMagic)
	wire.WriteInt32(&buf, V8)
	wire.WriteInt32(&buf, 1)
	if err := WriteLocalityGroup(&buf, &LocalityGroup{
		IsDefault:       true,
		ColumnFamilies:  map[string]int64{},
		NumTotalEntries: 0,
		RootIndex:       &IndexBlock{Offsets: []int32{}, Data: []byte{}},
	}, V8); err != nil {
		t.Fatal(err)
	}
	wire.WriteBool(&buf, false) // hasSamples = false
	wire.WriteBool(&buf, true)  // hasVectorIndex = true
	// Pad with arbitrary bytes that we expect to land in VectorIndexRaw.
	buf.Write([]byte{0xab, 0xcd, 0xef, 0x12, 0x34})

	r, err := Parse(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.VectorIndexRaw, []byte{0xab, 0xcd, 0xef, 0x12, 0x34}) {
		t.Errorf("VectorIndexRaw = %x", r.VectorIndexRaw)
	}
}

func TestParse_V8SampleGroupsErrorsCleanly(t *testing.T) {
	// Sample groups are unimplemented; Parse must error rather than silently
	// drop them or hang.
	var buf bytes.Buffer
	wire.WriteInt32(&buf, RIndexMagic)
	wire.WriteInt32(&buf, V8)
	wire.WriteInt32(&buf, 1)
	if err := WriteLocalityGroup(&buf, &LocalityGroup{
		IsDefault:       true,
		ColumnFamilies:  map[string]int64{},
		NumTotalEntries: 0,
		RootIndex:       &IndexBlock{Offsets: []int32{}, Data: []byte{}},
	}, V8); err != nil {
		t.Fatal(err)
	}
	wire.WriteBool(&buf, true) // hasSamples = true
	if err := WriteLocalityGroup(&buf, &LocalityGroup{
		IsDefault:       true,
		ColumnFamilies:  map[string]int64{},
		NumTotalEntries: 0,
		RootIndex:       &IndexBlock{Offsets: []int32{}, Data: []byte{}},
	}, V8); err != nil {
		t.Fatal(err)
	}

	_, err := Parse(buf.Bytes())
	if err == nil {
		t.Errorf("expected error for unimplemented sample groups")
	}
}
