package bcfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// buildSyntheticBCFile assembles a minimal v3 BCFile in memory: some data
// bytes, a MetaIndex with one entry, then crypto-params (placeholder
// zero-length region), then the v3 trailer. We don't compress anything —
// the test only exercises container parsing.
func buildSyntheticBCFile(t *testing.T, mi *MetaIndex, dataBlock []byte, dataBlockOffset int64) ([]byte, Footer) {
	t.Helper()
	var buf bytes.Buffer

	// 1. Pad with leading bytes so dataBlockOffset is honored.
	if dataBlockOffset > 0 {
		buf.Write(bytes.Repeat([]byte{0xaa}, int(dataBlockOffset)))
	}
	// 2. Write the data block (raw — no codec for this test).
	buf.Write(dataBlock)
	// 3. MetaIndex block — capture its offset before writing.
	metaIndexOffset := int64(buf.Len())
	if err := WriteMetaIndex(&buf, mi); err != nil {
		t.Fatal(err)
	}
	// 4. Crypto params block (empty placeholder for v3). Record offset.
	cryptoOffset := int64(buf.Len())
	// Write a placeholder 8 bytes so the region isn't empty — we never
	// parse this; we just need an offset that's between MetaIndex end and
	// trailer start.
	binary.Write(&buf, binary.BigEndian, uint64(0))

	// 5. Trailer.
	footer := Footer{
		Version:            APIVersion3,
		OffsetIndexMeta:    metaIndexOffset,
		OffsetCryptoParams: cryptoOffset,
	}
	if err := WriteFooter(&buf, footer); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), footer
}

func TestReader_ParsesFooterAndMetaIndex(t *testing.T) {
	want := &MetaIndex{Entries: map[string]MetaIndexEntry{
		DataIndexBlockName: {
			Name:            DataIndexBlockName,
			CompressionAlgo: "gz",
			Region:          BlockRegion{Offset: 100, CompressedSize: 50, RawSize: 200},
		},
		"RFile.index": {
			Name:            "RFile.index",
			CompressionAlgo: "snappy",
			Region:          BlockRegion{Offset: 200, CompressedSize: 80, RawSize: 256},
		},
	}}
	dataBlock := bytes.Repeat([]byte{0x42}, 100)
	bs, footer := buildSyntheticBCFile(t, want, dataBlock, 100)

	r, err := NewReader(bytes.NewReader(bs), int64(len(bs)))
	if err != nil {
		t.Fatal(err)
	}

	if r.Footer() != footer {
		t.Errorf("Footer = %+v, want %+v", r.Footer(), footer)
	}
	got := r.MetaIndex()
	if len(got.Entries) != 2 {
		t.Fatalf("entries len = %d, want 2", len(got.Entries))
	}
	for name, w := range want.Entries {
		g, err := r.MetaBlockEntry(name)
		if err != nil {
			t.Errorf("MetaBlockEntry(%q): %v", name, err)
			continue
		}
		if g != w {
			t.Errorf("entry %q: got %+v, want %+v", name, g, w)
		}
	}
}

func TestReader_MissingMetaBlock(t *testing.T) {
	mi := &MetaIndex{Entries: map[string]MetaIndexEntry{
		DataIndexBlockName: {Name: DataIndexBlockName, CompressionAlgo: "gz"},
	}}
	bs, _ := buildSyntheticBCFile(t, mi, nil, 0)

	r, err := NewReader(bytes.NewReader(bs), int64(len(bs)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.MetaBlockEntry("nonexistent")
	if !errors.Is(err, ErrNoSuchMetaBlock) {
		t.Errorf("err = %v, want ErrNoSuchMetaBlock", err)
	}
}

func TestReader_RawBlockBoundsCheck(t *testing.T) {
	mi := &MetaIndex{Entries: map[string]MetaIndexEntry{
		DataIndexBlockName: {Name: DataIndexBlockName, CompressionAlgo: "gz"},
	}}
	bs, _ := buildSyntheticBCFile(t, mi, nil, 0)
	r, err := NewReader(bytes.NewReader(bs), int64(len(bs)))
	if err != nil {
		t.Fatal(err)
	}
	// Region claims more bytes than the file has.
	_, err = r.RawBlock(BlockRegion{Offset: 0, CompressedSize: int64(len(bs) + 1), RawSize: 1})
	if err == nil {
		t.Errorf("expected out-of-bounds error")
	}
	// Negative CompressedSize.
	_, err = r.RawBlock(BlockRegion{Offset: 0, CompressedSize: -1})
	if err == nil {
		t.Errorf("expected negative-size error")
	}
}

func TestReader_RawBlockReturnsExactBytes(t *testing.T) {
	dataBlock := []byte("hello, bcfile data block")
	mi := &MetaIndex{Entries: map[string]MetaIndexEntry{
		DataIndexBlockName: {Name: DataIndexBlockName, CompressionAlgo: "none"},
	}}
	bs, _ := buildSyntheticBCFile(t, mi, dataBlock, 50)
	r, err := NewReader(bytes.NewReader(bs), int64(len(bs)))
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.RawBlock(BlockRegion{Offset: 50, CompressedSize: int64(len(dataBlock)), RawSize: int64(len(dataBlock))})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, dataBlock) {
		t.Errorf("RawBlock = %q, want %q", got, dataBlock)
	}
}

func TestReader_NotABCFile(t *testing.T) {
	junk := bytes.Repeat([]byte{0xff}, 200)
	_, err := NewReader(bytes.NewReader(junk), int64(len(junk)))
	if !errors.Is(err, ErrBadMagic) {
		t.Errorf("err = %v, want ErrBadMagic", err)
	}
}
