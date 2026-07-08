package bcfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// Reader is a partial BCFile reader: it parses the trailer and meta-block
// index but stops short of decompressing data/meta blocks. The block
// decompressor lives in a sibling package (see `internal/rfile/bcfile/block`
// when it lands) and is layered on top: callers grab a BlockRegion from
// here and ask the decompressor to materialize it.
//
// Why split? Decompression pulls in codec libraries (snappy, zstd, gzip)
// that have transitive deps + version concerns. Keeping container parsing
// dependency-free lets callers test the read path against captured trailer
// bytes without ever installing a codec, and lets the codec layer evolve
// independently.
type Reader struct {
	src        io.ReaderAt
	fileLength int64

	footer    Footer
	metaIndex *MetaIndex
}

// ErrNoSuchMetaBlock is returned by MetaBlock when the requested name
// isn't present in the index.
var ErrNoSuchMetaBlock = errors.New("bcfile: no such meta block")

// NewReader parses the BCFile trailer + MetaIndex from src. It does NOT
// touch the DataIndex or any data blocks (those are gated on a codec).
//
// The src must support io.ReaderAt; we issue exactly two ReadAt calls:
// one for the trailer and one for the MetaIndex bytes.
func NewReader(src io.ReaderAt, fileLength int64) (*Reader, error) {
	footer, err := ReadFooter(src, fileLength)
	if err != nil {
		return nil, err
	}
	r := &Reader{src: src, fileLength: fileLength, footer: footer}

	// MetaIndex starts at footer.OffsetIndexMeta and runs up to the start
	// of the trailer (cryptoParams in v3, version+magic in v1). We don't
	// know the exact length on disk because it's variable-length encoded,
	// so we read a generous slice and parse just what we need. Cap the
	// slice at what's actually before the trailer.
	miEnd := r.metaIndexUpperBound()
	miLen := miEnd - footer.OffsetIndexMeta
	if miLen <= 0 {
		return nil, fmt.Errorf("bcfile: MetaIndex region empty (start=%d, end=%d)",
			footer.OffsetIndexMeta, miEnd)
	}
	miBuf := make([]byte, miLen)
	if _, err := src.ReadAt(miBuf, footer.OffsetIndexMeta); err != nil {
		return nil, fmt.Errorf("bcfile: read MetaIndex region: %w", err)
	}
	mi, err := ReadMetaIndex(bytes.NewReader(miBuf))
	if err != nil {
		return nil, err
	}
	r.metaIndex = mi
	return r, nil
}

// Footer returns the parsed BCFile trailer.
func (r *Reader) Footer() Footer { return r.footer }

// Source returns the underlying io.ReaderAt the Reader was constructed
// with. Higher-level layers (the RFile reader, the block decompressor)
// need direct access to issue their own ReadAt calls — exposing it here
// avoids forcing them to plumb the original source through alongside
// the *bcfile.Reader.
func (r *Reader) Source() io.ReaderAt { return r.src }

// FileLength returns the byte length of the underlying file.
func (r *Reader) FileLength() int64 { return r.fileLength }

// MetaIndex returns the parsed meta-block index.
func (r *Reader) MetaIndex() *MetaIndex { return r.metaIndex }

// MetaBlockEntry returns the index entry for the named meta block, or
// ErrNoSuchMetaBlock.
func (r *Reader) MetaBlockEntry(name string) (MetaIndexEntry, error) {
	e, ok := r.metaIndex.Lookup(name)
	if !ok {
		return MetaIndexEntry{}, fmt.Errorf("%w: %q", ErrNoSuchMetaBlock, name)
	}
	return e, nil
}

// RawBlock returns the on-disk (still-compressed) bytes of one block.
// Codec dispatch happens in the decompressor layer; callers that just
// want to feed bytes to a snappy/gzip/zstd reader use this. The slice
// is freshly allocated.
func (r *Reader) RawBlock(region BlockRegion) ([]byte, error) {
	if region.CompressedSize < 0 {
		return nil, fmt.Errorf("bcfile: negative CompressedSize %d", region.CompressedSize)
	}
	if region.Offset < 0 || region.Offset+region.CompressedSize > r.fileLength {
		return nil, fmt.Errorf("bcfile: block region out of bounds: offset=%d size=%d fileLen=%d",
			region.Offset, region.CompressedSize, r.fileLength)
	}
	buf := make([]byte, region.CompressedSize)
	if _, err := r.src.ReadAt(buf, region.Offset); err != nil {
		return nil, fmt.Errorf("bcfile: read block @ %d (%d bytes): %w",
			region.Offset, region.CompressedSize, err)
	}
	return buf, nil
}

// metaIndexUpperBound returns the byte offset where the trailer begins —
// i.e. the upper bound of the MetaIndex region.
func (r *Reader) metaIndexUpperBound() int64 {
	trailerSize := int64(MagicSize + VersionSize)
	if r.footer.Version.CompatibleWith(APIVersion3) {
		trailerSize += 16 // offsetIndexMeta + offsetCryptoParams
	} else {
		trailerSize += 8 // offsetIndexMeta only
	}
	// In v3, the crypto params block lives between MetaIndex and the
	// trailer; we don't know its length statically. Conservative bound:
	// stop at offsetCryptoParams. For v1 there's no crypto block, so
	// stop at the trailer.
	if r.footer.Version.CompatibleWith(APIVersion3) && r.footer.OffsetCryptoParams > r.footer.OffsetIndexMeta {
		return r.footer.OffsetCryptoParams
	}
	return r.fileLength - trailerSize
}
