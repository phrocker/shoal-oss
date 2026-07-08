package bcfile

import (
	"fmt"
	"io"
)

// BlockRegion identifies one compressed region inside a BCFile: where it
// starts (Offset), how many bytes it occupies on disk (CompressedSize),
// and how many bytes it expands to (RawSize). Mirrors BCFile.BlockRegion.
//
// Used for both data blocks (listed in DataIndex) and meta blocks
// (referenced by MetaIndexEntry).
type BlockRegion struct {
	Offset         int64
	CompressedSize int64
	RawSize        int64
}

// ReadBlockRegion deserializes a BlockRegion: three back-to-back BCFile
// vlongs (offset, compressedSize, rawSize).
func ReadBlockRegion(r ByteAndReader) (BlockRegion, error) {
	off, _, err := ReadVLong(r)
	if err != nil {
		return BlockRegion{}, fmt.Errorf("BlockRegion offset: %w", err)
	}
	csz, _, err := ReadVLong(r)
	if err != nil {
		return BlockRegion{}, fmt.Errorf("BlockRegion compressedSize: %w", err)
	}
	rsz, _, err := ReadVLong(r)
	if err != nil {
		return BlockRegion{}, fmt.Errorf("BlockRegion rawSize: %w", err)
	}
	return BlockRegion{Offset: off, CompressedSize: csz, RawSize: rsz}, nil
}

// WriteBlockRegion serializes a BlockRegion in BCFile wire form.
func WriteBlockRegion(w io.Writer, br BlockRegion) error {
	bw := byteWriterShim{w: w}
	if _, err := WriteVLong(bw, br.Offset); err != nil {
		return err
	}
	if _, err := WriteVLong(bw, br.CompressedSize); err != nil {
		return err
	}
	if _, err := WriteVLong(bw, br.RawSize); err != nil {
		return err
	}
	return nil
}
