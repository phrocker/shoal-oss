package rfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/phrocker/shoal/internal/rfile/adjacency"
	"github.com/phrocker/shoal/internal/rfile/bcfile"
	"github.com/phrocker/shoal/internal/rfile/bcfile/block"
	"github.com/phrocker/shoal/internal/rfile/blockmeta"
	"github.com/phrocker/shoal/internal/rfile/index"
	"github.com/phrocker/shoal/internal/rfile/relkey"
	"github.com/phrocker/shoal/internal/rfile/wire"
)

// DefaultBlockSize is the per-data-block target byte size for newly
// produced RFiles. Mirrors Java's default of 100KB
// (Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE). The writer flushes a
// block once its accumulated raw bytes reach or exceed this threshold.
const DefaultBlockSize = 100 * 1024

// DefaultIndexBlockSize is the per-index-block target byte size for
// the multi-level index. Mirrors Java's default of 128KB
// (Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX). Once a level's
// accumulated entries (their serialized bytes plus the int32 offset
// table) exceed this threshold, the level is finalized as a BCFile
// data block and a parent-level entry pointing at it is created.
const DefaultIndexBlockSize = 128 * 1024

// Writer produces an RFile by streaming bytes to an io.Writer. Use:
//
//	w := rfile.NewWriter(out, rfile.WriterOptions{Codec: "gz"})
//	for each cell: w.Append(key, value)
//	w.Close()
//
// V0 limitations:
//   - Single locality group (the default LG). No named LGs / sample
//     groups / vector index. Java's RFile.Writer wraps every output in
//     a default LG by default too, so this is the common case.
//   - Cell ordering: caller MUST Append cells in non-decreasing Key
//     order. The writer doesn't sort or check; out-of-order keys would
//     produce a malformed RFile.
//
// Multi-level index: the writer mirrors Java's
// MultiLevelIndex.Writer.flush(int level, Key lastKey, boolean last).
// Each level accumulates IndexEntries; when a level overflows
// IndexBlockSize, it's serialized + compressed + written as a BCFile
// data block, and a parent-level entry pointing at that block is
// created. The topmost level always stays in memory and is written
// inline as the root in the RFile.index meta block. This means files
// of any practical cluster size produce a compact root regardless of
// data-block count.
type Writer struct {
	out  *bcfile.Writer
	comp *block.Compressor
	opts WriterOptions

	// Currently-accumulating data block.
	blockBuf     bytes.Buffer
	blockPrev    *Key // for relkey prefix compression
	blockLast    *Key // becomes IndexEntry.Key on flush
	blockEntries int32

	// Multi-level index state.
	levels     []*indexLevel
	totalAdded int32 // count of data blocks (leaf IndexEntries) added

	// pendingLeaf defers the most-recently-flushed data block's
	// IndexEntry by one position. We need this because Java's
	// MultiLevelIndex distinguishes add() (= non-last) from addLast()
	// (= force-cascade), and we don't know mid-stream which flushBlock
	// is the final one. By deferring, the next flushBlock promotes the
	// previous deferral as non-last, and Close promotes the deferral
	// as last.
	pendingLeaf *index.IndexEntry

	// Whole-file metadata.
	firstKey *Key
	cfCounts map[string]int64

	// blockmeta accumulators. Empty when no builders configured. Per-
	// block snapshots are appended to blockOverlays at every data-block
	// flush; the assembled BlockMeta is written as the RFile.blockmeta
	// meta-block at Close.
	bmBuilders    []blockmeta.OverlayBuilder
	blockOverlays [][]blockmeta.Overlay

	// adjBuilder, when non-nil, accumulates out-edge cells (cells whose
	// column family equals opts.AdjacencyEdgeCF) across the whole file
	// and emits the shoal.adjacency CSR meta-block at Close. nil when
	// no edge CF was configured — stock/legacy behavior.
	adjBuilder *adjacency.Builder

	closed bool
}

// WriterOptions tunes Writer construction.
type WriterOptions struct {
	// Codec is the compression algorithm used for every data block.
	// Empty defaults to "none". Must be registered in the Writer's
	// compressor (see Compressor option). The reader on the other side
	// will need the matching decoder registered.
	Codec string

	// BlockSize is the raw-byte size threshold at which the writer
	// flushes the current data block. Zero defaults to DefaultBlockSize.
	BlockSize int

	// IndexBlockSize is the raw-byte threshold at which a level of the
	// multi-level index is finalized as its own BCFile data block. Zero
	// defaults to DefaultIndexBlockSize. Set this small (e.g. 200) in
	// tests to force multi-level structure.
	IndexBlockSize int

	// Compressor overrides the default block.DefaultCompressor (which
	// supports "none" + "gz"). Use this to register snappy/zstd/lz4
	// encoders without forking the default registry.
	Compressor *block.Compressor

	// BlockMetaBuilders, when non-empty, enables RFile.blockmeta
	// emission. Each builder aggregates per-block state during Append
	// and produces one Overlay per block at flush time. Empty list
	// produces no meta-block (legacy / stock behavior).
	//
	// Builders are called in registration order per cell; per-block
	// overlays in the meta-block appear in the same order.
	BlockMetaBuilders []blockmeta.OverlayBuilder

	// AdjacencyEdgeCF, when non-empty, enables shoal.adjacency emission.
	// Cells whose column family equals this string are mirrored into a
	// CSR (compressed sparse row) out-edge index — a binary-searchable
	// node directory + contiguous per-node edge slices — written as the
	// shoal.adjacency meta-block at Close. The edge cells are ALSO
	// written into data blocks as usual; the index is a read accelerator,
	// not a replacement, so Scan/compaction/parity are unaffected. Empty
	// produces no adjacency block (stock behavior).
	AdjacencyEdgeCF string
}

// NewWriter constructs a Writer over out.
func NewWriter(out io.Writer, opts WriterOptions) (*Writer, error) {
	if opts.Codec == "" {
		opts.Codec = block.CodecNone
	}
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}
	if opts.IndexBlockSize <= 0 {
		opts.IndexBlockSize = DefaultIndexBlockSize
	}
	if opts.Compressor == nil {
		opts.Compressor = block.DefaultCompressor()
	}
	if !opts.Compressor.Has(opts.Codec) {
		return nil, fmt.Errorf("rfile: codec %q not registered in compressor", opts.Codec)
	}
	return &Writer{
		out:        bcfile.NewWriter(out, opts.Codec),
		comp:       opts.Compressor,
		opts:       opts,
		cfCounts:   map[string]int64{},
		bmBuilders: opts.BlockMetaBuilders,
		adjBuilder: newAdjBuilder(opts.AdjacencyEdgeCF),
	}, nil
}

// newAdjBuilder returns an adjacency.Builder for edgeCF, or nil when no
// edge CF is configured.
func newAdjBuilder(edgeCF string) *adjacency.Builder {
	if edgeCF == "" {
		return nil
	}
	return adjacency.NewBuilder([]byte(edgeCF))
}

// Append writes one cell. key and value must be in non-decreasing Key
// order across calls (caller's responsibility — the writer does NOT
// sort). value may be nil (writes a zero-length value).
//
// After writing, if the current block's accumulated raw bytes reach
// the block-size threshold, the block is flushed: compressed, written
// via bcfile.Writer, and recorded in the in-memory RFile-level index.
func (w *Writer) Append(key *Key, value []byte) error {
	if w.closed {
		return errors.New("rfile: writer closed")
	}
	if key == nil {
		return errors.New("rfile: nil key")
	}
	// On a fresh block, blockPrev=nil so relkey writes the cell uncompressed.
	if err := relkey.EncodeKey(&w.blockBuf, w.blockPrev, key, value); err != nil {
		return fmt.Errorf("rfile.Append: relkey encode: %w", err)
	}
	keyClone := key.Clone()
	if w.firstKey == nil {
		w.firstKey = keyClone
	}
	w.blockPrev = keyClone
	w.blockLast = keyClone
	w.blockEntries++
	w.cfCounts[string(key.ColumnFamily)]++

	// Feed every blockmeta builder for this cell. Builders see the
	// caller-supplied slices directly — they MUST NOT retain them
	// (documented on the OverlayBuilder interface).
	for _, b := range w.bmBuilders {
		b.AppendCell(key.Row, key.ColumnFamily, key.ColumnQualifier, key.ColumnVisibility, key.Timestamp, value)
	}

	// Mirror edge cells into the adjacency CSR builder. Add copies the
	// slices, so it's safe even though key aliases caller buffers.
	if w.adjBuilder != nil && bytes.Equal(key.ColumnFamily, w.adjBuilder.EdgeCF()) {
		w.adjBuilder.Add(key.Row, key.ColumnQualifier, value, key.ColumnVisibility, key.Timestamp, key.Deleted)
	}

	if w.blockBuf.Len() >= w.opts.BlockSize {
		if err := w.flushDataBlock(); err != nil {
			return err
		}
	}
	return nil
}

// flushDataBlock compresses the current data block, appends it to the
// BCFile, and produces an IndexEntry for it. The new IndexEntry becomes
// the deferred pendingLeaf; any previously-deferred leaf is now
// confirmed non-last and pushed into the multi-level index.
//
// No-op if the current block is empty.
func (w *Writer) flushDataBlock() error {
	if w.blockBuf.Len() == 0 {
		return nil
	}
	raw := w.blockBuf.Bytes()
	rawSize := int64(len(raw))
	compressed, err := w.comp.Encode(raw, w.opts.Codec)
	if err != nil {
		return fmt.Errorf("rfile.flushDataBlock: compress (codec=%s): %w", w.opts.Codec, err)
	}
	region, err := w.out.AppendDataBlock(compressed, rawSize)
	if err != nil {
		return fmt.Errorf("rfile.flushDataBlock: BCFile append: %w", err)
	}
	leaf := &index.IndexEntry{
		Key:            w.blockLast,
		NumEntries:     w.blockEntries,
		Offset:         region.Offset,
		CompressedSize: region.CompressedSize,
		RawSize:        region.RawSize,
	}

	// Snapshot every blockmeta builder for the just-flushed block.
	// nil-payload Snapshot results suppress emission for that builder
	// on this block (matches the Empty case for zone maps). Reset for
	// the next block.
	if len(w.bmBuilders) > 0 {
		var overlays []blockmeta.Overlay
		for _, b := range w.bmBuilders {
			payload := b.Snapshot()
			if payload != nil {
				overlays = append(overlays, blockmeta.Overlay{
					Type:    b.OverlayType(),
					Payload: payload,
				})
			}
			b.Reset()
		}
		w.blockOverlays = append(w.blockOverlays, overlays)
	}

	// Promote the previously-deferred leaf as non-last (a newer leaf has
	// arrived, so the previous one is definitely not the final block).
	if w.pendingLeaf != nil {
		if err := w.indexAdd(w.pendingLeaf, false); err != nil {
			return err
		}
	}
	w.pendingLeaf = leaf

	// Reset block state for the next block.
	w.blockBuf.Reset()
	w.blockPrev = nil
	w.blockLast = nil
	w.blockEntries = 0
	return nil
}

// Close flushes the final block (if any), promotes the deferred final
// leaf as last (cascading through the multi-level index), serializes
// the RFile.index meta block, then finalizes the BCFile. Idempotent.
//
// After Close, callers must close the underlying io.Writer themselves
// (bcfile.Writer doesn't own it).
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.flushDataBlock(); err != nil {
		return err
	}
	if w.pendingLeaf != nil {
		if err := w.indexAdd(w.pendingLeaf, true); err != nil {
			return err
		}
		w.pendingLeaf = nil
	}

	root, err := w.buildRoot()
	if err != nil {
		return fmt.Errorf("rfile.Close: build root index: %w", err)
	}
	lg := &index.LocalityGroup{
		IsDefault:       true,
		ColumnFamilies:  w.cfCounts,
		FirstKey:        w.firstKey,
		NumTotalEntries: w.totalAdded,
		RootIndex:       root,
	}

	// Serialize the RFile.index meta block.
	var rfIdx bytes.Buffer
	if err := writeRFileIndexMeta(&rfIdx, lg); err != nil {
		return fmt.Errorf("rfile.Close: serialize RFile.index: %w", err)
	}
	rawSize := int64(rfIdx.Len())
	// V0: store the RFile.index meta block uncompressed. Java compresses
	// it with the default codec; the reader honors the per-meta-block
	// codec name in the MetaIndex either way.
	if _, err := w.out.AppendMetaBlock(IndexMetaBlockName, block.CodecNone, rfIdx.Bytes(), rawSize); err != nil {
		return fmt.Errorf("rfile.Close: register %q meta block: %w", IndexMetaBlockName, err)
	}

	// Optionally emit RFile.blockmeta. Only when the writer was
	// configured with at least one OverlayBuilder.
	if len(w.bmBuilders) > 0 {
		bm := &blockmeta.BlockMeta{
			Version:       blockmeta.V1,
			BlockOverlays: w.blockOverlays,
		}
		var bmBuf bytes.Buffer
		if err := blockmeta.Write(&bmBuf, bm); err != nil {
			return fmt.Errorf("rfile.Close: serialize %q: %w", blockmeta.MetaBlockName, err)
		}
		if _, err := w.out.AppendMetaBlock(blockmeta.MetaBlockName, block.CodecNone, bmBuf.Bytes(), int64(bmBuf.Len())); err != nil {
			return fmt.Errorf("rfile.Close: register %q meta block: %w", blockmeta.MetaBlockName, err)
		}
	}

	// Optionally emit shoal.adjacency. Only when an edge CF was
	// configured AND at least one edge cell was seen (an empty graph
	// produces no block, matching the "no index" default).
	if w.adjBuilder != nil {
		if ix := w.adjBuilder.Build(); ix != nil {
			var adjBuf bytes.Buffer
			if err := adjacency.Write(&adjBuf, ix); err != nil {
				return fmt.Errorf("rfile.Close: serialize %q: %w", adjacency.MetaBlockName, err)
			}
			compressed, err := w.comp.Encode(adjBuf.Bytes(), w.opts.Codec)
			if err != nil {
				return fmt.Errorf("rfile.Close: compress %q: %w", adjacency.MetaBlockName, err)
			}
			if _, err := w.out.AppendMetaBlock(adjacency.MetaBlockName, w.opts.Codec, compressed, int64(adjBuf.Len())); err != nil {
				return fmt.Errorf("rfile.Close: register %q meta block: %w", adjacency.MetaBlockName, err)
			}
		}
	}

	if err := w.out.Close(); err != nil {
		return fmt.Errorf("rfile.Close: BCFile close: %w", err)
	}
	return nil
}

// indexLevel accumulates IndexEntries for one level of the multi-level
// index. Mirrors Java's MultiLevelIndex.IndexBlock during construction:
// a running entryData byte buffer + parallel entryOffsets[] for the
// per-entry start positions.
type indexLevel struct {
	level       int32
	levelOffset int32 // stamped into IndexBlock.Offset on serialize

	entryData    []byte
	entryOffsets []int32
}

// addEntry appends one IndexEntry to the level's accumulators.
func (l *indexLevel) addEntry(e *index.IndexEntry) error {
	var buf bytes.Buffer
	if err := index.WriteIndexEntry(&buf, e); err != nil {
		return err
	}
	l.entryOffsets = append(l.entryOffsets, int32(len(l.entryData)))
	l.entryData = append(l.entryData, buf.Bytes()...)
	return nil
}

// size mirrors Java IndexBlock.getSize: serialized entry bytes plus the
// per-entry int32 offset table.
func (l *indexLevel) size() int {
	return len(l.entryData) + 4*len(l.entryOffsets)
}

// numEntries returns the count of accumulated entries.
func (l *indexLevel) numEntries() int { return len(l.entryOffsets) }

// toBlock snapshots the current accumulators into a fresh IndexBlock.
// Caller copies because the level may be reset and reused.
func (l *indexLevel) toBlock(hasNext bool) *index.IndexBlock {
	offsets := make([]int32, len(l.entryOffsets))
	copy(offsets, l.entryOffsets)
	data := make([]byte, len(l.entryData))
	copy(data, l.entryData)
	return &index.IndexBlock{
		Level:   l.level,
		Offset:  l.levelOffset,
		HasNext: hasNext,
		Offsets: offsets,
		Data:    data,
	}
}

// indexAdd is the external entry point for the multi-level builder. It
// mirrors Java's MultiLevelIndex.Writer.add (last=false) and addLast
// (last=true): bump totalAdded, append the leaf at level 0, then
// recursively cascade-flush starting at level 0.
func (w *Writer) indexAdd(leaf *index.IndexEntry, last bool) error {
	w.totalAdded++
	if err := w.indexAddAtLevel(0, leaf); err != nil {
		return err
	}
	return w.indexFlushLevel(0, leaf.Key, last)
}

// indexAddAtLevel materializes level on demand and appends the entry.
// New levels are stamped with levelOffset = w.totalAdded so the IndexBlock
// records the global leaf-position of the first entry.
func (w *Writer) indexAddAtLevel(level int32, e *index.IndexEntry) error {
	if int(level) == len(w.levels) {
		w.levels = append(w.levels, &indexLevel{
			level:       level,
			levelOffset: w.totalAdded - 1,
		})
	}
	return w.levels[level].addEntry(e)
}

// indexFlushLevel mirrors Java MultiLevelIndex.Writer.flush(level, lastKey, last).
//
// If last && this level is the topmost: return without writing — the
// topmost level always stays in memory, to be serialized inline as the
// RFile.index meta block's root.
//
// Else if the level is overflowing (or last is true), serialize the
// level's IndexBlock as a BCFile data block, add a non-leaf entry
// pointing at that block to level+1, recurse to flush(level+1, ...).
// Reset (or drop) the just-flushed level.
func (w *Writer) indexFlushLevel(level int32, lastKey *Key, last bool) error {
	if last && int(level) == len(w.levels)-1 {
		return nil
	}
	lvl := w.levels[level]
	if lvl == nil {
		return fmt.Errorf("rfile: cascade hit nil level %d", level)
	}
	overflow := lvl.size() > w.opts.IndexBlockSize && lvl.numEntries() > 1
	if !overflow && !last {
		return nil
	}
	if lvl.numEntries() == 0 {
		// Nothing accumulated at this level — happens at addLast time
		// when the cascade reaches a level that was just-reset by an
		// earlier mid-stream cascade. Skip and let the parent level
		// handle finalization.
		return nil
	}

	// Serialize the IndexBlock and write it as a BCFile data block.
	ib := lvl.toBlock(!last)
	var ibBuf bytes.Buffer
	if err := index.WriteIndexBlock(&ibBuf, ib); err != nil {
		return fmt.Errorf("rfile: serialize IndexBlock at level %d: %w", level, err)
	}
	rawSize := int64(ibBuf.Len())
	compressed, err := w.comp.Encode(ibBuf.Bytes(), w.opts.Codec)
	if err != nil {
		return fmt.Errorf("rfile: compress IndexBlock at level %d: %w", level, err)
	}
	region, err := w.out.AppendDataBlock(compressed, rawSize)
	if err != nil {
		return fmt.Errorf("rfile: write IndexBlock at level %d: %w", level, err)
	}

	// Push a non-leaf entry up to the parent level. Java passes 0 for
	// numEntries on non-leaf entries (the field is meaningful only for
	// leaf entries pointing at data blocks); we mirror that.
	parent := &index.IndexEntry{
		Key:            lastKey,
		NumEntries:     0,
		Offset:         region.Offset,
		CompressedSize: region.CompressedSize,
		RawSize:        region.RawSize,
	}
	if err := w.indexAddAtLevel(level+1, parent); err != nil {
		return err
	}
	if err := w.indexFlushLevel(level+1, lastKey, last); err != nil {
		return err
	}

	if last {
		w.levels[level] = nil
	} else {
		w.levels[level] = &indexLevel{
			level:       level,
			levelOffset: w.totalAdded,
		}
	}
	return nil
}

// buildRoot returns the IndexBlock that goes inline in the RFile.index
// meta block. For an empty file (no Append calls), this is a fresh
// empty level-0 block. Otherwise it's the topmost level — the one the
// cascade left in memory.
func (w *Writer) buildRoot() (*index.IndexBlock, error) {
	if len(w.levels) == 0 {
		return &index.IndexBlock{
			Level: 0, Offset: 0, HasNext: false,
		}, nil
	}
	top := w.levels[len(w.levels)-1]
	if top == nil {
		return nil, errors.New("rfile: top level finalized; multi-level cascade bug")
	}
	return top.toBlock(false), nil
}

// writeRFileIndexMeta emits the RFile.index wire form: magic + version
// + group count + N × LocalityGroup + V8 trailing flags.
//
// CRITICAL: Java's V8 reader (RFile.java:1438) calls mb.readBoolean()
// UNCONDITIONALLY for the "hasSamples" flag — without our two trailing
// false bytes, Java EOFs partway through. Confirmed via Java writer at
// RFile.java:662-703 which emits a false-bool for samples and a false-
// bool for vector index whenever those features aren't in use.
func writeRFileIndexMeta(w io.Writer, lg *index.LocalityGroup) error {
	if err := wire.WriteInt32(w, index.RIndexMagic); err != nil {
		return err
	}
	if err := wire.WriteInt32(w, index.V8); err != nil {
		return err
	}
	if err := wire.WriteInt32(w, 1); err != nil { // group count
		return err
	}
	if err := index.WriteLocalityGroup(w, lg, index.V8); err != nil {
		return err
	}
	// V8 trailer: two trailing booleans for sample-groups + vector-index.
	// Both false (we don't produce either feature in V0).
	if err := wire.WriteBool(w, false); err != nil {
		return fmt.Errorf("write hasSamples=false: %w", err)
	}
	if err := wire.WriteBool(w, false); err != nil {
		return fmt.Errorf("write hasVectorIndex=false: %w", err)
	}
	return nil
}
