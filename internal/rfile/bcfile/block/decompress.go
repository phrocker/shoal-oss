// Package block provides BCFile block decompression — both synchronous
// (single block on demand) and asynchronous (sharkbite-style readahead
// prefetcher that overlaps decompression with consumer iteration).
//
// Synchronous: Decompressor.Block(src, region, codec) → []byte.
// Asynchronous: Prefetcher.Next() pulls from a background goroutine that
// fetches+decompresses block N+1 while the consumer is still on block N.
//
// The async pattern mirrors sharkbite's LocalityGroupReader::startReadAhead
// (src/data/constructs/rfile/meta/LocalityGroupReader.cpp:234-291). Sharkbite
// uses a std::async future + condition variable handoff with depth=1; this
// package generalizes to any depth via a buffered channel — the underlying
// rendezvous is identical when depth=1.
package block

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/phrocker/shoal/internal/rfile/bcfile"
)

// CodecName values mirror the strings BCFile writes in MetaIndexEntry /
// DataIndex defaultCompression. From Hadoop's compression codec registry.
const (
	CodecNone   = "none"
	CodecGzip   = "gz"     // Hadoop's GzipCodec name in BCFile
	CodecSnappy = "snappy" // Hadoop SnappyCodec — block-framed; see snappy.go
	CodecZstd   = "zstd"   // not yet supported
	CodecLZ4    = "lz4"    // not yet supported
)

// ErrUnsupportedCodec is returned when a BCFile uses a codec this package
// doesn't yet decompress. Wraps the codec name for diagnostics.
var ErrUnsupportedCodec = errors.New("block: unsupported codec")

// ErrSizeMismatch indicates the codec produced a different number of
// bytes than BCFile's BlockRegion.RawSize advertised. This is structural
// corruption (or a codec-name lie) — surface loud, don't fall through.
var ErrSizeMismatch = errors.New("block: decompressed size mismatch")

// CodecFunc decompresses one BCFile block. Inputs:
//   - compressed: the raw on-disk bytes (region.CompressedSize long)
//   - rawSize: expected uncompressed size from the BlockRegion
//
// Output: rawSize-byte slice. Implementations MUST validate that the
// decompressed length matches rawSize and return ErrSizeMismatch otherwise.
type CodecFunc func(compressed []byte, rawSize int64) ([]byte, error)

// Decompressor dispatches BCFile block reads through registered codecs.
// Default registers "none" + "gz" — extend via Register for snappy / zstd
// once those wire formats are settled.
type Decompressor struct {
	mu     sync.RWMutex
	codecs map[string]CodecFunc
}

// NewDecompressor constructs a Decompressor with no codecs registered.
// Most callers want Default().
func NewDecompressor() *Decompressor {
	return &Decompressor{codecs: map[string]CodecFunc{}}
}

// Default returns a Decompressor wired for the codecs we support today:
// "none", "gz" (zlib via DefaultCodec), and "snappy" (Hadoop block-framed
// snappy via BlockDecompressorStream). zstd/lz4 are still unregistered
// pending real-cluster confirmation that they're in use.
func Default() *Decompressor {
	d := NewDecompressor()
	d.Register(CodecNone, decompressNone)
	d.Register(CodecGzip, decompressGzip)
	d.Register(CodecSnappy, decompressSnappy)
	return d
}

// Register installs a codec. Concurrent-safe; replaces any prior entry
// for the same name. Returns the receiver for fluent chains.
func (d *Decompressor) Register(name string, fn CodecFunc) *Decompressor {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.codecs[name] = fn
	return d
}

// Has reports whether the named codec is registered.
func (d *Decompressor) Has(name string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.codecs[name]
	return ok
}

// Block reads the bytes at region from src, dispatches to the codec named
// by `codec`, and returns the decompressed payload (length == region.RawSize).
//
// Concurrency: the read against `src` and the decompression itself are
// independent — multiple goroutines may call Block concurrently against
// the same Decompressor + io.ReaderAt. That's the substrate the prefetcher
// relies on.
func (d *Decompressor) Block(src io.ReaderAt, region bcfile.BlockRegion, codec string) ([]byte, error) {
	if region.CompressedSize < 0 {
		return nil, fmt.Errorf("block: negative CompressedSize %d", region.CompressedSize)
	}
	if region.RawSize < 0 {
		return nil, fmt.Errorf("block: negative RawSize %d", region.RawSize)
	}
	d.mu.RLock()
	fn, ok := d.codecs[codec]
	d.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedCodec, codec)
	}
	compressed := make([]byte, region.CompressedSize)
	if region.CompressedSize > 0 {
		if _, err := src.ReadAt(compressed, region.Offset); err != nil {
			return nil, fmt.Errorf("block: read compressed bytes @ %d: %w", region.Offset, err)
		}
	}
	return fn(compressed, region.RawSize)
}

// decompressNone is the identity codec: input == output, no transformation.
// BCFile uses "none" for blocks where compression would inflate the data.
func decompressNone(compressed []byte, rawSize int64) ([]byte, error) {
	if int64(len(compressed)) != rawSize {
		return nil, fmt.Errorf("%w: codec=none compressed=%d rawSize=%d",
			ErrSizeMismatch, len(compressed), rawSize)
	}
	out := make([]byte, len(compressed))
	copy(out, compressed)
	return out, nil
}

// decompressGzip handles BCFile's "gz" codec. The codec name is
// misleading — Hadoop's CompressionAlgorithmConfiguration.Gz returns
// "org.apache.hadoop.io.compress.DefaultCodec", which is the zlib
// (deflate-with-zlib-header) format produced by java.util.zip.Deflater
// with default nowrap=false. So "gz" on disk is NOT RFC 1952 gzip — it's
// RFC 1950 zlib. Decompress with Go's compress/zlib, NOT compress/gzip.
//
// Caught this the hard way: our gzip+gzip pair self-roundtripped fine
// but Java's PrintInfo --dump errored with "incorrect header check"
// trying to inflate our gzip-headered stream as zlib.
func decompressGzip(compressed []byte, rawSize int64) ([]byte, error) {
	zr, err := zlib.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("block: zlib header: %w", err)
	}
	defer zr.Close()
	// Pre-size the buffer so the codec doesn't append-grow. If rawSize is
	// honest this is a single allocation.
	out := make([]byte, 0, rawSize)
	buf := bytes.NewBuffer(out)
	if _, err := io.Copy(buf, zr); err != nil {
		return nil, fmt.Errorf("block: zlib body: %w", err)
	}
	got := buf.Bytes()
	if int64(len(got)) != rawSize {
		return nil, fmt.Errorf("%w: codec=gz got=%d rawSize=%d",
			ErrSizeMismatch, len(got), rawSize)
	}
	return got, nil
}
