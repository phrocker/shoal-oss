// Package scanserver implements the Thrift TabletScanClientService
// server surface — single-shot StartScan only. ContinueScan returns
// empty results; CloseScan is a no-op; StartMultiScan returns an
// "unsupported" error. Per the V0 spec, every scan() request is
// self-contained — no scanID state is retained across calls.
//
// One Server instance per shoal pod. Holds:
//   - metadata.Walker — does the ZK + tablet-location lookups
//   - cache.LocatorCache — caches tablet→location maps
//   - cache.BlockCache — caches decompressed RFile blocks
//   - storage.Backend — fetches RFile bytes from GCS
//   - block.Decompressor / Compressor — codec registry
//
// Everything is concurrent-safe; multiple Thrift goroutines share the
// same Server.
package scanserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/phrocker/shoal/internal/cache"
	"github.com/phrocker/shoal/internal/rfile/bcfile"
	"github.com/phrocker/shoal/internal/rfile/bcfile/block"
	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletscan"
)

// Compile-time check that Server satisfies the generated Thrift
// service interface.
var _ tabletscan.TabletScanClientService = (*Server)(nil)

// Server holds the long-lived state needed to serve scans.
type Server struct {
	locator cache.TableLocator
	blocks  *cache.BlockCache
	storage storage.Backend
	dec     *block.Decompressor
	logger  *slog.Logger

	// File-level byte cache: GCS path → full RFile bytes. Avoids
	// re-pulling the same 30MB+ file on every scan against the same
	// tablet — critical for shoal winning hedge races. Pruned by total
	// bytes (default cap matches block cache; configurable via
	// Options.FileBytesCap). Pre-flight: nil cache disables; default
	// 1GB if unset.
	files *fileCache

	// Per-scan visibility filtering uses the auth list directly. No
	// shared evaluator across scans because auths differ per request.
	// (Construct a fresh Evaluator per scan; relies on the per-CV-bytes
	// cache being warm WITHIN a scan; cross-scan reuse isn't safe
	// because auths differ.)

	// walOpener opens one QWAL segment as an entry stream for the
	// opt-in WAL-merged read path (walscan.go). nil disables the WAL
	// route: a route-hinted scan against a tablet with log: entries
	// errors rather than silently dropping unflushed writes.
	walOpener walSegmentOpener
	// walPeerPort is appended to bare (port-less) peer hostnames when
	// resolving WAL segment replicas. Zero is fine when peers already
	// carry an explicit ":port".
	walPeerPort int
}

// Options configures a Server. Zero defaults pick sensible values where
// possible; nil-required fields error in NewServer.
type Options struct {
	// Locator does the tablet → file list lookup. *metadata.Walker
	// satisfies this; tests stub it.
	Locator cache.TableLocator
	// BlockCache (optional). nil disables caching.
	BlockCache *cache.BlockCache
	// Storage opens RFile bytes by path. Required.
	Storage      storage.Backend
	Decompressor *block.Decompressor
	Logger       *slog.Logger

	// FileBytesCap is the byte budget for the file cache (full RFile
	// bytes by GCS path). Zero defaults to 1GB. Negative disables.
	// Tserver wins hedge races primarily because of warm caches; this
	// cache closes the gap so warm shoal scans skip the GCS round-trip.
	FileBytesCap int64

	// WALPeerPort is appended to bare peer hostnames when resolving WAL
	// segment replicas for the opt-in WAL-merged read path. Zero is fine
	// when recorded peers already carry an explicit ":port".
	WALPeerPort int
}

// NewServer constructs a scan Server. Returns an error if any of the
// non-defaultable dependencies (Locator, Storage) are nil.
func NewServer(opts Options) (*Server, error) {
	if opts.Locator == nil {
		return nil, errors.New("scanserver: Locator is required")
	}
	if opts.Storage == nil {
		return nil, errors.New("scanserver: Storage is required")
	}
	dec := opts.Decompressor
	if dec == nil {
		dec = block.Default()
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	fileCap := opts.FileBytesCap
	if fileCap == 0 {
		fileCap = 1 << 30 // 1 GB
	}
	var fc *fileCache
	if fileCap > 0 {
		fc = newFileCache(fileCap)
	}
	return &Server{
		locator:     opts.Locator,
		blocks:      opts.BlockCache,
		storage:     opts.Storage,
		dec:         dec,
		logger:      logger,
		files:       fc,
		walPeerPort: opts.WALPeerPort,
	}, nil
}

// ContinueScan: V0 returns an empty result with More=false. shoal
// completes every scan in StartScan; clients that follow up with
// ContinueScan get an immediate "no more" signal.
func (s *Server) ContinueScan(ctx context.Context, tinfo *client.TInfo, scanID data.ScanID, busyTimeout int64) (*data.ScanResult_, error) {
	return &data.ScanResult_{Results: nil, More: false}, nil
}

// CloseScan: no-op. We hold no per-scan state.
func (s *Server) CloseScan(ctx context.Context, tinfo *client.TInfo, scanID data.ScanID) error {
	return nil
}

// StartMultiScan implements the BatchScanner-shaped server-side path.
// Single-shot: every tablet's range list is scanned to completion or
// to the global byte budget, results are concatenated into one
// MultiScanResult, and ContinueMultiScan signals exhausted.
//
// Implementation: handled in multiscan_handler.go.

// ContinueMultiScan: shoal completes every multi-scan in one shot, so
// any follow-up call gets an immediate "no more" signal.
func (s *Server) ContinueMultiScan(ctx context.Context, tinfo *client.TInfo, scanID data.ScanID, busyTimeout int64) (*data.MultiScanResult_, error) {
	return &data.MultiScanResult_{Results: nil, More: false}, nil
}

// CloseMultiScan: no-op.
func (s *Server) CloseMultiScan(ctx context.Context, tinfo *client.TInfo, scanID data.ScanID) error {
	return nil
}

// GetActiveScans: V0 holds no per-scan state, so there are no active
// scans to report. Java callers (e.g., monitoring) get an empty list.
func (s *Server) GetActiveScans(ctx context.Context, tinfo *client.TInfo, credentials *security.TCredentials) ([]*tabletscan.ActiveScan, error) {
	return nil, nil
}

// openRFile fetches one RFile via the storage backend and parses its
// header. Returns a bcfile.Reader sitting on a memory-resident copy of
// the bytes. File-cache aware: warm path returns cached bytes + a
// fresh bcfile.Reader over them in microseconds, skipping the GCS
// round-trip entirely.
//
// We pull the entire RFile into memory rather than streaming because:
//   - Typical Accumulo RFiles are 10s of MB to ~100MB — fits comfortably.
//   - The compressed-block cache holds DECOMPRESSED blocks; the BCFile
//     reader still needs ReaderAt access to the COMPRESSED bytes for
//     blocks not yet decompressed.
//   - GCS Range reads have high per-request latency; one big pull is
//     dramatically cheaper than many small ones for a hot file.
func (s *Server) openRFile(ctx context.Context, gsPath string) (*bcfile.Reader, []byte, error) {
	if buf, ok := s.files.Get(gsPath); ok {
		bc, err := bcfile.NewReader(byteReaderAt(buf), int64(len(buf)))
		if err != nil {
			return nil, nil, fmt.Errorf("parse BCFile %q (cached): %w", gsPath, err)
		}
		return bc, buf, nil
	}
	f, err := s.storage.Open(ctx, gsPath)
	if err != nil {
		return nil, nil, fmt.Errorf("storage.Open %q: %w", gsPath, err)
	}
	defer f.Close()
	size := f.Size()
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return nil, nil, fmt.Errorf("read %q (%d bytes): %w", gsPath, size, err)
	}
	bc, err := bcfile.NewReader(byteReaderAt(buf), size)
	if err != nil {
		return nil, nil, fmt.Errorf("parse BCFile %q: %w", gsPath, err)
	}
	s.files.Put(gsPath, buf)
	return bc, buf, nil
}

// byteReaderAt is the standard adapter from []byte → io.ReaderAt.
type byteReaderAt []byte

func (b byteReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, errors.New("byteReaderAt: offset past end")
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, errors.New("byteReaderAt: short read")
	}
	return n, nil
}
