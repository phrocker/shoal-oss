package promotion

import (
	"context"

	"github.com/phrocker/shoal/accumulo"
	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage"
)

// Promoter is the Accumulo-side capability Promote needs: reconciling a
// destination table's splits ahead of a multi-tablet export, then
// submitting the staged bulk directory as a Bulk Import V2 FATE
// operation. *accumulo.Connector satisfies this.
type Promoter interface {
	// AddTableSplits reconciles tableName's destination splits through
	// Accumulo's manager TABLE_SPLIT FATE operation (never a direct
	// metadata/ZooKeeper edit), so a subsequent BulkImport carrying a
	// widened multi-tablet load mapping (see RequiredDestinationSplits)
	// can pass Accumulo's own server-side load-mapping validation.
	AddTableSplits(ctx context.Context, tableName string, splits [][]byte) error
	// BulkImport submits bulkDir as a Bulk Import V2 (TABLE_BULK_IMPORT2)
	// FATE operation against tableName.
	BulkImport(ctx context.Context, tableName, bulkDir string, opts accumulo.BulkImportOptions) error
}

// Options controls a Promote call.
type Options struct {
	// SetTime mirrors accumulo.BulkImportOptions.SetTime: when true,
	// Accumulo assigns each imported entry's time at import time instead of
	// trusting timestamps already present in the RFiles.
	SetTime bool
}

// Promote reconciles manifest's required destination splits, stages its
// already-exported RFiles into a flat Accumulo bulk directory (computing
// and writing the Bulk Import V2 load mapping; see StageBulkDir and
// BuildLoadMapping), and then submits the bulk import through conn — in
// that order, each step failing closed before the next can run.
//
// For a multi-tablet manifest, Promote first calls
// RequiredDestinationSplits (a pure, network-free check) and, if it
// reports any rows, submits them through conn.AddTableSplits before doing
// anything else. That reconciles the destination's real tablet
// boundaries to the exact rows BuildLoadMapping's widened KeyExtents
// reference, which Accumulo's own server-side
// PrepBulkImport.validateLoadMapping check otherwise requires to already
// exist. A single-tablet (or legacy) manifest requires no splits at all,
// so AddTableSplits is never called for one. Every Accumulo-facing step —
// AddTableSplits and BulkImport alike — goes exclusively through the
// manager's FATE machinery (TABLE_SPLIT and TABLE_BULK_IMPORT2
// respectively); Promote never edits tablet metadata directly, so the
// manager remains the sole authority over the promoted table's resulting
// tablet layout and file set.
//
// Reconciling splits ahead of staging narrows, but does not close, the
// window for a concurrent structural change on the destination: a merge
// racing between AddTableSplits succeeding and BulkImport's own
// server-side validation running could still remove a split Promote just
// added. Accumulo's own defense against that is to reject the bulk
// import cleanly (a "concurrent merge" style failure), not to corrupt
// data, so this residual race is a safe-failure gap, not a correctness
// one — see docs/promotion.md §5.
//
// An empty load mapping (manifest.RFiles has nothing to import) stages an
// empty bulk directory but skips the BulkImport FATE call entirely:
// submitting a zero-file bulk import is a needless round trip to the
// manager for a no-op. Splits are still reconciled first if the manifest
// declares a multi-tablet chain, even when every tablet in it ends up
// empty, so a caller inspecting the destination afterwards sees the same
// tablet boundaries regardless of which tablets happened to carry files.
//
// Promote validates tableName and bulkDir, and the manifest's tablet
// chain (via RequiredDestinationSplits), before making any Accumulo call
// or writing anything to dst, so a malformed manifest or invalid
// destination never adds a split, stages a file, or submits a bulk
// import before the call fails.
//
// Promote does not itself retry on failure, and retry safety differs by
// which step failed. A failure in validation, AddTableSplits, or
// StageBulkDir (before BulkImport is ever called) is always safe to
// retry: split reconciliation is idempotent (AddTableSplits treats a row
// that already is a tablet's end row as already satisfied, refreshing
// only its mergeability metadata) and staging is deterministic and
// copy-based, so calling Promote again with the same arguments
// reproduces the same destination splits, staged bytes, and
// loadmap.json. Once BulkImport has been invoked, a blind retry is not
// always safe: FATE submission has no built-in dedup/idempotency (see
// docs/promotion.md §5), so an ambiguous failure there — for example a
// timeout after the manager received the request but before the caller
// observed a response — leaves the caller unable to tell whether the
// bulk import already happened, and resubmitting risks a duplicate
// import.
func Promote(
	ctx context.Context,
	src storage.Backend,
	manifest *engine.RFileExportManifest,
	dst storage.Backend,
	bulkDir string,
	conn Promoter,
	tableName string,
	opts Options,
) (LoadMapping, error) {
	if err := validatePromotionDestination(dst, tableName, bulkDir); err != nil {
		return nil, err
	}
	splits, err := RequiredDestinationSplits(manifest)
	if err != nil {
		return nil, err
	}
	if len(splits) > 0 {
		if err := conn.AddTableSplits(ctx, tableName, splits); err != nil {
			return nil, err
		}
	}
	mapping, err := StageBulkDir(ctx, src, manifest, dst, bulkDir)
	if err != nil {
		return nil, err
	}
	if len(mapping) == 0 {
		return mapping, nil
	}
	if err := conn.BulkImport(ctx, tableName, bulkDir, accumulo.BulkImportOptions{SetTime: opts.SetTime}); err != nil {
		return nil, err
	}
	return mapping, nil
}
