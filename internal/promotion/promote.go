package promotion

import (
	"context"

	"github.com/phrocker/shoal/accumulo"
	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage"
)

// BulkImporter is the Accumulo-side capability Promote needs: submitting a
// staged bulk directory as a Bulk Import V2 FATE operation.
// *accumulo.Connector satisfies this.
type BulkImporter interface {
	BulkImport(ctx context.Context, tableName, bulkDir string, opts accumulo.BulkImportOptions) error
}

// Options controls a Promote call.
type Options struct {
	// SetTime mirrors accumulo.BulkImportOptions.SetTime: when true,
	// Accumulo assigns each imported entry's time at import time instead of
	// trusting timestamps already present in the RFiles.
	SetTime bool
}

// Promote stages manifest's already-exported RFiles into a flat Accumulo
// bulk directory (computing and writing the currently-supported single-
// tablet Bulk Import V2 load mapping; see StageBulkDir) and then submits
// the bulk import through conn.
//
// Submission is the only step that talks to Accumulo, and it goes
// exclusively through the manager's FATE machinery
// (TABLE_BULK_IMPORT2) — Promote never edits tablet metadata directly, so
// the manager remains the sole authority over the promoted table's
// resulting state. Promote itself currently supports only unambiguous
// single-tablet exports: BuildLoadMapping fails closed on any split-
// bearing or multi-tablet manifest before StageBulkDir can write to dst
// or BulkImport can submit a FATE operation.
//
// An empty load mapping (manifest.RFiles has nothing to import) stages an
// empty bulk directory but skips the BulkImport FATE call entirely:
// submitting a zero-file bulk import is a needless round trip to the
// manager for a no-op.
//
// Promote validates tableName and bulkDir before staging, so an invalid
// destination never writes any files or loadmap.json to dst before the
// call fails.
//
// Promote does not itself retry on failure; callers wanting resumable
// promotion can safely call Promote again with the same arguments (staging
// is idempotent — see StageBulkDir) once the underlying cause is resolved.
func Promote(
	ctx context.Context,
	src storage.Backend,
	manifest *engine.RFileExportManifest,
	dst storage.Backend,
	bulkDir string,
	conn BulkImporter,
	tableName string,
	opts Options,
) (LoadMapping, error) {
	if err := validatePromotionDestination(tableName, bulkDir); err != nil {
		return nil, err
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
