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
// bulk directory (computing and writing the Bulk Import V2 load mapping
// from the manifest's own tablet partitioning; see StageBulkDir) and then
// submits the bulk import through conn.
//
// Submission is the only step that talks to Accumulo, and it goes
// exclusively through the manager's FATE machinery
// (TABLE_BULK_IMPORT2) — Promote never edits tablet metadata directly, so
// the manager remains the sole authority over the promoted table's
// resulting state. Accumulo's own FATE machinery decides how to reconcile
// the requested KeyExtents against the destination table's current splits;
// Promote does not create, assume, or race any competing authority over
// that decision.
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
	mapping, err := StageBulkDir(ctx, src, manifest, dst, bulkDir)
	if err != nil {
		return nil, err
	}
	if err := conn.BulkImport(ctx, tableName, bulkDir, accumulo.BulkImportOptions{SetTime: opts.SetTime}); err != nil {
		return nil, err
	}
	return mapping, nil
}
