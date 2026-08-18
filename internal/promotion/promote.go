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

	// DestinationTablets, when non-empty, is the destination table's real,
	// current tablets (e.g. from internal/metadata.Walker.LocateTable).
	// Promote validates the manifest's derived load mapping against them
	// (see ValidateAgainstDestination) before staging, so a destination
	// that isn't already split at the source table's boundaries fails
	// fast with an actionable local error instead of failing deep inside
	// Accumulo's FATE machinery (BULK_CONCURRENT_MERGE) after RFiles have
	// already been staged. Nil/empty skips this check entirely — Promote's
	// prior, unchecked behavior — since a caller with no destination
	// lookup available still needs to be able to promote.
	DestinationTablets []KeyExtent
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
// resulting state. PrepBulkImport validates the requested KeyExtents
// against the destination table's current splits — it does not create or
// reconcile them (see internal/promotion package doc's "Known limitation"
// paragraph and docs/promotion.md §3) — and rejects the whole FATE
// operation if they don't already match. Promote does not create, assume,
// or race any competing authority over that decision; it only performs
// the same check locally, as an optional early failure, when
// Options.DestinationTablets is supplied.
//
// An empty load mapping (manifest.RFiles has nothing to import, or every
// tablet's files were already covered by an earlier promotion of the same
// manifest) stages an empty bulk directory but skips the BulkImport FATE
// call entirely: submitting a zero-file bulk import is a needless
// round trip to the manager for a no-op.
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
	if len(opts.DestinationTablets) > 0 {
		preflight, err := BuildLoadMapping(manifest)
		if err != nil {
			return nil, err
		}
		if err := ValidateAgainstDestination(preflight, opts.DestinationTablets); err != nil {
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
