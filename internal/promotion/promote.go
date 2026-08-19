package promotion

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

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
	// ListTableSplits reports tableName's real, current bounded split
	// rows in ascending order (accumulo.Connector.ListTableSplits).
	// Promote uses it, immediately after AddTableSplits, to confirm the
	// destination has exactly the required rows and nothing else in the
	// range BuildLoadMapping's widened mapping depends on — see
	// verifyNoUnexpectedDestinationSplits.
	ListTableSplits(ctx context.Context, tableName string) ([][]byte, error)
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
// BuildLoadMapping's widening rule is only provably correct against
// Accumulo's own PrepBulkImport.validateLoadMapping walk when the
// destination's real splits, at or before the last required row, are
// exactly the required rows — no fewer (AddTableSplits already
// guarantees that) and, just as importantly, no more. An extra,
// unrelated split anywhere in that range — pre-existing or added by
// another actor between reconciliation attempts — silently changes the
// real predecessor row an earlier mapping entry leaves Accumulo's
// validation walk resting on, which the next entry's widened
// prevEndRow can then never re-match (see docs/promotion.md §3.2/§5 for
// the full trace). So immediately after AddTableSplits, Promote calls
// conn.ListTableSplits and runs verifyNoUnexpectedDestinationSplits: if
// the destination has any such extra split, Promote fails closed with a
// clear, actionable error before staging or submitting anything, rather
// than letting BuildLoadMapping construct a mapping that Accumulo's own
// validation would reject for reasons the failure would not explain. A
// trailing extra split strictly after the last required row is left
// alone by this specific check: it falls entirely inside the final,
// always-unbounded entry's own span, so it can never reproduce the
// prevEndRow mismatch described above. That is not the same as
// harmless in every sense a bulk import can fail, though: Accumulo's
// own PrepBulkImport.validateLoadMapping separately enforces
// table.bulk.max.tablets (default 100, admin-configurable per table,
// since Accumulo 2.1.0) by counting how many real destination tablets
// the final mapping entry's files overlap — a count trailing splits
// inflate directly. Enough of them can still make BulkImport reject
// the load mapping for a reason this package neither detects nor
// explains up front; Promote does not enforce that limit itself,
// since doing so accurately needs a way to read the destination
// table's actual (possibly non-default) property value, which the
// Promoter interface does not expose today — see docs/promotion.md §5
// item 3 for the full reasoning.
//
// This check narrows, but cannot close, the window for a concurrent
// structural change on the destination: a split or merge racing between
// verifyNoUnexpectedDestinationSplits succeeding and BulkImport's own
// server-side validation running could still invalidate the mapping
// Promote just staged. Accumulo's own defense against that is to reject
// the bulk import cleanly (a "concurrent merge" style failure), not to
// corrupt data, so this residual race is a safe-failure gap, not a
// correctness one — see docs/promotion.md §5.
//
// An empty load mapping (manifest.RFiles has nothing to import) stages an
// empty bulk directory but skips the BulkImport FATE call entirely:
// submitting a zero-file bulk import is a needless round trip to the
// manager for a no-op. Splits are still reconciled first if the manifest
// declares a multi-tablet chain, even when every tablet in it ends up
// empty, so a caller inspecting the destination afterwards sees the same
// tablet boundaries regardless of which tablets happened to carry files.
//
// Promote validates tableName and bulkDir, and the whole manifest --
// its tablet chain shape (via RequiredDestinationSplits), its RFiles'
// own references into that chain (via a BuildLoadMapping preflight:
// every RFile.TabletIndex must be declared, and a repeated
// DestinationPath must not disagree about which index it belongs to),
// and everything StageBulkDir itself would reject before writing a
// single byte -- including that every RFile actually exists at src and
// matches its recorded size/SHA256 (via a stagingPreflight call:
// engine.VerifyRFileExport, source-alias dedup, flattened-basename
// validity and uniqueness, and staging write-target aliasing against
// both src and dst; see stagingPreflight) -- before making any Accumulo
// call or writing anything to dst. Without the RFile-level check, a
// manifest whose tablet chain is well-formed but whose RFiles reference
// an undeclared index, or declare one DestinationPath under two
// conflicting indexes, would only be rejected later, inside
// StageBulkDir's own BuildLoadMapping call. Without the staging-level
// check, a manifest whose tablets and RFiles are otherwise well-formed
// but which references a missing or corrupt export file, flattens two
// different tablets' files to the same bulk-directory basename (or an
// invalid one), or aliases src or dst, would only be rejected later
// still, inside StageBulkDir's own verification and staging calls.
// Either gap, left unclosed, means AddTableSplits below would already
// have mutated the destination's real splits by the time a permanently
// unstageable manifest is finally caught. Running both preflights here
// first, and discarding their results, closes both gaps: a malformed
// manifest of any of these kinds, or an invalid destination, never adds
// a split, stages a file, or submits a bulk import before the call
// fails.
//
// The RFile-level and path/naming parts of these preflights are cheap
// to recompute -- StageBulkDir redoes its own equivalent chain/dedup/
// flatten/alias checks further on regardless, since every one of those
// calls is a pure or local-probe-only function of the manifest and the
// backends alone, so recomputing never observes a different
// destination state. engine.VerifyRFileExport is not cheap the same
// way -- it streams and hashes every RFile's actual bytes -- but
// Promote still lets StageBulkDir run it again below, deliberately, at
// the cost of hashing every RFile twice: AddTableSplits and
// verifyNoUnexpectedDestinationSplits/ListTableSplits, both of which
// run between this preflight and StageBulkDir, are a real manager
// round-trip taking arbitrary time, and src is not guaranteed
// immutable across it (a local path or an object-store key can be
// overwritten in place while that call is in flight). Skipping
// StageBulkDir's own verification to avoid the duplicate cost would
// leave that window unchecked: a source object replaced during
// AddTableSplits/ListTableSplits would be staged and bulk-imported
// without ever being verified against the manifest it actually matches
// at copy time (see stagingPreflight's own doc comment for the same
// reasoning from the staging side, and
// TestPromoteRejectsSourceMutatedDuringAddTableSplits for the
// regression test proving this window is closed).
//
// Promote does not itself retry on failure, and retry safety differs by
// which step failed. A failure in validation, AddTableSplits, or
// verifyNoUnexpectedDestinationSplits, or StageBulkDir (before
// BulkImport is ever called) is always safe to retry: split
// reconciliation is idempotent (AddTableSplits treats a row that
// already is a tablet's end row as already satisfied, refreshing only
// its mergeability metadata), the split-verification check is a
// read-only comparison, and staging is deterministic and copy-based, so
// calling Promote again with the same arguments reproduces the same
// destination splits, staged bytes, and loadmap.json. Once BulkImport
// has been invoked, a blind retry is not always safe: FATE submission
// has no built-in dedup/idempotency (see docs/promotion.md §5), so an
// ambiguous failure there — for example a timeout after the manager
// received the request but before the caller observed a response —
// leaves the caller unable to tell whether the bulk import already
// happened, and resubmitting risks a duplicate import.
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
	if _, err := BuildLoadMapping(manifest); err != nil {
		return nil, err
	}
	if err := stagingPreflight(ctx, src, manifest, dst, bulkDir); err != nil {
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
		if err := verifyNoUnexpectedDestinationSplits(ctx, conn, tableName, splits); err != nil {
			return nil, err
		}
	}
	// Deliberately re-verifies (see the doc comment above and
	// stagingPreflight's own): AddTableSplits and
	// verifyNoUnexpectedDestinationSplits/ListTableSplits just ran, and
	// src is not guaranteed unchanged across that round-trip.
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

// verifyNoUnexpectedDestinationSplits confirms that tableName's real,
// current split rows at or before the last entry of required are
// exactly required — no more, no fewer — and fails closed with a clear
// error otherwise.
//
// This exists because of a real, non-obvious gap in
// BuildLoadMapping's widening rule (see the trace in
// docs/promotion.md §3.2 and Promote's own doc comment above): the
// rule only produces a mapping Accumulo's own
// PrepBulkImport.validateLoadMapping walk can validate when the
// destination's splits in that range are exactly the required ones.
// AddTableSplits guarantees the required rows are present once it
// succeeds, but it does not — and, submitting through Accumulo's
// manager-owned TABLE_SPLIT FATE operation as it does, safely cannot —
// remove a pre-existing or concurrently-added split that doesn't
// belong to this promotion. Detecting that here, before StageBulkDir
// or BulkImport ever run, turns what would otherwise be a confusing
// "concurrent merge" style rejection deep inside Accumulo (or, worse,
// silence if a caller weren't watching for it) into an explicit,
// Shoal-side failure that names the offending row.
//
// required must be sorted ascending, non-empty, and free of
// duplicates, matching RequiredDestinationSplits's own contract; this
// is only ever called with that function's own output.
func verifyNoUnexpectedDestinationSplits(ctx context.Context, conn Promoter, tableName string, required [][]byte) error {
	actual, err := conn.ListTableSplits(ctx, tableName)
	if err != nil {
		return fmt.Errorf("promotion: list destination splits for %q: %w", tableName, err)
	}
	lastRequired := required[len(required)-1]
	inRange := actual
	for i, row := range actual {
		if bytes.Compare(row, lastRequired) > 0 {
			inRange = actual[:i]
			break
		}
	}
	matches := len(inRange) == len(required)
	if matches {
		for i, row := range required {
			if !bytes.Equal(inRange[i], row) {
				matches = false
				break
			}
		}
	}
	if matches {
		return nil
	}
	return fmt.Errorf(
		"promotion: destination table %q has unexpected splits at or before %q: found %s, required exactly %s; "+
			"another actor may have split or merged the destination since this promotion reconciled its splits, "+
			"which would break the widened load mapping's prevEndRow handoff (see docs/promotion.md §3.2/§5) — "+
			"resolve the destination's splits and retry",
		tableName, formatSplitRow(lastRequired), formatSplitRows(inRange), formatSplitRows(required),
	)
}

// formatSplitRow renders a single split row for an error message: a
// quoted string when row is valid UTF-8, otherwise its hex encoding.
func formatSplitRow(row []byte) string {
	if utf8.Valid(row) {
		return fmt.Sprintf("%q", row)
	}
	return fmt.Sprintf("hex:%x", row)
}

// formatSplitRows renders an ordered list of split rows for an error
// message.
func formatSplitRows(rows [][]byte) string {
	parts := make([]string, len(rows))
	for i, row := range rows {
		parts[i] = formatSplitRow(row)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
