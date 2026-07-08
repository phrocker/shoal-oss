package scanserver

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletscan"
	"github.com/phrocker/shoal/internal/visfilter"
)

// StartMultiScan implements the BatchScanner-shaped server-side path.
// Single-shot: every (tablet, ranges) pair in the ScanBatch is scanned
// to completion (or until the global byte budget is hit), results are
// concatenated into one MultiScanResult, and ContinueMultiScan signals
// exhausted.
//
// Compared to StartScan:
//   - Caller hands in a ScanBatch (map TKeyExtent → []TRange) instead of
//     a single (extent, range) pair.
//   - Per-tablet range lists are normalized (sort + merge overlaps),
//     then driven through scanTabletRanges.
//   - The remaining budget shrinks across tablets — once we've used 4MB,
//     we stop and report MultiScanResult.More=true so the caller knows
//     it didn't get everything. (V0.5 doesn't issue continuation tokens
//     for "resume from key X"; the Java SDK gets back what shoal could
//     fit and falls back to tserver for the rest if needed.)
//
// Failures and partial-scan handling: V0.5 treats per-tablet failures as
// fatal — we return an error to the Thrift call site rather than try to
// fill MultiScanResult.Failures. Future hardening can downgrade to
// per-tablet recoverable failures so a single tablet miss doesn't fail
// the whole batch.
func (s *Server) StartMultiScan(
	ctx context.Context,
	tinfo *client.TInfo,
	credentials *security.TCredentials,
	batch data.ScanBatch,
	columns []*data.TColumn,
	ssiList []*data.IterInfo,
	ssio map[string]map[string]string,
	authorizations [][]byte,
	waitForWrites bool,
	samplerConfig *tabletscan.TSamplerConfiguration,
	batchTimeOut int64,
	classLoaderContext string,
	executionHints map[string]string,
	busyTimeout int64,
) (*data.InitialMultiScan, error) {
	t0 := time.Now()
	if len(batch) == 0 {
		return &data.InitialMultiScan{
			ScanID:  0,
			Result_: &data.MultiScanResult_{Results: nil, More: false},
		}, nil
	}

	auths := visfilter.NewAuthorizations(authorizations...)
	ev := visfilter.NewEvaluator(auths)

	allResults := make([]*data.TKeyValue, 0, 64)
	totalBytes := 0
	tabletsScanned := 0
	truncated := false

	// Iterator post-processor is built ONCE per multiscan but applied
	// per-tablet. Each tablet gets its own (top-K) result; the Java
	// client merges across tablets to form the global top-K. To avoid
	// shared state we instantiate a fresh post-processor per tablet
	// inside the loop.
	hasIterator := len(ssiList) > 0

	// V0.5 simplification: when the caller hands in a single TKeyExtent
	// that's table-id-only (no EndRow / no PrevEndRow), we fan out the
	// ranges across our own tablet map. This lets the Java SDK skip
	// client-side binning entirely — it sends "all ranges for table X"
	// and shoal does the per-row routing internally. Drops the need for
	// a TabletLocator on the SDK side, which is the whole point of
	// going around the AccumuloClient's ScanServerSelector.
	expanded := batch
	if len(batch) == 1 {
		for extent, ranges := range batch {
			if extent != nil && extent.EndRow == nil && extent.PrevEndRow == nil && len(ranges) > 0 {
				binned, berr := s.binRangesByTablet(ctx, extent, ranges)
				if berr != nil {
					return nil, fmt.Errorf("multiscan bin ranges (table=%s): %w", string(extent.Table), berr)
				}
				expanded = binned
			}
		}
	}

	for extent, ranges := range expanded {
		if extent == nil || len(ranges) == 0 {
			continue
		}
		// Use the same per-extent file lookup as StartScan. The first
		// range hands lookupFiles a sample for the auto-locate path
		// when the caller didn't pre-resolve the extent.
		files, err := s.lookupFiles(ctx, extent, ranges[0])
		if err != nil {
			return nil, fmt.Errorf("multiscan lookup files (table=%s): %w", string(extent.Table), err)
		}

		remaining := MaxResultBytes - totalBytes
		if remaining <= 0 {
			truncated = true
			break
		}
		var perTabletProc cellPostProcessor
		if hasIterator {
			pp, perr := buildPostProcessor(ssiList, ssio)
			if perr != nil {
				return nil, fmt.Errorf("multiscan: build iterator: %w", perr)
			}
			perTabletProc = pp
		}
		results, used, perTabletTrunc, err := s.scanTabletRanges(
			ctx, files, ranges, columns, ev, remaining, perTabletProc,
		)
		if err != nil {
			return nil, fmt.Errorf("multiscan tablet (table=%s): %w", string(extent.Table), err)
		}
		allResults = append(allResults, results...)
		totalBytes += used
		tabletsScanned++
		if perTabletTrunc {
			truncated = true
			break
		}
	}

	s.logger.LogAttrs(ctx, slog.LevelInfo, "multiscan complete",
		slog.Int("tablets_in_batch", len(batch)),
		slog.Int("tablets_scanned", tabletsScanned),
		slog.Int("cells_returned", len(allResults)),
		slog.Int("approx_bytes", totalBytes),
		slog.Bool("truncated", truncated),
		slog.Duration("dur", time.Since(t0)),
		slog.Int("vis_cache", ev.CacheSize()),
	)

	return &data.InitialMultiScan{
		ScanID: 0,
		Result_: &data.MultiScanResult_{
			Results: allResults,
			More:    truncated,
		},
	}, nil
}

// binRangesByTablet groups single-row-style ranges by the tablet that
// contains the range start. Each output extent is a fully-resolved
// TKeyExtent (table + endRow + prevEndRow) so downstream lookupFiles
// finds it directly.
//
// V0.5 limitation: a range whose start lands in tablet T is assigned
// entirely to T, even if its stop crosses T's boundary. For the chat
// path's exact-row lookups (Range.exact(vertexId)) this is always
// correct. Multi-row ranges that span tablets would lose cells past
// the boundary — explicitly rejected with an error in that case so
// the Java SDK can fall back to tserver instead of silently truncating.
func (s *Server) binRangesByTablet(ctx context.Context, extent *data.TKeyExtent, ranges []*data.TRange) (data.ScanBatch, error) {
	tableID := string(extent.Table)
	tablets, err := s.locator.LocateTable(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("locate table %q: %w", tableID, err)
	}

	out := make(data.ScanBatch, len(tablets))
	for _, r := range ranges {
		if r == nil {
			continue
		}
		if r.InfiniteStartKey || r.Start == nil || len(r.Start.Row) == 0 {
			return nil, fmt.Errorf("multiscan binner: range with no start row not supported in auto-bin mode")
		}
		startRow := r.Start.Row
		// Check stop boundary: must NOT cross out of the start tablet.
		// For Range.exact(row) the stop is row+0x00 — same tablet. For
		// arbitrary multi-row ranges we error out and let the caller
		// fan out via tserver.
		var matched *data.TKeyExtent
		for _, t := range tablets {
			if rowInTablet(startRow, t) {
				if !r.InfiniteStopKey && r.Stop != nil && len(r.Stop.Row) > 0 {
					stopRow := r.Stop.Row
					// Stop = row+0x00 lands inside tablet T iff startRow
					// is inside T. For other shapes, validate that the
					// stop row is also inside T (lexicographically <=
					// EndRow when EndRow is non-nil).
					if t.EndRow != nil && string(stopRow) > string(t.EndRow) &&
						!(len(stopRow) == len(startRow)+1 && stopRow[len(stopRow)-1] == 0) {
						return nil, fmt.Errorf("multiscan binner: range [%q, %q) crosses tablet boundary at %q — caller must pre-bin",
							string(startRow), string(stopRow), string(t.EndRow))
					}
				}
				matched = &data.TKeyExtent{
					Table:      extent.Table,
					EndRow:     t.EndRow,
					PrevEndRow: t.PrevRow,
				}
				break
			}
		}
		if matched == nil {
			return nil, fmt.Errorf("multiscan binner: no tablet covers row %q in table %q",
				string(startRow), tableID)
		}
		// Group by extent identity. data.ScanBatch's key is *TKeyExtent
		// (pointer identity) — we want value-equality. Build a string
		// key for grouping, dedupe, then assemble the final map.
		out = appendToScanBatch(out, matched, r)
	}
	return out, nil
}

// appendToScanBatch ensures we group ranges under one extent value
// (TKeyExtent has no usable hash key as a *pointer*, so value-compare).
func appendToScanBatch(b data.ScanBatch, extent *data.TKeyExtent, r *data.TRange) data.ScanBatch {
	for k, v := range b {
		if k != nil && extentEqual(k, extent) {
			b[k] = append(v, r)
			return b
		}
	}
	b[extent] = []*data.TRange{r}
	return b
}

func extentEqual(a, b *data.TKeyExtent) bool {
	return string(a.Table) == string(b.Table) &&
		string(a.EndRow) == string(b.EndRow) &&
		string(a.PrevEndRow) == string(b.PrevEndRow)
}
