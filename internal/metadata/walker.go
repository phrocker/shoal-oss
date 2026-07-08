package metadata

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"
	"unicode"

	"github.com/phrocker/shoal/internal/scanclient"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/zk"
)

// Walker walks the ZK → root tablet → metadata table chain to materialize
// tablet→file maps for every user table. V0: pull-based, no caching.
// Caching + exception-driven invalidation lands in internal/cache.
type Walker struct {
	locator    *zk.Locator
	creds      *security.TCredentials
	accVersion string
	logger     *slog.Logger
}

// NewWalker constructs a Walker. The supplied creds are used for every
// outbound Thrift scan; accVersion populates the AccumuloProtocol header
// (must match the server's major.minor). Uses slog.Default() for logging;
// override with WithLogger.
func NewWalker(locator *zk.Locator, creds *security.TCredentials, accVersion string) *Walker {
	return &Walker{
		locator:    locator,
		creds:      creds,
		accVersion: accVersion,
		logger:     slog.Default(),
	}
}

// WithLogger returns a copy of w that emits structured scan diagnostics
// through l. Pass slog.New(slog.NewTextHandler(io.Discard, nil)) to silence.
func (w *Walker) WithLogger(l *slog.Logger) *Walker {
	cp := *w
	if l == nil {
		l = slog.Default()
	}
	cp.logger = l
	return &cp
}

// ScanRootTablet asks ZK for the root tserver, opens a scan client to it,
// scans the root tablet's full range, and returns the metadata table's
// tablets (with locations + files).
//
// Returns an error wrapping zk.RootTabletLocation's nil case as a retry-
// able state — caller decides whether to back off and retry.
func (w *Walker) ScanRootTablet(ctx context.Context) ([]TabletInfo, error) {
	loc, err := w.locator.RootTabletLocation(ctx)
	if err != nil {
		return nil, fmt.Errorf("locate root tablet: %w", err)
	}
	if loc == nil {
		return nil, fmt.Errorf("root tablet has no current location (likely mid-move; retry)")
	}
	return w.scanTablet(ctx, loc.HostPort, rootTabletExtent())
}

// ScanMetadataTablet opens a scan client to the tserver hosting mdTablet
// (which must be a metadata-table tablet — caller's responsibility) and
// scans it. Returns the user-table tablets it contains.
func (w *Walker) ScanMetadataTablet(ctx context.Context, mdTablet TabletInfo) ([]TabletInfo, error) {
	if mdTablet.TableID != MetadataTableID {
		return nil, fmt.Errorf("expected metadata-table tablet (id %q), got %q", MetadataTableID, mdTablet.TableID)
	}
	if mdTablet.Location == nil {
		return nil, fmt.Errorf("metadata tablet has no current location (likely mid-move; retry)")
	}
	return w.scanTablet(ctx, mdTablet.Location.HostPort, tabletExtent(mdTablet))
}

// BootstrapAll walks the entire chain and returns user-table tablets
// grouped by tableID. The metadata-table's own tablets ("!0") are included
// in the result so callers can re-scan if invalidated.
func (w *Walker) BootstrapAll(ctx context.Context) (map[string][]TabletInfo, error) {
	mdTablets, err := w.ScanRootTablet(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan root: %w", err)
	}

	out := map[string][]TabletInfo{
		MetadataTableID: mdTablets,
	}
	for _, mdt := range mdTablets {
		userTablets, err := w.ScanMetadataTablet(ctx, mdt)
		if err != nil {
			return nil, fmt.Errorf("scan metadata tablet %q: %w", mdt.EndRow, err)
		}
		for _, t := range userTablets {
			out[t.TableID] = append(out[t.TableID], t)
		}
	}
	return out, nil
}

// LocateTable returns the tablets for tableID, scanning the bootstrap chain
// fresh on every call. Cheap implementation for V0 — defers to BootstrapAll
// and indexes by table; the cache layer in internal/cache calls this on
// miss / post-invalidation. Replaceable later with narrow-range metadata
// scans once we want to avoid full re-walks.
func (w *Walker) LocateTable(ctx context.Context, tableID string) ([]TabletInfo, error) {
	all, err := w.BootstrapAll(ctx)
	if err != nil {
		return nil, err
	}
	return all[tableID], nil
}

// RawRootScan returns the un-aggregated TKeyValue stream from the root
// tablet, in wire form (relative-key compression NOT yet materialized).
// This is the substrate for shoal-bootstrap -dump-root-scan: callers can
// inspect the raw bytes before they're folded through AggregateRows.
func (w *Walker) RawRootScan(ctx context.Context) (host string, kvs []*data.TKeyValue, more bool, err error) {
	loc, err := w.locator.RootTabletLocation(ctx)
	if err != nil {
		return "", nil, false, fmt.Errorf("locate root tablet: %w", err)
	}
	if loc == nil {
		return "", nil, false, fmt.Errorf("root tablet has no current location (likely mid-move; retry)")
	}
	extent := rootTabletExtent()

	cli, err := scanclient.Dial(loc.HostPort, w.locator.InstanceID(), w.accVersion)
	if err != nil {
		return loc.HostPort, nil, false, fmt.Errorf("dial %s: %w", loc.HostPort, err)
	}
	defer cli.Close()

	w.logger.LogAttrs(ctx, slog.LevelInfo, "raw scan",
		slog.String("phase", "root"),
		slog.String("host", loc.HostPort),
		extentAttr(extent),
	)
	start := time.Now()
	scan, err := cli.SimpleScan(ctx, scanclient.SimpleScanRequest{
		Credentials: w.creds,
		Extent:      extent,
		Range:       fullRange(),
	})
	dur := time.Since(start)
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelError, "raw scan failed",
			slog.String("host", loc.HostPort),
			extentAttr(extent),
			slog.Duration("dur", dur),
			slog.String("err", err.Error()),
		)
		return loc.HostPort, nil, false, fmt.Errorf("scan %s: %w", loc.HostPort, err)
	}
	if scan == nil || scan.Result_ == nil {
		return loc.HostPort, nil, false, fmt.Errorf("scan %s: nil InitialScan result", loc.HostPort)
	}
	w.logger.LogAttrs(ctx, slog.LevelInfo, "raw scan ok",
		slog.String("host", loc.HostPort),
		extentAttr(extent),
		slog.Int("kvs", len(scan.Result_.Results)),
		slog.Bool("more", scan.Result_.More),
		slog.Duration("dur", dur),
	)
	return loc.HostPort, scan.Result_.Results, scan.Result_.More, nil
}

func (w *Walker) scanTablet(ctx context.Context, hostPort string, extent *data.TKeyExtent) ([]TabletInfo, error) {
	cli, err := scanclient.Dial(hostPort, w.locator.InstanceID(), w.accVersion)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", hostPort, err)
	}
	defer cli.Close()

	w.logger.LogAttrs(ctx, slog.LevelInfo, "scan tablet",
		slog.String("host", hostPort),
		extentAttr(extent),
	)
	start := time.Now()
	scan, err := cli.SimpleScan(ctx, scanclient.SimpleScanRequest{
		Credentials: w.creds,
		Extent:      extent,
		Range:       fullRange(),
	})
	dur := time.Since(start)
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelError, "scan tablet failed",
			slog.String("host", hostPort),
			extentAttr(extent),
			slog.Duration("dur", dur),
			slog.String("err", err.Error()),
		)
		return nil, fmt.Errorf("scan %s: %w", hostPort, err)
	}
	// V0 single-shot — first batch only. If `scan.Result_.More` is true we
	// drop the rest. For metadata scans this is typically fine (root tablet
	// has small N; metadata tablets bound by Accumulo's split policy). If
	// it bites, we add continueScan/closeScan in #9.5.
	if scan == nil || scan.Result_ == nil {
		return nil, fmt.Errorf("scan %s: nil InitialScan result", hostPort)
	}
	w.logger.LogAttrs(ctx, slog.LevelInfo, "scan tablet ok",
		slog.String("host", hostPort),
		extentAttr(extent),
		slog.Int("kvs", len(scan.Result_.Results)),
		slog.Bool("more", scan.Result_.More),
		slog.Duration("dur", dur),
	)
	return AggregateRows(scan.Result_.Results)
}

// rootTabletExtent returns the TKeyExtent identifying the root tablet:
// table = "+r", no boundaries.
func rootTabletExtent() *data.TKeyExtent {
	return &data.TKeyExtent{
		Table:      []byte(RootTableID),
		EndRow:     nil,
		PrevEndRow: nil,
	}
}

// tabletExtent converts a TabletInfo into the TKeyExtent that identifies
// it on the wire.
func tabletExtent(t TabletInfo) *data.TKeyExtent {
	return &data.TKeyExtent{
		Table:      []byte(t.TableID),
		EndRow:     t.EndRow,
		PrevEndRow: t.PrevRow,
	}
}

// fullRange covers an entire tablet — both ends infinite.
func fullRange() *data.TRange {
	return &data.TRange{
		InfiniteStartKey: true,
		InfiniteStopKey:  true,
	}
}

// extentAttr renders a TKeyExtent as a slog Group — table as a string,
// endRow/prevEndRow rendered with PrintableBytes so non-ASCII rows show
// up as hex rather than mojibake in log lines.
func extentAttr(e *data.TKeyExtent) slog.Attr {
	if e == nil {
		return slog.String("extent", "<nil>")
	}
	return slog.Group("extent",
		slog.String("table", string(e.Table)),
		slog.String("prev", PrintableBytes(e.PrevEndRow)),
		slog.String("end", PrintableBytes(e.EndRow)),
	)
}

// PrintableBytes renders a row/qualifier/value byte slice in a form safe
// to drop into a log line: ASCII printable bytes pass through; otherwise
// hex-encoded with a "0x" prefix. nil → "<nil>"; empty → "".
func PrintableBytes(b []byte) string {
	if b == nil {
		return "<nil>"
	}
	if len(b) == 0 {
		return ""
	}
	for _, c := range b {
		if c < 0x20 || c >= 0x7f || !unicode.IsPrint(rune(c)) {
			return "0x" + hex.EncodeToString(b)
		}
	}
	return string(b)
}
