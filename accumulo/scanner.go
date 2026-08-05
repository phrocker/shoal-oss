package accumulo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/phrocker/shoal/internal/scanclient"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletserver"
)

const defaultScannerBatchSize int32 = 1024
const scannerCloseTimeout = 5 * time.Second

// ErrRangeSpansTablets indicates that a range exceeds the initial
// single-tablet scanner scope.
var ErrRangeSpansTablets = errors.New("accumulo: scan range spans tablets")

// Key is a Go-native Accumulo key.
type Key struct {
	Row              []byte
	ColumnFamily     []byte
	ColumnQualifier  []byte
	ColumnVisibility []byte
	Timestamp        int64
}

// KeyValue is one scanned Accumulo entry.
type KeyValue struct {
	Key   Key
	Value []byte
}

// ScannerOptions configures a Scanner.
type ScannerOptions struct {
	BatchSize      int32
	Authorizations [][]byte
	// Parallelism bounds concurrent tablet scans performed by BatchScanner.
	// Zero uses one worker. Scanner ignores this field.
	Parallelism int
	// UseMultiScan groups tablet ranges by server into Accumulo multi-scan
	// RPCs. Multi-scan result order is server-defined.
	UseMultiScan bool
}

// CleanupError reports that scan results are usable but the server-side scan
// session could not be closed.
type CleanupError struct {
	ScanID int64
	Err    error
}

func (e *CleanupError) Error() string {
	return fmt.Sprintf("accumulo: close scan %d: %v", e.ScanID, e.Err)
}

// Unwrap returns the close failure.
func (e *CleanupError) Unwrap() error { return e.Err }

// Scanner executes scans for one table through its Connector.
type Scanner struct {
	connector *Connector
	table     Table
	options   ScannerOptions
}

// NewScanner constructs a scanner for table.
func (c *Connector) NewScanner(table Table, options ScannerOptions) (*Scanner, error) {
	if table.ID == "" && table.Name == "" {
		return nil, fmt.Errorf("%w: scanner table identity is empty", ErrTableNotFound)
	}
	if options.BatchSize < 0 {
		return nil, errors.New("accumulo: scanner batch size must not be negative")
	}
	if options.Parallelism < 0 {
		return nil, errors.New("accumulo: scanner parallelism must not be negative")
	}
	if _, err := c.discoveryState(); err != nil {
		return nil, err
	}
	options.Authorizations = cloneByteSlices(options.Authorizations)
	return &Scanner{
		connector: c,
		table: Table{
			Name: table.Name,
			ID:   table.ID,
		},
		options: options,
	}, nil
}

// Scan reads a range that must fit within one tablet. If the tablet server
// rejects a stale assignment, Scan invalidates discovery and retries once.
func (s *Scanner) Scan(ctx context.Context, scanRange *Range) ([]KeyValue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scanRange == nil {
		return nil, errors.New("accumulo: scan range is nil")
	}
	table, err := s.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	routingRow := scanRange.routingRow()
	tablet, err := s.locateTablet(ctx, table, routingRow)
	if err != nil {
		return nil, err
	}
	return s.scanLocated(ctx, table, routingRow, tablet, scanRange)
}

func (s *Scanner) scanLocated(
	ctx context.Context,
	table Table,
	routingRow []byte,
	tablet Tablet,
	scanRange *Range,
) ([]KeyValue, error) {
	var priorCleanup error
	for attempt := 0; attempt < 2; attempt++ {
		if !scanRange.fitsTablet(tablet) {
			return nil, fmt.Errorf(
				"%w: table=%s start=%q end=%q tabletEnd=%q",
				ErrRangeSpansTablets,
				table.ID,
				scanRange.startRow,
				scanRange.endRow,
				tablet.Extent.EndRow,
			)
		}

		values, scanErr := s.scanTablet(ctx, tablet, scanRange)
		if scanErr == nil || attempt == 1 || !isStaleScanError(scanErr) {
			return values, errors.Join(priorCleanup, scanErr)
		}
		var cleanupErr *CleanupError
		errors.As(scanErr, &cleanupErr)
		if invalidateErr := s.connector.InvalidateTablet(table, routingRow); invalidateErr != nil {
			return values, errors.Join(priorCleanup, scanErr, invalidateErr)
		}
		if cleanupErr != nil {
			priorCleanup = errors.Join(priorCleanup, cleanupErr)
		}
		tablet, scanErr = s.locateTablet(ctx, table, routingRow)
		if scanErr != nil {
			return nil, errors.Join(priorCleanup, scanErr)
		}
	}
	return nil, priorCleanup
}

func (s *Scanner) locateTablet(ctx context.Context, table Table, routingRow []byte) (Tablet, error) {
	for attempt := 0; attempt < 2; attempt++ {
		tablet, err := s.connector.LocateTablet(ctx, table, routingRow)
		if err == nil {
			return tablet, nil
		}
		if attempt == 1 || !isStaleScanError(err) {
			return Tablet{}, err
		}
		if invalidateErr := s.connector.InvalidateTablet(table, routingRow); invalidateErr != nil {
			return Tablet{}, errors.Join(err, invalidateErr)
		}
	}
	return Tablet{}, ErrTabletNotLocated
}

func (s *Scanner) resolveTable(ctx context.Context) (Table, error) {
	switch {
	case s.table.ID == "":
		return s.connector.TableByName(ctx, s.table.Name)
	case s.table.Name == "":
		return s.connector.TableByID(ctx, s.table.ID)
	default:
		return s.table, nil
	}
}

func (s *Scanner) scanTablet(ctx context.Context, tablet Tablet, scanRange *Range) ([]KeyValue, error) {
	if tablet.Server == nil || tablet.Server.HostPort == "" {
		return nil, fmt.Errorf("%w: table=%s", ErrTabletNotLocated, tablet.Extent.TableID)
	}
	batchSize := s.options.BatchSize
	if batchSize == 0 {
		batchSize = defaultScannerBatchSize
	}
	initial, err := s.connector.scan.Start(ctx, tablet.Server.HostPort, scanclient.StartRequest{
		Extent:             tabletExtentToThrift(tablet),
		Range:              scanRange.toThrift(),
		BatchSize:          batchSize,
		Authorizations:     cloneByteSlices(s.options.Authorizations),
		ReadaheadThreshold: int64(batchSize),
	})
	if initial == nil && err != nil {
		return nil, err
	}
	if initial == nil {
		return nil, errors.New("accumulo: start scan returned no result")
	}

	values, appendErr := appendScanResult(nil, initial.Result_)
	resultErr := errors.Join(err, appendErr)
	more := initial.Result_ != nil && initial.Result_.More
	if appendErr != nil {
		more = false
	}
	if more && initial.ScanID == 0 {
		resultErr = errors.Join(resultErr, errors.New("accumulo: continuing scan has zero scan ID"))
		more = false
	}
	for more {
		result, continueErr := s.connector.scan.Continue(ctx, tablet.Server.HostPort, initial.ScanID, 0)
		if continueErr != nil {
			resultErr = errors.Join(resultErr, continueErr)
			break
		}
		var appendErr error
		values, appendErr = appendScanResult(values, result)
		resultErr = errors.Join(resultErr, appendErr)
		if appendErr != nil {
			break
		}
		more = result != nil && result.More
	}

	var cleanupErr error
	if initial.ScanID != 0 {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), scannerCloseTimeout)
		defer cancel()
		if closeErr := s.connector.scan.CloseScan(closeCtx, tablet.Server.HostPort, initial.ScanID); closeErr != nil {
			cleanupErr = &CleanupError{ScanID: int64(initial.ScanID), Err: closeErr}
		}
	}
	return values, errors.Join(resultErr, cleanupErr)
}

func appendScanResult(values []KeyValue, result *data.ScanResult_) ([]KeyValue, error) {
	if result == nil {
		return values, nil
	}
	return appendKeyValues(values, result.Results)
}

func appendKeyValues(values []KeyValue, entries []*data.TKeyValue) ([]KeyValue, error) {
	for _, entry := range entries {
		if entry == nil || entry.Key == nil {
			return values, errors.New("accumulo: scan result contains a nil key")
		}
		values = append(values, KeyValue{
			Key: Key{
				Row:              cloneRow(entry.Key.Row),
				ColumnFamily:     cloneRow(entry.Key.ColFamily),
				ColumnQualifier:  cloneRow(entry.Key.ColQualifier),
				ColumnVisibility: cloneRow(entry.Key.ColVisibility),
				Timestamp:        entry.Key.Timestamp,
			},
			Value: cloneRow(entry.Value),
		})
	}
	return values, nil
}

func tabletExtentToThrift(tablet Tablet) *data.TKeyExtent {
	return &data.TKeyExtent{
		Table:      []byte(tablet.Extent.TableID),
		EndRow:     cloneRow(tablet.Extent.EndRow),
		PrevEndRow: cloneRow(tablet.Extent.PrevRow),
	}
}

func isStaleScanError(err error) bool {
	if errors.Is(err, ErrTabletNotLocated) {
		return true
	}
	var notServing *tabletserver.NotServingTabletException
	return errors.As(err, &notServing)
}

func cloneByteSlices(values [][]byte) [][]byte {
	if values == nil {
		return nil
	}
	cloned := make([][]byte, len(values))
	for i := range values {
		cloned[i] = cloneRow(values[i])
	}
	return cloned
}
