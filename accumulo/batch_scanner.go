package accumulo

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// BatchScanner scans one or more ranges across tablet boundaries.
// Results are returned in input-range and tablet order.
type BatchScanner struct {
	scanner *Scanner
}

type batchScanSegment struct {
	routingRow []byte
	tablet     Tablet
	scanRange  *Range
}

type batchScanResult struct {
	values []KeyValue
	err    error
}

// NewBatchScanner constructs a multi-tablet scanner for table.
func (c *Connector) NewBatchScanner(table Table, options ScannerOptions) (*BatchScanner, error) {
	scanner, err := c.NewScanner(table, options)
	if err != nil {
		return nil, err
	}
	return &BatchScanner{scanner: scanner}, nil
}

// Scan reads ranges in input order, splitting each range at tablet boundaries.
// Cleanup errors are accumulated while scanning continues because returned
// results remain usable.
func (s *BatchScanner) Scan(ctx context.Context, ranges []*Range) ([]KeyValue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		return nil, errors.New("accumulo: batch scan requires at least one range")
	}
	table, err := s.scanner.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	scanner := *s.scanner
	scanner.table = table

	segments, err := scanner.planBatchScan(ctx, table, ranges)
	if err != nil {
		return nil, err
	}
	results := scanner.executeBatchScan(ctx, table, segments)

	var values []KeyValue
	var cleanupErr error
	for _, result := range results {
		values = append(values, result.values...)
		if result.err == nil {
			continue
		}
		if !onlyCleanupErrors(result.err) {
			return values, errors.Join(cleanupErr, result.err)
		}
		cleanupErr = errors.Join(cleanupErr, result.err)
	}
	return values, cleanupErr
}

func (s *Scanner) planBatchScan(
	ctx context.Context,
	table Table,
	ranges []*Range,
) ([]batchScanSegment, error) {
	var segments []batchScanSegment
	for index, scanRange := range ranges {
		if scanRange == nil {
			return nil, fmt.Errorf("accumulo: batch scan range %d is nil", index)
		}
		remaining := cloneRange(scanRange)
		for {
			routingRow := remaining.routingRow()
			tablet, err := s.locateTablet(ctx, table, routingRow)
			if err != nil {
				return nil, err
			}
			segment, done := clipRangeToTablet(remaining, tablet)
			segments = append(segments, batchScanSegment{
				routingRow: routingRow,
				tablet:     tablet,
				scanRange:  segment,
			})
			if done {
				break
			}
			remaining = &Range{
				startRow:       cloneRow(tablet.Extent.EndRow),
				endRow:         cloneRow(remaining.endRow),
				startInclusive: false,
				endInclusive:   remaining.endInclusive,
			}
		}
	}
	return segments, nil
}

func (s *Scanner) executeBatchScan(
	ctx context.Context,
	table Table,
	segments []batchScanSegment,
) []batchScanResult {
	results := make([]batchScanResult, len(segments))
	parallelism := s.options.Parallelism
	if parallelism == 0 {
		parallelism = 1
	}
	if parallelism > len(segments) {
		parallelism = len(segments)
	}

	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(parallelism)
	for range parallelism {
		go func() {
			defer workers.Done()
			for index := range jobs {
				segment := segments[index]
				results[index].values, results[index].err = s.scanLocated(
					ctx,
					table,
					segment.routingRow,
					segment.tablet,
					segment.scanRange,
				)
			}
		}()
	}
	for index := range segments {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	return results
}

func clipRangeToTablet(scanRange *Range, tablet Tablet) (*Range, bool) {
	if scanRange.fitsTablet(tablet) {
		return cloneRange(scanRange), true
	}
	return &Range{
		startRow:       cloneRow(scanRange.startRow),
		endRow:         cloneRow(tablet.Extent.EndRow),
		startInclusive: scanRange.startInclusive,
		endInclusive:   true,
	}, false
}

func cloneRange(scanRange *Range) *Range {
	return &Range{
		startRow:       cloneRow(scanRange.startRow),
		endRow:         cloneRow(scanRange.endRow),
		startInclusive: scanRange.startInclusive,
		endInclusive:   scanRange.endInclusive,
	}
}

func onlyCleanupErrors(err error) bool {
	if err == nil {
		return true
	}
	if _, ok := err.(*CleanupError); ok {
		return true
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return false
	}
	children := joined.Unwrap()
	if len(children) == 0 {
		return false
	}
	for _, child := range children {
		if !onlyCleanupErrors(child) {
			return false
		}
	}
	return true
}
