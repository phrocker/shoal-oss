package accumulo

import (
	"context"
	"errors"
	"fmt"
)

// BatchScanner scans one or more ranges across tablet boundaries.
// Ranges are scanned sequentially in input order.
type BatchScanner struct {
	scanner *Scanner
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

	var values []KeyValue
	var cleanupErr error
	for index, scanRange := range ranges {
		if scanRange == nil {
			return values, errors.Join(
				cleanupErr,
				fmt.Errorf("accumulo: batch scan range %d is nil", index),
			)
		}
		remaining := cloneRange(scanRange)
		for {
			tablet, err := scanner.locateTablet(ctx, table, remaining.routingRow())
			if err != nil {
				return values, errors.Join(cleanupErr, err)
			}
			segment, done, err := clipRangeToTablet(remaining, tablet)
			if err != nil {
				return values, errors.Join(cleanupErr, err)
			}
			segmentValues, scanErr := scanner.Scan(ctx, segment)
			values = append(values, segmentValues...)
			if scanErr != nil {
				if !onlyCleanupErrors(scanErr) {
					return values, errors.Join(cleanupErr, scanErr)
				}
				cleanupErr = errors.Join(cleanupErr, scanErr)
			}
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
	return values, cleanupErr
}

func clipRangeToTablet(scanRange *Range, tablet Tablet) (*Range, bool, error) {
	if scanRange.fitsTablet(tablet) {
		return cloneRange(scanRange), true, nil
	}
	if tablet.Extent.EndRow == nil {
		return nil, false, errors.New("accumulo: unbounded tablet did not contain scan range")
	}
	segment, err := NewRange(
		scanRange.startRow,
		scanRange.startInclusive,
		tablet.Extent.EndRow,
		true,
	)
	if err != nil {
		return nil, false, err
	}
	return segment, false, nil
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
