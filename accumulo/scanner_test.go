package accumulo

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/phrocker/shoal/internal/metadata"
	"github.com/phrocker/shoal/internal/scanclient"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletserver"
)

type fakeScannerAdapter struct {
	mu sync.Mutex

	startResults []*data.InitialScan
	startErrors  []error
	continues    []*data.ScanResult_
	continueErr  error
	closeErr     error
	closeErrors  []error

	addresses     []string
	startRequests []scanclient.StartRequest
	startCalls    int
	continueCalls int
	closeCalls    int
	onFirstStart  func()
	closeContext  error
}

func (f *fakeScannerAdapter) Start(
	_ context.Context,
	address string,
	req scanclient.StartRequest,
) (*data.InitialScan, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addresses = append(f.addresses, address)
	f.startRequests = append(f.startRequests, req)
	index := f.startCalls
	f.startCalls++
	if index == 0 && f.onFirstStart != nil {
		f.onFirstStart()
	}
	var result *data.InitialScan
	if index < len(f.startResults) {
		result = f.startResults[index]
	}
	var err error
	if index < len(f.startErrors) {
		err = f.startErrors[index]
	}
	return result, err
}

func (f *fakeScannerAdapter) Continue(
	ctx context.Context,
	_ string,
	_ data.ScanID,
	_ int64,
) (*data.ScanResult_, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := f.continueCalls
	f.continueCalls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.continueErr != nil {
		return nil, f.continueErr
	}
	if index >= len(f.continues) {
		return &data.ScanResult_{}, nil
	}
	return f.continues[index], nil
}

func (f *fakeScannerAdapter) CloseScan(ctx context.Context, _ string, _ data.ScanID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := f.closeCalls
	f.closeCalls++
	f.closeContext = ctx.Err()
	if index < len(f.closeErrors) {
		return f.closeErrors[index]
	}
	return f.closeErr
}

func (f *fakeScannerAdapter) Close() error { return nil }

func TestRangeWireBoundaries(t *testing.T) {
	scanRange, err := NewRange([]byte("a"), false, []byte("z"), true)
	if err != nil {
		t.Fatal(err)
	}
	wire := scanRange.toThrift()
	if !bytes.Equal(wire.Start.Row, []byte("a")) || wire.StartKeyInclusive {
		t.Fatalf("start = %+v inclusive=%v", wire.Start, wire.StartKeyInclusive)
	}
	if !bytes.Equal(wire.Stop.Row, []byte{'z', 0}) || wire.StopKeyInclusive {
		t.Fatalf("stop = %+v inclusive=%v", wire.Stop, wire.StopKeyInclusive)
	}
	if got := scanRange.routingRow(); !bytes.Equal(got, []byte{'a', 0}) {
		t.Fatalf("routing row = %q", got)
	}
	firstTablet := Tablet{Extent: TabletExtent{EndRow: []byte("k")}}
	exclusiveFollowingRow, _ := NewRange([]byte("a"), true, []byte{'k', 0}, false)
	if !exclusiveFollowingRow.fitsTablet(firstTablet) {
		t.Fatal("exclusive following-row stop should fit the preceding tablet")
	}
	inclusiveFollowingRow, _ := NewRange([]byte("a"), true, []byte{'k', 0}, true)
	if inclusiveFollowingRow.fitsTablet(firstTablet) {
		t.Fatal("inclusive following-row stop should span into the next tablet")
	}
}

func TestScannerContinuationAndCleanup(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	adapter := &fakeScannerAdapter{
		startResults: []*data.InitialScan{{
			ScanID: 7,
			Result_: &data.ScanResult_{
				Results: []*data.TKeyValue{testEntry("a", "one")},
				More:    true,
			},
		}},
		continues: []*data.ScanResult_{{
			Results: []*data.TKeyValue{testEntry("b", "two")},
		}},
	}
	connector.scan = adapter
	scanner, err := connector.NewScanner(Table{Name: "events"}, ScannerOptions{
		BatchSize:      2,
		Authorizations: [][]byte{[]byte("public")},
	})
	if err != nil {
		t.Fatal(err)
	}
	scanRange, _ := NewRange([]byte("a"), true, []byte("k"), true)
	values, err := scanner.Scan(context.Background(), scanRange)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || string(values[0].Value) != "one" || string(values[1].Key.Row) != "b" {
		t.Fatalf("values = %+v", values)
	}
	if adapter.startCalls != 1 || adapter.continueCalls != 1 || adapter.closeCalls != 1 {
		t.Fatalf("calls start/continue/close = %d/%d/%d", adapter.startCalls, adapter.continueCalls, adapter.closeCalls)
	}
	req := adapter.startRequests[0]
	if req.BatchSize != 2 || len(req.Authorizations) != 1 || string(req.Extent.Table) != "1" {
		t.Fatalf("start request = %+v", req)
	}
	values[0].Key.Row[0] = 'z'
	if string(adapter.startResults[0].Result_.Results[0].Key.Row) != "a" {
		t.Fatal("public key mutation leaked into wire result")
	}
}

func TestScannerRetriesNotServingAssignmentOnce(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{
		"1": {{TableID: "1", EndRow: []byte("k"), Location: &metadata.Location{HostPort: "old:9997"}}},
	}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	adapter := &fakeScannerAdapter{
		startResults: []*data.InitialScan{nil, {
			ScanID:  8,
			Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("a", "ok")}},
		}},
		startErrors: []error{tabletserver.NewNotServingTabletException(), nil},
	}
	adapter.onFirstStart = func() {
		walker.mu.Lock()
		walker.tablets["1"][0].Location = &metadata.Location{HostPort: "new:9997"}
		walker.mu.Unlock()
	}
	connector.scan = adapter
	scanner, err := connector.NewScanner(Table{ID: "1", Name: "events"}, ScannerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	scanRange, _ := NewRangeRow([]byte("a"))
	values, err := scanner.Scan(context.Background(), scanRange)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || adapter.startCalls != 2 {
		t.Fatalf("values=%+v startCalls=%d", values, adapter.startCalls)
	}
	if got := adapter.addresses; len(got) != 2 || got[0] != "old:9997" || got[1] != "new:9997" {
		t.Fatalf("addresses = %v", got)
	}
}

func TestScannerDoesNotDuplicateCleanupErrorWhenInvalidationFails(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	closeErr := errors.New("close failed")
	adapter := &fakeScannerAdapter{
		startResults: []*data.InitialScan{{
			ScanID:  12,
			Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("a", "ok")}},
		}},
		startErrors: []error{tabletserver.NewNotServingTabletException()},
		closeErr:    closeErr,
	}
	adapter.onFirstStart = func() {
		connector.discovery = nil
	}
	connector.scan = adapter
	scanner, err := connector.NewScanner(Table{ID: "1", Name: "events"}, ScannerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	scanRange, _ := NewRangeRow([]byte("a"))
	values, err := scanner.Scan(context.Background(), scanRange)
	if len(values) != 1 || !errors.Is(err, closeErr) || !errors.Is(err, ErrDiscoveryUnavailable) {
		t.Fatalf("values=%+v error=%v", values, err)
	}
	if got := strings.Count(err.Error(), closeErr.Error()); got != 1 {
		t.Fatalf("cleanup error occurrences = %d, want 1: %v", got, err)
	}
}

func TestScannerRangeAndCleanupErrors(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	closeErr := errors.New("close failed")
	adapter := &fakeScannerAdapter{
		startResults: []*data.InitialScan{{
			ScanID:  9,
			Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("a", "ok")}},
		}},
		closeErr: closeErr,
	}
	connector.scan = adapter
	scanner, err := connector.NewScanner(Table{ID: "1", Name: "events"}, ScannerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wide, _ := NewRange([]byte("a"), true, []byte("z"), true)
	if _, err := scanner.Scan(context.Background(), wide); !errors.Is(err, ErrRangeSpansTablets) {
		t.Fatalf("wide range error = %v", err)
	}
	oneRow, _ := NewRangeRow([]byte("a"))
	values, err := scanner.Scan(context.Background(), oneRow)
	var cleanupErr *CleanupError
	if len(values) != 1 || !errors.As(err, &cleanupErr) || !errors.Is(err, closeErr) {
		t.Fatalf("values=%+v error=%v", values, err)
	}
}

func TestScannerCancellationStillClosesServerScan(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &fakeScannerAdapter{
		startResults: []*data.InitialScan{{
			ScanID:  10,
			Result_: &data.ScanResult_{More: true},
		}},
		onFirstStart: cancel,
	}
	connector.scan = adapter
	scanner, err := connector.NewScanner(Table{ID: "1", Name: "events"}, ScannerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	scanRange, _ := NewRangeRow([]byte("a"))
	if _, err := scanner.Scan(ctx, scanRange); !errors.Is(err, context.Canceled) {
		t.Fatalf("scan error = %v, want context.Canceled", err)
	}
	if adapter.continueCalls != 1 || adapter.closeCalls != 1 {
		t.Fatalf("continue/close calls = %d/%d, want 1/1", adapter.continueCalls, adapter.closeCalls)
	}
	if adapter.closeContext != nil {
		t.Fatalf("close context error = %v, want active cleanup context", adapter.closeContext)
	}
}

func TestScannerPreservesInitialBatchWithLeaseCleanupError(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	leaseErr := errors.New("release transport")
	adapter := &fakeScannerAdapter{
		startResults: []*data.InitialScan{{
			ScanID:  11,
			Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("a", "ok")}},
		}},
		startErrors: []error{leaseErr},
	}
	connector.scan = adapter
	scanner, err := connector.NewScanner(Table{ID: "1", Name: "events"}, ScannerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	scanRange, _ := NewRangeRow([]byte("a"))
	values, err := scanner.Scan(context.Background(), scanRange)
	if len(values) != 1 || !errors.Is(err, leaseErr) {
		t.Fatalf("values=%+v error=%v", values, err)
	}
	if adapter.closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1", adapter.closeCalls)
	}
}

func TestBatchScannerSplitsRangeAcrossTablets(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	adapter := &fakeScannerAdapter{
		startResults: []*data.InitialScan{
			{ScanID: 20, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("a", "one")}}},
			{ScanID: 21, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("m", "two")}}},
			{ScanID: 22, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("z", "three")}}},
		},
	}
	connector.scan = adapter
	scanner, err := connector.NewBatchScanner(Table{ID: "1", Name: "events"}, ScannerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	scanRange, _ := NewRange([]byte("a"), true, []byte("z"), true)
	values, err := scanner.Scan(context.Background(), []*Range{scanRange})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 {
		t.Fatalf("values = %+v", values)
	}
	if got := adapter.addresses; len(got) != 3 ||
		got[0] != "ts1:9997" || got[1] != "ts2:9997" || got[2] != "ts3:9997" {
		t.Fatalf("addresses = %v", got)
	}
	assertWireRange(t, adapter.startRequests[0].Range, []byte("a"), true, []byte{'k', 0})
	assertWireRange(t, adapter.startRequests[1].Range, []byte("k"), false, []byte{'p', 0})
	assertWireRange(t, adapter.startRequests[2].Range, []byte("p"), false, []byte{'z', 0})
}

func TestBatchScannerContinuesAfterCleanupError(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	closeErr := errors.New("close first tablet")
	adapter := &fakeScannerAdapter{
		startResults: []*data.InitialScan{
			{ScanID: 23, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("a", "one")}}},
			{ScanID: 24, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("n", "two")}}},
		},
		closeErrors: []error{closeErr, nil},
	}
	connector.scan = adapter
	scanner, err := connector.NewBatchScanner(Table{ID: "1", Name: "events"}, ScannerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	scanRange, _ := NewRange([]byte("a"), true, []byte("n"), true)
	values, err := scanner.Scan(context.Background(), []*Range{scanRange})
	if len(values) != 2 || adapter.startCalls != 2 || !errors.Is(err, closeErr) {
		t.Fatalf("values=%+v startCalls=%d error=%v", values, adapter.startCalls, err)
	}
}

func TestBatchScannerSupportsUnboundedAndMultipleRanges(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	adapter := &fakeScannerAdapter{
		startResults: []*data.InitialScan{
			{ScanID: 25, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("a", "one")}}},
			{ScanID: 26, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("m", "two")}}},
			{ScanID: 27, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("z", "three")}}},
			{ScanID: 28, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("b", "four")}}},
			{ScanID: 29, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("n", "five")}}},
			{ScanID: 30, Result_: &data.ScanResult_{Results: []*data.TKeyValue{testEntry("q", "six")}}},
		},
	}
	connector.scan = adapter
	scanner, err := connector.NewBatchScanner(Table{ID: "1", Name: "events"}, ScannerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oneRow, _ := NewRangeRow([]byte("b"))
	spanning, _ := NewRange([]byte("n"), true, []byte("q"), true)
	values, err := scanner.Scan(context.Background(), []*Range{InfiniteRange(), oneRow, spanning})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 6 || adapter.startCalls != 6 {
		t.Fatalf("values=%+v startCalls=%d", values, adapter.startCalls)
	}
	if !adapter.startRequests[0].Range.InfiniteStartKey {
		t.Fatal("first infinite-range segment should have an unbounded start")
	}
	if !adapter.startRequests[2].Range.InfiniteStopKey {
		t.Fatal("last infinite-range segment should have an unbounded stop")
	}
	if got := adapter.addresses; len(got) != 6 ||
		got[3] != "ts1:9997" || got[4] != "ts2:9997" || got[5] != "ts3:9997" {
		t.Fatalf("addresses = %v", got)
	}
}

func TestBatchScannerValidatesRanges(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	connector.scan = &fakeScannerAdapter{}
	scanner, err := connector.NewBatchScanner(Table{ID: "1", Name: "events"}, ScannerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scanner.Scan(context.Background(), nil); err == nil {
		t.Fatal("empty ranges should fail")
	}
	oneRow, _ := NewRangeRow([]byte("a"))
	if _, err := scanner.Scan(context.Background(), []*Range{oneRow, nil}); err == nil {
		t.Fatal("nil range should fail")
	}
}

func assertWireRange(t *testing.T, scanRange *data.TRange, start []byte, startInclusive bool, stop []byte) {
	t.Helper()
	if !bytes.Equal(scanRange.Start.Row, start) || scanRange.StartKeyInclusive != startInclusive {
		t.Fatalf("start=%q inclusive=%v, want %q/%v", scanRange.Start.Row, scanRange.StartKeyInclusive, start, startInclusive)
	}
	if !bytes.Equal(scanRange.Stop.Row, stop) || scanRange.StopKeyInclusive {
		t.Fatalf("stop=%q inclusive=%v, want %q/false", scanRange.Stop.Row, scanRange.StopKeyInclusive, stop)
	}
}

func testEntry(row, value string) *data.TKeyValue {
	return &data.TKeyValue{
		Key: &data.TKey{
			Row:          []byte(row),
			ColFamily:    []byte("cf"),
			ColQualifier: []byte("cq"),
			Timestamp:    17,
		},
		Value: []byte(value),
	}
}
