package accumulo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/ingestclient"
	"github.com/phrocker/shoal/internal/metadata"
	clientpkg "github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
)

type fakeBatchWriterIngest struct {
	mu       sync.Mutex
	sessions []*fakeBatchWriterSession
	starts   []string
}

func (f *fakeBatchWriterIngest) start(
	_ context.Context,
	address string,
	_ ingestclient.Durability,
) (batchWriterSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	session := &fakeBatchWriterSession{}
	f.starts = append(f.starts, address)
	f.sessions = append(f.sessions, session)
	return session, nil
}

type blockingBatchWriterWalker struct {
	entered chan struct{}
}

func (w *blockingBatchWriterWalker) LocateTable(
	ctx context.Context,
	_ string,
) ([]metadata.TabletInfo, error) {
	close(w.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

type sequencedBatchWriterWalker struct {
	mu        sync.Mutex
	snapshots [][]metadata.TabletInfo
	calls     int
}

func (w *sequencedBatchWriterWalker) LocateTable(
	ctx context.Context,
	_ string,
) ([]metadata.TabletInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	index := w.calls
	w.calls++
	if index >= len(w.snapshots) {
		index = len(w.snapshots) - 1
	}
	return append([]metadata.TabletInfo(nil), w.snapshots[index]...), nil
}

type batchWriterApply struct {
	extent    *data.TKeyExtent
	mutations []*data.TMutation
}

type fakeBatchWriterSession struct {
	mu sync.Mutex

	applies       []batchWriterApply
	closeCalls    int
	cancelCalls   int
	closeResult   *data.UpdateErrors
	closeErr      error
	applyErr      error
	cancelResult  bool
	cancelErr     error
	applyEntered  chan struct{}
	applyRelease  <-chan struct{}
	closeEntered  chan struct{}
	cancelContext error
}

func (f *fakeBatchWriterSession) Apply(
	ctx context.Context,
	extent *data.TKeyExtent,
	mutations []*data.TMutation,
) error {
	f.mu.Lock()
	f.applies = append(f.applies, batchWriterApply{
		extent:    cloneThriftExtent(extent),
		mutations: cloneThriftMutations(mutations),
	})
	entered := f.applyEntered
	release := f.applyRelease
	err := f.applyErr
	f.mu.Unlock()
	if entered != nil {
		entered <- struct{}{}
	}
	if release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
		}
	}
	return err
}

func (f *fakeBatchWriterSession) Close(context.Context) (*data.UpdateErrors, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	if f.closeEntered != nil {
		f.closeEntered <- struct{}{}
	}
	return f.closeResult, f.closeErr
}

func (f *fakeBatchWriterSession) Cancel(ctx context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls++
	f.cancelContext = ctx.Err()
	return f.cancelResult, f.cancelErr
}

func newBatchWriterTestWriter(
	t *testing.T,
	tablets []metadata.TabletInfo,
	options BatchWriterOptions,
	ingest *fakeBatchWriterIngest,
) *BatchWriter {
	t.Helper()
	writer := newBatchWriterTestWriterWithLocator(
		t,
		&fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": tablets}},
		options,
	)
	writer.startSession = ingest.start
	return writer
}

func newBatchWriterTestWriterWithLocator(
	t *testing.T,
	locator interface {
		LocateTable(context.Context, string) ([]metadata.TabletInfo, error)
	},
	options BatchWriterOptions,
) *BatchWriter {
	t.Helper()
	connector := testConnectorWithDiscovery(
		t,
		locator,
		&fakeTableNames{
			byName: map[string]string{"events": "1"},
			byID:   map[string]string{"1": "events"},
		},
	)
	writer, err := connector.NewBatchWriter(
		Table{Name: "events"},
		options,
	)
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func TestBatchWriterRoutesBatchesAndCopiesMutation(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{
		MaxMemoryBytes: 1 << 20,
		MaxBatchBytes:  1,
		Durability:     DurabilitySync,
	}, ingest)

	row := []byte("a")
	value := []byte("one")
	mutation, err := NewMutation(row)
	if err != nil {
		t.Fatal(err)
	}
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, value)
	if err := writer.Add(context.Background(), mutation); err != nil {
		t.Fatal(err)
	}
	row[0] = 'z'
	value[0] = 'x'
	mutation.PutLatest([]byte("cf"), []byte("later"), nil, []byte("ignored"))

	for _, row := range []string{"b", "n", "q"} {
		mutation, err := NewMutation([]byte(row))
		if err != nil {
			t.Fatal(err)
		}
		mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte(row))
		if err := writer.Add(context.Background(), mutation); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	ingest.mu.Lock()
	defer ingest.mu.Unlock()
	if got, want := ingest.starts, []string{"ts1:9997", "ts2:9997", "ts3:9997"}; !equalStrings(got, want) {
		t.Fatalf("starts = %v, want %v", got, want)
	}
	if len(ingest.sessions[0].applies) != 2 {
		t.Fatalf("ts1 apply calls = %d, want 2 bounded batches", len(ingest.sessions[0].applies))
	}
	first := ingest.sessions[0].applies[0].mutations[0]
	if !bytes.Equal(first.Row, []byte("a")) {
		t.Fatalf("first row = %q, want a", first.Row)
	}
	expected, _ := cclient.NewMutation([]byte("a"))
	expected.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("one"))
	expectedWire, err := expected.ToThrift()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Data, expectedWire.Data) ||
		!equalByteSlices(first.Values, expectedWire.Values) ||
		first.Entries != expectedWire.Entries {
		t.Fatalf("snapshotted mutation = %+v, want original mutation", first)
	}
}

func TestBatchWriterMemoryBoundFlushesSynchronously(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{
		MaxMemoryBytes: 1,
	}, ingest)
	mutation, _ := NewMutation([]byte("a"))
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("value"))
	if err := writer.Add(context.Background(), mutation); err != nil {
		t.Fatal(err)
	}
	ingest.mu.Lock()
	defer ingest.mu.Unlock()
	if len(ingest.sessions) != 1 || ingest.sessions[0].closeCalls != 1 {
		t.Fatalf("sessions = %d close calls = %d, want synchronous submission", len(ingest.sessions), ingest.sessions[0].closeCalls)
	}
}

func TestBatchWriterRetryOptionsValidation(t *testing.T) {
	if _, err := normalizeBatchWriterOptions(BatchWriterOptions{
		MaxRetries: -1,
	}); err == nil {
		t.Fatal("expected negative MaxRetries error")
	}
	if _, err := normalizeBatchWriterOptions(BatchWriterOptions{
		RetryBackoff: -time.Nanosecond,
	}); err == nil {
		t.Fatal("expected negative RetryBackoff error")
	}
}

func TestBatchWriterAddRollsBackBeforeSubmission(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	walker := &blockingBatchWriterWalker{entered: make(chan struct{})}
	connector := testConnectorWithDiscovery(
		t,
		walker,
		&fakeTableNames{
			byName: map[string]string{"events": "1"},
			byID:   map[string]string{"1": "events"},
		},
	)
	writer, err := connector.NewBatchWriter(
		Table{Name: "events"},
		BatchWriterOptions{MaxMemoryBytes: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	writer.startSession = ingest.start
	mutation, _ := NewMutation([]byte("a"))
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("value"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- writer.Add(ctx, mutation)
	}()
	<-walker.entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Add = %v, want context.Canceled", err)
	}
	if len(writer.pending) != 0 || writer.pendingBytes != 0 {
		t.Fatalf("failed Add retained %d mutations and %d bytes", len(writer.pending), writer.pendingBytes)
	}
}

func TestBatchWriterConcurrentAdds(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{}, ingest)
	const mutationCount = 32
	errs := make(chan error, mutationCount)
	var callers sync.WaitGroup
	callers.Add(mutationCount)
	for index := 0; index < mutationCount; index++ {
		go func(index int) {
			defer callers.Done()
			mutation, err := NewMutation([]byte(fmt.Sprintf("row-%02d", index)))
			if err == nil {
				mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte{byte(index)})
				err = writer.Add(context.Background(), mutation)
			}
			errs <- err
		}(index)
	}
	callers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	ingest.mu.Lock()
	defer ingest.mu.Unlock()
	submitted := 0
	rows := make(map[string]struct{}, mutationCount)
	for _, session := range ingest.sessions {
		for _, apply := range session.applies {
			submitted += len(apply.mutations)
			for _, mutation := range apply.mutations {
				rows[string(mutation.Row)] = struct{}{}
			}
		}
	}
	if submitted != mutationCount {
		t.Fatalf("submitted = %d, want %d", submitted, mutationCount)
	}
	if len(rows) != mutationCount {
		t.Fatalf("unique rows = %d, want %d", len(rows), mutationCount)
	}
}

func TestBatchWriterCloseLifecycle(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{}, ingest)
	mutation, _ := NewMutation([]byte("a"))
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("value"))
	if err := writer.Add(context.Background(), mutation); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(context.Background(), mutation); !errors.Is(err, ErrBatchWriterClosed) {
		t.Fatalf("Add after Close = %v, want ErrBatchWriterClosed", err)
	}
	if err := writer.Flush(context.Background()); !errors.Is(err, ErrBatchWriterClosed) {
		t.Fatalf("Flush after Close = %v, want ErrBatchWriterClosed", err)
	}
	ingest.mu.Lock()
	defer ingest.mu.Unlock()
	if len(ingest.sessions) != 1 || ingest.sessions[0].closeCalls != 1 {
		t.Fatalf("sessions = %d close calls = %d, want one submission", len(ingest.sessions), ingest.sessions[0].closeCalls)
	}
}

func TestBatchWriterWaitingCallerHonorsContext(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{}, ingest)
	writer.startSession = func(
		_ context.Context,
		_ string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		return &fakeBatchWriterSession{}, nil
	}
	if err := writer.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer writer.unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := writer.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush = %v, want DeadlineExceeded", err)
	}
}

func TestBatchWriterCancellationCleansUpAndFailsSticky(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{}, ingest)
	session := &fakeBatchWriterSession{
		applyEntered: make(chan struct{}, 1),
		applyRelease: make(chan struct{}),
		cancelResult: true,
	}
	writer.startSession = func(
		_ context.Context,
		_ string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		return session, nil
	}
	mutation, _ := NewMutation([]byte("a"))
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("value"))
	if err := writer.Add(context.Background(), mutation); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- writer.Flush(ctx)
	}()
	<-session.applyEntered
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrBatchWriterFailed) {
		t.Fatalf("Flush = %v, want canceled sticky failure", err)
	}
	session.mu.Lock()
	if session.cancelCalls != 1 || session.cancelContext != nil {
		t.Fatalf("cancel calls = %d cleanup context = %v", session.cancelCalls, session.cancelContext)
	}
	session.mu.Unlock()
	if err := writer.Add(context.Background(), mutation); !errors.Is(err, ErrBatchWriterFailed) {
		t.Fatalf("Add after failure = %v, want ErrBatchWriterFailed", err)
	}
}

func TestBatchWriterSurfacesAccumuloUpdateErrors(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{}, ingest)
	session := &fakeBatchWriterSession{
		closeResult: &data.UpdateErrors{
			FailedExtents: map[*data.TKeyExtent]int64{
				&data.TKeyExtent{Table: []byte("1"), EndRow: []byte("k")}: 1,
			},
			ViolationSummaries: []*data.TConstraintViolationSummary{
				{
					ConstrainClass:             "example.Constraint",
					ViolationCode:              7,
					ViolationDescription:       "rejected",
					NumberOfViolatingMutations: 2,
				},
			},
			AuthorizationFailures: map[*data.TKeyExtent]clientpkg.SecurityErrorCode{
				&data.TKeyExtent{
					Table:  []byte("1"),
					EndRow: []byte("k"),
				}: clientpkg.SecurityErrorCode_PERMISSION_DENIED,
			},
		},
	}
	writer.startSession = func(
		_ context.Context,
		_ string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		return session, nil
	}
	for _, row := range []string{"a", "b"} {
		mutation, _ := NewMutation([]byte(row))
		mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte(row))
		if err := writer.Add(context.Background(), mutation); err != nil {
			t.Fatal(err)
		}
	}
	err := writer.Flush(context.Background())
	var rejection *MutationRejectionError
	if !errors.Is(err, ErrBatchWriterFailed) || !errors.As(err, &rejection) {
		t.Fatalf("Flush = %v, want MutationRejectionError", err)
	}
	if got := rejection.FailedExtents[0]; got.Submitted != 2 || got.Committed != 1 {
		t.Fatalf("failed extent = %+v", got)
	}
	if rejection.AuthorizationFailures[0].Code != "PERMISSION_DENIED" {
		t.Fatalf("authorization code = %q", rejection.AuthorizationFailures[0].Code)
	}
}

func TestMalformedUpdateErrorsIncludeTabletServer(t *testing.T) {
	const server = "ts-malformed:9997"
	extent := func(endRow string) *data.TKeyExtent {
		return &data.TKeyExtent{
			Table:  []byte("1"),
			EndRow: []byte(endRow),
		}
	}
	plan := serverPlan{
		address: server,
		extents: []extentPlan{
			{
				extent:    extent("k"),
				mutations: []bufferedMutation{{row: []byte("a")}},
			},
		},
	}
	cases := []struct {
		name   string
		errors *data.UpdateErrors
	}{
		{
			name: "nil failed extent",
			errors: &data.UpdateErrors{
				FailedExtents: map[*data.TKeyExtent]int64{nil: 0},
			},
		},
		{
			name: "unknown failed extent",
			errors: &data.UpdateErrors{
				FailedExtents: map[*data.TKeyExtent]int64{extent("z"): 0},
			},
		},
		{
			name: "invalid committed count",
			errors: &data.UpdateErrors{
				FailedExtents: map[*data.TKeyExtent]int64{extent("k"): 2},
			},
		},
		{
			name: "duplicate failed extent",
			errors: &data.UpdateErrors{
				FailedExtents: map[*data.TKeyExtent]int64{
					extent("k"): 0,
					extent("k"): 0,
				},
			},
		},
		{
			name: "nil authorization extent",
			errors: &data.UpdateErrors{
				AuthorizationFailures: map[*data.TKeyExtent]clientpkg.SecurityErrorCode{
					nil: clientpkg.SecurityErrorCode_PERMISSION_DENIED,
				},
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := decodeMutationUpdateErrors(plan, testCase.errors)
			if err == nil {
				t.Fatal("expected malformed update error")
			}
			if !strings.Contains(err.Error(), server) {
				t.Fatalf("error = %q, want tablet server %q", err, server)
			}
		})
	}
}

func TestBatchWriterRelocatesExplicitlyUncommittedSuffix(t *testing.T) {
	walker := &sequencedBatchWriterWalker{
		snapshots: [][]metadata.TabletInfo{
			{
				{
					TableID: "1",
					EndRow:  []byte("k"),
					Location: &metadata.Location{
						HostPort: "ts1:9997",
						Session:  "old",
					},
				},
			},
			{
				{
					TableID: "1",
					EndRow:  []byte("k"),
					Location: &metadata.Location{
						HostPort: "ts2:9997",
						Session:  "new",
					},
				},
			},
		},
	}
	writer := newBatchWriterTestWriterWithLocator(t, walker, BatchWriterOptions{
		MaxRetries:   1,
		RetryBackoff: time.Nanosecond,
	})
	first := &fakeBatchWriterSession{
		closeResult: &data.UpdateErrors{
			FailedExtents: map[*data.TKeyExtent]int64{
				&data.TKeyExtent{Table: []byte("1"), EndRow: []byte("k")}: 1,
			},
		},
	}
	second := &fakeBatchWriterSession{}
	var starts []string
	writer.startSession = func(
		_ context.Context,
		address string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		starts = append(starts, address)
		switch address {
		case "ts1:9997":
			return first, nil
		case "ts2:9997":
			return second, nil
		default:
			return nil, fmt.Errorf("unexpected tablet server %q", address)
		}
	}
	for _, row := range []string{"a", "b"} {
		mutation, _ := NewMutation([]byte(row))
		mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte(row))
		if err := writer.Add(context.Background(), mutation); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{"ts1:9997", "ts2:9997"}; !equalStrings(starts, want) {
		t.Fatalf("starts = %v, want %v", starts, want)
	}
	if got, want := appliedMutationRows(first), []string{"a", "b"}; !equalStrings(got, want) {
		t.Fatalf("initial rows = %v, want %v", got, want)
	}
	if got, want := appliedMutationRows(second), []string{"b"}; !equalStrings(got, want) {
		t.Fatalf("relocated rows = %v, want only uncommitted suffix %v", got, want)
	}
}

func TestBatchWriterAcceptsFullyCommittedFailedExtent(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{
		MaxRetries:   1,
		RetryBackoff: time.Nanosecond,
	}, ingest)
	session := &fakeBatchWriterSession{
		closeResult: &data.UpdateErrors{
			FailedExtents: map[*data.TKeyExtent]int64{
				&data.TKeyExtent{Table: []byte("1"), EndRow: []byte("k")}: 2,
			},
		},
	}
	starts := 0
	writer.startSession = func(
		_ context.Context,
		_ string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		starts++
		return session, nil
	}
	for _, row := range []string{"a", "b"} {
		mutation, _ := NewMutation([]byte(row))
		mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte(row))
		if err := writer.Add(context.Background(), mutation); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if starts != 1 {
		t.Fatalf("start calls = %d, want no empty retry", starts)
	}
}

func TestBatchWriterRetriesMovedTabletBeforeAcceptance(t *testing.T) {
	walker := &sequencedBatchWriterWalker{
		snapshots: [][]metadata.TabletInfo{
			{
				{
					TableID:  "1",
					Location: &metadata.Location{HostPort: "ts1:9997"},
				},
			},
			{
				{
					TableID:  "1",
					Location: &metadata.Location{HostPort: "ts2:9997"},
				},
			},
		},
	}
	writer := newBatchWriterTestWriterWithLocator(t, walker, BatchWriterOptions{
		MaxRetries:   1,
		RetryBackoff: time.Nanosecond,
	})
	var starts []string
	success := &fakeBatchWriterSession{}
	writer.startSession = func(
		_ context.Context,
		address string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		starts = append(starts, address)
		if address == "ts1:9997" {
			return nil, errors.New("tablet server moved")
		}
		return success, nil
	}
	mutation, _ := NewMutation([]byte("row"))
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("value"))
	if err := writer.Add(context.Background(), mutation); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{"ts1:9997", "ts2:9997"}; !equalStrings(starts, want) {
		t.Fatalf("starts = %v, want %v", starts, want)
	}
	if got := appliedMutationRows(success); !equalStrings(got, []string{"row"}) {
		t.Fatalf("successful rows = %v, want row", got)
	}
}

func TestBatchWriterPreAcceptanceRetryExhaustionIsNotSticky(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{
		MaxRetries:   1,
		RetryBackoff: time.Nanosecond,
	}, ingest)
	attempts := 0
	success := &fakeBatchWriterSession{}
	writer.startSession = func(
		_ context.Context,
		_ string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		attempts++
		if attempts <= 2 {
			return nil, errors.New("dial unavailable")
		}
		return success, nil
	}
	mutation, _ := NewMutation([]byte("a"))
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("value"))
	if err := writer.Add(context.Background(), mutation); err != nil {
		t.Fatal(err)
	}
	err := writer.Flush(context.Background())
	if !errors.Is(err, ErrBatchWriterRetryExhausted) {
		t.Fatalf("Flush = %v, want ErrBatchWriterRetryExhausted", err)
	}
	if errors.Is(err, ErrBatchWriterFailed) {
		t.Fatalf("pre-acceptance failure became sticky: %v", err)
	}
	if len(writer.pending) != 1 {
		t.Fatalf("pending = %d, want retained mutation", len(writer.pending))
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := appliedMutationRows(success); !equalStrings(got, []string{"a"}) {
		t.Fatalf("successful rows = %v, want a", got)
	}
}

func TestBatchWriterExplicitRetryExhaustionIsSticky(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{
		MaxRetries:   1,
		RetryBackoff: time.Nanosecond,
	}, ingest)
	starts := 0
	writer.startSession = func(
		_ context.Context,
		_ string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		starts++
		return &fakeBatchWriterSession{
			closeResult: &data.UpdateErrors{
				FailedExtents: map[*data.TKeyExtent]int64{
					&data.TKeyExtent{Table: []byte("1"), EndRow: []byte("k")}: 0,
				},
			},
		}, nil
	}
	mutation, _ := NewMutation([]byte("a"))
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("value"))
	if err := writer.Add(context.Background(), mutation); err != nil {
		t.Fatal(err)
	}
	err := writer.Flush(context.Background())
	var rejection *MutationRejectionError
	if !errors.Is(err, ErrBatchWriterRetryExhausted) ||
		!errors.Is(err, ErrBatchWriterFailed) ||
		!errors.As(err, &rejection) {
		t.Fatalf("Flush = %v, want sticky retry exhaustion with rejection details", err)
	}
	if starts != 2 {
		t.Fatalf("start calls = %d, want initial attempt plus one retry", starts)
	}
	if got := rejection.FailedExtents[0]; got.Submitted != 1 || got.Committed != 0 {
		t.Fatalf("failed extent = %+v", got)
	}
}

func TestBatchWriterDoesNotRetryAmbiguousApplyFailure(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{
		MaxRetries:   3,
		RetryBackoff: time.Nanosecond,
	}, ingest)
	starts := 0
	session := &fakeBatchWriterSession{
		applyErr:     errors.New("apply response lost"),
		cancelResult: true,
	}
	writer.startSession = func(
		_ context.Context,
		_ string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		starts++
		return session, nil
	}
	mutation, _ := NewMutation([]byte("a"))
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("value"))
	if err := writer.Add(context.Background(), mutation); err != nil {
		t.Fatal(err)
	}
	err := writer.Flush(context.Background())
	if !errors.Is(err, ErrBatchWriterFailed) {
		t.Fatalf("Flush = %v, want ErrBatchWriterFailed", err)
	}
	if starts != 1 {
		t.Fatalf("start calls = %d, want no ambiguous retry", starts)
	}
}

func TestBatchWriterRetryBackoffHonorsCancellation(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{
		MaxRetries:   3,
		RetryBackoff: time.Hour,
	}, ingest)
	closeEntered := make(chan struct{}, 1)
	session := &fakeBatchWriterSession{
		closeEntered: closeEntered,
		closeResult: &data.UpdateErrors{
			FailedExtents: map[*data.TKeyExtent]int64{
				&data.TKeyExtent{Table: []byte("1"), EndRow: []byte("k")}: 0,
			},
		},
	}
	starts := 0
	writer.startSession = func(
		_ context.Context,
		_ string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		starts++
		return session, nil
	}
	mutation, _ := NewMutation([]byte("a"))
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("value"))
	if err := writer.Add(context.Background(), mutation); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- writer.Flush(ctx)
	}()
	<-closeEntered
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrBatchWriterFailed) {
		t.Fatalf("Flush = %v, want sticky context cancellation", err)
	}
	if starts != 1 {
		t.Fatalf("start calls = %d, want cancellation before retry", starts)
	}
}

func TestBatchWriterCloseCleanupErrorIsStable(t *testing.T) {
	ingest := &fakeBatchWriterIngest{}
	writer := newBatchWriterTestWriter(t, discoveryTablets(), BatchWriterOptions{}, ingest)
	closeFailure := errors.New("close transport failed")
	cleanupFailure := errors.New("cancel transport failed")
	session := &fakeBatchWriterSession{
		closeErr:  closeFailure,
		cancelErr: cleanupFailure,
	}
	writer.startSession = func(
		_ context.Context,
		_ string,
		_ ingestclient.Durability,
	) (batchWriterSession, error) {
		return session, nil
	}
	mutation, _ := NewMutation([]byte("a"))
	mutation.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("value"))
	if err := writer.Add(context.Background(), mutation); err != nil {
		t.Fatal(err)
	}
	first := writer.Close(context.Background())
	second := writer.Close(context.Background())
	var cleanup *BatchWriterCleanupError
	if !errors.Is(first, ErrBatchWriterFailed) ||
		!errors.Is(first, closeFailure) ||
		!errors.Is(first, cleanupFailure) ||
		!errors.As(first, &cleanup) {
		t.Fatalf("Close = %v, want sticky cleanup error", first)
	}
	if first != second {
		t.Fatalf("repeated Close returned a different error")
	}
}

func cloneThriftExtent(extent *data.TKeyExtent) *data.TKeyExtent {
	if extent == nil {
		return nil
	}
	return &data.TKeyExtent{
		Table:      cloneRow(extent.Table),
		EndRow:     cloneRow(extent.EndRow),
		PrevEndRow: cloneRow(extent.PrevEndRow),
	}
}

func cloneThriftMutations(mutations []*data.TMutation) []*data.TMutation {
	cloned := make([]*data.TMutation, len(mutations))
	for index, mutation := range mutations {
		cloned[index] = &data.TMutation{
			Row:     cloneRow(mutation.Row),
			Data:    cloneRow(mutation.Data),
			Values:  cloneByteSlices(mutation.Values),
			Entries: mutation.Entries,
			Sources: append([]string(nil), mutation.Sources...),
		}
	}
	return cloned
}

func appliedMutationRows(session *fakeBatchWriterSession) []string {
	session.mu.Lock()
	defer session.mu.Unlock()
	var rows []string
	for _, apply := range session.applies {
		for _, mutation := range apply.mutations {
			rows = append(rows, string(mutation.Row))
		}
	}
	return rows
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalByteSlices(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}
