package allocator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
)

type faultMode uint8

const (
	faultNone faultMode = iota
	faultUnknownBefore
	faultUnknownAfter
	faultReject
	faultPartialFirst
)

type memoryStore struct {
	mu       sync.Mutex
	cells    map[string]Cell
	faults   []faultMode
	captured []Mutation
}

func newMemoryStore() *memoryStore { return &memoryStore{cells: make(map[string]Cell)} }

func cellKey(c Coordinate) string {
	return string(c.Row) + "\x00" + string(c.Family) + "\x00" + string(c.Qualifier) + "\x00" + string(c.Visibility)
}

func (s *memoryStore) ReadExact(ctx context.Context, coordinates []Coordinate) ([]Cell, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Cell, 0, len(coordinates))
	for _, coordinate := range coordinates {
		if cell, ok := s.cells[cellKey(coordinate)]; ok {
			result = append(result, cloneCell(cell))
		}
	}
	return result, nil
}

func (s *memoryStore) ScanRowPrefix(_ context.Context, row, family, start, visibility []byte, limit int) ([]Cell, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []Cell
	for _, cell := range s.cells {
		if bytes.Equal(cell.Coordinate.Row, row) && bytes.Equal(cell.Coordinate.Family, family) &&
			bytes.Equal(cell.Coordinate.Visibility, visibility) &&
			bytes.Compare(cell.Coordinate.Qualifier, start) >= 0 {
			result = append(result, cloneCell(cell))
		}
	}
	sortCells(result)
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func sortCells(cells []Cell) {
	for i := range cells {
		for j := i + 1; j < len(cells); j++ {
			if bytes.Compare(cells[j].Coordinate.Qualifier, cells[i].Coordinate.Qualifier) < 0 {
				cells[i], cells[j] = cells[j], cells[i]
			}
		}
	}
}

func (s *memoryStore) CompareAndMutate(ctx context.Context, mutation Mutation) (Status, error) {
	if err := ctx.Err(); err != nil {
		return StatusUnknown, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.captured = append(s.captured, cloneMutation(mutation))
	fault := faultNone
	if len(s.faults) != 0 {
		fault, s.faults = s.faults[0], s.faults[1:]
	}
	if fault == faultUnknownBefore {
		return StatusUnknown, ErrConditionalUnknown
	}
	for _, condition := range mutation.Conditions {
		cell, found := s.cells[cellKey(condition.Coordinate)]
		if condition.Absent {
			if found {
				return StatusRejected, nil
			}
		} else if !found || !bytes.Equal(cell.Value, condition.Value) {
			return StatusRejected, nil
		}
	}
	if fault == faultReject {
		return StatusRejected, nil
	}
	updates := mutation.Updates
	if fault == faultPartialFirst {
		updates = updates[:1]
	}
	for _, update := range updates {
		key := cellKey(update.Coordinate)
		if update.Delete {
			delete(s.cells, key)
		} else {
			s.cells[key] = Cell{Coordinate: update.Coordinate.clone(), Value: append([]byte(nil), update.Value...)}
		}
	}
	if fault == faultUnknownAfter || fault == faultPartialFirst {
		return StatusUnknown, ErrConditionalUnknown
	}
	return StatusAccepted, nil
}

func cloneCell(cell Cell) Cell {
	return Cell{Coordinate: cell.Coordinate.clone(), Value: append([]byte(nil), cell.Value...)}
}

func cloneMutation(value Mutation) Mutation {
	result := Mutation{Row: append([]byte(nil), value.Row...)}
	for _, condition := range value.Conditions {
		result.Conditions = append(result.Conditions, Condition{
			Coordinate: condition.Coordinate.clone(), Value: append([]byte(nil), condition.Value...), Absent: condition.Absent,
		})
	}
	for _, update := range value.Updates {
		result.Updates = append(result.Updates, Update{
			Coordinate: update.Coordinate.clone(), Value: append([]byte(nil), update.Value...),
			Delete: update.Delete, Timestamp: update.Timestamp,
		})
	}
	return result
}

func testHead(max uint32) coordination.AllocatorHeadV1 {
	return coordination.AllocatorHeadV1{
		NextEpoch: 1, HistoryFloor: 1, RetentionGeneration: 2,
		WriterAuthorityGeneration: 3, WriterMode: coordination.WriterModeAccumuloPrimary,
		WriterHolder: coordination.OwnerID("authority"), WriterFence: 4, MaxActiveReservations: max,
	}
}

func newTestClient(t *testing.T, store *memoryStore, max uint32) *Client {
	t.Helper()
	client, err := New(Config{
		Domain: coordination.DomainID("domain"), ControlVisibility: []byte("CONTROL"), Store: store,
		Clock:      func() time.Time { return time.Date(2026, 8, 27, 15, 0, 0, 0, time.UTC) },
		MaxRetries: 2, RetryBackoff: time.Nanosecond, MaxCheckpointAdvance: 16, MaxRetireBatch: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	head := testHead(max)
	value, _ := coordination.MarshalAllocatorHeadV1(head)
	store.cells[cellKey(client.headCoordinate())] = Cell{Coordinate: client.headCoordinate(), Value: value}
	return client
}

func reserveRequest(head coordination.AllocatorHeadV1, txn string) ReserveRequest {
	return ReserveRequest{
		Predecessor: head, TXN: coordination.TXN(txn), Owner: coordination.OwnerID("worker"),
		LeaseUntil:          time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC),
		Authority:           Authority{Generation: 3, Mode: coordination.WriterModeAccumuloPrimary, Holder: coordination.OwnerID("authority"), Fence: 4},
		RetentionGeneration: 2,
	}
}

func TestReserveAcceptedUnknownAndIdempotentConflict(t *testing.T) {
	for _, fault := range []faultMode{faultNone, faultUnknownBefore, faultUnknownAfter} {
		t.Run(fmt.Sprint(fault), func(t *testing.T) {
			store := newMemoryStore()
			client := newTestClient(t, store, 4)
			if fault != faultNone {
				store.faults = []faultMode{fault, faultNone}
			}
			head, _ := client.CurrentHead(context.Background())
			reservation, err := client.Reserve(context.Background(), reserveRequest(head, "txn"))
			if err != nil || reservation.Epoch != 1 {
				t.Fatalf("Reserve = %#v, %v", reservation, err)
			}
			retry, err := client.Reserve(context.Background(), reserveRequest(head, "txn"))
			if err != nil || !reservationEqual(retry, reservation) {
				t.Fatalf("idempotent retry = %#v, %v", retry, err)
			}
			conflict := reserveRequest(head, "other")
			if _, err := client.Reserve(context.Background(), conflict); !errors.Is(err, ErrConflict) {
				t.Fatalf("conflicting retry error = %v", err)
			}
			if len(store.captured[0].Conditions) != 2 || len(store.captured[0].Updates) != 2 {
				t.Fatalf("allocation mutation shape = %#v", store.captured[0])
			}
		})
	}
}

func TestReserveRejectsAuthorityRetentionWindowAndExhaustion(t *testing.T) {
	store := newMemoryStore()
	client := newTestClient(t, store, 1)
	head, _ := client.CurrentHead(context.Background())
	tests := []func(*ReserveRequest){
		func(r *ReserveRequest) { r.Authority.Generation++ },
		func(r *ReserveRequest) { r.Authority.Fence++ },
		func(r *ReserveRequest) { r.Authority.Holder = coordination.OwnerID("other") },
		func(r *ReserveRequest) { r.Authority.Mode = coordination.WriterModeEmbeddedPrimary },
		func(r *ReserveRequest) { r.RetentionGeneration++ },
	}
	for i, mutate := range tests {
		request := reserveRequest(head, fmt.Sprint(i))
		mutate(&request)
		if _, err := client.Reserve(context.Background(), request); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale request %d = %v", i, err)
		}
	}
	if _, err := client.Reserve(context.Background(), reserveRequest(head, "one")); err != nil {
		t.Fatal(err)
	}
	full, _ := client.CurrentHead(context.Background())
	if _, err := client.Reserve(context.Background(), reserveRequest(full, "two")); !errors.Is(err, ErrWindowFull) {
		t.Fatalf("window full = %v", err)
	}
	exhausted := testHead(2)
	exhausted.NextEpoch = coordination.Epoch(^uint64(0) >> 1)
	exhausted.RetiredThrough = exhausted.NextEpoch - 1
	exhausted.Frontier = exhausted.RetiredThrough
	exhausted.VisibleAt = time.Date(2026, 8, 27, 15, 0, 0, 0, time.UTC)
	exhausted.CheckpointDigest = coordination.Sum([]byte("checkpoint"))
	value, _ := coordination.MarshalAllocatorHeadV1(exhausted)
	store.cells[cellKey(client.headCoordinate())] = Cell{Coordinate: client.headCoordinate(), Value: value}
	if _, err := client.Reserve(context.Background(), reserveRequest(exhausted, "max")); !errors.Is(err, ErrExhausted) {
		t.Fatalf("exhaustion = %v", err)
	}
}

func TestAllocationOneSidedUnknownIsCorruption(t *testing.T) {
	store := newMemoryStore()
	client := newTestClient(t, store, 4)
	store.faults = []faultMode{faultPartialFirst}
	head, _ := client.CurrentHead(context.Background())
	if _, err := client.Reserve(context.Background(), reserveRequest(head, "txn")); !errors.Is(err, ErrCorruption) {
		t.Fatalf("one-sided allocation = %v", err)
	}
}

func reserveOne(t *testing.T, client *Client, txn string) coordination.ReservationV1 {
	t.Helper()
	head, err := client.CurrentHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := client.Reserve(context.Background(), reserveRequest(head, txn))
	if err != nil {
		t.Fatal(err)
	}
	return reservation
}

func TestTerminalizeCrashBoundariesAndContradictoryOutcome(t *testing.T) {
	for _, faults := range [][]faultMode{
		nil,
		{faultUnknownBefore, faultNone, faultNone},
		{faultUnknownAfter, faultNone},
		{faultNone, faultUnknownBefore, faultNone},
		{faultNone, faultUnknownAfter},
	} {
		store := newMemoryStore()
		client := newTestClient(t, store, 8)
		reservation := reserveOne(t, client, "txn")
		store.faults = faults
		completion, outcome, err := client.Terminalize(context.Background(), reservation, coordination.StateCommitted)
		if err != nil || completion != CompletionOutcomeDurable {
			t.Fatalf("Terminalize faults %v = %v %#v %v", faults, completion, outcome, err)
		}
		retry, got, err := client.Terminalize(context.Background(), coordination.ReservationV1{
			Epoch: reservation.Epoch, TXN: reservation.TXN, Owner: reservation.Owner,
			LeaseUntil: reservation.LeaseUntil, Fence: reservation.Fence,
			AuthorityGeneration: reservation.AuthorityGeneration, State: coordination.StateCommitted,
		}, coordination.StateCommitted)
		if err != nil || retry != CompletionOutcomeDurable || !outcomeEqual(got, outcome) {
			t.Fatalf("terminal retry = %v %#v %v", retry, got, err)
		}
	}

	store := newMemoryStore()
	client := newTestClient(t, store, 8)
	reservation := reserveOne(t, client, "txn")
	other, _ := coordination.NewEpochOutcomeV1(reservation.Epoch, coordination.TXN("other"), coordination.StateAborted, reservation.Fence, reservation.AuthorityGeneration)
	value, _ := coordination.MarshalEpochOutcomeV1(other)
	store.cells[cellKey(client.outcomeCoordinate(reservation.Epoch))] = Cell{Coordinate: client.outcomeCoordinate(reservation.Epoch), Value: value}
	if _, _, err := client.Terminalize(context.Background(), reservation, coordination.StateCommitted); !errors.Is(err, ErrCorruption) {
		t.Fatalf("contradictory outcome = %v", err)
	}
}

func TestFrontierContiguousDigestMonotonicUnknownAndRetirement(t *testing.T) {
	store := newMemoryStore()
	client := newTestClient(t, store, 8)
	first := reserveOne(t, client, "one")
	second := reserveOne(t, client, "two")
	if _, _, err := client.Terminalize(context.Background(), second, coordination.StateAborted); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AdvanceFrontier(context.Background()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("frontier skipped hole: %v", err)
	}
	_, firstOutcome, err := client.Terminalize(context.Background(), first, coordination.StateCommitted)
	if err != nil {
		t.Fatal(err)
	}
	secondOutcome, _ := client.Outcome(context.Background(), second.Epoch)
	wantDigest := OutcomesDigest([]coordination.EpochOutcomeV1{firstOutcome, secondOutcome})
	if wantDigest != OutcomesDigest([]coordination.EpochOutcomeV1{secondOutcome, firstOutcome}) {
		t.Fatal("outcomes digest is order-dependent")
	}
	store.faults = []faultMode{faultUnknownAfter}
	checkpoint, err := client.AdvanceFrontier(context.Background())
	if err != nil || checkpoint.Frontier != 2 || checkpoint.OutcomesDigest != wantDigest {
		t.Fatalf("AdvanceFrontier = %#v, %v", checkpoint, err)
	}
	latest, err := client.LatestCheckpoint(context.Background())
	if err != nil || !checkpointEqual(latest, checkpoint) {
		t.Fatalf("LatestCheckpoint = %#v, %v", latest, err)
	}
	next := reserveOne(t, client, "three")
	if _, _, err := client.Terminalize(context.Background(), next, coordination.StatePoisoned); err != nil {
		t.Fatal(err)
	}
	checkpoint2, err := client.AdvanceFrontier(context.Background())
	if err != nil || !checkpoint2.VisibleAt.After(checkpoint.VisibleAt) {
		t.Fatalf("monotonic checkpoint = %#v, %v", checkpoint2, err)
	}
	retired, err := client.Retire(context.Background())
	if err != nil || retired.RetiredThrough != 3 || retired.ActiveReservations != 0 {
		t.Fatalf("Retire = %#v, %v", retired, err)
	}
	for epoch := coordination.Epoch(1); epoch <= 3; epoch++ {
		if _, err := client.Reservation(context.Background(), epoch); !errors.Is(err, ErrNotFound) {
			t.Fatalf("reservation %d remains: %v", epoch, err)
		}
		if _, err := client.Outcome(context.Background(), epoch); err != nil {
			t.Fatalf("outcome %d retired: %v", epoch, err)
		}
	}
}

func TestCheckpointAndRetirementPartialUnknownCorruption(t *testing.T) {
	store := newMemoryStore()
	client := newTestClient(t, store, 8)
	reservation := reserveOne(t, client, "txn")
	if _, _, err := client.Terminalize(context.Background(), reservation, coordination.StateCommitted); err != nil {
		t.Fatal(err)
	}
	store.faults = []faultMode{faultPartialFirst}
	if _, err := client.AdvanceFrontier(context.Background()); !errors.Is(err, ErrCorruption) {
		t.Fatalf("partial checkpoint = %v", err)
	}

	store = newMemoryStore()
	client = newTestClient(t, store, 8)
	reservation = reserveOne(t, client, "txn")
	if _, _, err := client.Terminalize(context.Background(), reservation, coordination.StateCommitted); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AdvanceFrontier(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.faults = []faultMode{faultPartialFirst}
	if _, err := client.Retire(context.Background()); !errors.Is(err, ErrCorruption) {
		t.Fatalf("partial retirement = %v", err)
	}
}

func TestCheckpointAndRetirementHonorBatchBounds(t *testing.T) {
	store := newMemoryStore()
	client := newTestClient(t, store, 8)
	client.maxCheckpointAdvance = 2
	client.maxRetireBatch = 1
	for i := 0; i < 3; i++ {
		reservation := reserveOne(t, client, fmt.Sprintf("txn-%d", i))
		if _, _, err := client.Terminalize(context.Background(), reservation, coordination.StateAborted); err != nil {
			t.Fatal(err)
		}
	}
	first, err := client.AdvanceFrontier(context.Background())
	if err != nil || first.Frontier != 2 {
		t.Fatalf("first bounded checkpoint = %#v, %v", first, err)
	}
	second, err := client.AdvanceFrontier(context.Background())
	if err != nil || second.Frontier != 3 {
		t.Fatalf("second bounded checkpoint = %#v, %v", second, err)
	}
	for epoch := coordination.Epoch(1); epoch <= 3; epoch++ {
		head, err := client.Retire(context.Background())
		if err != nil || head.RetiredThrough != epoch {
			t.Fatalf("bounded retirement %d = %#v, %v", epoch, head, err)
		}
	}
}

func TestConcurrentReservationsAreUniqueAndContiguous(t *testing.T) {
	store := newMemoryStore()
	client := newTestClient(t, store, 128)
	const count = 64
	var failures atomic.Int32
	epochs := make(chan coordination.Epoch, count)
	var wait sync.WaitGroup
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			for attempt := 0; attempt < 1000; attempt++ {
				head, err := client.CurrentHead(context.Background())
				if err != nil {
					continue
				}
				reservation, err := client.Reserve(context.Background(), reserveRequest(head, fmt.Sprintf("txn-%d", index)))
				if err == nil {
					epochs <- reservation.Epoch
					return
				}
				if !errors.Is(err, ErrConflict) {
					failures.Add(1)
					return
				}
			}
			failures.Add(1)
		}(i)
	}
	wait.Wait()
	close(epochs)
	if failures.Load() != 0 {
		t.Fatalf("%d allocations failed", failures.Load())
	}
	seen := make(map[coordination.Epoch]bool, count)
	for epoch := range epochs {
		if seen[epoch] {
			t.Fatalf("duplicate epoch %d", epoch)
		}
		seen[epoch] = true
	}
	for epoch := coordination.Epoch(1); epoch <= count; epoch++ {
		if !seen[epoch] {
			t.Fatalf("missing epoch %d", epoch)
		}
	}
}
