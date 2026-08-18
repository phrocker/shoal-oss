package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal/accumulo"
)

func TestConnectorRegistryLifecycle(t *testing.T) {
	registry := newConnectorRegistry()
	first := &ownedConnector{}
	second := &ownedConnector{}

	firstID, ok := registry.add(first)
	if !ok || firstID == 0 {
		t.Fatalf("add(first) = %d, %v", firstID, ok)
	}
	secondID, ok := registry.add(second)
	if !ok || secondID == 0 || secondID == firstID {
		t.Fatalf("add(second) = %d, %v; first = %d", secondID, ok, firstID)
	}
	if got, ok := registry.get(firstID); !ok || got != first {
		t.Fatalf("get(first) = %p, %v", got, ok)
	}
	if got, ok := registry.remove(firstID); !ok || got != first {
		t.Fatalf("remove(first) = %p, %v", got, ok)
	}
	if _, ok := registry.get(firstID); ok {
		t.Fatal("removed connector remains registered")
	}
}

func TestOwnedScannerCloseCancelsAndJoinsActiveCalls(t *testing.T) {
	scanner := newOwnedScanner(nil, nil)
	ctx, done, err := scanner.begin(0)
	if err != nil {
		t.Fatal(err)
	}

	closeReturned := make(chan struct{})
	go func() {
		scanner.close()
		close(closeReturned)
	}()

	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("context error = %v, want canceled", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("close did not cancel active call")
	}
	select {
	case <-closeReturned:
		t.Fatal("close returned before active call completed")
	default:
	}

	done()
	select {
	case <-closeReturned:
	case <-time.After(time.Second):
		t.Fatal("close did not join completed call")
	}
	if _, _, err := scanner.begin(0); !errors.Is(err, accumulo.ErrConnectorClosed) {
		t.Fatalf("begin after close error = %v, want connector closed", err)
	}
}

func TestOwnedScannerCloseIsConcurrentAndIdempotent(t *testing.T) {
	scanner := newOwnedScanner(nil, nil)
	const callers = 16
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			scanner.close()
		}()
	}
	wait.Wait()
}

func TestOwnedScannerDeadline(t *testing.T) {
	scanner := newOwnedScanner(nil, nil)
	ctx, done, err := scanner.begin(time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("context error = %v, want deadline exceeded", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("operation context did not reach deadline")
	}
}

type fakeCAPIWriter struct {
	add   func(context.Context, *accumulo.Mutation) error
	flush func(context.Context) error
	close func(context.Context) error
}

func (w *fakeCAPIWriter) Add(ctx context.Context, mutation *accumulo.Mutation) error {
	return w.add(ctx, mutation)
}

func (w *fakeCAPIWriter) Flush(ctx context.Context) error {
	return w.flush(ctx)
}

func (w *fakeCAPIWriter) Close(ctx context.Context) error {
	return w.close(ctx)
}

func TestOwnedBatchWriterCloseCancelsAndJoinsActiveCalls(t *testing.T) {
	writer := newOwnedBatchWriter(&fakeCAPIWriter{
		add:   func(context.Context, *accumulo.Mutation) error { return nil },
		flush: func(context.Context) error { return nil },
		close: func(context.Context) error { return nil },
	}, nil)
	ctx, done, err := writer.begin(0)
	if err != nil {
		t.Fatal(err)
	}

	closeReturned := make(chan error, 1)
	go func() {
		closeReturned <- writer.close(time.Second)
	}()

	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("context error = %v, want canceled", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("close did not cancel active writer call")
	}
	select {
	case <-closeReturned:
		t.Fatal("close returned before active writer call completed")
	default:
	}

	done()
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not join active writer call")
	}
	if _, _, err := writer.begin(0); !errors.Is(err, accumulo.ErrBatchWriterClosed) {
		t.Fatalf("begin after close error = %v, want batch writer closed", err)
	}
}

func TestOwnedBatchWriterCloseDeadlineBoundsActiveWait(t *testing.T) {
	writer := newOwnedBatchWriter(&fakeCAPIWriter{
		add:   func(context.Context, *accumulo.Mutation) error { return nil },
		flush: func(context.Context) error { return nil },
		close: func(context.Context) error { return nil },
	}, nil)
	ctx, done, err := writer.begin(0)
	if err != nil {
		t.Fatal(err)
	}
	defer done()

	start := time.Now()
	if err := writer.close(20 * time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("close exceeded deadline bound: %v", elapsed)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("active context error = %v, want canceled", ctx.Err())
	}
	done()
	if err := writer.close(time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("repeated close error = %v, want sticky deadline", err)
	}
}

func TestOwnedBatchWriterRejectsOperationsAfterConnectorClose(t *testing.T) {
	owner := &ownedConnector{}
	owner.closed.Store(true)
	writer := newOwnedBatchWriter(&fakeCAPIWriter{
		add:   func(context.Context, *accumulo.Mutation) error { return nil },
		flush: func(context.Context) error { return nil },
		close: func(context.Context) error { return nil },
	}, owner)
	if _, _, err := writer.begin(0); !errors.Is(err, accumulo.ErrConnectorClosed) {
		t.Fatalf("begin error = %v, want connector closed", err)
	}
}

func TestMutationRegistryLifecycle(t *testing.T) {
	registry := newMutationRegistry()
	mutation, err := accumulo.NewMutation([]byte("row"))
	if err != nil {
		t.Fatal(err)
	}
	id, ok := registry.add(mutation)
	if !ok || id == 0 {
		t.Fatalf("add = %d, %v", id, ok)
	}
	if got, ok := registry.get(id); !ok || got != mutation {
		t.Fatalf("get = %p, %v", got, ok)
	}
	if got, ok := registry.remove(id); !ok || got != mutation {
		t.Fatalf("remove = %p, %v", got, ok)
	}
	if _, ok := registry.get(id); ok {
		t.Fatal("removed mutation remains registered")
	}
}
