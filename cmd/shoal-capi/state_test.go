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
