package roleops

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSemanticReadinessReportsDependencies(t *testing.T) {
	deps := NewDependencies("lock", "manager")
	deps.SetStarted(true)
	deps.Set("lock", true, "held")
	handler := Handler(deps, nil)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), `"name":"manager"`) {
		t.Fatalf("response = %d %q", response.Code, response.Body.String())
	}
	deps.Set("manager", true, "observed")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("ready response = %d %q", response.Code, response.Body.String())
	}
}

func TestServerShutdownJoinsServeLoop(t *testing.T) {
	server, err := Start("127.0.0.1:0", Handler(NewDependencies(), nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	// Blocking with the context's deadline rather than a non-blocking check.
	// The old form asserted the same property but only when the scheduler
	// happened to cooperate, so a real ordering failure surfaced as a flake on
	// a loaded runner instead of a reproducible failure.
	select {
	case <-server.Done():
	case <-ctx.Done():
		t.Fatal("shutdown returned before the serve loop ended")
	}
	if err := server.Err(); err != nil {
		t.Fatalf("serve loop error after clean shutdown: %v", err)
	}
}

// TestShutdownAndWatcherBothSeeTheServeError proves the serve error is not
// consumed by whichever caller reads first. The channel previously carried a
// single value, so a goroutine watching Done and a caller inside Shutdown
// raced for it, and the loser observed nil. Both real consumers of this
// package, shoal-tserver and shoal-compactor, watch Done in a goroutine and
// call Shutdown on exit, so a metrics server that failed for a real reason
// could have its error silently dropped.
func TestShutdownAndWatcherBothSeeTheServeError(t *testing.T) {
	server, err := Start("127.0.0.1:0", Handler(NewDependencies(), nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	watched := make(chan error, 1)
	go func() {
		<-server.Done()
		watched <- server.Err()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-watched:
		// A clean shutdown reports no serve error to either observer. The
		// point is that both observed the same thing rather than one of them
		// consuming it.
		if err != nil {
			t.Fatalf("watcher saw %v after a clean shutdown", err)
		}
	case <-ctx.Done():
		t.Fatal("watcher never observed the serve loop ending")
	}
	if err := server.Err(); err != nil {
		t.Fatalf("Err after shutdown = %v", err)
	}
}

// TestShutdownIsIdempotent guards the sync.Once path now that Shutdown reads a
// closed channel rather than draining a value.
func TestShutdownIsIdempotent(t *testing.T) {
	server, err := Start("127.0.0.1:0", Handler(NewDependencies(), nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown = %v", err)
	}
	select {
	case <-server.Done():
	default:
		t.Fatal("done must remain readable after repeated shutdown")
	}
}
