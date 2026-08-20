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
	select {
	case <-server.Done():
	default:
		t.Fatal("shutdown returned before serve loop ended")
	}
}
