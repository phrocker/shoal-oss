package tserverprocess

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/tserver"
)

type admission bool

func (a admission) Accepting() bool { return bool(a) }

func TestOperationsReadinessAndMetrics(t *testing.T) {
	host := tserver.NewHost()
	handler := OperationsHandler(host, admission(true))
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready without lock = %d", response.Code)
	}

	if err := host.AdoptLock(tserver.LockID{
		UUID: "11111111-1111-4111-8111-111111111111", Sequence: 1,
	}); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("ready with lock = %d", response.Code)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(response.Body.String(), "shoal_tserver_lock_losses_total") {
		t.Fatalf("metrics = %q", response.Body.String())
	}
}
