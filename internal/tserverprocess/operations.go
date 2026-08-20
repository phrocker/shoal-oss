package tserverprocess

import (
	"fmt"
	"net/http"

	"github.com/phrocker/shoal/internal/tserver"
)

type ScanAdmission interface {
	Accepting() bool
}

func OperationsHandler(host *tserver.Host, scans ScanAdmission) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		_, held := host.Lock()
		if !held || scans == nil || !scans.Accepting() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		metrics := host.Metrics()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		writeMetric(w, "shoal_tserver_tablets", "state", "loading", metrics.Loading)
		writeMetric(w, "shoal_tserver_tablets", "state", "hosted", metrics.Hosted)
		writeMetric(w, "shoal_tserver_tablets", "state", "unloading", metrics.Unloading)
		writeCounter(w, "shoal_tserver_assignments_total", metrics.Assignments)
		writeCounter(w, "shoal_tserver_loads_total", metrics.Loads)
		writeCounter(w, "shoal_tserver_load_failures_total", metrics.LoadFailures)
		writeCounter(w, "shoal_tserver_unloads_total", metrics.Unloads)
		writeCounter(w, "shoal_tserver_forced_unloads_total", metrics.ForcedUnloads)
		writeCounter(w, "shoal_tserver_rejected_stale_total", metrics.RejectedStale)
		writeCounter(w, "shoal_tserver_rejected_duplicate_total", metrics.RejectedDuplicate)
		writeCounter(w, "shoal_tserver_lock_losses_total", metrics.LockLosses)
		writeCounter(w, "shoal_tserver_dropped_on_lock_loss_total", metrics.DroppedOnLockLoss)
	})
	return mux
}

func writeMetric(w http.ResponseWriter, name, label, value string, count int) {
	_, _ = fmt.Fprintf(w, "%s{%s=%q} %d\n", name, label, value, count)
}

func writeCounter(w http.ResponseWriter, name string, count uint64) {
	_, _ = fmt.Fprintf(w, "%s %d\n", name, count)
}
