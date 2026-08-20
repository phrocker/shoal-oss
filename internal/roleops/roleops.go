// Package roleops provides the shared operational HTTP, readiness, and
// bounded-shutdown surface used by distributed Shoal roles.
package roleops

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Dependency struct {
	Name   string `json:"name"`
	Ready  bool   `json:"ready"`
	Detail string `json:"detail,omitempty"`
}

type Dependencies struct {
	mu      sync.RWMutex
	values  map[string]Dependency
	started atomic.Bool
}

func NewDependencies(names ...string) *Dependencies {
	d := &Dependencies{values: make(map[string]Dependency, len(names))}
	for _, name := range names {
		d.values[name] = Dependency{Name: name, Detail: "initializing"}
	}
	return d
}

func (d *Dependencies) Set(name string, ready bool, detail string) {
	d.mu.Lock()
	d.values[name] = Dependency{Name: name, Ready: ready, Detail: detail}
	d.mu.Unlock()
}

func (d *Dependencies) SetStarted(started bool) { d.started.Store(started) }

func (d *Dependencies) Snapshot() []Dependency {
	d.mu.RLock()
	out := make([]Dependency, 0, len(d.values))
	for _, value := range d.values {
		out = append(out, value)
	}
	d.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (d *Dependencies) Ready() bool {
	if !d.started.Load() {
		return false
	}
	for _, dependency := range d.Snapshot() {
		if !dependency.Ready {
			return false
		}
	}
	return true
}

type MetricsWriter func(*strings.Builder)

func Handler(dependencies *Dependencies, writeMetrics MetricsWriter) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/startupz", func(w http.ResponseWriter, _ *http.Request) {
		status := http.StatusOK
		started := dependencies != nil && dependencies.started.Load()
		if !started {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(struct {
			Started      bool         `json:"started"`
			Dependencies []Dependency `json:"dependencies"`
		}{Started: started, Dependencies: dependenciesSnapshot(dependencies)})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		status := http.StatusOK
		if dependencies == nil || !dependencies.Ready() {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(struct {
			Ready        bool         `json:"ready"`
			Dependencies []Dependency `json:"dependencies"`
		}{Ready: status == http.StatusOK, Dependencies: dependenciesSnapshot(dependencies)})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var b strings.Builder
		b.WriteString("# HELP shoal_dependency_ready Whether an operational dependency is ready.\n")
		b.WriteString("# TYPE shoal_dependency_ready gauge\n")
		for _, dependency := range dependenciesSnapshot(dependencies) {
			value := 0
			if dependency.Ready {
				value = 1
			}
			fmt.Fprintf(&b, "shoal_dependency_ready{name=%q} %d\n", dependency.Name, value)
		}
		if writeMetrics != nil {
			writeMetrics(&b)
		}
		_, _ = w.Write([]byte(b.String()))
	})
	return mux
}

func dependenciesSnapshot(dependencies *Dependencies) []Dependency {
	if dependencies == nil {
		return nil
	}
	return dependencies.Snapshot()
}

type Server struct {
	http *http.Server
	done chan error
	once sync.Once
}

func Start(address string, handler http.Handler, tlsConfig *tls.Config) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		listener = tls.NewListener(listener, tlsConfig.Clone())
	}
	server := &Server{
		http: &http.Server{
			Addr:              address,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
		done: make(chan error, 1),
	}
	go func() {
		err := server.http.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		server.done <- err
		close(server.done)
	}()
	return server, nil
}

func (s *Server) Done() <-chan error { return s.done }

func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	var shutdownErr error
	s.once.Do(func() {
		shutdownErr = s.http.Shutdown(ctx)
		if shutdownErr != nil {
			_ = s.http.Close()
		}
		select {
		case serveErr := <-s.done:
			shutdownErr = errors.Join(shutdownErr, serveErr)
		case <-ctx.Done():
			_ = s.http.Close()
			shutdownErr = errors.Join(shutdownErr, ctx.Err())
		}
	})
	return shutdownErr
}

func RunBounded(ctx context.Context, steps ...func(context.Context) error) error {
	var combined error
	for _, step := range steps {
		if step != nil {
			combined = errors.Join(combined, step(ctx))
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(combined, err)
		}
	}
	return combined
}
