package main

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletserver"
)

// compactorRole is the coordinator-facing CompactorService state for the
// single job this worker executes at a time.
type compactorRole struct {
	mu        sync.RWMutex
	job       *tabletserver.TExternalCompactionJob
	cancel    context.CancelFunc
	cancelled *atomic.Bool
}

func (r *compactorRole) begin(
	job *tabletserver.TExternalCompactionJob,
	cancel context.CancelFunc,
	cancelled *atomic.Bool,
) {
	r.mu.Lock()
	r.job = job
	r.cancel = cancel
	r.cancelled = cancelled
	r.mu.Unlock()
}

func (r *compactorRole) end(ecid string) {
	r.mu.Lock()
	if r.job != nil && r.job.GetExternalCompactionId() == ecid {
		r.job = nil
		r.cancel = nil
		r.cancelled = nil
	}
	r.mu.Unlock()
}

func (r *compactorRole) GetRunningCompaction(
	context.Context,
	*client.TInfo,
	*security.TCredentials,
) (*tabletserver.TExternalCompactionJob, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.job, nil
}

func (r *compactorRole) GetRunningCompactionId(
	context.Context,
	*client.TInfo,
	*security.TCredentials,
) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.job == nil {
		return "", nil
	}
	return r.job.GetExternalCompactionId(), nil
}

func (r *compactorRole) GetActiveCompactions(
	context.Context,
	*client.TInfo,
	*security.TCredentials,
) ([]*tabletserver.ActiveCompaction, error) {
	return nil, nil
}

func (r *compactorRole) Cancel(
	_ context.Context,
	_ *client.TInfo,
	_ *security.TCredentials,
	ecid string,
) error {
	r.mu.RLock()
	job, cancel, cancelled := r.job, r.cancel, r.cancelled
	r.mu.RUnlock()
	if job == nil || job.GetExternalCompactionId() != ecid || cancel == nil {
		return nil
	}
	cancelled.Store(true)
	cancel()
	return nil
}
