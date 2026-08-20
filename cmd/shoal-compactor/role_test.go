package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCompactorRoleCancellationAndLifecycle(t *testing.T) {
	role := &compactorRole{}
	job := translatableJob(newECID())
	ctx, cancel := context.WithCancel(context.Background())
	var cancelled atomic.Bool
	role.begin(job, cancel, &cancelled)

	id, err := role.GetRunningCompactionId(context.Background(), nil, nil)
	if err != nil || id != job.GetExternalCompactionId() {
		t.Fatalf("running id=%q err=%v", id, err)
	}
	if err := role.Cancel(context.Background(), nil, nil, newECID()); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal("wrong ECID cancelled the job")
	}
	if err := role.Cancel(context.Background(), nil, nil, job.GetExternalCompactionId()); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() == nil || !cancelled.Load() {
		t.Fatal("matching ECID did not cancel the job")
	}

	role.end(job.GetExternalCompactionId())
	id, err = role.GetRunningCompactionId(context.Background(), nil, nil)
	if err != nil || id != "" {
		t.Fatalf("running id after end=%q err=%v", id, err)
	}
}

func TestCompactorRoleConcurrentReadsAndTransitions(t *testing.T) {
	role := &compactorRole{}
	job := translatableJob(newECID())
	var cancelled atomic.Bool
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				role.begin(job, func() {}, &cancelled)
				_, _ = role.GetRunningCompaction(context.Background(), nil, nil)
				_, _ = role.GetRunningCompactionId(context.Background(), nil, nil)
				role.end(job.GetExternalCompactionId())
			}
		}()
	}
	wg.Wait()
}
