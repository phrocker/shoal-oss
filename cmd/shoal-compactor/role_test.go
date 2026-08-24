package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	clientgen "github.com/phrocker/shoal-oss/internal/thrift/gen/client"
	"github.com/phrocker/shoal-oss/internal/thrift/gen/security"
	"github.com/phrocker/shoal-oss/internal/tserverprocess"
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

func TestCompactorRoleRejectsUntrustedCredentials(t *testing.T) {
	trusted := &security.TCredentials{
		Principal:      "!SYSTEM",
		TokenClassName: "system-token",
		Token:          []byte("trusted"),
		InstanceId:     "instance",
	}
	role := &compactorRole{auth: tserverprocess.ExactAuthenticator{
		Identities: []*security.TCredentials{trusted},
	}}
	job := translatableJob(newECID())
	ctx, cancel := context.WithCancel(context.Background())
	var cancelled atomic.Bool
	role.begin(job, cancel, &cancelled)

	if _, err := role.GetRunningCompactionId(context.Background(), nil, trusted); err != nil {
		t.Fatalf("trusted read: %v", err)
	}
	untrusted := &security.TCredentials{Principal: "attacker"}
	_, err := role.GetRunningCompactionId(context.Background(), nil, untrusted)
	var securityErr *clientgen.ThriftSecurityException
	if !errors.As(err, &securityErr) ||
		securityErr.Code != clientgen.SecurityErrorCode_PERMISSION_DENIED {
		t.Fatalf("untrusted read error = %v", err)
	}
	if err := role.Cancel(context.Background(), nil, untrusted, job.GetExternalCompactionId()); !errors.As(err, &securityErr) {
		t.Fatalf("untrusted cancel error = %v", err)
	}
	if ctx.Err() != nil || cancelled.Load() {
		t.Fatal("untrusted credentials cancelled the job")
	}
}
