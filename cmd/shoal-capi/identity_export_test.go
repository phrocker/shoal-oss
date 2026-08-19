package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/phrocker/shoal/accumulo"
)

func TestReadConnectorIdentityUsesCapturedValues(t *testing.T) {
	connector := &fakeConnectorAPI{principal: "alice"}
	owned := newOwnedConnector(connector, &fakeConnectorInstance{})

	ctx, done, err := owned.begin(0)
	if err != nil {
		t.Fatal(err)
	}
	identity, principal, err := readConnectorIdentity(ctx, owned)
	done()
	if err != nil {
		t.Fatal(err)
	}
	if identity != (accumulo.InstanceInfo{Name: "test", ID: "test-id"}) {
		t.Fatalf("identity = %#v", identity)
	}
	if principal != "alice" {
		t.Fatalf("principal = %q, want alice", principal)
	}
}

func TestReadConnectorIdentityHonorsDeadline(t *testing.T) {
	connector := &fakeConnectorAPI{
		identity: func(ctx context.Context) (accumulo.InstanceInfo, string, error) {
			<-ctx.Done()
			return accumulo.InstanceInfo{}, "", ctx.Err()
		},
	}
	owned := newOwnedConnector(connector, &fakeConnectorInstance{})
	ctx, done, err := owned.begin(time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	_, _, err = readConnectorIdentity(ctx, owned)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("readConnectorIdentity error = %v, want deadline exceeded", err)
	}
}

func TestReadConnectorIdentityConcurrentCloseCancelsAndJoins(t *testing.T) {
	started := make(chan struct{})
	connector := &fakeConnectorAPI{
		identity: func(ctx context.Context) (accumulo.InstanceInfo, string, error) {
			close(started)
			<-ctx.Done()
			return accumulo.InstanceInfo{}, "", ctx.Err()
		},
	}
	owned := newOwnedConnector(connector, &fakeConnectorInstance{})

	readDone := make(chan error, 1)
	go func() {
		ctx, done, err := owned.begin(0)
		if err != nil {
			readDone <- err
			return
		}
		defer done()
		_, _, err = readConnectorIdentity(ctx, owned)
		readDone <- err
	}()
	<-started

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- owned.close()
	}()
	if err := <-readDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("identity read error = %v, want canceled", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("close error = %v", err)
	}

	ctx, done, err := owned.begin(0)
	if done != nil {
		done()
	}
	if ctx != nil || !errors.Is(err, accumulo.ErrConnectorClosed) {
		t.Fatalf("begin after close = %v, %v; want connector closed", ctx, err)
	}
}
