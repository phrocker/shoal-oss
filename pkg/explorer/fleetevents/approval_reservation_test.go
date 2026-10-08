// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleetevents

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
)

// TestPublicPublishReservesApprovalKinds pins the approval namespace (#451).
// Approval transitions publish nothing yet, which is exactly when a forged
// "approval.approved" would go uncontradicted, so every approval kind —
// including ones no build emits — is refused on the public route and admitted
// by no operation on the trusted one.
//
// The refusal is asserted by its message, not merely by an error: a kind the
// event validator rejected for some other reason would pass a bare err != nil
// check without the reservation existing at all.
func TestPublicPublishReservesApprovalKinds(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	backend := &memoryBackend{}
	service := testService(
		t, "publisher", now, backend, &generationReader{generation: 7},
		&leaseValidator{}, &auditor{},
	)
	for _, kind := range []string{
		"approval.requested", "approval.approved", "approval.refused",
		"approval.expired", "approval.enqueued", "approval.anything_later",
	} {
		request := PublishRequest{
			Token: []byte("public-token"), RetryUntil: now.Add(time.Hour),
			Event: eventAt(0),
		}
		request.Event.Kind = kind
		_, err := service.Publish(context.Background(), request)
		if err == nil || !strings.Contains(
			err.Error(), "require trusted publication") {
			t.Fatalf("public publish of %q = %v, want the reservation", kind, err)
		}
		for _, operation := range []auth.Operation{
			auth.OperationDispatch, auth.OperationInvoke,
			auth.OperationExecute, auth.OperationActionApprove,
			auth.OperationEventPublish,
		} {
			if lifecyclePublicationPermits(kind, operation) {
				t.Fatalf("lifecyclePublicationPermits(%q, %q) = true",
					kind, operation)
			}
		}
	}
	if len(backend.events) != 0 {
		t.Fatalf("reserved approval events appended: %d", len(backend.events))
	}
	// The reservation is a prefix, not a substring: a kind that merely
	// mentions approval elsewhere is not reserved, and an ordinary kind still
	// publishes through the same service, so the refusals above are the
	// reservation and not a service that refuses everything.
	if isReservedLifecycleKind("gateway.approval_requested") {
		t.Fatal("a non-approval kind was reserved")
	}
	control := PublishRequest{
		Token: []byte("control-token"), RetryUntil: now.Add(time.Hour),
		Event: eventAt(0),
	}
	if _, err := service.Publish(context.Background(), control); err != nil {
		t.Fatalf("control publish = %v", err)
	}
}
