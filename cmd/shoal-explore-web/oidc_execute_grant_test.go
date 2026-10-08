// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package main

import (
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
)

// TestTheFleetMappingDoesNotGrantExecute is the guard on an upgrade hazard, and
// it exists because adding one line to a list was the whole of the mistake.
//
// oidcFleetOperations is not a ceiling something else narrows: authority()
// grants it verbatim to any token whose claim matches -oidc-fleet-values. Every
// fleet principal is minted with the same authorization domain, source and
// policy, and authorizeResource compares only those three — AuthorizeObject
// validates ObjectID and never consults it. So the only thing separating two
// fleet principals' queued work is the principal check that OperationExecute
// skips by design.
//
// Granting execute here would therefore hand every existing fleet-mapped token,
// on upgrade, the ability to pull another principal's records, claim them, and
// complete them with a fabricated outcome — the exact property the operation
// was introduced to prevent.
//
// So execute stays unreachable through this mapping until it has one of its
// own. That makes #437's capability real in the authorization model and not yet
// reachable by an OIDC-minted token, which is the honest state and is why this
// asserts an absence rather than a presence.
func TestTheFleetMappingDoesNotGrantExecute(t *testing.T) {
	for _, operations := range []struct {
		name string
		set  []auth.Operation
	}{
		{"fleet", oidcFleetOperations},
		{"contributor", oidcContributorOperations},
		{"reader", oidcReaderOperations},
	} {
		for _, operation := range operations.set {
			if operation == auth.OperationExecute {
				t.Fatalf("the %s mapping grants OperationExecute. Every "+
					"principal it mints shares one authorization scope, so this "+
					"lets any of them claim and complete another's queued work",
					operations.name)
			}
		}
	}

	// And the operation is still a real one, so this is a statement about the
	// mapping rather than about the vocabulary. A test that passed because the
	// constant had been deleted would assert nothing.
	if err := auth.OperationExecute.Validate(); err != nil {
		t.Fatalf("OperationExecute is not a valid operation: %v", err)
	}
	if !auth.ServiceRoleActionExecution.Allows(auth.OperationExecute) {
		t.Fatal("ServiceRoleActionExecution no longer grants execute")
	}
}
