// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"testing"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestAWithheldMarkerIsNeverStored is the durability half of #696's marker.
//
// OutputWithheld is set by readableRecords on the copy handed to a reader. A
// stored one would make a redaction durable: the record would then read as
// withheld to everyone, including the action's own principal, who is exactly
// who the marker exists to serve. No writer produces it, so this refuses it
// rather than trusting that none ever will.
func TestAWithheldMarkerIsNeverStored(t *testing.T) {
	// Left exactly as the fixture builds it — valid, and queued rather than
	// succeeded, because the refusal is unconditional and a succeeded record
	// would drag in claim and execution requirements that say nothing about
	// the marker.
	record := testActionRecord()
	if _, err := encodeAction(record); err != nil {
		t.Fatalf("an ordinary record was refused, so this test compares "+
			"against nothing: %v", err)
	}

	record.OutputWithheld = true
	_, err := encodeAction(record)
	if err == nil {
		t.Fatal("a redacted record was stored; the redaction would then be " +
			"durable and withhold the Output from its own principal")
	}
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("err = %v, want invalid-argument", err)
	}

	// And #746's marker, which is the same read-path value for the same
	// reason: a stored one would withhold a reference from the operator who
	// needs it to reconcile with the other system, permanently.
	record.OutputWithheld = false
	record.AmbiguityReportsWithheld = true
	_, err = encodeAction(record)
	if err == nil {
		t.Fatal("a record with a durable withheld-reports marker was stored")
	}
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("err = %v, want invalid-argument", err)
	}
}
