package main

import (
	"testing"

	"github.com/phrocker/shoal/accumulo"
)

func TestFailReturnsOutOfMemoryWhenErrorMessageAllocationFails(t *testing.T) {
	testErrorMessageAllocFailAfter(0)
	defer testErrorMessageAllocReset()

	status, outErrorNil := testFailStatusWhenErrorMessageAllocationFails("shoal: invalid input")
	if status != 3 {
		t.Fatalf("status = %d, want 3", status)
	}
	if !outErrorNil {
		t.Fatal("outError should remain nil when error allocation fails")
	}
}

func TestAllocateWriteFailureReturnsNilOnBridgeStringAllocationFailure(t *testing.T) {
	tests := []struct {
		name      string
		failAfter uint
		data      writeFailureData
	}{
		{
			name:      "failed extent second string",
			failAfter: 1,
			data: writeFailureData{
				failedExtents: []flattenedFailedExtent{{
					server: "server:9997",
					value: accumulo.FailedExtent{
						Extent: accumulo.TabletExtent{TableID: "5"},
					},
				}},
			},
		},
		{
			name:      "constraint third string",
			failAfter: 2,
			data: writeFailureData{
				constraints: []flattenedConstraint{{
					server: "server:9997",
					value: accumulo.ConstraintViolation{
						ConstraintClass:            "Constraint",
						Description:                "bad mutation",
						NumberOfViolatingMutations: 1,
					},
				}},
			},
		},
		{
			name:      "authorization third string",
			failAfter: 2,
			data: writeFailureData{
				authorizations: []flattenedAuthorization{{
					server: "server:9997",
					value: accumulo.AuthorizationFailure{
						Extent: accumulo.TabletExtent{TableID: "5"},
						Code:   "PERMISSION_DENIED",
					},
				}},
			},
		},
		{
			name:      "cleanup second string",
			failAfter: 1,
			data: writeFailureData{
				cleanups: []flattenedCleanup{{
					server:  "server:9997",
					message: "cancel failed",
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testStringAllocFailAfter(test.failAfter)
			defer testStringAllocReset()

			if !testAllocateWriteFailureReturnedNil(test.data) {
				t.Fatal("allocateWriteFailure should return nil when bridge string allocation fails")
			}
		})
	}
}
