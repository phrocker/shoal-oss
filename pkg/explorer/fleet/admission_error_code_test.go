// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestAMisshapenAdmissionErrorCodeIsRefusedAndTheTokenSurvives is #748.
//
// #740 made a misshapen error code adjudicated rather than refused on the
// completion routes, because by then the executor has performed the work and
// refusing would strand it. The admission surface funnels into the same
// completeClaim, so it inherited an adjudication built for a situation it is
// not in: nothing has happened yet when its report arrives.
//
// The consequence is the one the call site's own comment already describes
// for the cases it refused before this: the report spends a live one-shot
// token on a failure the caller was never told about, and comes back
// recorded as invalid_executor_error with the caller's own code gone.
//
// The refusal is only worth having if the token survives it, so that is what
// this asserts. A test that stopped at "the report was refused" would pass
// against an implementation that burned the token on the way out.
func TestAMisshapenAdmissionErrorCodeIsRefusedAndTheTokenSurvives(t *testing.T) {
	for _, probe := range []struct {
		name string
		code string
		// reserved codes are the service's own constants, so naming one in
		// the refusal discloses nothing the caller did not already know.
		// Arbitrary bytes are the case the no-echo rule is for.
		serviceOwned bool
	}{
		{name: "prose", code: "the target said: no"},
		{name: "an embedded quotation", code: `it said "no"`},
		{name: "newlines", code: "refused\nby upstream"},
		{name: "uppercase", code: "UpstreamRefused"},
		{name: "over length",
			code: strings.Repeat("a", MaxReportedActionErrorCodeBytes+1)},
		// Already refused before #748, kept here so a change that replaces
		// the reservation with the shape rule cannot pass.
		{name: "a code the service assigns", code: "invalid_executor_error",
			serviceOwned: true},
	} {
		t.Run(probe.name, func(t *testing.T) {
			harness := newAdmissionHarness(t, nil)
			grant, err := harness.service.Request(
				harness.context(t, "request"), harness.request(
					"request", "admission", "complete",
					Effects{EffectEgressesContent}, nil))
			if err != nil {
				t.Fatal(err)
			}

			_, err = harness.service.Report(
				harness.context(t, "report"), AdmissionReport{
					Token: grant.Token, Failed: true,
					ErrorCode: probe.code,
					Context:   dispatchContext(harness.now, "report"),
				})
			if err == nil {
				t.Fatal("the report was accepted and its code adjudicated, " +
					"which spends the token on an outcome the caller never " +
					"reported")
			}
			if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
				t.Fatalf("err = %v, want invalid-argument", err)
			}
			// The refusal must not hand caller-controlled bytes back: an
			// error carrying them puts them in a log and a response body,
			// which is what the refusal exists to prevent. A reserved code
			// is exempt because it is one of the service's own constants
			// and naming it tells the caller something useful.
			if !probe.serviceOwned &&
				strings.Contains(err.Error(), probe.code) {
				t.Fatalf("the refusal echoed the code it refused: %v", err)
			}
			// A reserved code must be refused *as* reserved, not as
			// misshapen. invalid_executor_error is a perfectly good
			// lowercase token, so telling the caller its code is not one
			// would send them to fix the one thing that is already right.
			// This is also what makes the reservation check worth keeping
			// in front of the shape check, which would otherwise catch the
			// same values and say the wrong thing about them.
			if probe.serviceOwned &&
				!strings.Contains(err.Error(), "assigned by the service") {
				t.Fatalf("a reserved code was refused as misshapen: %v", err)
			}

			// The whole point: the token is still live, so the caller can
			// report the outcome it actually has.
			record, err := harness.service.Report(
				harness.context(t, "retry"), AdmissionReport{
					Token: grant.Token, Failed: true,
					ErrorCode: "upstream_refused",
					Context:   dispatchContext(harness.now, "retry"),
				})
			if err != nil {
				t.Fatalf("the refusal spent the token, so the caller can "+
					"never report what actually happened: %v", err)
			}
			if record.ErrorCode != "upstream_refused" {
				t.Fatalf("code = %q, want the caller's own", record.ErrorCode)
			}
			if record.ErrorCodeOrigin != ErrorCodeOriginExecutor {
				t.Fatalf("origin = %q, want %q: this code is the reporter's",
					record.ErrorCodeOrigin, ErrorCodeOriginExecutor)
			}
		})
	}
}

// TestAWellShapedAdmissionErrorCodeStillPasses keeps the refusal from being
// a blanket one: every code the tree produces is a token, including the
// effects gateway's composed target_rejected_NNN family, and empty is how a
// report says there was no error.
func TestAWellShapedAdmissionErrorCodeStillPasses(t *testing.T) {
	for _, code := range []string{
		"", "upstream_refused", "target_rejected_502", "quiet_failure",
	} {
		if err := validateAdmittedActionErrorCode(code); err != nil {
			t.Fatalf("a code the tree produces was refused: %q, %v",
				code, err)
		}
	}
}
