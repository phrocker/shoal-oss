// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestADeclaredVocabularyIsCanonical pins what a descriptor may declare.
//
// Refusing here is safe in a way refusing at completion is not, and that
// asymmetry is the whole shape of #740. Registration happens before any work
// runs, so nothing has been performed that a refusal would strand; by
// completion the executor has already done whatever the action describes,
// which is why a reported code is adjudicated instead.
//
// Sorted and deduplicated because the registry digest covers the vocabulary
// (v3): two descriptors declaring the same codes in a different order are
// the same descriptor, and a reordering must not re-register as a new
// generation.
func TestADeclaredVocabularyIsCanonical(t *testing.T) {
	codes, err := canonicalErrorCodes(
		[]string{"upstream_refused", "input_invalid", "upstream_refused"})
	if err != nil {
		t.Fatal(err)
	}
	if len(codes) != 2 || codes[0] != "input_invalid" ||
		codes[1] != "upstream_refused" {
		t.Fatalf("codes = %v, want them sorted and deduplicated", codes)
	}
	// Nil rather than an empty slice, so a descriptor that declares nothing
	// is byte-identical to one written before the field existed.
	if empty, err := canonicalErrorCodes(nil); err != nil || empty != nil {
		t.Fatalf("declaring nothing = %v, %v; want nil", empty, err)
	}

	for _, probe := range []struct {
		name  string
		codes []string
	}{
		{"prose", []string{"the target said: no"}},
		{"uppercase", []string{"Upstream_Refused"}},
		{"a leading digit", []string{"502_rejected"}},
		{"empty", []string{""}},
		{"over length", []string{
			strings.Repeat("a", MaxReportedActionErrorCodeBytes+1)}},
		// A descriptor cannot claim a code the service assigns. Otherwise
		// an action could declare invalid_executor_error and an executor
		// reporting it would be recorded as the executor's own word, which
		// is exactly what #508 reserves those codes to prevent.
		{"a service code", []string{"invalid_executor_error"}},
		{"another service code", []string{"executor_error"}},
		{"too many", make([]string, MaxActionErrorCodes+1)},
	} {
		t.Run(probe.name, func(t *testing.T) {
			if _, err := canonicalErrorCodes(probe.codes); err == nil {
				t.Fatalf("a descriptor declared %v", probe.codes)
			}
		})
	}
}

// TestADeclaredVocabularyReachesTheDescriptor is the join: canonicalisation
// is worth nothing if the registration path drops the field on the way, and
// a unit test of canonicalErrorCodes cannot see that.
func TestADeclaredVocabularyReachesTheDescriptor(t *testing.T) {
	schema := json.RawMessage(`{"type":"object"}`)
	capabilities, err := canonicalCapabilities([]Capability{{
		Name: "ops", Actions: []Action{{
			Name: "deploy", InputSchema: schema, OutputSchema: schema,
			// Out of order, to prove canonicalisation ran on this path
			// and not merely that the slice survived it.
			ErrorCodes: []string{"upstream_refused", "input_invalid"},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	declared := capabilities[0].Actions[0].ErrorCodes
	if len(declared) != 2 || declared[0] != "input_invalid" {
		t.Fatalf("the descriptor's vocabulary is %v, want it canonical",
			declared)
	}
	if !capabilities[0].Actions[0].declaresErrorCode("upstream_refused") {
		t.Fatal("a declared code is not recognized as declared")
	}
	if capabilities[0].Actions[0].declaresErrorCode("upstream_unreachable") {
		t.Fatal("an undeclared code is recognized as declared")
	}

	// And a bad vocabulary is refused by the registration path, not only by
	// the helper.
	if _, err := canonicalCapabilities([]Capability{{
		Name: "ops", Actions: []Action{{
			Name: "deploy", InputSchema: schema, OutputSchema: schema,
			ErrorCodes: []string{"the target said: no"},
		}},
	}}); err == nil {
		t.Fatal("a descriptor registered prose as its failure vocabulary")
	}
}

// TestADeclaredVocabularySurvivesTheRegistryWire pins the HTTP contract.
//
// Action defines MarshalJSON and UnmarshalJSON, so its struct tags are dead
// and the hand-written field lists are what the wire carries. The registry
// wire embeds []Capability directly, so a client registering over HTTP with
// error_codes would have them silently dropped by a field list that had not
// been extended — which reads as an agent that declares nothing, and under a
// closed vocabulary an agent that declares nothing has every failure code
// replaced.
//
// The durable codec is a separate encoding with the same hazard and its own
// test; neither covers the other, and the field was missing from both.
func TestADeclaredVocabularySurvivesTheRegistryWire(t *testing.T) {
	schema := json.RawMessage(`{"type":"object"}`)
	action := Action{
		Name: "query", InputSchema: schema, OutputSchema: schema,
		ErrorCodes: []string{"input_invalid", "upstream_refused"},
	}
	encoded, err := json.Marshal(action)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"error_codes"`) {
		t.Fatalf("the declared vocabulary is not on the wire: %s", encoded)
	}

	var decoded Action
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.ErrorCodes) != 2 ||
		decoded.ErrorCodes[0] != "input_invalid" ||
		decoded.ErrorCodes[1] != "upstream_refused" {
		t.Fatalf("the vocabulary did not survive the round trip: %v",
			decoded.ErrorCodes)
	}

	// Omitted when empty, so an action that declares nothing marshals
	// byte-for-byte as it did before the field existed — which is what
	// keeps descriptorDigest unchanged for every existing descriptor, the
	// same reason requires_approval and requires_attestation are omitted.
	plain, err := json.Marshal(Action{
		Name: "query", InputSchema: schema, OutputSchema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "error_codes") {
		t.Fatalf("an action declaring nothing names the field: %s", plain)
	}
}
