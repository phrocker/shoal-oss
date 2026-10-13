// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"testing"
)

// TestEvidenceBindingsAreBoundedInAggregate is #738.
//
// #722 caps one binding at four mebibytes and nothing capped their sum. A
// record holds up to MaxActionEvidence references and encodeAction refuses
// one past 3*MaxActionPayloadBytes, so bindings that are each legal can make
// the record collectively unwritable — and bindings are attached at
// completion, after the executor has performed the external effect, so the
// write that fails loses the outcome at the point losing it costs most.
func TestEvidenceBindingsAreBoundedInAggregate(t *testing.T) {
	// Deliberately uneven, so that when a binding does not fit there is
	// still budget left over. An even division leaves nothing remaining at
	// the overflow, and then a truncating implementation produces an empty
	// slice and is indistinguishable from one that drops the binding whole
	// — the first version of this test divided the budget in four and
	// passed with truncation in place.
	const tail = 100
	first := bytes.Repeat([]byte{'a'}, MaxActionEvidenceBindingBytes-tail)
	oversized := bytes.Repeat([]byte{'b'}, tail*2)
	fits := bytes.Repeat([]byte{'c'}, tail)

	evidence := make([]EvidenceRef, 4)
	bindings := [][]byte{first, oversized, fits, oversized}
	applyEvidenceBindings(evidence, bindings)

	if !bytes.Equal(evidence[0].Binding, first) {
		t.Fatalf("the first binding is %d bytes, want the %d the gate "+
			"returned", len(evidence[0].Binding), len(first))
	}
	// Dropped whole, never shortened. A member of a bound reference can be
	// covered by more than one entry, so cutting entries off a binding can
	// remove the one naming the labelled registration while a laxer entry
	// still covers the member — which opens exactly the evidence #705
	// closed. Only an empty binding means "unbound" to the read gate.
	if len(evidence[1].Binding) != 0 {
		t.Fatalf("the oversized binding was kept as %d bytes with %d of "+
			"budget left; a shortened binding can open evidence the full "+
			"one closed", len(evidence[1].Binding), tail)
	}
	// A smaller later binding still fits and is still exact.
	if !bytes.Equal(evidence[2].Binding, fits) {
		t.Fatalf("a binding inside the remaining budget was dropped: %d "+
			"bytes, want %d", len(evidence[2].Binding), len(fits))
	}
	if len(evidence[3].Binding) != 0 {
		t.Fatalf("a binding was kept past an exhausted budget: %d bytes",
			len(evidence[3].Binding))
	}

	total := 0
	for _, reference := range evidence {
		total += len(reference.Binding)
	}
	if total > MaxActionEvidenceBindingBytes {
		t.Fatalf("bindings total %d bytes, over the %d budget",
			total, MaxActionEvidenceBindingBytes)
	}
}

// TestEveryBindingIsKeptWhenTheyFit: the budget must not cost precision in
// ordinary use. A binding set built from hash-length identifiers runs to
// roughly three hundred kilobytes, well inside the budget, and an unbound
// reference is held to every registered revision — so a budget that bit at
// realistic sizes would over-deny readers for nothing.
func TestEveryBindingIsKeptWhenTheyFit(t *testing.T) {
	evidence := make([]EvidenceRef, MaxActionEvidence)
	bindings := make([][]byte, len(evidence))
	for index := range bindings {
		bindings[index] = bytes.Repeat([]byte("binding"), 64)
	}
	applyEvidenceBindings(evidence, bindings)
	for index, reference := range evidence {
		if !bytes.Equal(reference.Binding, bindings[index]) {
			t.Fatalf("reference %d lost its binding at a realistic size",
				index)
		}
	}
}

// TestAGateThatReturnsNoBindingStillClearsEveryReference guards the rule the
// budget must not weaken: result.Evidence is executor-supplied, so a binding
// the gate did not return must never survive onto the record. An executor
// that presented its own would name whatever registration set it liked, and
// the read gate trusts the binding to decide what the evidence is held to.
func TestAGateThatReturnsNoBindingStillClearsEveryReference(t *testing.T) {
	evidence := []EvidenceRef{
		{Binding: []byte("executor-chose-this")},
		{Binding: []byte("and-this")},
	}
	applyEvidenceBindings(evidence, nil)
	for index, reference := range evidence {
		if len(reference.Binding) != 0 {
			t.Fatalf("reference %d kept an executor-presented binding %q",
				index, reference.Binding)
		}
	}
}
