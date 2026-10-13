// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"context"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/evidencelabels"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// bindingGate is a node gate that can bind, which is what the authorized
// client's NodeGate does and what a gate from anywhere else cannot.
type bindingGate struct {
	*catalogGate
	bindings  [][]byte
	valid     bool
	bindCalls int
	sawGraphs []evidencelabels.Graph
}

func (g *bindingGate) BindGraphEvidence(
	_ context.Context, _ shoal.ID, _ time.Time,
	references []interaction.EvidenceReference,
) ([][]byte, bool, error) {
	g.bindCalls++
	if !g.valid {
		return nil, false, nil
	}
	if g.bindings != nil {
		return g.bindings, true, nil
	}
	result := make([][]byte, len(references))
	for i := range references {
		result[i] = []byte{byte('b'), byte('0' + i)}
	}
	return result, true, nil
}

func (g *bindingGate) GraphsVisibleToReader(
	ctx context.Context, graphs []evidencelabels.Graph,
) ([]bool, error) {
	g.sawGraphs = append(g.sawGraphs, graphs...)
	return g.catalogGate.GraphsVisibleToReader(ctx, graphs)
}

// TestARecordedReferenceCarriesItsBinding is the fleet half of #705.
//
// Node and edge identities do not depend on a revision — a document node is
// hashed from its URI, a span from its text — so a relabel moves the
// registrations a reference's members belong to, and deciding on the current
// ones alone opened evidence recorded over a labelled revision. The binding
// is what the recorder saw, stored with the reference and handed back on
// every read.
//
// The fleet plane never reads it. What it must do is store it, keep it
// through a redaction clone, and pass exactly it to the gate.
func TestARecordedReferenceCarriesItsBinding(t *testing.T) {
	fixture := labelledFixture(t)
	gate := &bindingGate{
		catalogGate: fixtureCatalog(t, fixture.authority.Resolver(), fixture.now),
		valid:       true,
	}
	fixture.service.evidenceNodes = gate

	_, open := structuredEvidence(t)
	result := ExecutionResult{
		Evidence:             []EvidenceRef{open},
		EvidenceSnapshotID:   "snapshot",
		EvidenceSnapshotAsOf: fixture.now,
	}
	bindings, joined, err := fixture.service.evidencePathsJoin(
		fixture.enqueuer, result)
	if err != nil || !joined {
		t.Fatalf("evidencePathsJoin = %v, %v", joined, err)
	}
	if gate.bindCalls != 1 {
		t.Fatalf("the binder was called %d times, want 1: a gate that can "+
			"bind must not fall through to the validate-only path",
			gate.bindCalls)
	}
	if len(bindings) != 1 || string(bindings[0]) != "b0" {
		t.Fatalf("bindings = %q, want the gate's answer verbatim", bindings)
	}

	// Stored on the reference, and handed back to the read gate as-is.
	stored := cloneActionRecord(
		fixture.dispatchStore.records[string(fixture.queued.ID)])
	stored.Evidence = []EvidenceRef{open}
	stored.Evidence[0].Binding = bindings[0]
	stored.EvidenceSnapshotID = "snapshot"
	stored.EvidenceSnapshotAsOf = fixture.now

	if _, err := fixture.service.readableRecord(fixture.enqueuer, stored); err != nil {
		t.Fatal(err)
	}
	if len(gate.sawGraphs) == 0 {
		t.Fatal("the read gate was asked about no graph, so this test " +
			"pins nothing about what it was handed")
	}
	if string(gate.sawGraphs[0].Binding) != "b0" {
		t.Fatalf("the gate saw Binding %q, want the stored one: an unbound "+
			"reference is held to every registered revision, so losing it "+
			"here over-closes every read",
			gate.sawGraphs[0].Binding)
	}

	// A clone must not share the binding's backing array with its original,
	// for the reason every other slice on the reference is copied.
	clone := cloneActionEvidence(stored.Evidence)
	if len(clone[0].Binding) == 0 {
		t.Fatal("the clone dropped the binding")
	}
	clone[0].Binding[0] = 'X'
	if stored.Evidence[0].Binding[0] == 'X' {
		t.Fatal("the clone shares the binding's array with its original")
	}
}

// TestAGateThatCannotBindStillValidates pins the fallback. NodeGate carries no
// BindGraphEvidence, so a gate implemented outside the authorized client takes
// this path: its references are stored unbound, which the read gate holds to
// every registered revision — over-closing, never opening.
func TestAGateThatCannotBindStillValidates(t *testing.T) {
	fixture := labelledFixture(t)
	plain := fixtureCatalog(t, fixture.authority.Resolver(), fixture.now)
	fixture.service.evidenceNodes = plain

	_, open := structuredEvidence(t)
	bindings, joined, err := fixture.service.evidencePathsJoin(
		fixture.enqueuer, ExecutionResult{
			Evidence:             []EvidenceRef{open},
			EvidenceSnapshotID:   "snapshot",
			EvidenceSnapshotAsOf: fixture.now,
		})
	if err != nil {
		t.Fatal(err)
	}
	if !joined {
		t.Fatal("a validate-only gate refused evidence it used to accept")
	}
	if bindings != nil {
		t.Fatalf("bindings = %q, want none: this gate cannot bind, and a "+
			"fabricated binding would claim a registration set nobody read",
			bindings)
	}
}

// TestRefusedEvidenceBindsNothing pins that a refusal still refuses. The
// binding call replaced the validation, so a gate answering false must not
// become a record that keeps its evidence.
func TestRefusedEvidenceBindsNothing(t *testing.T) {
	fixture := labelledFixture(t)
	gate := &bindingGate{
		catalogGate: fixtureCatalog(t, fixture.authority.Resolver(), fixture.now),
		valid:       false,
	}
	fixture.service.evidenceNodes = gate

	_, open := structuredEvidence(t)
	bindings, joined, err := fixture.service.evidencePathsJoin(
		fixture.enqueuer, ExecutionResult{
			Evidence:             []EvidenceRef{open},
			EvidenceSnapshotID:   "snapshot",
			EvidenceSnapshotAsOf: fixture.now,
		})
	if err != nil {
		t.Fatal(err)
	}
	if joined {
		t.Fatal("a gate that refused the evidence was read as accepting it")
	}
	if bindings != nil {
		t.Fatalf("bindings = %q, want none for refused evidence", bindings)
	}
}

// TestAnExecutorCannotSupplyItsOwnBinding is the integrity half of #705.
//
// result.Evidence comes from the executor, and the read gate trusts a
// reference's binding to decide which registrations it is held to. An
// executor that could set one would name whatever registration set it liked
// and open its own evidence. Only the gate's answer may reach the record.
func TestAnExecutorCannotSupplyItsOwnBinding(t *testing.T) {
	forged := []byte("forged-by-the-executor")
	for _, probe := range []struct {
		name     string
		bindings [][]byte
		want     string
	}{
		{"no gate bound anything", nil, ""},
		{"the gate bound fewer references than were supplied",
			[][]byte{[]byte("real")}, ""},
		{"the gate bound this reference", [][]byte{nil, []byte("real")}, "real"},
	} {
		t.Run(probe.name, func(t *testing.T) {
			evidence := []EvidenceRef{
				{AnchorID: "first", Binding: forged},
				{AnchorID: "second", Binding: forged},
			}
			applyEvidenceBindings(evidence, probe.bindings)
			if got := string(evidence[1].Binding); got != probe.want {
				t.Fatalf("binding = %q, want %q: an executor-supplied "+
					"binding must never survive, and only the gate's answer "+
					"may take its place", got, probe.want)
			}
			if string(evidence[0].Binding) == string(forged) {
				t.Fatal("the first reference kept the executor's binding")
			}
		})
	}
}
