package interaction_test

import (
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestSessionCanonicalPreservesExactEvidence(t *testing.T) {
	evidence := interaction.EvidenceReference{
		AnchorID: "anchor-1", Kind: interaction.EvidenceGraph,
		NodeIDs: []shoal.ID{"node-b", "node-a"},
		EdgeIDs: []shoal.ID{"edge-1"},
		Assertions: []interaction.AssertionReference{{
			AssertionID: "assertion-1", EdgeID: "edge-1",
			Origin: ontology.AssertionInferred,
		}},
	}
	session := interaction.Session{
		ID:           interaction.DerivedID("session", "exact-evidence"),
		RecordedAt:   time.Unix(1700000000, 0).UTC(),
		Operation:    interaction.OperationRetrieval,
		SeedNodeIDs:  []shoal.ID{"node-a", "node-b"},
		SeedEvidence: []interaction.EvidenceReference{evidence},
	}
	canonical, err := session.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if len(canonical.SeedEvidence) != 1 ||
		len(canonical.SeedEvidence[0].Assertions) != 1 ||
		canonical.SeedEvidence[0].Assertions[0].Origin !=
			ontology.AssertionInferred ||
		len(canonical.TouchedEdgeIDs()) != 1 {
		t.Fatalf("canonical evidence = %+v", canonical.SeedEvidence)
	}
	bad := evidence
	bad.Assertions[0].EdgeID = "missing-edge"
	session.SeedEvidence = []interaction.EvidenceReference{bad}
	if err := session.Validate(); err == nil {
		t.Fatal("assertion detached from its authoritative edge was accepted")
	}
}
