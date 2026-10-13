// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestARecordAtTheBindingBudgetStillEncodes is the half of #738 that is
// actually a claim about this package.
//
// fleet.MaxActionEvidenceBindingBytes exists so that a completion cannot
// fail at encodeAction after the executor has already performed the external
// effect: bindings are attached at completion, and a record that cannot be
// encoded is an outcome lost at the point losing it costs most. The budget
// is only worth having if a record carrying it fits, so this builds the
// largest record the service can produce and encodes it.
//
// Everything caller-growable is at its ceiling at once: Input and Output at
// their payload bounds, MaxActionEvidence references, bindings summing to
// the budget, and both lists that grow on caller-triggered transitions — the
// claim history and the ambiguity reports — full and each field at its own
// bound. A fixture that maxed only the bindings would leave the headroom
// those consume unmeasured, and the question here is whether the sum fits.
func TestARecordAtTheBindingBudgetStillEncodes(t *testing.T) {
	record := testActionRecord()
	record.State = fleet.DispatchSucceeded
	record.ClaimID = []byte("claim")
	record.ClaimFence = 1
	record.ClaimLease = time.Minute
	record.ClaimLeaseUntil = record.UpdatedAt.Add(time.Minute)
	record.ExecutionPolicyGeneration = 1
	record.ExecutionExpiresAt = record.UpdatedAt.Add(time.Hour)
	record.EvidenceSnapshotID = "snapshot"
	record.EvidenceSnapshotAsOf = record.UpdatedAt

	payload := func(fill string, size int) json.RawMessage {
		body := strings.Repeat(fill, size-len(`{"v":""}`))
		return json.RawMessage(`{"v":"` + body + `"}`)
	}
	record.Input = payload("i", fleet.MaxActionPayloadBytes)
	record.Output = payload("o", fleet.MaxActionOutputBytes)

	// One member per reference, because the total members across all
	// references is itself bounded at MaxActionEvidence — so the most
	// references a record can carry is that many with one member each,
	// which is also the most bindings it can carry.
	perBinding := fleet.MaxActionEvidenceBindingBytes / fleet.MaxActionEvidence
	for index := 0; index < fleet.MaxActionEvidence; index++ {
		record.Evidence = append(record.Evidence, fleet.EvidenceRef{
			AnchorID: shoal.ID(fmt.Sprintf("anchor-%d", index)),
			Kind:     interaction.EvidenceGraph,
			NodeIDs:  []shoal.ID{shoal.ID(fmt.Sprintf("node-%d", index))},
			Binding:  bytes.Repeat([]byte{'b'}, perBinding),
		})
	}

	for index := 0; index < fleet.MaxActionClaimHistory; index++ {
		record.ClaimHistory = append(record.ClaimHistory, fleet.ClaimHolder{
			Subject: "subject", Actor: "actor", ClientID: "client",
			ClaimID:    []byte(fmt.Sprintf("held-claim-%d", index)),
			ClaimFence: uint64(index + 1), HeldAt: record.UpdatedAt,
		})
	}
	for index := 0; index < fleet.MaxActionAmbiguityReports; index++ {
		record.AmbiguityReports = append(
			record.AmbiguityReports, fleet.AmbiguityReport{
				ClaimFence: uint64(index + 1),
				Outcome:    fleet.AmbiguityEffectObserved,
				Target:     strings.Repeat("t", fleet.MaxAmbiguityTargetBytes),
				Reference: strings.Repeat(
					"r", fleet.MaxAmbiguityReferenceBytes),
				Subject: "subject", Actor: "actor",
				ReportedAt: record.UpdatedAt,
			})
	}

	encoded, err := encodeAction(record)
	if err != nil {
		t.Fatalf("the largest record the service can produce cannot be "+
			"written, so a completion carrying it loses an effect that has "+
			"already happened: %v", err)
	}
	limit := 3 * fleet.MaxActionPayloadBytes
	if len(encoded) > limit {
		t.Fatalf("encoded = %d bytes, over the %d bound", len(encoded), limit)
	}
	t.Logf("largest producible record: %d bytes of %d, %d to spare",
		len(encoded), limit, limit-len(encoded))

	// And the bindings survive the round trip: a budget that fit because
	// the codec dropped them would prove nothing.
	decoded, err := decodeAction(encoded)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for index, reference := range decoded.Evidence {
		total += len(reference.Binding)
		if !bytes.Equal(reference.Binding, record.Evidence[index].Binding) {
			t.Fatalf("reference %d lost its binding in the codec", index)
		}
	}
	if total != fleet.MaxActionEvidenceBindingBytes {
		t.Fatalf("decoded bindings total %d, want the full budget %d",
			total, fleet.MaxActionEvidenceBindingBytes)
	}
}
