// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/collector"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestAdminWireWorstCases measures the largest admin receipt and
// registration record: every ID at shoal.MaxIDBytes, the most authority IDs
// and change summaries, the widest numbers and timestamps.
func TestAdminWireWorstCases(t *testing.T) {
	long := func(prefix string) string {
		return EncodeID(shoal.ID(prefix + strings.Repeat("x", shoal.MaxIDBytes-len(prefix))))
	}
	at := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	receipt := AdminReceipt{Schema: Schema, CollectorID: long(""), Generation: 1<<63 - 1, State: string(collector.Provisioned), Changed: true, Sequence: 1<<63 - 1}
	var b bytes.Buffer
	if e := json.NewEncoder(&b).Encode(receipt); e != nil {
		t.Fatal(e)
	}
	t.Logf("admin receipt worst case: %d bytes", b.Len())
	if b.Len() > MaxAdminReceiptBytes {
		t.Fatalf("admin receipt is %d bytes, above MaxAdminReceiptBytes", b.Len())
	}

	record := RegistrationRecord{Schema: Schema, CollectorID: long(""), Subject: long(""), ClientID: long(""), Domain: EncodeKey(bytes.Repeat([]byte("d"), shoal.MaxIDBytes)), Control: string(collector.CandidateControlled), Mode: string(collector.ServerObserved), Generation: 1<<63 - 1, State: string(collector.Enrolled), EnrollmentID: long("enrollment:"), ChangeCount: 1<<63 - 1}
	for i := range collector.MaxAuthorityPolicyIDs {
		record.AuthorityPolicyIDs = append(record.AuthorityPolicyIDs, long(fmt.Sprintf("%02d", i)))
	}
	for i := range MaxRegistrationChanges {
		record.Changes = append(record.Changes, ChangeSummary{Sequence: 1<<63 - 1 - int64(i), Action: ChangeProvision, Source: SourceCollectorAdmin, Generation: 1<<63 - 1, State: string(collector.Provisioned), At: at, By: &AdminAttribution{Subject: long(""), Actor: long(""), ClientID: long(""), RequestID: long(""), CorrelationID: long(""), AuthorizationFingerprint: "auth-sha256:" + strings.Repeat("f", 64)}})
	}
	b.Reset()
	if e := json.NewEncoder(&b).Encode(record); e != nil {
		t.Fatal(e)
	}
	t.Logf("registration record worst case: %d bytes", b.Len())
	if b.Len() > MaxResponseBytes {
		t.Fatalf("registration record is %d bytes, above MaxResponseBytes", b.Len())
	}
}

func TestAdminWireRoundTripAndValidation(t *testing.T) {
	p := Provisioning{CollectorID: "collector:a", Subject: "subject", Domain: []byte("domain"), AuthorityPolicyIDs: []shoal.ID{"b", "a"}, Control: collector.ExternalControlled, Mode: collector.Imported, Generation: 3}
	got, e := DecodeProvision(EncodeProvision(p))
	if e != nil || got.CollectorID != p.CollectorID || got.ClientID != "" || got.Generation != 3 || got.AuthorityPolicyIDs[0] != "a" {
		t.Fatalf("round trip %+v %v", got, e)
	}
	for name, mutate := range map[string]func(*ProvisionRequest){
		"zero generation":     func(w *ProvisionRequest) { w.Generation = 0 },
		"negative generation": func(w *ProvisionRequest) { w.Generation = -1 },
		"empty subject":       func(w *ProvisionRequest) { w.Subject = "" },
		"non-canonical id":    func(w *ProvisionRequest) { w.CollectorID += "=" },
		"empty domain":        func(w *ProvisionRequest) { w.Domain = "" },
		"no authority":        func(w *ProvisionRequest) { w.AuthorityPolicyIDs = []string{} },
		"bad control":         func(w *ProvisionRequest) { w.Control = "trusted" },
		"bad mode":            func(w *ProvisionRequest) { w.Mode = "" },
	} {
		w := EncodeProvision(p)
		mutate(&w)
		if _, e := DecodeProvision(w); e == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if _, _, e := DecodeRevoke(RevokeRequest{CollectorID: EncodeID("c"), Generation: 0}); e == nil {
		t.Fatal("revoke generation 0 accepted")
	}
	for name, r := range map[string]AdminReceipt{
		"changed without sequence": {Schema: Schema, CollectorID: EncodeID("c"), Generation: 1, State: "provisioned", Changed: true},
		"sequence without changed": {Schema: Schema, CollectorID: EncodeID("c"), Generation: 1, State: "provisioned", Sequence: 1},
		"unknown state":            {Schema: Schema, CollectorID: EncodeID("c"), Generation: 1, State: "active"},
	} {
		if r.Validate() == nil {
			t.Fatalf("%s receipt accepted", name)
		}
	}
	record := RegistrationRecord{Schema: Schema, CollectorID: EncodeID("c"), Subject: EncodeID("s"), Domain: EncodeKey([]byte("d")), AuthorityPolicyIDs: []string{EncodeID("a"), EncodeID("b")}, Control: string(collector.ExternalControlled), Mode: string(collector.ServerObserved), Generation: 2, State: "revoked", ChangeCount: 2, Changes: []ChangeSummary{
		{Sequence: 2, Action: ChangeRevoke, Source: SourceOperatorAPI, Generation: 2, State: "revoked", At: time.Now().UTC()},
		{Sequence: 1, Action: ChangeProvision, Source: SourceCollectorAdmin, Generation: 1, State: "provisioned", At: time.Now().UTC(), By: &AdminAttribution{Subject: EncodeID("admin"), Actor: EncodeID("admin"), RequestID: EncodeID("r"), AuthorizationFingerprint: "auth-sha256:00"}},
	}}
	if e := record.Validate(); e != nil {
		t.Fatalf("valid record refused: %v", e)
	}
	for name, mutate := range map[string]func(*RegistrationRecord){
		"unsorted authority":       func(r *RegistrationRecord) { r.AuthorityPolicyIDs = []string{EncodeID("b"), EncodeID("a")} },
		"oldest first":             func(r *RegistrationRecord) { r.Changes[0], r.Changes[1] = r.Changes[1], r.Changes[0] },
		"admin change without by":  func(r *RegistrationRecord) { r.Changes[1].By = nil },
		"operator change with by":  func(r *RegistrationRecord) { r.Changes[0].By = r.Changes[1].By },
		"revoke not revoked":       func(r *RegistrationRecord) { r.Changes[0].State = "provisioned" },
		"more changes than count":  func(r *RegistrationRecord) { r.ChangeCount = 1 },
		"enrollment while revoked": func(r *RegistrationRecord) { r.EnrollmentID = EncodeID("e") },
	} {
		changed := record
		changed.Changes = append([]ChangeSummary(nil), record.Changes...)
		mutate(&changed)
		if changed.Validate() == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}
