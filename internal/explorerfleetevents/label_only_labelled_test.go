// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleetevents

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
)

// TestAnOutsiderOfOnlyLabelledEvidenceSeesTheWireOfNone: an action whose only
// evidence is labelled reads, for an outsider, exactly as an action that
// recorded no evidence at all, on Status, TeamActions, the replays, and the
// HTTP wire, whose evidence_snapshot_id would otherwise say evidence was
// withheld (#398, #564).
func TestAnOutsiderOfOnlyLabelledEvidenceSeesTheWireOfNone(t *testing.T) {
	now := time.Now().UTC().Add(time.Minute).Truncate(time.Second)
	labelled := newLabelPlaneWith(t, planeOptions{
		choose: fullLabels, wireEvents: true, label: "secret",
		now: now, evidence: "labelled",
	})
	none := newLabelPlaneWith(t, planeOptions{
		choose: fullLabels, wireEvents: true, label: "secret",
		now: now, evidence: "none",
	})
	if labelled.completed.EvidenceSnapshotID == "" || len(labelled.completed.Evidence) != 1 {
		t.Fatal("the labelled plane recorded no pinned evidence; the probe is vacuous")
	}
	read := func(p labelPlane) (fleet.ActionRecord, fleet.ActionRecord, fleet.ActionRecord, fleet.ActionRecord, []byte) {
		t.Helper()
		outsider := p.reader(t)
		request := integrationRequestContext(p.now)
		status, err := p.dispatch.Status(outsider, fleet.StatusRequest{ID: p.completed.ID, Context: request})
		if err != nil {
			t.Fatal(err)
		}
		team, err := p.dispatch.TeamActions(outsider, fleet.TeamActionListRequest{
			Limit: 10, SourceIDs: [][]byte{[]byte("source")},
			PolicyIDs: [][]byte{[]byte("policy")}, Context: request,
		})
		if err != nil || len(team.Actions) != 1 {
			t.Fatalf("TeamActions = %d, %v", len(team.Actions), err)
		}
		owner := bindPlaneSubjectWith(t, p.authority, p.now, "owner", nil,
			auth.OperationDispatch, auth.OperationInvoke)
		enqueue := integrationEnqueueRequest(p.now, "labelled", "enqueue-labelled")
		enqueued, err := p.dispatch.Enqueue(owner, enqueue)
		if err != nil {
			t.Fatal(err)
		}
		invoked, err := p.dispatch.Invoke(owner, fleet.InvokeRequest{
			Enqueue: enqueue, ClaimID: []byte("claim-labelled"), Lease: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		handler, err := webapi.NewFleetDispatchHandler(p.dispatch)
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(map[string]any{
			"request_id":     base64.RawURLEncoding.EncodeToString([]byte("request")),
			"correlation_id": base64.RawURLEncoding.EncodeToString([]byte("correlation")),
			"reason_code":    "test", "deadline": p.now.Add(time.Minute),
		})
		if err != nil {
			t.Fatal(err)
		}
		httpRequest := httptest.NewRequest(http.MethodPost,
			"http://fleet.test/api/v1/fleet/actions/"+
				base64.RawURLEncoding.EncodeToString(p.completed.ID)+"/status",
			bytes.NewReader(body)).WithContext(outsider)
		httpRequest.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httpRequest)
		if response.Code != http.StatusOK {
			t.Fatalf("HTTP status = %d: %s", response.Code, response.Body.String())
		}
		return status, team.Actions[0], enqueued, invoked, response.Body.Bytes()
	}
	gotStatus, gotTeam, gotEnqueue, gotInvoke, gotWire := read(labelled)
	wantStatus, wantTeam, wantEnqueue, wantInvoke, wantWire := read(none)
	// This reader is the action's own principal holding none of its labels —
	// an outsider to the evidence, not to the action. Since #696 it is told
	// that an Output was withheld rather than handed one derived from
	// evidence it may not see, and OutputWithheld is that statement. The
	// concession is deliberate and only to this class of reader; a third
	// party is answered as though the action does not exist, which
	// TestAThirdPartyIsNotToldTheActionExists below covers.
	//
	// So the parity claim is narrowed to exactly one field and strengthened
	// everywhere else: nothing *but* the Output pair may vary with whether
	// the evidence was labelled. That is the #623 bar, and it is a stronger
	// statement than the original, which compared whole records and so would
	// have accepted a second field drifting alongside the first.
	for _, path := range []struct {
		name      string
		got, want fleet.ActionRecord
	}{
		{"Status", gotStatus, wantStatus}, {"TeamActions", gotTeam, wantTeam},
		{"enqueue replay", gotEnqueue, wantEnqueue}, {"invoke replay", gotInvoke, wantInvoke},
	} {
		if !path.got.OutputWithheld {
			t.Fatalf("%s: the labelled plane did not mark its Output "+
				"withheld, so the reader was handed output derived from "+
				"evidence it cannot see", path.name)
		}
		if len(path.got.Output) != 0 {
			t.Fatalf("%s: Output survived alongside the marker: %s",
				path.name, path.got.Output)
		}
		if path.want.OutputWithheld || len(path.want.Output) == 0 {
			t.Fatalf("%s: the unlabelled plane withheld its Output, so the "+
				"comparison has nothing to distinguish and is vacuous",
				path.name)
		}
		normalized := path.got
		normalized.Output = path.want.Output
		normalized.OutputWithheld = path.want.OutputWithheld
		if !reflect.DeepEqual(normalized, path.want) {
			t.Fatalf("%s to a reader of no labels, ignoring the Output pair:"+
				"\n%#v\nwant, as if no evidence was recorded,\n%#v",
				path.name, normalized, path.want)
		}
	}
	// The wire is compared field by field with the Output pair removed, not
	// merely asserted to differ. "They differ" would pass if some unrelated
	// field had drifted too, which is the whole thing this parity test is
	// for.
	var gotFields, wantFields map[string]any
	if err := json.Unmarshal(gotWire, &gotFields); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(wantWire, &wantFields); err != nil {
		t.Fatal(err)
	}
	if gotFields["output_withheld"] != true {
		t.Fatalf("the wire did not carry the withheld marker: %s", gotWire)
	}
	if _, present := gotFields["output"]; present {
		t.Fatalf("the wire carried an Output alongside the marker: %s", gotWire)
	}
	if _, present := wantFields["output"]; !present {
		t.Fatalf("the unlabelled plane's wire carried no Output, so the "+
			"comparison has nothing to distinguish: %s", wantWire)
	}
	for _, field := range []string{"output", "output_withheld"} {
		delete(gotFields, field)
		delete(wantFields, field)
	}
	if !reflect.DeepEqual(gotFields, wantFields) {
		t.Fatalf("the wire differs outside the Output pair:\n%#v\nwant\n%#v",
			gotFields, wantFields)
	}
}

// TestAThirdPartyIsNotToldTheActionExists is the other side of the #696
// decision, at the plane rather than the unit.
//
// The reader above is the action's own principal and keeps the record with
// the marker. A third party — the TeamActions audience the issue is about —
// is answered as though the action does not exist, because for it a marker
// would be exactly the disclosure the marker exists to make.
//
// This is where the decision's cost is visible and should stay visible: the
// action is present in a third party's page while it is queued, and gone once
// it succeeds carrying evidence that reader cannot see. That is recorded on
// #696 as a residual, not claimed as indistinguishability.
func TestAThirdPartyIsNotToldTheActionExists(t *testing.T) {
	now := time.Now().UTC().Add(time.Minute).Truncate(time.Second)
	plane := newLabelPlaneWith(t, planeOptions{
		choose: fullLabels, wireEvents: true, label: "secret",
		now: now, evidence: "labelled",
	})
	stranger := bindPlaneSubjectWith(t, plane.authority, plane.now, "stranger",
		nil, readerOperations...)

	page, err := plane.dispatch.TeamActions(stranger, fleet.TeamActionListRequest{
		Limit: 10, SourceIDs: [][]byte{[]byte("source")},
		PolicyIDs: [][]byte{[]byte("policy")},
		Context:   integrationRequestContext(plane.now),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range page.Actions {
		if string(action.ID) == string(plane.completed.ID) {
			t.Fatalf("a third party was served the completed action: "+
				"output=%q withheld=%v. Its Output was derived from evidence "+
				"this reader cannot see, and a marker would tell it so",
				action.Output, action.OutputWithheld)
		}
	}

	// And the same read on a plane whose evidence is unlabelled does serve
	// it. Without this the test would pass on a stranger who can never see
	// any action, which proves nothing about withholding — and it is also
	// where the decision's cost is visible: the action is in one page and
	// not the other.
	open := newLabelPlaneWith(t, planeOptions{
		choose: fullLabels, wireEvents: true, label: "secret",
		now: now, evidence: "none",
	})
	openStranger := bindPlaneSubjectWith(t, open.authority, open.now,
		"stranger", nil, readerOperations...)
	openPage, err := open.dispatch.TeamActions(openStranger,
		fleet.TeamActionListRequest{
			Limit: 10, SourceIDs: [][]byte{[]byte("source")},
			PolicyIDs: [][]byte{[]byte("policy")},
			Context:   integrationRequestContext(open.now),
		})
	if err != nil {
		t.Fatal(err)
	}
	served := false
	for _, action := range openPage.Actions {
		if string(action.ID) == string(open.completed.ID) {
			served = true
		}
	}
	if !served {
		t.Fatal("a third party sees no completed action even when its " +
			"evidence is unlabelled, so the probe above is vacuous")
	}
}
