// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const testPolicyDigest = "atpl:policy:v1:" +
	"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func atplLifecycle() fleet.Lifecycle {
	lifecycle := testLifecycle()
	lifecycle.ReasonCode = fleet.ReasonCodeATPLApply
	lifecycle.ReasonDetail = testPolicyDigest
	return lifecycle
}

func trustedReceipt(
	lifecycle fleet.Lifecycle, asserted interaction.CallerAssertedReason,
) interaction.Session {
	session := lifecycleSession(lifecycle, asserted)
	session.RecordedAt = lifecycle.SnapshotAsOf.Add(time.Second)
	session.Actor = interaction.ActorContext{
		SubjectID: lifecycle.Subject, ActorID: lifecycle.Actor,
		ClientID:   lifecycle.ClientID,
		OnBehalfOf: append([]shoal.ID(nil), lifecycle.OnBehalfOf...),
	}
	session.Reason, _ = interaction.NewReason(
		"audit_purpose", lifecycle.AuditPurpose)
	return session
}

func TestLifecycleRecorderRecordsAssertedPolicyDigestBesideAuditPurpose(t *testing.T) {
	lifecycle := atplLifecycle()
	sink := &trustedLifecycleRecorder{lifecycle: lifecycle}
	recorder, err := NewLifecycleRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordLifecycle(context.Background(), lifecycle); err != nil {
		t.Fatal(err)
	}
	requested := sink.requests[0]
	want := interaction.CallerAssertedReason{
		Code: fleet.ReasonCodeATPLApply, Source: testPolicyDigest,
	}
	if requested.CallerAssertedReason != want {
		t.Fatalf("caller-asserted reason = %#v", requested.CallerAssertedReason)
	}
	// The trusted reason slot is never filled from the request.
	if requested.Reason != (interaction.Reason{}) {
		t.Fatalf("request filled the trusted reason: %#v", requested.Reason)
	}
	unbound := lifecycleSession(lifecycle, interaction.CallerAssertedReason{})
	if requested.QueryDigest == unbound.QueryDigest {
		t.Fatal("asserted reason is not bound into the receipt digest")
	}
	other := lifecycle
	other.ReasonDetail = "atpl:policy:v1:" + strings.Repeat("f", 64)
	otherAsserted, err := fleet.CallerAssertedRegistryReason(
		other.ReasonCode, other.ReasonDetail)
	if err != nil {
		t.Fatal(err)
	}
	if lifecycleSession(other, otherAsserted).QueryDigest ==
		requested.QueryDigest {
		t.Fatal("receipt digest does not distinguish policy digests")
	}
	if lifecycleSessionID(other) != requested.ID {
		t.Fatal("asserted reason changed the receipt identity")
	}
}

func TestLifecycleRecorderAuditPurposeUnchangedByAssertedReason(t *testing.T) {
	plain := testLifecycle()
	plain.ReasonCode = "test"
	asserting := atplLifecycle()
	results := make([]interaction.Reason, 0, 2)
	for _, lifecycle := range []fleet.Lifecycle{plain, asserting} {
		store := &reconcilingLifecycleStore{
			trustedLifecycleRecorder: trustedLifecycleRecorder{
				lifecycle: lifecycle,
			},
		}
		recorder, err := NewLifecycleRecorderWithReader(store, store)
		if err != nil {
			t.Fatal(err)
		}
		if err := recorder.RecordLifecycle(
			context.Background(), lifecycle,
		); err != nil {
			t.Fatal(err)
		}
		results = append(results, store.stored.Reason)
	}
	want, err := interaction.NewReason("audit_purpose", plain.AuditPurpose)
	if err != nil {
		t.Fatal(err)
	}
	if results[0] != want || results[1] != want {
		t.Fatalf("audit purpose reasons = %#v, want %#v", results, want)
	}
}

func TestLifecycleRecorderRejectsForgedAssertedReason(t *testing.T) {
	lifecycle := atplLifecycle()
	recorder, err := NewLifecycleRecorder(&trustedLifecycleRecorder{
		lifecycle: lifecycle,
		mutate: func(session *interaction.Session) {
			session.CallerAssertedReason.Source =
				"atpl:policy:v1:" + strings.Repeat("0", 64)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = recorder.RecordLifecycle(context.Background(), lifecycle)
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) ||
		!explorer.IsCommittedInteraction(err) {
		t.Fatalf("forged asserted reason = %v", err)
	}
}

func TestLifecycleRecorderRefusesMalformedPolicyDigestBeforeAnyWrite(t *testing.T) {
	hex := strings.Repeat("a", 64)
	for name, detail := range map[string]string{
		"empty":           "",
		"bare hex":        hex,
		"uppercase hex":   "atpl:policy:v1:" + strings.ToUpper(hex),
		"short":           "atpl:policy:v1:" + hex[:63],
		"long":            "atpl:policy:v1:" + hex + "a",
		"other version":   "atpl:policy:v2:" + hex,
		"plan digest":     "atpl:plan:v2:" + hex,
		"trailing space":  "atpl:policy:v1:" + hex + " ",
		"trailing NUL":    "atpl:policy:v1:" + hex + "\x00",
		"non-hex":         "atpl:policy:v1:" + strings.Repeat("g", 64),
		"free text":       "applied from my laptop",
		"oversized":       "atpl:policy:v1:" + strings.Repeat("a", 1<<16),
		"leading newline": "\natpl:policy:v1:" + hex,
	} {
		t.Run(name, func(t *testing.T) {
			lifecycle := atplLifecycle()
			lifecycle.ReasonDetail = detail
			store := &reconcilingLifecycleStore{
				trustedLifecycleRecorder: trustedLifecycleRecorder{
					lifecycle: lifecycle,
				},
			}
			recorder, err := NewLifecycleRecorderWithReader(store, store)
			if err != nil {
				t.Fatal(err)
			}
			err = recorder.RecordLifecycle(context.Background(), lifecycle)
			if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) ||
				!strings.Contains(err.Error(), "atpl:policy:v1:<64 lowercase hex>") {
				t.Fatalf("malformed policy digest error = %v", err)
			}
			if explorer.IsCommittedInteraction(err) {
				t.Fatalf("refusal reported a commit: %v", err)
			}
			if len(store.requests) != 0 || store.stored.ID != "" {
				t.Fatalf("malformed digest was written: %#v", store.requests)
			}
		})
	}
}

func TestLifecycleRecorderKeepsOtherReasonDetailOnlyAsDigest(t *testing.T) {
	lifecycle := testLifecycle()
	lifecycle.ReasonCode = "operator-renewal"
	lifecycle.ReasonDetail = "free text <script>alert(1)</script>"
	sink := &trustedLifecycleRecorder{lifecycle: lifecycle}
	recorder, err := NewLifecycleRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordLifecycle(context.Background(), lifecycle); err != nil {
		t.Fatal(err)
	}
	requested := sink.requests[0]
	want := interaction.CallerAssertedReason{
		Code:         "operator-renewal",
		DetailDigest: interaction.Digest(lifecycle.ReasonDetail),
	}
	if requested.CallerAssertedReason != want {
		t.Fatalf("caller-asserted reason = %#v", requested.CallerAssertedReason)
	}
	if strings.Contains(fmt.Sprintf("%#v", requested), "script") {
		t.Fatal("free-form reason detail reached the durable session")
	}
	// A non-ATPL code cannot smuggle a verbatim source.
	lifecycle.ReasonDetail = testPolicyDigest
	lifecycle.ReasonCode = "atpl-apply-ish"
	asserted, err := fleet.CallerAssertedRegistryReason(
		lifecycle.ReasonCode, lifecycle.ReasonDetail)
	if err != nil || asserted.Source != "" ||
		asserted.DetailDigest != interaction.Digest(testPolicyDigest) {
		t.Fatalf("non-ATPL asserted reason = %#v, %v", asserted, err)
	}
}

func TestLifecycleRecorderRefusesUnrecordableReasonCode(t *testing.T) {
	lifecycle := testLifecycle()
	lifecycle.ReasonCode = "has space"
	sink := &trustedLifecycleRecorder{lifecycle: lifecycle}
	recorder, err := NewLifecycleRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = recorder.RecordLifecycle(context.Background(), lifecycle)
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) ||
		len(sink.requests) != 0 {
		t.Fatalf("unrecordable reason code = %v, writes %d",
			err, len(sink.requests))
	}
}

func TestLifecycleRecorderRetryWithDifferentAssertedReasonConflicts(t *testing.T) {
	lifecycle := atplLifecycle()
	store := &reconcilingLifecycleStore{
		trustedLifecycleRecorder: trustedLifecycleRecorder{lifecycle: lifecycle},
	}
	recorder, err := NewLifecycleRecorderWithReader(store, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordLifecycle(context.Background(), lifecycle); err != nil {
		t.Fatal(err)
	}
	// An exact retry reconciles.
	if err := recorder.RecordLifecycle(context.Background(), lifecycle); err != nil {
		t.Fatalf("exact retry = %v", err)
	}
	for name, mutate := range map[string]func(*fleet.Lifecycle){
		"other digest": func(lifecycle *fleet.Lifecycle) {
			lifecycle.ReasonDetail = "atpl:policy:v1:" + strings.Repeat("b", 64)
		},
		"other code": func(lifecycle *fleet.Lifecycle) {
			lifecycle.ReasonCode = "test"
		},
	} {
		t.Run(name, func(t *testing.T) {
			retry := lifecycle
			mutate(&retry)
			err := recorder.RecordLifecycle(context.Background(), retry)
			if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
				t.Fatalf("divergent asserted retry = %v", err)
			}
		})
	}
	if store.stored.CallerAssertedReason.Source != testPolicyDigest {
		t.Fatalf("stored receipt changed: %#v", store.stored.CallerAssertedReason)
	}
}

func TestLifecycleRecorderReconcilesReceiptWrittenBeforeAssertedReasons(t *testing.T) {
	lifecycle := atplLifecycle()
	// Exactly what the previous recorder persisted for this request: no
	// asserted reason and the original query digest.
	previous := trustedReceipt(lifecycle, interaction.CallerAssertedReason{})
	store := &reconcilingLifecycleStore{
		trustedLifecycleRecorder: trustedLifecycleRecorder{lifecycle: lifecycle},
		stored:                   previous,
	}
	recorder, err := NewLifecycleRecorderWithReader(store, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordLifecycle(context.Background(), lifecycle); err != nil {
		t.Fatalf("pre-upgrade receipt reconciliation = %v", err)
	}
	if !store.stored.CallerAssertedReason.IsZero() {
		t.Fatal("reconciliation rewrote the pre-upgrade receipt")
	}

	// Legacy acceptance only relaxes the absent field: a different mutation
	// still conflicts.
	divergent := previous
	divergent.ResultID = "different-agent"
	store.stored = divergent
	err = recorder.RecordLifecycle(context.Background(), lifecycle)
	if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("divergent pre-upgrade receipt = %v", err)
	}
	mutated := previous
	mutated.QueryDigest = interaction.Digest("other mutation")
	store.stored = mutated
	err = recorder.RecordLifecycle(context.Background(), lifecycle)
	if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("pre-upgrade receipt with other mutation = %v", err)
	}
}

func TestLifecycleRecorderReconcilesV1ReceiptForAssertingRetry(t *testing.T) {
	lifecycle := atplLifecycle()
	accepted := trustedReceipt(lifecycle, interaction.CallerAssertedReason{})
	accepted.ID = legacyLifecycleSessionID(lifecycle)
	store := &reconcilingLifecycleStore{
		trustedLifecycleRecorder: trustedLifecycleRecorder{lifecycle: lifecycle},
		stored:                   accepted,
	}
	recorder, err := NewLifecycleRecorderWithReader(store, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordLifecycle(context.Background(), lifecycle); err != nil {
		t.Fatalf("v1 receipt reconciliation = %v", err)
	}
	if len(store.requests) != 0 {
		t.Fatalf("v2 receipt written despite v1 match: %d", len(store.requests))
	}
}

func TestLifecycleRecorderRecoversDroppedCommittedAssertingResult(t *testing.T) {
	cause := context.DeadlineExceeded
	lifecycle := atplLifecycle()
	asserted, err := fleet.CallerAssertedRegistryReason(
		lifecycle.ReasonCode, lifecycle.ReasonDetail)
	if err != nil {
		t.Fatal(err)
	}
	store := &reconcilingLifecycleStore{
		trustedLifecycleRecorder: trustedLifecycleRecorder{lifecycle: lifecycle},
		stored:                   trustedReceipt(lifecycle, asserted),
		recordErr:                explorer.MarkCommittedInteraction(cause),
	}
	recorder, err := NewLifecycleRecorderWithReader(store, store)
	if err != nil {
		t.Fatal(err)
	}
	err = recorder.RecordLifecycle(context.Background(), lifecycle)
	if !explorer.IsCommittedInteraction(err) ||
		shoal.IsErrorCode(err, shoal.ErrorInternal) ||
		shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("dropped committed asserting result = %v", err)
	}
}
