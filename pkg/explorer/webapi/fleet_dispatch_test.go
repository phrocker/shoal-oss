// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package webapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestFleetDispatchEvidenceWirePreservesExactAnchors(t *testing.T) {
	record := fleet.ActionRecord{
		EvidenceSnapshotID:   "snapshot",
		EvidenceSnapshotAsOf: time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC),
		Evidence: []fleet.EvidenceRef{
			{
				AnchorID: "document-anchor", Kind: interaction.EvidenceDocument,
				Citation: document.Citation{
					DocumentID: "document", RevisionID: "revision",
					SectionID: "section", SpanID: "span",
					Range: document.SourceRange{
						Start: document.SourcePosition{Offset: 3, Page: 1},
						End:   document.SourcePosition{Offset: 9, Page: 2},
					},
				},
				NodeIDs:    []shoal.ID{"document", "section", "span"},
				Visibility: []string{"restricted"},
			},
			{
				AnchorID: "graph-anchor", Kind: interaction.EvidenceGraph,
				NodeIDs: []shoal.ID{"left", "right"}, EdgeIDs: []shoal.ID{"edge"},
				Assertions: []interaction.AssertionReference{{
					AssertionID: "assertion", EdgeID: "edge",
					Origin: ontology.AssertionDerived,
				}},
				Visibility: []string{"restricted"},
			},
		},
	}
	wire := encodeFleetAction(record)
	decoded, err := decodeEvidence(wire.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	for index := range decoded {
		decoded[index].NodeIDs = append([]shoal.ID(nil), decoded[index].NodeIDs...)
		decoded[index].EdgeIDs = append([]shoal.ID(nil), decoded[index].EdgeIDs...)
		decoded[index].Assertions = append(
			[]interaction.AssertionReference(nil), decoded[index].Assertions...)
		record.Evidence[index].NodeIDs = append(
			[]shoal.ID(nil), record.Evidence[index].NodeIDs...)
		record.Evidence[index].EdgeIDs = append(
			[]shoal.ID(nil), record.Evidence[index].EdgeIDs...)
		record.Evidence[index].Assertions = append(
			[]interaction.AssertionReference(nil), record.Evidence[index].Assertions...)
	}
	if !reflect.DeepEqual(decoded, record.Evidence) ||
		wire.EvidenceSnapshotID != encodeFleetID("snapshot") ||
		!wire.EvidenceSnapshotAsOf.Equal(record.EvidenceSnapshotAsOf) {
		t.Fatalf("evidence wire lost exact identity: %#v", wire)
	}
}

func TestFleetDispatchMountRequiresAuthenticationAndPreservesOpaqueIDs(t *testing.T) {
	anonymous, err := NewHandler(&stubWorkspaceService{}, "example.test")
	if err != nil {
		t.Fatal(err)
	}
	if err := anonymous.MountFleetDispatch(&stubDispatchProvider{}); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("anonymous mount = %v", err)
	}

	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "subject", Actor: "actor", AuthorizationDomain: []byte("domain"),
		AllowedOperations:  []auth.Operation{auth.OperationDispatch},
		PermittedSourceIDs: [][]byte{[]byte("source")},
		PermittedPolicyIDs: [][]byte{[]byte("policy")}, PolicyGeneration: 1,
		AuthenticationExpires: now.Add(time.Hour), RequestID: "request",
		CorrelationID: "correlation",
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewAuthenticatedHandler(&stubWorkspaceService{},
		AuthenticatorFunc(func(*http.Request) (auth.Decision, error) { return decision, nil }),
		authority.Binder(), "example.test")
	if err != nil {
		t.Fatal(err)
	}
	provider := &stubDispatchProvider{resolver: authority.Resolver()}
	if err := handler.MountFleetDispatch(provider); err != nil {
		t.Fatal(err)
	}
	actionID := []byte{'a', 0, 255}
	body, _ := json.Marshal(fleetEnqueueWire{
		Context: fleetRequestContextWire{
			RequestID: encodeFleetID("request"), ReasonCode: "operator_request",
			CorrelationID: encodeFleetID("correlation"), Deadline: now.Add(time.Minute),
		},
		ID:             base64.RawURLEncoding.EncodeToString(actionID),
		IdempotencyKey: base64.RawURLEncoding.EncodeToString([]byte{'k', 0, 255}),
		AgentID:        encodeFleetID("agent"), AgentGeneration: 1,
		Capability: "search", Action: "query", SourceID: []byte("source"),
		PolicyID: []byte("policy"), ObjectID: encodeFleetID("object"),
		Input: json.RawMessage(`{"value":1}`),
	})
	request := httptest.NewRequest(http.MethodPost, "http://example.test/api/v1/fleet/actions", bytes.NewReader(body))
	request.Host = "example.test"
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if !provider.enqueued || !bytes.Equal(provider.actionID, actionID) {
		t.Fatalf("provider action=%x enqueued=%v", provider.actionID, provider.enqueued)
	}
}

func TestNewFleetHandlerRequiresBothProviders(t *testing.T) {
	if _, err := NewFleetHandler(nil, &stubDispatchProvider{}); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("missing registry provider = %v", err)
	}
	if _, err := NewFleetHandler(&stubFleetProvider{}, nil); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("missing dispatch provider = %v", err)
	}
	handler, err := NewFleetHandler(&stubFleetProvider{}, &stubDispatchProvider{})
	if err != nil || handler == nil {
		t.Fatalf("fleet handler = %v, %v", handler, err)
	}
	if FleetRoutePrefix != "/api/v1/fleet/" {
		t.Fatalf("fleet route prefix = %q", FleetRoutePrefix)
	}
}

type stubDispatchProvider struct {
	resolver auth.Resolver
	enqueued bool
	actionID []byte
}

func (p *stubDispatchProvider) Enqueue(ctx context.Context, request fleet.EnqueueRequest) (fleet.ActionRecord, error) {
	if _, err := p.resolver.Resolve(ctx); err != nil {
		return fleet.ActionRecord{}, err
	}
	p.enqueued = true
	p.actionID = append([]byte(nil), request.ID...)
	return fleet.ActionRecord{
		ID: request.ID, Version: 1, State: fleet.DispatchQueued,
		AgentID: request.AgentID, AgentGeneration: request.AgentGeneration,
		Capability: request.Capability, Action: request.Action,
		RequestID: request.Context.RequestID, CorrelationID: request.Context.CorrelationID,
		Deadline: request.Context.Deadline, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}, nil
}

func (*stubDispatchProvider) Claim(context.Context, fleet.ClaimRequest) (fleet.ActionRecord, error) {
	return fleet.ActionRecord{}, nil
}
func (*stubDispatchProvider) CompleteClaim(context.Context, fleet.CompletionRequest) (fleet.ActionRecord, error) {
	return fleet.ActionRecord{}, nil
}
func (*stubDispatchProvider) Cancel(context.Context, fleet.CancelRequest) (fleet.ActionRecord, error) {
	return fleet.ActionRecord{}, nil
}
func (*stubDispatchProvider) Status(context.Context, fleet.StatusRequest) (fleet.ActionRecord, error) {
	return fleet.ActionRecord{}, nil
}
func (*stubDispatchProvider) Pull(context.Context, fleet.PullActionsRequest) (fleet.ActionPage, error) {
	return fleet.ActionPage{}, nil
}
func (*stubDispatchProvider) Invoke(context.Context, fleet.InvokeRequest) (fleet.ActionRecord, error) {
	return fleet.ActionRecord{}, nil
}

// TestFleetActionWireCarriesTheWorkAndItsIdempotencyKey pins the two fields
// whose absence made an out-of-process worker impossible (#435).
//
// A worker could pull an action, claim it under a fence, and complete it
// without ever receiving the parameters of the operation: Input was on the
// enqueue wire, validated against the action's InputSchema, and stored on the
// record, and this encoder dropped it. ExecutorKey was dropped the same way,
// which forced each worker to invent its own idempotency key — and a
// hand-written digest that omits length prefixes is non-injective, so two
// surfaces can be made to collide and a collision makes a provider return a
// cached success without performing the effect.
//
// Both are asserted byte-for-byte rather than for presence, because the whole
// value of Input is that it is exactly what the enqueuer supplied, and the
// whole value of ExecutorKey is that it equals the key an in-process executor
// receives for the same action.
func TestFleetActionWireCarriesTheWorkAndItsIdempotencyKey(t *testing.T) {
	const input = `{"method":"POST","path":"/v1/messages","body":{"text":"ship it"}}`
	// Chosen so the two base64 alphabets actually differ. An earlier version of
	// this test used bytes that encode identically under StdEncoding and
	// RawURLEncoding, so the alphabet assertion below could not fail and a
	// mutation swapping the encoder survived. These bytes produce "+/++" in the
	// standard alphabet and "-_--" in the URL one.
	executorKey := []byte{0xfb, 0xff, 0xbe, 0xfb, 0xff, 0xbe}
	record := fleet.ActionRecord{
		ID:          []byte("action-id"),
		Version:     3,
		State:       fleet.DispatchClaimed,
		Capability:  "effects.http",
		Action:      "post",
		Input:       json.RawMessage(input),
		ExecutorKey: executorKey,
	}

	wire := encodeFleetAction(record)

	if string(wire.Input) != input {
		t.Fatalf("input = %s, want it unchanged: a worker that does not receive "+
			"this has nothing to perform", wire.Input)
	}
	if want := base64.RawURLEncoding.EncodeToString(executorKey); wire.ExecutorKey != want {
		t.Fatalf("executor_key = %q, want %q", wire.ExecutorKey, want)
	}
	// Unpadded base64url, like every other opaque identity on this wire. A
	// padded or standard-alphabet spelling would be a second spelling of one
	// identity, and this value is used as a deduplication key at a third party
	// where two spellings are two keys.
	if strings.ContainsAny(wire.ExecutorKey, "=+/") {
		t.Fatalf("executor_key %q is not unpadded base64url", wire.ExecutorKey)
	}
	// And positively: the URL alphabet's own characters are present, so this
	// asserts the right encoding rather than merely the absence of the wrong
	// one. A test that only checks for absent characters passes against an
	// encoder that emits neither alphabet's distinctive bytes.
	if !strings.ContainsAny(wire.ExecutorKey, "-_") {
		t.Fatalf("executor_key %q does not use the URL alphabet", wire.ExecutorKey)
	}

	// The encoder copies rather than aliases, so a caller mutating the wire
	// cannot reach back into the record it was built from.
	if len(wire.Input) > 0 {
		wire.Input[0] = 'X'
		if record.Input[0] == 'X' {
			t.Fatal("the wire aliases the record's input")
		}
	}

	// An action with no input encodes nothing rather than a JSON null, because
	// the field is omitempty and a literal null is not an absent value to a
	// client distinguishing the two.
	bare := encodeFleetAction(fleet.ActionRecord{ID: []byte("x"), Version: 1})
	encoded, err := json.Marshal(bare)
	if err != nil {
		t.Fatal(err)
	}
	for _, absent := range []string{`"input"`, `"executor_key"`} {
		if strings.Contains(string(encoded), absent) {
			t.Fatalf("%s is present for a record that has none: %s", absent, encoded)
		}
	}
}

// TestTheTeamOverviewDoesNotReadActionInput is the reason emitting input on
// every action response is safe, and it is asserted rather than assumed.
//
// All six existing-action routes reach the store through authorizedCurrent, and
// Pull filters on sameActionPrincipal, so every consumer of fleetActionWire is
// scoped to the action's own principal. The one dispatch read that is not —
// DispatchService.TeamActions, which documents that it "does not require the
// reader to be the action's originating principal" — projects its own view in
// pkg/explorer/teamoverview and never touches Input.
//
// If that ever changes, adding Input here becomes a disclosure of one
// principal's request parameters to a team-overview reader, so this test exists
// to fail at that moment rather than to describe today.
func TestTheTeamOverviewDoesNotReadActionInput(t *testing.T) {
	sources, err := filepath.Glob("../teamoverview/*.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) == 0 {
		t.Fatal("no team overview sources found, so this guard checks nothing")
	}
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		body, readErr := os.ReadFile(source)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.Contains(string(body), ".Input") {
			t.Fatalf("%s reads an action's Input. The team overview is the one "+
				"dispatch read that does not require the reader to be the "+
				"action's principal, so exposing Input through it discloses one "+
				"principal's request parameters to another", source)
		}
	}
}
