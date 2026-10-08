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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/teamoverview"
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
// whose absence made an out-of-process worker impossible (#435), and pins that
// they ride the claim response alone.
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

	wire := encodeClaimedFleetAction(record)

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
	// where two spellings are two keys. pkg/explorer/mcp returns the record
	// whole and so emits the padded standard spelling; a worker must therefore
	// compare decoded bytes and never one spelling against the other.
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

	// The shared encoder carries neither, for the same record. This is the
	// assertion that bounds both the disclosure and the response size: six of
	// the seven routes encoding an action use it, five of those are
	// commit-bearing, and an over-budget response on a commit-bearing route is
	// reported as an indeterminate commit rather than a failure.
	shared := encodeFleetAction(record)
	if len(shared.Input) != 0 {
		t.Fatalf("the shared encoder carries input %s, so every action response "+
			"carries a payload bounded only by MaxActionPayloadBytes", shared.Input)
	}
	if shared.ExecutorKey != "" {
		t.Fatalf("the shared encoder carries executor_key %q", shared.ExecutorKey)
	}
	// Everything else must survive the split, or the claim response would be
	// the only one worth reading.
	shared.Input, shared.ExecutorKey = wire.Input, wire.ExecutorKey
	if !reflect.DeepEqual(shared, wire) {
		t.Fatalf("the claim encoder differs from the shared one beyond the two "+
			"claimant fields:\n claim  = %+v\n shared = %+v", wire, shared)
	}

	// An action with no input encodes nothing rather than a JSON null, because
	// the field is omitempty and a literal null is not an absent value to a
	// client distinguishing the two.
	bare := encodeClaimedFleetAction(fleet.ActionRecord{ID: []byte("x"), Version: 1})
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

// TestOnlyTheClaimRouteEncodesTheWork pins the call sites, because the bound on
// both the disclosure and the response size is "one record on one route" and
// nothing else in the type system enforces it.
//
// encodeClaimedFleetAction is one line away from being usable anywhere, and the
// cost of using it on the wrong route is not a leak alone. requestMayCommit
// reports enqueue, invoke, claim, complete and cancel as commit-bearing, so
// writeResponse answers an over-budget response there with 503 and
// X-Commit-Outcome: indeterminate. MaxActionPayloadBytes is a fixed 1 MiB and a
// workspace's OutputBytes budget narrows to any non-zero value, so on the
// enqueue echo that would be a successful enqueue reported as an unknown
// outcome, repeatably, for as long as the record existed. On Pull it would be
// up to MaxDispatchListResults — 256 — payloads in one page.
func TestOnlyTheClaimRouteEncodesTheWork(t *testing.T) {
	body, err := os.ReadFile("fleet_dispatch.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(body)
	// Count call sites, not definitions: the definition's own name and its one
	// internal call to the shared encoder are excluded by requiring the "("
	// and by subtracting the declaration.
	claimed := strings.Count(source, "encodeClaimedFleetAction(") -
		strings.Count(source, "func encodeClaimedFleetAction(")
	if claimed != 1 {
		t.Fatalf("encodeClaimedFleetAction has %d call sites, want exactly 1 "+
			"(the claim route). Every added site is a route whose response "+
			"carries a 1 MiB-bounded payload against a narrowable output "+
			"budget, and five of the seven are commit-bearing", claimed)
	}
	// And it is the claim route specifically. Ordering the handlers by
	// position, the one containing the call must be the claim registration.
	call := strings.Index(source, "writeResponse(w, http.StatusOK, encodeClaimedFleetAction(")
	if call < 0 {
		t.Fatal("no route writes a claimed action; the claimant now gets no input")
	}
	handler := strings.LastIndex(source[:call], `mux.HandleFunc("`)
	if handler < 0 {
		t.Fatal("the claimed-action response is not inside a route registration")
	}
	route := source[handler : handler+strings.Index(source[handler:], "\n")]
	if !strings.Contains(route, "/claim") {
		t.Fatalf("the claimed-action encoder is used by %s, not the claim route", route)
	}
}

// TestTheTeamOverviewDoesNotReadActionInput guards the one dispatch read that
// does not require the reader to be the action's originating principal.
//
// DispatchService.TeamActions documents exactly that, and it projects its own
// view rather than returning fleetActionWire. So the execute grant #437
// introduced is the bound on who reads an action's input only for as long as
// that projection stays a projection. If a field of type fleet.ActionRecord
// ever appears in the response, Input and ExecutorKey ship with it: the record
// carries no JSON tags, so encoding/json emits every exported field under its
// Go name.
//
// This walks the real teamoverview.Response rather than grepping for ".Input".
// The grep it replaced could not see the shape that actually matters — the
// record marshalled whole, which names no field at all — and pkg/explorer/mcp
// is the standing proof that the shape occurs: FleetActionToolResult embeds
// fleet.ActionRecord and so already emits both values to its caller.
func TestTheTeamOverviewDoesNotReadActionInput(t *testing.T) {
	record := reflect.TypeOf(fleet.ActionRecord{})
	visited := map[reflect.Type]bool{}
	var walk func(path string, carrier reflect.Type)
	walk = func(path string, carrier reflect.Type) {
		for carrier.Kind() == reflect.Pointer || carrier.Kind() == reflect.Slice ||
			carrier.Kind() == reflect.Array || carrier.Kind() == reflect.Map {
			carrier = carrier.Elem()
		}
		if carrier == record {
			t.Errorf("%s has type fleet.ActionRecord. The team overview is the "+
				"one dispatch read that does not require the reader to be the "+
				"action's principal, and that type has no JSON tags, so "+
				"returning it whole discloses one principal's request "+
				"parameters and its target-facing idempotency key to every "+
				"reader of the team's overview", path)
			return
		}
		if carrier.Kind() != reflect.Struct || visited[carrier] {
			return
		}
		visited[carrier] = true
		for i := range carrier.NumField() {
			field := carrier.Field(i)
			if field.PkgPath != "" {
				continue // unexported, so encoding/json never emits it
			}
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "" {
				name = field.Name
			}
			if name == "input" || name == "executor_key" ||
				field.Name == "Input" || field.Name == "ExecutorKey" {
				t.Errorf("%s.%s is named %q, so the team overview projects an "+
					"action's input or its executor key to a reader who need "+
					"not be the action's principal", path, field.Name, name)
			}
			walk(path+"."+field.Name, field.Type)
		}
	}
	walk("teamoverview.Response", reflect.TypeOf(teamoverview.Response{}))
	if len(visited) < 2 {
		t.Fatalf("walked %d struct types, so this guard checks nothing", len(visited))
	}
}
