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
	"reflect"
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
