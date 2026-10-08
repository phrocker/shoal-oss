// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// The end-to-end tests for the OIDC approver mapping (#451 slice 2) mint
// every principal through the real OIDC authenticator from signed tokens and
// drive the composition openService builds for the shipped binary, as
// approval_acceptance_test.go does. No decision here is hand-built except
// where a test says it is a counterfactual.

type oidcApprovalWorld struct {
	t      *testing.T
	h      *approvalHarness
	issuer *fakeOIDCIssuer
	edit   func(*oidcConfig)
	authn  atomic.Pointer[oidcAuthenticator]
	digest atomic.Value // auth.Digest

	mu      sync.Mutex
	written []fleet.ApprovalRecord
	tokens  []string
}

func newOIDCApprovalWorld(
	t *testing.T, edit func(*oidcConfig),
) *oidcApprovalWorld {
	t.Helper()
	w := &oidcApprovalWorld{t: t, issuer: newFakeOIDCIssuer(t), edit: edit}
	h := &approvalHarness{t: t, root: t.TempDir()}
	h.clock.Store(time.Now().UTC().Add(time.Minute).Truncate(time.Second).UnixNano())
	authority, err := auth.NewAuthorityWithClock(h.now)
	if err != nil {
		t.Fatal(err)
	}
	h.authority = authority
	h.reader = &mutableFleetGeneration{}
	h.reader.value.Store(workspacePolicyGeneration)
	w.h = h
	w.configure(approverMappingDocument(w.issuer.server.URL))
	h.mapping = func(context.Context) (auth.Digest, error) {
		return w.digest.Load().(auth.Digest), nil
	}
	h.wrap = func(store fleet.ApprovalStore) fleet.ApprovalStore {
		return hookedApprovalStore{ApprovalStore: store, before: func(
			mutation fleet.ApprovalMutation,
		) error {
			w.mu.Lock()
			w.written = append(w.written, fleet.CloneApprovalRecord(mutation.Record))
			w.mu.Unlock()
			return nil
		}}
	}
	h.open()
	t.Cleanup(h.close)
	h.register("gateway", "approval-registration", 0, true, "")
	return w
}

// configure is what an operator does to the mapping file: the
// authenticator is rebuilt from it and the in-force digest follows. Taking
// effect in the service needs a restart (h.reopen), as in the binary.
func (w *oidcApprovalWorld) configure(document map[string]any) {
	w.t.Helper()
	config := approverTestConfig(w.t, w.issuer, w.h.now, document)
	if w.edit != nil {
		w.edit(&config)
	}
	authenticator := newTestOIDCAuthenticator(w.t, config)
	w.authn.Store(authenticator)
	w.digest.Store(authenticator.approverMappingDigest())
}

func (w *oidcApprovalWorld) sign(claims jwt.MapClaims) string {
	w.t.Helper()
	token := w.issuer.signRS256(w.t, testKID, claims)
	w.mu.Lock()
	w.tokens = append(w.tokens, token)
	w.mu.Unlock()
	return token
}

// fleetToken is a human's workspace token under the fleet mapping.
func (w *oidcApprovalWorld) fleetToken(subject string, extra jwt.MapClaims) string {
	claims := w.issuer.defaultClaims(w.h.now())
	claims["sub"] = subject
	claims["access"] = []string{"fleet"}
	for key, value := range extra {
		claims[key] = value
	}
	return w.sign(claims)
}

// approverToken is a human's token on the approver audience.
func (w *oidcApprovalWorld) approverToken(subject string) string {
	return w.sign(approverClaims(w.issuer, w.h.now(), subject))
}

func (w *oidcApprovalWorld) decision(token string) auth.Decision {
	w.t.Helper()
	decision, err := w.authn.Load().Authenticate(bearerRequest(token))
	if err != nil {
		w.t.Fatalf("authenticate: %v", err)
	}
	return decision
}

// bind binds a minted decision for the fleet services.
//
// The fleet services require a correlation ID on the decision, and neither
// built-in authenticator sets one (a gap that predates this change and
// applies to every fleet route under OIDC and -dev-auth alike). The decision
// is therefore re-minted with one, exactly as the MCP server correlates a
// session — every other field, the grant provenance included, is the
// authenticator's.
func (w *oidcApprovalWorld) bind(decision auth.Decision) context.Context {
	w.t.Helper()
	selected, _ := decision.SelectedOntology()
	correlated, err := auth.NewDecision(auth.DecisionConfig{
		Subject:                decision.Subject(),
		Actor:                  decision.Actor(),
		ClientID:               decision.ClientID(),
		OnBehalfOf:             decision.OnBehalfOf(),
		AuthorizationDomain:    decision.AuthorizationDomain(),
		AllowedOperations:      decision.AllowedOperations(),
		PermittedSourceIDs:     decision.PermittedSourceIDs(),
		PermittedPolicyIDs:     decision.PermittedPolicyIDs(),
		PolicyGeneration:       decision.PolicyGeneration(),
		AuthenticationExpires:  decision.AuthenticationExpires(),
		RequestID:              decision.RequestID(),
		CorrelationID:          "oidc-approval-correlation",
		AuditPurpose:           decision.AuditPurpose(),
		ServiceRole:            decision.ServiceRole(),
		ServiceCeilingIdentity: decision.ServiceCeilingIdentity(),
		SelectedOntology:       selected,
		GrantProvenance:        decision.GrantProvenance(),
	})
	if err != nil {
		w.t.Fatal(err)
	}
	ctx, err := w.h.authority.Binder().Bind(context.Background(), correlated)
	if err != nil {
		w.t.Fatal(err)
	}
	return ctx
}

func (w *oidcApprovalWorld) as(token string) context.Context {
	return w.bind(w.decision(token))
}

func (w *oidcApprovalWorld) request(
	ctx context.Context, request heldRequest,
) (fleet.ApprovalReceipt, error) {
	return w.h.opened.approvals.Request(ctx, w.h.enqueue(request))
}

func (w *oidcApprovalWorld) decide(
	ctx context.Context, receipt fleet.ApprovalReceipt,
) (fleet.ApprovalRecord, error) {
	return w.h.opened.approvals.Decide(ctx, fleet.ApprovalDecisionRequest{
		ID: receipt.ID, RequestDigest: receipt.RequestDigest,
		PolicyGeneration: receipt.PolicyGeneration,
		Verdict:          fleet.ApprovalVerdictApprove,
		Context:          w.h.context(w.h.now().Add(time.Minute)),
	})
}

func (w *oidcApprovalWorld) status(
	ctx context.Context, id []byte,
) fleet.ApprovalStatus {
	w.t.Helper()
	status, err := w.h.opened.approvals.Status(ctx, fleet.ApprovalStatusRequest{
		ID: id, Context: w.h.context(w.h.now().Add(time.Minute)),
	})
	if err != nil {
		w.t.Fatalf("status %s: %v", id, err)
	}
	return status
}

func (w *oidcApprovalWorld) identity(subject string) shoal.ID {
	return shoal.ID("oidc:" + w.issuer.server.URL + "#" + subject)
}

func assertNotIndependent(t *testing.T, name string, err error) {
	t.Helper()
	if !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) ||
		!strings.Contains(err.Error(), "not independent") {
		t.Fatalf("%s: decide = %v, want the independence refusal", name, err)
	}
}

// TestOIDCApproverApprovesAnOIDCRequest is the positive case end to end, and
// the shared-actor finding that motivated approver actor = subject.
func TestOIDCApproverApprovesAnOIDCRequest(t *testing.T) {
	w := newOIDCApprovalWorld(t, nil)
	h := w.h
	alice := w.as(w.fleetToken("alice", nil))
	request := h.held("oidc-held-1")
	receipt, err := w.request(alice, request)
	if err != nil || receipt.State != fleet.ApprovalPending {
		t.Fatalf("alice's request = %+v, %v", receipt, err)
	}
	h.assertNotWorkAs(alice, request)

	bobDecision := w.decision(w.approverToken("bob"))

	// The finding. Before this change an OIDC approver could only have been
	// minted with the shared actor every OIDC token carries; the approval
	// service counts the requester's actor as involved, so that approver
	// overlapped every OIDC requester — and was refused under a message
	// blaming independence. The counterfactual is bob's own decision with
	// that one field changed.
	shared, err := auth.NewDecision(auth.DecisionConfig{
		Subject: bobDecision.Subject(), Actor: oidcActor,
		ClientID:              bobDecision.ClientID(),
		AuthorizationDomain:   bobDecision.AuthorizationDomain(),
		AllowedOperations:     bobDecision.AllowedOperations(),
		PermittedSourceIDs:    bobDecision.PermittedSourceIDs(),
		PermittedPolicyIDs:    bobDecision.PermittedPolicyIDs(),
		PolicyGeneration:      bobDecision.PolicyGeneration(),
		AuthenticationExpires: bobDecision.AuthenticationExpires(),
		RequestID:             "shared-actor-counterfactual",
		GrantProvenance:       bobDecision.GrantProvenance(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if status := w.status(alice, request.id); status.Approval.Request.Actor != oidcActor {
		t.Fatalf("an OIDC requester's actor is %q; the finding assumes %q",
			status.Approval.Request.Actor, oidcActor)
	}
	_, err = w.decide(w.bind(shared), receipt)
	assertNotIndependent(t, "an approver with the shared OIDC actor", err)

	// Self-approval: alice holds the approver mapping too, and her approver
	// identity is her requester identity.
	_, err = w.decide(w.as(w.approverToken("alice")), receipt)
	assertNotIndependent(t, "the requester as approver", err)

	// Bob, independent, sees it and approves it.
	bob := w.bind(bobDecision)
	if status := w.status(bob, request.id); status.State != fleet.ApprovalPending {
		t.Fatalf("bob's view = %q", status.State)
	}
	decided, err := w.decide(bob, receipt)
	if err != nil {
		t.Fatalf("bob's approval: %v", err)
	}
	if decided.State != fleet.ApprovalApproved ||
		decided.ApproverSubject != w.identity("bob") ||
		decided.ApproverActor != w.identity("bob") ||
		decided.ApproverClientID != w.identity(testApproverClient) {
		t.Fatalf("decided = %+v", decided)
	}
	digest := w.digest.Load().(auth.Digest)
	wantProvenance := auth.GrantProvenance{
		Issuer: w.issuer.server.URL, Subject: "bob",
		ClaimPath:     []string{"realm_access", "roles"},
		MatchedValue:  testApproverValue,
		MappingDigest: digest,
	}
	if decided.ApproverMappingDigest != digest ||
		!decided.ApproverProvenance.Equal(wantProvenance) {
		t.Fatalf("decision provenance = %x %+v", decided.ApproverMappingDigest,
			decided.ApproverProvenance)
	}

	// The requester comes back for it, and it becomes work exactly once.
	again, err := w.request(alice, request)
	if err != nil || again.State != fleet.ApprovalEnqueued ||
		again.Action.ApproverSubject != w.identity("bob") {
		t.Fatalf("materialization = %+v, %v", again, err)
	}

	// The requester sees who approved their own request — accountability
	// for that request, and intended — and nothing about the mapping.
	handler, err := webapi.NewFleetApprovalHandler(h.opened.approvals)
	if err != nil {
		t.Fatal(err)
	}
	body := serveApprovalStatus(t, h, handler, alice, request.id)
	if body["approver"] != base64.RawURLEncoding.EncodeToString(
		[]byte(w.identity("bob"))) {
		t.Fatalf("the requester's status does not name the approver: %v", body)
	}
	raw, _ := json.Marshal(body)
	for _, hidden := range []string{
		testApproverValue, "realm_access", testApproverAudience,
		hex.EncodeToString(digest[:]), digest.String(),
		base64.RawURLEncoding.EncodeToString([]byte(testApproverValue)),
		base64.StdEncoding.EncodeToString(digest[:]),
	} {
		if bytes.Contains(raw, []byte(hidden)) {
			t.Fatalf("approval status discloses mapping detail %q: %s", hidden, raw)
		}
	}

	// The audit: every approval transition the service wrote holds the
	// provenance it was decided under, and no byte of any token.
	w.mu.Lock()
	written := append([]fleet.ApprovalRecord(nil), w.written...)
	tokens := append([]string(nil), w.tokens...)
	w.mu.Unlock()
	sawDecision := false
	for _, record := range written {
		if record.Verdict == fleet.ApprovalVerdictApprove {
			sawDecision = true
			if !record.ApproverProvenance.Equal(wantProvenance) {
				t.Fatalf("an approved record lost its provenance: %+v", record)
			}
		}
		var encoded bytes.Buffer
		if err := gob.NewEncoder(&encoded).Encode(record); err != nil {
			t.Fatal(err)
		}
		assertNoTokenBytes(t, "an approval record",
			append(encoded.Bytes(), []byte(fmt.Sprintf("%#v", record))...), tokens)
	}
	if !sawDecision {
		t.Fatal("no approved record was written")
	}
	// And nothing on disk — corpus, interaction audit, policy, approvals —
	// carries a token segment.
	if err := filepath.WalkDir(h.root, func(
		path string, entry fs.DirEntry, err error,
	) error {
		if err != nil || entry.IsDir() {
			return err
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		assertNoTokenBytes(t, path, content, tokens)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// assertNoTokenBytes refuses any JWT segment of any token. The header
// segment is shared by every token from one key, so finding none of it is
// the strongest form of the check.
func assertNoTokenBytes(t *testing.T, where string, content []byte, tokens []string) {
	t.Helper()
	for _, token := range tokens {
		for index, segment := range strings.Split(token, ".") {
			if len(segment) < 16 {
				continue
			}
			if bytes.Contains(content, []byte(segment)) {
				t.Fatalf("%s holds JWT segment %d of a token", where, index)
			}
		}
	}
}

func serveApprovalStatus(
	t *testing.T, h *approvalHarness, handler http.Handler,
	ctx context.Context, id []byte,
) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{
		"request_id":  base64.RawURLEncoding.EncodeToString([]byte("status")),
		"reason_code": "test",
		"deadline":    h.now().Add(time.Hour).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/fleet/approvals/" +
		base64.RawURLEncoding.EncodeToString(id) + "/status"
	request := httptest.NewRequest(
		http.MethodPost, path, bytes.NewReader(encoded)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d %s", recorder.Code, recorder.Body)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

// assertNotWorkAs is assertNotWork for an OIDC requester.
func (h *approvalHarness) assertNotWorkAs(ctx context.Context, request heldRequest) {
	h.t.Helper()
	if _, err := h.opened.fleetDispatch.Status(ctx, fleet.StatusRequest{
		ID: request.id, Context: h.context(h.now().Add(time.Minute)),
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		h.t.Fatalf("status of held request = %v, want not found", err)
	}
}

// TestOIDCApproverDelegationChainIsRefused: an approver in the request's own
// delegation chain, or in the agent's registration chain, is refused — the
// same #489 rule, now reachable by OIDC principals.
func TestOIDCApproverDelegationChainIsRefused(t *testing.T) {
	t.Run("agent registration chain", func(t *testing.T) {
		w := newOIDCApprovalWorld(t, nil)
		h := w.h
		carol := w.as(w.fleetToken("carol", nil))
		for _, spec := range []struct{ id, parent shoal.ID }{
			{"oidc-parent", ""}, {"oidc-child", "oidc-parent"},
		} {
			if _, err := h.opened.fleetRegistry.Register(carol, fleet.RegisterRequest{
				Context:         h.context(h.now().Add(time.Minute)),
				RegistrationKey: shoal.ID("registration-" + spec.id),
				Spec: fleet.Spec{
					ID: spec.id, ParentID: spec.parent,
					AuthorizationDomain: workspaceAuthorizationDomain,
					Scopes: []fleet.Scope{{
						SourceID: workspaceSourceID, PolicyID: workspaceGrantPolicyID,
					}},
					ExecutorRef:    "local",
					Capabilities:   approvalActions(true),
					LeaseExpiresAt: h.now().Add(19 * time.Hour),
				},
			}); err != nil {
				t.Fatalf("carol registers %s: %v", spec.id, err)
			}
		}
		request := h.held("oidc-chain-1")
		enqueue := h.enqueue(request)
		enqueue.AgentID = "oidc-child"
		alice := w.as(w.fleetToken("alice", nil))
		receipt, err := h.opened.approvals.Request(alice, enqueue)
		if err != nil || receipt.State != fleet.ApprovalPending {
			t.Fatalf("request against the child = %+v, %v", receipt, err)
		}
		_, err = w.decide(w.as(w.approverToken("carol")), receipt)
		assertNotIndependent(t, "the agents' registrant", err)
		if _, err := w.decide(w.as(w.approverToken("bob")), receipt); err != nil {
			t.Fatalf("an independent approver: %v", err)
		}
	})
	t.Run("request delegation chain", func(t *testing.T) {
		w := newOIDCApprovalWorld(t, func(config *oidcConfig) {
			config.delegationClaim = "on_behalf_of"
		})
		h := w.h
		alice := w.as(w.fleetToken("alice", jwt.MapClaims{
			"on_behalf_of": []string{"dan"},
		}))
		request := h.held("oidc-obo-1")
		receipt, err := w.request(alice, request)
		if err != nil || receipt.State != fleet.ApprovalPending {
			t.Fatalf("delegated request = %+v, %v", receipt, err)
		}
		if got := w.status(alice, request.id).Approval.Request.OnBehalfOf; len(got) != 1 ||
			got[0] != w.identity("dan") {
			t.Fatalf("request on-behalf-of = %v", got)
		}
		_, err = w.decide(w.as(w.approverToken("dan")), receipt)
		assertNotIndependent(t, "the principal the request is made for", err)
		if _, err := w.decide(w.as(w.approverToken("bob")), receipt); err != nil {
			t.Fatalf("an independent approver: %v", err)
		}
	})
}

// TestOIDCApproverMappingMovedAcrossRestart: an approval given under one
// mapping does not survive a restart under another. The approved request
// becomes unresolvable/approver_mapping_moved and never work; a pending
// request is unaffected until decided, and is then decided under the new
// mapping only.
func TestOIDCApproverMappingMovedAcrossRestart(t *testing.T) {
	w := newOIDCApprovalWorld(t, nil)
	h := w.h
	aliceToken := w.fleetToken("alice", nil)
	approved, pending := h.held("moved-approved"), h.held("moved-pending")
	approvedReceipt, err := w.request(w.as(aliceToken), approved)
	if err != nil {
		t.Fatal(err)
	}
	pendingReceipt, err := w.request(w.as(aliceToken), pending)
	if err != nil {
		t.Fatal(err)
	}
	oldBob := w.decision(w.approverToken("bob"))
	if _, err := w.decide(w.bind(oldBob), approvedReceipt); err != nil {
		t.Fatalf("approval under the first mapping: %v", err)
	}
	before := w.digest.Load().(auth.Digest)

	// The operator changes who is an approver, and the service restarts.
	document := approverMappingDocument(w.issuer.server.URL)
	document["values"] = []string{testApproverValue, "release-approvers"}
	w.configure(document)
	h.reopen()
	if after := w.digest.Load().(auth.Digest); after == before {
		t.Fatal("the mapping change did not move the digest")
	}

	alice := w.as(aliceToken)
	if _, err := w.request(alice, approved); !errors.Is(
		err, fleet.ErrApproverMappingMoved) ||
		!shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("materializing under a moved mapping = %v", err)
	}
	status := w.status(alice, approved.id)
	if status.State != fleet.ApprovalUnresolvable ||
		status.Condition != fleet.ApprovalConditionApproverMappingMoved ||
		status.Approval.State != fleet.ApprovalApproved {
		t.Fatalf("status = %q/%q stored %q", status.State, status.Condition,
			status.Approval.State)
	}
	h.assertNotWorkAs(alice, approved)

	// A decision minted under the old mapping cannot decide now.
	if _, err := w.decide(w.bind(oldBob), pendingReceipt); !errors.Is(
		err, fleet.ErrApproverMappingMoved) {
		t.Fatalf("a decision under the old mapping = %v", err)
	}
	// Nor can an approver no mapping granted, now that one is in force.
	if _, err := w.decide(h.as(approver), pendingReceipt); !errors.Is(
		err, fleet.ErrApproverMappingMoved) {
		t.Fatalf("an unmapped approver under a mapping = %v", err)
	}
	// The pending request was unaffected, and a current approver decides it.
	if got := w.status(alice, pending.id); got.State != fleet.ApprovalPending ||
		got.Condition != fleet.ApprovalConditionNone {
		t.Fatalf("pending request after the change = %q/%q", got.State, got.Condition)
	}
	if _, err := w.decide(w.as(w.approverToken("bob")), pendingReceipt); err != nil {
		t.Fatalf("a current approver: %v", err)
	}
	if receipt, err := w.request(alice, pending); err != nil ||
		receipt.State != fleet.ApprovalEnqueued {
		t.Fatalf("materializing under the current mapping = %+v, %v", receipt, err)
	}
}
