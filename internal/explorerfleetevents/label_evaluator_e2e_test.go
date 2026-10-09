// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleetevents

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/internal/explorerfleet"
	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/explorer/evidencelabels"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleetevents"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// labelPlane is the production composition of the label evaluator (#564): a
// real corpus behind a real authorized client holding a document on source B
// labelled "secret", and dispatch and fleet events composed through
// ComposeDispatchWithAttestations and composeWithPublisher with that client's
// evaluator and translator, exactly as shoal-explore-web wires them.
type labelPlane struct {
	authority *auth.Authority
	dispatch  *fleet.DispatchService
	events    *fleetevents.Service
	backend   *Adapter
	completed fleet.ActionRecord
	reference fleet.EvidenceRef
	now       time.Time
}

// fullLabels is the production wiring: the client's evaluator and translator.
func fullLabels(client *authorized.Client) explorerfleet.DispatchLabels {
	return explorerfleet.DispatchLabels{
		Visibility: client.LabelVisibility(), Translator: client.LabelTranslator(),
	}
}

// newLabelPlane composes the plane with the dispatch labels choose picks from
// the real client, and the client's evaluator on fleet events when
// wireEvents.
func newLabelPlane(
	t *testing.T,
	choose func(*authorized.Client) explorerfleet.DispatchLabels,
	wireEvents bool,
) labelPlane {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = corpus.Close() })
	selector, err := authorized.NewStaticPolicySelector(sourceB, []byte("policy-b"))
	if err != nil {
		t.Fatal(err)
	}
	client, err := authorized.NewClient(authorized.Config{
		Base: corpus, Resolver: authority.Resolver(), PolicySelector: selector,
		PolicyStore:      authorized.NewMemoryPolicyStore(),
		GenerationReader: integrationGeneration{},
		Clock:            func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	labels := choose(client)

	// The labelled document, ingested by a principal holding its label.
	ingester := bindPlaneSubject(t, authority, now, "ingester", true,
		auth.OperationIngest, auth.OperationRead)
	receipt, err := client.Ingest(ingester, explorer.Source{
		URI: "file:///b/closed.md", MediaType: explorer.MediaTypeMarkdown,
		Content:  "# Closed\n\nA paragraph the action retrieved.\n",
		Metadata: shoal.Metadata{interaction.PropertyVisibility: "secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := client.Document(ingester, receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	section := view.Root
	for len(section.Spans) == 0 {
		if len(section.Children) == 0 {
			t.Fatal("the ingested document has no span")
		}
		section = section.Children[0]
	}
	span := section.Spans[0]
	// What an executor reports: the node's shoal.visibility, free-form.
	reported := fleet.EvidenceRef{
		AnchorID: "anchor-b-closed", Kind: interaction.EvidenceDocument,
		Citation: document.Citation{
			DocumentID: receipt.Document.ID, RevisionID: receipt.Revision.ID,
			SectionID: span.SectionID, SpanID: span.ID, Range: span.Range,
		},
		NodeIDs:    []shoal.ID{receipt.Document.ID, span.SectionID, span.ID},
		Visibility: []string{"secret"},
	}

	config := runtimeConfig(t.TempDir())
	config = explorerfleet.ConfigureRuntime(config)
	ConfigureRuntime(&config)
	runtime, err := explorercoord.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	executor := &integrationExecutor{result: fleet.ExecutionResult{
		Output:             json.RawMessage(`{"ok":true}`),
		EvidenceSnapshotID: "snapshot", EvidenceSnapshotAsOf: now,
		Evidence: []fleet.EvidenceRef{openEvidence, reported},
	}}
	registry, _ := newIntegrationRegistry(t, authority.Resolver(), now, executor)
	backend, err := New(runtime, config.Domain)
	if err != nil {
		t.Fatal(err)
	}
	var visibility evidencelabels.Visibility
	if wireEvents {
		visibility = client.LabelVisibility()
	}
	events, publisher, err := composeWithPublisher(
		backend, authority.Resolver(), integrationGeneration{},
		integrationAuditor{}, integrationLease{}, bytes.Repeat([]byte{7}, 32),
		func() time.Time { return now }, visibility)
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := explorerfleet.ComposeDispatchWithAttestations(
		runtime, registry, authority.Resolver(), integrationActionRecorder{},
		publisher, nil, func() time.Time { return now }, nil, labels)
	if err != nil {
		t.Fatal(err)
	}
	owner := bindPlaneSubject(t, authority, now, "owner", false,
		auth.OperationDispatch, auth.OperationInvoke)
	queued, err := dispatch.Enqueue(owner,
		integrationEnqueueRequest(now, "labelled", "enqueue-labelled"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := dispatch.Claim(owner, fleet.ClaimRequest{
		ID: queued.ID, ExpectedVersion: queued.Version,
		ClaimID: []byte("claim-labelled"), Lease: time.Minute,
		Context: integrationRequestContext(now),
	})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := dispatch.ExecuteClaim(owner, claimed)
	if err != nil || completed.State != fleet.DispatchSucceeded {
		t.Fatalf("complete = %#v, %v", completed, err)
	}
	return labelPlane{
		authority: authority, dispatch: dispatch, events: events,
		backend: backend, completed: completed, reference: reported, now: now,
	}
}

// bindPlaneSubject binds a decision on the action's source and on B; holds
// adds B's secret label policy.
func bindPlaneSubject(
	t *testing.T, authority *auth.Authority, now time.Time,
	subject shoal.ID, holds bool, operations ...auth.Operation,
) context.Context {
	t.Helper()
	policies := [][]byte{[]byte("policy"), []byte("policy-b")}
	if holds {
		policies = append(policies, sourceBLabelPolicy("secret"))
	}
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: subject, Actor: "actor", ClientID: "client",
		AuthorizationDomain:   []byte("domain"),
		AllowedOperations:     operations,
		PermittedSourceIDs:    [][]byte{[]byte("source"), sourceB},
		PermittedPolicyIDs:    policies,
		PolicyGeneration:      1,
		AuthenticationExpires: now.Add(2 * time.Hour),
		RequestID:             "request", CorrelationID: "correlation",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

// secretBTerms is the grant labels of B's secret label policy.
func secretBTerms(t *testing.T) []string {
	t.Helper()
	policy, err := auth.NewPolicy(auth.PolicyConfig{
		AuthorizationDomain: []byte("domain"), SourceID: sourceB,
		GrantPolicyID: sourceBLabelPolicy("secret"), Epoch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	terms, err := policy.VisibilityTerms()
	if err != nil {
		t.Fatal(err)
	}
	labels, err := interaction.Conjoin(terms)
	if err != nil {
		t.Fatal(err)
	}
	return labels
}

func (p labelPlane) status(t *testing.T, ctx context.Context) fleet.ActionRecord {
	t.Helper()
	record, err := p.dispatch.Status(ctx, fleet.StatusRequest{
		ID: p.completed.ID, Context: integrationRequestContext(p.now),
	})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func (p labelPlane) delivered(t *testing.T, subject shoal.ID, holds bool) fleetevents.Event {
	t.Helper()
	ctx := bindPlaneSubject(t, p.authority, p.now, subject, holds,
		auth.OperationSubscriptionCreate)
	subscription, err := p.events.Create(ctx, fleetevents.CreateRequest{
		Token: []byte("subscribe-" + subject), RetryUntil: p.now.Add(time.Hour),
		AgentID: "agent", AgentGeneration: 1, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := p.events.Pull(ctx, fleetevents.PullRequest{
		SubscriptionID: subscription.ID, Limit: fleetevents.MaxPageSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range page.Events {
		if event.Kind == "action.completed" {
			return event
		}
	}
	t.Fatal("the completion was not delivered")
	return fleetevents.Event{}
}

func anchorsOf(references []fleet.EvidenceRef) []shoal.ID {
	result := make([]shoal.ID, 0, len(references))
	for _, reference := range references {
		result = append(result, reference.AnchorID)
	}
	return result
}

// TestTheProductionCompositionRecordsAndDecidesStructuredLabels is #564 end
// to end. The executor reports the retrieved node's free-form label; dispatch
// records the structured terms of the label policy enforcing it on that node;
// a reader holding that policy sees the evidence exactly as stored on Status
// and on delivery, and one who does not sees neither it nor any trace of it.
func TestTheProductionCompositionRecordsAndDecidesStructuredLabels(t *testing.T) {
	plane := newLabelPlane(t, fullLabels, true)
	var recorded fleet.EvidenceRef
	for _, reference := range plane.completed.Evidence {
		if reference.AnchorID == plane.reference.AnchorID {
			recorded = reference
		}
	}
	if !reflect.DeepEqual(recorded.Visibility, secretBTerms(t)) {
		t.Fatalf("the evidence was recorded with %v, want the label policy's "+
			"terms %v: a free-form label is held by no reader",
			recorded.Visibility, secretBTerms(t))
	}

	holder := bindPlaneSubject(t, plane.authority, plane.now, "owner", true,
		auth.OperationDispatch, auth.OperationInvoke)
	if got := plane.status(t, holder); !reflect.DeepEqual(got.Evidence, plane.completed.Evidence) {
		t.Fatalf("a holder's Status returned %#v, want the stored %#v",
			got.Evidence, plane.completed.Evidence)
	}
	outsider := bindPlaneSubject(t, plane.authority, plane.now, "owner", false,
		auth.OperationDispatch, auth.OperationInvoke)
	if got := anchorsOf(plane.status(t, outsider).Evidence); !reflect.DeepEqual(
		got, []shoal.ID{openEvidence.AnchorID}) {
		t.Fatalf("an outsider's Status returned anchors %v", got)
	}

	stored := storedCompletion(t, labelledDelivery{backend: plane.backend})
	if got := plane.delivered(t, "holder", true); !reflect.DeepEqual(got, stored) {
		t.Fatalf("a holding subscriber received\n%#v\nwant the stored\n%#v", got, stored)
	}
	outside := plane.delivered(t, "outsider", false)
	encoded, err := json.Marshal(outside)
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range append(secretBTerms(t), string(plane.reference.Citation.DocumentID),
		string(plane.reference.AnchorID)) {
		if bytes.Contains(encoded, []byte(marker)) {
			t.Fatalf("an outsider received %q: %s", marker, encoded)
		}
	}
}

// TestEachConsumerFailsClosedWithoutTheEvaluator is the wiring check: with
// the evaluator or the translator left out of one consumer, a holder no
// longer sees the labelled evidence there. Every probe here must fail, which
// is what makes the passing composition above evidence of its wiring.
func TestEachConsumerFailsClosedWithoutTheEvaluator(t *testing.T) {
	holderSees := func(t *testing.T, plane labelPlane) []shoal.ID {
		t.Helper()
		holder := bindPlaneSubject(t, plane.authority, plane.now, "owner", true,
			auth.OperationDispatch, auth.OperationInvoke)
		return anchorsOf(plane.status(t, holder).Evidence)
	}
	only := []shoal.ID{openEvidence.AnchorID}
	t.Run("dispatch without the translator", func(t *testing.T) {
		plane := newLabelPlane(t, func(client *authorized.Client) explorerfleet.DispatchLabels {
			return explorerfleet.DispatchLabels{Visibility: client.LabelVisibility()}
		}, true)
		if got := holderSees(t, plane); !reflect.DeepEqual(got, only) {
			t.Fatalf("without the translator a holder still saw %v", got)
		}
	})
	t.Run("dispatch without the evaluator", func(t *testing.T) {
		plane := newLabelPlane(t, func(client *authorized.Client) explorerfleet.DispatchLabels {
			return explorerfleet.DispatchLabels{Translator: client.LabelTranslator()}
		}, true)
		if got := holderSees(t, plane); !reflect.DeepEqual(got, only) {
			t.Fatalf("without the evaluator a holder still saw %v", got)
		}
	})
	t.Run("events without the evaluator", func(t *testing.T) {
		plane := newLabelPlane(t, fullLabels, false)
		got := plane.delivered(t, "holder", true)
		for _, reference := range got.ConsumedEvidence {
			if reference.AnchorID == plane.reference.AnchorID {
				t.Fatal("without the evaluator a holding subscriber still " +
					"received the labelled reference")
			}
		}
	})
}
