/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package contextpack

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

var testDirectorySequence atomic.Uint64

func TestBuildFromEmbeddedExplorerExactEvidenceAndDeterminism(t *testing.T) {
	client, request, response, pins := embeddedFixture(t)
	builder := Builder{Reader: client}
	input := InitialRequest{
		Request: request, Response: response, Pins: pins,
		Metadata: shoal.Metadata{"application": "test"},
	}
	first, err := builder.Build(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := builder.Build(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID() != second.ID() {
		t.Fatalf("repeated build IDs differ: %q != %q", first.ID(), second.ID())
	}
	if first.Snapshot().ID() != pins.Snapshot.ID() ||
		first.Authorization().Fingerprint() != pins.Authorization.Fingerprint() {
		t.Fatal("pack lost snapshot or authorization identity")
	}
	if first.Metadata()[metadataRequestKey] != string(response.RequestID) {
		t.Fatal("pack lost retrieval request identity")
	}

	var documentCount, graphCount int
	for _, anchor := range first.Evidence() {
		switch anchor.Kind() {
		case inference.AnchorDocument:
			documentCount++
			citation, quote, ok := anchor.Document()
			if !ok || citation.SpanID == "" || quote == "" {
				t.Fatal("document anchor lost exact citation or quote")
			}
		case inference.AnchorGraph:
			graphCount++
			path, ok := anchor.Path()
			if !ok || len(path.Nodes) == 0 {
				t.Fatal("graph anchor lost exact path")
			}
		default:
			t.Fatalf("unexpected anchor kind %q", anchor.Kind())
		}
	}
	if documentCount == 0 || graphCount == 0 {
		t.Fatalf("document anchors = %d, graph anchors = %d", documentCount, graphCount)
	}

	reversed := cloneResponse(response)
	reverseResults(reversed.Results)
	for index := range reversed.Results {
		reverseEvidence(reversed.Results[index].Evidence)
	}
	reordered, err := builder.Build(context.Background(), InitialRequest{
		Request: request, Response: reversed, Pins: pins,
		Metadata: shoal.Metadata{"application": "test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID() != reordered.ID() {
		t.Fatal("input result/evidence order changed canonical pack identity")
	}
}

func TestBuildDocumentGraphAndMixedSelection(t *testing.T) {
	client, request, response, pins := embeddedFixture(t)
	builder := Builder{Reader: client}
	for name, selection := range map[string]EvidenceSelection{
		"document": {Documents: true},
		"graph":    {Paths: true},
		"mixed":    {},
	} {
		t.Run(name, func(t *testing.T) {
			pack, err := builder.Build(context.Background(), InitialRequest{
				Request: request, Response: response, Selection: selection, Pins: pins,
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, anchor := range pack.Evidence() {
				if name == "document" && anchor.Kind() != inference.AnchorDocument {
					t.Fatal("document-only pack contains graph evidence")
				}
				if name == "graph" && anchor.Kind() != inference.AnchorGraph {
					t.Fatal("graph-only pack contains document evidence")
				}
			}
		})
	}
}

func TestBuildRejectsStaleCitationRangeRevisionAndPath(t *testing.T) {
	client, request, response, pins := embeddedFixture(t)
	builder := Builder{Reader: client}

	staleRevision := cloneResponse(response)
	staleRevision.Results[0].Evidence[0].Citation.RevisionID = "stale-revision"
	_, err := builder.Build(context.Background(), InitialRequest{
		Request: request, Response: staleRevision, Pins: pins,
	})
	assertCode(t, err, shoal.ErrorNotFound)

	badRange := cloneResponse(response)
	evidence := &badRange.Results[0].Evidence[0]
	evidence.Citation.Range.Start.Offset++
	evidence.Citation.Range.End.Offset++
	_, err = builder.Build(context.Background(), InitialRequest{
		Request: request, Response: badRange, Pins: pins,
	})
	assertCode(t, err, shoal.ErrorInvalidArgument)

	badPath := cloneResponse(response)
	badPath.Results[0].Evidence[0].Path.Nodes[0].Labels =
		append(badPath.Results[0].Evidence[0].Path.Nodes[0].Labels, "stale")
	_, err = builder.Build(context.Background(), InitialRequest{
		Request: request, Response: badPath,
		Selection: EvidenceSelection{Paths: true}, Pins: pins,
	})
	assertCode(t, err, shoal.ErrorInvalidArgument)
}

func TestOpenSectionAndExpandNeighborsAreExplicitBoundedAndImmutable(t *testing.T) {
	client, request, response, pins := embeddedFixture(t)
	builder := Builder{Reader: client}
	initial, err := builder.Build(context.Background(), InitialRequest{
		Request: request, Response: response,
		Selection: EvidenceSelection{Documents: true}, Pins: pins,
	})
	if err != nil {
		t.Fatal(err)
	}
	citation, _, _ := initial.Evidence()[0].Document()
	view, err := client.Document(context.Background(), citation.DocumentID, citation.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	sectionID := firstSectionWithSpans(t, view.Root)
	opened, err := builder.OpenSection(context.Background(), initial, OpenSectionRequest{
		DocumentID: citation.DocumentID,
		RevisionID: citation.RevisionID,
		SectionIDs: []shoal.ID{sectionID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(opened.Evidence()) < len(initial.Evidence()) {
		t.Fatal("opening a section removed existing evidence")
	}
	if initial.ID() == opened.ID() && len(opened.Evidence()) != len(initial.Evidence()) {
		t.Fatal("expanded immutable pack retained its old identity")
	}

	full, err := builder.Build(context.Background(), InitialRequest{
		Request: request, Response: response, Pins: pins,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := firstGraphPath(t, full)
	expanded, err := builder.ExpandNeighbors(context.Background(), full, ExpandNeighborsRequest{
		NodeIDs: []shoal.ID{path.Nodes[0].ID},
		Depth:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(expanded.Evidence()) < len(full.Evidence()) {
		t.Fatal("neighbor expansion removed existing evidence")
	}
	if len(full.Evidence()) == 0 {
		t.Fatal("input pack was mutated")
	}
	if _, err := (Builder{
		Reader: client, Limits: Limits{MaxPathNodes: 2},
	}).ExpandNeighbors(context.Background(), initial, ExpandNeighborsRequest{
		NodeIDs: []shoal.ID{path.Nodes[0].ID},
		Depth:   2,
	}); err != nil {
		t.Fatalf("neighborhood depth was incorrectly treated as path length: %v", err)
	}

	_, err = (Builder{Reader: client, Limits: Limits{MaxHierarchyDepth: 1}}).
		OpenSection(context.Background(), initial, OpenSectionRequest{
			DocumentID: citation.DocumentID, RevisionID: citation.RevisionID,
			SectionIDs: []shoal.ID{sectionID}, Depth: 2,
		})
	assertCode(t, err, shoal.ErrorInvalidArgument)
	_, err = builder.OpenSection(context.Background(), initial, OpenSectionRequest{
		DocumentID: citation.DocumentID, RevisionID: citation.RevisionID,
		SectionIDs: []shoal.ID{sectionID, sectionID},
	})
	assertCode(t, err, shoal.ErrorInvalidArgument)
	_, err = builder.ExpandNeighbors(context.Background(), full, ExpandNeighborsRequest{
		NodeIDs: []shoal.ID{path.Nodes[0].ID, path.Nodes[0].ID},
	})
	assertCode(t, err, shoal.ErrorInvalidArgument)
}

func TestContextByteLimitIncludesPinsAndCanonicalFraming(t *testing.T) {
	client, request, response, pins := embeddedFixture(t)
	request.Text = "  exact   context  "
	pack, err := (Builder{Reader: client}).Build(context.Background(), InitialRequest{
		Request: request, Response: response, Pins: pins,
	})
	if err != nil {
		t.Fatal(err)
	}
	if pack.Query() != "exact context" {
		t.Fatalf("normalized query = %q", pack.Query())
	}
	size, _, _, _, err := contextPackByteSize(
		pack.Query(), pack.Evidence(), pins, pack.Metadata(), DefaultMaxPathNodes)
	if err != nil {
		t.Fatal(err)
	}
	if err := enforcePackBounds(
		pack.Query(), pack.Evidence(), pins, pack.Metadata(),
		mustLimits(t, Limits{MaxContextBytes: size}),
	); err != nil {
		t.Fatalf("exact context byte limit rejected: %v", err)
	}
	if _, err := (Builder{
		Reader: client, Limits: Limits{MaxContextBytes: size},
	}).Build(context.Background(), InitialRequest{
		Request: request, Response: response, Pins: pins,
	}); err != nil {
		t.Fatalf("normalized query rejected at exact context byte limit: %v", err)
	}
	if err := enforcePackBounds(
		pack.Query(), pack.Evidence(), pins, pack.Metadata(),
		mustLimits(t, Limits{MaxContextBytes: size - 1}),
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("over-limit context error = %v", err)
	}
}

func TestRetrievalIdentityPreservesOpaqueIDs(t *testing.T) {
	request, err := (retrieval.Request{Text: "query"}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	left := retrieval.Response{Results: []retrieval.Result{{ID: shoal.ID("\xff")}}}
	right := retrieval.Response{Results: []retrieval.Result{{ID: shoal.ID("\xfe")}}}
	leftIdentity, err := retrievalIdentity(request, left)
	if err != nil {
		t.Fatal(err)
	}
	rightIdentity, err := retrievalIdentity(request, right)
	if err != nil {
		t.Fatal(err)
	}
	if leftIdentity == rightIdentity {
		t.Fatal("opaque result IDs collided in retrieval identity")
	}
}

func TestBoundsFailClosed(t *testing.T) {
	client, request, response, pins := embeddedFixture(t)
	if len(response.Results) < 2 {
		t.Fatal("fixture requires at least two results")
	}
	_, err := (Builder{Reader: client, Limits: Limits{MaxResults: 1}}).
		Build(context.Background(), InitialRequest{
			Request: request, Response: response, Pins: pins,
		})
	assertCode(t, err, shoal.ErrorInvalidArgument)

	_, err = (Builder{Reader: client, Limits: Limits{MaxAnchors: 1}}).
		Build(context.Background(), InitialRequest{
			Request: request, Response: response, Pins: pins,
		})
	assertCode(t, err, shoal.ErrorInvalidArgument)

	_, err = (Builder{Reader: client, Limits: Limits{MaxSections: 1}}).
		Build(context.Background(), InitialRequest{
			Request: request, Response: response,
			Selection: EvidenceSelection{Documents: true}, Pins: pins,
		})
	assertCode(t, err, shoal.ErrorInvalidArgument)

	_, err = (Builder{Reader: client, Limits: Limits{MaxQuoteBytes: 1}}).
		Build(context.Background(), InitialRequest{
			Request: request, Response: response,
			Selection: EvidenceSelection{Documents: true}, Pins: pins,
		})
	assertCode(t, err, shoal.ErrorInvalidArgument)

	_, err = (Builder{Reader: client, Limits: Limits{MaxContextBytes: 32}}).
		Build(context.Background(), InitialRequest{
			Request: request, Response: response, Pins: pins,
		})
	assertCode(t, err, shoal.ErrorInvalidArgument)

	_, err = (Builder{Reader: client, Limits: Limits{MaxGraphNodes: 1}}).
		Build(context.Background(), InitialRequest{
			Request: request, Response: response,
			Selection: EvidenceSelection{Paths: true}, Pins: pins,
		})
	assertCode(t, err, shoal.ErrorInvalidArgument)

	_, err = (Builder{Reader: client, Limits: Limits{MaxProvenanceBytes: 1}}).
		Build(context.Background(), InitialRequest{
			Request: request, Response: response, Pins: pins,
		})
	assertCode(t, err, shoal.ErrorInvalidArgument)
}

func TestMutationIsolationAndNoUncitedExplanationText(t *testing.T) {
	client, request, response, pins := embeddedFixture(t)
	const uncited = "this explanation is not evidence"
	response.Results[0].Explanation.Summary = uncited
	builder := Builder{Reader: client}
	pack, err := builder.Build(context.Background(), InitialRequest{
		Request: request, Response: response, Pins: pins,
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeID := pack.ID()
	response.Results[0].Evidence[0].Quote = "mutated"
	response.Results[0].Evidence[0].Path.Nodes[0].Labels = []string{"mutated"}
	response.Results[0].Explanation.Summary = "mutated"
	if pack.ID() != beforeID {
		t.Fatal("caller mutation changed pack identity")
	}
	for key, value := range pack.Metadata() {
		if strings.Contains(key, uncited) || strings.Contains(value, uncited) {
			t.Fatal("uncited explanation prose entered context metadata")
		}
	}
	for _, anchor := range pack.Evidence() {
		if _, quote, ok := anchor.Document(); ok && strings.Contains(quote, uncited) {
			t.Fatal("uncited explanation prose entered evidence")
		}
	}
}

func TestCancellationAndAuthorizedNotFoundShape(t *testing.T) {
	_, request, response, pins := embeddedFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := &recordingReader{}
	_, err := (Builder{Reader: reader}).Build(ctx, InitialRequest{
		Request: request, Response: response, Pins: pins,
	})
	assertCode(t, err, shoal.ErrorUnavailable)
	if reader.documentCalls != 0 || reader.neighborhoodCalls != 0 {
		t.Fatal("canceled construction called the Explorer seam")
	}

	notFound := shoal.NewError(shoal.ErrorNotFound, "document revision not found")
	for _, documentID := range []shoal.ID{"hidden", "absent"} {
		reader := &recordingReader{documentErr: notFound}
		modified := cloneResponse(response)
		modified.Results[0].Evidence[0].Citation.DocumentID = documentID
		_, err := (Builder{Reader: reader}).Build(context.Background(), InitialRequest{
			Request: request, Response: modified,
			Selection: EvidenceSelection{Documents: true}, Pins: pins,
		})
		if err == nil || err.Error() !=
			fmt.Sprintf("result %q document evidence: %v", modified.Results[0].ID, notFound) {
			t.Fatalf("%s error shape = %v", documentID, err)
		}
	}
}

func TestHydratedDuplicatesRequireExactContentAndRequestedIdentity(t *testing.T) {
	client, request, response, pins := embeddedFixture(t)
	citation := response.Results[0].Evidence[0].Citation
	view, err := client.Document(context.Background(), citation.DocumentID, citation.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	conflict := cloneView(view)
	conflict.Document.Title += " changed"
	_, err = (Builder{Reader: client}).Build(context.Background(), InitialRequest{
		Request: request, Response: response, Pins: pins,
		Documents: []explorer.DocumentView{view, conflict},
	})
	assertCode(t, err, shoal.ErrorInvalidArgument)

	opaqueLeft := cloneView(view)
	opaqueLeft.Document.Metadata = shoal.Metadata{"opaque": "\xff"}
	opaqueRight := cloneView(view)
	opaqueRight.Document.Metadata = shoal.Metadata{"opaque": "\xfe"}
	_, err = (Builder{Reader: client}).Build(context.Background(), InitialRequest{
		Request: request, Response: response, Pins: pins,
		Documents: []explorer.DocumentView{opaqueLeft, opaqueRight},
	})
	assertCode(t, err, shoal.ErrorInvalidArgument)

	mismatched := cloneView(view)
	mismatched.Document.ID = "different-document"
	reader := &recordingReader{documentView: mismatched}
	_, err = (Builder{Reader: reader}).Build(context.Background(), InitialRequest{
		Request: request, Response: response,
		Selection: EvidenceSelection{Documents: true}, Pins: pins,
	})
	assertCode(t, err, shoal.ErrorInvalidArgument)
}

type recordingReader struct {
	documentView      explorer.DocumentView
	documentErr       error
	neighborhood      explorer.Neighborhood
	neighborhoodErr   error
	documentCalls     int
	neighborhoodCalls int
}

func (r *recordingReader) Document(
	context.Context, shoal.ID, shoal.ID,
) (explorer.DocumentView, error) {
	r.documentCalls++
	return cloneView(r.documentView), r.documentErr
}

func (r *recordingReader) Neighborhood(
	context.Context, explorer.NeighborhoodRequest,
) (explorer.Neighborhood, error) {
	r.neighborhoodCalls++
	return r.neighborhood, r.neighborhoodErr
}

func embeddedFixture(
	t *testing.T,
) (*explorer.Explorer, retrieval.Request, retrieval.Response, Pins) {
	t.Helper()
	path := filepath.Join(
		"testdata",
		fmt.Sprintf("contextpack-%d-%d", os.Getpid(), testDirectorySequence.Add(1)),
	)
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	client, err := explorer.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close Explorer: %v", err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("remove Explorer fixture: %v", err)
		}
	})
	for index, source := range []explorer.Source{
		{
			URI: "memory://alpha", Title: "Alpha", MediaType: explorer.MediaTypeMarkdown,
			Content: "# Alpha\n\nGrounded context uses exact citations.\n\n## Details\n\nGraph paths stay exact.\n",
		},
		{
			URI: "memory://beta", Title: "Beta", MediaType: explorer.MediaTypeMarkdown,
			Content: "# Beta\n\nExact context is deterministic and bounded.\n",
		},
	} {
		if _, err := client.IngestWithOptions(context.Background(), source, explorer.IngestOptions{
			CreatedAt: time.Date(2026, 8, 28, 10, index, 0, 0, time.UTC),
		}); err != nil {
			t.Fatal(err)
		}
	}
	request, err := (retrieval.Request{
		Text:    "exact context",
		TopK:    10,
		Modes:   []retrieval.Mode{retrieval.ModeLexical, retrieval.ModeTree, retrieval.ModeGraph},
		Explain: true,
	}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Retrieve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) < 2 {
		t.Fatalf("retrieval returned %d results", len(response.Results))
	}
	snapshot, err := inference.NewSnapshotPin(
		"snapshot:test", time.Date(2026, 8, 28, 10, 5, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	auth, err := inference.NewAuthPin(
		"auth:test", time.Date(2026, 8, 28, 11, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return client, request, response, Pins{
		Snapshot: snapshot, Authorization: auth, PolicyID: "policy:test",
	}
}

func firstSectionWithSpans(t *testing.T, root explorer.SectionView) shoal.ID {
	t.Helper()
	if len(root.Spans) > 0 {
		return root.Section.ID
	}
	for _, child := range root.Children {
		if id := firstSectionWithSpans(t, child); id != "" {
			return id
		}
	}
	t.Fatal("document has no section with spans")
	return ""
}

func firstGraphPath(t *testing.T, pack inference.ContextPack) graph.Path {
	t.Helper()
	for _, anchor := range pack.Evidence() {
		if path, ok := anchor.Path(); ok {
			return path
		}
	}
	t.Fatal("pack has no graph path")
	return graph.Path{}
}

func reverseResults(values []retrieval.Result) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func reverseEvidence(values []retrieval.Evidence) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func assertCode(t *testing.T, err error, code shoal.ErrorCode) {
	t.Helper()
	if !shoal.IsErrorCode(err, code) {
		t.Fatalf("error = %v, want code %q", err, code)
	}
}

func mustLimits(t *testing.T, limits Limits) Limits {
	t.Helper()
	normalized, err := normalizeLimits(limits)
	if err != nil {
		t.Fatal(err)
	}
	return normalized
}
