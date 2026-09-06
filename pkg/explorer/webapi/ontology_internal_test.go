// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package webapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestOntologyEndpointIsNotPubliclyReachable(t *testing.T) {
	handler, err := NewHandler(allowlistStubService{}, "127.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1/ontology"},
		{http.MethodGet, "/api/v1/ontology/proposals"},
		{http.MethodPost, "/api/v1/ontology/proposals"},
		{http.MethodGet, "/api/v1/ontology/proposals/cHJvcG9zYWw/blast-radius"},
		{http.MethodPost, "/api/v1/ontology/proposals/cHJvcG9zYWw/transition"},
		{http.MethodPost, "/api/v1/extract"},
	} {
		request := httptest.NewRequest(testCase.method, testCase.path, nil)
		if handler.publiclyReachable(request) {
			t.Fatalf("%s %s is public, want authenticated API route",
				testCase.method, testCase.path)
		}
	}
}

func TestOntologyInterpretationReportsUseOpaqueIDCodec(t *testing.T) {
	schema, _ := ontology.NewOntologySchema("wire", "Wire", "", nil)
	property, _ := ontology.NewPropertyDefinition(
		"name", "Name", "", ontology.ValueString, nil, nil)
	concept, _ := ontology.NewConceptDefinition(
		"person", "Person", "", []shoal.ID{property.ID()}, nil)
	version, _ := ontology.NewOntologyVersion(
		schema, "1", time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC),
		[]ontology.ConceptDefinition{concept}, nil,
		[]ontology.PropertyDefinition{property}, nil)
	evidence, _ := ontology.NewEvidenceRef(document.Citation{
		DocumentID: "doc", RevisionID: "rev", SectionID: "section", SpanID: "span",
		Range: document.SourceRange{
			Start: document.SourcePosition{Offset: 0, Page: 1},
			End:   document.SourcePosition{Offset: 4, Page: 1},
		},
	}, "name", nil)
	provenance, _ := ontology.NewExtractionProvenance(
		"provider", "model", "1", "prompt", "1", "extractor", "1", nil)
	value, _ := ontology.NewStringValue("Ada")
	identity, _ := ontology.NewOntologyIdentity(version)
	assertion, err := ontology.NewAssertion(
		"person-1", property.ID(), value, ontology.AssertionExplicit, 1,
		[]ontology.EvidenceRef{evidence}, provenance, nil,
		ontology.WithAssertionSubjectType(concept.ID()),
		ontology.WithAssertionOntology(identity))
	if err != nil {
		t.Fatal(err)
	}
	reports := ontologyInterpretationReports(
		[]ontology.AssertionInterpretation{
			ontology.ReadAssertionUnder(assertion, identity),
		})
	if len(reports) != 1 ||
		reports[0].AssertionID != encodeID(assertion.ID()) ||
		reports[0].SchemaID != encodeID(identity.SchemaID()) ||
		reports[0].Predicate != encodeID(property.ID()) {
		t.Fatalf("interpretation report IDs = %#v", reports)
	}
}

type allowlistStubService struct{}

func (allowlistStubService) Documents(context.Context, DocumentsRequest) (DocumentsResponse, error) {
	return DocumentsResponse{}, nil
}

func (allowlistStubService) Document(context.Context, DocumentRequest) (DocumentResponse, error) {
	return DocumentResponse{}, nil
}

func (allowlistStubService) Retrieve(context.Context, RetrievalRequest) (RetrievalResponse, error) {
	return RetrievalResponse{}, nil
}

func (allowlistStubService) Neighborhood(
	context.Context,
	NeighborhoodRequest,
) (NeighborhoodResponse, error) {
	return NeighborhoodResponse{}, nil
}

func (allowlistStubService) Path(context.Context, PathRequest) (PathResponse, error) {
	return PathResponse{}, nil
}

var _ Service = allowlistStubService{}
