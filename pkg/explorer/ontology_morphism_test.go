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
 */

package explorer

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestPublishedMorphismAndHistoricAssertionSurviveRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "corpus")
	schema, _ := ontology.NewOntologySchema("restart", "Restart", "", nil)
	person, _ := ontology.NewConceptDefinition("person", "Person", "", nil, nil)
	org, _ := ontology.NewConceptDefinition("org", "Org", "", nil, nil)
	oldRel, _ := ontology.NewRelationshipDefinition(
		"member-of", "Member Of", "", []shoal.ID{person.ID()},
		[]shoal.ID{org.ID()}, nil, true, nil)
	newRel, _ := ontology.NewRelationshipDefinition(
		"belongs-to", "Belongs To", "", []shoal.ID{person.ID()},
		[]shoal.ID{org.ID()}, nil, true, nil)
	at := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	base, _ := ontology.NewOntologyVersion(
		schema, "1", at, []ontology.ConceptDefinition{person, org},
		[]ontology.RelationshipDefinition{oldRel}, nil, nil)
	target, _ := ontology.NewOntologyVersion(
		schema, "2", at.Add(time.Second), []ontology.ConceptDefinition{person, org},
		[]ontology.RelationshipDefinition{newRel}, nil, nil)
	evidence, _ := ontology.NewEvidenceRef(document.Citation{
		DocumentID: "doc", RevisionID: "rev", SectionID: "section", SpanID: "span",
		Range: document.SourceRange{
			Start: document.SourcePosition{Offset: 0, Page: 1},
			End:   document.SourcePosition{Offset: 4, Page: 1},
		},
	}, "rename evidence", nil)
	morphism, err := ontology.NewOntologyMorphism(ontology.MorphismConfig{
		Kind: ontology.MorphismRename, SourceVersion: base, TargetVersion: target,
		Sources: []shoal.ID{oldRel.ID()}, Targets: []shoal.ID{newRel.ID()},
		Evidence: []ontology.EvidenceRef{evidence}, Rationale: "rename relationship",
	})
	if err != nil {
		t.Fatal(err)
	}

	proposal, err := ontology.NewGovernedProposalWithMorphisms(
		schema, base, target, []ontology.OntologyMorphism{morphism},
		"author", "rename", at.Add(2*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	corpus, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := corpus.CreateOntologyProposal(ctx, proposal, base); err != nil {
		t.Fatal(err)
	}
	for index, state := range []ontology.ProposalState{
		ontology.ProposalSubmitted, ontology.ProposalApproved, ontology.ProposalPublished,
	} {
		proposal, err = corpus.TransitionOntologyProposal(
			ctx, proposal.ID(), state, "governor", "approved",
			at.Add(time.Duration(index+3)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	corpus, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	proposals, err := corpus.OntologyProposals(ctx)
	if err != nil || len(proposals) != 1 || len(proposals[0].Morphisms()) != 1 ||
		proposals[0].State() != ontology.ProposalPublished {
		t.Fatalf("restored proposals = %#v, err = %v", proposals, err)
	}
	provenance, _ := ontology.NewExtractionProvenance(
		"provider", "model", "1", "prompt", "1", "extractor", "1", nil)
	value, _ := ontology.NewReferenceValue("org-1")
	baseIdentity, _ := ontology.NewOntologyIdentity(base)
	assertion, err := ontology.NewAssertion(
		"person-1", oldRel.ID(), value, ontology.AssertionExplicit, 1,
		[]ontology.EvidenceRef{evidence}, provenance, nil,
		ontology.WithAssertionSubjectType(person.ID()),
		ontology.WithAssertionObjectType(org.ID()),
		ontology.WithAssertionOntology(baseIdentity))
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity, _ := ontology.NewOntologyIdentity(target)
	read, err := corpus.InterpretAssertions(
		ctx, []ontology.Assertion{assertion}, targetIdentity)
	if err != nil || len(read) != 1 || !read[0].Resolved() ||
		read[0].Predicate() != newRel.ID() ||
		read[0].Original().Predicate() != oldRel.ID() {
		t.Fatalf("restart interpretation = %#v, err = %v", read, err)
	}
}

func TestInterpretAssertionsResolvesSameVersionWithoutProposalCatalog(t *testing.T) {
	corpus, err := Open(filepath.Join(t.TempDir(), "corpus"))
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	schema, _ := ontology.NewOntologySchema("direct", "Direct", "", nil)
	property, _ := ontology.NewPropertyDefinition(
		"name", "Name", "", ontology.ValueString, nil, nil)
	concept, _ := ontology.NewConceptDefinition(
		"person", "Person", "", []shoal.ID{property.ID()}, nil)
	version, _ := ontology.NewOntologyVersion(
		schema, "1", time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC),
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
	read, err := corpus.InterpretAssertions(
		context.Background(), []ontology.Assertion{assertion}, identity)
	if err != nil || len(read) != 1 || !read[0].Resolved() ||
		read[0].Predicate() != property.ID() {
		t.Fatalf("same-version interpretation = %#v, err = %v", read, err)
	}
}

func TestPublishedTransitionWithoutMorphismsRemainsReadable(t *testing.T) {
	corpus, err := Open(filepath.Join(t.TempDir(), "corpus"))
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	schema, _ := ontology.NewOntologySchema("additive", "Additive", "", nil)
	property, _ := ontology.NewPropertyDefinition(
		"name", "Name", "", ontology.ValueString, nil, nil)
	person, _ := ontology.NewConceptDefinition(
		"person", "Person", "", []shoal.ID{property.ID()}, nil)
	organization, _ := ontology.NewConceptDefinition(
		"organization", "Organization", "", nil, nil)
	at := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	base, _ := ontology.NewOntologyVersion(
		schema, "1", at, []ontology.ConceptDefinition{person}, nil,
		[]ontology.PropertyDefinition{property}, nil)
	target, _ := ontology.NewOntologyVersion(
		schema, "2", at.Add(time.Second),
		[]ontology.ConceptDefinition{person, organization}, nil,
		[]ontology.PropertyDefinition{property}, nil)
	proposal, err := ontology.NewGovernedProposal(
		schema, base, target, "author", "add organization",
		at.Add(2*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := corpus.CreateOntologyProposal(ctx, proposal, base); err != nil {
		t.Fatal(err)
	}
	for index, state := range []ontology.ProposalState{
		ontology.ProposalSubmitted, ontology.ProposalApproved, ontology.ProposalPublished,
	} {
		proposal, err = corpus.TransitionOntologyProposal(
			ctx, proposal.ID(), state, "governor", "approved",
			at.Add(time.Duration(index+3)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}
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
	baseIdentity, _ := ontology.NewOntologyIdentity(base)
	assertion, err := ontology.NewAssertion(
		"person-1", property.ID(), value, ontology.AssertionExplicit, 1,
		[]ontology.EvidenceRef{evidence}, provenance, nil,
		ontology.WithAssertionSubjectType(person.ID()),
		ontology.WithAssertionOntology(baseIdentity))
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity, _ := ontology.NewOntologyIdentity(target)
	read, err := corpus.InterpretAssertions(
		ctx, []ontology.Assertion{assertion}, targetIdentity)
	if err != nil || len(read) != 1 || !read[0].Resolved() ||
		len(read[0].AppliedMorphisms()) != 0 {
		t.Fatalf("additive transition read = %#v, err = %v", read, err)
	}
}
