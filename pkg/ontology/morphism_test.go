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

package ontology

import (
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestOntologyLensSafetySemanticsAndImmutableOriginals(t *testing.T) {
	f := newMorphismFixture(t)
	widen := mustMorphism(t, MorphismConfig{
		Kind: MorphismWiden, SourceVersion: f.v1, TargetVersion: f.v2,
		Sources: []shoal.ID{f.v1rel.ID()}, Targets: []shoal.ID{f.v2rel.ID()},
		Evidence: []EvidenceRef{f.evidence}, Rationale: "support contractors",
	})
	if widen.Safety() != MorphismSafeWidening {
		t.Fatalf("widen safety = %q", widen.Safety())
	}
	lens, err := NewOntologyLens(f.v2, []OntologyMorphism{widen})
	if err != nil {
		t.Fatal(err)
	}
	read := lens.Read(f.personAssertion)
	if !read.Resolved() || read.Predicate() != f.v2rel.ID() {
		t.Fatalf("widening read = %#v", read)
	}

	narrow := mustMorphism(t, MorphismConfig{
		Kind: MorphismNarrow, SourceVersion: f.v2, TargetVersion: f.v1,
		Sources: []shoal.ID{f.v2rel.ID()}, Targets: []shoal.ID{f.v1rel.ID()},
		Evidence: []EvidenceRef{f.evidence}, Rationale: "remove contractors",
	})
	narrowLens, err := NewOntologyLens(f.v1, []OntologyMorphism{narrow})
	if err != nil {
		t.Fatal(err)
	}
	read = narrowLens.Read(f.contractorAssertion)
	if read.Resolved() || read.Status() != InterpretationUnresolved {
		t.Fatal("unsafe narrowing silently retained an incompatible assertion")
	}
	if originalType, _ := read.Original().SubjectType(); originalType != f.contractor.ID() {
		t.Fatal("narrowing rewrote the historic assertion")
	}
}

func TestOntologyLensRenameSplitMergeAndUnknown(t *testing.T) {
	f := newMorphismFixture(t)
	rename := mustMorphism(t, MorphismConfig{
		Kind: MorphismRename, SourceVersion: f.renameFrom, TargetVersion: f.renameTo,
		Sources: []shoal.ID{f.oldRel.ID()}, Targets: []shoal.ID{f.newRel.ID()},
		Evidence: []EvidenceRef{f.evidence}, Rationale: "terminology change",
	})
	lens, _ := NewOntologyLens(f.renameTo, []OntologyMorphism{rename})
	read := lens.Read(f.renameAssertion)
	if !read.Resolved() || read.Predicate() != f.newRel.ID() {
		t.Fatal("explicit rename mapping was not applied")
	}

	if read.Original().Predicate() != f.oldRel.ID() {
		t.Fatal("rename mutated the original assertion")
	}

	discriminator, err := NewMorphismDiscriminator(
		"kind", map[string]shoal.ID{"person": f.person.ID(), "org": f.org.ID()})
	if err != nil {
		t.Fatal(err)
	}
	split := mustMorphism(t, MorphismConfig{
		Kind: MorphismSplit, SourceVersion: f.splitFrom, TargetVersion: f.splitTo,
		Sources: []shoal.ID{f.party.ID()}, Targets: []shoal.ID{f.person.ID(), f.org.ID()},
		Discriminator: discriminator, Evidence: []EvidenceRef{f.evidence},
		Rationale: "separate parties",
	})
	splitLens, _ := NewOntologyLens(f.splitTo, []OntologyMorphism{split})
	if got := splitLens.Read(f.splitAssertion); !got.Resolved() {
		t.Fatalf("discriminated split unresolved: %s", got.Reason())
	}
	ambiguous := mustPropertyAssertion(
		t, f.party.ID(), f.name.ID(), f.splitFrom, nil, f.evidence, f.provenance)
	if got := splitLens.Read(ambiguous); got.Resolved() {
		t.Fatal("split without a discriminator guessed a target")
	}

	merge := mustMorphism(t, MorphismConfig{
		Kind: MorphismMerge, SourceVersion: f.splitTo, TargetVersion: f.splitFrom,
		Sources: []shoal.ID{f.person.ID(), f.org.ID()}, Targets: []shoal.ID{f.party.ID()},
		Evidence: []EvidenceRef{f.evidence}, Rationale: "unified party view",
	})
	mergeLens, _ := NewOntologyLens(f.splitFrom, []OntologyMorphism{merge})
	merged := mergeLens.Read(mustPropertyAssertion(
		t, f.person.ID(), f.name.ID(), f.splitTo, nil, f.evidence, f.provenance))
	if !merged.Resolved() {
		t.Fatalf("merge unresolved: %s", merged.Reason())
	}
	if original, _ := merged.Original().SubjectType(); original != f.person.ID() {
		t.Fatal("lossy merge did not retain the original type")
	}
	if effective, _ := merged.SubjectType(); effective != f.party.ID() {
		t.Fatal("merge did not expose the selected target type")
	}

	unknown := mustPropertyAssertion(
		t, f.party.ID(), f.name.ID(), OntologyVersion{}, nil, f.evidence, f.provenance)
	if got := mergeLens.Read(unknown); got.Resolved() || got.Reading() != OntologyUnresolved {
		t.Fatal("unknown ontology was silently reinterpreted")
	}
}

func TestOntologyMorphismDefensivelyOwnsSafetyInputs(t *testing.T) {
	f := newMorphismFixture(t)
	sources := []shoal.ID{f.oldRel.ID()}
	targets := []shoal.ID{f.newRel.ID()}
	metadata := shoal.Metadata{"review": "approved"}
	morphism := mustMorphism(t, MorphismConfig{
		Kind: MorphismRename, SourceVersion: f.renameFrom, TargetVersion: f.renameTo,
		Sources: sources, Targets: targets, Evidence: []EvidenceRef{f.evidence},
		Rationale: "rename safely tracked", Metadata: metadata,
	})
	sources[0] = f.person.ID()
	targets[0] = f.org.ID()
	metadata["review"] = "mutated"
	returnedSources := morphism.Sources()
	returnedSources[0] = f.person.ID()
	returnedMetadata := morphism.Metadata()
	returnedMetadata["review"] = "mutated-again"
	if err := morphism.Validate(); err != nil {
		t.Fatalf("caller mutation changed morphism: %v", err)
	}
	if morphism.Sources()[0] != f.oldRel.ID() ||
		morphism.Metadata()["review"] != "approved" {
		t.Fatal("morphism retained caller-owned mutable data")
	}
}

type morphismFixture struct {
	v1, v2, renameFrom, renameTo, splitFrom, splitTo                      OntologyVersion
	v1rel, v2rel, oldRel, newRel                                          RelationshipDefinition
	person, contractor, org, party                                        ConceptDefinition
	name                                                                  PropertyDefinition
	evidence                                                              EvidenceRef
	provenance                                                            ExtractionProvenance
	personAssertion, contractorAssertion, renameAssertion, splitAssertion Assertion
}

func newMorphismFixture(t *testing.T) morphismFixture {
	t.Helper()
	schema, _ := NewOntologySchema("fleet", "Fleet", "", nil)
	name, _ := NewPropertyDefinition("name", "Name", "", ValueString, nil, nil)
	person, _ := NewConceptDefinition("person", "Person", "", []shoal.ID{name.ID()}, nil)
	contractor, _ := NewConceptDefinition("contractor", "Contractor", "", nil, nil)
	org, _ := NewConceptDefinition("org", "Organization", "", []shoal.ID{name.ID()}, nil)
	party, _ := NewConceptDefinition("party", "Party", "", []shoal.ID{name.ID()}, nil)
	v1rel, _ := NewRelationshipDefinition(
		"works-for", "Works For", "", []shoal.ID{person.ID()},
		[]shoal.ID{org.ID()}, nil, true, nil)
	v2rel, _ := NewRelationshipDefinition(
		"works-for", "Works For", "", []shoal.ID{person.ID(), contractor.ID()},
		[]shoal.ID{org.ID()}, nil, true, nil)
	oldRel, _ := NewRelationshipDefinition(
		"employed-by", "Employed By", "", []shoal.ID{person.ID()},
		[]shoal.ID{org.ID()}, nil, true, nil)
	newRel, _ := NewRelationshipDefinition(
		"works-at", "Works At", "", []shoal.ID{person.ID()},
		[]shoal.ID{org.ID()}, nil, true, nil)
	at := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	version := func(label string, concepts []ConceptDefinition, rels []RelationshipDefinition) OntologyVersion {
		v, err := NewOntologyVersion(schema, label, at, concepts, rels, []PropertyDefinition{name}, nil)
		if err != nil {
			t.Fatal(err)
		}
		at = at.Add(time.Second)
		return v
	}
	f := morphismFixture{
		v1:         version("1", []ConceptDefinition{person, contractor, org}, []RelationshipDefinition{v1rel}),
		v2:         version("2", []ConceptDefinition{person, contractor, org}, []RelationshipDefinition{v2rel}),
		renameFrom: version("rename-1", []ConceptDefinition{person, org}, []RelationshipDefinition{oldRel}),
		renameTo:   version("rename-2", []ConceptDefinition{person, org}, []RelationshipDefinition{newRel}),
		splitFrom:  version("split-1", []ConceptDefinition{party}, nil),
		splitTo:    version("split-2", []ConceptDefinition{person, org}, nil),
		v1rel:      v1rel, v2rel: v2rel, oldRel: oldRel, newRel: newRel,
		person: person, contractor: contractor, org: org, party: party, name: name,
	}
	citation := document.Citation{
		DocumentID: "doc", RevisionID: "rev", SectionID: "section", SpanID: "span",
		Range: document.SourceRange{
			Start: document.SourcePosition{Offset: 0, Page: 1},
			End:   document.SourcePosition{Offset: 4, Page: 1},
		},
	}
	f.evidence, _ = NewEvidenceRef(citation, "fact", nil)
	f.provenance, _ = NewExtractionProvenance(
		"provider", "model", "1", "prompt", "1", "extractor", "1", nil)
	f.personAssertion = mustRelationshipAssertion(
		t, person.ID(), v1rel.ID(), org.ID(), f.v1, f.evidence, f.provenance)
	f.contractorAssertion = mustRelationshipAssertion(
		t, contractor.ID(), v2rel.ID(), org.ID(), f.v2, f.evidence, f.provenance)
	f.renameAssertion = mustRelationshipAssertion(
		t, person.ID(), oldRel.ID(), org.ID(), f.renameFrom, f.evidence, f.provenance)
	f.splitAssertion = mustPropertyAssertion(
		t, party.ID(), name.ID(), f.splitFrom,
		shoal.Metadata{"kind": "person"}, f.evidence, f.provenance)
	return f
}

func mustMorphism(t *testing.T, config MorphismConfig) OntologyMorphism {
	t.Helper()
	m, err := NewOntologyMorphism(config)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func mustRelationshipAssertion(
	t *testing.T, subjectType, predicate, objectType shoal.ID,
	version OntologyVersion, evidence EvidenceRef, provenance ExtractionProvenance,
) Assertion {
	t.Helper()
	value, _ := NewReferenceValue("object")
	identity, _ := NewOntologyIdentity(version)
	assertion, err := NewAssertion(
		"subject", predicate, value, AssertionExplicit, 1,
		[]EvidenceRef{evidence}, provenance, nil,
		WithAssertionSubjectType(subjectType), WithAssertionObjectType(objectType),
		WithAssertionOntology(identity))
	if err != nil {
		t.Fatal(err)
	}
	return assertion
}

func mustPropertyAssertion(
	t *testing.T, subjectType, predicate shoal.ID, version OntologyVersion,
	metadata shoal.Metadata, evidence EvidenceRef, provenance ExtractionProvenance,
) Assertion {
	t.Helper()
	value, _ := NewStringValue("Ada")
	options := []AssertionOption{WithAssertionSubjectType(subjectType)}
	if version.ID() != "" {
		identity, _ := NewOntologyIdentity(version)
		options = append(options, WithAssertionOntology(identity))
	}
	assertion, err := NewAssertion(
		"subject", predicate, value, AssertionExplicit, 1,
		[]EvidenceRef{evidence}, provenance, metadata, options...)
	if err != nil {
		t.Fatal(err)
	}
	return assertion
}
