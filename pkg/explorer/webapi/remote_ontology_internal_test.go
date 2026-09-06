// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package webapi

import (
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestValidateRemoteOntologyInterpretations(t *testing.T) {
	schema, _ := ontology.NewOntologySchema("remote", "Remote", "", nil)
	property, _ := ontology.NewPropertyDefinition(
		"name", "Name", "", ontology.ValueString, nil, nil)
	concept, _ := ontology.NewConceptDefinition(
		"person", "Person", "", []shoal.ID{property.ID()}, nil)
	at := time.Date(2026, time.September, 6, 0, 0, 0, 0, time.UTC)
	version, _ := ontology.NewOntologyVersion(
		schema, "1", at, []ontology.ConceptDefinition{concept}, nil,
		[]ontology.PropertyDefinition{property}, nil)
	identity, _ := ontology.NewOntologyIdentity(version)
	evidence, _ := ontology.NewEvidenceRef(document.Citation{
		DocumentID: "doc", RevisionID: "rev", SectionID: "section",
		Range: document.SourceRange{},
	}, "", nil)
	provenance, _ := ontology.NewExtractionProvenance(
		"provider", "model", "1", "prompt", "1", "extractor", "1", nil)
	value, _ := ontology.NewStringValue("Ada")
	assertion, err := ontology.NewAssertion(
		"person-1", property.ID(), value, ontology.AssertionExplicit, 1,
		[]ontology.EvidenceRef{evidence}, provenance, nil,
		ontology.WithAssertionSubjectType(concept.ID()),
		ontology.WithAssertionOntology(identity))
	if err != nil {
		t.Fatal(err)
	}
	valid := OntologyInterpretationReport{
		AssertionID: encodeID(assertion.ID()),
		SchemaID:    encodeID(identity.SchemaID()), VersionID: encodeID(identity.VersionID()),
		Reading: ontology.OntologySameVersion, Status: ontology.InterpretationResolved,
		OriginalSubjectType: encodeID(concept.ID()), SubjectType: encodeID(concept.ID()),
		OriginalPredicate: encodeID(property.ID()), Predicate: encodeID(property.ID()),
	}
	if err := validateRemoteOntologyInterpretations(
		[]ontology.Assertion{assertion}, []OntologyInterpretationReport{valid},
	); err != nil {
		t.Fatal(err)
	}
	for name, reports := range map[string][]OntologyInterpretationReport{
		"duplicate": {valid, valid},
		"absent": {{
			AssertionID: encodeID("assertion:absent"),
			SchemaID:    valid.SchemaID, VersionID: valid.VersionID,
			Reading: valid.Reading, Status: valid.Status,
			OriginalSubjectType: valid.OriginalSubjectType,
			SubjectType:         valid.SubjectType,
			OriginalPredicate:   valid.OriginalPredicate,
			Predicate:           valid.Predicate,
		}},
		"invalid-status": {{
			AssertionID: valid.AssertionID,
			SchemaID:    valid.SchemaID, VersionID: valid.VersionID,
			Reading: valid.Reading, Status: "forged",
			OriginalSubjectType: valid.OriginalSubjectType,
			SubjectType:         valid.SubjectType,
			OriginalPredicate:   valid.OriginalPredicate,
			Predicate:           valid.Predicate,
		}},
		"wrong-original": {{
			AssertionID: valid.AssertionID,
			SchemaID:    valid.SchemaID, VersionID: valid.VersionID,
			Reading: valid.Reading, Status: valid.Status,
			OriginalSubjectType: valid.OriginalSubjectType,
			SubjectType:         valid.SubjectType,
			OriginalPredicate:   encodeID("property:other"),
			Predicate:           valid.Predicate,
		}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateRemoteOntologyInterpretations(
				[]ontology.Assertion{assertion}, reports,
			); err == nil {
				t.Fatal("malformed remote interpretation was accepted")
			}
		})
	}
}
