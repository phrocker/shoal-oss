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

package ontology_test

import (
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

var _ interface{ Validate() error } = ontology.OntologyIdentity{}

// successorVersion returns a second version of the fixture schema whose
// definition keys are unchanged but whose meaning has moved: title becomes
// required. Definition IDs are derived from namespace and key alone, so this is
// the drift case -- the same concept and property IDs under a different
// meaning.
func successorVersion(t *testing.T, fixture ontologyFixture) ontology.OntologyVersion {
	t.Helper()
	required, err := ontology.NewFlagConstraint(ontology.ConstraintRequired)
	if err != nil {
		t.Fatal(err)
	}
	title := mustProperty(
		t, "title", "Title", "Canonical display title",
		ontology.ValueString, []ontology.Constraint{required}, nil,
	)
	version, err := ontology.NewOntologyVersion(
		fixture.schema, "3.0.0", testTime,
		[]ontology.ConceptDefinition{fixture.person, fixture.project},
		[]ontology.RelationshipDefinition{fixture.worksOn},
		[]ontology.PropertyDefinition{title, fixture.score},
		shoal.Metadata{"owner": "knowledge"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if title.ID() != fixture.title.ID() {
		t.Fatal("changing a definition's meaning changed its ID; the drift case no longer holds")
	}
	return version
}

func assertionUnder(
	t *testing.T,
	fixture ontologyFixture,
	options ...ontology.AssertionOption,
) ontology.Assertion {
	t.Helper()
	value, err := ontology.NewStringValue("subject")
	if err != nil {
		t.Fatal(err)
	}
	options = append(
		[]ontology.AssertionOption{
			ontology.WithAssertionSubjectType(fixture.person.ID()),
		},
		options...,
	)
	assertion, err := ontology.NewAssertion(
		"entity:person-1", fixture.title.ID(), value,
		ontology.AssertionExplicit, 0.9,
		[]ontology.EvidenceRef{fixture.evidence}, fixture.provenance,
		shoal.Metadata{"review": "pending"}, options...,
	)
	if err != nil {
		t.Fatal(err)
	}
	return assertion
}

func mustIdentity(t *testing.T, version ontology.OntologyVersion) ontology.OntologyIdentity {
	t.Helper()
	identity, err := ontology.NewOntologyIdentity(version)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

// TestAssertionsUnderDifferentOntologyVersionsAreDistinguishable is the
// prerequisite defect from issue #279: concept:Person is the same ID under
// every version, so without a recorded ontology identity two assertions made
// under incompatible meanings are indistinguishable.
func TestAssertionsUnderDifferentOntologyVersionsAreDistinguishable(t *testing.T) {
	fixture := newOntologyFixture(t)
	successor := successorVersion(t, fixture)

	if fixture.person.ID() != fixture.person.ID() ||
		successor.Concepts()[0].ID() != fixture.version.Concepts()[0].ID() {
		t.Fatal("concept keys are not stable across versions")
	}
	if successor.ID() == fixture.version.ID() {
		t.Fatal("two ontology versions with different definitions share an ID")
	}

	first := mustIdentity(t, fixture.version)
	second := mustIdentity(t, successor)
	if first == second {
		t.Fatal("identities for different versions compare equal")
	}
	if first.SchemaID() != second.SchemaID() {
		t.Fatal("versions of one schema reported different schema identities")
	}

	underFirst := assertionUnder(t, fixture, ontology.WithAssertionOntology(first))
	underSecond := assertionUnder(t, fixture, ontology.WithAssertionOntology(second))

	if underFirst.ID() == underSecond.ID() {
		t.Fatal("assertions made under different ontology versions share an identity")
	}
	recordedFirst, known := underFirst.Ontology()
	if !known || recordedFirst != first {
		t.Fatalf("assertion did not carry its ontology identity: %v, known=%v",
			recordedFirst, known)
	}
	recordedSecond, known := underSecond.Ontology()
	if !known || recordedSecond != second {
		t.Fatalf("assertion did not carry its ontology identity: %v, known=%v",
			recordedSecond, known)
	}
	if recordedFirst.VersionID() == recordedSecond.VersionID() {
		t.Fatal("assertions recorded the same version ID for different versions")
	}

	// Reading either assertion under the other's version is detectable.
	if reading := underFirst.ReadUnder(first); reading != ontology.OntologySameVersion {
		t.Fatalf("reading under its own version = %q, want same_version", reading)
	}
	if reading := underFirst.ReadUnder(second); reading != ontology.OntologyOtherVersion {
		t.Fatalf("reading under a different version = %q, want other_version", reading)
	}
	if reading := underSecond.ReadUnder(first); reading != ontology.OntologyOtherVersion {
		t.Fatalf("reading under a different version = %q, want other_version", reading)
	}
	if !underFirst.ReadUnder(second).Resolved() {
		t.Fatal("a version mismatch was not reported as resolved")
	}
}

func TestForeignSchemaIsDistinguishedFromAForeignVersion(t *testing.T) {
	fixture := newOntologyFixture(t)
	otherSchema, err := ontology.NewOntologySchema(
		"other-work", "Other work ontology", "A different schema", nil)
	if err != nil {
		t.Fatal(err)
	}
	otherVersion, err := ontology.NewOntologyVersion(
		otherSchema, "1.0.0", testTime,
		[]ontology.ConceptDefinition{fixture.person, fixture.project},
		[]ontology.RelationshipDefinition{fixture.worksOn},
		[]ontology.PropertyDefinition{fixture.title, fixture.score}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertion := assertionUnder(
		t, fixture, ontology.WithAssertionOntology(mustIdentity(t, fixture.version)))
	reading := assertion.ReadUnder(mustIdentity(t, otherVersion))
	if reading != ontology.OntologyOtherSchema {
		t.Fatalf("reading under another schema = %q, want other_schema", reading)
	}
}

// TestAssertionWithoutOntologyReportsUnknown pins the #274 rule: an assertion
// that recorded nothing must say so, and must not be read as having been made
// under whatever version is in hand.
func TestAssertionWithoutOntologyReportsUnknown(t *testing.T) {
	fixture := newOntologyFixture(t)
	successor := successorVersion(t, fixture)
	assertion := assertionUnder(t, fixture)

	recorded, known := assertion.Ontology()
	if known {
		t.Fatalf("assertion with no recorded ontology reported %v as known", recorded)
	}
	if recorded != ontology.UnknownOntology() {
		t.Fatal("an unrecorded ontology identity is not the unknown identity")
	}
	if recorded.SchemaID() != "" || recorded.VersionID() != "" {
		t.Fatalf("unknown identity carries fabricated IDs: %v", recorded)
	}
	if recorded.String() != "unknown" {
		t.Fatalf("unknown identity renders as %q", recorded.String())
	}

	// It adopts neither the version it could plausibly have been made under nor
	// any other, and it never reports agreement.
	for name, identity := range map[string]ontology.OntologyIdentity{
		"first":     mustIdentity(t, fixture.version),
		"successor": mustIdentity(t, successor),
		"unknown":   ontology.UnknownOntology(),
	} {
		reading := assertion.ReadUnder(identity)
		if reading != ontology.OntologyUnresolved {
			t.Fatalf("reading an unrecorded assertion under %s = %q, want unresolved",
				name, reading)
		}
		if reading.Resolved() {
			t.Fatalf("unresolved reading under %s reported itself resolved", name)
		}
	}

	// A reader that holds no ontology cannot resolve a recorded assertion
	// either; the unknown state is absorbing on both sides.
	recordedAssertion := assertionUnder(
		t, fixture, ontology.WithAssertionOntology(mustIdentity(t, fixture.version)))
	if reading := recordedAssertion.ReadUnder(
		ontology.UnknownOntology()); reading != ontology.OntologyUnresolved {
		t.Fatalf("reading under an unknown reader = %q, want unresolved", reading)
	}

	// An unresolved assertion is a different fact from the same assertion made
	// under a known version.
	if assertion.ID() == recordedAssertion.ID() {
		t.Fatal("an unresolved assertion shares an identity with a version-qualified one")
	}
}

func TestOntologyIdentityRejectsPartialAndSwappedIdentities(t *testing.T) {
	fixture := newOntologyFixture(t)
	identity := mustIdentity(t, fixture.version)

	if _, err := ontology.NewOntologyIdentityFromIDs(
		identity.VersionID(), identity.SchemaID()); err == nil {
		t.Fatal("swapped schema and version IDs were accepted")
	} else {
		assertInvalidArgument(t, err)
	}
	for name, ids := range map[string][2]shoal.ID{
		"schema only":  {identity.SchemaID(), ""},
		"version only": {"", identity.VersionID()},
	} {
		partial, err := ontology.NewOntologyIdentityFromIDs(ids[0], ids[1])
		if err == nil {
			t.Fatalf("%s identity was accepted: %v", name, partial)
		}
		assertInvalidArgument(t, err)
	}

	if ontology.UnknownOntology().Known() {
		t.Fatal("the unknown identity reported itself known")
	}
	if ontology.UnknownOntology().Validate() == nil {
		t.Fatal("the unknown identity validated as a resolved identity")
	}
	if !identity.Known() || identity.Validate() != nil {
		t.Fatal("a resolved identity did not validate")
	}
	if ontology.ReadOntologyUnder(identity, identity) != ontology.OntologySameVersion {
		t.Fatal("an identity did not match itself")
	}
}

func TestExtractionResultRejectsAssertionsFromAnotherVersion(t *testing.T) {
	fixture := newOntologyFixture(t)
	successor := successorVersion(t, fixture)
	request, err := ontology.NewExtractionRequest(
		fixture.version, []ontology.EvidenceRef{fixture.evidence},
		"Extract cited facts.", fixture.provenance,
		ontology.DefaultExtractionLimits(), nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	foreign := assertionUnder(
		t, fixture, ontology.WithAssertionOntology(mustIdentity(t, successor)))
	_, err = ontology.NewExtractionResult(
		request, []ontology.Assertion{foreign}, nil, testTime.Add(time.Minute), nil)
	assertInvalidArgument(t, err)

	matching := assertionUnder(
		t, fixture, ontology.WithAssertionOntology(mustIdentity(t, fixture.version)))
	result, err := ontology.NewExtractionResult(
		request, []ontology.Assertion{matching}, nil, testTime.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("result under the pinned version: %v", err)
	}
	if len(result.UnresolvedOntologyAssertions()) != 0 {
		t.Fatal("a version-qualified assertion was reported as unresolved")
	}
}

func TestExtractionResultReportsUnresolvedAssertionsWithoutStampingThem(t *testing.T) {
	fixture := newOntologyFixture(t)
	request, err := ontology.NewExtractionRequest(
		fixture.version, []ontology.EvidenceRef{fixture.evidence},
		"Extract cited facts.", fixture.provenance,
		ontology.DefaultExtractionLimits(), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	unresolved := assertionUnder(t, fixture)
	result, err := ontology.NewExtractionResult(
		request, []ontology.Assertion{unresolved}, nil, testTime.Add(time.Minute), nil)
	if err != nil {
		t.Fatalf("result with an unresolved assertion: %v", err)
	}
	reported := result.UnresolvedOntologyAssertions()
	if len(reported) != 1 || reported[0] != unresolved.ID() {
		t.Fatalf("unresolved assertions = %v, want [%s]", reported, unresolved.ID())
	}
	if recorded, known := result.Assertions()[0].Ontology(); known {
		t.Fatalf("the result stamped the request's version onto an assertion: %v", recorded)
	}
}
