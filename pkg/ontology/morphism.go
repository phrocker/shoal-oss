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

package ontology

import (
	"sort"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const MaxLensTransitions = 32

type MorphismKind string

const (
	MorphismWiden  MorphismKind = "widen"
	MorphismNarrow MorphismKind = "narrow"
	MorphismRename MorphismKind = "rename"
	MorphismSplit  MorphismKind = "split"
	MorphismMerge  MorphismKind = "merge"
)

type MorphismSafety string

const (
	MorphismSafeWidening          MorphismSafety = "safe_widening"
	MorphismUnsafeNarrowing       MorphismSafety = "unsafe_narrowing"
	MorphismUnsafeRename          MorphismSafety = "unsafe_rename"
	MorphismRequiresDiscriminator MorphismSafety = "requires_discriminator"
	MorphismLossyMerge            MorphismSafety = "lossy_merge"
)

type DiscriminatorChoice struct {
	value  string
	target shoal.ID
}

type MorphismDiscriminator struct {
	metadataKey string
	choices     []DiscriminatorChoice
}

func NewMorphismDiscriminator(
	metadataKey string, choices map[string]shoal.ID,
) (MorphismDiscriminator, error) {
	d := MorphismDiscriminator{metadataKey: metadataKey}
	for value, target := range choices {
		d.choices = append(d.choices, DiscriminatorChoice{value: value, target: target})
	}
	sort.Slice(d.choices, func(i, j int) bool { return d.choices[i].value < d.choices[j].value })
	if err := d.Validate(); err != nil {
		return MorphismDiscriminator{}, err
	}
	return d, nil
}

func (d MorphismDiscriminator) Validate() error {
	if !requiredWire(d.metadataKey) || len(d.choices) == 0 {
		return invalid("split discriminator key and choices are required")
	}
	for i, choice := range d.choices {
		if !requiredWire(choice.value) || ValidateID(choice.target) != nil {
			return invalid("split discriminator choice is invalid")
		}
		if i > 0 && d.choices[i-1].value >= choice.value {
			return invalid("split discriminator choices must be unique and ordered")
		}
	}
	return nil
}

func (d MorphismDiscriminator) MetadataKey() string { return d.metadataKey }
func (d MorphismDiscriminator) Choices() map[string]shoal.ID {
	out := make(map[string]shoal.ID, len(d.choices))
	for _, choice := range d.choices {
		out[choice.value] = choice.target
	}
	return out
}
func (d MorphismDiscriminator) resolve(metadata shoal.Metadata) (shoal.ID, bool) {
	value, ok := metadata[d.metadataKey]
	if !ok {
		return "", false
	}
	i := sort.Search(len(d.choices), func(i int) bool { return d.choices[i].value >= value })
	if i == len(d.choices) || d.choices[i].value != value {
		return "", false
	}
	return d.choices[i].target, true
}
func (d MorphismDiscriminator) canonical() string {
	parts := []string{d.metadataKey}
	for _, choice := range d.choices {
		parts = append(parts, choice.value, string(choice.target))
	}
	return canonicalParts(parts...)
}

type MorphismConfig struct {
	Kind          MorphismKind
	SourceVersion OntologyVersion
	TargetVersion OntologyVersion
	Sources       []shoal.ID
	Targets       []shoal.ID
	Discriminator MorphismDiscriminator
	Evidence      []EvidenceRef
	Rationale     string
	Metadata      shoal.Metadata
}

type OntologyMorphism struct {
	id            shoal.ID
	kind          MorphismKind
	safety        MorphismSafety
	source        OntologyIdentity
	target        OntologyIdentity
	sources       []shoal.ID
	targets       []shoal.ID
	discriminator MorphismDiscriminator
	evidence      []EvidenceRef
	rationale     string
	metadata      shoal.Metadata
}

func NewOntologyMorphism(config MorphismConfig) (OntologyMorphism, error) {
	if err := config.SourceVersion.Validate(); err != nil {
		return OntologyMorphism{}, err
	}
	if err := config.TargetVersion.Validate(); err != nil {
		return OntologyMorphism{}, err
	}
	source, _ := NewOntologyIdentity(config.SourceVersion)
	target, _ := NewOntologyIdentity(config.TargetVersion)
	m := OntologyMorphism{
		kind: config.Kind, safety: safetyForMorphism(config.Kind),
		source: source, target: target,
		sources: canonicalizeIDs(config.Sources), targets: canonicalizeIDs(config.Targets),
		discriminator: config.Discriminator, evidence: cloneEvidence(config.Evidence),
		rationale: config.Rationale, metadata: cloneMetadata(config.Metadata),
	}
	sort.Slice(m.evidence, func(i, j int) bool { return m.evidence[i].ID() < m.evidence[j].ID() })
	if err := validateMorphismSemantics(m, config.SourceVersion, config.TargetVersion); err != nil {
		return OntologyMorphism{}, err
	}
	id, err := morphismID(m)
	if err != nil {
		return OntologyMorphism{}, err
	}
	m.id = id
	if err := m.Validate(); err != nil {
		return OntologyMorphism{}, err
	}
	return m, nil
}

func safetyForMorphism(kind MorphismKind) MorphismSafety {
	switch kind {
	case MorphismWiden:
		return MorphismSafeWidening
	case MorphismNarrow:
		return MorphismUnsafeNarrowing
	case MorphismRename:
		return MorphismUnsafeRename
	case MorphismSplit:
		return MorphismRequiresDiscriminator
	case MorphismMerge:
		return MorphismLossyMerge
	default:
		return ""
	}
}

func (m OntologyMorphism) Validate() error {
	if err := validateTypedID(m.id, "morphism"); err != nil {
		return err
	}
	if m.safety == "" || m.safety != safetyForMorphism(m.kind) {
		return invalid("morphism safety does not match its kind")
	}
	if err := m.source.Validate(); err != nil {
		return err
	}
	if err := m.target.Validate(); err != nil {
		return err
	}
	if m.source.SchemaID() != m.target.SchemaID() ||
		m.source.VersionID() == m.target.VersionID() {
		return invalid("morphism must connect distinct versions of one schema")
	}
	if !requiredWire(m.rationale) || len(m.evidence) == 0 {
		return invalid("morphism rationale and evidence are required")
	}
	for i, evidence := range m.evidence {
		if err := evidence.Validate(); err != nil {
			return err
		}
		if _, derived := evidence.Derivation(); derived {
			return invalid("morphism evidence must cite an immutable source observation")
		}
		if i > 0 && m.evidence[i-1].ID() >= evidence.ID() {
			return invalid("morphism evidence must be unique and ordered")
		}
	}
	if err := validateMorphismIDs(m.sources, "morphism sources"); err != nil {
		return err
	}
	if err := validateMorphismIDs(m.targets, "morphism targets"); err != nil {
		return err
	}

	switch m.kind {
	case MorphismWiden, MorphismNarrow:
		if len(m.sources) != 1 || len(m.targets) != 1 || m.sources[0] != m.targets[0] ||
			IDNamespace(m.sources[0]) != "relationship" {
			return invalid("domain/range morphism requires one unchanged relationship identity")
		}
	case MorphismRename:
		if len(m.sources) != 1 || len(m.targets) != 1 ||
			m.sources[0] == m.targets[0] ||
			IDNamespace(m.sources[0]) != IDNamespace(m.targets[0]) {
			return invalid("rename requires one source and one distinct same-kind target")
		}
	case MorphismSplit:
		if len(m.sources) != 1 || len(m.targets) < 2 {
			return invalid("split requires one source and multiple targets")
		}
		if !sameDefinitionNamespace(m.sources, m.targets) {
			return invalid("split source and targets must have the same definition kind")
		}
		if err := m.discriminator.Validate(); err != nil {
			return err
		}
		covered := map[shoal.ID]bool{}
		for _, choice := range m.discriminator.choices {
			if !containsID(m.targets, choice.target) {
				return invalid("split discriminator points outside targets")
			}
			covered[choice.target] = true
		}
		if len(covered) != len(m.targets) {
			return invalid("split discriminator must cover every target")
		}
	case MorphismMerge:
		if len(m.sources) < 2 || len(m.targets) != 1 {
			return invalid("merge requires multiple sources and one target")
		}
		if !sameDefinitionNamespace(m.targets, m.sources) {
			return invalid("merge sources and target must have the same definition kind")
		}
	default:
		return invalid("unknown morphism kind")
	}

	for _, id := range append(append([]shoal.ID(nil), m.sources...), m.targets...) {
		if ValidateID(id) != nil {
			return invalid("morphism definition identity is invalid")
		}
	}
	if err := validateMetadata(m.metadata); err != nil {
		return err
	}
	expected, err := morphismID(m)
	if err != nil || expected != m.id {
		return invalid("morphism ID is not canonical")
	}
	return nil
}

func sameDefinitionNamespace(reference, values []shoal.ID) bool {
	if len(reference) == 0 {
		return false
	}
	namespace := IDNamespace(reference[0])
	if namespace != "concept" && namespace != "relationship" && namespace != "property" {
		return false
	}
	for _, id := range append(append([]shoal.ID(nil), reference...), values...) {
		if IDNamespace(id) != namespace {
			return false
		}
	}
	return true
}

func validateMorphismIDs(values []shoal.ID, name string) error {
	if len(values) == 0 {
		return invalid(name + " cannot be empty")
	}
	for i, id := range values {
		if err := ValidateID(id); err != nil {
			return err
		}
		if i > 0 && values[i-1] >= id {
			return invalid(name + " must be unique and canonically ordered")
		}
	}
	return nil
}

func validateMorphismSemantics(
	m OntologyMorphism, source, target OntologyVersion,
) error {
	if source.Schema().ID() != target.Schema().ID() {
		return invalid("morphism versions belong to different schemas")
	}
	for _, id := range m.sources {
		if !definitionExists(source, id) {
			return invalid("morphism source is absent from its source version")
		}
	}
	for _, id := range m.targets {
		if !definitionExists(target, id) {
			return invalid("morphism target is absent from its target version")
		}
	}
	if m.kind == MorphismWiden || m.kind == MorphismNarrow {
		before, _ := source.relationship(m.sources[0])
		after, _ := target.relationship(m.targets[0])
		if before.Directed() != after.Directed() {
			return invalid("domain/range morphism cannot change directionality")
		}
		beforeSubsetAfter := idSubset(before.FromConcepts(), after.FromConcepts()) &&
			idSubset(before.ToConcepts(), after.ToConcepts())
		afterSubsetBefore := idSubset(after.FromConcepts(), before.FromConcepts()) &&
			idSubset(after.ToConcepts(), before.ToConcepts())
		equal := beforeSubsetAfter && afterSubsetBefore
		if m.kind == MorphismWiden && (!beforeSubsetAfter || equal) {
			return invalid("widening must monotonically add relationship endpoints")
		}
		if m.kind == MorphismNarrow && (!afterSubsetBefore || equal) {
			return invalid("narrowing must remove relationship endpoints")
		}
	}
	return nil
}

func definitionExists(version OntologyVersion, id shoal.ID) bool {
	switch IDNamespace(id) {
	case "concept":
		_, ok := version.concept(id)
		return ok
	case "relationship":
		_, ok := version.relationship(id)
		return ok
	case "property":
		_, ok := version.property(id)
		return ok
	default:
		return false
	}
}

func idSubset(left, right []shoal.ID) bool {
	for _, id := range left {
		if !containsID(right, id) {
			return false
		}
	}
	return true
}

func (v OntologyVersion) concept(id shoal.ID) (ConceptDefinition, bool) {
	for _, concept := range v.concepts {
		if concept.ID() == id {
			return concept.clone(), true
		}
	}
	return ConceptDefinition{}, false
}

func morphismID(m OntologyMorphism) (shoal.ID, error) {
	evidence := make([]string, len(m.evidence))
	for i, item := range m.evidence {
		evidence[i] = string(item.ID())
	}
	return deriveID("morphism", string(m.kind), string(m.safety),
		m.source.canonical(), m.target.canonical(), canonicalIDs(m.sources),
		canonicalIDs(m.targets), m.discriminator.canonical(),
		canonicalParts(evidence...), m.rationale, canonicalMetadata(m.metadata))
}

func (m OntologyMorphism) ID() shoal.ID                         { return m.id }
func (m OntologyMorphism) Kind() MorphismKind                   { return m.kind }
func (m OntologyMorphism) Safety() MorphismSafety               { return m.safety }
func (m OntologyMorphism) Source() OntologyIdentity             { return m.source }
func (m OntologyMorphism) Target() OntologyIdentity             { return m.target }
func (m OntologyMorphism) Sources() []shoal.ID                  { return cloneIDs(m.sources) }
func (m OntologyMorphism) Targets() []shoal.ID                  { return cloneIDs(m.targets) }
func (m OntologyMorphism) Discriminator() MorphismDiscriminator { return m.discriminator }
func (m OntologyMorphism) Evidence() []EvidenceRef              { return cloneEvidence(m.evidence) }
func (m OntologyMorphism) Rationale() string                    { return m.rationale }
func (m OntologyMorphism) Metadata() shoal.Metadata             { return cloneMetadata(m.metadata) }
func (m OntologyMorphism) clone() OntologyMorphism {
	m.sources = cloneIDs(m.sources)
	m.targets = cloneIDs(m.targets)
	m.evidence = cloneEvidence(m.evidence)
	m.metadata = cloneMetadata(m.metadata)
	m.discriminator.choices = append([]DiscriminatorChoice(nil), m.discriminator.choices...)
	return m
}

type InterpretationStatus string

const (
	InterpretationResolved   InterpretationStatus = "resolved"
	InterpretationUnresolved InterpretationStatus = "unresolved"
)

type AssertionInterpretation struct {
	original    Assertion
	reader      OntologyIdentity
	reading     OntologyReading
	status      InterpretationStatus
	subjectType shoal.ID
	predicate   shoal.ID
	objectType  shoal.ID
	applied     []shoal.ID
	reason      string
}

func (i AssertionInterpretation) Original() Assertion          { return i.original.clone() }
func (i AssertionInterpretation) Reader() OntologyIdentity     { return i.reader }
func (i AssertionInterpretation) Reading() OntologyReading     { return i.reading }
func (i AssertionInterpretation) Status() InterpretationStatus { return i.status }
func (i AssertionInterpretation) Resolved() bool               { return i.status == InterpretationResolved }
func (i AssertionInterpretation) SubjectType() (shoal.ID, bool) {
	return i.subjectType, i.subjectType != ""
}
func (i AssertionInterpretation) Predicate() shoal.ID { return i.predicate }
func (i AssertionInterpretation) ObjectType() (shoal.ID, bool) {
	return i.objectType, i.objectType != ""
}
func (i AssertionInterpretation) AppliedMorphisms() []shoal.ID {
	return cloneIDs(i.applied)
}
func (i AssertionInterpretation) Reason() string { return i.reason }

func UnresolvedInterpretation(
	assertion Assertion, reader OntologyIdentity, reason string,
) AssertionInterpretation {
	subjectType, _ := assertion.SubjectType()
	objectType, _ := assertion.ObjectType()
	return AssertionInterpretation{
		original: assertion.clone(), reader: reader,
		reading: assertion.ReadUnder(reader), status: InterpretationUnresolved,
		subjectType: subjectType, predicate: assertion.Predicate(), objectType: objectType,
		reason: strings.TrimSpace(reason),
	}
}

// ReadAssertionUnder resolves the identity-only case without requiring schema
// material. Exact same-version assertions retain their original effective
// identifiers; every other comparison remains explicitly unresolved.
func ReadAssertionUnder(
	assertion Assertion, reader OntologyIdentity,
) AssertionInterpretation {
	if err := assertion.Validate(); err != nil {
		return UnresolvedInterpretation(assertion, reader, "assertion is malformed")
	}
	if !reader.Known() || reader.Validate() != nil {
		return UnresolvedInterpretation(assertion, reader, "selected ontology is unresolved")
	}
	if assertion.ReadUnder(reader) != OntologySameVersion {
		return UnresolvedInterpretation(
			assertion, reader, "ontology schema material is unavailable for reinterpretation")
	}
	subjectType, _ := assertion.SubjectType()
	objectType, _ := assertion.ObjectType()
	return AssertionInterpretation{
		original: assertion.clone(), reader: reader,
		reading: OntologySameVersion, status: InterpretationResolved,
		subjectType: subjectType, predicate: assertion.Predicate(), objectType: objectType,
	}
}

type OntologyLens struct {
	target    OntologyVersion
	identity  OntologyIdentity
	morphisms []OntologyMorphism
}

func NewOntologyLens(
	target OntologyVersion, morphisms []OntologyMorphism,
) (OntologyLens, error) {
	if err := target.Validate(); err != nil {
		return OntologyLens{}, err
	}
	identity, _ := NewOntologyIdentity(target)
	lens := OntologyLens{target: target.clone(), identity: identity}
	for _, morphism := range morphisms {
		if err := morphism.Validate(); err != nil {
			return OntologyLens{}, err
		}
		if morphism.Source().SchemaID() != identity.SchemaID() {
			continue
		}
		lens.morphisms = append(lens.morphisms, morphism.clone())
	}
	sort.Slice(lens.morphisms, func(i, j int) bool {
		return lens.morphisms[i].ID() < lens.morphisms[j].ID()
	})
	return lens, nil
}

func (l OntologyLens) Identity() OntologyIdentity { return l.identity }

func (l OntologyLens) Read(assertion Assertion) AssertionInterpretation {
	if err := assertion.Validate(); err != nil {
		return UnresolvedInterpretation(assertion, l.identity, "assertion is malformed")
	}
	reading := assertion.ReadUnder(l.identity)
	if reading == OntologyUnresolved || reading == OntologyMalformed ||
		reading == OntologyOtherSchema {
		return UnresolvedInterpretation(assertion, l.identity, string(reading))
	}
	if reading == OntologySameVersion {
		return ReadAssertionUnder(assertion, l.identity)
	}
	subjectType, _ := assertion.SubjectType()
	objectType, _ := assertion.ObjectType()
	result := AssertionInterpretation{
		original: assertion.clone(), reader: l.identity, reading: reading,
		status: InterpretationResolved, subjectType: subjectType,
		predicate: assertion.Predicate(), objectType: objectType,
	}
	path, ok := l.uniquePath(assertion.ontologyIdentity)
	if !ok {
		return UnresolvedInterpretation(assertion, l.identity, "no unique published morphism path")
	}
	for _, step := range path {
		var reason string
		result.subjectType, reason = mapDefinition(result.subjectType, assertion.metadata, step)
		if reason != "" {
			return UnresolvedInterpretation(assertion, l.identity, reason)
		}
		result.predicate, reason = mapDefinition(result.predicate, assertion.metadata, step)
		if reason != "" {
			return UnresolvedInterpretation(assertion, l.identity, reason)
		}
		result.objectType, reason = mapDefinition(result.objectType, assertion.metadata, step)
		if reason != "" {
			return UnresolvedInterpretation(assertion, l.identity, reason)
		}
		for _, morphism := range step {
			result.applied = append(result.applied, morphism.ID())
		}
	}
	if err := l.validateInterpretation(assertion, result); err != nil {
		return UnresolvedInterpretation(assertion, l.identity, err.Error())
	}
	return result
}

func (l OntologyLens) uniquePath(from OntologyIdentity) ([][]OntologyMorphism, bool) {
	type edge struct {
		to OntologyIdentity
		ms []OntologyMorphism
	}
	grouped := map[string]*edge{}
	for _, m := range l.morphisms {
		key := m.Source().String() + "->" + m.Target().String()
		if grouped[key] == nil {
			grouped[key] = &edge{to: m.Target()}
		}
		grouped[key].ms = append(grouped[key].ms, m)
	}
	adj := map[string][]edge{}
	for _, m := range l.morphisms {
		key := m.Source().String() + "->" + m.Target().String()
		e := grouped[key]
		if len(adj[m.Source().String()]) > 0 {
			found := false
			for _, existing := range adj[m.Source().String()] {
				if existing.to == e.to {
					found = true
				}
			}
			if found {
				continue
			}
		}
		adj[m.Source().String()] = append(adj[m.Source().String()], *e)
	}
	type state struct {
		at   OntologyIdentity
		path [][]OntologyMorphism
	}
	queue := []state{{at: from}}
	var found [][][]OntologyMorphism
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if len(current.path) > MaxLensTransitions {
			continue
		}
		if current.at == l.identity {
			found = append(found, current.path)
			if len(found) > 1 {
				return nil, false
			}
			continue
		}
		for _, next := range adj[current.at.String()] {
			seen := false
			for _, step := range current.path {
				if len(step) > 0 && step[0].Source() == next.to {
					seen = true
				}
			}
			if seen {
				continue
			}
			path := append(append([][]OntologyMorphism(nil), current.path...), next.ms)
			queue = append(queue, state{at: next.to, path: path})
		}
	}
	if len(found) != 1 {
		return nil, false
	}
	return found[0], true
}

func mapDefinition(
	id shoal.ID, metadata shoal.Metadata, morphisms []OntologyMorphism,
) (shoal.ID, string) {
	if id == "" {
		return "", ""
	}
	var mapped shoal.ID
	for _, m := range morphisms {
		if !containsID(m.sources, id) {
			continue
		}
		next := id
		switch m.kind {
		case MorphismRename, MorphismMerge:
			next = m.targets[0]
		case MorphismSplit:
			var ok bool
			next, ok = m.discriminator.resolve(metadata)
			if !ok {
				return "", "split discriminator is absent or unrecognized"
			}
		case MorphismWiden, MorphismNarrow:
		}
		if mapped != "" && mapped != next {
			return "", "multiple morphisms give incompatible meanings"
		}
		mapped = next
	}
	if mapped == "" {
		return id, ""
	}
	return mapped, ""
}

func (l OntologyLens) validateInterpretation(
	assertion Assertion, interpretation AssertionInterpretation,
) error {
	switch IDNamespace(interpretation.predicate) {
	case "relationship":
		relationship, ok := l.target.relationship(interpretation.predicate)
		if !ok || assertion.Object().Type() != ValueReference ||
			interpretation.subjectType == "" || interpretation.objectType == "" {
			return invalid("relationship is not resolvable in selected ontology")
		}
		forward := containsID(relationship.FromConcepts(), interpretation.subjectType) &&
			containsID(relationship.ToConcepts(), interpretation.objectType)
		reverse := !relationship.Directed() &&
			containsID(relationship.ToConcepts(), interpretation.subjectType) &&
			containsID(relationship.FromConcepts(), interpretation.objectType)
		if !forward && !reverse {
			return invalid("relationship endpoints are incompatible with selected ontology")
		}
	case "property":
		property, ok := l.target.property(interpretation.predicate)
		if !ok || validatePropertyValue(property, assertion.Object(), nil) != nil {
			return invalid("property is incompatible with selected ontology")
		}
		if !definitionExists(l.target, interpretation.subjectType) {
			return invalid("property subject type is absent from selected ontology")
		}
		owners := make([]shoal.ID, 0)
		for _, concept := range l.target.concepts {
			if containsID(concept.Properties(), interpretation.predicate) {
				owners = append(owners, concept.ID())
			}
		}
		for _, relationship := range l.target.relationships {
			if containsID(relationship.Properties(), interpretation.predicate) {
				owners = append(owners, relationship.ID())
			}
		}
		if len(owners) > 0 && !containsID(canonicalizeIDs(owners), interpretation.subjectType) {
			return invalid("property does not apply to the selected subject type")
		}
	default:
		return invalid("predicate is absent from selected ontology")
	}
	return nil
}
