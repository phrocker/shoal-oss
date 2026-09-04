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
	"reflect"

	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const MaxOntologyBlastRadiusItems uint32 = MaxOntologyConcepts + MaxOntologyRelationships + MaxOntologyProperties

// OntologyProposalBlastRadiusProvider is the optional read-only service
// extension behind GET /api/v1/ontology/proposals/{id}/blast-radius.
type OntologyProposalBlastRadiusProvider interface {
	OntologyProposalBlastRadius(context.Context, shoal.ID) (OntologyBlastRadiusReport, error)
}

type ontologyAssertionImpactCounter interface {
	OntologyAssertionImpactCounts(
		context.Context,
		[]shoal.ID,
	) (map[shoal.ID]OntologyAssertionImpactProjection, error)
}

type OntologyBlastRadiusResponse struct {
	BlastRadius OntologyBlastRadiusReport `json:"blast_radius"`
}

type OntologyBlastRadiusReport struct {
	ProposalID           string                             `json:"proposal_id"`
	ActiveVersionID      string                             `json:"active_version_id"`
	BaseVersionID        string                             `json:"base_version_id,omitempty"`
	ProposedVersionID    string                             `json:"proposed_version_id"`
	Summary              OntologyBlastRadiusSummary         `json:"summary"`
	RemovedConcepts      []OntologyConceptImpactProjection  `json:"removed_concepts"`
	RemovedRelationships []OntologyRelationImpactProjection `json:"removed_relationships"`
	RemovedProperties    []OntologyPropertyImpactProjection `json:"removed_properties"`
	ChangedConcepts      []OntologyConceptChangeProjection  `json:"changed_concepts"`
	ChangedRelationships []OntologyRelationChangeProjection `json:"changed_relationships"`
	ChangedProperties    []OntologyPropertyChangeProjection `json:"changed_properties"`
	AddedConcepts        []OntologyConceptProjection        `json:"added_concepts"`
	AddedRelationships   []OntologyRelationProjection       `json:"added_relationships"`
	AddedProperties      []OntologyPropertyProjection       `json:"added_properties"`
	Limits               OntologyBlastRadiusLimits          `json:"limits"`
}

type OntologyBlastRadiusSummary struct {
	RemovedConcepts      uint32 `json:"removed_concepts"`
	RemovedRelationships uint32 `json:"removed_relationships"`
	RemovedProperties    uint32 `json:"removed_properties"`
	ChangedConcepts      uint32 `json:"changed_concepts"`
	ChangedRelationships uint32 `json:"changed_relationships"`
	ChangedProperties    uint32 `json:"changed_properties"`
	AddedConcepts        uint32 `json:"added_concepts"`
	AddedRelationships   uint32 `json:"added_relationships"`
	AddedProperties      uint32 `json:"added_properties"`
	DestructiveChanges   uint32 `json:"destructive_changes"`
	AdditiveChanges      uint32 `json:"additive_changes"`
	CountsComputed       bool   `json:"counts_computed"`
}

type OntologyBlastRadiusLimits struct {
	MaxItems                   uint32 `json:"max_items"`
	MaxConcepts                uint32 `json:"max_concepts"`
	MaxRelationships           uint32 `json:"max_relationships"`
	MaxProperties              uint32 `json:"max_properties"`
	MaxDefinitionProperties    uint32 `json:"max_definition_properties"`
	MaxRelationshipEndpointSet uint32 `json:"max_relationship_endpoint_sets"`
}

type OntologyAssertionImpactProjection struct {
	Computed      bool    `json:"computed"`
	AssertedCount *uint64 `json:"asserted_count,omitempty"`
	DerivedCount  *uint64 `json:"derived_count,omitempty"`
}

type OntologyConceptImpactProjection struct {
	Concept OntologyConceptProjection         `json:"concept"`
	Impact  OntologyAssertionImpactProjection `json:"impact"`
}

type OntologyRelationImpactProjection struct {
	Relationship OntologyRelationProjection        `json:"relationship"`
	Impact       OntologyAssertionImpactProjection `json:"impact"`
}

type OntologyPropertyImpactProjection struct {
	Property OntologyPropertyProjection        `json:"property"`
	Impact   OntologyAssertionImpactProjection `json:"impact"`
}

type OntologyConceptChangeProjection struct {
	Before OntologyConceptProjection         `json:"before"`
	After  OntologyConceptProjection         `json:"after"`
	Fields []string                          `json:"fields"`
	Impact OntologyAssertionImpactProjection `json:"impact"`
}

type OntologyRelationChangeProjection struct {
	Before OntologyRelationProjection        `json:"before"`
	After  OntologyRelationProjection        `json:"after"`
	Fields []string                          `json:"fields"`
	Impact OntologyAssertionImpactProjection `json:"impact"`
}

type OntologyPropertyChangeProjection struct {
	Before OntologyPropertyProjection        `json:"before"`
	After  OntologyPropertyProjection        `json:"after"`
	Fields []string                          `json:"fields"`
	Impact OntologyAssertionImpactProjection `json:"impact"`
}

func (s *EmbeddedService) OntologyProposalBlastRadius(
	ctx context.Context,
	proposalID shoal.ID,
) (OntologyBlastRadiusReport, error) {
	if err := shoal.ValidateRequiredID("ontology proposal ID", proposalID); err != nil {
		return OntologyBlastRadiusReport{}, err
	}
	active, configured, err := s.ActiveOntology(ctx)
	if err != nil {
		return OntologyBlastRadiusReport{}, err
	}
	if !configured {
		return OntologyBlastRadiusReport{}, shoal.NewError(
			shoal.ErrorUnavailable, "an active ontology is required to compute blast radius")
	}
	// This active-side bound check is load-bearing; TestOntologyProposalBlastRadiusRejectsOversizedActiveOntology
	// pins that blast-radius reports error rather than silently emitting an
	// over-limit active schema diff.
	if err := enforceOntologyBounds(active); err != nil {
		return OntologyBlastRadiusReport{}, err
	}
	proposals, err := s.OntologyProposals(ctx)
	if err != nil {
		return OntologyBlastRadiusReport{}, err
	}
	for _, proposal := range proposals {
		if proposal.ID() != proposalID {
			continue
		}
		counts, err := ontologyImpactCounts(ctx, s.client, active, proposal.ProposedVersion())
		if err != nil {
			return OntologyBlastRadiusReport{}, err
		}
		return computeOntologyBlastRadius(active, proposal, counts)
	}
	return OntologyBlastRadiusReport{}, shoal.NewError(
		shoal.ErrorNotFound, "ontology proposal not found")
}

func ontologyProposalBlastRadiusFor(
	ctx context.Context,
	service Service,
	proposalID shoal.ID,
) (OntologyBlastRadiusResponse, error) {
	provider, ok := service.(OntologyProposalBlastRadiusProvider)
	if !ok {
		return OntologyBlastRadiusResponse{}, shoal.NewError(
			shoal.ErrorUnavailable, "workspace capability \"ontology proposal blast radius\" is unavailable")
	}
	report, err := provider.OntologyProposalBlastRadius(ctx, proposalID)
	if err != nil {
		return OntologyBlastRadiusResponse{}, err
	}
	return OntologyBlastRadiusResponse{BlastRadius: report}, nil
}

func ontologyImpactCounts(
	ctx context.Context,
	client any,
	active ontology.OntologyVersion,
	proposed ontology.OntologyVersion,
) (map[shoal.ID]OntologyAssertionImpactProjection, error) {
	counter, ok := client.(ontologyAssertionImpactCounter)
	if !ok {
		return nil, nil
	}
	ids := changedOntologyElementIDs(active, proposed)
	return counter.OntologyAssertionImpactCounts(ctx, ids)
}

func computeOntologyBlastRadius(
	active ontology.OntologyVersion,
	proposal ontology.GovernedProposal,
	counts map[shoal.ID]OntologyAssertionImpactProjection,
) (OntologyBlastRadiusReport, error) {
	if err := proposal.Validate(); err != nil {
		return OntologyBlastRadiusReport{}, err
	}
	proposed := proposal.ProposedVersion()
	if err := enforceOntologyBounds(proposed); err != nil {
		return OntologyBlastRadiusReport{}, err
	}
	report := OntologyBlastRadiusReport{
		ProposalID:           encodeID(proposal.ID()),
		ActiveVersionID:      encodeID(active.ID()),
		ProposedVersionID:    encodeID(proposed.ID()),
		RemovedConcepts:      []OntologyConceptImpactProjection{},
		RemovedRelationships: []OntologyRelationImpactProjection{},
		RemovedProperties:    []OntologyPropertyImpactProjection{},
		ChangedConcepts:      []OntologyConceptChangeProjection{},
		ChangedRelationships: []OntologyRelationChangeProjection{},
		ChangedProperties:    []OntologyPropertyChangeProjection{},
		AddedConcepts:        []OntologyConceptProjection{},
		AddedRelationships:   []OntologyRelationProjection{},
		AddedProperties:      []OntologyPropertyProjection{},
		Limits:               ontologyBlastRadiusLimits(),
	}
	if baseVersionID, ok := proposal.BaseVersionID(); ok {
		report.BaseVersionID = encodeID(baseVersionID)
	}
	activeConcepts := conceptsByID(active)
	activeRelationships := relationshipsByID(active)
	activeProperties := propertiesByID(active)
	proposedConcepts := conceptsByID(proposed)
	proposedRelationships := relationshipsByID(proposed)
	proposedProperties := propertiesByID(proposed)

	for _, concept := range active.Concepts() {
		after, exists := proposedConcepts[concept.ID()]
		if !exists {
			projected, err := projectConceptDefinition(concept)
			if err != nil {
				return OntologyBlastRadiusReport{}, err
			}
			report.RemovedConcepts = append(report.RemovedConcepts, OntologyConceptImpactProjection{
				Concept: projected, Impact: impactFor(counts, concept.ID()),
			})
			continue
		}
		fields := changedConceptFields(concept, after)
		if len(fields) == 0 {
			continue
		}
		beforeProjected, err := projectConceptDefinition(concept)
		if err != nil {
			return OntologyBlastRadiusReport{}, err
		}
		afterProjected, err := projectConceptDefinition(after)
		if err != nil {
			return OntologyBlastRadiusReport{}, err
		}
		report.ChangedConcepts = append(report.ChangedConcepts, OntologyConceptChangeProjection{
			Before: beforeProjected, After: afterProjected, Fields: fields,
			Impact: impactFor(counts, concept.ID()),
		})
	}
	for _, relationship := range active.Relationships() {
		after, exists := proposedRelationships[relationship.ID()]
		if !exists {
			projected, err := projectRelationshipDefinition(relationship)
			if err != nil {
				return OntologyBlastRadiusReport{}, err
			}
			report.RemovedRelationships = append(
				report.RemovedRelationships,
				OntologyRelationImpactProjection{
					Relationship: projected, Impact: impactFor(counts, relationship.ID()),
				},
			)
			continue
		}
		fields := changedRelationshipFields(relationship, after)
		if len(fields) == 0 {
			continue
		}
		beforeProjected, err := projectRelationshipDefinition(relationship)
		if err != nil {
			return OntologyBlastRadiusReport{}, err
		}
		afterProjected, err := projectRelationshipDefinition(after)
		if err != nil {
			return OntologyBlastRadiusReport{}, err
		}
		report.ChangedRelationships = append(report.ChangedRelationships, OntologyRelationChangeProjection{
			Before: beforeProjected, After: afterProjected, Fields: fields,
			Impact: impactFor(counts, relationship.ID()),
		})
	}
	for _, property := range active.Properties() {
		after, exists := proposedProperties[property.ID()]
		if !exists {
			projected, err := projectPropertyDefinition(property)
			if err != nil {
				return OntologyBlastRadiusReport{}, err
			}
			report.RemovedProperties = append(report.RemovedProperties, OntologyPropertyImpactProjection{
				Property: projected, Impact: impactFor(counts, property.ID()),
			})
			continue
		}
		fields := changedPropertyFields(property, after)
		if len(fields) == 0 {
			continue
		}
		beforeProjected, err := projectPropertyDefinition(property)
		if err != nil {
			return OntologyBlastRadiusReport{}, err
		}
		afterProjected, err := projectPropertyDefinition(after)
		if err != nil {
			return OntologyBlastRadiusReport{}, err
		}
		report.ChangedProperties = append(report.ChangedProperties, OntologyPropertyChangeProjection{
			Before: beforeProjected, After: afterProjected, Fields: fields,
			Impact: impactFor(counts, property.ID()),
		})
	}
	for _, concept := range proposed.Concepts() {
		if _, exists := activeConcepts[concept.ID()]; exists {
			continue
		}
		projected, err := projectConceptDefinition(concept)
		if err != nil {
			return OntologyBlastRadiusReport{}, err
		}
		report.AddedConcepts = append(report.AddedConcepts, projected)
	}
	for _, relationship := range proposed.Relationships() {
		if _, exists := activeRelationships[relationship.ID()]; exists {
			continue
		}
		projected, err := projectRelationshipDefinition(relationship)
		if err != nil {
			return OntologyBlastRadiusReport{}, err
		}
		report.AddedRelationships = append(report.AddedRelationships, projected)
	}
	for _, property := range proposed.Properties() {
		if _, exists := activeProperties[property.ID()]; exists {
			continue
		}
		projected, err := projectPropertyDefinition(property)
		if err != nil {
			return OntologyBlastRadiusReport{}, err
		}
		report.AddedProperties = append(report.AddedProperties, projected)
	}
	report.Summary = ontologyBlastRadiusSummary(report)
	// This report-side bound is load-bearing; TestOntologyProposalBlastRadiusRejectsOverLimitReport
	// pins that two individually valid ontology versions can still produce a
	// diff too large to emit, and the server must error rather than truncate it.
	if err := enforceOntologyBlastRadiusBounds(report); err != nil {
		return OntologyBlastRadiusReport{}, err
	}
	return report, nil
}

func conceptsByID(version ontology.OntologyVersion) map[shoal.ID]ontology.ConceptDefinition {
	concepts := version.Concepts()
	result := make(map[shoal.ID]ontology.ConceptDefinition, len(concepts))
	for _, concept := range concepts {
		result[concept.ID()] = concept
	}
	return result
}

func relationshipsByID(
	version ontology.OntologyVersion,
) map[shoal.ID]ontology.RelationshipDefinition {
	relationships := version.Relationships()
	result := make(map[shoal.ID]ontology.RelationshipDefinition, len(relationships))
	for _, relationship := range relationships {
		result[relationship.ID()] = relationship
	}
	return result
}

func propertiesByID(version ontology.OntologyVersion) map[shoal.ID]ontology.PropertyDefinition {
	properties := version.Properties()
	result := make(map[shoal.ID]ontology.PropertyDefinition, len(properties))
	for _, property := range properties {
		result[property.ID()] = property
	}
	return result
}

func changedOntologyElementIDs(
	active ontology.OntologyVersion,
	proposed ontology.OntologyVersion,
) []shoal.ID {
	ids := []shoal.ID{}
	proposedConcepts := conceptsByID(proposed)
	proposedRelationships := relationshipsByID(proposed)
	proposedProperties := propertiesByID(proposed)
	for _, concept := range active.Concepts() {
		after, exists := proposedConcepts[concept.ID()]
		if !exists || len(changedConceptFields(concept, after)) > 0 {
			ids = append(ids, concept.ID())
		}
	}
	for _, relationship := range active.Relationships() {
		after, exists := proposedRelationships[relationship.ID()]
		if !exists || len(changedRelationshipFields(relationship, after)) > 0 {
			ids = append(ids, relationship.ID())
		}
	}
	for _, property := range active.Properties() {
		after, exists := proposedProperties[property.ID()]
		if !exists || len(changedPropertyFields(property, after)) > 0 {
			ids = append(ids, property.ID())
		}
	}
	return ids
}

func changedConceptFields(
	before, after ontology.ConceptDefinition,
) []string {
	// This property-set comparison is load-bearing; TestOntologyProposalBlastRadiusReportsStructuralDiffAndUnknownCounts
	// pins that narrowing a concept's declared properties is reported before
	// governance transitions can publish it.
	if reflect.DeepEqual(before.Properties(), after.Properties()) {
		return nil
	}
	return []string{"properties"}
}

func changedRelationshipFields(
	before, after ontology.RelationshipDefinition,
) []string {
	fields := []string{}
	if !reflect.DeepEqual(before.FromConcepts(), after.FromConcepts()) {
		fields = append(fields, "from_concepts")
	}
	// This endpoint-set comparison is load-bearing; TestOntologyProposalBlastRadiusReportsStructuralDiffAndUnknownCounts
	// pins that changing a relationship's target concepts is reported because
	// narrowed joins can orphan existing relationship assertions.
	if !reflect.DeepEqual(before.ToConcepts(), after.ToConcepts()) {
		fields = append(fields, "to_concepts")
	}
	if before.Directed() != after.Directed() {
		fields = append(fields, "directed")
	}
	if !reflect.DeepEqual(before.Properties(), after.Properties()) {
		fields = append(fields, "properties")
	}
	return fields
}

func changedPropertyFields(
	before, after ontology.PropertyDefinition,
) []string {
	fields := []string{}
	// This value-type comparison is load-bearing; TestOntologyProposalBlastRadiusReportsStructuralDiffAndUnknownCounts
	// pins that property type changes are reported because stored values may no
	// longer validate under the proposed ontology.
	if before.ValueType() != after.ValueType() {
		fields = append(fields, "value_type")
	}
	if !reflect.DeepEqual(before.Constraints(), after.Constraints()) {
		fields = append(fields, "constraints")
	}
	return fields
}

func projectConceptDefinition(
	concept ontology.ConceptDefinition,
) (OntologyConceptProjection, error) {
	if err := concept.Validate(); err != nil {
		return OntologyConceptProjection{}, err
	}
	return OntologyConceptProjection{
		ID: encodeID(concept.ID()), Key: concept.Key(), Name: concept.Name(),
		Description: concept.Description(),
		Properties:  encodeOntologyIDs(concept.Properties()),
	}, nil
}

func projectRelationshipDefinition(
	relationship ontology.RelationshipDefinition,
) (OntologyRelationProjection, error) {
	if err := relationship.Validate(); err != nil {
		return OntologyRelationProjection{}, err
	}
	return OntologyRelationProjection{
		ID: encodeID(relationship.ID()), Key: relationship.Key(),
		Name: relationship.Name(), Description: relationship.Description(),
		Directed:     relationship.Directed(),
		FromConcepts: encodeOntologyIDs(relationship.FromConcepts()),
		ToConcepts:   encodeOntologyIDs(relationship.ToConcepts()),
		Properties:   encodeOntologyIDs(relationship.Properties()),
	}, nil
}

func projectPropertyDefinition(
	property ontology.PropertyDefinition,
) (OntologyPropertyProjection, error) {
	if err := property.Validate(); err != nil {
		return OntologyPropertyProjection{}, err
	}
	constraints, err := projectConstraints(property.Constraints())
	if err != nil {
		return OntologyPropertyProjection{}, err
	}
	return OntologyPropertyProjection{
		ID: encodeID(property.ID()), Key: property.Key(), Name: property.Name(),
		Description: property.Description(), ValueType: string(property.ValueType()),
		Constraints: constraints,
	}, nil
}

func impactFor(
	counts map[shoal.ID]OntologyAssertionImpactProjection,
	id shoal.ID,
) OntologyAssertionImpactProjection {
	if impact, ok := counts[id]; ok {
		return impact
	}
	// This explicit false is load-bearing; TestOntologyProposalBlastRadiusReportsStructuralDiffAndUnknownCounts
	// pins that missing corpus counts render as "not computed" instead of a
	// misleading zero asserted/derived impact.
	return OntologyAssertionImpactProjection{Computed: false}
}

func ontologyBlastRadiusSummary(
	report OntologyBlastRadiusReport,
) OntologyBlastRadiusSummary {
	summary := OntologyBlastRadiusSummary{
		RemovedConcepts:      uint32(len(report.RemovedConcepts)),
		RemovedRelationships: uint32(len(report.RemovedRelationships)),
		RemovedProperties:    uint32(len(report.RemovedProperties)),
		ChangedConcepts:      uint32(len(report.ChangedConcepts)),
		ChangedRelationships: uint32(len(report.ChangedRelationships)),
		ChangedProperties:    uint32(len(report.ChangedProperties)),
		AddedConcepts:        uint32(len(report.AddedConcepts)),
		AddedRelationships:   uint32(len(report.AddedRelationships)),
		AddedProperties:      uint32(len(report.AddedProperties)),
	}
	summary.DestructiveChanges = summary.RemovedConcepts +
		summary.RemovedRelationships + summary.RemovedProperties +
		summary.ChangedConcepts + summary.ChangedRelationships +
		summary.ChangedProperties
	summary.AdditiveChanges = summary.AddedConcepts +
		summary.AddedRelationships + summary.AddedProperties
	summary.CountsComputed = allImpactCountsComputed(report)
	return summary
}

func allImpactCountsComputed(report OntologyBlastRadiusReport) bool {
	for _, item := range report.RemovedConcepts {
		if !item.Impact.Computed {
			return false
		}
	}
	for _, item := range report.RemovedRelationships {
		if !item.Impact.Computed {
			return false
		}
	}
	for _, item := range report.RemovedProperties {
		if !item.Impact.Computed {
			return false
		}
	}
	for _, item := range report.ChangedConcepts {
		if !item.Impact.Computed {
			return false
		}
	}
	for _, item := range report.ChangedRelationships {
		if !item.Impact.Computed {
			return false
		}
	}
	for _, item := range report.ChangedProperties {
		if !item.Impact.Computed {
			return false
		}
	}
	return true
}

func enforceOntologyBlastRadiusBounds(report OntologyBlastRadiusReport) error {
	count := len(report.RemovedConcepts) + len(report.RemovedRelationships) +
		len(report.RemovedProperties) + len(report.ChangedConcepts) +
		len(report.ChangedRelationships) + len(report.ChangedProperties) +
		len(report.AddedConcepts) + len(report.AddedRelationships) +
		len(report.AddedProperties)
	if uint32(count) > MaxOntologyBlastRadiusItems {
		return ontologyBoundError("blast radius item", count, MaxOntologyBlastRadiusItems)
	}
	return nil
}

func ontologyBlastRadiusLimits() OntologyBlastRadiusLimits {
	return OntologyBlastRadiusLimits{
		MaxItems:                   MaxOntologyBlastRadiusItems,
		MaxConcepts:                MaxOntologyConcepts,
		MaxRelationships:           MaxOntologyRelationships,
		MaxProperties:              MaxOntologyProperties,
		MaxDefinitionProperties:    MaxOntologyDefinitionProperties,
		MaxRelationshipEndpointSet: MaxOntologyRelationshipEndpointSets,
	}
}
