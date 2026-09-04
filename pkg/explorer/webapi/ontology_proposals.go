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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	MaxOntologyProposals           uint32 = 256
	MaxOntologyProposalTransitions uint32 = 128
)

type OntologyProposalProvider interface {
	OntologyProposals(context.Context) ([]ontology.GovernedProposal, error)
	CreateOntologyProposal(
		context.Context, CreateOntologyProposalRequest,
	) (ontology.GovernedProposal, error)
	TransitionOntologyProposal(
		context.Context, shoal.ID, TransitionOntologyProposalRequest,
	) (ontology.GovernedProposal, error)
}

type ontologyProposalStoreClient interface {
	OntologyProposals(context.Context) ([]ontology.GovernedProposal, error)
	CreateOntologyProposal(
		context.Context, ontology.GovernedProposal, ontology.OntologyVersion,
	) error
	TransitionOntologyProposal(
		context.Context,
		shoal.ID,
		ontology.ProposalState,
		string,
		string,
		time.Time,
	) (ontology.GovernedProposal, error)
}

type OntologyProposalsResponse struct {
	Proposals []OntologyProposalProjection `json:"proposals"`
	Limits    OntologyProposalLimits       `json:"limits"`
}

type OntologyProposalResponse struct {
	Proposal OntologyProposalProjection `json:"proposal"`
}

type OntologyProposalProjection struct {
	ID               string                         `json:"id"`
	BaseSchemaID     string                         `json:"base_schema_id"`
	BaseVersionID    string                         `json:"base_version_id,omitempty"`
	ProposedBy       string                         `json:"proposed_by"`
	Rationale        string                         `json:"rationale"`
	CreatedAt        time.Time                      `json:"created_at"`
	UpdatedAt        time.Time                      `json:"updated_at"`
	State            string                         `json:"state"`
	ProposedOntology OntologyResponse               `json:"proposed_ontology"`
	Transitions      []ProposalTransitionProjection `json:"transitions"`
}

type ProposalTransitionProjection struct {
	From  string    `json:"from"`
	To    string    `json:"to"`
	Actor string    `json:"actor"`
	Note  string    `json:"note"`
	At    time.Time `json:"at"`
}

type OntologyProposalLimits struct {
	MaxProposals              uint32 `json:"max_proposals"`
	MaxTransitions            uint32 `json:"max_transitions"`
	MaxConcepts               uint32 `json:"max_concepts"`
	MaxRelationships          uint32 `json:"max_relationships"`
	MaxProperties             uint32 `json:"max_properties"`
	MaxConstraintsPerProperty uint32 `json:"max_constraints_per_property"`
	MaxAllowedValues          uint32 `json:"max_allowed_values"`
}

type CreateOntologyProposalRequest struct {
	Rationale       string                       `json:"rationale"`
	ProposedVersion OntologyProposalVersionDraft `json:"proposed_version"`
}

type TransitionOntologyProposalRequest struct {
	State string `json:"state"`
	Note  string `json:"note"`
}

type OntologyProposalVersionDraft struct {
	Version       string                              `json:"version"`
	Concepts      []OntologyProposalConceptDraft      `json:"concepts"`
	Relationships []OntologyProposalRelationshipDraft `json:"relationships"`
	Properties    []OntologyProposalPropertyDraft     `json:"properties"`
}

type OntologyProposalConceptDraft struct {
	Key         string   `json:"key"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Properties  []string `json:"properties,omitempty"`
}

type OntologyProposalRelationshipDraft struct {
	Key          string   `json:"key"`
	Name         string   `json:"name"`
	Description  string   `json:"description,omitempty"`
	FromConcepts []string `json:"from_concepts"`
	ToConcepts   []string `json:"to_concepts"`
	Properties   []string `json:"properties,omitempty"`
	Directed     bool     `json:"directed"`
}

type OntologyProposalPropertyDraft struct {
	Key         string                            `json:"key"`
	Name        string                            `json:"name"`
	Description string                            `json:"description,omitempty"`
	ValueType   ontology.ValueType                `json:"value_type"`
	Constraints []OntologyProposalConstraintDraft `json:"constraints,omitempty"`
}

type OntologyProposalConstraintDraft struct {
	Kind          ontology.ConstraintKind      `json:"kind"`
	Count         *uint32                      `json:"count,omitempty"`
	Value         *OntologyProposalValueDraft  `json:"value,omitempty"`
	Pattern       string                       `json:"pattern,omitempty"`
	AllowedValues []OntologyProposalValueDraft `json:"allowed_values,omitempty"`
}

type OntologyProposalValueDraft struct {
	Type  ontology.ValueType `json:"type"`
	Value json.RawMessage    `json:"value"`
}

func (s *EmbeddedService) OntologyProposals(
	ctx context.Context,
) ([]ontology.GovernedProposal, error) {
	store, ok := s.client.(ontologyProposalStoreClient)
	if !ok {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable, "workspace capability \"ontology proposals\" is unavailable")
	}
	return store.OntologyProposals(ctx)
}

func (s *EmbeddedService) CreateOntologyProposal(
	ctx context.Context,
	request CreateOntologyProposalRequest,
) (ontology.GovernedProposal, error) {
	store, ok := s.client.(ontologyProposalStoreClient)
	if !ok {
		return ontology.GovernedProposal{}, shoal.NewError(
			shoal.ErrorUnavailable, "workspace capability \"ontology proposals\" is unavailable")
	}
	base, configured, err := s.ActiveOntology(ctx)
	if err != nil {
		return ontology.GovernedProposal{}, err
	}
	if !configured {
		return ontology.GovernedProposal{}, shoal.NewError(
			shoal.ErrorUnavailable, "an active ontology is required to propose a refinement")
	}
	now := s.now()
	proposed, err := ontologyVersionFromProposalDraft(
		base.Schema(), request.ProposedVersion, now)
	if err != nil {
		return ontology.GovernedProposal{}, err
	}
	if err := enforceOntologyBounds(proposed); err != nil {
		return ontology.GovernedProposal{}, err
	}
	proposal, err := ontology.NewGovernedProposal(
		base.Schema(), base, proposed, ontologyActor(ctx), request.Rationale, now, nil)
	if err != nil {
		return ontology.GovernedProposal{}, err
	}
	if err := store.CreateOntologyProposal(ctx, proposal, base); err != nil {
		return ontology.GovernedProposal{}, err
	}
	return proposal, nil
}

func (s *EmbeddedService) TransitionOntologyProposal(
	ctx context.Context,
	proposalID shoal.ID,
	request TransitionOntologyProposalRequest,
) (ontology.GovernedProposal, error) {
	store, ok := s.client.(ontologyProposalStoreClient)
	if !ok {
		return ontology.GovernedProposal{}, shoal.NewError(
			shoal.ErrorUnavailable, "workspace capability \"ontology proposals\" is unavailable")
	}
	next := ontology.ProposalState(strings.TrimSpace(request.State))
	proposal, err := store.TransitionOntologyProposal(
		ctx, proposalID, next, ontologyActor(ctx), request.Note, s.now())
	if err != nil {
		return ontology.GovernedProposal{}, err
	}
	if next == ontology.ProposalPublished {
		// This assignment is load-bearing; TestOntologyProposalPublishUpdatesActiveOntology
		// pins that a published proposal becomes the active ontology read by the
		// existing /api/v1/ontology surface.
		if err := s.SetOntologyVersion(proposal.ProposedVersion()); err != nil {
			return ontology.GovernedProposal{}, err
		}
	}
	return proposal, nil
}

func ontologyProposalsFor(
	ctx context.Context,
	service Service,
) (OntologyProposalsResponse, error) {
	provider, ok := service.(OntologyProposalProvider)
	if !ok {
		return OntologyProposalsResponse{
			Proposals: []OntologyProposalProjection{},
			Limits:    ontologyProposalLimits(),
		}, nil
	}
	proposals, err := provider.OntologyProposals(ctx)
	if err != nil {
		return OntologyProposalsResponse{}, err
	}
	return projectOntologyProposals(proposals)
}

func createOntologyProposalFor(
	ctx context.Context,
	service Service,
	request CreateOntologyProposalRequest,
) (OntologyProposalResponse, error) {
	provider, ok := service.(OntologyProposalProvider)
	if !ok {
		return OntologyProposalResponse{}, shoal.NewError(
			shoal.ErrorUnavailable, "workspace capability \"ontology proposals\" is unavailable")
	}
	proposal, err := provider.CreateOntologyProposal(ctx, request)
	if err != nil {
		return OntologyProposalResponse{}, err
	}
	projected, err := projectOntologyProposal(proposal)
	if err != nil {
		return OntologyProposalResponse{}, err
	}
	return OntologyProposalResponse{Proposal: projected}, nil
}

func transitionOntologyProposalFor(
	ctx context.Context,
	service Service,
	proposalID shoal.ID,
	request TransitionOntologyProposalRequest,
) (OntologyProposalResponse, error) {
	provider, ok := service.(OntologyProposalProvider)
	if !ok {
		return OntologyProposalResponse{}, shoal.NewError(
			shoal.ErrorUnavailable, "workspace capability \"ontology proposals\" is unavailable")
	}
	proposal, err := provider.TransitionOntologyProposal(ctx, proposalID, request)
	if err != nil {
		return OntologyProposalResponse{}, err
	}
	projected, err := projectOntologyProposal(proposal)
	if err != nil {
		return OntologyProposalResponse{}, err
	}
	return OntologyProposalResponse{Proposal: projected}, nil
}

func projectOntologyProposals(
	proposals []ontology.GovernedProposal,
) (OntologyProposalsResponse, error) {
	if uint32(len(proposals)) > MaxOntologyProposals {
		return OntologyProposalsResponse{}, ontologyBoundError(
			"proposal", len(proposals), MaxOntologyProposals)
	}
	ordered := append([]ontology.GovernedProposal(nil), proposals...)
	// This sort is load-bearing; TestOntologyProposalEndpointReturnsStableOrdering
	// pins deterministic newest-first JSON even when a provider returns an
	// unordered slice.
	sort.Slice(ordered, func(left, right int) bool {
		if ordered[left].UpdatedAt().Equal(ordered[right].UpdatedAt()) {
			return shoal.CompareID(ordered[left].ID(), ordered[right].ID()) < 0
		}
		return ordered[left].UpdatedAt().After(ordered[right].UpdatedAt())
	})
	response := OntologyProposalsResponse{
		Proposals: []OntologyProposalProjection{},
		Limits:    ontologyProposalLimits(),
	}
	for _, proposal := range ordered {
		projected, err := projectOntologyProposal(proposal)
		if err != nil {
			return OntologyProposalsResponse{}, err
		}
		response.Proposals = append(response.Proposals, projected)
	}
	return response, nil
}

func projectOntologyProposal(
	proposal ontology.GovernedProposal,
) (OntologyProposalProjection, error) {
	if err := proposal.Validate(); err != nil {
		return OntologyProposalProjection{}, err
	}
	if err := enforceOntologyBounds(proposal.ProposedVersion()); err != nil {
		return OntologyProposalProjection{}, err
	}
	transitions := proposal.Transitions()
	if uint32(len(transitions)) > MaxOntologyProposalTransitions {
		return OntologyProposalProjection{}, ontologyBoundError(
			"proposal transition", len(transitions), MaxOntologyProposalTransitions)
	}
	proposed, err := projectOntology(proposal.ProposedVersion())
	if err != nil {
		return OntologyProposalProjection{}, err
	}
	baseVersionID, _ := proposal.BaseVersionID()
	projected := OntologyProposalProjection{
		ID:               encodeID(proposal.ID()),
		BaseSchemaID:     encodeID(proposal.Schema().ID()),
		BaseVersionID:    encodeOptionalID(baseVersionID),
		ProposedBy:       proposal.ProposedBy(),
		Rationale:        proposal.Rationale(),
		CreatedAt:        proposal.CreatedAt(),
		UpdatedAt:        proposal.UpdatedAt(),
		State:            string(proposal.State()),
		ProposedOntology: proposed,
		Transitions:      []ProposalTransitionProjection{},
	}
	for _, transition := range transitions {
		projected.Transitions = append(projected.Transitions, ProposalTransitionProjection{
			From:  string(transition.From()),
			To:    string(transition.To()),
			Actor: transition.Actor(),
			Note:  transition.Note(),
			At:    transition.At(),
		})
	}
	return projected, nil
}

func ontologyProposalLimits() OntologyProposalLimits {
	return OntologyProposalLimits{
		MaxProposals:              MaxOntologyProposals,
		MaxTransitions:            MaxOntologyProposalTransitions,
		MaxConcepts:               MaxOntologyConcepts,
		MaxRelationships:          MaxOntologyRelationships,
		MaxProperties:             MaxOntologyProperties,
		MaxConstraintsPerProperty: MaxOntologyConstraintsPerProperty,
		MaxAllowedValues:          MaxOntologyAllowedValues,
	}
}

func ontologyVersionFromProposalDraft(
	schema ontology.OntologySchema,
	draft OntologyProposalVersionDraft,
	createdAt time.Time,
) (ontology.OntologyVersion, error) {
	properties, propertyIDs, err := proposalPropertiesFromDraft(draft.Properties)
	if err != nil {
		return ontology.OntologyVersion{}, err
	}
	concepts, conceptIDs, err := proposalConceptsFromDraft(
		draft.Concepts, propertyIDs)
	if err != nil {
		return ontology.OntologyVersion{}, err
	}
	relationships, err := proposalRelationshipsFromDraft(
		draft.Relationships, conceptIDs, propertyIDs)
	if err != nil {
		return ontology.OntologyVersion{}, err
	}
	return ontology.NewOntologyVersion(
		schema, draft.Version, createdAt, concepts, relationships, properties, nil)
}

func proposalPropertiesFromDraft(
	drafts []OntologyProposalPropertyDraft,
) ([]ontology.PropertyDefinition, map[string]shoal.ID, error) {
	properties := make([]ontology.PropertyDefinition, 0, len(drafts))
	ids := make(map[string]shoal.ID, len(drafts))
	for _, draft := range drafts {
		constraints, err := proposalConstraintsFromDraft(draft.Constraints)
		if err != nil {
			return nil, nil, fmt.Errorf("ontology property %q constraints: %w", draft.Key, err)
		}
		property, err := ontology.NewPropertyDefinition(
			draft.Key, draft.Name, draft.Description, draft.ValueType, constraints, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("ontology property %q: %w", draft.Key, err)
		}
		if _, duplicate := ids[draft.Key]; duplicate {
			return nil, nil, fmt.Errorf("ontology property key %q is duplicated", draft.Key)
		}
		ids[draft.Key] = property.ID()
		properties = append(properties, property)
	}
	return properties, ids, nil
}

func proposalConceptsFromDraft(
	drafts []OntologyProposalConceptDraft,
	propertyIDs map[string]shoal.ID,
) ([]ontology.ConceptDefinition, map[string]shoal.ID, error) {
	concepts := make([]ontology.ConceptDefinition, 0, len(drafts))
	ids := make(map[string]shoal.ID, len(drafts))
	for _, draft := range drafts {
		properties, err := resolveProposalDefinitionIDs(
			"property", draft.Properties, propertyIDs)
		if err != nil {
			return nil, nil, fmt.Errorf("ontology concept %q: %w", draft.Key, err)
		}
		concept, err := ontology.NewConceptDefinition(
			draft.Key, draft.Name, draft.Description, properties, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("ontology concept %q: %w", draft.Key, err)
		}
		if _, duplicate := ids[draft.Key]; duplicate {
			return nil, nil, fmt.Errorf("ontology concept key %q is duplicated", draft.Key)
		}
		ids[draft.Key] = concept.ID()
		concepts = append(concepts, concept)
	}
	return concepts, ids, nil
}

func proposalRelationshipsFromDraft(
	drafts []OntologyProposalRelationshipDraft,
	conceptIDs map[string]shoal.ID,
	propertyIDs map[string]shoal.ID,
) ([]ontology.RelationshipDefinition, error) {
	relationships := make([]ontology.RelationshipDefinition, 0, len(drafts))
	seen := make(map[string]struct{}, len(drafts))
	for _, draft := range drafts {
		from, err := resolveProposalDefinitionIDs(
			"concept", draft.FromConcepts, conceptIDs)
		if err != nil {
			return nil, fmt.Errorf("ontology relationship %q sources: %w", draft.Key, err)
		}
		to, err := resolveProposalDefinitionIDs(
			"concept", draft.ToConcepts, conceptIDs)
		if err != nil {
			return nil, fmt.Errorf("ontology relationship %q targets: %w", draft.Key, err)
		}
		properties, err := resolveProposalDefinitionIDs(
			"property", draft.Properties, propertyIDs)
		if err != nil {
			return nil, fmt.Errorf("ontology relationship %q properties: %w", draft.Key, err)
		}
		relationship, err := ontology.NewRelationshipDefinition(
			draft.Key, draft.Name, draft.Description, from, to,
			properties, draft.Directed, nil)
		if err != nil {
			return nil, fmt.Errorf("ontology relationship %q: %w", draft.Key, err)
		}
		if _, duplicate := seen[draft.Key]; duplicate {
			return nil, fmt.Errorf("ontology relationship key %q is duplicated", draft.Key)
		}
		seen[draft.Key] = struct{}{}
		relationships = append(relationships, relationship)
	}
	return relationships, nil
}

func proposalConstraintsFromDraft(
	drafts []OntologyProposalConstraintDraft,
) ([]ontology.Constraint, error) {
	constraints := make([]ontology.Constraint, 0, len(drafts))
	for _, draft := range drafts {
		constraint, err := proposalConstraintFromDraft(draft)
		if err != nil {
			return nil, err
		}
		constraints = append(constraints, constraint)
	}
	return constraints, nil
}

func proposalConstraintFromDraft(
	draft OntologyProposalConstraintDraft,
) (ontology.Constraint, error) {
	switch draft.Kind {
	case ontology.ConstraintRequired, ontology.ConstraintUnique:
		return ontology.NewFlagConstraint(draft.Kind)
	case ontology.ConstraintMinimumCount, ontology.ConstraintMaximumCount:
		if draft.Count == nil {
			return ontology.Constraint{}, fmt.Errorf("%s constraint requires count", draft.Kind)
		}
		return ontology.NewCountConstraint(draft.Kind, *draft.Count)
	case ontology.ConstraintMinimumValue, ontology.ConstraintMaximumValue:
		if draft.Value == nil {
			return ontology.Constraint{}, fmt.Errorf("%s constraint requires value", draft.Kind)
		}
		value, err := proposalValueFromDraft(*draft.Value)
		if err != nil {
			return ontology.Constraint{}, err
		}
		return ontology.NewValueConstraint(draft.Kind, value)
	case ontology.ConstraintPattern:
		return ontology.NewPatternConstraint(draft.Pattern)
	case ontology.ConstraintAllowedValues:
		values := make([]ontology.Value, 0, len(draft.AllowedValues))
		for _, item := range draft.AllowedValues {
			value, err := proposalValueFromDraft(item)
			if err != nil {
				return ontology.Constraint{}, err
			}
			values = append(values, value)
		}
		return ontology.NewAllowedValuesConstraint(values)
	default:
		return ontology.Constraint{}, fmt.Errorf("unknown constraint kind %q", draft.Kind)
	}
}

func proposalValueFromDraft(draft OntologyProposalValueDraft) (ontology.Value, error) {
	switch draft.Type {
	case ontology.ValueString:
		var value string
		if err := json.Unmarshal(draft.Value, &value); err != nil {
			return ontology.Value{}, fmt.Errorf("string ontology value: %w", err)
		}
		return ontology.NewStringValue(value)
	case ontology.ValueInteger:
		integer, err := jsonNumberInt64(draft.Value)
		if err != nil {
			return ontology.Value{}, fmt.Errorf("integer ontology value: %w", err)
		}
		return ontology.NewIntegerValue(integer), nil
	case ontology.ValueNumber:
		number, err := jsonNumberFloat64(draft.Value)
		if err != nil {
			return ontology.Value{}, fmt.Errorf("number ontology value: %w", err)
		}
		return ontology.NewNumberValue(number)
	case ontology.ValueBoolean:
		var value bool
		if err := json.Unmarshal(draft.Value, &value); err != nil {
			return ontology.Value{}, fmt.Errorf("boolean ontology value: %w", err)
		}
		return ontology.NewBooleanValue(value), nil
	case ontology.ValueTimestamp:
		var value time.Time
		if err := json.Unmarshal(draft.Value, &value); err != nil {
			return ontology.Value{}, fmt.Errorf("timestamp ontology value: %w", err)
		}
		return ontology.NewTimestampValue(value)
	case ontology.ValueReference:
		var value string
		if err := json.Unmarshal(draft.Value, &value); err != nil {
			return ontology.Value{}, fmt.Errorf("reference ontology value: %w", err)
		}
		return ontology.NewReferenceValue(shoal.ID(value))
	default:
		return ontology.Value{}, fmt.Errorf("unknown ontology value type %q", draft.Type)
	}
}

func jsonNumberInt64(raw json.RawMessage) (int64, error) {
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		var text string
		if textErr := json.Unmarshal(raw, &text); textErr != nil {
			return 0, err
		}
		number = json.Number(text)
	}
	return number.Int64()
}

func jsonNumberFloat64(raw json.RawMessage) (float64, error) {
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		var text string
		if textErr := json.Unmarshal(raw, &text); textErr != nil {
			return 0, err
		}
		number = json.Number(text)
	}
	return number.Float64()
}

func resolveProposalDefinitionIDs(
	kind string,
	keys []string,
	ids map[string]shoal.ID,
) ([]shoal.ID, error) {
	resolved := make([]shoal.ID, 0, len(keys))
	for _, key := range keys {
		id, ok := ids[key]
		if !ok {
			return nil, fmt.Errorf("unknown %s key %q", kind, key)
		}
		resolved = append(resolved, id)
	}
	return resolved, nil
}

func ontologyActor(ctx context.Context) string {
	if identity, ok := identityFromContext(ctx); ok {
		if strings.TrimSpace(identity.Subject) != "" {
			return identity.Subject
		}
		if strings.TrimSpace(identity.Actor) != "" {
			return identity.Actor
		}
	}
	return "anonymous"
}

func (s *EmbeddedService) now() time.Time {
	if s.clock != nil {
		return s.clock().UTC()
	}
	return time.Now().UTC()
}
