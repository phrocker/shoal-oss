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
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// ExtractionContractVersion identifies the public extraction envelope.
type ExtractionContractVersion string

const (
	// ExtractionContractV1 is the fail-closed grounded extraction contract.
	ExtractionContractV1 ExtractionContractVersion = "v1"
)

const (
	hardMaxEvidence         = 4096
	hardMaxAssertions       = 16384
	hardMaxProposals        = 1024
	hardMaxQuoteBytes       = 1 << 20
	hardMaxInstructionBytes = 1 << 20
	hardMaxPathNodes        = 16384
	hardMaxPathEdges        = 32768
	hardMaxSchemaMembers    = 32768
	hardMaxMetadataEntries  = 4096
	hardMaxMetadataBytes    = 1 << 20
)

// ExtractionLimits bounds one v1 extraction envelope. Zero assertions or
// proposals disables that output category.
type ExtractionLimits struct {
	MaxEvidence         uint32
	MaxAssertions       uint32
	MaxProposals        uint32
	MaxQuoteBytes       uint32
	MaxInstructionBytes uint32
	MaxPathNodes        uint32
	MaxPathEdges        uint32
	MaxSchemaMembers    uint32
	MaxMetadataEntries  uint32
	MaxMetadataBytes    uint32
}

// DefaultExtractionLimits returns conservative v1 limits.
func DefaultExtractionLimits() ExtractionLimits {
	return ExtractionLimits{
		MaxEvidence: 256, MaxAssertions: 1024, MaxProposals: 64,
		MaxQuoteBytes: 64 << 10, MaxInstructionBytes: 64 << 10,
		MaxPathNodes: 1024, MaxPathEdges: 2048, MaxSchemaMembers: 4096,
		MaxMetadataEntries: 256, MaxMetadataBytes: 64 << 10,
	}
}

// Validate checks non-zero input bounds and fixed v1 safety ceilings.
func (l ExtractionLimits) Validate() error {
	if l.MaxEvidence == 0 || l.MaxEvidence > hardMaxEvidence ||
		l.MaxAssertions > hardMaxAssertions ||
		l.MaxProposals > hardMaxProposals ||
		l.MaxQuoteBytes == 0 || l.MaxQuoteBytes > hardMaxQuoteBytes ||
		l.MaxInstructionBytes == 0 ||
		l.MaxInstructionBytes > hardMaxInstructionBytes ||
		l.MaxPathNodes == 0 || l.MaxPathNodes > hardMaxPathNodes ||
		l.MaxPathEdges == 0 || l.MaxPathEdges > hardMaxPathEdges ||
		l.MaxSchemaMembers == 0 || l.MaxSchemaMembers > hardMaxSchemaMembers ||
		l.MaxMetadataEntries == 0 ||
		l.MaxMetadataEntries > hardMaxMetadataEntries ||
		l.MaxMetadataBytes == 0 || l.MaxMetadataBytes > hardMaxMetadataBytes {
		return invalid("extraction limits are outside v1 safety bounds")
	}
	return nil
}

func (l ExtractionLimits) canonical() string {
	return canonicalParts(
		strconv.FormatUint(uint64(l.MaxEvidence), 10),
		strconv.FormatUint(uint64(l.MaxAssertions), 10),
		strconv.FormatUint(uint64(l.MaxProposals), 10),
		strconv.FormatUint(uint64(l.MaxQuoteBytes), 10),
		strconv.FormatUint(uint64(l.MaxInstructionBytes), 10),
		strconv.FormatUint(uint64(l.MaxPathNodes), 10),
		strconv.FormatUint(uint64(l.MaxPathEdges), 10),
		strconv.FormatUint(uint64(l.MaxSchemaMembers), 10),
		strconv.FormatUint(uint64(l.MaxMetadataEntries), 10),
		strconv.FormatUint(uint64(l.MaxMetadataBytes), 10),
	)
}

// ExtractionRequest supplies an immutable schema snapshot and cited evidence
// to an extractor.
type ExtractionRequest struct {
	id              shoal.ID
	contractVersion ExtractionContractVersion
	version         OntologyVersion
	evidence        []EvidenceRef
	instructions    string
	provenance      ExtractionProvenance
	limits          ExtractionLimits
	metadata        shoal.Metadata
}

// NewExtractionRequest creates a canonical extraction request.
func NewExtractionRequest(
	version OntologyVersion,
	evidence []EvidenceRef,
	instructions string,
	provenance ExtractionProvenance,
	limits ExtractionLimits,
	metadata shoal.Metadata,
) (ExtractionRequest, error) {
	request := ExtractionRequest{
		contractVersion: ExtractionContractV1,
		version:         version.clone(),
		evidence:        cloneEvidence(evidence),
		instructions:    instructions,
		provenance:      provenance.clone(),
		limits:          limits,
		metadata:        cloneMetadata(metadata),
	}
	sort.Slice(request.evidence, func(left, right int) bool {
		return string(request.evidence[left].ID()) <
			string(request.evidence[right].ID())
	})
	id, err := extractionRequestID(request)
	if err != nil {
		return ExtractionRequest{}, err
	}
	request.id = id
	if err := request.Validate(); err != nil {
		return ExtractionRequest{}, err
	}
	return request, nil
}

// Validate checks request identity, schema, evidence, and wire values.
func (r ExtractionRequest) Validate() error {
	if err := validateTypedID(r.id, "extraction"); err != nil {
		return err
	}
	if r.contractVersion != ExtractionContractV1 {
		return invalid("unsupported extraction contract version")
	}
	if err := r.version.Validate(); err != nil {
		return err
	}
	if err := r.provenance.Validate(); err != nil {
		return err
	}
	if err := r.limits.Validate(); err != nil {
		return err
	}
	if err := validateBoundedVersion(r.version, r.limits); err != nil {
		return err
	}
	if len(r.evidence) == 0 {
		return invalid("extraction request requires evidence")
	}
	if uint64(len(r.evidence)) > uint64(r.limits.MaxEvidence) {
		return invalid("extraction evidence exceeds request limit")
	}
	for index, evidence := range r.evidence {
		if err := evidence.Validate(); err != nil {
			return err
		}
		if uint64(len(evidence.Quote())) > uint64(r.limits.MaxQuoteBytes) {
			return invalid("evidence quote exceeds request limit")
		}
		if path, present := evidence.Path(); present {
			if uint64(len(path.Nodes)) > uint64(r.limits.MaxPathNodes) ||
				uint64(len(path.Edges)) > uint64(r.limits.MaxPathEdges) {
				return invalid("evidence graph path exceeds request limit")
			}
		}
		if err := validateBoundedMetadata(evidence.Metadata(), r.limits); err != nil {
			return err
		}
		if index > 0 &&
			string(r.evidence[index-1].ID()) >= string(evidence.ID()) {
			return invalid("request evidence must be unique and canonically ordered")
		}
	}
	if !requiredWire(r.instructions) {
		return invalid("extraction instructions are required")
	}
	if uint64(len(r.instructions)) > uint64(r.limits.MaxInstructionBytes) {
		return invalid("extraction instructions exceed request limit")
	}
	if err := validateMetadata(r.metadata); err != nil {
		return err
	}
	if err := validateBoundedMetadata(r.metadata, r.limits); err != nil {
		return err
	}
	if err := validateBoundedMetadata(r.provenance.Metadata(), r.limits); err != nil {
		return err
	}
	expected, err := extractionRequestID(r)
	if err != nil || expected != r.id {
		return invalid("extraction request ID is not canonical")
	}
	return nil
}

func (r ExtractionRequest) ID() shoal.ID {
	return r.id
}

func (r ExtractionRequest) ContractVersion() ExtractionContractVersion {
	return r.contractVersion
}

func (r ExtractionRequest) Version() OntologyVersion {
	return r.version.clone()
}

func (r ExtractionRequest) Evidence() []EvidenceRef {
	return cloneEvidence(r.evidence)
}

func (r ExtractionRequest) Instructions() string {
	return r.instructions
}

func (r ExtractionRequest) Provenance() ExtractionProvenance {
	return r.provenance.clone()
}

func (r ExtractionRequest) Limits() ExtractionLimits {
	return r.limits
}

func (r ExtractionRequest) Metadata() shoal.Metadata {
	return cloneMetadata(r.metadata)
}

func (r ExtractionRequest) clone() ExtractionRequest {
	r.version = r.version.clone()
	r.evidence = cloneEvidence(r.evidence)
	r.provenance = r.provenance.clone()
	r.metadata = cloneMetadata(r.metadata)
	return r
}

func extractionRequestID(request ExtractionRequest) (shoal.ID, error) {
	if err := request.version.Validate(); err != nil {
		return "", err
	}
	if request.contractVersion != ExtractionContractV1 {
		return "", invalid("unsupported extraction contract version")
	}
	if err := request.provenance.Validate(); err != nil {
		return "", err
	}
	if err := request.limits.Validate(); err != nil {
		return "", err
	}
	if !requiredWire(request.instructions) {
		return "", invalid("extraction instructions are required")
	}
	if err := validateMetadata(request.metadata); err != nil {
		return "", err
	}
	evidenceIDs := make([]string, len(request.evidence))
	for index, evidence := range request.evidence {
		if err := evidence.Validate(); err != nil {
			return "", err
		}
		evidenceIDs[index] = string(evidence.ID())
	}
	return deriveID(
		"extraction",
		"request",
		string(request.contractVersion),
		string(request.version.ID()),
		canonicalParts(evidenceIDs...),
		request.instructions,
		request.provenance.canonical(),
		request.limits.canonical(),
		canonicalMetadata(request.metadata),
	)
}

// ExtractionResult is the immutable output of one exact request.
type ExtractionResult struct {
	id              shoal.ID
	contractVersion ExtractionContractVersion
	requestID       shoal.ID
	assertions      []Assertion
	proposals       []GovernedProposal
	provenance      ExtractionProvenance
	limits          ExtractionLimits
	completedAt     time.Time
	metadata        shoal.Metadata
}

// NewExtractionResult creates a request-bound extraction result.
func NewExtractionResult(
	request ExtractionRequest,
	assertions []Assertion,
	proposals []GovernedProposal,
	completedAt time.Time,
	metadata shoal.Metadata,
) (ExtractionResult, error) {
	if err := request.Validate(); err != nil {
		return ExtractionResult{}, err
	}
	result := ExtractionResult{
		contractVersion: request.ContractVersion(),
		requestID:       request.ID(),
		assertions:      cloneAssertions(assertions),
		proposals:       cloneProposals(proposals),
		provenance:      request.Provenance(),
		limits:          request.Limits(),
		completedAt:     normalizeTime(completedAt),
		metadata:        cloneMetadata(metadata),
	}
	sort.Slice(result.assertions, func(left, right int) bool {
		return string(result.assertions[left].ID()) <
			string(result.assertions[right].ID())
	})
	sort.Slice(result.proposals, func(left, right int) bool {
		return string(result.proposals[left].ID()) <
			string(result.proposals[right].ID())
	})
	id, err := extractionResultID(result)
	if err != nil {
		return ExtractionResult{}, err
	}
	result.id = id
	if err := result.ValidateFor(request); err != nil {
		return ExtractionResult{}, err
	}
	return result, nil
}

// Validate checks result-local invariants. ValidateFor additionally verifies
// request evidence and schema membership.
func (r ExtractionResult) Validate() error {
	if err := validateTypedID(r.id, "extraction"); err != nil {
		return err
	}
	if err := validateTypedID(r.requestID, "extraction"); err != nil {
		return err
	}
	if r.contractVersion != ExtractionContractV1 {
		return invalid("unsupported extraction contract version")
	}
	if err := r.provenance.Validate(); err != nil {
		return err
	}
	if err := r.limits.Validate(); err != nil {
		return err
	}
	if uint64(len(r.assertions)) > uint64(r.limits.MaxAssertions) {
		return invalid("extraction assertions exceed result limit")
	}
	if uint64(len(r.proposals)) > uint64(r.limits.MaxProposals) {
		return invalid("extraction proposals exceed result limit")
	}
	if err := validateTime(r.completedAt, "extraction completion time"); err != nil {
		return err
	}
	if err := validateMetadata(r.metadata); err != nil {
		return err
	}
	if err := validateBoundedMetadata(r.metadata, r.limits); err != nil {
		return err
	}
	if err := validateBoundedMetadata(r.provenance.Metadata(), r.limits); err != nil {
		return err
	}
	for index, assertion := range r.assertions {
		if err := assertion.Validate(); err != nil {
			return err
		}
		if assertion.Provenance().canonical() != r.provenance.canonical() {
			return invalid("assertion provenance does not match extraction result")
		}
		if uint64(len(assertion.Evidence())) > uint64(r.limits.MaxEvidence) {
			return invalid("assertion evidence exceeds result limit")
		}
		if err := validateBoundedMetadata(assertion.Metadata(), r.limits); err != nil {
			return err
		}
		if index > 0 &&
			string(r.assertions[index-1].ID()) >= string(assertion.ID()) {
			return invalid("result assertions must be unique and canonically ordered")
		}
	}
	for index, proposal := range r.proposals {
		if err := proposal.Validate(); err != nil {
			return err
		}
		if proposal.State() != ProposalDraft {
			return invalid("extracted ontology proposals must be drafts")
		}
		if err := validateBoundedVersion(proposal.ProposedVersion(), r.limits); err != nil {
			return err
		}
		if err := validateBoundedMetadata(proposal.Metadata(), r.limits); err != nil {
			return err
		}
		if index > 0 &&
			string(r.proposals[index-1].ID()) >= string(proposal.ID()) {
			return invalid("result proposals must be unique and canonically ordered")
		}
	}
	expected, err := extractionResultID(r)
	if err != nil || expected != r.id {
		return invalid("extraction result ID is not canonical")
	}
	return nil
}

// ValidateFor verifies that the result only cites request evidence and only
// uses definitions from the request's immutable ontology version.
func (r ExtractionResult) ValidateFor(request ExtractionRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if r.requestID != request.ID() {
		return invalid("extraction result does not match request")
	}
	if r.contractVersion != request.ContractVersion() ||
		r.limits != request.Limits() ||
		r.provenance.canonical() != request.Provenance().canonical() {
		return invalid("extraction result envelope does not match request")
	}
	requestEvidence := make(map[shoal.ID]struct{}, len(request.evidence))
	for _, evidence := range request.evidence {
		requestEvidence[evidence.ID()] = struct{}{}
	}
	for _, assertion := range r.assertions {
		for _, evidence := range assertion.Evidence() {
			if _, exists := requestEvidence[evidence.ID()]; !exists {
				return invalid("assertion cites evidence outside the extraction request")
			}
		}
		switch IDNamespace(assertion.Predicate()) {
		case "property":
			property, exists := request.version.property(assertion.Predicate())
			if !exists {
				return invalid("assertion references an unknown property")
			}
			if !valueMatchesType(assertion.Object(), property.ValueType()) {
				return invalid("assertion value does not match property type")
			}
		case "relationship":
			if _, exists := request.version.relationship(assertion.Predicate()); !exists {
				return invalid("assertion references an unknown relationship")
			}
			if assertion.Object().Type() != ValueReference {
				return invalid("relationship assertion requires a reference object")
			}
		default:
			return invalid("assertion predicate is not in the ontology")
		}
	}
	for _, proposal := range r.proposals {
		if proposal.Schema().ID() != request.version.Schema().ID() {
			return invalid("extracted proposal belongs to a different schema")
		}
	}
	return nil
}

func (r ExtractionResult) ID() shoal.ID {
	return r.id
}

func (r ExtractionResult) ContractVersion() ExtractionContractVersion {
	return r.contractVersion
}

func (r ExtractionResult) RequestID() shoal.ID {
	return r.requestID
}

func (r ExtractionResult) Assertions() []Assertion {
	return cloneAssertions(r.assertions)
}

func (r ExtractionResult) Proposals() []GovernedProposal {
	return cloneProposals(r.proposals)
}

func (r ExtractionResult) Provenance() ExtractionProvenance {
	return r.provenance.clone()
}

func (r ExtractionResult) Limits() ExtractionLimits {
	return r.limits
}

func (r ExtractionResult) CompletedAt() time.Time {
	return r.completedAt
}

func (r ExtractionResult) Metadata() shoal.Metadata {
	return cloneMetadata(r.metadata)
}

func extractionResultID(result ExtractionResult) (shoal.ID, error) {
	if err := validateTypedID(result.requestID, "extraction"); err != nil {
		return "", err
	}
	if result.contractVersion != ExtractionContractV1 {
		return "", invalid("unsupported extraction contract version")
	}
	if err := result.provenance.Validate(); err != nil {
		return "", err
	}
	if err := result.limits.Validate(); err != nil {
		return "", err
	}
	if err := validateTime(result.completedAt, "extraction completion time"); err != nil {
		return "", err
	}
	if err := validateMetadata(result.metadata); err != nil {
		return "", err
	}
	assertionIDs := make([]string, len(result.assertions))
	for index, assertion := range result.assertions {
		if err := assertion.Validate(); err != nil {
			return "", err
		}
		assertionIDs[index] = string(assertion.ID())
	}
	proposalIDs := make([]string, len(result.proposals))
	for index, proposal := range result.proposals {
		if err := proposal.Validate(); err != nil {
			return "", err
		}
		proposalIDs[index] = string(proposal.ID())
	}
	return deriveID(
		"extraction",
		"result",
		string(result.contractVersion),
		string(result.requestID),
		canonicalParts(assertionIDs...),
		canonicalParts(proposalIDs...),
		result.provenance.canonical(),
		result.limits.canonical(),
		canonicalTime(result.completedAt),
		canonicalMetadata(result.metadata),
	)
}

func validateBoundedVersion(version OntologyVersion, limits ExtractionLimits) error {
	memberCount := uint64(len(version.Concepts())) +
		uint64(len(version.Relationships())) +
		uint64(len(version.Properties()))
	if memberCount > uint64(limits.MaxSchemaMembers) {
		return invalid("ontology schema members exceed extraction limit")
	}
	if err := validateBoundedMetadata(version.Schema().Metadata(), limits); err != nil {
		return err
	}
	if err := validateBoundedMetadata(version.Metadata(), limits); err != nil {
		return err
	}
	for _, concept := range version.Concepts() {
		if err := validateBoundedMetadata(concept.Metadata(), limits); err != nil {
			return err
		}
	}
	for _, relationship := range version.Relationships() {
		if err := validateBoundedMetadata(relationship.Metadata(), limits); err != nil {
			return err
		}
	}
	for _, property := range version.Properties() {
		if err := validateBoundedMetadata(property.Metadata(), limits); err != nil {
			return err
		}
	}
	return nil
}

func validateBoundedMetadata(metadata shoal.Metadata, limits ExtractionLimits) error {
	if uint64(len(metadata)) > uint64(limits.MaxMetadataEntries) {
		return invalid("metadata entries exceed extraction limit")
	}
	var size uint64
	for key, value := range metadata {
		size += uint64(len(key)) + uint64(len(value))
		if size > uint64(limits.MaxMetadataBytes) {
			return invalid("metadata bytes exceed extraction limit")
		}
	}
	return nil
}

func cloneAssertions(values []Assertion) []Assertion {
	cloned := make([]Assertion, len(values))
	for index, value := range values {
		cloned[index] = value.clone()
	}
	return cloned
}

func cloneProposals(values []GovernedProposal) []GovernedProposal {
	cloned := make([]GovernedProposal, len(values))
	for index, value := range values {
		cloned[index] = value.clone()
	}
	return cloned
}

// Extractor is implemented by provider adapters without exposing provider SDK
// values. Implementations must not retain or mutate requests and must return
// results that pass ValidateFor. Compiling or persisting a validated result is
// a separate operation outside this interface.
type Extractor interface {
	Extract(context.Context, ExtractionRequest) (ExtractionResult, error)
}
