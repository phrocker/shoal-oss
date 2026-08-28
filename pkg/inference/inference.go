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

// Package inference defines provider-neutral contracts for grounded
// generation. It does not define model transports, prompt execution, storage,
// or orchestration.
package inference

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	MaxQueryBytes               = 16 * 1024
	MaxEvidenceAnchors          = 1024
	MaxQuoteBytes               = 64 * 1024
	MaxPathNodes                = 1024
	MaxPathEdges                = 1023
	MaxContextPackBytes         = 8 * 1024 * 1024
	MaxClaims                   = 4096
	MaxIssues                   = 4096
	MaxEvidenceRefsPerOutcome   = 256
	MaxClaimValueBytes          = 1024 * 1024
	MaxProvenanceParameters     = 128
	MaxProvenanceParameterBytes = 64 * 1024
	MaxIssueInputBytes          = 64 * 1024
	MaxIssueReasonBytes         = 16 * 1024
	MaxInferenceResultBytes     = 16 * 1024 * 1024
)

// AnchorKind discriminates exact document evidence from graph-native
// explanations.
type AnchorKind string

const (
	AnchorDocument AnchorKind = "document"
	AnchorGraph    AnchorKind = "graph"
)

// EvidenceAnchor is one immutable, exact evidence location. Exactly one of a
// document citation or graph path is present.
type EvidenceAnchor struct {
	id       shoal.ID
	kind     AnchorKind
	citation document.Citation
	quote    string
	path     graph.Path
}

// NewDocumentAnchor creates an exact citation-backed anchor. Quote bytes must
// have the same length as the citation's half-open source range.
func NewDocumentAnchor(citation document.Citation, quote string) (EvidenceAnchor, error) {
	anchor := EvidenceAnchor{
		kind:     AnchorDocument,
		citation: citation,
		quote:    quote,
	}
	id, err := anchorID(anchor)
	if err != nil {
		return EvidenceAnchor{}, err
	}
	anchor.id = id
	return anchor, nil
}

// NewGraphAnchor creates a graph-native anchor without a placeholder
// document citation.
func NewGraphAnchor(path graph.Path) (EvidenceAnchor, error) {
	anchor := EvidenceAnchor{
		kind: AnchorGraph,
		path: canonicalizePath(path),
	}
	id, err := anchorID(anchor)
	if err != nil {
		return EvidenceAnchor{}, err
	}
	anchor.id = id
	return anchor, nil
}

// Validate checks the discriminator, active variant, nested contract, public
// bounds, and content-derived identity.
func (a EvidenceAnchor) Validate() error {
	if err := shoal.ValidateRequiredID("evidence anchor ID", a.id); err != nil {
		return err
	}
	expected, err := anchorID(a)
	if err != nil {
		return err
	}
	if expected != a.id {
		return invalid("evidence anchor ID is not canonical")
	}
	return nil
}

func (a EvidenceAnchor) ID() shoal.ID { return a.id }

func (a EvidenceAnchor) Kind() AnchorKind { return a.kind }

// Document returns the exact citation and quote for a document anchor.
func (a EvidenceAnchor) Document() (document.Citation, string, bool) {
	return a.citation, a.quote, a.kind == AnchorDocument
}

// Path returns an independently owned graph path for a graph anchor.
func (a EvidenceAnchor) Path() (graph.Path, bool) {
	return clonePath(a.path), a.kind == AnchorGraph
}

func (a EvidenceAnchor) clone() EvidenceAnchor {
	a.path = clonePath(a.path)
	return a
}

func anchorID(anchor EvidenceAnchor) (shoal.ID, error) {
	switch anchor.kind {
	case AnchorDocument:
		if pathPresent(anchor.path) {
			return "", invalid("document evidence anchor cannot contain a graph path")
		}
		if err := validateCitation(anchor.citation); err != nil {
			return "", err
		}
		if !utf8.ValidString(anchor.quote) {
			return "", invalid("evidence quote must be valid UTF-8")
		}
		if len(anchor.quote) == 0 {
			return "", invalid("document evidence anchor requires a quote")
		}
		if len(anchor.quote) > MaxQuoteBytes {
			return "", invalid("evidence quote exceeds the public byte bound")
		}
		rangeBytes := anchor.citation.Range.End.Offset - anchor.citation.Range.Start.Offset
		if rangeBytes <= 0 || rangeBytes != int64(len(anchor.quote)) {
			return "", invalid("evidence quote length does not match citation range")
		}
		return deriveID(
			"evidence-anchor",
			string(AnchorDocument),
			canonicalCitation(anchor.citation),
			anchor.quote,
		), nil
	case AnchorGraph:
		if citationPresent(anchor.citation) || anchor.quote != "" {
			return "", invalid("graph evidence anchor cannot contain a citation or quote")
		}
		if err := validatePath(anchor.path); err != nil {
			return "", err
		}
		return deriveID(
			"evidence-anchor",
			string(AnchorGraph),
			canonicalPath(anchor.path),
		), nil
	default:
		return "", invalid("evidence anchor requires exactly one variant")
	}
}

// OntologyIdentity identifies an immutable ontology schema snapshot without
// embedding its definitions in a context pack.
type OntologyIdentity struct {
	schemaID  shoal.ID
	versionID shoal.ID
}

// NewOntologyIdentity extracts the schema and version identities from a
// validated ontology version.
func NewOntologyIdentity(version ontology.OntologyVersion) (OntologyIdentity, error) {
	if err := version.Validate(); err != nil {
		return OntologyIdentity{}, err
	}
	return NewOntologyIdentityFromIDs(version.Schema().ID(), version.ID())
}

// NewOntologyIdentityFromIDs creates an ontology identity when the caller
// already has validated schema and version identifiers.
func NewOntologyIdentityFromIDs(
	schemaID, versionID shoal.ID,
) (OntologyIdentity, error) {
	identity := OntologyIdentity{schemaID: schemaID, versionID: versionID}
	if err := identity.Validate(); err != nil {
		return OntologyIdentity{}, err
	}
	return identity, nil
}

func (i OntologyIdentity) Validate() error {
	if err := ontology.ValidateID(i.schemaID); err != nil {
		return err
	}
	if ontology.IDNamespace(i.schemaID) != "schema" {
		return invalid("ontology schema ID has an unexpected namespace")
	}
	if err := ontology.ValidateID(i.versionID); err != nil {
		return err
	}
	if ontology.IDNamespace(i.versionID) != "ontology-version" {
		return invalid("ontology version ID has an unexpected namespace")
	}
	return nil
}

func (i OntologyIdentity) SchemaID() shoal.ID  { return i.schemaID }
func (i OntologyIdentity) VersionID() shoal.ID { return i.versionID }

// SnapshotPin identifies the exact logical knowledge snapshot used to build a
// context pack.
type SnapshotPin struct {
	id   shoal.ID
	asOf time.Time
}

func NewSnapshotPin(id shoal.ID, asOf time.Time) (SnapshotPin, error) {
	pin := SnapshotPin{id: id, asOf: normalizeTime(asOf)}
	if err := pin.Validate(); err != nil {
		return SnapshotPin{}, err
	}
	return pin, nil
}

func (p SnapshotPin) Validate() error {
	if err := shoal.ValidateRequiredID("snapshot pin ID", p.id); err != nil {
		return err
	}
	return validateTime("snapshot pin time", p.asOf)
}

func (p SnapshotPin) ID() shoal.ID    { return p.id }
func (p SnapshotPin) AsOf() time.Time { return p.asOf }

// AuthPin identifies the exact authorized projection used to assemble a
// context pack without exposing credentials or grants.
type AuthPin struct {
	fingerprint shoal.ID
	expiresAt   time.Time
}

func NewAuthPin(fingerprint shoal.ID, expiresAt time.Time) (AuthPin, error) {
	pin := AuthPin{fingerprint: fingerprint, expiresAt: normalizeTime(expiresAt)}
	if err := pin.Validate(); err != nil {
		return AuthPin{}, err
	}
	return pin, nil
}

func (p AuthPin) Validate() error {
	if err := shoal.ValidateRequiredID("authorization fingerprint", p.fingerprint); err != nil {
		return err
	}
	return validateTime("authorization expiry", p.expiresAt)
}

func (p AuthPin) Fingerprint() shoal.ID { return p.fingerprint }
func (p AuthPin) ExpiresAt() time.Time  { return p.expiresAt }

// ContextPack is an immutable, canonical set of evidence and execution pins
// supplied to a Generator.
type ContextPack struct {
	id       shoal.ID
	query    string
	evidence []EvidenceAnchor
	ontology *OntologyIdentity
	snapshot SnapshotPin
	auth     AuthPin
	metadata shoal.Metadata
}

func NewContextPack(
	query string,
	evidence []EvidenceAnchor,
	ontologyIdentity *OntologyIdentity,
	snapshot SnapshotPin,
	auth AuthPin,
	metadata shoal.Metadata,
) (ContextPack, error) {
	normalized := ContextPack{
		query:    normalizeQuery(query),
		evidence: cloneAnchors(evidence),
		snapshot: snapshot,
		auth:     auth,
		metadata: cloneMetadata(metadata),
	}
	if ontologyIdentity != nil {
		copied := *ontologyIdentity
		normalized.ontology = &copied
	}
	sort.Slice(normalized.evidence, func(i, j int) bool {
		return shoal.CompareID(normalized.evidence[i].ID(), normalized.evidence[j].ID()) < 0
	})
	id, err := contextPackID(normalized)
	if err != nil {
		return ContextPack{}, err
	}
	normalized.id = id
	if err := normalized.Validate(); err != nil {
		return ContextPack{}, err
	}
	return normalized, nil
}

func (p ContextPack) Validate() error {
	if err := shoal.ValidateRequiredID("context pack ID", p.id); err != nil {
		return err
	}
	expected, err := contextPackID(p)
	if err != nil {
		return err
	}
	if expected != p.id {
		return invalid("context pack ID is not canonical")
	}
	return nil
}

func (p ContextPack) ID() shoal.ID               { return p.id }
func (p ContextPack) Query() string              { return p.query }
func (p ContextPack) Evidence() []EvidenceAnchor { return cloneAnchors(p.evidence) }
func (p ContextPack) Snapshot() SnapshotPin      { return p.snapshot }
func (p ContextPack) Authorization() AuthPin     { return p.auth }
func (p ContextPack) Metadata() shoal.Metadata   { return cloneMetadata(p.metadata) }

func (p ContextPack) Ontology() (OntologyIdentity, bool) {
	if p.ontology == nil {
		return OntologyIdentity{}, false
	}
	return *p.ontology, true
}

func contextPackID(pack ContextPack) (shoal.ID, error) {
	if !utf8.ValidString(pack.query) {
		return "", invalid("context query must be valid UTF-8")
	}
	if strings.TrimSpace(pack.query) == "" {
		return "", invalid("context query is required")
	}
	if pack.query != normalizeQuery(pack.query) {
		return "", invalid("context query is not normalized")
	}
	if len(pack.query) > MaxQueryBytes {
		return "", invalid("context query exceeds the public byte bound")
	}
	if len(pack.evidence) == 0 {
		return "", invalid("context pack requires evidence anchors")
	}
	if len(pack.evidence) > MaxEvidenceAnchors {
		return "", invalid("context pack has too many evidence anchors")
	}
	anchorIDs := make([]string, len(pack.evidence))
	for index, anchor := range pack.evidence {
		if err := anchor.Validate(); err != nil {
			return "", fmt.Errorf("context evidence: %w", err)
		}
		if index > 0 && shoal.CompareID(pack.evidence[index-1].ID(), anchor.ID()) >= 0 {
			return "", invalid("context evidence must be unique and canonically ordered")
		}
		anchorIDs[index] = string(anchor.ID())
	}
	ontologyPart := ""
	if pack.ontology != nil {
		if err := pack.ontology.Validate(); err != nil {
			return "", err
		}
		ontologyPart = canonicalParts(
			string(pack.ontology.SchemaID()), string(pack.ontology.VersionID()))
	}
	if err := pack.snapshot.Validate(); err != nil {
		return "", err
	}
	if err := pack.auth.Validate(); err != nil {
		return "", err
	}
	if pack.auth.ExpiresAt().Before(pack.snapshot.AsOf()) {
		return "", invalid("authorization expires before the pinned snapshot")
	}
	if err := validateMetadata("context metadata", pack.metadata); err != nil {
		return "", err
	}
	payloadBytes := len(pack.query) + metadataBytes(pack.metadata)
	for _, anchor := range pack.evidence {
		payloadBytes += anchorPayloadBytes(anchor)
		if payloadBytes > MaxContextPackBytes {
			return "", invalid("context pack exceeds the public byte bound")
		}
	}
	canonical := canonicalParts(
		pack.query,
		canonicalParts(anchorIDs...),
		ontologyPart,
		string(pack.snapshot.ID()),
		canonicalTime(pack.snapshot.AsOf()),
		string(pack.auth.Fingerprint()),
		canonicalTime(pack.auth.ExpiresAt()),
		canonicalMetadata(pack.metadata),
	)
	if payloadBytes+len(canonical) > MaxContextPackBytes {
		return "", invalid("context pack exceeds the public byte bound")
	}
	return deriveID("context-pack", canonical), nil
}

// ModelProvenance identifies a model invocation without carrying credentials
// or raw provider requests.
type ModelProvenance struct {
	provider   string
	model      string
	version    string
	parameters shoal.Metadata
	seed       int64
	hasSeed    bool
}

func NewModelProvenance(
	provider, model, version string,
	parameters shoal.Metadata,
	seed *int64,
) (ModelProvenance, error) {
	provenance := ModelProvenance{
		provider:   provider,
		model:      model,
		version:    version,
		parameters: cloneMetadata(parameters),
	}
	if seed != nil {
		provenance.seed = *seed
		provenance.hasSeed = true
	}
	if err := provenance.Validate(); err != nil {
		return ModelProvenance{}, err
	}
	return provenance, nil
}

func (p ModelProvenance) Validate() error {
	for name, value := range map[string]string{
		"model provider": p.provider,
		"model name":     p.model,
	} {
		if err := validateRequiredString(name, value, shoal.MaxSemanticStringBytes); err != nil {
			return err
		}
	}
	if err := validateOptionalString(
		"model version", p.version, shoal.MaxSemanticStringBytes,
	); err != nil {
		return err
	}
	if len(p.parameters) > MaxProvenanceParameters {
		return invalid("model parameters exceed the public entry bound")
	}
	if err := validateMetadata("model parameters", p.parameters); err != nil {
		return err
	}
	if metadataBytes(p.parameters) > MaxProvenanceParameterBytes {
		return invalid("model parameters exceed the public byte bound")
	}
	return nil
}

func (p ModelProvenance) Provider() string           { return p.provider }
func (p ModelProvenance) Model() string              { return p.model }
func (p ModelProvenance) Version() string            { return p.version }
func (p ModelProvenance) Parameters() shoal.Metadata { return cloneMetadata(p.parameters) }
func (p ModelProvenance) Seed() (int64, bool)        { return p.seed, p.hasSeed }

func (p ModelProvenance) clone() ModelProvenance {
	p.parameters = cloneMetadata(p.parameters)
	return p
}

// PromptProvenance identifies the bounded prompt template used for generation.
// Prompt text is deliberately absent.
type PromptProvenance struct {
	templateID string
	version    string
	hash       string
}

func NewPromptProvenance(templateID, version, hash string) (PromptProvenance, error) {
	provenance := PromptProvenance{
		templateID: templateID,
		version:    version,
		hash:       hash,
	}
	if err := provenance.Validate(); err != nil {
		return PromptProvenance{}, err
	}
	return provenance, nil
}

func (p PromptProvenance) Validate() error {
	for name, value := range map[string]string{
		"prompt template ID": p.templateID,
		"prompt version":     p.version,
	} {
		if err := validateRequiredString(name, value, shoal.MaxSemanticStringBytes); err != nil {
			return err
		}
	}
	return validateSHA256("prompt hash", p.hash)
}

func (p PromptProvenance) TemplateID() string { return p.templateID }
func (p PromptProvenance) Version() string    { return p.version }
func (p PromptProvenance) Hash() string       { return p.hash }

// ClaimStatus distinguishes source-observed claims from generated inferences.
type ClaimStatus string

const (
	ClaimObserved ClaimStatus = "observed"
	ClaimInferred ClaimStatus = "inferred"
)

// Claim is one immutable, grounded subject-predicate-object assertion.
type Claim struct {
	id          shoal.ID
	subject     shoal.ID
	predicate   shoal.ID
	object      ontology.Value
	confidence  shoal.Score
	evidenceIDs []shoal.ID
	status      ClaimStatus
	model       ModelProvenance
	prompt      PromptProvenance
	metadata    shoal.Metadata
}

func NewClaim(
	subject, predicate shoal.ID,
	object ontology.Value,
	confidence shoal.Score,
	evidenceIDs []shoal.ID,
	status ClaimStatus,
	model ModelProvenance,
	prompt PromptProvenance,
	metadata shoal.Metadata,
) (Claim, error) {
	claim := Claim{
		subject:     subject,
		predicate:   predicate,
		object:      object,
		confidence:  confidence,
		evidenceIDs: append([]shoal.ID(nil), evidenceIDs...),
		status:      status,
		model:       model.clone(),
		prompt:      prompt,
		metadata:    cloneMetadata(metadata),
	}
	sort.Slice(claim.evidenceIDs, func(i, j int) bool {
		return shoal.CompareID(claim.evidenceIDs[i], claim.evidenceIDs[j]) < 0
	})
	id, err := claimID(claim)
	if err != nil {
		return Claim{}, err
	}
	claim.id = id
	if err := claim.Validate(); err != nil {
		return Claim{}, err
	}
	return claim, nil
}

func (c Claim) Validate() error {
	if err := shoal.ValidateRequiredID("claim ID", c.id); err != nil {
		return err
	}
	expected, err := claimID(c)
	if err != nil {
		return err
	}
	if expected != c.id {
		return invalid("claim ID is not canonical")
	}
	return nil
}

func (c Claim) ID() shoal.ID                       { return c.id }
func (c Claim) Subject() shoal.ID                  { return c.subject }
func (c Claim) Predicate() shoal.ID                { return c.predicate }
func (c Claim) Object() ontology.Value             { return c.object }
func (c Claim) Confidence() shoal.Score            { return c.confidence }
func (c Claim) EvidenceIDs() []shoal.ID            { return append([]shoal.ID(nil), c.evidenceIDs...) }
func (c Claim) Status() ClaimStatus                { return c.status }
func (c Claim) ModelProvenance() ModelProvenance   { return c.model.clone() }
func (c Claim) PromptProvenance() PromptProvenance { return c.prompt }
func (c Claim) Metadata() shoal.Metadata           { return cloneMetadata(c.metadata) }

func (c Claim) clone() Claim {
	c.evidenceIDs = append([]shoal.ID(nil), c.evidenceIDs...)
	c.model = c.model.clone()
	c.metadata = cloneMetadata(c.metadata)
	return c
}

func claimID(claim Claim) (shoal.ID, error) {
	if err := shoal.ValidateRequiredID("claim subject", claim.subject); err != nil {
		return "", err
	}
	if err := shoal.ValidateRequiredID("claim predicate", claim.predicate); err != nil {
		return "", err
	}
	if err := claim.object.Validate(); err != nil {
		return "", fmt.Errorf("claim object: %w", err)
	}
	if len(canonicalValue(claim.object)) > MaxClaimValueBytes {
		return "", invalid("claim object exceeds the public byte bound")
	}
	if err := shoal.ValidateFiniteScore("claim confidence", claim.confidence); err != nil {
		return "", err
	}
	if claim.confidence < 0 || claim.confidence > 1 {
		return "", invalid("claim confidence must be between zero and one")
	}
	switch claim.status {
	case ClaimObserved, ClaimInferred:
	default:
		return "", invalid("claim status is invalid")
	}
	if len(claim.evidenceIDs) == 0 {
		return "", invalid("claim requires evidence references")
	}
	if len(claim.evidenceIDs) > MaxEvidenceRefsPerOutcome {
		return "", invalid("claim has too many evidence references")
	}
	evidence := make([]string, len(claim.evidenceIDs))
	for index, id := range claim.evidenceIDs {
		if err := shoal.ValidateRequiredID("claim evidence ID", id); err != nil {
			return "", err
		}
		if index > 0 && shoal.CompareID(claim.evidenceIDs[index-1], id) >= 0 {
			return "", invalid("claim evidence must be unique and canonically ordered")
		}
		evidence[index] = string(id)
	}
	if err := claim.model.Validate(); err != nil {
		return "", err
	}
	if err := claim.prompt.Validate(); err != nil {
		return "", err
	}
	if err := validateMetadata("claim metadata", claim.metadata); err != nil {
		return "", err
	}
	return deriveID(
		"claim",
		string(claim.subject),
		string(claim.predicate),
		canonicalValue(claim.object),
		canonicalScore(claim.confidence),
		canonicalParts(evidence...),
		string(claim.status),
		canonicalModel(claim.model),
		canonicalPrompt(claim.prompt),
		canonicalMetadata(claim.metadata),
	), nil
}

// IssueKind distinguishes an unresolved request from an unsupported output.
type IssueKind string

const (
	IssueUnresolved  IssueKind = "unresolved"
	IssueUnsupported IssueKind = "unsupported"
)

// Issue records a bounded, grounded outcome that could not become a claim.
type Issue struct {
	id          shoal.ID
	kind        IssueKind
	input       string
	reason      string
	evidenceIDs []shoal.ID
}

func NewIssue(
	kind IssueKind, input, reason string, evidenceIDs []shoal.ID,
) (Issue, error) {
	issue := Issue{
		kind:        kind,
		input:       input,
		reason:      reason,
		evidenceIDs: append([]shoal.ID(nil), evidenceIDs...),
	}
	sort.Slice(issue.evidenceIDs, func(i, j int) bool {
		return shoal.CompareID(issue.evidenceIDs[i], issue.evidenceIDs[j]) < 0
	})
	id, err := issueID(issue)
	if err != nil {
		return Issue{}, err
	}
	issue.id = id
	return issue, nil
}

func (i Issue) Validate() error {
	if err := shoal.ValidateRequiredID("inference issue ID", i.id); err != nil {
		return err
	}
	expected, err := issueID(i)
	if err != nil {
		return err
	}
	if expected != i.id {
		return invalid("inference issue ID is not canonical")
	}
	return nil
}

func (i Issue) ID() shoal.ID            { return i.id }
func (i Issue) Kind() IssueKind         { return i.kind }
func (i Issue) Input() string           { return i.input }
func (i Issue) Reason() string          { return i.reason }
func (i Issue) EvidenceIDs() []shoal.ID { return append([]shoal.ID(nil), i.evidenceIDs...) }

func issueID(issue Issue) (shoal.ID, error) {
	switch issue.kind {
	case IssueUnresolved, IssueUnsupported:
	default:
		return "", invalid("inference issue kind is invalid")
	}
	if err := validateRequiredString("inference issue input", issue.input, MaxIssueInputBytes); err != nil {
		return "", err
	}
	if err := validateRequiredString("inference issue reason", issue.reason, MaxIssueReasonBytes); err != nil {
		return "", err
	}
	if len(issue.evidenceIDs) > MaxEvidenceRefsPerOutcome {
		return "", invalid("inference issue has too many evidence references")
	}
	evidence := make([]string, len(issue.evidenceIDs))
	for index, id := range issue.evidenceIDs {
		if err := shoal.ValidateRequiredID("inference issue evidence ID", id); err != nil {
			return "", err
		}
		if index > 0 && shoal.CompareID(issue.evidenceIDs[index-1], id) >= 0 {
			return "", invalid("inference issue evidence must be unique and canonically ordered")
		}
		evidence[index] = string(id)
	}
	return deriveID(
		"inference-issue",
		string(issue.kind),
		issue.input,
		issue.reason,
		canonicalParts(evidence...),
	), nil
}

// InferenceResult is the immutable output associated with one context pack.
type InferenceResult struct {
	id            shoal.ID
	contextPackID shoal.ID
	claims        []Claim
	issues        []Issue
	generatedAt   time.Time
	metadata      shoal.Metadata
}

func NewInferenceResult(
	pack ContextPack,
	claims []Claim,
	issues []Issue,
	generatedAt time.Time,
	metadata shoal.Metadata,
) (InferenceResult, error) {
	if err := pack.Validate(); err != nil {
		return InferenceResult{}, err
	}
	result := InferenceResult{
		contextPackID: pack.ID(),
		claims:        cloneClaims(claims),
		issues:        cloneIssues(issues),
		generatedAt:   normalizeTime(generatedAt),
		metadata:      cloneMetadata(metadata),
	}
	sort.Slice(result.claims, func(i, j int) bool {
		return shoal.CompareID(result.claims[i].ID(), result.claims[j].ID()) < 0
	})
	sort.Slice(result.issues, func(i, j int) bool {
		return shoal.CompareID(result.issues[i].ID(), result.issues[j].ID()) < 0
	})
	id, err := inferenceResultID(result)
	if err != nil {
		return InferenceResult{}, err
	}
	result.id = id
	if err := result.ValidateFor(pack); err != nil {
		return InferenceResult{}, err
	}
	return result, nil
}

func (r InferenceResult) Validate() error {
	if err := shoal.ValidateRequiredID("inference result ID", r.id); err != nil {
		return err
	}
	expected, err := inferenceResultID(r)
	if err != nil {
		return err
	}
	if expected != r.id {
		return invalid("inference result ID is not canonical")
	}
	return nil
}

// ValidateFor additionally verifies that every outcome references evidence in
// the supplied context pack.
func (r InferenceResult) ValidateFor(pack ContextPack) error {
	if err := pack.Validate(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if r.contextPackID != pack.ID() {
		return invalid("inference result does not match context pack")
	}
	available := make(map[shoal.ID]struct{}, len(pack.evidence))
	for _, anchor := range pack.evidence {
		available[anchor.ID()] = struct{}{}
	}
	for _, claim := range r.claims {
		for _, id := range claim.evidenceIDs {
			if _, ok := available[id]; !ok {
				return invalid("claim references evidence outside the context pack")
			}
		}
	}
	for _, issue := range r.issues {
		for _, id := range issue.evidenceIDs {
			if _, ok := available[id]; !ok {
				return invalid("inference issue references evidence outside the context pack")
			}
		}
	}
	return nil
}

func (r InferenceResult) ID() shoal.ID             { return r.id }
func (r InferenceResult) ContextPackID() shoal.ID  { return r.contextPackID }
func (r InferenceResult) Claims() []Claim          { return cloneClaims(r.claims) }
func (r InferenceResult) GeneratedAt() time.Time   { return r.generatedAt }
func (r InferenceResult) Metadata() shoal.Metadata { return cloneMetadata(r.metadata) }

func (r InferenceResult) Unresolved() []Issue {
	return filterIssues(r.issues, IssueUnresolved)
}

func (r InferenceResult) Unsupported() []Issue {
	return filterIssues(r.issues, IssueUnsupported)
}

func inferenceResultID(result InferenceResult) (shoal.ID, error) {
	if err := shoal.ValidateRequiredID("inference result context pack ID", result.contextPackID); err != nil {
		return "", err
	}
	if len(result.claims) == 0 && len(result.issues) == 0 {
		return "", invalid("inference result requires at least one outcome")
	}
	if len(result.claims) > MaxClaims {
		return "", invalid("inference result has too many claims")
	}
	if len(result.issues) > MaxIssues {
		return "", invalid("inference result has too many issues")
	}
	claimIDs := make([]string, len(result.claims))
	for index, claim := range result.claims {
		if err := claim.Validate(); err != nil {
			return "", err
		}
		if index > 0 && shoal.CompareID(result.claims[index-1].ID(), claim.ID()) >= 0 {
			return "", invalid("inference claims must be unique and canonically ordered")
		}
		claimIDs[index] = string(claim.ID())
	}
	issueIDs := make([]string, len(result.issues))
	for index, issue := range result.issues {
		if err := issue.Validate(); err != nil {
			return "", err
		}
		if index > 0 && shoal.CompareID(result.issues[index-1].ID(), issue.ID()) >= 0 {
			return "", invalid("inference issues must be unique and canonically ordered")
		}
		issueIDs[index] = string(issue.ID())
	}
	if err := validateTime("inference generation time", result.generatedAt); err != nil {
		return "", err
	}
	if err := validateMetadata("inference result metadata", result.metadata); err != nil {
		return "", err
	}
	payloadBytes := metadataBytes(result.metadata)
	for _, claim := range result.claims {
		payloadBytes += claimPayloadBytes(claim)
		if payloadBytes > MaxInferenceResultBytes {
			return "", invalid("inference result exceeds the public byte bound")
		}
	}
	for _, issue := range result.issues {
		payloadBytes += issuePayloadBytes(issue)
		if payloadBytes > MaxInferenceResultBytes {
			return "", invalid("inference result exceeds the public byte bound")
		}
	}
	canonical := canonicalParts(
		string(result.contextPackID),
		canonicalParts(claimIDs...),
		canonicalParts(issueIDs...),
		canonicalTime(result.generatedAt),
		canonicalMetadata(result.metadata),
	)
	if payloadBytes+len(canonical) > MaxInferenceResultBytes {
		return "", invalid("inference result exceeds the public byte bound")
	}
	return deriveID("inference-result", canonical), nil
}

// Generator is the high-level provider-neutral grounded generation boundary.
// Text completion and embedding provider interfaces belong outside this
// package.
type Generator interface {
	Generate(context.Context, ContextPack) (InferenceResult, error)
}

func invalid(message string) error {
	return shoal.NewError(shoal.ErrorInvalidArgument, message)
}
