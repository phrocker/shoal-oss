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

// Package retrieval defines transport-neutral knowledge retrieval contracts.
package retrieval

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	DefaultTopK         uint32 = 20
	MaxTopK             uint32 = 1000
	MaxQueryBytes              = 16 * 1024
	MaxScopeIDs                = 10_000
	MaxModes                   = 4
	MaxExplanationBytes        = 16 * 1024

	// MaxBoundScopes bounds the (SourceID, PolicyID) pairs one SourceBound
	// may name, and MaxBoundComponentBytes each identity in a pair. The byte
	// bound matches the authorization policy component bound: a longer
	// identity names no policy, so it could only ever match nothing.
	MaxBoundScopes         = 64
	MaxBoundComponentBytes = 128

	// MaxIdempotencyTokenBytes is reserved for future additive generic
	// mutation APIs. Retrieval requests do not carry such a token.
	MaxIdempotencyTokenBytes = 128
)

// Mode selects a retrieval strategy. Multiple modes request a hybrid plan.
type Mode string

const (
	ModeLexical Mode = "lexical"
	ModeVector  Mode = "vector"
	ModeTree    Mode = "tree"
	ModeGraph   Mode = "graph"
)

// Scope bounds retrieval to known documents or graph nodes. IDs are ORed
// within each list. When both lists are nonempty, candidates must satisfy both
// dimensions through a canonical document/graph association.
type Scope struct {
	DocumentIDs []shoal.ID
	NodeIDs     []shoal.ID
}

// BoundScope is one (SourceID, PolicyID) pair a SourceBound admits. It has
// the shape a fleet.Descriptor declares a scope in, so an invocation's own
// scope can be handed to retrieval unchanged.
type BoundScope struct {
	SourceID []byte
	PolicyID []byte
}

// SourceBound confines retrieval to content governed by its scopes (#560).
//
// It is a filter and never a grant. An authorizing retriever applies it after
// its own authorization of the caller, so content the caller may not read is
// withheld exactly as it is without a bound, and content the caller may read
// but the bound does not cover is withheld as if it were not there. A bound
// naming a scope the caller holds no grant on therefore reaches nothing, and
// says nothing about what that scope contains.
//
// Content is within a bound when every policy term of its access rule names a
// scope of the bound, or is a label policy on the source of a scope it also
// names. Labels only narrow their source, so a labelled document on a bound
// source is within the bound; a document on another source is not, whatever
// labels the caller holds.
//
// A retriever that cannot honour a bound must refuse a request carrying one
// rather than ignore it: an ignored bound is a silent widening. Only the
// authorized Explorer client honours one today; the base engine, the gRPC
// transport and the remote web client refuse.
//
// A nil *SourceBound on a Request means no bound. A non-nil bound with no
// scopes is a bound that admits nothing, and retrieval under it is empty.
type SourceBound struct {
	Scopes []BoundScope
}

// Clone returns an independently owned copy.
func (b *SourceBound) Clone() *SourceBound {
	if b == nil {
		return nil
	}
	cloned := &SourceBound{Scopes: make([]BoundScope, 0, len(b.Scopes))}
	for _, scope := range b.Scopes {
		cloned.Scopes = append(cloned.Scopes, BoundScope{
			SourceID: append([]byte(nil), scope.SourceID...),
			PolicyID: append([]byte(nil), scope.PolicyID...),
		})
	}
	return cloned
}

// Normalize returns an independently owned bound with duplicate scopes
// removed and the rest in canonical (SourceID, PolicyID) order, or an error
// when the bound is malformed. A nil bound normalizes to nil.
func (b *SourceBound) Normalize() (*SourceBound, error) {
	if b == nil {
		return nil, nil
	}
	if len(b.Scopes) > MaxBoundScopes {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "retrieval source bound has too many scopes")
	}
	for _, scope := range b.Scopes {
		if err := validateBoundComponent(
			"retrieval source bound source ID", scope.SourceID); err != nil {
			return nil, err
		}
		if err := validateBoundComponent(
			"retrieval source bound policy ID", scope.PolicyID); err != nil {
			return nil, err
		}
	}
	cloned := b.Clone()
	sort.Slice(cloned.Scopes, func(i, j int) bool {
		return compareBoundScope(cloned.Scopes[i], cloned.Scopes[j]) < 0
	})
	unique := cloned.Scopes[:0]
	for _, scope := range cloned.Scopes {
		if len(unique) > 0 &&
			compareBoundScope(unique[len(unique)-1], scope) == 0 {
			continue
		}
		unique = append(unique, scope)
	}
	cloned.Scopes = unique
	return cloned, nil
}

// Admits reports whether the bound names the exact (source, policy) pair. A
// nil bound admits every pair.
func (b *SourceBound) Admits(sourceID, policyID []byte) bool {
	if b == nil {
		return true
	}
	for _, scope := range b.Scopes {
		if bytes.Equal(scope.SourceID, sourceID) &&
			bytes.Equal(scope.PolicyID, policyID) {
			return true
		}
	}
	return false
}

// AdmitsSource reports whether any scope of the bound names sourceID. A nil
// bound admits every source.
func (b *SourceBound) AdmitsSource(sourceID []byte) bool {
	if b == nil {
		return true
	}
	for _, scope := range b.Scopes {
		if bytes.Equal(scope.SourceID, sourceID) {
			return true
		}
	}
	return false
}

// IntersectBounds returns the bound admitting exactly what both admit. Nil is
// the unbounded identity, so intersecting with nil returns the other bound;
// two bounds intersect to the scopes they share, which may be none. The
// result is independently owned.
func IntersectBounds(left, right *SourceBound) *SourceBound {
	switch {
	case left == nil:
		return right.Clone()
	case right == nil:
		return left.Clone()
	}
	result := &SourceBound{Scopes: []BoundScope{}}
	for _, scope := range left.Scopes {
		if right.Admits(scope.SourceID, scope.PolicyID) &&
			!result.Admits(scope.SourceID, scope.PolicyID) {
			result.Scopes = append(result.Scopes, BoundScope{
				SourceID: append([]byte(nil), scope.SourceID...),
				PolicyID: append([]byte(nil), scope.PolicyID...),
			})
		}
	}
	return result
}

func compareBoundScope(left, right BoundScope) int {
	if compared := bytes.Compare(left.SourceID, right.SourceID); compared != 0 {
		return compared
	}
	return bytes.Compare(left.PolicyID, right.PolicyID)
}

func validateBoundComponent(name string, value []byte) error {
	if len(value) == 0 {
		return shoal.NewError(shoal.ErrorInvalidArgument, name+" is required")
	}
	if len(value) > MaxBoundComponentBytes {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, name+" exceeds the byte bound")
	}
	return nil
}

// ErrBoundUnsupported is the refusal a retriever that cannot honour a
// SourceBound returns for a request carrying one.
func ErrBoundUnsupported() error {
	return shoal.NewError(
		shoal.ErrorUnavailable,
		"source-bounded retrieval requires an authorizing retriever that honours the bound")
}

// Request describes one coarse knowledge retrieval operation.
type Request struct {
	Text    string
	TopK    uint32
	Modes   []Mode
	Scope   Scope
	AsOf    time.Time
	Explain bool
	// Bound optionally confines retrieval to content governed by named
	// (SourceID, PolicyID) scopes. Nil is today's unbounded retrieval. See
	// SourceBound: it only narrows, and a retriever that cannot honour it
	// refuses the request.
	Bound *SourceBound
}

// Normalize returns an independently owned, canonical request and never
// mutates caller slices. It applies public defaults, removes duplicate modes
// and scope IDs while preserving first occurrence, and normalizes AsOf to UTC.
func (r Request) Normalize() (Request, error) {
	normalized := r
	if normalized.TopK == 0 {
		normalized.TopK = DefaultTopK
	}
	if len(r.Modes) == 0 {
		normalized.Modes = []Mode{ModeLexical}
	} else {
		normalized.Modes = deduplicateModes(r.Modes)
	}
	normalized.Scope = Scope{
		DocumentIDs: deduplicateIDs(r.Scope.DocumentIDs),
		NodeIDs:     deduplicateIDs(r.Scope.NodeIDs),
	}
	if !r.AsOf.IsZero() {
		normalized.AsOf = r.AsOf.UTC()
	}
	bound, err := r.Bound.Normalize()
	if err != nil {
		return Request{}, err
	}
	normalized.Bound = bound
	if err := normalized.validateNormalized(); err != nil {
		return Request{}, err
	}
	return normalized, nil
}

// Validate checks transport-independent request invariants after public
// normalization.
func (r Request) Validate() error {
	_, err := r.Normalize()
	return err
}

func (r Request) validateNormalized() error {
	if !utf8.ValidString(r.Text) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "retrieval text must be valid UTF-8")
	}
	if strings.TrimSpace(r.Text) == "" {
		return shoal.NewError(shoal.ErrorInvalidArgument, "retrieval text is required")
	}
	if len(r.Text) > MaxQueryBytes {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "retrieval text exceeds the public byte bound")
	}
	if r.TopK == 0 || r.TopK > MaxTopK {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "retrieval top_k is outside the public bound")
	}
	if len(r.Modes) == 0 || len(r.Modes) > MaxModes {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "retrieval mode count is outside the public bound")
	}
	for _, mode := range r.Modes {
		if !validMode(mode) {
			return shoal.NewError(shoal.ErrorInvalidArgument, "unknown retrieval mode")
		}
	}
	if len(r.Scope.DocumentIDs)+len(r.Scope.NodeIDs) > MaxScopeIDs {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "retrieval scope has too many IDs")
	}
	for _, id := range r.Scope.DocumentIDs {
		if err := shoal.ValidateRequiredID("retrieval document ID", id); err != nil {
			return err
		}
	}
	for _, id := range r.Scope.NodeIDs {
		if err := shoal.ValidateRequiredID("retrieval node ID", id); err != nil {
			return err
		}
	}
	if !r.AsOf.IsZero() {
		year := r.AsOf.Year()
		if year < 1 || year > 9999 {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "retrieval as_of is outside the supported range")
		}
	}
	return nil
}

// ValidateSeedPlan rejects tree or graph shapes that have no bounded seed
// source under the fixed lexical, tree, then graph planner. The boolean states
// whether document-to-graph association seeds are implemented by the target.
func (r Request) ValidateSeedPlan(documentAssociationSeeds bool) error {
	normalized, err := r.Normalize()
	if err != nil {
		return err
	}
	hasLexical := normalized.HasMode(ModeLexical)
	hasVector := normalized.HasMode(ModeVector)
	hasTree := normalized.HasMode(ModeTree)
	hasGraph := normalized.HasMode(ModeGraph)

	treeSeeded := len(normalized.Scope.DocumentIDs) > 0 || hasLexical || hasVector
	if hasTree && !treeSeeded {
		return shoal.NewError(
			shoal.ErrorUnavailable,
			"tree retrieval requires document scope or earlier lexical/vector seeds",
		)
	}
	graphSeeded := len(normalized.Scope.NodeIDs) > 0 ||
		(documentAssociationSeeds && len(normalized.Scope.DocumentIDs) > 0) ||
		hasLexical || hasVector || (hasTree && treeSeeded)
	if hasGraph && !graphSeeded {
		return shoal.NewError(
			shoal.ErrorUnavailable,
			"graph retrieval requires node scope, document associations, or earlier seeds",
		)
	}
	return nil
}

// HasMode reports whether a request contains a mode.
func (r Request) HasMode(mode Mode) bool {
	for _, candidate := range r.Modes {
		if candidate == mode {
			return true
		}
	}
	return false
}

// Evidence ties a result to immutable source and, when applicable, the graph
// path used to reach it.
type Evidence struct {
	Citation document.Citation
	Quote    string
	Path     graph.Path
	Score    shoal.Score
}

// Validate checks transport-neutral evidence structure. Quote equality is a
// document-context check performed against canonical retained source bytes.
func (e Evidence) Validate() error {
	if err := e.Citation.Validate(); err != nil {
		return err
	}
	if err := shoal.ValidateFiniteScore("evidence score", e.Score); err != nil {
		return err
	}
	if pathPresent(e.Path) {
		if err := e.Path.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Explanation describes why a result was selected without exposing an
// execution engine or storage plan.
type Explanation struct {
	Modes   []Mode
	Summary string
	Scores  map[string]shoal.Score
}

// Result is one ranked, evidence-addressable retrieval result.
type Result struct {
	ID          shoal.ID
	Score       shoal.Score
	Evidence    []Evidence
	Explanation *Explanation
}

// Response is the complete, all-or-error result of one retrieval request.
type Response struct {
	RequestID         shoal.ID
	EmbeddingSpaceID  shoal.ID
	EmbeddingSpaceIDs []shoal.ID
	Results           []Result
}

// ValidateFor checks result bounds, uniqueness, finite scores, and normative
// deterministic result/evidence ordering for a normalized request.
func (r Response) ValidateFor(request Request) error {
	normalized, err := request.Normalize()
	if err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID("retrieval request ID", r.RequestID); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID(
		"retrieval embedding space ID", r.EmbeddingSpaceID,
	); err != nil {
		return err
	}
	if normalized.HasMode(ModeVector) {
		if (r.EmbeddingSpaceID == "") != (len(r.EmbeddingSpaceIDs) == 0) {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"vector retrieval embedding space aggregate and constituents "+
					"must be present together",
			)
		}
		if len(normalized.Modes) == 1 && len(r.Results) > 0 &&
			r.EmbeddingSpaceID == "" {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"vector-only retrieval results require embedding space provenance",
			)
		}
	} else if r.EmbeddingSpaceID != "" || len(r.EmbeddingSpaceIDs) > 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"non-vector retrieval response cannot carry embedding space provenance",
		)
	}
	if len(r.EmbeddingSpaceIDs) > MaxScopeIDs {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"retrieval embedding space constituents exceed the public bound",
		)
	}
	for index, id := range r.EmbeddingSpaceIDs {
		if err := shoal.ValidateRequiredID(
			"retrieval embedding space constituent ID", id,
		); err != nil {
			return err
		}
		if index > 0 &&
			shoal.CompareID(r.EmbeddingSpaceIDs[index-1], id) >= 0 {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"retrieval embedding space constituent IDs must be unique "+
					"and canonically ordered",
			)
		}
	}
	if len(r.EmbeddingSpaceIDs) > 0 {
		expected, err := EmbeddingSpaceSetID(r.EmbeddingSpaceIDs...)
		if err != nil {
			return err
		}
		if expected != r.EmbeddingSpaceID {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"retrieval embedding space set identity is not canonical",
			)
		}
	}
	if uint32(len(r.Results)) > normalized.TopK {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "retrieval response exceeds normalized top_k")
	}

	seen := make(map[shoal.ID]struct{}, len(r.Results))
	for index, result := range r.Results {
		if err := shoal.ValidateRequiredID("retrieval result ID", result.ID); err != nil {
			return err
		}
		if _, duplicate := seen[result.ID]; duplicate {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "retrieval response has duplicate result IDs")
		}
		seen[result.ID] = struct{}{}
		if err := shoal.ValidateFiniteScore("retrieval result score", result.Score); err != nil {
			return err
		}
		if index > 0 && CompareResult(r.Results[index-1], result) > 0 {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "retrieval results are not deterministically ordered")
		}
		for evidenceIndex, evidence := range result.Evidence {
			if err := evidence.Validate(); err != nil {
				return err
			}
			if evidenceIndex > 0 {
				compared := CompareEvidence(result.Evidence[evidenceIndex-1], evidence)
				if compared > 0 {
					return shoal.NewError(
						shoal.ErrorInvalidArgument,
						"retrieval evidence is not deterministically ordered",
					)
				}
				if compared == 0 {
					return shoal.NewError(
						shoal.ErrorInvalidArgument,
						"retrieval evidence has a duplicate ordering key",
					)
				}
			}
		}
		if result.Explanation != nil {
			if err := validateExplanation(*result.Explanation); err != nil {
				return err
			}
		}
	}
	return nil
}

// EmbeddingSpaceIdentityID hashes one provider-defined embedding-space
// identity into a stable opaque constituent ID.
func EmbeddingSpaceIdentityID(identity string) (shoal.ID, error) {
	if !utf8.ValidString(identity) || strings.TrimSpace(identity) == "" ||
		len(identity) > shoal.MaxSemanticStringBytes {
		return "", shoal.NewError(
			shoal.ErrorInvalidArgument,
			"embedding space identity is invalid",
		)
	}
	hash := sha256.New()
	var length [8]byte
	write := func(value string) {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	write("shoal-retrieval-embedding-space-v1")
	write(identity)
	return shoal.ID(
		"embedding-space_" + hex.EncodeToString(hash.Sum(nil)[:16]),
	), nil
}

// EmbeddingSpaceSetID returns a stable opaque identity for an exact set of
// canonical constituent embedding-space IDs.
func EmbeddingSpaceSetID(identities ...shoal.ID) (shoal.ID, error) {
	if len(identities) > MaxScopeIDs {
		return "", shoal.NewError(
			shoal.ErrorInvalidArgument,
			"embedding space constituents exceed the public bound",
		)
	}
	normalized := append([]shoal.ID(nil), identities...)
	for _, identity := range normalized {
		if err := shoal.ValidateRequiredID(
			"embedding space constituent ID", identity,
		); err != nil {
			return "", err
		}
	}
	sort.Slice(normalized, func(i, j int) bool {
		return shoal.CompareID(normalized[i], normalized[j]) < 0
	})
	unique := normalized[:0]
	for _, identity := range normalized {
		if len(unique) > 0 && unique[len(unique)-1] == identity {
			continue
		}
		unique = append(unique, identity)
	}
	if len(unique) == 0 {
		return "", nil
	}
	hash := sha256.New()
	var length [8]byte
	write := func(value string) {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	write("shoal-retrieval-embedding-space-set-v1")
	for _, identity := range unique {
		write(string(identity))
	}
	return shoal.ID(
		"embedding-space_" + hex.EncodeToString(hash.Sum(nil)[:16]),
	), nil
}

// CompareResult returns a negative value when left sorts before right under
// score-descending, raw-result-ID-ascending order.
func CompareResult(left, right Result) int {
	switch {
	case left.Score > right.Score:
		return -1
	case left.Score < right.Score:
		return 1
	default:
		return shoal.CompareID(left.ID, right.ID)
	}
}

// CompareEvidence compares the public evidence ordering key: score descending,
// citation tuple, then directed path tuple. A response must not contain two
// evidence values with the same ordering key.
func CompareEvidence(left, right Evidence) int {
	switch {
	case left.Score > right.Score:
		return -1
	case left.Score < right.Score:
		return 1
	}
	if compared := compareCitation(left.Citation, right.Citation); compared != 0 {
		return compared
	}
	return comparePath(left.Path, right.Path)
}

// SortResults orders results and each result's evidence by the public total
// orders. It mutates the supplied response slices but not nested values.
func SortResults(response *Response) {
	if response == nil {
		return
	}
	sort.SliceStable(response.Results, func(i, j int) bool {
		return CompareResult(response.Results[i], response.Results[j]) < 0
	})
	for index := range response.Results {
		sort.SliceStable(response.Results[index].Evidence, func(i, j int) bool {
			return CompareEvidence(
				response.Results[index].Evidence[i],
				response.Results[index].Evidence[j],
			) < 0
		})
	}
}

// Retriever is implemented by embedded and remote Shoal clients.
type Retriever interface {
	Retrieve(context.Context, Request) (Response, error)
}

func deduplicateModes(modes []Mode) []Mode {
	result := make([]Mode, 0, len(modes))
	seen := make(map[Mode]struct{}, len(modes))
	for _, mode := range modes {
		if _, duplicate := seen[mode]; duplicate {
			continue
		}
		seen[mode] = struct{}{}
		result = append(result, mode)
	}
	return result
}

func deduplicateIDs(ids []shoal.ID) []shoal.ID {
	if len(ids) == 0 {
		return nil
	}
	result := make([]shoal.ID, 0, len(ids))
	seen := make(map[shoal.ID]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func validMode(mode Mode) bool {
	switch mode {
	case ModeLexical, ModeVector, ModeTree, ModeGraph:
		return true
	default:
		return false
	}
}

func validateExplanation(explanation Explanation) error {
	if len(explanation.Summary) > MaxExplanationBytes {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "explanation summary exceeds the public byte bound")
	}
	for _, mode := range explanation.Modes {
		if !validMode(mode) {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "explanation contains an unknown mode")
		}
	}
	for name, score := range explanation.Scores {
		if err := shoal.ValidateSemanticString("explanation score name", name); err != nil {
			return err
		}
		if err := shoal.ValidateFiniteScore("explanation score", score); err != nil {
			return err
		}
	}
	return nil
}

func compareCitation(left, right document.Citation) int {
	for _, pair := range [][2]shoal.ID{
		{left.DocumentID, right.DocumentID},
		{left.RevisionID, right.RevisionID},
		{left.SectionID, right.SectionID},
		{left.SpanID, right.SpanID},
	} {
		if compared := shoal.CompareID(pair[0], pair[1]); compared != 0 {
			return compared
		}
	}
	switch {
	case left.Range.Start.Offset < right.Range.Start.Offset:
		return -1
	case left.Range.Start.Offset > right.Range.Start.Offset:
		return 1
	case left.Range.End.Offset < right.Range.End.Offset:
		return -1
	case left.Range.End.Offset > right.Range.End.Offset:
		return 1
	default:
		return 0
	}
}

func comparePath(left, right graph.Path) int {
	for index := 0; index < len(left.Nodes) && index < len(right.Nodes); index++ {
		if compared := shoal.CompareID(left.Nodes[index].ID, right.Nodes[index].ID); compared != 0 {
			return compared
		}
	}
	if len(left.Nodes) < len(right.Nodes) {
		return -1
	}
	if len(left.Nodes) > len(right.Nodes) {
		return 1
	}
	for index := 0; index < len(left.Edges) && index < len(right.Edges); index++ {
		if compared := shoal.CompareID(left.Edges[index].ID, right.Edges[index].ID); compared != 0 {
			return compared
		}
	}
	switch {
	case len(left.Edges) < len(right.Edges):
		return -1
	case len(left.Edges) > len(right.Edges):
		return 1
	default:
		return 0
	}
}

func pathPresent(path graph.Path) bool {
	return len(path.Nodes) > 0 || len(path.Edges) > 0
}
