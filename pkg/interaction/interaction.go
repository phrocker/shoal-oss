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

// Package interaction defines the reserved knowledge-graph namespace that
// records how an inference was served: which session ran, which turns it took,
// which tool calls it made, what those calls retrieved, and what the answer
// actually cited.
//
// Interaction nodes live in the same corpus as content but under a reserved
// kind namespace. Retrieval excludes them by default; they are reachable only
// by explicit traversal from an interaction seed or by an explicit
// kind-scoped query. A model must never be able to cite its own prior output
// as though it were source evidence.
//
// An interaction node carries the conjunction of every visibility label of
// every source node it touched. Visibility is never derived from the asker's
// grant set, because that would let a highly cleared user's session become a
// covert channel. A reviewed declassification path is deliberately absent from
// this package; it is a later, explicit, authority-bearing action.
package interaction

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// KindPrefix reserves the interaction node-kind namespace. Content ingestion
// must never mint a node kind under this prefix.
const KindPrefix = "interaction."

// Reserved interaction node kinds.
const (
	KindSession   = KindPrefix + "session"
	KindTurn      = KindPrefix + "turn"
	KindToolCall  = KindPrefix + "tool_call"
	KindTombstone = KindPrefix + "tombstone"
	KindFold      = KindPrefix + "fold"
)

// EdgeTypePrefix reserves the interaction edge-type namespace.
const EdgeTypePrefix = "interaction."

// Reserved interaction edge types. Retrieved and cited are deliberately
// distinct: what the model was shown is a larger set than what it cited, and
// visibility must be derived from everything it was shown.
const (
	EdgeHasTurn     = EdgeTypePrefix + "has_turn"
	EdgeHasToolCall = EdgeTypePrefix + "has_tool_call"
	EdgeRetrieved   = EdgeTypePrefix + "retrieved"
	EdgeCited       = EdgeTypePrefix + "cited"
	// EdgeFolds points from a fold summary to a session it folds. It is what
	// makes a fold rehydratable and what makes cross-session traversal
	// possible without widening anything.
	EdgeFolds = EdgeTypePrefix + "folds"
)

// Node and edge property keys. Values are identities, digests, counts, and
// visibility labels only. Prompts, questions, quotes, credentials, and
// model-chosen correlation strings are never persisted here.
const (
	// PropertyVisibility carries the canonical conjunction expression. It is
	// also the key content ingestion uses to declare a source's visibility.
	PropertyVisibility = "shoal.visibility"

	PropertySessionID     = "interaction.session_id"
	PropertyTurnID        = "interaction.turn_id"
	PropertyIndex         = "interaction.index"
	PropertyDecision      = "interaction.decision"
	PropertyToolKind      = "interaction.tool_kind"
	PropertyRetrieved     = "interaction.retrieved_count"
	PropertyCited         = "interaction.cited_count"
	PropertyTurnCount     = "interaction.turn_count"
	PropertyStopReason    = "interaction.stop_reason"
	PropertyRecordedAt    = "interaction.recorded_at"
	PropertyDeletedAt     = "interaction.deleted_at"
	PropertyNodeCount     = "interaction.node_count"
	PropertyEdgeCount     = "interaction.edge_count"
	PropertyQueryDigest   = "interaction.query_digest"
	PropertyRequestID     = "interaction.request_id"
	PropertyContextPackID = "interaction.context_pack_id"
	PropertyResultID      = "interaction.result_id"
	PropertyInputTokens   = "interaction.input_tokens"
	PropertyOutputTokens  = "interaction.output_tokens"
	PropertyFailed        = "interaction.failed"
	PropertyHarness       = "interaction.harness"
	PropertyProvider      = "interaction.provider"
	PropertyModel         = "interaction.model"
	PropertyModelVersion  = "interaction.model_version"
	PropertyPromptID      = "interaction.prompt_template_id"
	PropertyPromptVersion = "interaction.prompt_version"
	PropertyPromptHash    = "interaction.prompt_hash"
	PropertyToolPolicy    = "interaction.tool_policy"

	// PropertySummaryDigest carries the SHA-256 digest of a fold's
	// out-of-band summary text. The digest is stored so a fold can be
	// correlated with the summary it describes; the summary text itself is
	// never persisted, because it is derived from evidence the record is not
	// allowed to carry.
	PropertySummaryDigest = "interaction.summary_digest"
	PropertyFoldedAt      = "interaction.folded_at"
	PropertyFoldedCount   = "interaction.folded_session_count"
)

// LabelInteraction marks every interaction node so label-based consumers can
// exclude them without parsing kinds.
const LabelInteraction = "interaction"

// Public static bounds. They exist so a single recorded session can never
// become an unbounded write.
const (
	MaxTurns             = 4096
	MaxTouchedNodes      = 65536
	MaxVisibilityLabels  = 64
	MaxVisibilityLabelSz = 256
	MaxFoldMembers       = 4096
)

// IsInteractionKind reports whether a node kind is in the reserved namespace.
func IsInteractionKind(kind string) bool {
	return strings.HasPrefix(kind, KindPrefix)
}

// IsInteractionEdgeType reports whether an edge type is in the reserved
// namespace.
func IsInteractionEdgeType(edgeType string) bool {
	return strings.HasPrefix(edgeType, EdgeTypePrefix)
}

// ToolCall is one tool invocation made inside a turn. RetrievedNodeIDs are the
// source graph nodes the call put in front of the model, not only the ones it
// went on to cite.
type ToolCall struct {
	Kind             string
	RetrievedNodeIDs []shoal.ID
}

// Turn is one model decision. A turn that stopped rather than calling a tool
// carries no ToolCall.
type Turn struct {
	Index        int
	Decision     string
	InputTokens  int
	OutputTokens int
	Failed       bool
	ToolCall     *ToolCall
}

// Provenance is the redacted identity of the harness, model, prompt template,
// and tool policy that served an inference.
type Provenance struct {
	Harness      string
	Provider     string
	Model        string
	ModelVersion string
	PromptID     string
	PromptVer    string
	PromptHash   string
	ToolPolicy   string
}

// Session is one recorded inference. It carries identities, digests, counts,
// and the source node IDs it touched. It never carries the question, the
// prompt, the answer text, evidence quotes, authorization grants, or
// model-chosen correlation strings.
type Session struct {
	ID            shoal.ID
	RecordedAt    time.Time
	Provenance    Provenance
	QueryDigest   string
	RequestID     shoal.ID
	ContextPackID shoal.ID
	ResultID      shoal.ID
	StopReason    string

	// SeedNodeIDs are source nodes the session was shown before its first
	// turn. They count as retrieved.
	SeedNodeIDs []shoal.ID
	Turns       []Turn
	// CitedNodeIDs are source nodes the final answer actually cited.
	CitedNodeIDs []shoal.ID
}

// Subgraph is the materialized interaction record: its own nodes, its edges to
// itself and to the source nodes it touched, and the conjoined visibility the
// whole record requires.
type Subgraph struct {
	Nodes      []graph.Node
	Edges      []graph.Edge
	Visibility []string
	// TouchedNodeIDs is the sorted union of every source node the session was
	// shown or cited. Visibility is the conjunction over exactly this set.
	TouchedNodeIDs []shoal.ID
}

// VisibilityResolver reports the visibility labels a source node requires. It
// must return an error for a node it cannot resolve so recording fails closed
// rather than silently under-labeling an interaction record.
type VisibilityResolver func(shoal.ID) ([]string, error)

// NodeVisibility reads the declared visibility labels of a graph node. A node
// with no declared labels is public.
func NodeVisibility(node graph.Node) ([]string, error) {
	return ParseVisibility(node.Properties[PropertyVisibility])
}

// ParseVisibility parses a canonical conjunction expression into sorted unique
// labels. An empty expression is public.
func ParseVisibility(expression string) ([]string, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, nil
	}
	parts := strings.Split(expression, "&")
	labels := make([]string, 0, len(parts))
	for _, part := range parts {
		label := strings.TrimSpace(part)
		if err := validateLabel(label); err != nil {
			return nil, err
		}
		labels = append(labels, label)
	}
	return Conjoin(labels)
}

// Conjoin returns the conjunction of one or more visibility label sets: the
// sorted unique union of every label, because a reader must hold all of them.
func Conjoin(sets ...[]string) ([]string, error) {
	seen := make(map[string]struct{})
	for _, set := range sets {
		for _, label := range set {
			if err := validateLabel(label); err != nil {
				return nil, err
			}
			seen[label] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil, nil
	}
	if len(seen) > MaxVisibilityLabels {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"interaction visibility exceeds the public label bound",
		)
	}
	labels := make([]string, 0, len(seen))
	for label := range seen {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	return labels, nil
}

// Expression renders sorted labels as a canonical conjunction. Public
// visibility renders as the empty string.
func Expression(labels []string) string {
	return strings.Join(labels, "&")
}

func validateLabel(label string) error {
	if label == "" {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "visibility label cannot be empty")
	}
	if len(label) > MaxVisibilityLabelSz {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"visibility label exceeds the public byte bound",
		)
	}
	for i := 0; i < len(label); i++ {
		c := label[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_', c == '-', c == '.', c == ':':
		default:
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"visibility label contains an unsupported character",
			)
		}
	}
	return nil
}

// Validate checks the static shape of a session before it is materialized.
func (s Session) Validate() error {
	if err := shoal.ValidateRequiredID("interaction session ID", s.ID); err != nil {
		return err
	}
	if s.RecordedAt.IsZero() {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction session time is required")
	}
	if len(s.Turns) > MaxTurns {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction session exceeds the public turn bound")
	}
	touched := len(s.SeedNodeIDs) + len(s.CitedNodeIDs)
	for _, turn := range s.Turns {
		if turn.ToolCall != nil {
			touched += len(turn.ToolCall.RetrievedNodeIDs)
		}
	}
	if touched > MaxTouchedNodes {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"interaction session exceeds the public touched-node bound",
		)
	}
	for _, id := range s.SeedNodeIDs {
		if err := shoal.ValidateRequiredID("interaction seed node ID", id); err != nil {
			return err
		}
	}
	for _, id := range s.CitedNodeIDs {
		if err := shoal.ValidateRequiredID("interaction cited node ID", id); err != nil {
			return err
		}
	}
	for _, turn := range s.Turns {
		if turn.ToolCall == nil {
			continue
		}
		for _, id := range turn.ToolCall.RetrievedNodeIDs {
			if err := shoal.ValidateRequiredID(
				"interaction retrieved node ID", id,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// Subgraph materializes the session, turn, and tool-call nodes with their
// retrieved and cited edges. resolve supplies the visibility labels of every
// touched source node; if it fails for any node, the whole record fails rather
// than being written with an understated visibility.
func (s Session) Subgraph(resolve VisibilityResolver) (Subgraph, error) {
	if err := s.Validate(); err != nil {
		return Subgraph{}, err
	}
	if resolve == nil {
		return Subgraph{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction visibility resolver is required")
	}
	cache := make(map[shoal.ID][]string)
	labelsFor := func(ids []shoal.ID) ([]string, error) {
		sets := make([][]string, 0, len(ids))
		for _, id := range ids {
			if cached, ok := cache[id]; ok {
				sets = append(sets, cached)
				continue
			}
			labels, err := resolve(id)
			if err != nil {
				return nil, err
			}
			normalized, err := Conjoin(labels)
			if err != nil {
				return nil, err
			}
			cache[id] = normalized
			sets = append(sets, normalized)
		}
		return Conjoin(sets...)
	}

	sessionNode := graph.Node{
		ID:     s.ID,
		Kind:   KindSession,
		Labels: []string{LabelInteraction},
		Properties: shoal.Metadata{
			PropertyRecordedAt: s.RecordedAt.UTC().Format(time.RFC3339Nano),
			PropertyTurnCount:  strconv.Itoa(len(s.Turns)),
			PropertyCited:      strconv.Itoa(len(s.CitedNodeIDs)),
		},
	}
	setIfPresent(sessionNode.Properties, PropertyStopReason, s.StopReason)
	setIfPresent(sessionNode.Properties, PropertyQueryDigest, s.QueryDigest)
	setIfPresent(sessionNode.Properties, PropertyRequestID, string(s.RequestID))
	setIfPresent(sessionNode.Properties, PropertyContextPackID, string(s.ContextPackID))
	setIfPresent(sessionNode.Properties, PropertyResultID, string(s.ResultID))
	setIfPresent(sessionNode.Properties, PropertyHarness, s.Provenance.Harness)
	setIfPresent(sessionNode.Properties, PropertyProvider, s.Provenance.Provider)
	setIfPresent(sessionNode.Properties, PropertyModel, s.Provenance.Model)
	setIfPresent(sessionNode.Properties, PropertyModelVersion, s.Provenance.ModelVersion)
	setIfPresent(sessionNode.Properties, PropertyPromptID, s.Provenance.PromptID)
	setIfPresent(sessionNode.Properties, PropertyPromptVersion, s.Provenance.PromptVer)
	setIfPresent(sessionNode.Properties, PropertyPromptHash, s.Provenance.PromptHash)
	setIfPresent(sessionNode.Properties, PropertyToolPolicy, s.Provenance.ToolPolicy)

	nodes := []graph.Node{sessionNode}
	var edges []graph.Edge
	touched := make(map[shoal.ID]struct{})
	addTouched := func(ids []shoal.ID) {
		for _, id := range ids {
			touched[id] = struct{}{}
		}
	}

	seed := dedupeIDs(s.SeedNodeIDs)
	addTouched(seed)
	for _, id := range seed {
		edges = append(edges, provenanceEdge(EdgeRetrieved, s.ID, id))
	}
	cited := dedupeIDs(s.CitedNodeIDs)
	addTouched(cited)
	for _, id := range cited {
		edges = append(edges, provenanceEdge(EdgeCited, s.ID, id))
	}

	for _, turn := range s.Turns {
		turnID := DerivedID("turn", string(s.ID), strconv.Itoa(turn.Index))
		turnNode := graph.Node{
			ID:     turnID,
			Kind:   KindTurn,
			Labels: []string{LabelInteraction},
			Properties: shoal.Metadata{
				PropertySessionID:    string(s.ID),
				PropertyIndex:        strconv.Itoa(turn.Index),
				PropertyInputTokens:  strconv.Itoa(turn.InputTokens),
				PropertyOutputTokens: strconv.Itoa(turn.OutputTokens),
				PropertyFailed:       strconv.FormatBool(turn.Failed),
			},
		}
		setIfPresent(turnNode.Properties, PropertyDecision, turn.Decision)
		edges = append(edges, provenanceEdge(EdgeHasTurn, s.ID, turnID))

		turnVisibility := []string(nil)
		if turn.ToolCall != nil {
			retrieved := dedupeIDs(turn.ToolCall.RetrievedNodeIDs)
			addTouched(retrieved)
			callID := DerivedID("tool_call", string(turnID))
			callNode := graph.Node{
				ID:     callID,
				Kind:   KindToolCall,
				Labels: []string{LabelInteraction},
				Properties: shoal.Metadata{
					PropertySessionID: string(s.ID),
					PropertyTurnID:    string(turnID),
					PropertyIndex:     strconv.Itoa(turn.Index),
					PropertyRetrieved: strconv.Itoa(len(retrieved)),
				},
			}
			setIfPresent(callNode.Properties, PropertyToolKind, turn.ToolCall.Kind)
			callVisibility, err := labelsFor(retrieved)
			if err != nil {
				return Subgraph{}, err
			}
			setIfPresent(callNode.Properties, PropertyVisibility, Expression(callVisibility))
			turnVisibility = callVisibility
			nodes = append(nodes, callNode)
			edges = append(edges, provenanceEdge(EdgeHasToolCall, turnID, callID))
			for _, id := range retrieved {
				edges = append(edges, provenanceEdge(EdgeRetrieved, callID, id))
			}
		}
		setIfPresent(turnNode.Properties, PropertyVisibility, Expression(turnVisibility))
		nodes = append(nodes, turnNode)
	}

	touchedIDs := make([]shoal.ID, 0, len(touched))
	for id := range touched {
		touchedIDs = append(touchedIDs, id)
	}
	sort.Slice(touchedIDs, func(i, j int) bool {
		return shoal.CompareID(touchedIDs[i], touchedIDs[j]) < 0
	})
	visibility, err := labelsFor(touchedIDs)
	if err != nil {
		return Subgraph{}, err
	}
	setIfPresent(sessionNode.Properties, PropertyVisibility, Expression(visibility))

	sortNodes(nodes)
	sortEdges(edges)
	for _, node := range nodes {
		if err := node.Validate(); err != nil {
			return Subgraph{}, err
		}
	}
	for _, edge := range edges {
		if err := edge.Validate(); err != nil {
			return Subgraph{}, err
		}
	}
	return Subgraph{
		Nodes:          nodes,
		Edges:          edges,
		Visibility:     visibility,
		TouchedNodeIDs: touchedIDs,
	}, nil
}

// Tombstone is the durable, auditable residue of an explicitly deleted
// interaction record. Deletion is never a TTL and never silent.
type Tombstone struct {
	SessionID  shoal.ID
	DeletedAt  time.Time
	NodeCount  int
	EdgeCount  int
	Visibility []string
}

// Node materializes a tombstone. It keeps the visibility the deleted record
// required, so a deletion can never publish the fact that a restricted session
// existed to a reader who could not have seen the session itself.
func (t Tombstone) Node() (graph.Node, error) {
	if err := shoal.ValidateRequiredID("interaction session ID", t.SessionID); err != nil {
		return graph.Node{}, err
	}
	if t.DeletedAt.IsZero() {
		return graph.Node{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction deletion time is required")
	}
	visibility, err := Conjoin(t.Visibility)
	if err != nil {
		return graph.Node{}, err
	}
	node := graph.Node{
		ID:     TombstoneID(t.SessionID),
		Kind:   KindTombstone,
		Labels: []string{LabelInteraction},
		Properties: shoal.Metadata{
			PropertySessionID: string(t.SessionID),
			PropertyDeletedAt: t.DeletedAt.UTC().Format(time.RFC3339Nano),
			PropertyNodeCount: strconv.Itoa(t.NodeCount),
			PropertyEdgeCount: strconv.Itoa(t.EdgeCount),
		},
	}
	setIfPresent(node.Properties, PropertyVisibility, Expression(visibility))
	if err := node.Validate(); err != nil {
		return graph.Node{}, err
	}
	return node, nil
}

// TombstoneID is the stable identity of the tombstone for one session.
func TombstoneID(sessionID shoal.ID) shoal.ID {
	return DerivedID("tombstone", string(sessionID))
}

// SessionID derives a stable session identity from a transcript identity and
// the instant the record was captured. The instant is part of the identity so
// that serving the same deterministic inference twice produces two durable
// records rather than silently collapsing into one.
func SessionID(transcriptID shoal.ID, recordedAt time.Time) shoal.ID {
	return DerivedID(
		"session",
		string(transcriptID),
		recordedAt.UTC().Format(time.RFC3339Nano),
	)
}

// DerivedID mints a collision-resistant identity inside the interaction
// namespace from already-public identity components.
func DerivedID(kind string, parts ...string) shoal.ID {
	hash := sha256.New()
	var length [8]byte
	write := func(value string) {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	write(KindPrefix)
	write(kind)
	for _, part := range parts {
		write(part)
	}
	return shoal.ID(KindPrefix + kind + "_" + hex.EncodeToString(hash.Sum(nil)[:16]))
}

// Digest hashes free text into a non-reversible identity so a record can be
// correlated without persisting the text itself.
func Digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func provenanceEdge(edgeType string, from, to shoal.ID) graph.Edge {
	return graph.Edge{
		ID:     DerivedID("edge", edgeType, string(from), string(to)),
		From:   from,
		To:     to,
		Type:   edgeType,
		Weight: 1,
	}
}

func setIfPresent(metadata shoal.Metadata, key, value string) {
	if value == "" {
		return
	}
	metadata[key] = value
}

func dedupeIDs(ids []shoal.ID) []shoal.ID {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[shoal.ID]struct{}, len(ids))
	result := make([]shoal.ID, 0, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool {
		return shoal.CompareID(result[i], result[j]) < 0
	})
	return result
}

func sortNodes(nodes []graph.Node) {
	sort.Slice(nodes, func(i, j int) bool {
		return shoal.CompareID(nodes[i].ID, nodes[j].ID) < 0
	})
}

func sortEdges(edges []graph.Edge) {
	sort.Slice(edges, func(i, j int) bool {
		return shoal.CompareID(edges[i].ID, edges[j].ID) < 0
	})
}
