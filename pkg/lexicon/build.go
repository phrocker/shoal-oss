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

package lexicon

import (
	"math"
	"sort"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Hard bounds a bundle can never exceed, whatever Limits a build used. Load
// enforces them, and they bound the candidate set a text can produce.
const (
	HardMaxTermTokens      = 16
	HardMaxPostingsPerTerm = 64
	HardMaxTokenBytes      = 1024
)

// Limits bound a build. Exceeding any limit fails the build; nothing is ever
// silently dropped, because a lexicon that quietly omits an entity would make
// that entity look unknown. Zero fields take the defaults.
type Limits struct {
	MaxNodes           int
	MaxTerms           int
	MaxTokens          int
	MaxTermTokens      int
	MaxPostingsPerTerm int
	MaxTokenBytes      int
	MaxTemplates       int
	MaxBundleBytes     int
}

// DefaultLimits are sized for a few million terms on one host.
func DefaultLimits() Limits {
	return Limits{
		MaxNodes:           2_000_000,
		MaxTerms:           8_000_000,
		MaxTokens:          8_000_000,
		MaxTermTokens:      8,
		MaxPostingsPerTerm: 8,
		MaxTokenBytes:      128,
		MaxTemplates:       16_384,
		MaxBundleBytes:     1 << 30,
	}
}

func (l Limits) normalized() (Limits, error) {
	defaults := DefaultLimits()
	pick := func(value, fallback int) int {
		if value == 0 {
			return fallback
		}
		return value
	}
	l.MaxNodes = pick(l.MaxNodes, defaults.MaxNodes)
	l.MaxTerms = pick(l.MaxTerms, defaults.MaxTerms)
	l.MaxTokens = pick(l.MaxTokens, defaults.MaxTokens)
	l.MaxTermTokens = pick(l.MaxTermTokens, defaults.MaxTermTokens)
	l.MaxPostingsPerTerm = pick(l.MaxPostingsPerTerm, defaults.MaxPostingsPerTerm)
	l.MaxTokenBytes = pick(l.MaxTokenBytes, defaults.MaxTokenBytes)
	l.MaxTemplates = pick(l.MaxTemplates, defaults.MaxTemplates)
	l.MaxBundleBytes = pick(l.MaxBundleBytes, defaults.MaxBundleBytes)
	if l.MaxNodes < 0 || l.MaxTerms < 0 || l.MaxTokens < 0 ||
		l.MaxTemplates < 0 || l.MaxBundleBytes < 0 ||
		l.MaxTermTokens < 0 || l.MaxTermTokens > HardMaxTermTokens ||
		l.MaxPostingsPerTerm < 0 || l.MaxPostingsPerTerm > HardMaxPostingsPerTerm ||
		l.MaxTokenBytes < 0 || l.MaxTokenBytes > HardMaxTokenBytes {
		return Limits{}, invalid("lexicon limits are out of range")
	}
	return l, nil
}

func invalid(message string) error {
	return shoal.NewError(shoal.ErrorInvalidArgument, message)
}

func limitExceeded(name string) error {
	return invalid("lexicon build exceeds the " + name + " limit")
}

// Build compiles a bundle from in. The result depends only on the content of
// in, never on the order of nodes, properties or relationships, so rebuilding
// the same snapshot gives byte-identical bytes and the same ID.
func Build(in Input, limits Limits) (*Bundle, error) {
	limits, err := limits.normalized()
	if err != nil {
		return nil, err
	}
	if err := validateSnapshot(in.Snapshot); err != nil {
		return nil, err
	}
	if err := validateScope(in.Scope); err != nil {
		return nil, err
	}
	if len(in.Nodes) > limits.MaxNodes {
		return nil, limitExceeded("node")
	}

	type nodeTerms struct {
		kind  string
		terms map[string]Origin
	}
	byNode := make(map[shoal.ID]*nodeTerms, len(in.Nodes))
	for _, node := range in.Nodes {
		if err := node.Validate(); err != nil {
			return nil, shoal.WrapError(
				shoal.ErrorInvalidArgument, "lexicon node is invalid", err)
		}
		if strings.HasPrefix(string(node.ID), SentinelIDPrefix) {
			return nil, invalid("lexicon node ID uses the reserved sentinel prefix")
		}
		if _, duplicate := byNode[node.ID]; duplicate {
			return nil, invalid("lexicon node IDs must be unique")
		}
		entry := &nodeTerms{kind: node.Kind, terms: make(map[string]Origin)}
		byNode[node.ID] = entry
		for _, surface := range surfaces(node) {
			tokens := tokenTexts(surface.text)
			if len(tokens) == 0 {
				continue
			}
			if err := checkTerm(tokens, limits); err != nil {
				return nil, err
			}
			addTerm(entry.terms, tokens, surface.origin)
			if !in.DeriveInitialisms || surface.origin > OriginTitle ||
				len(tokens) < MinInitialismTokens {
				continue
			}
			if initialism, ok := deriveInitialism(tokens); ok {
				if err := checkTerm(initialism, limits); err != nil {
					return nil, err
				}
				addTerm(entry.terms, initialism, OriginDerived)
			}
		}
	}

	// Invert to term -> postings, keeping only nodes that produced a term.
	nodeIDs := make([]shoal.ID, 0, len(byNode))
	for id, entry := range byNode {
		if len(entry.terms) > 0 {
			nodeIDs = append(nodeIDs, id)
		}
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })
	nodes := make([]nodeEntry, len(nodeIDs))
	postingsByKey := make(map[string][]posting)
	tokenSet := make(map[string]struct{})
	for index, id := range nodeIDs {
		entry := byNode[id]
		nodes[index] = nodeEntry{id: id, kind: entry.kind}
		for key, origin := range entry.terms {
			postingsByKey[key] = append(postingsByKey[key],
				posting{node: uint32(index), origin: origin})
		}
	}
	if len(postingsByKey) > limits.MaxTerms {
		return nil, limitExceeded("term")
	}
	for key := range postingsByKey {
		for _, token := range strings.Split(key, termSeparator) {
			tokenSet[token] = struct{}{}
		}
	}
	if len(tokenSet) > limits.MaxTokens {
		return nil, limitExceeded("token")
	}
	tokens := make([]string, 0, len(tokenSet))
	for token := range tokenSet {
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	tokenIDs := make(map[string]uint32, len(tokens))
	for index, token := range tokens {
		tokenIDs[token] = uint32(index)
	}
	terms := make([]term, 0, len(postingsByKey))
	for key, postings := range postingsByKey {
		if len(postings) > limits.MaxPostingsPerTerm {
			return nil, limitExceeded("postings per term")
		}
		parts := strings.Split(key, termSeparator)
		ids := make([]uint32, len(parts))
		for index, part := range parts {
			ids[index] = tokenIDs[part]
		}
		sort.Slice(postings, func(i, j int) bool {
			return postings[i].node < postings[j].node
		})
		terms = append(terms, term{tokens: ids, postings: postings})
	}
	sort.Slice(terms, func(i, j int) bool {
		return compareTokenIDs(terms[i].tokens, terms[j].tokens) < 0
	})

	templates, err := DeriveTemplates(in.Relationships)
	if err != nil {
		return nil, err
	}
	if len(templates) > limits.MaxTemplates {
		return nil, limitExceeded("template")
	}

	var flags uint8
	if in.DeriveInitialisms {
		flags |= flagDeriveInitialisms
	}
	encoded, err := encode(&contents{
		flags:     flags,
		snapshot:  in.Snapshot,
		scope:     in.Scope,
		tokens:    tokens,
		nodes:     nodes,
		terms:     terms,
		templates: templates,
	})
	if err != nil {
		return nil, err
	}
	if len(encoded) > limits.MaxBundleBytes {
		return nil, limitExceeded("bundle byte")
	}
	// Loading the encoded bytes is the only way a Bundle is made, so a built
	// bundle and a loaded one cannot differ.
	return Load(encoded)
}

// termSeparator joins tokens into a map key. It is never a word rune.
const termSeparator = "\x00"

type surface struct {
	text   string
	origin Origin
}

func surfaces(node graph.Node) []surface {
	var out []surface
	if name := node.Properties[PropertyName]; name != "" {
		out = append(out, surface{text: name, origin: OriginName})
	} else if title := node.Properties[PropertyTitle]; title != "" {
		out = append(out, surface{text: title, origin: OriginTitle})
	}
	if key := node.Properties[PropertyEntityKey]; key != "" {
		out = append(out, surface{text: key, origin: OriginEntityKey})
	}
	for key, value := range node.Properties {
		if len(key) > len(PropertyAliasPrefix) &&
			strings.HasPrefix(key, PropertyAliasPrefix) && value != "" {
			out = append(out, surface{text: value, origin: OriginAlias})
		}
	}
	// Map iteration order is random; the result is order-independent because
	// addTerm keeps the minimum origin per term.
	return out
}

func checkTerm(tokens []string, limits Limits) error {
	if len(tokens) > limits.MaxTermTokens {
		return limitExceeded("term token")
	}
	for _, token := range tokens {
		if len(token) > limits.MaxTokenBytes {
			return limitExceeded("token byte")
		}
	}
	return nil
}

func addTerm(terms map[string]Origin, tokens []string, origin Origin) {
	key := strings.Join(tokens, termSeparator)
	if existing, ok := terms[key]; !ok || origin < existing {
		terms[key] = origin
	}
}

// deriveInitialism joins the first rune of each token. The joined string is
// re-tokenized so the derived term is itself normalized; if it does not come
// back as exactly one token nothing is derived.
func deriveInitialism(tokens []string) ([]string, bool) {
	var builder strings.Builder
	for _, token := range tokens {
		for _, r := range token {
			builder.WriteRune(r)
			break
		}
	}
	derived := tokenTexts(builder.String())
	if len(derived) != 1 {
		return nil, false
	}
	return derived, true
}

func compareTokenIDs(a, b []uint32) int {
	for index := 0; index < len(a) && index < len(b); index++ {
		if a[index] != b[index] {
			if a[index] < b[index] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

func validateSnapshot(snapshot Snapshot) error {
	if snapshot.ID == "" {
		return invalid("lexicon snapshot ID is required")
	}
	if err := shoal.ValidateSemanticString(
		"lexicon snapshot ID", snapshot.ID); err != nil {
		return err
	}
	if snapshot.AsOf.IsZero() {
		return invalid("lexicon snapshot time is required")
	}
	if !representableNanos(snapshot.AsOf) {
		return invalid("lexicon snapshot time is out of range")
	}
	return nil
}

var (
	minNanosTime = time.Unix(0, math.MinInt64).UTC()
	maxNanosTime = time.Unix(0, math.MaxInt64).UTC()
)

func representableNanos(at time.Time) bool {
	return !at.Before(minNanosTime) && !at.After(maxNanosTime)
}

func validateScope(scope Scope) error {
	switch value := scope.(type) {
	case ScopeServerFiltered:
		return nil
	case ScopePinned:
		if value.Digest == ([32]byte{}) {
			return invalid("lexicon pinned scope digest is required")
		}
		return nil
	default:
		return invalid("lexicon scope is required")
	}
}
