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
	"crypto/sha256"
)

// Bundle is an immutable, loaded lexicon. It is safe for concurrent use.
//
// A Bundle never exposes its bytes. Shipping requires a pinned scope; see
// ForShipping.
type Bundle struct {
	id            BundleID
	data          []byte
	contents      *contents
	tokenIndex    map[string]uint32
	maxTermTokens int
	maxPostings   int
	automaton     automaton
}

func newBundle(data []byte, c *contents) *Bundle {
	b := &Bundle{
		id:         sha256.Sum256(data),
		data:       data,
		contents:   c,
		tokenIndex: make(map[string]uint32, len(c.tokens)),
	}
	for index, token := range c.tokens {
		b.tokenIndex[token] = uint32(index)
	}
	for _, t := range c.terms {
		b.maxTermTokens = max(b.maxTermTokens, len(t.tokens))
		b.maxPostings = max(b.maxPostings, len(t.postings))
	}
	b.automaton = buildAutomaton(len(c.tokens), c.terms)
	return b
}

// ID is the SHA-256 of the bundle's canonical bytes.
func (b *Bundle) ID() BundleID { return b.id }

// Snapshot is the graph snapshot the bundle is pinned to. AsOf is in UTC.
func (b *Bundle) Snapshot() Snapshot { return b.contents.snapshot }

// Scope is ScopeServerFiltered or ScopePinned.
func (b *Bundle) Scope() Scope { return b.contents.scope }

// TermCount is the number of distinct terms.
func (b *Bundle) TermCount() int { return len(b.contents.terms) }

// Templates returns a copy of the lookup templates, sorted by ID.
func (b *Bundle) Templates() []Template {
	out := make([]Template, len(b.contents.templates))
	for index, template := range b.contents.templates {
		out[index] = cloneTemplate(template)
	}
	return out
}

// CandidateBound is the most distinct node IDs Candidates can return for any
// text of at most maxTokens tokens: every span of at most the bundle's longest
// term matches at most one term, and every term has at most the bundle's
// largest posting count. It depends only on the bundle, never on the text.
func (b *Bundle) CandidateBound(maxTokens int) int {
	if maxTokens <= 0 {
		return 0
	}
	spans := 0
	for length := 1; length <= b.maxTermTokens && length <= maxTokens; length++ {
		spans += maxTokens - length + 1
	}
	return spans * b.maxPostings
}

// Shippable is a bundle whose bytes may leave the server. Only a bundle built
// for a pinned scope can become one, so an unscoped (server-filtered) bundle
// cannot be exported through this package.
type Shippable struct {
	bundle *Bundle
}

// ForShipping returns the shippable view of a pinned bundle, and false for a
// server-filtered bundle.
func (b *Bundle) ForShipping() (Shippable, bool) {
	if _, ok := b.contents.scope.(ScopePinned); !ok {
		return Shippable{}, false
	}
	return Shippable{bundle: b}, true
}

// Bytes returns a copy of the canonical bundle bytes.
func (s Shippable) Bytes() []byte {
	if s.bundle == nil {
		return nil
	}
	return append([]byte(nil), s.bundle.data...)
}

// ID is the shipped bundle's ID.
func (s Shippable) ID() BundleID {
	if s.bundle == nil {
		return BundleID{}
	}
	return s.bundle.id
}

// Scope is the pinned scope the shipped bundle was built for.
func (s Shippable) Scope() ScopePinned {
	if s.bundle == nil {
		return ScopePinned{}
	}
	return s.bundle.contents.scope.(ScopePinned)
}
