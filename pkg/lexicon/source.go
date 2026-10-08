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

// Package lexicon builds a graph-derived vocabulary bundle: the names,
// aliases and derived abbreviations of graph nodes, compiled into a token-level
// Aho-Corasick matcher that returns node IDs, plus lookup templates derived
// from ontology relationship definitions.
//
// A bundle is content-addressed (its ID is the SHA-256 of its canonical bytes)
// and pinned to the graph snapshot it was built from. It has one of two
// scopes. A server-filtered bundle may contain any node; every match must be
// filtered against the caller's current authorization before it is used or
// echoed, and the bundle's bytes cannot be exported. A pinned bundle is built
// from a node set that was already filtered for one authorization scope, names
// that scope's digest, and is the only kind that can be shipped.
//
// The package does no authorization itself: Candidates reports every overlap
// regardless of visibility, and Select must be given the caller's visibility.
// See pkg/explorer/authorized ResolveMentions for the composition.
package lexicon

import (
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/ontology"
)

// Graph property keys the builder reads. Names come from PropertyName, else
// PropertyTitle; the ontology entity key and every alias key add further
// terms. Labels are not used in this version.
const (
	PropertyName      = "name"
	PropertyTitle     = "title"
	PropertyEntityKey = "shoal.ontology.entity_key"
	// PropertyAliasPrefix reserves "shoal.lexicon.alias.<n>" keys for
	// aliases. Any non-empty suffix is accepted; the suffix carries no
	// meaning beyond letting a node hold several aliases.
	PropertyAliasPrefix = "shoal.lexicon.alias."
)

// SentinelIDPrefix is reserved for padding identifiers that authorization
// lookups use to keep their batch size fixed. Build refuses any node whose ID
// starts with it, so a padding identifier can never be a lexicon candidate.
const SentinelIDPrefix = "\x00shoal.lexicon.sentinel/"

// Snapshot pins a bundle to one immutable graph frontier. It mirrors
// explorer.Snapshot without importing the explorer.
type Snapshot struct {
	ID       string
	AsOf     time.Time
	Frontier uint64
}

// Scope is the authorization scope a bundle was built for. It is either
// ScopeServerFiltered or ScopePinned.
type Scope interface {
	scopeKind() byte
}

// ScopeServerFiltered marks a bundle built from nodes of any visibility. Its
// matches must be filtered against the caller's current authorization, and
// it cannot be exported.
type ScopeServerFiltered struct{}

// ScopePinned marks a bundle built from nodes already filtered for one
// authorization scope, identified by Digest. Only such a bundle can be
// shipped.
type ScopePinned struct {
	Digest [32]byte
}

const (
	scopeKindServerFiltered byte = 1
	scopeKindPinned         byte = 2
)

func (ScopeServerFiltered) scopeKind() byte { return scopeKindServerFiltered }
func (ScopePinned) scopeKind() byte         { return scopeKindPinned }

// Input is everything a build reads. Nothing in the graph lists every node, so
// the caller supplies the node set; for a pinned scope it must already be
// filtered to that scope. Order of Nodes, of their properties and of
// Relationships does not affect the result.
type Input struct {
	Snapshot      Snapshot
	Scope         Scope
	Nodes         []graph.Node
	Relationships []ontology.RelationshipDefinition
}

// Origin records where a term came from. Lower values are stronger: when one
// node reaches the same term several ways, the posting keeps the lowest.
type Origin uint8

const (
	OriginName      Origin = 1
	OriginTitle     Origin = 2
	OriginEntityKey Origin = 3
	OriginAlias     Origin = 4
	// OriginDerived is an initialism derived from a name or title of two or
	// more tokens.
	OriginDerived Origin = 5
)

func (o Origin) valid() bool {
	return o >= OriginName && o <= OriginDerived
}

func (o Origin) String() string {
	switch o {
	case OriginName:
		return "name"
	case OriginTitle:
		return "title"
	case OriginEntityKey:
		return "entity_key"
	case OriginAlias:
		return "alias"
	case OriginDerived:
		return "derived"
	default:
		return "invalid"
	}
}
