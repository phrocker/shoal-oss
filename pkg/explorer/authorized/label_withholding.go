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

package authorized

import (
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Output-label withholding (interim for #567 and #568, until #564).
//
// Interaction records and folds store the output-policy label expression they
// require of a reader (for example "project-x&secret"), but no read evaluates
// it against the reader yet: auth.Decision carries no label set (#564). The
// records themselves stay readable to anyone authorized on their touched
// sources, because team overview and provenance exist to show other
// principals' sessions. The expression, though, names compartments and
// projects, so it is the one part of a record that is legible without any
// other access.
//
// Until the per-reader evaluator lands, every interaction and fold read that
// leaves this client therefore withholds the label expression from any reader
// that did not record it. Withholding sets each field exactly as a record that
// never had labels stores it (the empty string, a nil slice, an absent
// metadata key), so a withheld record is indistinguishable from an unlabelled
// one and carries no shape marker (#566). Session, fold and node IDs and the
// digests stay visible; that is the stated residual.
//
// "Recorded it" means the reader's authorization fingerprint equals the one
// pinned into the record when it was written. That is the same identity the
// client already uses for every recorder-only rule: tombstones, zero-hit
// records, and exact-retry replay (summaryFingerprintMatchesDecision,
// interactionPinMatchesDecision). The fingerprint binds the authorization
// domain, subject, actor, client, delegation chain and exact grants, and the
// trusted sink rejects a record whose pin is not the recording decision's. It
// fails closed: a recorder whose grants have since changed is treated as a
// different reader and loses sight of the expression, which costs only that
// recorder a label it can no longer prove it is the projection for.

// readerFingerprint is the decision's authorization fingerprint, or "" when it
// cannot be computed. An empty fingerprint never matches a record, because a
// durable record always carries a non-empty one.
func readerFingerprint(decision auth.Decision) shoal.ID {
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return ""
	}
	return shoal.ID(fingerprint.String())
}

// readerRecorded reports whether the reader is the authorization projection
// that recorded a session pinned to recorded.
func readerRecorded(recorded, reader shoal.ID) bool {
	return reader != "" && recorded == reader
}

// withholdInteractionLabels returns record as the reader may see it: unchanged
// for the recorder, and with every label expression cleared otherwise.
func withholdInteractionLabels(
	record explorer.InteractionRecord, reader shoal.ID,
) explorer.InteractionRecord {
	if readerRecorded(record.Summary.AuthorizationFingerprint, reader) {
		return record
	}
	record.Summary.Visibility = ""
	record.Session.RequiredVisibility = nil
	return record
}

// withholdSubgraphLabels clears the visibility properties ConjoinSubgraphVisibility
// stamps on every derived node and edge of an interaction subgraph, unless the
// reader recorded the session. A record stored without labels never sets
// these keys, so deleting them reproduces its shape exactly.
func withholdSubgraphLabels(
	subgraph explorer.Neighborhood, recorded, reader shoal.ID,
) explorer.Neighborhood {
	if readerRecorded(recorded, reader) {
		return subgraph
	}
	nodes := make([]graph.Node, len(subgraph.Nodes))
	for index, node := range subgraph.Nodes {
		node.Properties = withholdVisibilityMetadata(node.Properties)
		nodes[index] = node
	}
	edges := make([]graph.Edge, len(subgraph.Edges))
	for index, edge := range subgraph.Edges {
		edge.Properties = withholdVisibilityMetadata(edge.Properties)
		edges[index] = edge
	}
	if subgraph.Nodes != nil {
		subgraph.Nodes = nodes
	}
	if subgraph.Edges != nil {
		subgraph.Edges = edges
	}
	return subgraph
}

func withholdVisibilityMetadata(metadata shoal.Metadata) shoal.Metadata {
	if metadata == nil {
		return nil
	}
	cloned := cloneMetadata(metadata)
	// The digest and term count stand in for an expression too long for one
	// metadata value. A digest of a short label conjunction is guessable, so
	// it is withheld with the expression it stands for.
	delete(cloned, interaction.PropertyVisibility)
	delete(cloned, interaction.PropertyVisibilityDigest)
	delete(cloned, interaction.PropertyVisibilityCount)
	return cloned
}

// foldRecorders reports, for each member of a fold, whether the reader
// recorded it, and whether the reader recorded every member. A fold's own
// visibility conjoins every member's, so it is shown only to a reader who
// recorded all of them.
type foldRecorders struct {
	members []bool
	all     bool
}

func (r foldRecorders) member(index int) bool {
	return index < len(r.members) && r.members[index]
}

// withholdFoldLabels returns fold with each member's label set cleared unless
// the reader recorded that member.
func withholdFoldLabels(
	fold interaction.Fold, recorders foldRecorders,
) interaction.Fold {
	members := make([]interaction.FoldMember, len(fold.Members))
	for index, member := range fold.Members {
		if !recorders.member(index) {
			member.Visibility = nil
		}
		members[index] = member
	}
	if fold.Members != nil {
		fold.Members = members
	}
	return fold
}
