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

package explorer

import (
	"sort"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/extraction"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Canonical shared extraction content (#700).
//
// An extracted entity's node ID depends only on its namespace, concept and
// key, and a relation's edge ID only on its type and endpoints, so several
// documents extract the very same node or edge. Whatever an extractor
// reports beside that identity (the surface text it took a display name
// from, a confidence) differs between extractions, and the attacker
// controls it: "Zeta" and "zeta" are the same entity. If that presentation
// were part of the node, a second extraction would disagree with the first
// and be refused, which tells the second extractor that some document,
// readable or not, already holds the entity. So a shared node or relation
// carries only what its identity determines, for every extractor and for
// records stored before this rule: the graph is built from, and the policy
// catalog stores, CanonicalExtractedNode and CanonicalExtractedRelation.
// The extractor's confidence stays on the relation's assertion.

const (
	extractedEntityIDNamespace   = "inferred-entity"
	extractedRelationIDNamespace = "inferred-edge"
	extractedRelationProperty    = "ontology_relationship_id"
)

// CanonicalExtractedNode returns an extracted entity node reduced to its
// identity, and false (with node unchanged) for any other node: its kind and
// labels are the concept key and a display form of the entity key, and its
// properties are the concept, the key and the namespace.
func CanonicalExtractedNode(node graph.Node) (graph.Node, bool) {
	conceptID := node.Properties[extraction.GraphPropertyOntologyConceptID]
	conceptKey := node.Properties[extraction.GraphPropertyOntologyConceptKey]
	key := node.Properties[extraction.GraphPropertyEntityKey]
	if ontology.IDNamespace(node.ID) != extractedEntityIDNamespace ||
		conceptID == "" || conceptKey == "" || key == "" {
		return node, false
	}
	properties := shoal.Metadata{
		extraction.GraphPropertyOntologyConceptID:  conceptID,
		extraction.GraphPropertyOntologyConceptKey: conceptKey,
		extraction.GraphPropertyEntityKey:          key,
	}
	if namespace := node.Properties[extraction.GraphPropertyEntityNamespace]; namespace != "" {
		properties[extraction.GraphPropertyEntityNamespace] = namespace
	}
	labels := []string{conceptKey}
	if display := entityKeyDisplay(key); display != conceptKey {
		labels = append(labels, display)
	}
	sort.Strings(labels)
	return graph.Node{
		ID: node.ID, Kind: "ontology:" + conceptKey,
		Labels: labels, Properties: properties,
	}, true
}

// CanonicalExtractedRelation returns an extracted relation edge reduced to
// its identity (weight 1, only its relationship property), and false (with
// edge unchanged) for any other edge.
func CanonicalExtractedRelation(edge graph.Edge) (graph.Edge, bool) {
	relationship := edge.Properties[extractedRelationProperty]
	if ontology.IDNamespace(edge.ID) != extractedRelationIDNamespace || relationship == "" {
		return edge, false
	}
	return graph.Edge{
		ID: edge.ID, From: edge.From, To: edge.To, Type: edge.Type, Weight: 1,
		Properties: shoal.Metadata{extractedRelationProperty: relationship},
	}, true
}

// entityKeyDisplay is the entity key with its separators as spaces.
func entityKeyDisplay(key string) string {
	words := strings.FieldsFunc(key, func(r rune) bool {
		return r == '_' || r == '-' || r == '.'
	})
	if len(words) == 0 {
		return key
	}
	return strings.Join(words, " ")
}
