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

package authorized_test

import (
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const authorizedSkillMarkdown = `# Private Skill

Tools:
- private-cli

Capabilities:
- Secret graph extraction
`

func TestExtractDocumentAuthorizationControlsDerivedGraph(t *testing.T) {
	f := newFixture(t)
	visible, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///visible.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Visible\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	hidden, err := f.clientB.Ingest(f.admin(t), explorer.Source{
		URI: "file:///hidden/SKILL.md", MediaType: explorer.MediaTypeMarkdown,
		Content: authorizedSkillMarkdown,
	})
	if err != nil {
		t.Fatal(err)
	}
	version := authorizedSkillsOntologyVersion(t)
	extracted, err := f.clientB.ExtractDocument(f.bob(t), explorer.ExtractionRequest{
		DocumentID: hidden.Document.ID, RevisionID: hidden.Revision.ID,
		Version: version,
	})
	if err != nil {
		t.Fatal(err)
	}
	if extracted.EntityCount == 0 || len(extracted.EntityNodeIDs) == 0 {
		t.Fatalf("authorized extraction produced no entities: %+v", extracted)
	}
	bobGraph, err := f.clientB.Neighborhood(f.bob(t), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{hidden.Document.ID}, Depth: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasNode(bobGraph, extracted.EntityNodeIDs[0]) {
		t.Fatalf("authorized reader did not see extracted entity %s", extracted.EntityNodeIDs[0])
	}
	if countAuthorizedEdgesOfType(bobGraph.Edges, "provides_capability") != 1 {
		t.Fatalf("authorized reader relationship edges = %+v, want provides_capability", bobGraph.Edges)
	}
	if len(bobGraph.Assertions) != 2 {
		t.Fatalf("authorized reader relationship assertions = %d, want 2", len(bobGraph.Assertions))
	}
	bobSeedGraph, err := f.clientB.Neighborhood(f.bob(t), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{extracted.EntityNodeIDs[0]}, Depth: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasNode(bobSeedGraph, extracted.EntityNodeIDs[0]) {
		t.Fatalf("authorized direct seed missed extracted entity %s", extracted.EntityNodeIDs[0])
	}
	err = f.clientA.Connect(f.admin(t), graph.Edge{
		ID: "visible-to-hidden-extracted-skill", From: visible.Document.ID,
		To: extracted.EntityNodeIDs[0], Type: "mentions", Weight: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	aliceGraph, err := f.clientA.Neighborhood(f.alice(t), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{visible.Document.ID}, Depth: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if hasNode(aliceGraph, extracted.EntityNodeIDs[0]) ||
		countAuthorizedEdgesOfType(aliceGraph.Edges, "mentions") != 0 {
		t.Fatalf("unauthorized extracted entity leaked through endpoint filtering: %+v", aliceGraph)
	}
	if _, err := f.clientA.Neighborhood(f.alice(t), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{extracted.EntityNodeIDs[0]}, Depth: 1,
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("unauthorized extracted entity seed error = %v", err)
	}
}

func TestExtractDocumentRejectsUnreadableSource(t *testing.T) {
	f := newFixture(t)
	hidden, err := f.clientB.Ingest(f.admin(t), explorer.Source{
		URI: "file:///hidden/SKILL.md", MediaType: explorer.MediaTypeMarkdown,
		Content: authorizedSkillMarkdown,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.clientA.ExtractDocument(f.alice(t), explorer.ExtractionRequest{
		DocumentID: hidden.Document.ID, RevisionID: hidden.Revision.ID,
		Version: authorizedSkillsOntologyVersion(t),
	})
	if !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("unreadable source extraction error = %v", err)
	}
}

func authorizedSkillsOntologyVersion(t *testing.T) ontology.OntologyVersion {
	t.Helper()
	name, err := ontology.NewPropertyDefinition(
		"name", "Name", "Human-readable name", ontology.ValueString, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	skill, err := ontology.NewConceptDefinition(
		"skill", "Skill", "Agent skill", []shoal.ID{name.ID()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tool, err := ontology.NewConceptDefinition(
		"tool", "Tool", "Usable tool", []shoal.ID{name.ID()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := ontology.NewConceptDefinition(
		"capability", "Capability", "Provided feature", []shoal.ID{name.ID()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	providesTool, err := ontology.NewRelationshipDefinition(
		"provides_tool", "Provides tool", "Skill provides tool",
		[]shoal.ID{skill.ID()}, []shoal.ID{tool.ID()}, nil, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	providesCapability, err := ontology.NewRelationshipDefinition(
		"provides_capability", "Provides capability", "Skill provides capability",
		[]shoal.ID{skill.ID()}, []shoal.ID{capability.ID()}, nil, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	dependsOn, err := ontology.NewRelationshipDefinition(
		"depends_on", "Depends on", "Skill depends on another skill",
		[]shoal.ID{skill.ID()}, []shoal.ID{skill.ID()}, nil, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := ontology.NewOntologySchema(
		"agent-skills-auth", "Agent Skills Auth", "Authorized skill ontology", nil)
	if err != nil {
		t.Fatal(err)
	}
	version, err := ontology.NewOntologyVersion(
		schema, "v1", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		[]ontology.ConceptDefinition{skill, tool, capability},
		[]ontology.RelationshipDefinition{providesTool, providesCapability, dependsOn},
		[]ontology.PropertyDefinition{name}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return version
}

func countAuthorizedEdgesOfType(edges []graph.Edge, edgeType string) int {
	count := 0
	for _, edge := range edges {
		if edge.Type == edgeType {
			count++
		}
	}
	return count
}
