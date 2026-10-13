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
	"testing"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestIsDocumentContainsEdge pins that only the exact edge a document parse
// derives passes (#732): the policy catalog relies on it so that a stored
// application edge can never pass as an intrinsic edge.
func TestIsDocumentContainsEdge(t *testing.T) {
	exact := containsEdge("doc_a", "section_a")
	if !IsDocumentContainsEdge(exact) {
		t.Fatalf("the parse's own edge %+v is refused", exact)
	}
	other := containsEdge("doc_a", "section_b")
	for name, mutate := range map[string]func(*graph.Edge){
		"another ID":            func(edge *graph.Edge) { edge.ID = "legacy-application-contains" },
		"another pair's ID":     func(edge *graph.Edge) { edge.ID = other.ID },
		"another source":        func(edge *graph.Edge) { edge.From = "doc_b" },
		"another target":        func(edge *graph.Edge) { edge.To = "section_b" },
		"another type":          func(edge *graph.Edge) { edge.Type = "related" },
		"another weight":        func(edge *graph.Edge) { edge.Weight = 0.5 },
		"a property":            func(edge *graph.Edge) { edge.Properties = shoal.Metadata{"k": "v"} },
		"an empty-key property": func(edge *graph.Edge) { edge.Properties = shoal.Metadata{"": ""} },
	} {
		edge := cloneEdge(exact)
		mutate(&edge)
		if IsDocumentContainsEdge(edge) {
			t.Errorf("%s: %+v passes as a document contains edge", name, edge)
		}
	}
}
