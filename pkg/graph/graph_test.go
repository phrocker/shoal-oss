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

package graph_test

import (
	"testing"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestPathRequiresConnectedEdges(t *testing.T) {
	path := graph.Path{
		Nodes: []graph.Node{{ID: "a"}, {ID: "b"}},
		Edges: []graph.Edge{{ID: "edge-1", From: "a", To: "b", Type: "supports"}},
	}
	if err := path.Validate(); err != nil {
		t.Fatalf("expected valid path: %v", err)
	}

	path.Edges[0].To = "c"
	if err := path.Validate(); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestPathRequiresNodeAndEdgeIdentity(t *testing.T) {
	tests := []graph.Path{
		{Nodes: []graph.Node{{}}},
		{
			Nodes: []graph.Node{{ID: "a"}, {ID: "b"}},
			Edges: []graph.Edge{{From: "a", To: "b", Type: "supports"}},
		},
		{
			Nodes: []graph.Node{{ID: "a"}, {ID: "b"}},
			Edges: []graph.Edge{{ID: "edge-1", From: "a", To: "b"}},
		},
	}
	for i, path := range tests {
		if err := path.Validate(); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			t.Fatalf("case %d: expected invalid argument, got %v", i, err)
		}
	}
}
