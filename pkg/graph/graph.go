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

// Package graph defines Shoal's public knowledge graph contract.
package graph

import "github.com/phrocker/shoal-oss/pkg/shoal"

// Node is a schema-neutral knowledge graph node.
type Node struct {
	ID         shoal.ID
	Kind       string
	Labels     []string
	Properties shoal.Metadata
}

// Edge is a directed, typed relationship between two nodes.
type Edge struct {
	ID         shoal.ID
	From       shoal.ID
	To         shoal.ID
	Type       string
	Weight     shoal.Score
	Properties shoal.Metadata
}

// Path is an ordered graph explanation. Edges[i] connects Nodes[i] to
// Nodes[i+1].
type Path struct {
	Nodes []Node
	Edges []Edge
}

// Validate checks that a path is connected and structurally complete.
func (p Path) Validate() error {
	if len(p.Nodes) == 0 {
		return shoal.NewError(shoal.ErrorInvalidArgument, "graph path requires a node")
	}
	for _, node := range p.Nodes {
		if node.ID == "" {
			return shoal.NewError(shoal.ErrorInvalidArgument, "graph path node requires an ID")
		}
	}
	if len(p.Edges) != len(p.Nodes)-1 {
		return shoal.NewError(shoal.ErrorInvalidArgument, "graph path has inconsistent edges")
	}
	for i, edge := range p.Edges {
		if edge.ID == "" || edge.From == "" || edge.To == "" || edge.Type == "" {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "graph path edge is structurally incomplete")
		}
		if edge.From != p.Nodes[i].ID || edge.To != p.Nodes[i+1].ID {
			return shoal.NewError(shoal.ErrorInvalidArgument, "graph path is not connected")
		}
	}
	return nil
}
