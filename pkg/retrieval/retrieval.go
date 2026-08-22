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

// Package retrieval defines transport-neutral knowledge retrieval contracts.
package retrieval

import (
	"context"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Mode selects a retrieval strategy. Multiple modes request a hybrid plan.
type Mode string

const (
	ModeLexical Mode = "lexical"
	ModeVector  Mode = "vector"
	ModeTree    Mode = "tree"
	ModeGraph   Mode = "graph"
)

// Scope bounds retrieval to known documents or graph nodes.
type Scope struct {
	DocumentIDs []shoal.ID
	NodeIDs     []shoal.ID
}

// Request describes one coarse knowledge retrieval operation.
type Request struct {
	Text    string
	TopK    uint32
	Modes   []Mode
	Scope   Scope
	AsOf    time.Time
	Explain bool
}

// Validate checks transport-independent request invariants.
func (r Request) Validate() error {
	if strings.TrimSpace(r.Text) == "" {
		return shoal.NewError(shoal.ErrorInvalidArgument, "retrieval text is required")
	}
	for _, mode := range r.Modes {
		switch mode {
		case ModeLexical, ModeVector, ModeTree, ModeGraph:
		default:
			return shoal.NewError(shoal.ErrorInvalidArgument, "unknown retrieval mode")
		}
	}
	return nil
}

// Evidence ties a result to immutable source and, when applicable, the graph
// path used to reach it.
type Evidence struct {
	Citation document.Citation
	Quote    string
	Path     graph.Path
	Score    shoal.Score
}

// Explanation describes why a result was selected without exposing an
// execution engine or storage plan.
type Explanation struct {
	Modes   []Mode
	Summary string
	Scores  map[string]shoal.Score
}

// Result is one ranked, evidence-addressable retrieval result.
type Result struct {
	ID          shoal.ID
	Score       shoal.Score
	Evidence    []Evidence
	Explanation *Explanation
}

// Response is the complete result of one retrieval request.
type Response struct {
	RequestID shoal.ID
	Results   []Result
}

// Retriever is implemented by embedded and remote Shoal clients.
type Retriever interface {
	Retrieve(context.Context, Request) (Response, error)
}
