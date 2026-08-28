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

package harness

import (
	"context"
	"fmt"
	"sort"

	"github.com/phrocker/shoal-oss/pkg/contextpack"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type ExplorerToolHost struct {
	Client           explorer.Client
	BoundedClient    explorer.BoundedClient
	Builder          contextpack.Builder
	PolicyID         shoal.ID
	Metadata         shoal.Metadata
	RetrievalModes   []retrieval.Mode
	RetrievalExplain bool
}

func NewExplorerToolHost(client explorer.Client, builder contextpack.Builder) (*ExplorerToolHost, error) {
	if client == nil {
		return nil, invalid("explorer client is required")
	}
	if builder.Reader == nil {
		builder.Reader = client
	}
	host := &ExplorerToolHost{Client: client, Builder: builder}
	if bounded, ok := client.(explorer.BoundedClient); ok {
		host.BoundedClient = bounded
	}
	return host, nil
}

func (h *ExplorerToolHost) Retrieve(
	ctx context.Context,
	call ToolContext,
	request RetrieveRequest,
) (ToolResult, error) {
	if err := h.validate(); err != nil {
		return ToolResult{}, err
	}
	retrievalRequest := retrieval.Request{
		Text:    request.Query(),
		TopK:    uint32(request.Limit()),
		Modes:   h.RetrievalModes,
		AsOf:    call.Snapshot().AsOf(),
		Explain: h.RetrievalExplain,
	}
	response, err := h.Client.Retrieve(ctx, retrievalRequest)
	if err != nil {
		return ToolResult{}, err
	}
	pack, err := h.builder().Build(ctx, contextpack.InitialRequest{
		Request:  retrievalRequest,
		Response: response,
		Pins: contextpack.Pins{
			Snapshot:      call.Snapshot(),
			Authorization: call.Authorization(),
			PolicyID:      h.PolicyID,
		},
		Metadata: h.Metadata,
	})
	if err != nil {
		return ToolResult{}, err
	}
	return NewToolResult(
		call.CorrelationID(), ActionRetrieve, pack.Evidence(),
		call.Snapshot(), call.Authorization())
}

func (h *ExplorerToolHost) OpenSection(
	ctx context.Context,
	call ToolContext,
	request OpenSectionRequest,
) (ToolResult, error) {
	if err := h.validate(); err != nil {
		return ToolResult{}, err
	}
	current := call.Context()
	next, err := h.builder().OpenSection(ctx, current, contextpack.OpenSectionRequest{
		DocumentID: request.DocumentID(),
		RevisionID: request.RevisionID(),
		SectionIDs: []shoal.ID{request.SectionID()},
		Depth:      0,
	})
	if err != nil {
		return ToolResult{}, err
	}
	return NewToolResult(
		call.CorrelationID(), ActionOpenSection, verifiedAdditions(current, next),
		call.Snapshot(), call.Authorization())
}

func (h *ExplorerToolHost) Neighbors(
	ctx context.Context,
	call ToolContext,
	request NeighborsRequest,
) (ToolResult, error) {
	if err := h.validate(); err != nil {
		return ToolResult{}, err
	}
	if h.BoundedClient == nil {
		return ToolResult{}, invalid("bounded explorer client is required for neighbors")
	}
	bounded, err := h.BoundedClient.BoundedNeighborhood(ctx, explorer.BoundedNeighborhoodRequest{
		NodeIDs:   []shoal.ID{request.NodeID()},
		Depth:     uint32(request.Hops()),
		Fanout:    uint32(request.Fanout()),
		MaxNodes:  uint32(request.Fanout() + 1),
		Direction: explorer.GraphDirectionOutgoing,
	})
	if err != nil {
		return ToolResult{}, err
	}
	anchors, err := graphAnchorsFromNeighborhood(request.NodeID(), bounded.Neighborhood)
	if err != nil {
		return ToolResult{}, err
	}
	return NewToolResult(
		call.CorrelationID(), ActionNeighbors, anchors,
		call.Snapshot(), call.Authorization())
}

func (h *ExplorerToolHost) validate() error {
	if h == nil || h.Client == nil {
		return invalid("explorer client is required")
	}
	if h.Builder.Reader == nil {
		if h.Client == nil {
			return invalid("explorer reader is required")
		}
	}
	if _, err := (retrieval.Request{Text: "validate", Modes: h.RetrievalModes}).Normalize(); err != nil {
		return err
	}
	return nil
}

func (h *ExplorerToolHost) builder() contextpack.Builder {
	builder := h.Builder
	if builder.Reader == nil {
		builder.Reader = h.Client
	}
	return builder
}

func graphAnchorsFromNeighborhood(seed shoal.ID, neighborhood explorer.Neighborhood) ([]inference.EvidenceAnchor, error) {
	nodes := make(map[shoal.ID]graph.Node, len(neighborhood.Nodes))
	for _, node := range neighborhood.Nodes {
		nodes[node.ID] = node
	}
	if _, ok := nodes[seed]; !ok {
		return nil, invalid("bounded neighborhood omitted the requested node")
	}
	edges := append([]graph.Edge(nil), neighborhood.Edges...)
	sort.Slice(edges, func(i, j int) bool {
		return shoal.CompareID(edges[i].ID, edges[j].ID) < 0
	})
	anchors := make([]inference.EvidenceAnchor, 0, len(edges))
	for _, edge := range edges {
		if edge.From != seed {
			continue
		}
		from, fromOK := nodes[edge.From]
		to, toOK := nodes[edge.To]
		if !fromOK || !toOK {
			return nil, invalid("bounded neighborhood edge has a missing endpoint")
		}
		anchor, err := inference.NewGraphAnchor(graph.Path{
			Nodes: []graph.Node{from, to},
			Edges: []graph.Edge{edge},
		})
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
		anchors = append(anchors, anchor)
	}
	return anchors, nil
}
