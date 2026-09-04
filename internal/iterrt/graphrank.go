// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package iterrt

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/phrocker/shoal-oss/internal/rfile/wire"
)

// GraphRankIterator is the majc iterator on graph tables - a port of
// org.apache.accumulo.core.graph.GraphRankIterator.
//
// During Seek, the iterator buffers the source cells, identifies vertex rows,
// builds a directed adjacency list from edge column families, runs iterative
// PageRank over those vertices, and emits one rank property per vertex. Source
// cells pass through unchanged.
//
// Rank computation uses:
//
//	rank(v) = (1-dampingFactor)/N + dampingFactor * sum(rank(u)/outDegree(u))
//
// for incoming neighbors u that are also known vertices. Dangling rank is not
// redistributed; vertices with no incoming ranked neighbors receive only the
// teleport term after an iteration. The maxVertices guard skips rank emission
// entirely when too many vertices are present.
//
// Emission is deterministic: generated timestamps are the maximum source
// timestamp in the seek range + 1, rank iteration order is sorted, incoming
// contribution order is sorted, and the merged output buffer is key-sorted.
//
// Options:
//
//	dampingFactor        (float, default 0.85)
//	maxIterations        (int,   default 20)
//	edgeType             optional edge type; matches edgeCFPrefix + edgeType
//	maxVertices          (int,   default 100000; skip rank emission above this)
//	convergenceThreshold (float, default 0.0001)
//	vertexCF             vertex property column family (default "V")
//	edgeCFPrefix         outgoing edge column-family prefix (default "E_")
//	labelCQ              label column qualifier used for output visibility (default "_label")
//	rankCQ               emitted rank column qualifier (default "_rank")
//
// Column families and qualifiers are schema-agnostic strings supplied by
// options. iterrt intentionally does not import internal/graphschema.
type GraphRankIterator struct {
	source SortedKeyValueIterator

	dampingFactor        float64
	maxIterations        int
	edgeType             string
	maxVertices          int
	convergenceThreshold float64

	vertexCF     string
	edgeCFPrefix string
	labelCQ      string
	rankCQ       string

	out      []Cell
	outIndex int
	err      error
}

// GraphRankIterator option keys.
const (
	GraphRankDampingFactor        = "dampingFactor"
	GraphRankMaxIterations        = "maxIterations"
	GraphRankEdgeType             = "edgeType"
	GraphRankMaxVertices          = "maxVertices"
	GraphRankConvergenceThreshold = "convergenceThreshold"
	GraphRankVertexCF             = "vertexCF"
	GraphRankEdgeCFPrefix         = "edgeCFPrefix"
	GraphRankLabelCQ              = "labelCQ"
	GraphRankRankCQ               = "rankCQ"
)

const (
	graphRankDefaultDamping       = 0.85
	graphRankDefaultMaxIterations = 20
	graphRankDefaultMaxVertices   = 100000
	graphRankDefaultConvergence   = 0.0001
	graphRankDefaultVertexCF      = "V"
	graphRankDefaultEdgeCFPrefix  = "E_"
	graphRankDefaultLabelCQ       = "_label"
	graphRankDefaultRankCQ        = "_rank"
	graphRankIteratorErrorPrefix  = "iterrt: GraphRankIterator"
)

// NewGraphRankIterator constructs an un-Init'd graph-rank iterator.
func NewGraphRankIterator() *GraphRankIterator {
	return &GraphRankIterator{
		dampingFactor:        graphRankDefaultDamping,
		maxIterations:        graphRankDefaultMaxIterations,
		maxVertices:          graphRankDefaultMaxVertices,
		convergenceThreshold: graphRankDefaultConvergence,
		vertexCF:             graphRankDefaultVertexCF,
		edgeCFPrefix:         graphRankDefaultEdgeCFPrefix,
		labelCQ:              graphRankDefaultLabelCQ,
		rankCQ:               graphRankDefaultRankCQ,
	}
}

// Init parses options from the map.
func (g *GraphRankIterator) Init(source SortedKeyValueIterator, options map[string]string, env IteratorEnvironment) error {
	if source == nil {
		return errors.New(graphRankIteratorErrorPrefix + " requires a non-nil source")
	}
	g.source = source

	if s, ok := options[GraphRankDampingFactor]; ok && s != "" {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("%s bad %s=%q", graphRankIteratorErrorPrefix, GraphRankDampingFactor, s)
		}
		g.dampingFactor = v
	}
	if s, ok := options[GraphRankMaxIterations]; ok && s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("%s bad %s=%q", graphRankIteratorErrorPrefix, GraphRankMaxIterations, s)
		}
		g.maxIterations = v
	}
	if s, ok := options[GraphRankEdgeType]; ok {
		g.edgeType = s
	}
	if s, ok := options[GraphRankMaxVertices]; ok && s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("%s bad %s=%q", graphRankIteratorErrorPrefix, GraphRankMaxVertices, s)
		}
		g.maxVertices = v
	}
	if s, ok := options[GraphRankConvergenceThreshold]; ok && s != "" {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("%s bad %s=%q", graphRankIteratorErrorPrefix, GraphRankConvergenceThreshold, s)
		}
		g.convergenceThreshold = v
	}
	if s := options[GraphRankVertexCF]; s != "" {
		g.vertexCF = s
	}
	if s := options[GraphRankEdgeCFPrefix]; s != "" {
		g.edgeCFPrefix = s
	}
	if s := options[GraphRankLabelCQ]; s != "" {
		g.labelCQ = s
	}
	if s := options[GraphRankRankCQ]; s != "" {
		g.rankCQ = s
	}
	return nil
}

func (g *GraphRankIterator) Seek(r Range, columnFamilies [][]byte, inclusive bool) error {
	g.out = g.out[:0]
	g.outIndex = 0
	g.err = nil

	if err := g.source.Seek(r, columnFamilies, inclusive); err != nil {
		g.err = err
		return err
	}

	vertices := map[string]*graphRankVertex{}
	outgoing := map[string][]string{}
	incoming := map[string]map[string]struct{}{}
	var maxTS int64
	haveSource := false

	for g.source.HasTop() {
		k := g.source.GetTopKey().Clone()
		v := append([]byte(nil), g.source.GetTopValue()...)
		g.out = append(g.out, Cell{Key: k, Value: v})
		if !haveSource || k.Timestamp > maxTS {
			maxTS = k.Timestamp
			haveSource = true
		}

		row := string(k.Row)
		cf := string(k.ColumnFamily)
		cq := string(k.ColumnQualifier)

		if cf == g.vertexCF {
			vertex, ok := vertices[row]
			if !ok {
				vertex = &graphRankVertex{visibility: append([]byte(nil), k.ColumnVisibility...)}
				vertices[row] = vertex
			}
			if cq == g.labelCQ {
				vertex.visibility = append([]byte(nil), k.ColumnVisibility...)
			}
		}

		if strings.HasPrefix(cf, g.edgeCFPrefix) {
			if g.edgeType == "" || cf == g.edgeCFPrefix+g.edgeType {
				outgoing[row] = append(outgoing[row], cq)
				sources := incoming[cq]
				if sources == nil {
					sources = map[string]struct{}{}
					incoming[cq] = sources
				}
				sources[row] = struct{}{}
			}
		}

		if err := g.source.Next(); err != nil {
			g.err = err
			return err
		}
	}

	if len(vertices) > g.maxVertices || len(vertices) == 0 {
		g.sortOut()
		return nil
	}

	vertexIDs := graphRankKeys(vertices)
	sort.Strings(vertexIDs)
	n := len(vertexIDs)
	initialRank := 1.0 / float64(n)
	ranks := make(map[string]float64, n)
	for _, vertexID := range vertexIDs {
		ranks[vertexID] = initialRank
	}

	for iter := 0; iter < g.maxIterations; iter++ {
		newRanks := make(map[string]float64, n)
		maxDelta := 0.0

		for _, vertexID := range vertexIDs {
			rankSum := 0.0
			if sources := incoming[vertexID]; len(sources) > 0 {
				neighbors := graphRankKeys(sources)
				sort.Strings(neighbors) // Load-bearing determinism: TestGraphRankParity_DeterministicIncomingOrder pins contribution order.
				for _, neighbor := range neighbors {
					neighborRank, ok := ranks[neighbor]
					if !ok {
						continue
					}
					outDegree := len(outgoing[neighbor])
					if outDegree == 0 {
						outDegree = 1
					}
					rankSum += neighborRank / float64(outDegree)
				}
			}

			newRank := (1.0-g.dampingFactor)/float64(n) + g.dampingFactor*rankSum
			newRanks[vertexID] = newRank
			delta := math.Abs(newRank - ranks[vertexID])
			if delta > maxDelta {
				maxDelta = delta
			}
		}

		ranks = newRanks
		if maxDelta < g.convergenceThreshold {
			break
		}
	}

	rankTS := maxTS + 1 // Load-bearing determinism: TestGraphRank_DerivedTimestamps pins source-derived timestamps.
	for _, vertexID := range vertexIDs {
		vertex := vertices[vertexID]
		g.out = append(g.out, Cell{
			Key: &wire.Key{
				Row:              []byte(vertexID),
				ColumnFamily:     []byte(g.vertexCF),
				ColumnQualifier:  []byte(g.rankCQ),
				ColumnVisibility: append([]byte(nil), vertex.visibility...),
				Timestamp:        rankTS,
			},
			Value: []byte(strconv.FormatFloat(ranks[vertexID], 'g', -1, 64)),
		})
	}

	g.sortOut() // Load-bearing output ordering: TestGraphRank_OutputSortedByKey pins merged key order.
	return nil
}

func (g *GraphRankIterator) sortOut() {
	sort.SliceStable(g.out, func(i, j int) bool { return g.out[i].Key.Compare(g.out[j].Key) < 0 })
}

type graphRankVertex struct {
	visibility []byte
}

func graphRankKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func (g *GraphRankIterator) HasTop() bool { return g.err == nil && g.outIndex < len(g.out) }
func (g *GraphRankIterator) GetTopKey() *Key {
	if !g.HasTop() {
		return nil
	}
	return g.out[g.outIndex].Key
}
func (g *GraphRankIterator) GetTopValue() []byte {
	if !g.HasTop() {
		return nil
	}
	return g.out[g.outIndex].Value
}
func (g *GraphRankIterator) Next() error {
	if g.err != nil {
		return g.err
	}
	if !g.HasTop() {
		return errors.New(graphRankIteratorErrorPrefix + ".Next called without a top")
	}
	g.outIndex++
	return nil
}

func (g *GraphRankIterator) DeepCopy(env IteratorEnvironment) SortedKeyValueIterator {
	return &GraphRankIterator{
		source:               g.source.DeepCopy(env),
		dampingFactor:        g.dampingFactor,
		maxIterations:        g.maxIterations,
		edgeType:             g.edgeType,
		maxVertices:          g.maxVertices,
		convergenceThreshold: g.convergenceThreshold,
		vertexCF:             g.vertexCF,
		edgeCFPrefix:         g.edgeCFPrefix,
		labelCQ:              g.labelCQ,
		rankCQ:               g.rankCQ,
	}
}
