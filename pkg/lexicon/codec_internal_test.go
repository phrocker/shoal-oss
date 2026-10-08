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

package lexicon

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func internalBundle(t *testing.T) *Bundle {
	t.Helper()
	bundle, err := Build(Input{
		Snapshot: Snapshot{ID: "s", AsOf: time.Unix(1, 0), Frontier: 1},
		Scope:    ScopeServerFiltered{},
		Nodes: []graph.Node{
			{ID: "a", Kind: "k", Properties: shoal.Metadata{"name": "alpha beta"}},
			{ID: "b", Kind: "k", Properties: shoal.Metadata{"name": "gamma"}},
		},
	}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestNormalizationVersionIsPartOfTheID(t *testing.T) {
	bundle := internalBundle(t)
	data := append([]byte(nil), bundle.data...)
	binary.BigEndian.PutUint32(data[len(magic):], NormalizationVersion+1)
	if sha256.Sum256(data) == [32]byte(bundle.ID()) {
		t.Fatal("version change kept the digest")
	}
	if _, err := Load(data); err == nil ||
		!strings.Contains(err.Error(), "normalization version") {
		t.Fatalf("other normalization version: err = %v", err)
	}
	tables := strings.Replace(string(bundle.data), unicodeTables,
		strings.Replace(unicodeTables, "norm=", "norm=0", 1), 1)
	if _, err := Load([]byte(tables)); err == nil {
		t.Fatal("other Unicode tables accepted")
	}
}

func TestLoadRefusesDecodableNonCanonicalContents(t *testing.T) {
	original := internalBundle(t).contents
	mutations := map[string]func(c *contents){
		"unused token": func(c *contents) {
			c.tokens = append(c.tokens, "zzz")
		},
		"unsorted tokens": func(c *contents) {
			c.tokens[0], c.tokens[1] = c.tokens[1], c.tokens[0]
		},
		"unreferenced node": func(c *contents) {
			c.nodes = append(c.nodes, nodeEntry{id: "z", kind: "k"})
		},
		"unsorted terms": func(c *contents) {
			c.terms[0], c.terms[1] = c.terms[1], c.terms[0]
		},
		"duplicate posting": func(c *contents) {
			c.terms[0].postings = append(c.terms[0].postings, c.terms[0].postings[0])
		},
		"bad origin": func(c *contents) {
			c.terms[0].postings[0].origin = 0
		},
		"sentinel node": func(c *contents) {
			c.nodes[0].id = SentinelIDPrefix + "a"
		},
		"forged template": func(c *contents) {
			c.templates = []Template{{
				ID: "lookup:x:out", RelationKey: "x", Direction: DirectionOut,
				PhraseKey: "something else",
			}}
		},
	}
	for name, mutate := range mutations {
		c := cloneContents(original)
		mutate(c)
		data, err := encode(c)
		if err != nil {
			t.Fatalf("%s: encode: %v", name, err)
		}
		if _, err := Load(data); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func cloneContents(c *contents) *contents {
	out := *c
	out.tokens = append([]string(nil), c.tokens...)
	out.nodes = append([]nodeEntry(nil), c.nodes...)
	out.terms = make([]term, len(c.terms))
	for index, t := range c.terms {
		out.terms[index] = term{
			tokens:   append([]uint32(nil), t.tokens...),
			postings: append([]posting(nil), t.postings...),
		}
	}
	out.templates = append([]Template(nil), c.templates...)
	return &out
}
