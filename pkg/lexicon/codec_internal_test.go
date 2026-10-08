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
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func internalBundle(t testing.TB) *Bundle {
	t.Helper()
	bundle, err := Build(Input{
		Snapshot: Snapshot{ID: "s", AsOf: time.Unix(1, 0), Frontier: 1},

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
	data := append([]byte(nil), bundle.get().data...)
	binary.BigEndian.PutUint32(data[len(magic):], NormalizationVersion+1)
	if sha256.Sum256(data) == [32]byte(bundle.ID()) {
		t.Fatal("version change kept the digest")
	}
	if _, err := Load(data); err == nil ||
		!strings.Contains(err.Error(), "normalization version") {
		t.Fatalf("other normalization version: err = %v", err)
	}
	tables := strings.Replace(string(bundle.get().data), unicodeTables,
		strings.Replace(unicodeTables, "norm=", "norm=0", 1), 1)
	if _, err := Load([]byte(tables)); err == nil {
		t.Fatal("other Unicode tables accepted")
	}
}

func TestLoadRefusesDecodableNonCanonicalContents(t *testing.T) {
	original := internalBundle(t).get().contents
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
		"derived term without the option": func(c *contents) {
			c.flags = 0
			c.terms[0].postings[0].origin = OriginDerived
		},
		"unknown flag": func(c *contents) {
			c.flags = 0x80
		},
		"template concept outside the concept namespace": func(c *contents) {
			c.templates = []Template{validTemplate(func(t *Template) {
				t.SubjectConcepts = []shoal.ID{"node-a"}
			})}
		},
		"template concept not an ontology ID": func(c *contents) {
			c.templates = []Template{validTemplate(func(t *Template) {
				t.AnswerConcepts = []shoal.ID{"concept:"}
			})}
		},
		"template without concepts": func(c *contents) {
			c.templates = []Template{validTemplate(func(t *Template) {
				t.AnswerConcepts = nil
			})}
		},
		"template relation key with surrounding space": func(c *contents) {
			c.templates = []Template{validTemplate(func(t *Template) {
				t.RelationKey = " links"
				t.ID = templateID(t.RelationKey, t.Direction)
				t.PhraseKey = phraseKey(t.RelationKey, t.Direction)
			})}
		},
		"template relation key not UTF-8": func(c *contents) {
			c.templates = []Template{validTemplate(func(t *Template) {
				t.RelationKey = "links\xff"
				t.ID = templateID(t.RelationKey, t.Direction)
				t.PhraseKey = phraseKey(t.RelationKey, t.Direction)
			})}
		},
		"undirected template with different sides": func(c *contents) {
			c.templates = []Template{validTemplate(func(t *Template) {
				t.Direction = DirectionBoth
				t.ID = templateID(t.RelationKey, t.Direction)
				t.PhraseKey = phraseKey(t.RelationKey, t.Direction)
				t.AnswerConcepts = sortedIDs([]shoal.ID{testConceptA, testConceptB})
			})}
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

var (
	testConceptA = mustConceptID("alpha")
	testConceptB = mustConceptID("beta")
)

func mustConceptID(key string) shoal.ID {
	concept, err := ontology.NewConceptDefinition(key, key, "", nil, nil)
	if err != nil {
		panic(err)
	}
	return concept.ID()
}

// validTemplate is a canonical template, changed by mutate.
func validTemplate(mutate func(*Template)) Template {
	template := Template{
		ID: templateID("links", DirectionOut), RelationKey: "links",
		Direction: DirectionOut, SubjectConcepts: []shoal.ID{testConceptA},
		AnswerConcepts: []shoal.ID{testConceptA},
		PhraseKey:      phraseKey("links", DirectionOut),
	}
	mutate(&template)
	return template
}

func TestLoadAcceptsValidTemplate(t *testing.T) {
	c := cloneContents(internalBundle(t).get().contents)
	c.templates = []Template{validTemplate(func(*Template) {})}
	data, err := encode(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Load(data); err != nil {
		t.Fatalf("valid template refused: %v", err)
	}
}

// FuzzTokenize checks that every token is a fixed point of Tokenize and that
// spans are ordered and inside the input.
func FuzzTokenize(f *testing.F) {
	for _, seed := range []string{
		"Payments API", "ꮴ", "ᏣᎳᎩ", "pay\u00adments", "cafe\u200d\u0301", "⑴x",
		"İstanbul", "ﬁle", "ab\xffcd", "🚀 launch", "\ufeffx",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		// Two tokens from one compatibility expansion ("¾" is "3⁄4") share
		// that source character, so spans may overlap but never go back.
		start, end := 0, 0
		for _, token := range Tokenize(text) {
			if !canonicalToken(token.Text) && len(token.Text) <= HardMaxTokenBytes {
				t.Fatalf("token %q of %q is not canonical", token.Text, text)
			}
			if token.Start < start || token.End < end || token.End <= token.Start ||
				token.End > len(text) {
				t.Fatalf("bad span %d..%d after %d..%d in %q",
					token.Start, token.End, start, end, text)
			}
			start, end = token.Start, token.End
		}
	})
}

// FuzzLoad checks that Load never panics and accepts only bytes that rebuild
// to themselves.
func FuzzLoad(f *testing.F) {
	f.Add(internalBundle(f).get().data)
	f.Fuzz(func(t *testing.T, data []byte) {
		bundle, err := Load(data)
		if err != nil {
			return
		}
		again, err := encode(bundle.get().contents)
		if err != nil || string(again) != string(data) {
			t.Fatal("Load accepted non-canonical bytes")
		}
		_ = bundle.Candidates("a b c")
	})
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
