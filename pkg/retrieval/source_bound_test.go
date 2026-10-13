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

package retrieval_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func scope(source, policy string) retrieval.BoundScope {
	return retrieval.BoundScope{SourceID: []byte(source), PolicyID: []byte(policy)}
}

// TestRequestNormalizeOwnsAndCanonicalizesItsBound pins that a normalized
// request's bound is deduplicated, canonically ordered, and shares no memory
// with the caller's, and that an absent bound stays absent.
func TestRequestNormalizeOwnsAndCanonicalizesItsBound(t *testing.T) {
	unbounded, err := retrieval.Request{Text: "x"}.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if unbounded.Bound != nil {
		t.Fatal("an absent bound must normalize to absent, not to admit-nothing")
	}

	bound := &retrieval.SourceBound{Scopes: []retrieval.BoundScope{
		scope("b", "pb"), scope("a", "pa"), scope("b", "pb"),
	}}
	normalized, err := retrieval.Request{Text: "x", Bound: bound}.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	got := normalized.Bound.Scopes
	if len(got) != 2 || string(got[0].SourceID) != "a" || string(got[1].SourceID) != "b" {
		t.Fatalf("normalized scopes = %q", got)
	}
	bound.Scopes[1].SourceID[0] = 'z'
	if string(normalized.Bound.Scopes[0].SourceID) != "a" {
		t.Fatal("a normalized bound must not alias the caller's bytes")
	}

	empty, err := retrieval.Request{Text: "x", Bound: &retrieval.SourceBound{}}.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if empty.Bound == nil || len(empty.Bound.Scopes) != 0 {
		t.Fatal("a present bound with no scopes must stay present and admit nothing")
	}
}

func TestRequestNormalizeRejectsMalformedBounds(t *testing.T) {
	tooMany := &retrieval.SourceBound{}
	for index := 0; index <= retrieval.MaxBoundScopes; index++ {
		tooMany.Scopes = append(tooMany.Scopes,
			scope("s"+strings.Repeat("x", index), "p"))
	}
	long := strings.Repeat("x", retrieval.MaxBoundComponentBytes+1)
	for name, bound := range map[string]*retrieval.SourceBound{
		"empty source":   {Scopes: []retrieval.BoundScope{scope("", "p")}},
		"empty policy":   {Scopes: []retrieval.BoundScope{scope("s", "")}},
		"long source":    {Scopes: []retrieval.BoundScope{scope(long, "p")}},
		"long policy":    {Scopes: []retrieval.BoundScope{scope("s", long)}},
		"too many pairs": tooMany,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := retrieval.Request{Text: "x", Bound: bound}.Normalize()
			if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
				t.Fatalf("err = %v, want invalid argument", err)
			}
		})
	}
}

// TestIntersectBoundsOnlyNarrows pins the algebra a caller composes bounds
// with: nil is the identity, and two bounds share only common scopes.
func TestIntersectBoundsOnlyNarrows(t *testing.T) {
	ab := &retrieval.SourceBound{Scopes: []retrieval.BoundScope{
		scope("a", "pa"), scope("b", "pb"),
	}}
	bc := &retrieval.SourceBound{Scopes: []retrieval.BoundScope{
		scope("b", "pb"), scope("c", "pc"),
	}}
	if retrieval.IntersectBounds(nil, nil) != nil {
		t.Fatal("two absent bounds intersect to no bound")
	}
	if got := retrieval.IntersectBounds(nil, ab); got == nil || len(got.Scopes) != 2 || got == ab {
		t.Fatalf("nil ∩ ab = %#v, want an owned copy of ab", got)
	}
	if got := retrieval.IntersectBounds(ab, nil); got == nil || len(got.Scopes) != 2 {
		t.Fatalf("ab ∩ nil = %#v", got)
	}
	got := retrieval.IntersectBounds(ab, bc)
	if got == nil || len(got.Scopes) != 1 ||
		!bytes.Equal(got.Scopes[0].SourceID, []byte("b")) {
		t.Fatalf("ab ∩ bc = %#v, want only b", got)
	}
	disjoint := retrieval.IntersectBounds(ab, &retrieval.SourceBound{
		Scopes: []retrieval.BoundScope{scope("c", "pc")},
	})
	if disjoint == nil || len(disjoint.Scopes) != 0 {
		t.Fatalf("disjoint bounds = %#v, want a present bound admitting nothing", disjoint)
	}
	if !ab.Admits([]byte("a"), []byte("pa")) || ab.Admits([]byte("a"), []byte("pb")) {
		t.Fatal("Admits must match the exact pair")
	}
	var absent *retrieval.SourceBound
	if !absent.Admits([]byte("anything"), []byte("anything")) || !absent.AdmitsSource([]byte("x")) {
		t.Fatal("an absent bound admits everything")
	}
}
