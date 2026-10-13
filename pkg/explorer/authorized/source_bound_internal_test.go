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

package authorized

import (
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
)

// TestRuleWithinBound pins what "within a source bound" means for one access
// rule (#560): every term is an admitted scope, or a label policy on a source
// the rule itself anchors with an admitted scope.
func TestRuleWithinBound(t *testing.T) {
	policy := func(source string, grant []byte) auth.Policy {
		t.Helper()
		value, err := auth.NewPolicy(auth.PolicyConfig{
			AuthorizationDomain: []byte("bound-domain"),
			SourceID:            []byte(source), GrantPolicyID: grant, Epoch: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	label := func(source, name string) []byte {
		t.Helper()
		id, err := auth.LabelPolicyID([]byte(source), name)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	rule := func(policies ...auth.Policy) AccessRule {
		t.Helper()
		value, err := NewAccessRule(policies...)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	bound := func(pairs ...[2]string) *retrieval.SourceBound {
		value := &retrieval.SourceBound{Scopes: []retrieval.BoundScope{}}
		for _, pair := range pairs {
			value.Scopes = append(value.Scopes, retrieval.BoundScope{
				SourceID: []byte(pair[0]), PolicyID: []byte(pair[1]),
			})
		}
		return value
	}
	pA := policy("a", []byte("pa"))
	pB := policy("b", []byte("pb"))
	pAOther := policy("a", []byte("pq"))
	labelOnA := policy("a", label("a", "secret"))
	labelOnB := policy("b", label("b", "secret"))
	// A term on source a whose label policy encodes source b: not a label on
	// its own source, so it is not one the bound can treat as narrowing a.
	labelCrossed := policy("a", label("b", "secret"))

	onlyA := bound([2]string{"a", "pa"})
	both := bound([2]string{"a", "pa"}, [2]string{"b", "pb"})
	for name, test := range map[string]struct {
		rule  AccessRule
		bound *retrieval.SourceBound
		want  bool
	}{
		"no bound contains everything":       {rule(pB), nil, true},
		"an empty bound contains nothing":    {rule(pA), bound(), false},
		"exact scope":                        {rule(pA), onlyA, true},
		"own-source label":                   {rule(pA, labelOnA), onlyA, true},
		"another source":                     {rule(pB), onlyA, false},
		"another policy on the same source":  {rule(pAOther), onlyA, false},
		"a conjunction reaching outside":     {rule(pA, pB), onlyA, false},
		"a label on an unbound source":       {rule(pA, labelOnB), onlyA, false},
		"a label on a source not anchored":   {rule(pA, labelOnB), both, false},
		"a label on an anchored source":      {rule(pA, pB, labelOnB), both, true},
		"labels alone anchor nothing":        {rule(labelOnA), onlyA, false},
		"a label naming another source":      {rule(pA, labelCrossed), onlyA, false},
		"every term admitted across sources": {rule(pA, pB), both, true},
	} {
		t.Run(name, func(t *testing.T) {
			if got := ruleWithinBound(test.rule, test.bound); got != test.want {
				t.Fatalf("ruleWithinBound = %v, want %v", got, test.want)
			}
		})
	}
	if ruleWithinBound(AccessRule{}, onlyA) {
		t.Fatal("an invalid rule is within no bound")
	}
}
