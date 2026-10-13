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

package webapi

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func testBound(source, policy string) *retrieval.SourceBound {
	return &retrieval.SourceBound{Scopes: []retrieval.BoundScope{{
		SourceID: []byte(source), PolicyID: []byte(policy),
	}}}
}

// TestRemoteRetrievalRefusesASourceBound pins that the remote web client,
// whose wire query has no bound field (#560), refuses a bounded query at both
// the service and the encoder instead of sending it unbounded.
func TestRemoteRetrievalRefusesASourceBound(t *testing.T) {
	request := RetrievalRequest{Query: retrieval.Request{
		Text: "anything", TopK: 1, Bound: testBound("s", "p"),
	}}
	if _, err := json.Marshal(request); err == nil {
		t.Fatal("encoding a bounded retrieval request must fail, not drop the bound")
	}
	_, err := (&RemoteService{}).Retrieve(context.Background(), request)
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("remote bounded retrieval = %v, want unavailable", err)
	}
	request.Query.Bound = nil
	if _, err := json.Marshal(request); err != nil {
		t.Fatalf("an unbounded request must still encode: %v", err)
	}
}

// TestAskRequestBoundIsNeverDecoded pins that a chat body cannot set a bound:
// it is set in process by an executor carrying an invocation's scope.
func TestAskRequestBoundIsNeverDecoded(t *testing.T) {
	var input AskRequest
	if err := json.Unmarshal(
		[]byte(`{"question":"q","Bound":{"Scopes":[{"SourceID":"cw==","PolicyID":"cA=="}]}}`),
		&input); err != nil {
		t.Fatal(err)
	}
	if input.Bound != nil {
		t.Fatal("a request body set the retrieval bound")
	}
}

// TestChatCacheIdentitySeparatesBounds pins that a run cached for one bound,
// or none, is never replayed for another: its tools read what that bound
// allowed. Unbounded identities are unchanged from before bounds existed.
func TestChatCacheIdentitySeparatesBounds(t *testing.T) {
	unbounded := chatCacheIdentity("", 0, nil, nil)
	if unbounded != "web-chat-v1::0" {
		t.Fatalf("unbounded identity changed: %q", unbounded)
	}
	boundA := chatCacheIdentity("", 0, nil, testBound("a", "pa"))
	boundB := chatCacheIdentity("", 0, nil, testBound("b", "pb"))
	empty := chatCacheIdentity("", 0, nil, &retrieval.SourceBound{})
	// Length framing: ("ab","c") and ("a","bc") must not collide.
	framedLeft := chatCacheIdentity("", 0, nil, testBound("ab", "c"))
	framedRight := chatCacheIdentity("", 0, nil, testBound("a", "bc"))
	seen := map[string]string{}
	for name, identity := range map[string]string{
		"unbounded": unbounded, "a": boundA, "b": boundB, "empty": empty,
		"ab/c": framedLeft, "a/bc": framedRight,
	} {
		if other, ok := seen[identity]; ok {
			t.Fatalf("%s and %s share cache identity %q", name, other, identity)
		}
		seen[identity] = name
	}
	if again := chatCacheIdentity("", 0, nil, testBound("a", "pa")); again != boundA {
		t.Fatal("a bound's cache identity must be stable")
	}
}
