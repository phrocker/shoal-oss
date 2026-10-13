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

package retrievalgrpc

import (
	"testing"

	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestRequestToProtoRefusesASourceBound pins that the wire, which has no
// field for a source bound (#560), refuses one rather than dropping it and
// letting the server retrieve unbounded.
func TestRequestToProtoRefusesASourceBound(t *testing.T) {
	_, err := requestToProto(retrieval.Request{
		Text: "anything", TopK: 1,
		Bound: &retrieval.SourceBound{Scopes: []retrieval.BoundScope{{
			SourceID: []byte("s"), PolicyID: []byte("p"),
		}}},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("bounded request to proto = %v, want unavailable", err)
	}
	if _, err := requestToProto(retrieval.Request{Text: "anything", TopK: 1}); err != nil {
		t.Fatalf("an unbounded request must still convert: %v", err)
	}
}
