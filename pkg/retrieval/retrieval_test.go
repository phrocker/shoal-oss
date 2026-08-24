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
	"context"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type stubRetriever struct{}

func (stubRetriever) Retrieve(context.Context, retrieval.Request) (retrieval.Response, error) {
	return retrieval.Response{RequestID: "request-1"}, nil
}

func TestRetrieverUsesTransportNeutralValues(t *testing.T) {
	var client retrieval.Retriever = stubRetriever{}
	response, err := client.Retrieve(context.Background(), retrieval.Request{
		Text:  "why did the deployment fail?",
		TopK:  5,
		Modes: []retrieval.Mode{retrieval.ModeTree, retrieval.ModeGraph},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.RequestID != "request-1" {
		t.Fatalf("unexpected request ID %q", response.RequestID)
	}
}

func TestRequestRejectsUnknownMode(t *testing.T) {
	request := retrieval.Request{Text: "query", Modes: []retrieval.Mode{"cells"}}
	if err := request.Validate(); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestRequestRejectsInvalidUTF8(t *testing.T) {
	tests := map[string]retrieval.Request{
		"text": {
			Text: string([]byte{0xff}),
		},
		"document ID": {
			Text:  "query",
			Scope: retrieval.Scope{DocumentIDs: []shoal.ID{shoal.ID(string([]byte{0xff}))}},
		},
		"node ID": {
			Text:  "query",
			Scope: retrieval.Scope{NodeIDs: []shoal.ID{shoal.ID(string([]byte{0xff}))}},
		},
	}
	for name, request := range tests {
		t.Run(name, func(t *testing.T) {
			if err := request.Validate(); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
				t.Fatalf("expected invalid argument, got %v", err)
			}
		})
	}
}

func TestRequestRejectsAsOfOutsideProtobufRange(t *testing.T) {
	for _, asOf := range []time.Time{
		time.Date(0, time.December, 31, 23, 59, 59, 0, time.UTC),
		time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC),
	} {
		request := retrieval.Request{Text: "query", AsOf: asOf}
		if err := request.Validate(); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			t.Fatalf("Validate() for %v = %v, want invalid argument", asOf, err)
		}
	}
}
