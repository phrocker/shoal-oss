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

package interaction_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type recorderSink struct {
	ensureErr error
	recordErr error
	ensured   int
	recorded  []interaction.Session
}

func (s *recorderSink) EnsureInteractionSink(context.Context) error {
	s.ensured++
	return s.ensureErr
}

func (s *recorderSink) RecordInteraction(
	_ context.Context, session interaction.Session,
) error {
	s.recorded = append(s.recorded, session)
	return s.recordErr
}

func TestProductRecorderIsFailClosedAndCanonical(t *testing.T) {
	ctx := context.Background()
	fixed := time.Date(2026, time.September, 5, 22, 0, 0, 123, time.UTC)
	sink := &recorderSink{}
	recorder, err := interaction.NewRecorder(ctx, sink)
	if err != nil {
		t.Fatal(err)
	}
	if sink.ensured != 1 {
		t.Fatalf("sink checks = %d", sink.ensured)
	}
	if err := recorder.SetClock(func() time.Time { return fixed }); err != nil {
		t.Fatal(err)
	}
	id, err := interaction.OperationSessionID(
		interaction.OperationRetrieval, "request-1", fixed)
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := recorder.Record(ctx, interaction.Session{
		ID:        id,
		Operation: interaction.OperationRetrieval,
		SeedNodeIDs: []shoal.ID{
			"span-b", "span-a", "span-b",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !recorded.RecordedAt.Equal(fixed) ||
		len(recorded.SeedNodeIDs) != 2 ||
		recorded.SeedNodeIDs[0] != "span-a" {
		t.Fatalf("canonical recorded session = %+v", recorded)
	}

	sink.recordErr = errors.New("durable sink unavailable")
	if _, err := recorder.Record(ctx, interaction.Session{
		ID:        "session-failing",
		Operation: interaction.OperationToolCall,
	}); !errors.Is(err, sink.recordErr) {
		t.Fatalf("record error = %v", err)
	}

	unavailable := &recorderSink{ensureErr: errors.New("read-only")}
	if _, err := interaction.NewRecorder(ctx, unavailable); !errors.Is(
		err, unavailable.ensureErr,
	) {
		t.Fatalf("setup error = %v", err)
	}
}

func TestGenericRetrievalHasNoInferenceNode(t *testing.T) {
	session := interaction.Session{
		ID:         "retrieval-session",
		RecordedAt: time.Unix(1700000000, 0).UTC(),
		Operation:  interaction.OperationRetrieval,
		Actor: interaction.ActorContext{
			SubjectID:  "subject",
			ActorID:    "agent",
			ClientID:   "client",
			OnBehalfOf: []shoal.ID{"delegate-a", "delegate-b"},
		},
		Reason:      mustReason(t, "retrieve_context", "answer user request"),
		SeedNodeIDs: []shoal.ID{"span-a"},
	}
	subgraph, err := session.Subgraph(func(shoal.ID) ([]string, error) {
		return []string{"ops"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	kinds := make(map[string]int)
	var sessionProperties map[string]string
	for _, node := range subgraph.Nodes {
		kinds[node.Kind]++
		if node.Kind == interaction.KindSession {
			sessionProperties = node.Properties
		}
	}
	if kinds[interaction.KindSession] != 1 ||
		kinds[interaction.KindInference] != 0 {
		t.Fatalf("node kinds = %v", kinds)
	}
	if sessionProperties[interaction.PropertyOperation] !=
		string(interaction.OperationRetrieval) ||
		sessionProperties[interaction.PropertySubjectID] != "subject" ||
		sessionProperties[interaction.PropertyActorID] != "agent" ||
		sessionProperties[interaction.PropertyClientID] != "client" ||
		sessionProperties[interaction.PropertyDelegationCount] != "2" ||
		sessionProperties[interaction.PropertyDelegationID] == "" ||
		sessionProperties[interaction.PropertyReasonCode] != "retrieve_context" ||
		sessionProperties[interaction.PropertyReasonDigest] == "" {
		t.Fatalf("session properties = %+v", sessionProperties)
	}
	for _, edge := range subgraph.Edges {
		if edge.Type == interaction.EdgeHasInference {
			t.Fatal("generic retrieval materialized an inference edge")
		}
	}
}

func mustReason(t *testing.T, code, detail string) interaction.Reason {
	t.Helper()
	reason, err := interaction.NewReason(code, detail)
	if err != nil {
		t.Fatal(err)
	}
	return reason
}
