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

package teamoverview_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized/authorizedtest"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/teamoverview"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	teamLabelledSession shoal.ID = "interaction.session_team_labelled"
	teamPlainSession    shoal.ID = "interaction.session_team_plain"
)

// labelRoster is a one-person team whose member is the recorder, with no
// agents or fleet actions, so the overview's only activity is the recorder's
// sessions.
type labelRoster struct{}

func (labelRoster) BoundedNeighborhood(
	context.Context, explorer.BoundedNeighborhoodRequest,
) (explorer.BoundedNeighborhood, error) {
	return explorer.BoundedNeighborhood{Neighborhood: explorer.Neighborhood{
		Nodes: []graph.Node{
			{ID: "label-team", Kind: teamoverview.KindTeam,
				Properties: shoal.Metadata{"name": "Label Team"}},
			{ID: "label-person", Kind: teamoverview.KindPerson,
				Properties: shoal.Metadata{
					"name": "Recorder", "subject_id": authorizedtest.RecorderSubject,
				}},
		},
		Edges: []graph.Edge{{
			ID: "label-member", From: "label-person", To: "label-team",
			Type: teamoverview.RelationMemberOf,
		}},
	}}, nil
}

func (labelRoster) List(context.Context, fleet.ListRequest) (fleet.ListPage, error) {
	return fleet.ListPage{}, nil
}

func (labelRoster) TeamActions(
	context.Context, fleet.TeamActionListRequest,
) (fleet.ActionPage, error) {
	return fleet.ActionPage{}, nil
}

// consumedInteractions records exactly what the authorized client handed the
// team overview, so the test checks the records the view is built from as
// well as the view itself.
type consumedInteractions struct {
	source  teamoverview.InteractionSource
	records []explorer.InteractionRecord
}

func (c *consumedInteractions) InteractionRecordsPage(
	ctx context.Context, after shoal.ID, limit uint32,
) (explorer.InteractionRecordPage, error) {
	page, err := c.source.InteractionRecordsPage(ctx, after, limit)
	c.records = append(c.records, page.Records...)
	return page, err
}

// TestOverviewWithholdsLabelsFromTeammatesWhoDidNotRecordThem: team overview
// is cross-principal by design, so a teammate sees the recorder's sessions,
// but the records it is built from carry no label expression for that
// teammate, exactly as for a session recorded without labels.
func TestOverviewWithholdsLabelsFromTeammatesWhoDidNotRecordThem(t *testing.T) {
	f := authorizedtest.New(t)
	f.Record(t, authorizedtest.RecorderSubject, teamLabelledSession,
		authorizedtest.SecretLabels)
	f.Record(t, authorizedtest.RecorderSubject, teamPlainSession, nil)

	overview := func(subject string) ([]byte, []explorer.InteractionRecord) {
		t.Helper()
		consumed := &consumedInteractions{source: f.Client}
		service, err := teamoverview.NewService(teamoverview.Config{
			Graph: labelRoster{}, Agents: labelRoster{}, Actions: labelRoster{},
			Interactions: consumed, Resolver: f.Authority.Resolver(),
			Clock: func() time.Time { return time.Now().UTC() },
		})
		if err != nil {
			t.Fatal(err)
		}
		response, err := service.Overview(f.Context(t, subject), teamoverview.Request{
			TeamID:   "label-team",
			SourceID: authorizedtest.SourceID, PolicyID: authorizedtest.PolicyID,
		})
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, activity := range response.Activities {
			seen[activity.ID] = true
		}
		if !seen[string(teamLabelledSession)] || !seen[string(teamPlainSession)] {
			t.Fatalf("%s overview lost sessions: %+v", subject, response.Activities)
		}
		encoded, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		return encoded, consumed.records
	}

	body, records := overview(authorizedtest.ReaderSubject)
	if leaked := authorizedtest.Leaks(body); len(leaked) != 0 {
		t.Fatalf("team overview leaked %v: %s", leaked, body)
	}
	consumedBytes, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	if leaked := authorizedtest.Leaks(consumedBytes); len(leaked) != 0 {
		t.Fatalf("team overview was built from records carrying %v", leaked)
	}
	// The recorder's own overview is built from records that keep the labels.
	_, recorderRecords := overview(authorizedtest.RecorderSubject)
	recorderBytes, err := json.Marshal(recorderRecords)
	if err != nil {
		t.Fatal(err)
	}
	if len(authorizedtest.Leaks(recorderBytes)) != len(authorizedtest.SecretLabels) {
		t.Fatalf("recorder's overview records lost their labels: %s", recorderBytes)
	}

	// The labelled record the teammate's overview consumed is the unlabelled
	// record as its own recorder receives it, which passes through no
	// withholding and so is exactly a never-labelled record.
	find := func(records []explorer.InteractionRecord, id shoal.ID) explorer.InteractionRecord {
		for _, record := range records {
			if record.Summary.SessionID == id {
				return record
			}
		}
		t.Fatalf("record %s missing", id)
		return explorer.InteractionRecord{}
	}
	labelled := find(records, teamLabelledSession)
	plain := find(recorderRecords, teamPlainSession)
	labelled.Summary.SessionID, labelled.Session.ID = plain.Summary.SessionID, plain.Session.ID
	labelled.Summary.RecordedAt = plain.Summary.RecordedAt
	labelled.Session.RecordedAt = plain.Session.RecordedAt
	if !reflect.DeepEqual(labelled, plain) {
		t.Fatalf("withheld record differs from an unlabelled one:\n%#v\n%#v",
			labelled, plain)
	}
	labelledBytes, err := json.Marshal(labelled)
	if err != nil {
		t.Fatal(err)
	}
	plainBytes, err := json.Marshal(plain)
	if err != nil {
		t.Fatal(err)
	}
	if string(labelledBytes) != string(plainBytes) {
		t.Fatalf("withheld record bytes differ:\n%s\n%s", labelledBytes, plainBytes)
	}
}
