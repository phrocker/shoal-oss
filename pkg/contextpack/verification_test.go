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

package contextpack

import (
	"context"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestVerifyResultReturnsCompleteEvidenceAndRejectsSnapshotDrift(
	t *testing.T,
) {
	client, request, response, pins := embeddedFixture(t)
	builder := Builder{Reader: client}
	pack, err := builder.Build(context.Background(), InitialRequest{
		Request: request, Response: response, Pins: pins,
	})
	if err != nil {
		t.Fatal(err)
	}
	evidenceIDs := make([]shoal.ID, 0, len(pack.Evidence()))
	for _, anchor := range pack.Evidence() {
		evidenceIDs = append(evidenceIDs, anchor.ID())
	}
	issue, err := inference.NewIssue(
		inference.IssueUnsupported, "input", "reason", evidenceIDs)
	if err != nil {
		t.Fatal(err)
	}
	result, err := inference.NewInferenceResult(
		pack, nil, []inference.Issue{issue},
		pack.Snapshot().AsOf().Add(time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	reader := &verificationSnapshotReader{
		Explorer: client,
		snapshot: explorer.Snapshot{
			ID: string(pack.Snapshot().ID()), AsOf: pack.Snapshot().AsOf(),
		},
		authorization: pack.Authorization(),
	}
	verified, err := (Builder{Reader: reader}).VerifyResult(
		context.Background(), pack, result)
	if err != nil {
		t.Fatal(err)
	}
	if len(verified.Anchors()) != len(pack.Evidence()) {
		t.Fatalf(
			"verified anchors = %d, want %d",
			len(verified.Anchors()), len(pack.Evidence()),
		)
	}
	for _, anchor := range verified.Anchors() {
		reference, err := anchor.EvidenceReference()
		if err != nil {
			t.Fatal(err)
		}
		if reference.AnchorID != anchor.Anchor().ID() ||
			len(reference.NodeIDs) == 0 {
			t.Fatalf("evidence reference = %+v", reference)
		}
	}
	otherAuthorization, err := inference.NewAuthPin(
		"different-authorization",
		pack.Authorization().ExpiresAt(),
	)
	if err != nil {
		t.Fatal(err)
	}
	reader.authorization = otherAuthorization
	if _, err := (Builder{Reader: reader}).VerifyResult(
		context.Background(), pack, result,
	); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("authorization drift verification error = %v", err)
	}
	reader.authorization = pack.Authorization()
	if _, err := client.Ingest(context.Background(), explorer.Source{
		URI: "memory://snapshot-drift", MediaType: explorer.MediaTypeText,
		Content: "later publication",
	}); err != nil {
		t.Fatal(err)
	}
	reader.snapshot, err = client.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (Builder{Reader: reader}).VerifyResult(
		context.Background(), pack, result,
	); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("snapshot drift verification error = %v", err)
	}
}

type verificationSnapshotReader struct {
	*explorer.Explorer
	snapshot      explorer.Snapshot
	authorization inference.AuthPin
}

func (r *verificationSnapshotReader) Snapshot(
	context.Context,
) (explorer.Snapshot, error) {
	return r.snapshot, nil
}

func (r *verificationSnapshotReader) ValidateAuthorization(
	_ context.Context,
	pin inference.AuthPin,
) error {
	if pin.Fingerprint() != r.authorization.Fingerprint() ||
		!pin.ExpiresAt().Equal(r.authorization.ExpiresAt()) {
		return shoal.NewError(
			shoal.ErrorUnauthorized,
			"authorization pin does not match verification reader",
		)
	}
	return nil
}
