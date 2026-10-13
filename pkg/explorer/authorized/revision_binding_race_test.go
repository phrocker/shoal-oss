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

package authorized_test

import (
	"context"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// relabelAfterValidation runs after once, just after the first snapshot
// validation it serves succeeds: a relabel landing between a recording's
// validation and its write.
type relabelAfterValidation struct {
	inner authorized.EvidenceSnapshotValidator
	after func()
}

func (v *relabelAfterValidation) fire(err error) error {
	if err == nil && v.after != nil {
		after := v.after
		v.after = nil
		after()
	}
	return err
}

func (v *relabelAfterValidation) ValidateSnapshot(
	ctx context.Context, id shoal.ID, asOf time.Time, nodeIDs []shoal.ID,
) error {
	return v.fire(v.inner.ValidateSnapshot(ctx, id, asOf, nodeIDs))
}

func (v *relabelAfterValidation) ValidateEvidenceSnapshot(
	ctx context.Context, id shoal.ID, asOf time.Time,
	nodeIDs, edgeIDs []shoal.ID, references []interaction.EvidenceReference,
) error {
	return v.fire(v.inner.ValidateEvidenceSnapshot(
		ctx, id, asOf, nodeIDs, edgeIDs, references))
}

// TestRelabelRacingARecordingNeverOpensTheSession: the session's sources
// are read, authorized and validated over the labelled revision, and the
// relabel lands before the session is written. Either the write is refused
// (the corpus re-checks the snapshot when it writes) or the session is bound
// to the labelled revision; either way the reader cannot read it.
func TestRelabelRacingARecordingNeverOpensTheSession(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newBindingWorld(t, newFixture(t), store)
		inner, ok := trustedSnapshotValidator(w.f.base).(authorized.EvidenceSnapshotValidator)
		if !ok {
			t.Fatal("the corpus does not validate evidence snapshots")
		}
		var relabelled explorer.IngestResult
		validator := &relabelAfterValidation{inner: inner, after: func() {
			relabelled = w.relabel(w.client)
		}}
		selector := w.f.staticSelector(t, w.f.sourceA, w.f.policyA)
		recorder, err := authorized.NewClient(authorized.Config{
			Base:              w.f.base,
			InteractionWriter: trustedInteractionWriter(w.f.base),
			InteractionReader: trustedInteractionReader(w.f.base),
			SnapshotValidator: validator,
			FoldStore:         trustedFoldStore(w.f.base),
			Resolver:          w.f.authority.Resolver(),
			PolicySelector:    selector,
			PolicyStore:       store,
			GenerationReader:  w.f.reader,
			Clock:             w.f.clock.Now,
		})
		if err != nil {
			t.Fatal(err)
		}
		recordErr := w.tryRecord(recorder, bindingSession, w.closed.Document.ID)
		if relabelled.Revision.ID == "" {
			t.Fatal("the relabel did not race the recording")
		}
		_, err = w.client.InteractionRecord(w.as(w.reader), bindingSession)
		assertSameRefusal(t, "raced session", err, w.neverRecord(w.client))
		if recordErr != nil {
			return
		}
		recorded, err := store.InteractionRevisions(
			context.Background(), []shoal.ID{bindingSession})
		if err != nil {
			t.Fatal(err)
		}
		labelled := authorized.RevisionReference{
			DocumentID: w.closed.Document.ID, RevisionID: w.closed.Revision.ID,
		}
		for _, reference := range recorded[bindingSession] {
			if reference == labelled {
				return
			}
		}
		t.Fatalf("recorded %+v, which omits the labelled revision",
			recorded[bindingSession])
	})
}
