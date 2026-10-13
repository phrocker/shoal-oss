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

// The authorized client marks the two refusals that leave nothing committed
// (explorer.MarkRefusedWithoutEffect), so an upload batch may report them per
// file and go on (#687). It marks nothing else, and not a refusal whose
// claim rollback failed.

import (
	"errors"
	"reflect"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestIngestMarksOnlyRefusalsWithoutEffect pins which authorized ingest
// errors carry the guarantee: the historical refusal (claim rolled back, no
// registration) and the reason-required refusal (refused before the claim),
// but not an authorization refusal, and never a success.
func TestIngestMarksOnlyRefusalsWithoutEffect(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		f := newFixture(t)
		client := f.declassificationClient(t, store, true)
		owner := f.labelIngester(t, "owner", "secret")

		const historical = "file:///refused/historical.txt"
		for _, content := range []string{"a", "b"} {
			if _, err := client.Ingest(owner, labelledSource(historical, content, "secret")); err != nil {
				t.Fatal(err)
			}
		}
		before := readCatalogState(t, store, historical,
			explorer.SourceDocumentID(historical))
		_, err := client.Ingest(owner, labelledSource(historical, "a", "secret"))
		requireHistoricalRefusal(t, err)
		if !explorer.IsRefusedWithoutEffect(err) {
			t.Fatalf("historical refusal is not marked: %v", err)
		}
		if after := readCatalogState(t, store, historical,
			explorer.SourceDocumentID(historical)); !reflect.DeepEqual(after, before) {
			t.Fatalf("a marked refusal changed the catalog:\nbefore %+v\nafter  %+v", before, after)
		}

		const narrowed = "file:///refused/narrowed.txt"
		if _, err := client.Ingest(owner, labelledSource(narrowed, "secret", "secret")); err != nil {
			t.Fatal(err)
		}
		_, err = client.Ingest(owner, labelledSource(narrowed, "public", ""))
		if !authorized.IsDeclassificationReasonRequired(err) ||
			!shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) ||
			!explorer.IsRefusedWithoutEffect(err) {
			t.Fatalf("reasonless narrowing = %v, want a marked reason-required refusal", err)
		}

		_, err = client.Ingest(f.labelIngester(t, "outsider"),
			labelledSource(narrowed, "public", ""))
		if !shoal.IsErrorCode(err, shoal.ErrorNotFound) || explorer.IsRefusedWithoutEffect(err) {
			t.Fatalf("outsider refusal = %v, want an unmarked not found", err)
		}
	})
}

// TestHistoricalRefusalWithFailedRollbackIsNotMarked pins that the guarantee
// is given only once the claim rollback succeeded: a failed rollback leaves
// the claim's state unknown, so the refusal (joined with the catalog error)
// stops a batch instead of being skipped.
func TestHistoricalRefusalWithFailedRollbackIsNotMarked(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		f := newFixture(t)
		failing := &failFinalizeStore{PolicyStore: store}
		client := f.newClient(t, f.base, failing, f.sourceA, f.policyA, nil)
		owner := f.labelIngester(t, "owner", "secret")
		const uri = "file:///refused/rollback.txt"
		for _, content := range []string{"a", "b"} {
			if _, err := client.Ingest(owner, labelledSource(uri, content, "")); err != nil {
				t.Fatal(err)
			}
		}
		failing.rollbackErr = errors.New("injected rollback failure")
		_, err := client.Ingest(owner, labelledSource(uri, "a", ""))
		if !errors.Is(err, authorized.ErrHistoricalRevision) {
			t.Fatalf("refusal with a failed rollback = %v, want it to keep the refusal", err)
		}
		if explorer.IsRefusedWithoutEffect(err) {
			t.Fatalf("refusal with a failed rollback is marked: %v", err)
		}
	})
}
