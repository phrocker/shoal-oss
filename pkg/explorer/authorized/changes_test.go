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
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// seedInterleaved ingests a visible (policyA) document, a hidden (policyB)
// document, then another visible document, so the shared publication sequence
// is 1=visible, 2=hidden, 3=visible. It returns the two visible document IDs.
func seedInterleaved(t *testing.T, f *fixture) (shoal.ID, shoal.ID) {
	t.Helper()
	first, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///feed-visible-1.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "visible one",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.clientB.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///feed-hidden.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "hidden secret",
	}); err != nil {
		t.Fatal(err)
	}
	second, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///feed-visible-2.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "visible two",
	})
	if err != nil {
		t.Fatal(err)
	}
	return first.Document.ID, second.Document.ID
}

func TestChangesExcludesUnauthorizedPublicationsWithoutLeakingThem(t *testing.T) {
	f := newFixture(t)
	visible1, visible2 := seedInterleaved(t, f)

	page, err := f.clientA.Changes(f.alice(t), authorized.ChangeFeedRequest{Since: 0})
	if err != nil {
		t.Fatal(err)
	}

	// Alice sees exactly the two policyA publications and nothing about the
	// interleaved policyB publication at sequence 2.
	if len(page.Changes) != 2 {
		t.Fatalf("visible changes = %d, want 2 (%+v)", len(page.Changes), page.Changes)
	}
	if page.Changes[0].Document.ID != visible1 ||
		page.Changes[1].Document.ID != visible2 {
		t.Fatalf("visible docs = %s,%s want %s,%s",
			page.Changes[0].Document.ID, page.Changes[1].Document.ID,
			visible1, visible2)
	}
	// The cursor advances only along visible changes: it is the sequence of the
	// last visible publication (3), not the hidden one (2) and not a raw head.
	// Because Next lands on 3, a caller cannot even infer that sequence 2
	// existed and was withheld.
	if page.Next != 3 {
		t.Fatalf("Next = %d, want 3 (last visible sequence)", page.Next)
	}
	if page.More {
		t.Fatalf("More = true, want false: alice has seen every visible change")
	}

	// Resuming from the returned cursor yields nothing new and still reports no
	// withheld activity: a caught-up caller cannot distinguish "nothing changed"
	// from "changes it may not see occurred".
	resumed, err := f.clientA.Changes(f.alice(t), authorized.ChangeFeedRequest{
		Since:       page.Next,
		Incarnation: page.Incarnation,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed.Changes) != 0 || resumed.More {
		t.Fatalf("resumed = %+v, want empty and More=false", resumed)
	}
}

func TestChangesResumesExactlyAcrossWithheldPublications(t *testing.T) {
	f := newFixture(t)
	visible1, visible2 := seedInterleaved(t, f)

	// A one-item page must return the first visible change and, because a second
	// visible change (sequence 3) still exists beyond the withheld sequence 2,
	// report More without disclosing the withheld item.
	first, err := f.clientA.Changes(f.alice(t), authorized.ChangeFeedRequest{
		Since: 0,
		Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Changes) != 1 || first.Changes[0].Document.ID != visible1 {
		t.Fatalf("first page = %+v, want single visible doc %s", first.Changes, visible1)
	}
	if first.Next != 1 {
		t.Fatalf("first Next = %d, want 1", first.Next)
	}
	if !first.More {
		t.Fatal("More = false, want true: a later visible change remains")
	}

	// Resuming from the visible cursor delivers the next visible change exactly,
	// skipping the withheld sequence 2 with no gap and no duplicate.
	second, err := f.clientA.Changes(f.alice(t), authorized.ChangeFeedRequest{
		Since:       first.Next,
		Limit:       1,
		Incarnation: first.Incarnation,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Changes) != 1 || second.Changes[0].Document.ID != visible2 {
		t.Fatalf("second page = %+v, want single visible doc %s", second.Changes, visible2)
	}
	if second.Changes[0].Sequence != 3 {
		t.Fatalf("second change sequence = %d, want 3 (withheld 2 skipped)",
			second.Changes[0].Sequence)
	}
	if second.More {
		t.Fatal("More = true, want false: no visible change remains")
	}
}

func TestChangesAdminSeesBothPartitions(t *testing.T) {
	f := newFixture(t)
	seedInterleaved(t, f)

	page, err := f.clientA.Changes(f.admin(t), authorized.ChangeFeedRequest{Since: 0})
	if err != nil {
		t.Fatal(err)
	}
	// Admin holds both policies, so every publication is visible and the cursor
	// reaches the true head. This proves the withholding above is authorization,
	// not an artifact of the feed dropping changes.
	if len(page.Changes) != 3 {
		t.Fatalf("admin changes = %d, want 3", len(page.Changes))
	}
	if page.Next != 3 {
		t.Fatalf("admin Next = %d, want 3", page.Next)
	}
}

func TestChangesForeignIncarnationResynchronises(t *testing.T) {
	f := newFixture(t)
	seedInterleaved(t, f)

	_, err := f.clientA.Changes(f.alice(t), authorized.ChangeFeedRequest{
		Since:       0,
		Incarnation: "0000000000000000000000000000000000000000000000000000000000000000",
	})
	if err == nil {
		t.Fatal("expected resynchronise conflict for a foreign incarnation, got nil")
	}
	if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("error code = %v, want conflict", err)
	}
}
