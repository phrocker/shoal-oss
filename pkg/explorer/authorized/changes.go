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
	"context"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// ChangeFeedRequest asks for the caller's authorized document change feed.
// Since is the last visible publication sequence the caller has already
// consumed; the feed returns visible publications strictly after it. Incarnation
// binds the cursor to one corpus so it cannot be replayed against another.
type ChangeFeedRequest struct {
	Since       uint64
	Limit       int
	Incarnation string
}

// ChangeFeedPage is one authorized, ordered, resumable window.
type ChangeFeedPage struct {
	// Changes are visible to this caller, ascending by Sequence, all with
	// Sequence > request.Since.
	Changes []explorer.DocumentChange
	// Next is the sequence of the last visible change delivered, or
	// request.Since when none were. It only ever advances along changes the
	// caller can see, so it never encodes how many withheld changes were
	// skipped. Replaying it resumes exactly at the first undelivered visible
	// change.
	Next uint64
	// More reports that at least one further visible change exists beyond this
	// page. It is a bounded liveness hint and never a count.
	More bool
	// Incarnation is the corpus identity the caller must bind its cursor to.
	Incarnation string
}

// changeScanBatch bounds one base feed pull. The authorized feed may pull
// several batches to fill a page across withheld runs.
const changeScanBatch = explorer.MaxChangeLimit

// Changes returns the caller's authorized document change feed.
//
// Authorization posture. Every candidate change is passed through the same
// exact-revision catalog gate the read path enforces: a change is delivered
// only when this identity holds a policy registration for that precise revision
// and its rule authorizes listing. A caller therefore learns nothing about
// publications to documents it cannot see.
//
// No withheld count is reported, and that is a deliberate departure from the
// Documents and Retrieve listings, which disclose a corpus-wide suppressed
// count (PR #292). A feed is polled repeatedly with an advancing cursor, so a
// per-page withheld count -- or, equivalently, a cursor that advanced past
// withheld changes -- would let a caller reconstruct the timing and volume of
// other users' private writes at sequence resolution, a far sharper oracle than
// the one-shot corpus count #292 accepted. Instead the cursor advances only
// along visible changes and More is a bare liveness hint, so a caught-up caller
// (More false, no visible tail) can never mistake withheld activity for its own
// silence, and can never quantify it.
func (c *Client) Changes(
	ctx context.Context, request ChangeFeedRequest,
) (ChangeFeedPage, error) {
	reader, ok := c.base.(explorer.ChangeReader)
	if !ok {
		return ChangeFeedPage{}, shoal.NewError(
			shoal.ErrorUnavailable, "change feed is unavailable")
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationList)
	if err != nil {
		return ChangeFeedPage{}, err
	}

	pageSize := request.Limit
	if pageSize <= 0 {
		pageSize = explorer.DefaultChangeLimit
	}
	if pageSize > explorer.MaxChangeLimit {
		pageSize = explorer.MaxChangeLimit
	}

	page := ChangeFeedPage{
		Changes: make([]explorer.DocumentChange, 0, pageSize),
		Next:    request.Since,
	}
	rawSince := request.Since
	more := false
scan:
	for {
		feed, err := reader.Changes(ctx, explorer.ChangeRequest{
			Since:               rawSince,
			Limit:               changeScanBatch,
			ExpectedIncarnation: request.Incarnation,
		})
		if err != nil {
			return ChangeFeedPage{}, err
		}
		page.Incarnation = feed.Incarnation
		for index := range feed.Changes {
			change := feed.Changes[index]
			visible, err := c.changeVisible(ctx, decision, change, now)
			if err != nil {
				return ChangeFeedPage{}, err
			}
			if !visible {
				continue
			}
			// A visible change found once the page is full proves more visible
			// changes remain. It is not delivered, so Next stays at the last
			// delivered change and the next poll resumes exactly here.
			if len(page.Changes) == pageSize {
				more = true
				break scan
			}
			page.Changes = append(page.Changes, cloneDocumentChange(change))
			page.Next = change.Sequence
		}
		rawSince = feed.Cursor
		if !feed.More {
			// The base feed is exhausted to Head, so the page holds every
			// visible change and the caller is caught up on what it can see.
			break
		}
	}
	if err := guard.Check(ctx); err != nil {
		return ChangeFeedPage{}, err
	}
	page.More = more
	return page, nil
}

// changeVisible applies the exact-revision authorization gate. It is the same
// decision the Documents listing and the Document read enforce: presence of a
// registration for this precise revision, then its rule under OperationList.
func (c *Client) changeVisible(
	ctx context.Context,
	decision auth.Decision,
	change explorer.DocumentChange,
	now time.Time,
) (bool, error) {
	if err := validateChange(change); err != nil {
		return false, inconsistentBase()
	}
	registration, ok, err := c.policyStore.Revision(
		ctx, change.Document.ID, change.Revision.ID)
	if err != nil {
		return false, policyCatalogReadError(ctx, err)
	}
	if !ok {
		// No registration for this exact revision: withheld from every caller,
		// exactly as an uncataloged current revision is withheld from the
		// Documents listing. Not counted, never named.
		return false, nil
	}
	if registration.DocumentID != change.Document.ID ||
		registration.RevisionID != change.Revision.ID {
		return false, inconsistentBase()
	}
	allowed, err := ruleAllows(
		registration.Rule, decision, auth.OperationList, now)
	if err != nil {
		return false, err
	}
	return allowed, nil
}

func validateChange(change explorer.DocumentChange) error {
	if change.Kind != explorer.ChangeKindDocumentPublished {
		return inconsistentBase()
	}
	if change.Sequence == 0 {
		return inconsistentBase()
	}
	if err := change.Document.Validate(); err != nil {
		return err
	}
	if err := change.Revision.Validate(); err != nil {
		return err
	}
	if change.Document.RevisionID != change.Revision.ID ||
		change.Revision.DocumentID != change.Document.ID {
		return inconsistentBase()
	}
	return nil
}

func cloneDocumentChange(change explorer.DocumentChange) explorer.DocumentChange {
	change.Document = cloneDocument(change.Document)
	change.Revision = cloneRevision(change.Revision)
	return change
}
