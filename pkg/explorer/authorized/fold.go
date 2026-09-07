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

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type foldStore interface {
	FoldInteractions(context.Context, explorer.FoldRequest) (explorer.FoldResult, error)
	RehydrateFold(context.Context, shoal.ID) (interaction.Fold, error)
	Folds(context.Context) ([]explorer.FoldSummary, error)
}

type foldPageStore interface {
	FoldsPage(
		context.Context, shoal.ID, uint32,
	) (explorer.FoldSummaryPage, error)
}

// FoldInteractions creates a native provenance fold only after every named
// session and all source evidence it touched have been authorized for the
// current caller.
func (c *Client) FoldInteractions(
	ctx context.Context, request explorer.FoldRequest,
) (explorer.FoldResult, error) {
	store, err := c.foldStore()
	if err != nil {
		return explorer.FoldResult{}, err
	}
	_, guard, _, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return explorer.FoldResult{}, err
	}
	for _, sessionID := range request.SessionIDs {
		if _, err := c.Interaction(ctx, sessionID); err != nil {
			return explorer.FoldResult{}, err
		}
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.FoldResult{}, err
	}
	result, err := store.FoldInteractions(ctx, request)
	if err != nil {
		return explorer.FoldResult{}, directBaseError(err)
	}
	// Once the durable mutation has succeeded, return its exact outcome even
	// if cancellation races afterward; reporting a failure would falsely imply
	// that retrying cannot encounter the already-created content-addressed fold.
	return result, nil
}

// Folds lists only folds whose complete member provenance is currently
// authorized. A revoked member makes the fold disappear rather than leaking
// its existence or stale visibility.
func (c *Client) Folds(ctx context.Context) ([]explorer.FoldSummary, error) {
	store, err := c.foldStore()
	if err != nil {
		return nil, err
	}
	_, guard, _, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return nil, err
	}
	values, err := store.Folds(ctx)
	if err != nil {
		return nil, directBaseError(err)
	}
	visible := make([]explorer.FoldSummary, 0, len(values))
	for _, value := range values {
		fold, readErr := store.RehydrateFold(ctx, value.FoldID)
		if readErr != nil {
			if shoal.IsErrorCode(readErr, shoal.ErrorNotFound) ||
				shoal.IsErrorCode(readErr, shoal.ErrorUnavailable) ||
				shoal.IsErrorCode(readErr, shoal.ErrorConflict) {
				continue
			}
			return nil, directBaseError(readErr)
		}
		memberVisible, memberErr := c.foldMembersVisible(ctx, fold)
		if memberErr != nil {
			return nil, memberErr
		}
		if memberVisible {
			visible = append(visible, value)
		}
	}
	if err := guard.Check(ctx); err != nil {
		return nil, err
	}
	return visible, nil
}

// FoldsPage authorizes at most one bounded raw page of fold summaries.
func (c *Client) FoldsPage(
	ctx context.Context, after shoal.ID, limit uint32,
) (explorer.FoldSummaryPage, error) {
	store, err := c.foldStore()
	if err != nil {
		return explorer.FoldSummaryPage{}, err
	}
	pager, ok := store.(foldPageStore)
	if !ok || isNilDependency(pager) {
		return explorer.FoldSummaryPage{}, shoal.NewError(
			shoal.ErrorUnavailable,
			"underlying Explorer has no bounded fold page capability",
		)
	}
	_, guard, _, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return explorer.FoldSummaryPage{}, err
	}
	page, err := pager.FoldsPage(ctx, after, limit)
	if err != nil {
		return explorer.FoldSummaryPage{}, directBaseError(err)
	}
	visible := explorer.FoldSummaryPage{NextAfter: page.NextAfter}
	for _, value := range page.Folds {
		fold, readErr := store.RehydrateFold(ctx, value.FoldID)
		if readErr != nil {
			if shoal.IsErrorCode(readErr, shoal.ErrorNotFound) ||
				shoal.IsErrorCode(readErr, shoal.ErrorConflict) {
				continue
			}
			return explorer.FoldSummaryPage{}, directBaseError(readErr)
		}
		memberVisible, memberErr := c.foldMembersVisible(ctx, fold)
		if memberErr != nil {
			return explorer.FoldSummaryPage{}, memberErr
		}
		if memberVisible {
			visible.Folds = append(visible.Folds, value)
		}
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.FoldSummaryPage{}, err
	}
	return visible, nil
}

// RehydrateFold returns the exact retrieved/cited split only when every folded
// session remains visible to the current caller.
func (c *Client) RehydrateFold(
	ctx context.Context, foldID shoal.ID,
) (interaction.Fold, error) {
	store, err := c.foldStore()
	if err != nil {
		return interaction.Fold{}, err
	}
	_, guard, _, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return interaction.Fold{}, err
	}
	fold, err := store.RehydrateFold(ctx, foldID)
	if err != nil {
		return interaction.Fold{}, directBaseError(err)
	}
	visible, err := c.foldMembersVisible(ctx, fold)
	if err != nil {
		return interaction.Fold{}, err
	}
	if !visible {
		return interaction.Fold{}, auth.ObjectNotFound()
	}
	if err := guard.Check(ctx); err != nil {
		return interaction.Fold{}, err
	}
	return fold, nil
}

func (c *Client) foldMembersVisible(
	ctx context.Context, fold interaction.Fold,
) (bool, error) {
	for _, member := range fold.Members {
		if _, err := c.Interaction(ctx, member.SessionID); err != nil {
			if shoal.IsErrorCode(err, shoal.ErrorNotFound) ||
				shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

func (c *Client) foldStore() (foldStore, error) {
	store, ok := c.base.(foldStore)
	if !ok || isNilDependency(store) {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"underlying Explorer has no provenance fold store",
		)
	}
	return store, nil
}
