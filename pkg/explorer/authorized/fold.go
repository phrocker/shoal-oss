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
	_, guard, _, err := c.begin(ctx, auth.OperationConnect)
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
	fold, err := store.RehydrateFold(ctx, result.FoldID)
	if err != nil {
		return committedFoldFailure(directBaseError(err))
	}
	if err := validateDurableFoldResult(result, fold); err != nil {
		return committedFoldFailure(err)
	}
	// The durable winner is authoritative for retries. Reauthorize every one
	// of its canonical members rather than only the caller-supplied request.
	for _, member := range fold.Members {
		if _, err := c.Interaction(ctx, member.SessionID); err != nil {
			return committedFoldFailure(err)
		}
	}
	if err := guard.Check(ctx); err != nil {
		return committedFoldFailure(err)
	}
	return result, nil
}

func validateDurableFoldResult(
	result explorer.FoldResult,
	fold interaction.Fold,
) error {
	canonical, err := fold.Canonical()
	if err != nil {
		return err
	}
	foldID, err := canonical.ID()
	if err != nil {
		return err
	}
	matchesID := result.FoldID == foldID
	if !matchesID {
		legacy := canonical
		legacy.Members = make([]interaction.FoldMember, len(canonical.Members))
		for index, member := range canonical.Members {
			legacy.Members[index] = member
			legacy.Members[index].TouchedEdgeIDs = nil
		}
		legacyID, legacyErr := legacy.ID()
		if legacyErr != nil {
			return legacyErr
		}
		matchesID = result.FoldID == legacyID
	}
	var retrieved, cited []shoal.ID
	visibilitySets := make([][]string, 0, len(canonical.Members))
	for _, member := range canonical.Members {
		retrieved = append(retrieved, member.RetrievedNodeIDs...)
		cited = append(cited, member.CitedNodeIDs...)
		visibilitySets = append(visibilitySets, member.Visibility)
	}
	visibility, err := interaction.Conjoin(visibilitySets...)
	if err != nil {
		return err
	}
	if !matchesID ||
		!result.FoldedAt.Equal(canonical.FoldedAt) ||
		result.MemberCount != len(canonical.Members) ||
		result.RetrievedCount != countDistinctIDs(retrieved) ||
		result.CitedCount != countDistinctIDs(cited) ||
		result.Visibility != interaction.Expression(visibility) {
		return shoal.NewError(
			shoal.ErrorConflict,
			"fold result does not match its durable record",
		)
	}
	return nil
}

func countDistinctIDs(values []shoal.ID) int {
	unique := make(map[shoal.ID]struct{}, len(values))
	for _, value := range values {
		unique[value] = struct{}{}
	}
	return len(unique)
}

func committedFoldFailure(err error) (explorer.FoldResult, error) {
	return explorer.FoldResult{}, explorer.MarkCommittedInteraction(err)
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
		allowed, authorizationErr := c.foldMembersVisible(ctx, fold)
		if authorizationErr != nil {
			return nil, authorizationErr
		}
		if allowed {
			visible = append(visible, value)
		}
	}
	if err := guard.Check(ctx); err != nil {
		return nil, err
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
		if shoal.IsErrorCode(err, shoal.ErrorNotFound) ||
			shoal.IsErrorCode(err, shoal.ErrorUnavailable) ||
			shoal.IsErrorCode(err, shoal.ErrorConflict) {
			return interaction.Fold{}, auth.ObjectNotFound()
		}
		return interaction.Fold{}, directBaseError(err)
	}
	allowed, err := c.foldMembersVisible(ctx, fold)
	if err != nil {
		return interaction.Fold{}, err
	}
	if !allowed {
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
			if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

func (c *Client) foldStore() (FoldStore, error) {
	if isNilDependency(c.foldSource) {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"trusted provenance fold store is unavailable",
		)
	}
	return c.foldSource, nil
}
