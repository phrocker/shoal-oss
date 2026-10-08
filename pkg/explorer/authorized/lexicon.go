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
	"fmt"
	"sort"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/lexicon"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	// MaxMentionBytes bounds the text ResolveMentions accepts.
	MaxMentionBytes = 4096
	// MaxMentionTokens bounds the tokens ResolveMentions accepts after
	// normalization.
	MaxMentionTokens = 32
	// MaxCandidateIDs is the fixed size of the one policy-store batch every
	// ResolveMentions call issues. A bundle is usable only when no text of
	// MaxMentionTokens tokens can produce more distinct candidates; with the
	// default lexicon limits (8 tokens per term, 8 postings per term) the
	// worst case is 228 spans x 8 = 1824.
	MaxCandidateIDs = 2048
)

// lexiconSentinels pad every authorization batch to MaxCandidateIDs. They use
// lexicon.SentinelIDPrefix, which lexicon.Build refuses for any node, so no
// sentinel is ever a candidate, and a registration the store returns for one
// is never read.
var lexiconSentinels = func() []shoal.ID {
	ids := make([]shoal.ID, MaxCandidateIDs)
	for index := range ids {
		ids[index] = shoal.ID(fmt.Sprintf("%s%04d", lexicon.SentinelIDPrefix, index))
	}
	return ids
}()

func mentionOverBound() error {
	return shoal.NewError(shoal.ErrorInvalidArgument,
		fmt.Sprintf("mention text exceeds %d bytes or %d tokens",
			MaxMentionBytes, MaxMentionTokens))
}

// ResolveMentions links mentions in text to the nodes of b that the caller may
// currently see, under the neighborhood operation.
//
// Every candidate the bundle reports is checked against the caller's current
// node rules before any choice is made. A node the caller may not see is
// dropped silently, exactly as a node absent from the catalog is, so a hidden
// entity matches as nothing: the result, the error, and the policy-store
// traffic are the same as for an unknown name. Selection (leftmost-longest,
// ambiguity) runs only over visible nodes.
//
// The store is read in exactly one Nodes call of exactly MaxCandidateIDs
// identifiers, padded with sentinels, whatever the text and whatever is
// hidden. Errors come only from the caller's own authorization, the input
// bound (one generic error for every caller), a bundle too large to bound, or
// the policy store.
func (c *Client) ResolveMentions(
	ctx context.Context,
	b *lexicon.Bundle,
	text string,
) ([]lexicon.Mention, error) {
	if b == nil {
		return nil, dependencyRequired("lexicon bundle")
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationNeighborhood)
	if err != nil {
		return nil, err
	}
	if b.CandidateBound(MaxMentionTokens) > MaxCandidateIDs {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument,
			"lexicon bundle exceeds the mention candidate bound")
	}
	if len(text) > MaxMentionBytes {
		return nil, mentionOverBound()
	}
	tokens := lexicon.Tokenize(text)
	if len(tokens) > MaxMentionTokens {
		return nil, mentionOverBound()
	}
	candidates := b.CandidatesForTokens(tokens)

	distinct := make(map[shoal.ID]struct{})
	for _, candidate := range candidates {
		for _, id := range candidate.NodeIDs {
			distinct[id] = struct{}{}
		}
	}
	if len(distinct) > MaxCandidateIDs {
		// Unreachable: CandidateBound was checked above. Failing closed with
		// the store untouched is still the only safe answer.
		return nil, inconsistentBase()
	}
	batch := make([]shoal.ID, 0, MaxCandidateIDs)
	for id := range distinct {
		batch = append(batch, id)
	}
	sort.Slice(batch, func(i, j int) bool { return batch[i] < batch[j] })
	batch = append(batch, lexiconSentinels[len(batch):]...)

	registrations, err := c.policyStore.Nodes(ctx, batch)
	if err != nil {
		return nil, policyCatalogReadError(ctx, err)
	}
	visible := make(map[shoal.ID]bool, len(distinct))
	for _, id := range batch[:len(distinct)] {
		registration, ok := registrations[id]
		if !ok {
			continue
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, auth.OperationNeighborhood, now)
		if err != nil {
			return nil, err
		}
		if allowed {
			visible[id] = true
		}
	}
	mentions := lexicon.Select(candidates, func(id shoal.ID) bool {
		return visible[id]
	})
	if err := guard.Check(ctx); err != nil {
		return nil, err
	}
	return mentions, nil
}

// LexiconScopeNodes returns, in input order, copies of the nodes the caller
// may currently see under the neighborhood operation. It is the filter for
// building a lexicon.ScopePinned bundle for the caller's scope: such a bundle
// is built from this output only, so its bytes hold nothing the caller cannot
// already see. The node data itself is the builder's; only visibility comes
// from the policy catalog, read in one batch.
func (c *Client) LexiconScopeNodes(
	ctx context.Context,
	nodes []graph.Node,
) ([]graph.Node, error) {
	decision, guard, now, err := c.begin(ctx, auth.OperationNeighborhood)
	if err != nil {
		return nil, err
	}
	ids := make([]shoal.ID, len(nodes))
	for index, node := range nodes {
		if err := shoal.ValidateRequiredID("graph node ID", node.ID); err != nil {
			return nil, err
		}
		ids[index] = node.ID
	}
	resolved, err := c.resolveNodes(ctx, ids)
	if err != nil {
		return nil, err
	}
	var visible []graph.Node
	for _, node := range nodes {
		registration, ok := resolved[node.ID]
		if !ok {
			continue
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, auth.OperationNeighborhood, now)
		if err != nil {
			return nil, err
		}
		if allowed {
			visible = append(visible, cloneGraphNode(node))
		}
	}
	if err := guard.Check(ctx); err != nil {
		return nil, err
	}
	return visible, nil
}
