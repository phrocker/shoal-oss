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
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// SnapshotValidator verifies corpus frontiers pinned into interaction records.
type SnapshotValidator interface {
	ValidateSnapshot(
		context.Context, shoal.ID, time.Time, []shoal.ID,
	) error
}

// EvidenceSnapshotValidator additionally binds exact source edges to the
// pinned corpus frontier.
type EvidenceSnapshotValidator interface {
	SnapshotValidator
	ValidateEvidenceSnapshot(
		context.Context,
		shoal.ID,
		time.Time,
		[]shoal.ID,
		[]shoal.ID,
		[]interaction.EvidenceReference,
	) error
}

// Config supplies the trusted dependencies for an authorization-enforcing
// Explorer client.
type Config struct {
	Base explorer.Client
	// VectorScorer is an optional explicitly trusted scorer for authorized
	// vector retrieval validation. It is intentionally separate from Base:
	// Base responses are treated as untrusted and validated canonically.
	VectorScorer VectorScorer
	// InteractionWriter is the explicitly trusted durable sink for
	// interaction records. It is separate from Base because Base responses
	// and mutation acknowledgements are not authorization evidence.
	InteractionWriter explorer.InteractionWriter
	// InteractionReader is the explicitly trusted source for durable
	// interaction envelopes. It is intentionally separate from Base because
	// authorization decisions for derived views depend on the stored source
	// set and authorization fingerprint.
	InteractionReader explorer.InteractionReader
	// SnapshotValidator is the explicitly trusted verifier for historical
	// corpus frontiers pinned into interaction records.
	SnapshotValidator  SnapshotValidator
	Resolver           auth.Resolver
	PolicySelector     PolicySelector
	EdgePolicySelector EdgePolicySelector
	PolicyStore        PolicyStore
	GenerationReader   auth.GenerationReader
	Clock              func() time.Time
	// Mosaic optionally enables the sensitivity-domain co-occurrence budget
	// that defends against the mosaic effect. A zero MaxDomains disables it; a
	// nonzero MaxDomains requires PolicyStore to implement CoOccurrenceLedger
	// and a positive Window, so an enabled-but-unbacked control fails closed at
	// construction.
	Mosaic MosaicBudget
}

// Client enforces trusted-context authorization around an Explorer client.
type Client struct {
	base               explorer.Client
	vectorScorer       VectorScorer
	interactionSink    explorer.InteractionWriter
	interactionSource  explorer.InteractionReader
	snapshotValidator  SnapshotValidator
	resolver           auth.Resolver
	policySelector     PolicySelector
	edgePolicySelector EdgePolicySelector
	policyStore        PolicyStore
	generationReader   auth.GenerationReader
	clock              func() time.Time
	mosaic             MosaicBudget
	ledger             CoOccurrenceLedger
	mutationMu         sync.Mutex
	vectorMu           sync.Mutex
	budgetMu           sync.Mutex
	vectorAvailability authorizedVectorAvailabilityCache
}

type authorizedVectorAvailabilityCache struct {
	key       string
	checkedAt time.Time
	available bool
}

// NewClient validates every dependency and constructs an additive wrapper.
// An edge selector may be supplied explicitly or implemented by PolicySelector.
func NewClient(config Config) (*Client, error) {
	if isNilDependency(config.Base) {
		return nil, dependencyRequired("base Explorer client")
	}
	if isNilDependency(config.Resolver) {
		return nil, dependencyRequired("authorization resolver")
	}
	if isNilDependency(config.PolicySelector) {
		return nil, dependencyRequired("policy selector")
	}
	if isNilDependency(config.PolicyStore) {
		return nil, dependencyRequired("policy store")
	}
	if isNilDependency(config.GenerationReader) {
		return nil, dependencyRequired("generation reader")
	}
	if config.Clock == nil {
		return nil, dependencyRequired("clock")
	}
	hasInteractionWriter := !isNilDependency(config.InteractionWriter)
	hasInteractionReader := !isNilDependency(config.InteractionReader)
	hasSnapshotValidator := !isNilDependency(config.SnapshotValidator)
	if hasInteractionWriter &&
		(!hasInteractionReader || !hasSnapshotValidator) {
		return nil, dependencyRequired(
			"trusted interaction writer, reader, and snapshot validator")
	}
	if hasSnapshotValidator && !hasInteractionWriter {
		return nil, dependencyRequired("trusted interaction writer")
	}
	edgeSelector := config.EdgePolicySelector
	if isNilDependency(edgeSelector) {
		var ok bool
		edgeSelector, ok = config.PolicySelector.(EdgePolicySelector)
		if !ok || isNilDependency(edgeSelector) {
			return nil, dependencyRequired("edge policy selector")
		}
	}
	var ledger CoOccurrenceLedger
	if config.Mosaic.enabled() {
		if config.Mosaic.Window <= 0 {
			return nil, dependencyRequired("mosaic co-occurrence window")
		}
		var ok bool
		ledger, ok = config.PolicyStore.(CoOccurrenceLedger)
		if !ok || isNilDependency(ledger) {
			return nil, dependencyRequired("co-occurrence ledger")
		}
	}
	return &Client{
		base:               config.Base,
		vectorScorer:       config.VectorScorer,
		interactionSink:    config.InteractionWriter,
		interactionSource:  config.InteractionReader,
		snapshotValidator:  config.SnapshotValidator,
		resolver:           config.Resolver,
		policySelector:     config.PolicySelector,
		edgePolicySelector: edgeSelector,
		policyStore:        config.PolicyStore,
		generationReader:   config.GenerationReader,
		clock:              config.Clock,
		mosaic:             config.Mosaic,
		ledger:             ledger,
	}, nil
}

// Ingest authorizes and catalogs one immutable revision. A successful base
// commit is never reported as success until its exact policy registration is
// complete.
func (c *Client) Ingest(
	ctx context.Context,
	source explorer.Source,
) (
	returned explorer.IngestResult,
	returnedErr error,
) {
	decision, guard, now, err := c.begin(ctx, auth.OperationIngest)
	if err != nil {
		return explorer.IngestResult{}, err
	}
	ownedSource := cloneSource(source)
	rule, err := c.selectIngestRule(
		ctx, decision, cloneSource(ownedSource), now)
	if err != nil {
		return explorer.IngestResult{}, err
	}

	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	lease, err := c.policyStore.AcquireMutation(ctx)
	if err != nil {
		return explorer.IngestResult{}, policyCatalogWriteError(ctx, err)
	}
	defer lease.Release()
	if err := guard.Check(ctx); err != nil {
		return explorer.IngestResult{}, err
	}
	claim, err := c.claimSourceMutation(
		ctx, ownedSource.URI, decision, rule, now)
	if err != nil {
		return explorer.IngestResult{}, err
	}
	claimOutcome := sourceClaimRollback
	defer func() {
		cleanupContext := context.WithoutCancel(ctx)
		var cleanupErr error
		switch claimOutcome {
		case sourceClaimCommit:
			cleanupErr = c.policyStore.CommitSourceClaim(
				cleanupContext, claim)
		case sourceClaimPend:
			cleanupErr = c.policyStore.PendSourceClaim(
				cleanupContext, claim)
		case sourceClaimRollback:
			cleanupErr = c.policyStore.RollbackSourceClaim(
				cleanupContext, claim)
		}
		if cleanupErr != nil {
			catalogErr := policyCatalogWriteError(
				cleanupContext, cleanupErr)
			returned = explorer.IngestResult{}
			if returnedErr == nil {
				returnedErr = catalogErr
			} else {
				returnedErr = errors.Join(returnedErr, catalogErr)
			}
		}
	}()
	if err := guard.Check(ctx); err != nil {
		return explorer.IngestResult{}, err
	}
	result, err := c.base.Ingest(ctx, cloneSource(ownedSource))
	if err != nil {
		if explorer.IsIndeterminateCommit(err) {
			claimOutcome = sourceClaimPend
		}
		return explorer.IngestResult{}, err
	}
	if result.Disposition == explorer.IngestApplied {
		claimOutcome = sourceClaimCommit
	}
	view, err := c.base.Document(
		ctx, result.Document.ID, result.Revision.ID)
	if err != nil {
		return explorer.IngestResult{}, err
	}
	if view.Document.ID != result.Document.ID ||
		view.Document.RevisionID != result.Revision.ID ||
		view.Revision.ID != result.Revision.ID ||
		view.Revision.DocumentID != result.Document.ID {
		return explorer.IngestResult{}, inconsistentBase()
	}
	nodeIDs, err := documentViewNodeIDs(view)
	if err != nil {
		return explorer.IngestResult{}, inconsistentBase()
	}
	summaries, err := c.base.Documents(ctx)
	if err != nil {
		return explorer.IngestResult{}, err
	}
	current, err := exactRevisionIsCurrent(
		summaries, result.Document.ID, result.Revision.ID)
	if err != nil {
		return explorer.IngestResult{}, err
	}
	if !current {
		claimOutcome = sourceClaimRollback
	}
	var intrinsicEdges []graph.Edge
	if current {
		intrinsicEdges, err = c.intrinsicEdges(ctx, nodeIDs)
		if err != nil {
			return explorer.IngestResult{}, err
		}
	}
	digest, err := documentViewDigest(view)
	if err != nil {
		return explorer.IngestResult{}, inconsistentBase()
	}
	if err := c.policyStore.PutRevision(ctx, RevisionRegistration{
		DocumentID:     result.Document.ID,
		RevisionID:     result.Revision.ID,
		NodeIDs:        nodeIDs,
		IntrinsicEdges: intrinsicEdges,
		ContentDigest:  digest,
		Rule:           rule,
		Current:        current,
	}); err != nil {
		return explorer.IngestResult{}, policyCatalogWriteError(ctx, err)
	}
	if current {
		claimOutcome = sourceClaimCommit
	}
	sectionCount, spanCount := documentViewCounts(view)
	cloned := explorer.IngestResult{
		Disposition:  result.Disposition,
		Document:     cloneDocument(view.Document),
		Revision:     cloneRevision(view.Revision),
		SectionCount: sectionCount,
		SpanCount:    spanCount,
	}
	if cloned.Disposition != explorer.IngestApplied &&
		cloned.Disposition != explorer.IngestUnchanged {
		return explorer.IngestResult{}, inconsistentBase()
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.IngestResult{}, err
	}
	c.invalidateAuthorizedVectorAvailability()
	return cloned, nil
}

func documentViewCounts(view explorer.DocumentView) (int, int) {
	sections, spans := 0, 0
	var visit func(explorer.SectionView)
	visit = func(section explorer.SectionView) {
		sections++
		spans += len(section.Spans)
		for _, child := range section.Children {
			visit(child)
		}
	}
	visit(view.Root)
	return sections, spans
}

type sourceClaimOutcome uint8

const (
	sourceClaimRollback sourceClaimOutcome = iota
	sourceClaimCommit
	sourceClaimPend
)

// claimSourceMutation acquires shared source-URI mutation ownership before the
// base is changed. Existing claims authorize retries after a base commit even
// when revision registration failed. Legacy registered documents may backfill
// a missing claim only after authorization under their current rule.
func (c *Client) claimSourceMutation(
	ctx context.Context,
	sourceURI string,
	decision auth.Decision,
	selectedRule AccessRule,
	now time.Time,
) (SourcePolicyClaim, error) {
	if err := validateSourceURI(sourceURI); err != nil {
		return SourcePolicyClaim{}, err
	}
	existingClaim, claimed, err := c.policyStore.SourceClaim(ctx, sourceURI)
	if err != nil {
		return SourcePolicyClaim{}, policyCatalogReadError(ctx, err)
	}
	var expected *SourcePolicyClaim
	if claimed {
		allowed, ruleErr := sourceClaimAllowsMutation(
			existingClaim, selectedRule, decision, now)
		if ruleErr != nil {
			return SourcePolicyClaim{}, ruleErr
		}
		if !allowed {
			return SourcePolicyClaim{}, auth.ObjectNotFound()
		}
		expected = &existingClaim
	} else if err := c.authorizeLegacySource(
		ctx, sourceURI, decision, now); err != nil {
		return SourcePolicyClaim{}, err
	}
	claim, err := c.policyStore.CompareAndSwapSourceClaim(
		ctx, sourceURI, expected, selectedRule)
	if err == nil {
		return claim, nil
	}
	if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		return SourcePolicyClaim{}, policyCatalogWriteError(ctx, err)
	}
	latest, ok, readErr := c.policyStore.SourceClaim(ctx, sourceURI)
	if readErr != nil {
		return SourcePolicyClaim{}, policyCatalogReadError(ctx, readErr)
	}
	if ok {
		allowed, ruleErr := sourceClaimAllowsMutation(
			latest, selectedRule, decision, now)
		if ruleErr != nil {
			return SourcePolicyClaim{}, ruleErr
		}
		if !allowed {
			return SourcePolicyClaim{}, auth.ObjectNotFound()
		}
	}
	return SourcePolicyClaim{}, policyCatalogWriteError(ctx, err)
}

func sourceClaimAllowsMutation(
	claim SourcePolicyClaim,
	selectedRule AccessRule,
	decision auth.Decision,
	now time.Time,
) (bool, error) {
	if claim.Pending && !claim.Rule.equal(selectedRule) {
		return false, nil
	}
	allowed, err := ruleAllows(
		claim.Rule, decision, auth.OperationIngest, now)
	if err != nil || !allowed {
		return allowed, err
	}
	if claim.Pending && claim.PreviousRule != nil {
		return ruleAllows(
			*claim.PreviousRule, decision, auth.OperationIngest, now)
	}
	return true, nil
}

func (c *Client) authorizeLegacySource(
	ctx context.Context,
	sourceURI string,
	decision auth.Decision,
	now time.Time,
) error {
	summaries, err := c.base.Documents(ctx)
	if err != nil {
		return err
	}
	var documentID shoal.ID
	found := false
	for _, summary := range summaries {
		if summary.SourceURI != sourceURI {
			continue
		}
		if err := validateSummary(summary); err != nil {
			return inconsistentBase()
		}
		if found && summary.Document.ID != documentID {
			return inconsistentBase()
		}
		documentID = summary.Document.ID
		found = true
	}
	if !found {
		return nil
	}
	registration, ok, err := c.policyStore.CurrentRevision(ctx, documentID)
	if err != nil {
		return policyCatalogReadError(ctx, err)
	}
	if !ok {
		return catalogUnavailable()
	}
	for _, summary := range summaries {
		if summary.Document.ID == documentID &&
			registration.RevisionID != summary.Revision.ID {
			return catalogUnavailable()
		}
	}
	allowed, err := ruleAllows(
		registration.Rule, decision, auth.OperationIngest, now)
	if err != nil {
		return err
	}
	if !allowed {
		return auth.ObjectNotFound()
	}
	return nil
}

func (c *Client) begin(
	ctx context.Context,
	operation auth.Operation,
) (auth.Decision, auth.GenerationGuard, time.Time, error) {
	if err := contextFailure(ctx); err != nil {
		return auth.Decision{}, auth.GenerationGuard{}, time.Time{}, err
	}
	decision, err := c.resolver.Resolve(ctx)
	if err != nil {
		return auth.Decision{}, auth.GenerationGuard{}, time.Time{},
			resolverFailure(ctx, err)
	}
	now := c.clock()
	if now.IsZero() {
		return auth.Decision{}, auth.GenerationGuard{}, time.Time{},
			authorizationDenied()
	}
	if err := decision.Authorize(operation, auth.ResourceRequest{
		AuthorizationDomain: decision.AuthorizationDomain(),
	}, now); err != nil {
		if contextErr := contextFailure(ctx); contextErr != nil {
			return auth.Decision{}, auth.GenerationGuard{}, time.Time{}, contextErr
		}
		return auth.Decision{}, auth.GenerationGuard{}, time.Time{},
			authorizationDenied()
	}
	guard, err := auth.NewGenerationGuard(decision, c.generationReader)
	if err != nil {
		return auth.Decision{}, auth.GenerationGuard{}, time.Time{},
			authorizationDenied()
	}
	if err := guard.Check(ctx); err != nil {
		return auth.Decision{}, auth.GenerationGuard{}, time.Time{}, err
	}
	return decision, guard, now, nil
}

func (c *Client) selectIngestRule(
	ctx context.Context,
	decision auth.Decision,
	source explorer.Source,
	now time.Time,
) (AccessRule, error) {
	policy, err := c.policySelector.SelectPolicy(ctx, decision, source)
	if err != nil {
		return AccessRule{}, policySelectionError(ctx, err)
	}
	return selectedPolicyRule(decision, auth.OperationIngest, policy, now)
}

func (c *Client) selectEdgeRule(
	ctx context.Context,
	decision auth.Decision,
	edge graph.Edge,
	now time.Time,
) (AccessRule, error) {
	policy, err := c.edgePolicySelector.SelectEdgePolicy(ctx, decision, edge)
	if err != nil {
		return AccessRule{}, policySelectionError(ctx, err)
	}
	return selectedPolicyRule(decision, auth.OperationConnect, policy, now)
}

func selectedPolicyRule(
	decision auth.Decision,
	operation auth.Operation,
	policy auth.Policy,
	now time.Time,
) (AccessRule, error) {
	if policy.Epoch() != decision.PolicyGeneration() {
		return AccessRule{}, authorizationDenied()
	}
	rule, err := NewAccessRule(policy)
	if err != nil {
		return AccessRule{}, shoal.NewError(
			shoal.ErrorUnavailable, "trusted policy selection is invalid")
	}
	if err := rule.Authorize(decision, operation, now); err != nil {
		return AccessRule{}, authorizationDenied()
	}
	return rule, nil
}

func (c *Client) intrinsicEdges(
	ctx context.Context,
	nodeIDs []shoal.ID,
) ([]graph.Edge, error) {
	neighborhood, err := c.base.Neighborhood(ctx, explorer.NeighborhoodRequest{
		NodeIDs: append([]shoal.ID(nil), nodeIDs...),
		Depth:   1,
		EdgeTypes: []string{
			"contains",
		},
	})
	if err != nil {
		return nil, err
	}
	nodes := make(map[shoal.ID]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		nodes[nodeID] = struct{}{}
	}
	edges := make([]graph.Edge, 0, len(neighborhood.Edges))
	for _, edge := range neighborhood.Edges {
		if edge.Type != "contains" {
			continue
		}
		if _, ok := nodes[edge.From]; !ok {
			continue
		}
		if _, ok := nodes[edge.To]; !ok {
			continue
		}
		edges = append(edges, cloneGraphEdge(edge))
	}
	sort.Slice(edges, func(left, right int) bool {
		return shoal.CompareID(edges[left].ID, edges[right].ID) < 0
	})
	return edges, nil
}

func exactRevisionIsCurrent(
	summaries []explorer.DocumentSummary,
	documentID, revisionID shoal.ID,
) (bool, error) {
	found := false
	current := false
	for _, summary := range summaries {
		if summary.Document.ID != documentID {
			continue
		}
		if found {
			return false, inconsistentBase()
		}
		found = true
		current = summary.Revision.ID == revisionID &&
			summary.Document.RevisionID == revisionID
	}
	if !found {
		return false, inconsistentBase()
	}
	return current, nil
}

func policySelectionError(ctx context.Context, err error) error {
	if contextErr := contextFailure(ctx); contextErr != nil {
		return contextErr
	}
	if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		return authorizationDenied()
	}
	return shoal.NewError(
		shoal.ErrorUnavailable, "trusted policy selection unavailable")
}

func resolverFailure(ctx context.Context, err error) error {
	if contextErr := contextFailure(ctx); contextErr != nil {
		return contextErr
	}
	var shoalErr *shoal.Error
	if !errors.As(err, &shoalErr) || shoalErr == nil {
		return shoal.NewError(
			shoal.ErrorUnavailable, "authorization resolution unavailable")
	}
	switch shoalErr.Code {
	case shoal.ErrorCanceled:
		return shoal.NewError(shoal.ErrorCanceled, "operation canceled")
	case shoal.ErrorDeadline:
		return shoal.NewError(
			shoal.ErrorDeadline, "operation deadline exceeded")
	case shoal.ErrorUnavailable:
		return shoal.NewError(
			shoal.ErrorUnavailable, "authorization resolution unavailable")
	case shoal.ErrorInternal:
		return shoal.NewError(
			shoal.ErrorInternal, "authorization resolution failed")
	case shoal.ErrorUnauthorized, shoal.ErrorNotFound, shoal.ErrorInvalidArgument:
		return authorizationDenied()
	default:
		return shoal.NewError(
			shoal.ErrorUnavailable, "authorization resolution unavailable")
	}
}

func policyCatalogReadError(ctx context.Context, _ error) error {
	if contextErr := contextFailure(ctx); contextErr != nil {
		return contextErr
	}
	return catalogUnavailable()
}

func policyCatalogWriteError(ctx context.Context, err error) error {
	if contextErr := contextFailure(ctx); contextErr != nil {
		return contextErr
	}
	if shoal.IsErrorCode(err, shoal.ErrorConflict) {
		return catalogConflict()
	}
	return catalogUnavailable()
}

func authorizationDenied() error {
	return shoal.NewError(shoal.ErrorUnauthorized, "authorization denied")
}

func inconsistentBase() error {
	return shoal.NewError(
		shoal.ErrorInternal, "underlying Explorer returned inconsistent data")
}

func dependencyRequired(name string) error {
	return shoal.NewError(shoal.ErrorInvalidArgument, name+" is required")
}

func isNilDependency(value any) bool {
	if value == nil {
		return true
	}
	reflection := reflect.ValueOf(value)
	switch reflection.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflection.IsNil()
	default:
		return false
	}
}

func contextFailure(ctx context.Context) error {
	if ctx == nil {
		return shoal.NewError(shoal.ErrorInvalidArgument, "context is required")
	}
	switch err := ctx.Err(); {
	case errors.Is(err, context.Canceled):
		return shoal.WrapError(shoal.ErrorCanceled, "operation canceled", err)
	case errors.Is(err, context.DeadlineExceeded):
		return shoal.WrapError(
			shoal.ErrorDeadline, "operation deadline exceeded", err)
	default:
		return nil
	}
}

func rulesShareDomain(left, right AccessRule) bool {
	if len(left.policies) == 0 || len(right.policies) == 0 {
		return false
	}
	return bytes.Equal(
		left.policies[0].AuthorizationDomain(),
		right.policies[0].AuthorizationDomain(),
	)
}

var (
	_ explorer.Client        = (*Client)(nil)
	_ explorer.BoundedClient = (*Client)(nil)
)
