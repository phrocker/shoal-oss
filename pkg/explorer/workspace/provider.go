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

package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"reflect"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// OntologyChoices authorizes a settings-selected immutable ontology identity.
// Implementations must consult trusted governed state, never ambient request
// text.
type OntologyChoices interface {
	AuthorizeOntology(context.Context, auth.Decision, ontology.OntologyIdentity) error
}

// CeilingResolver returns the configured ceiling for a trusted service
// decision. User decisions do not require a service ceiling.
type CeilingResolver interface {
	ResolveServiceCeiling(context.Context, auth.Decision) (auth.ServiceCeiling, error)
}

// StaticOntologyChoices is an exact, immutable set of governable identities.
type StaticOntologyChoices struct {
	identities map[ontology.OntologyIdentity]struct{}
}

// NewStaticOntologyChoices constructs an exact configured choice set.
func NewStaticOntologyChoices(
	identities ...ontology.OntologyIdentity,
) (StaticOntologyChoices, error) {
	result := StaticOntologyChoices{
		identities: make(map[ontology.OntologyIdentity]struct{}, len(identities)),
	}
	for _, identity := range identities {
		if err := identity.Validate(); err != nil {
			return StaticOntologyChoices{}, err
		}
		result.identities[identity] = struct{}{}
	}
	return result, nil
}

// AuthorizeOntology accepts only an exact configured identity.
func (c StaticOntologyChoices) AuthorizeOntology(
	ctx context.Context,
	_ auth.Decision,
	identity ontology.OntologyIdentity,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	if _, ok := c.identities[identity]; !ok {
		return authDenied()
	}
	return nil
}

// ProviderOptions configures trusted decision, ceiling, ontology, and clock
// dependencies.
type ProviderOptions struct {
	Resolver        auth.Resolver
	CeilingResolver CeilingResolver
	OntologyChoices OntologyChoices
	Clock           func() time.Time
}

// Provider adds ownership and authorization enforcement to a settings Store.
type Provider struct {
	store           Store
	resolver        auth.Resolver
	ceilingResolver CeilingResolver
	ontologyChoices OntologyChoices
	clock           func() time.Time
}

// NewProvider constructs a usable authorized settings provider.
func NewProvider(store Store, options ProviderOptions) (*Provider, error) {
	if absent(store) {
		return nil, invalid("workspace settings store is required")
	}
	if absent(options.Resolver) {
		return nil, invalid("workspace settings decision resolver is required")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &Provider{
		store: store, resolver: options.Resolver,
		ceilingResolver: options.CeilingResolver,
		ontologyChoices: options.OntologyChoices,
		clock:           options.Clock,
	}, nil
}

// Get authorizes and returns one owned workspace's current settings.
func (p *Provider) Get(
	ctx context.Context,
	workspaceID shoal.ID,
) (Settings, error) {
	decision, _, err := p.authorize(
		ctx, auth.OperationWorkspaceSettingsRead, workspaceID)
	if err != nil {
		return Settings{}, err
	}
	settings, err := p.store.Load(ctx, workspaceID)
	if err != nil {
		return Settings{}, err
	}
	if settings.Owner != decision.Subject() {
		return Settings{}, authDenied()
	}
	if !bytes.Equal(
		settings.AuthorizationDomain, decision.AuthorizationDomain()) {
		return Settings{}, authDenied()
	}
	return settings.clone(), nil
}

// Update authorizes, validates, and atomically writes one narrowing revision.
func (p *Provider) Update(
	ctx context.Context,
	workspaceID shoal.ID,
	request UpdateRequest,
) (Settings, error) {
	decision, now, err := p.authorize(
		ctx, auth.OperationWorkspaceSettingsWrite, workspaceID)
	if err != nil {
		return Settings{}, err
	}
	if err := shoal.ValidateRequiredID(
		"settings mutation ID", request.MutationID); err != nil {
		return Settings{}, err
	}
	candidate, err := p.normalizeUpdate(ctx, decision, now, request.Narrowing)
	if err != nil {
		return Settings{}, err
	}
	current, loadErr := p.store.Load(ctx, workspaceID)
	switch {
	case loadErr == nil:
		if current.Owner != decision.Subject() {
			return Settings{}, authDenied()
		}
		if !bytes.Equal(
			current.AuthorizationDomain, decision.AuthorizationDomain()) {
			return Settings{}, authDenied()
		}
		if current.Revision == request.ExpectedRevision {
			if err := ensureMonotonic(current.Narrowing, candidate); err != nil {
				return Settings{}, err
			}
		}
	case shoal.IsErrorCode(loadErr, shoal.ErrorNotFound):
		if request.ExpectedRevision != 0 {
			return Settings{}, versionConflict()
		}
	default:
		return Settings{}, loadErr
	}
	return p.store.CompareAndSwap(
		ctx, workspaceID, decision.Subject(), decision.AuthorizationDomain(),
		request.ExpectedRevision, request.MutationID, candidate)
}

// Effective returns a currently revalidated decision plus budget, output label,
// and cache-partition effects for one owned workspace.
func (p *Provider) Effective(
	ctx context.Context,
	workspaceID shoal.ID,
	baseLimits Limits,
	baseOutputPolicies []auth.Policy,
) (EffectiveDecision, error) {
	return p.Apply(ctx, workspaceID, baseLimits, baseOutputPolicies)
}

// Apply loads one owned settings revision and derives its complete effect from
// the caller's current decision.
func (p *Provider) Apply(
	ctx context.Context,
	workspaceID shoal.ID,
	baseLimits Limits,
	baseOutputPolicies []auth.Policy,
) (EffectiveDecision, error) {
	decision, now, err := p.authorize(
		ctx, auth.OperationWorkspaceSettingsRead, workspaceID)
	if err != nil {
		return EffectiveDecision{}, err
	}
	settings, err := p.store.Load(ctx, workspaceID)
	if err != nil {
		return EffectiveDecision{}, err
	}
	if settings.Owner != decision.Subject() {
		return EffectiveDecision{}, authDenied()
	}
	if !bytes.Equal(
		settings.AuthorizationDomain, decision.AuthorizationDomain()) {
		return EffectiveDecision{}, authDenied()
	}
	options := ApplyOptions{
		Now: now, BaseLimits: baseLimits,
		BaseOutputPolicies: baseOutputPolicies,
		OntologyChoices:    p.ontologyChoices,
	}
	if decision.TrustedService() {
		if absent(p.ceilingResolver) {
			return EffectiveDecision{}, authDenied()
		}
		ceiling, err := p.ceilingResolver.ResolveServiceCeiling(ctx, decision)
		if err != nil {
			return EffectiveDecision{}, authDenied()
		}
		options.ServiceCeiling = &ceiling
	}
	return DeriveEffectiveDecision(ctx, decision, settings, options)
}

func (p *Provider) authorize(
	ctx context.Context,
	operation auth.Operation,
	workspaceID shoal.ID,
) (auth.Decision, time.Time, error) {
	if err := shoal.ValidateRequiredID("workspace ID", workspaceID); err != nil {
		return auth.Decision{}, time.Time{}, err
	}
	decision, err := p.resolver.Resolve(ctx)
	if err != nil {
		return auth.Decision{}, time.Time{}, err
	}
	now := p.clock()
	if now.IsZero() {
		return auth.Decision{}, time.Time{}, authDenied()
	}
	if err := decision.Authorize(operation, auth.ResourceRequest{
		AuthorizationDomain: decision.AuthorizationDomain(),
		ObjectID:            workspaceID,
	}, now); err != nil {
		return auth.Decision{}, time.Time{}, err
	}
	return decision, now, nil
}

func (p *Provider) normalizeUpdate(
	ctx context.Context,
	decision auth.Decision,
	now time.Time,
	value UpdateNarrowing,
) (Narrowing, error) {
	operations, err := normalizeOperations(value.AllowedOperations)
	if err != nil {
		return Narrowing{}, err
	}
	sources, err := normalizeIDs("permitted source IDs", value.PermittedSourceIDs)
	if err != nil {
		return Narrowing{}, err
	}
	policies, err := normalizeIDs("permitted policy IDs", value.PermittedPolicyIDs)
	if err != nil {
		return Narrowing{}, err
	}
	budgets, err := normalizeBudgets(value.Budgets)
	if err != nil {
		return Narrowing{}, err
	}
	ontologySelection := value.SelectedOntology
	if err := validateOntology(ontologySelection); err != nil {
		return Narrowing{}, err
	}
	if err := validateDecisionSelections(
		decision, operations, sources, policies); err != nil {
		return Narrowing{}, err
	}
	if ontologySelection.Present {
		if absent(p.ontologyChoices) {
			return Narrowing{}, authDenied()
		}
		if err := p.ontologyChoices.AuthorizeOntology(
			ctx, decision, ontologySelection.Identity); err != nil {
			return Narrowing{}, authDenied()
		}
	}
	outputPolicies := make([]auth.Policy, 0, len(value.OutputPolicies))
	if len(value.OutputPolicies) > MaxOutputPolicies {
		return Narrowing{}, invalid("output policies exceed the public bound")
	}
	var ceiling *auth.ServiceCeiling
	if decision.TrustedService() {
		if absent(p.ceilingResolver) {
			return Narrowing{}, authDenied()
		}
		resolved, err := p.ceilingResolver.ResolveServiceCeiling(ctx, decision)
		if err != nil {
			return Narrowing{}, authDenied()
		}
		if resolved.Identity() != decision.ServiceCeilingIdentity() ||
			resolved.Role() != decision.ServiceRole() {
			return Narrowing{}, authDenied()
		}
		ceiling = &resolved
	}
	for _, spec := range value.OutputPolicies {
		config := auth.PolicyConfig{
			AuthorizationDomain: decision.AuthorizationDomain(),
			SourceID:            append([]byte(nil), spec.SourceID...),
			GrantPolicyID:       append([]byte(nil), spec.GrantPolicyID...),
			Epoch:               spec.Epoch,
		}
		var policy auth.Policy
		if decision.TrustedService() {
			policy, err = auth.NewServicePolicy(config, decision)
		} else {
			policy, err = auth.NewPolicy(config)
		}
		if err != nil {
			return Narrowing{}, err
		}
		if err := decision.Authorize(
			auth.OperationWorkspaceSettingsWrite,
			auth.ResourceRequest{
				AuthorizationDomain: policy.AuthorizationDomain(),
				SourceID:            policy.SourceID(),
				PolicyID:            policy.GrantPolicyID(),
			},
			now,
		); err != nil {
			return Narrowing{}, err
		}
		if ceiling != nil {
			if _, err := auth.DeriveScannerAuthorizations(
				decision, auth.OperationWorkspaceSettingsWrite,
				policy, *ceiling, now,
			); err != nil {
				return Narrowing{}, authDenied()
			}
		}
		outputPolicies = append(outputPolicies, policy)
	}
	outputPolicies, err = normalizePolicies(outputPolicies)
	if err != nil {
		return Narrowing{}, err
	}
	return Narrowing{
		AllowedOperations:  operations,
		PermittedSourceIDs: sources,
		PermittedPolicyIDs: policies,
		Budgets:            budgets,
		OutputPolicies:     outputPolicies,
		SelectedOntology:   ontologySelection,
	}, nil
}

// ApplyOptions supplies trusted non-settings limits and output policies.
type ApplyOptions struct {
	Now                time.Time
	BaseLimits         Limits
	BaseOutputPolicies []auth.Policy
	ServiceCeiling     *auth.ServiceCeiling
	OntologyChoices    OntologyChoices
}

// EffectiveDecision is the complete result of applying one settings revision.
type EffectiveDecision struct {
	decision       auth.Decision
	settingsID     shoal.ID
	revision       uint64
	limits         Limits
	outputPolicies []auth.Policy
	cache          map[string]uint64
}

// Decision returns the fully preserved, narrowed authorization decision.
func (e EffectiveDecision) Decision() auth.Decision { return e.decision }

// SettingsID returns the stable settings identity.
func (e EffectiveDecision) SettingsID() shoal.ID { return e.settingsID }

// Revision returns the applied settings revision.
func (e EffectiveDecision) Revision() uint64 { return e.revision }

// Limits returns the concrete lowered budgets.
func (e EffectiveDecision) Limits() Limits { return e.limits }

// OutputPolicies returns independent immutable policy values whose labels must
// be conjoined with labels derived from output provenance.
func (e EffectiveDecision) OutputPolicies() []auth.Policy {
	return append([]auth.Policy(nil), e.outputPolicies...)
}

// CacheDimensions returns settings partition dimensions suitable for merging
// into auth.CacheKeyConfig.Limits.
func (e EffectiveDecision) CacheDimensions() map[string]uint64 {
	result := make(map[string]uint64, len(e.cache))
	for key, value := range e.cache {
		result[key] = value
	}
	return result
}

// OutputVisibility conjoins all base and settings-added output policies.
func (e EffectiveDecision) OutputVisibility() ([]byte, error) {
	return auth.ConjoinPolicies(e.outputPolicies...)
}

// DeriveEffectiveDecision applies settings to a current trusted decision. It
// never copies identity, delegation, purpose, expiry, generation, role, or
// ceiling from settings.
func DeriveEffectiveDecision(
	ctx context.Context,
	base auth.Decision,
	settings Settings,
	options ApplyOptions,
) (EffectiveDecision, error) {
	if err := validateContext(ctx); err != nil {
		return EffectiveDecision{}, err
	}
	if err := validateSettings(settings); err != nil {
		return EffectiveDecision{}, err
	}
	if settings.Owner != base.Subject() {
		return EffectiveDecision{}, authDenied()
	}
	if !bytes.Equal(
		settings.AuthorizationDomain, base.AuthorizationDomain()) {
		return EffectiveDecision{}, authDenied()
	}
	if options.Now.IsZero() {
		return EffectiveDecision{}, invalid("application time is required")
	}
	if err := base.Authorize(
		auth.OperationWorkspaceSettingsRead,
		auth.ResourceRequest{
			AuthorizationDomain: base.AuthorizationDomain(),
			ObjectID:            settings.WorkspaceID,
		},
		options.Now,
	); err != nil {
		return EffectiveDecision{}, err
	}
	limits, err := normalizeLimits(options.BaseLimits)
	if err != nil {
		return EffectiveDecision{}, err
	}
	narrowing, err := normalizeNarrowing(settings.Narrowing)
	if err != nil {
		return EffectiveDecision{}, err
	}
	if err := validateDecisionSelections(
		base, narrowing.AllowedOperations,
		narrowing.PermittedSourceIDs, narrowing.PermittedPolicyIDs,
	); err != nil {
		return EffectiveDecision{}, authDenied()
	}
	if base.TrustedService() {
		if options.ServiceCeiling == nil ||
			options.ServiceCeiling.Identity() != base.ServiceCeilingIdentity() ||
			options.ServiceCeiling.Role() != base.ServiceRole() {
			return EffectiveDecision{}, authDenied()
		}
	} else if options.ServiceCeiling != nil {
		return EffectiveDecision{}, invalid(
			"a service ceiling cannot be applied to a user decision")
	}
	selected, selectedSet := base.SelectedOntology()
	if narrowing.SelectedOntology.Present {
		if absent(options.OntologyChoices) {
			return EffectiveDecision{}, authDenied()
		}
		if err := options.OntologyChoices.AuthorizeOntology(
			ctx, base, narrowing.SelectedOntology.Identity); err != nil {
			return EffectiveDecision{}, authDenied()
		}
		selected = narrowing.SelectedOntology.Identity
		selectedSet = true
	}
	for _, policy := range narrowing.OutputPolicies {
		if err := authorizeOutputPolicy(
			base, policy, options.ServiceCeiling, options.Now); err != nil {
			return EffectiveDecision{}, authDenied()
		}
	}
	outputPolicies := append(
		append([]auth.Policy(nil), options.BaseOutputPolicies...),
		narrowing.OutputPolicies...,
	)
	outputPolicies, err = normalizePolicies(outputPolicies)
	if err != nil {
		return EffectiveDecision{}, err
	}
	for _, policy := range options.BaseOutputPolicies {
		if err := authorizeOutputPolicy(
			base, policy, options.ServiceCeiling, options.Now); err != nil {
			return EffectiveDecision{}, authDenied()
		}
	}

	operations := base.AllowedOperations()
	if narrowing.AllowedOperations.Present {
		operations = append([]auth.Operation(nil), narrowing.AllowedOperations.Values...)
	}
	sources := base.PermittedSourceIDs()
	if narrowing.PermittedSourceIDs.Present {
		sources = cloneByteSlices(narrowing.PermittedSourceIDs.Values)
	}
	policies := base.PermittedPolicyIDs()
	if narrowing.PermittedPolicyIDs.Present {
		policies = cloneByteSlices(narrowing.PermittedPolicyIDs.Values)
	}
	config := auth.DecisionConfig{
		Subject:                base.Subject(),
		Actor:                  base.Actor(),
		ClientID:               base.ClientID(),
		OnBehalfOf:             base.OnBehalfOf(),
		AuthorizationDomain:    base.AuthorizationDomain(),
		AllowedOperations:      operations,
		PermittedSourceIDs:     sources,
		PermittedPolicyIDs:     policies,
		PolicyGeneration:       base.PolicyGeneration(),
		AuthenticationExpires:  base.AuthenticationExpires(),
		RequestID:              base.RequestID(),
		CorrelationID:          base.CorrelationID(),
		AuditPurpose:           base.AuditPurpose(),
		ServiceRole:            base.ServiceRole(),
		ServiceCeilingIdentity: base.ServiceCeilingIdentity(),
	}
	if selectedSet {
		config.SelectedOntology = selected
	}
	decision, err := auth.NewDecision(config)
	if err != nil {
		return EffectiveDecision{}, err
	}
	limits = applyBudgets(limits, narrowing.Budgets)
	cache := map[string]uint64{
		"workspace_settings_revision": settings.Revision,
	}
	for key, value := range settingsIdentityDimensions(settings.SettingsID) {
		cache[key] = value
	}
	return EffectiveDecision{
		decision:       decision,
		settingsID:     settings.SettingsID,
		revision:       settings.Revision,
		limits:         limits,
		outputPolicies: outputPolicies,
		cache:          cache,
	}, nil
}

// Apply is the concise package-level form of DeriveEffectiveDecision.
func Apply(
	ctx context.Context,
	base auth.Decision,
	settings Settings,
	options ApplyOptions,
) (EffectiveDecision, error) {
	return DeriveEffectiveDecision(ctx, base, settings, options)
}

func authorizeOutputPolicy(
	decision auth.Decision,
	policy auth.Policy,
	ceiling *auth.ServiceCeiling,
	now time.Time,
) error {
	if err := decision.Authorize(
		auth.OperationWorkspaceSettingsRead,
		auth.ResourceRequest{
			AuthorizationDomain: policy.AuthorizationDomain(),
			SourceID:            policy.SourceID(),
			PolicyID:            policy.GrantPolicyID(),
		},
		now,
	); err != nil {
		return err
	}
	if decision.TrustedService() {
		if ceiling == nil {
			return authDenied()
		}
		_, err := auth.DeriveScannerAuthorizations(
			decision, auth.OperationWorkspaceSettingsRead, policy, *ceiling, now)
		return err
	}
	if policy.ServiceRole() != "" {
		return authDenied()
	}
	return nil
}

func validateDecisionSelections(
	decision auth.Decision,
	operations OperationSelection,
	sources, policies IDSelection,
) error {
	if operations.Present &&
		!operationSubset(operations.Values, decision.AllowedOperations()) {
		return authDenied()
	}
	if sources.Present &&
		!byteSubset(sources.Values, decision.PermittedSourceIDs()) {
		return authDenied()
	}
	if policies.Present &&
		!byteSubset(policies.Values, decision.PermittedPolicyIDs()) {
		return authDenied()
	}
	return nil
}

func ensureMonotonic(previous, next Narrowing) error {
	if widenedOperationSelection(previous.AllowedOperations, next.AllowedOperations) ||
		widenedIDSelection(previous.PermittedSourceIDs, next.PermittedSourceIDs) ||
		widenedIDSelection(previous.PermittedPolicyIDs, next.PermittedPolicyIDs) ||
		widenedBudgets(previous.Budgets, next.Budgets) ||
		!policySubset(previous.OutputPolicies, next.OutputPolicies) ||
		(previous.SelectedOntology.Present && !next.SelectedOntology.Present) {
		return authDenied()
	}
	return nil
}

func widenedOperationSelection(previous, next OperationSelection) bool {
	switch {
	case !previous.Present:
		return false
	case !next.Present:
		return true
	default:
		return !operationSubset(next.Values, previous.Values)
	}
}

func widenedIDSelection(previous, next IDSelection) bool {
	switch {
	case !previous.Present:
		return false
	case !next.Present:
		return true
	default:
		return !byteSubset(next.Values, previous.Values)
	}
}

func widenedBudgets(previous, next Budgets) bool {
	return widenedUint32(previous.RetrievalTopK, next.RetrievalTopK) ||
		widenedUint32(previous.GraphDepth, next.GraphDepth) ||
		widenedUint32(previous.GraphFanout, next.GraphFanout) ||
		widenedUint32(previous.GraphNodes, next.GraphNodes) ||
		widenedUint64(previous.OutputBytes, next.OutputBytes)
}

func widenedUint32(previous, next *uint32) bool {
	if previous == nil {
		return false
	}
	return next == nil || *next > *previous
}

func widenedUint64(previous, next *uint64) bool {
	if previous == nil {
		return false
	}
	return next == nil || *next > *previous
}

func operationSubset(subset, superset []auth.Operation) bool {
	allowed := make(map[auth.Operation]struct{}, len(superset))
	for _, operation := range superset {
		allowed[operation] = struct{}{}
	}
	for _, operation := range subset {
		if _, ok := allowed[operation]; !ok {
			return false
		}
	}
	return true
}

func byteSubset(subset, superset [][]byte) bool {
	for _, value := range subset {
		index := sort.Search(len(superset), func(i int) bool {
			return bytes.Compare(superset[i], value) >= 0
		})
		if index >= len(superset) || !bytes.Equal(superset[index], value) {
			return false
		}
	}
	return true
}

func policySubset(subset, superset []auth.Policy) bool {
	available := make(map[string]struct{}, len(superset))
	for _, policy := range superset {
		encoded, err := policy.Encode()
		if err != nil {
			return false
		}
		available[string(encoded)] = struct{}{}
	}
	for _, policy := range subset {
		encoded, err := policy.Encode()
		if err != nil {
			return false
		}
		if _, ok := available[string(encoded)]; !ok {
			return false
		}
	}
	return true
}

func applyBudgets(base Limits, narrowing Budgets) Limits {
	if narrowing.RetrievalTopK != nil && *narrowing.RetrievalTopK < base.RetrievalTopK {
		base.RetrievalTopK = *narrowing.RetrievalTopK
	}
	if narrowing.GraphDepth != nil && *narrowing.GraphDepth < base.GraphDepth {
		base.GraphDepth = *narrowing.GraphDepth
	}
	if narrowing.GraphFanout != nil && *narrowing.GraphFanout < base.GraphFanout {
		base.GraphFanout = *narrowing.GraphFanout
	}
	if narrowing.GraphNodes != nil && *narrowing.GraphNodes < base.GraphNodes {
		base.GraphNodes = *narrowing.GraphNodes
	}
	if narrowing.OutputBytes != nil && *narrowing.OutputBytes < base.OutputBytes {
		base.OutputBytes = *narrowing.OutputBytes
	}
	return base
}

func settingsIdentityDimensions(id shoal.ID) map[string]uint64 {
	sum := sha256.Sum256([]byte(id))
	return map[string]uint64{
		"workspace_settings_identity_0": binary.BigEndian.Uint64(sum[0:8]),
		"workspace_settings_identity_1": binary.BigEndian.Uint64(sum[8:16]),
		"workspace_settings_identity_2": binary.BigEndian.Uint64(sum[16:24]),
		"workspace_settings_identity_3": binary.BigEndian.Uint64(sum[24:32]),
	}
}

func absent(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
