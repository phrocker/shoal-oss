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
	"sort"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// tightenChanges lists what one TightenRule call rewrote, so the durable
// store can persist exactly those records in one batch. Document-kind node
// and intrinsic-edge projections are not listed: they are rebuilt from the
// current revision on load.
type tightenChanges struct {
	revisions   []revisionKey
	nodes       []shoal.ID
	edges       []shoal.ID
	sourceClaim string
}

func (c tightenChanges) changed() bool {
	return len(c.revisions) > 0 || len(c.nodes) > 0 || len(c.edges) > 0 ||
		c.sourceClaim != ""
}

// TightenRule narrows the catalog rule of one document (#570). It is how the
// startup label migration closes documents that were labelled before labels
// were enforced: their registrations carry the bare source rule, and `to`
// adds the label policies.
//
// It refuses (InvalidArgument) unless `to`'s components are a strict superset
// of `from`'s, so it can never widen. The added components (the delta, `to`
// minus `from`) are conjoined onto every registration in scope through
// NewAccessRule, which only ever adds conjuncts; a registration that already
// carries the delta is left alone, which makes a repeated call a no-op.
//
// The revision registration (documentID, revisionID) must exist (NotFound)
// and its rule must be `from` or already include `to` (Conflict otherwise),
// so a caller acting on a stale read cannot rewrite a document that changed
// underneath it. What is in scope depends on which revision that is:
//
//   - revisionID is the document's current revision: every revision
//     registration of the document, historical ones included; the current
//     node and intrinsic-edge projections (rebuilt from the tightened current
//     revision); every RegistrationExtracted node and edge bound to the
//     document, whatever revision asserted it; every RegistrationApplication
//     edge with an endpoint among those extracted nodes (see below); and the
//     committed or pending source claim for sourceURI, whose Rule and
//     PreviousRule both take the delta and whose version advances. sourceURI
//     is required here because the catalog keeps no document-to-URI index; a
//     URI with no claim is not an error. A claim held by an in-flight
//     mutation conflicts.
//   - revisionID is a historical revision: that revision registration, and
//     the extracted nodes and edges bound to exactly that revision, plus
//     application edges touching those nodes. sourceURI is ignored. This lets
//     a caller translate a historical revision's own labels, which may differ
//     from the current revision's.
//
// Registrations are selected by their stored RegistrationKind, never by an ID
// prefix. RegistrationMaterialized nodes and edges belong to no document and
// are never touched, nor are pending edge reservations (a reservation still
// holding the old rule can only conflict when it is committed).
//
// Application edges: an edge persisted before RegistrationKind existed
// decodes as RegistrationApplication even when ExtractDocument wrote it, so a
// legacy extracted relation cannot be told apart from a Connect edge. Every
// application edge with an endpoint among this document's extracted nodes
// takes the delta. That only narrows: reading such an edge already requires
// reading that endpoint, which carries the delta, so no reader loses an edge
// it could otherwise see. The cost is that retrying the identical Connect
// afterwards conflicts, because the stored rule no longer equals the one the
// selector picks. A legacy relation whose endpoints both belong to OTHER
// documents (shared entities first extracted elsewhere) names nothing that
// ties it to this document and keeps its rule; it regains a document binding
// the next time this document is extracted (extractedEdgeMerge).
//
// The call is atomic: every rewritten registration is derived before any is
// stored, under one lock, so a failure changes nothing.
func (s *MemoryPolicyStore) TightenRule(
	ctx context.Context,
	documentID, revisionID shoal.ID,
	sourceURI string,
	from, to AccessRule,
) (bool, error) {
	changes, err := s.tightenRule(ctx, documentID, revisionID, sourceURI, from, to)
	if err != nil {
		return false, err
	}
	return changes.changed(), nil
}

func (s *MemoryPolicyStore) tightenRule(
	ctx context.Context,
	documentID, revisionID shoal.ID,
	sourceURI string,
	from, to AccessRule,
) (tightenChanges, error) {
	if err := contextFailure(ctx); err != nil {
		return tightenChanges{}, err
	}
	if err := shoal.ValidateRequiredID("document ID", documentID); err != nil {
		return tightenChanges{}, err
	}
	if err := shoal.ValidateRequiredID("revision ID", revisionID); err != nil {
		return tightenChanges{}, err
	}
	fromRule, err := from.clone()
	if err != nil {
		return tightenChanges{}, err
	}
	toRule, err := to.clone()
	if err != nil {
		return tightenChanges{}, err
	}
	delta, err := strictRuleExtension(fromRule, toRule)
	if err != nil {
		return tightenChanges{}, err
	}
	if s == nil {
		return tightenChanges{}, catalogUnavailable()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.initialize()
	key := revisionKey{documentID: documentID, revisionID: revisionID}
	target, ok := s.revisions[key]
	if !ok {
		return tightenChanges{}, shoal.NewError(
			shoal.ErrorNotFound, "revision registration is not in the catalog")
	}
	if !target.Rule.equal(fromRule) && !ruleIncludes(target.Rule, toRule) {
		return tightenChanges{}, catalogConflict()
	}
	currentKey, hasCurrent := s.current[documentID]
	whole := hasCurrent && currentKey == key
	if whole {
		if err := validateSourceURI(sourceURI); err != nil {
			return tightenChanges{}, err
		}
	}
	inScope := func(document, revision shoal.ID) bool {
		return document == documentID && (whole || revision == revisionID)
	}

	// Derive everything first; nothing is stored until all of it is built.
	var changes tightenChanges
	revisions := make(map[revisionKey]RevisionRegistration)
	for candidate, registration := range s.revisions {
		if !inScope(candidate.documentID, candidate.revisionID) {
			continue
		}
		rule, changed, err := conjoinDelta(registration.Rule, delta)
		if err != nil {
			return tightenChanges{}, err
		}
		if !changed {
			continue
		}
		tightened := cloneRevisionRegistration(registration)
		tightened.Rule = rule
		revisions[candidate] = tightened
		changes.revisions = append(changes.revisions, candidate)
	}
	extractedNodes := make(map[shoal.ID]struct{})
	nodes := make(map[shoal.ID]NodeRegistration)
	for nodeID, registration := range s.nodes {
		if registration.Kind != RegistrationExtracted ||
			!inScope(registration.DocumentID, registration.RevisionID) {
			continue
		}
		extractedNodes[nodeID] = struct{}{}
		rule, changed, err := conjoinDelta(registration.Rule, delta)
		if err != nil {
			return tightenChanges{}, err
		}
		if !changed {
			continue
		}
		tightened, err := cloneNodeRegistration(registration)
		if err != nil {
			return tightenChanges{}, catalogUnavailable()
		}
		tightened.Rule = rule
		nodes[nodeID] = tightened
		changes.nodes = append(changes.nodes, nodeID)
	}
	edges := make(map[shoal.ID]EdgeRegistration)
	for edgeID, registration := range s.edges {
		switch registration.Kind {
		case RegistrationExtracted:
			if !inScope(registration.DocumentID, registration.RevisionID) {
				continue
			}
		case RegistrationApplication:
			_, fromExtracted := extractedNodes[registration.Edge.From]
			_, toExtracted := extractedNodes[registration.Edge.To]
			if !fromExtracted && !toExtracted {
				continue
			}
		default:
			continue
		}
		rule, changed, err := conjoinDelta(registration.Rule, delta)
		if err != nil {
			return tightenChanges{}, err
		}
		if !changed {
			continue
		}
		tightened, err := cloneEdgeRegistrationChecked(registration)
		if err != nil {
			return tightenChanges{}, catalogUnavailable()
		}
		tightened.Rule = rule
		edges[edgeID] = tightened
		changes.edges = append(changes.edges, edgeID)
	}
	var claim *sourceClaimState
	if whole {
		if state, exists := s.sourceClaims[sourceURI]; exists {
			if state.held {
				return tightenChanges{}, catalogConflict()
			}
			rule, ruleChanged, err := conjoinDelta(state.claim.Rule, delta)
			if err != nil {
				return tightenChanges{}, err
			}
			var previous *AccessRule
			previousChanged := false
			if state.claim.PreviousRule != nil {
				narrowed, changed, err := conjoinDelta(
					*state.claim.PreviousRule, delta)
				if err != nil {
					return tightenChanges{}, err
				}
				previous, previousChanged = &narrowed, changed
			}
			if ruleChanged || previousChanged {
				if s.sourceVersion == ^uint64(0) {
					return tightenChanges{}, catalogUnavailable()
				}
				tightened, err := cloneSourcePolicyClaim(state.claim)
				if err != nil {
					return tightenChanges{}, catalogUnavailable()
				}
				tightened.Rule = rule
				tightened.PreviousRule = previous
				claim = &sourceClaimState{claim: tightened}
				changes.sourceClaim = sourceURI
			}
		}
	}

	// Store.
	for candidate, registration := range revisions {
		s.revisions[candidate] = registration
	}
	if whole && len(revisions) > 0 {
		if current, ok := revisions[key]; ok {
			s.replaceCurrent(key, current)
		}
	}
	for nodeID, registration := range nodes {
		s.nodes[nodeID] = registration
	}
	for edgeID, registration := range edges {
		s.edges[edgeID] = registration
	}
	if claim != nil {
		s.sourceVersion++
		claim.claim.Version = s.sourceVersion
		s.sourceClaims[sourceURI] = *claim
	}
	sort.Slice(changes.revisions, func(left, right int) bool {
		return shoal.CompareID(changes.revisions[left].revisionID,
			changes.revisions[right].revisionID) < 0
	})
	sortIDs(changes.nodes)
	sortIDs(changes.edges)
	return changes, nil
}

// DocumentRevisions returns every revision registration of one document,
// ordered by revision ID, with Current set on the current one.
func (s *MemoryPolicyStore) DocumentRevisions(
	ctx context.Context,
	documentID shoal.ID,
) ([]RevisionRegistration, error) {
	if err := contextFailure(ctx); err != nil {
		return nil, err
	}
	if err := shoal.ValidateRequiredID("document ID", documentID); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, catalogUnavailable()
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	currentKey, hasCurrent := s.current[documentID]
	registrations := make([]RevisionRegistration, 0)
	for key, registration := range s.revisions {
		if key.documentID != documentID {
			continue
		}
		cloned := cloneRevisionRegistration(registration)
		cloned.Current = hasCurrent && currentKey == key
		registrations = append(registrations, cloned)
	}
	sort.Slice(registrations, func(left, right int) bool {
		return shoal.CompareID(registrations[left].RevisionID,
			registrations[right].RevisionID) < 0
	})
	return registrations, nil
}

// LabelMigration returns the recorded label-migration marker.
func (s *MemoryPolicyStore) LabelMigration(
	ctx context.Context,
) (LabelMigrationRecord, bool, error) {
	if err := contextFailure(ctx); err != nil {
		return LabelMigrationRecord{}, false, err
	}
	if s == nil {
		return LabelMigrationRecord{}, false, catalogUnavailable()
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.labelMigration == nil {
		return LabelMigrationRecord{}, false, nil
	}
	return s.labelMigration.clone(), true, nil
}

// PutLabelMigration records the label-migration marker, replacing any
// earlier one.
func (s *MemoryPolicyStore) PutLabelMigration(
	ctx context.Context,
	record LabelMigrationRecord,
) error {
	if err := contextFailure(ctx); err != nil {
		return err
	}
	if record.Version == 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "label migration version is required")
	}
	if s == nil {
		return catalogUnavailable()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := record.clone()
	s.labelMigration = &cloned
	return nil
}

// strictRuleExtension returns the components of `to` that `from` lacks,
// refusing unless every component of `from` is in `to` and `to` has at least
// one more. Components compare by logical identity, as AccessRule equality
// does.
func strictRuleExtension(from, to AccessRule) ([]auth.Policy, error) {
	if !ruleIncludes(to, from) {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument,
			"refusing to change an access rule: the new rule does not keep "+
				"every component of the old one, so it would widen access")
	}
	if len(to.keys) <= len(from.keys) {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument,
			"refusing to change an access rule: the new rule adds no component")
	}
	delta := make([]auth.Policy, 0, len(to.keys)-len(from.keys))
	for index, key := range to.keys {
		if !ruleHasKey(from, key) {
			delta = append(delta, to.policies[index])
		}
	}
	return delta, nil
}

// ruleIncludes reports whether every component of inner is a component of
// outer, so that outer is at least as narrow as inner.
func ruleIncludes(outer, inner AccessRule) bool {
	if len(inner.keys) == 0 || len(outer.keys) == 0 {
		return false
	}
	for _, key := range inner.keys {
		if !ruleHasKey(outer, key) {
			return false
		}
	}
	return true
}

func ruleHasKey(rule AccessRule, key []byte) bool {
	for _, candidate := range rule.keys {
		if bytes.Equal(candidate, key) {
			return true
		}
	}
	return false
}

// conjoinDelta adds the delta's components to rule through NewAccessRule and
// reports whether that changed it. It can only add conjuncts.
func conjoinDelta(rule AccessRule, delta []auth.Policy) (AccessRule, bool, error) {
	components := rule.components()
	if len(components) == 0 {
		return AccessRule{}, false, catalogUnavailable()
	}
	narrowed, err := NewAccessRule(append(components, delta...)...)
	if err != nil {
		return AccessRule{}, false, err
	}
	if narrowed.equal(rule) {
		return rule, false, nil
	}
	return narrowed, true, nil
}

func sortIDs(ids []shoal.ID) {
	sort.Slice(ids, func(left, right int) bool {
		return shoal.CompareID(ids[left], ids[right]) < 0
	})
}
