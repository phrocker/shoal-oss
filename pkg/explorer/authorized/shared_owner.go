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
	"sort"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Shared extracted registrations (#700).
//
// An extracted entity's node ID depends only on its extraction namespace,
// type and key, and a relation's edge ID only on its type and endpoints, so
// several documents can extract the very same node or relation. The catalog
// used to keep only the FIRST extractor as the registration's owner. A reader
// who could read a later extractor, but not the first, then lost the node as
// soon as the first document was relabelled, which told them that a document
// they cannot read contains the entity (an entity-key oracle).
//
// Now every extracting document revision owns the registration. The first
// keeps the registration's DocumentID, RevisionID and Rule, as before, so a
// registration recorded before this change is simply a registration with one
// owner; every later one is recorded as a co-owner. A reader sees the
// registration when ANY owner's effective rule allows it, and each owner's
// effective rule is bound to the revision that extracted it (#570, #695):
//
//   - the first owner: its own rule, AND its document's current rule when its
//     revision is no longer current (effectiveRegistrations, unchanged);
//   - a co-owner: its own rule, AND the rule its revision is registered under
//     now, AND its document's current rule. The middle term makes a co-owner
//     self-protecting: a TightenRule that its stored rule missed (a catalog
//     rolled back to a binary that does not know co-owners, then forward
//     again) is still enforced.
//
// An owner whose document has no current revision, or whose revision is not
// registered, contributes nothing. Read paths project a shared registration
// onto ONE owner the reader can read (projectOwner), so everything downstream
// (labels, revision bindings, domains) sees only a rule of a document that
// reader may read. A reader who can read no owner gets the first surviving
// owner, which denies.
//
// Owners never change the node's content: a co-owner is only recorded when
// its extraction produced exactly the stored node (or edge), so there is
// nothing per-owner to show.

// ExtractionOwner is one document revision, beyond a registration's first
// owner, that extracted a shared node or relation, with the rule it was
// extracted under.
type ExtractionOwner struct {
	DocumentID shoal.ID
	RevisionID shoal.ID
	Rule       AccessRule
}

func (o ExtractionOwner) reference() RevisionReference {
	return RevisionReference{DocumentID: o.DocumentID, RevisionID: o.RevisionID}
}

func compareOwners(left, right ExtractionOwner) int {
	return compareRevisionReferences(left.reference(), right.reference())
}

func cloneOwners(owners []ExtractionOwner) ([]ExtractionOwner, error) {
	if len(owners) == 0 {
		return nil, nil
	}
	cloned := make([]ExtractionOwner, len(owners))
	for index, owner := range owners {
		rule, err := owner.Rule.clone()
		if err != nil {
			return nil, err
		}
		owner.Rule = rule
		cloned[index] = owner
	}
	return cloned, nil
}

// ownerMerge is what recording one more extraction of a shared registration
// does.
type ownerMerge uint8

const (
	ownerMergeConflict ownerMerge = iota
	// ownerMergeKeep: the extracting revision already owns it, under the
	// same rule.
	ownerMergeKeep
	// ownerMergeAdd: the extracting revision becomes a co-owner.
	ownerMergeAdd
)

// mergeOwner decides an extraction by (documentID, revisionID) under rule of
// a registration whose content already matched. A revision that already owns
// it under a different rule conflicts: that is the extracting document's own
// state, never another document's.
func mergeOwner(
	primaryDocument, primaryRevision shoal.ID,
	primaryRule AccessRule,
	coOwners []ExtractionOwner,
	incoming ExtractionOwner,
) ownerMerge {
	if incoming.DocumentID == primaryDocument &&
		incoming.RevisionID == primaryRevision {
		if primaryRule.equal(incoming.Rule) {
			return ownerMergeKeep
		}
		return ownerMergeConflict
	}
	for _, owner := range coOwners {
		if owner.DocumentID == incoming.DocumentID &&
			owner.RevisionID == incoming.RevisionID {
			if owner.Rule.equal(incoming.Rule) {
				return ownerMergeKeep
			}
			return ownerMergeConflict
		}
	}
	return ownerMergeAdd
}

// insertOwner returns owners with incoming added in canonical order.
func insertOwner(
	owners []ExtractionOwner, incoming ExtractionOwner,
) []ExtractionOwner {
	next := make([]ExtractionOwner, 0, len(owners)+1)
	next = append(next, owners...)
	next = append(next, incoming)
	sort.Slice(next, func(left, right int) bool {
		return compareOwners(next[left], next[right]) < 0
	})
	return next
}

// canonicalOwners reports whether owners is strictly ascending, holds valid
// identities and rules, and never repeats the first owner. It is what the
// durable store requires of a reconstructed registration.
func canonicalOwners(
	primaryDocument, primaryRevision shoal.ID,
	owners []ExtractionOwner,
) bool {
	for index, owner := range owners {
		if shoal.ValidateRequiredID("owner document ID", owner.DocumentID) != nil ||
			shoal.ValidateRequiredID("owner revision ID", owner.RevisionID) != nil {
			return false
		}
		if len(owner.Rule.components()) == 0 {
			return false
		}
		if owner.DocumentID == primaryDocument && owner.RevisionID == primaryRevision {
			return false
		}
		if index > 0 && compareOwners(owners[index-1], owner) >= 0 {
			return false
		}
	}
	return true
}
