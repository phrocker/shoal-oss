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
	"strings"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// materializationOwnerPrefix opens every graph-materialization identity
// (explorer.materializedID("materialization", ...)). A node registered by
// MaterializeGraph names its materialization as DocumentID; it is not derived
// from any document and has no current revision.
const materializationOwnerPrefix = "materialized-materialization-"

// effectiveNodeRegistrations applies the rule a derived node registration
// carries today. An entity extracted from a document (ExtractDocument, via
// PutNode) is governed by the rule of the revision it came from AND the
// document's current rule, exactly
// as revisionAllows governs an older revision: when the document is
// relabelled, an entity extracted from an earlier, less restricted revision
// closes with it (#570). A document-derived node whose document has no
// current registration, or whose conjoined rule cannot be built, is dropped,
// which every caller treats as unregistered and therefore denied.
//
// Intrinsic nodes always belong to the current revision and keep their rule.
// Graph-materialization nodes are owned by a materialization, not a document,
// and keep their registered rule. Extracted edges are registered without a
// document; they are admitted only when both endpoints pass this check.
func (c *Client) effectiveNodeRegistrations(
	ctx context.Context,
	registrations map[shoal.ID]NodeRegistration,
) (map[shoal.ID]NodeRegistration, error) {
	documentIDs := make([]shoal.ID, 0, len(registrations))
	seen := make(map[shoal.ID]struct{}, len(registrations))
	for _, registration := range registrations {
		if !documentDerived(registration) {
			continue
		}
		if _, duplicate := seen[registration.DocumentID]; duplicate {
			continue
		}
		seen[registration.DocumentID] = struct{}{}
		documentIDs = append(documentIDs, registration.DocumentID)
	}
	if len(documentIDs) == 0 {
		return registrations, nil
	}
	currents, err := c.policyStore.CurrentRevisions(ctx, documentIDs)
	if err != nil {
		return nil, policyCatalogReadError(ctx, err)
	}
	effective := make(map[shoal.ID]NodeRegistration, len(registrations))
	for nodeID, registration := range registrations {
		if !documentDerived(registration) {
			effective[nodeID] = registration
			continue
		}
		current, ok := currents[registration.DocumentID]
		if !ok || current.DocumentID != registration.DocumentID {
			continue
		}
		if current.RevisionID == registration.RevisionID {
			effective[nodeID] = registration
			continue
		}
		rule, err := conjoinRules(registration.Rule, current.Rule)
		if err != nil {
			continue
		}
		registration.Rule = rule
		effective[nodeID] = registration
	}
	return effective, nil
}

// documentDerived reports whether a registration was written by PutNode for a
// document (ExtractDocument), as opposed to an intrinsic node of the current
// revision or a graph-materialization node. Intrinsic registrations are
// written by PutRevision's current projection without a Node and always name
// the current revision, so they need no conjunction; PutNode registrations
// always carry the Node they register.
func documentDerived(registration NodeRegistration) bool {
	return registration.Node.ID != "" &&
		!strings.HasPrefix(string(registration.DocumentID), materializationOwnerPrefix)
}

// conjoinRules is the AND of two rules, built only through NewAccessRule.
func conjoinRules(left, right AccessRule) (AccessRule, error) {
	leftPolicies, rightPolicies := left.components(), right.components()
	if len(leftPolicies) == 0 || len(rightPolicies) == 0 {
		return AccessRule{}, inconsistentBase()
	}
	return NewAccessRule(append(leftPolicies, rightPolicies...)...)
}
