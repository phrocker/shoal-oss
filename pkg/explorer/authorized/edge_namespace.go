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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Application edge IDs live in a namespace of their writer (#699).
//
// Edge IDs are one space shared by every edge in the corpus: intrinsic
// "contains" edges (derived from a document's URI), extraction and
// derivation edges, and every principal's application edges. If Connect
// accepted any caller-chosen ID, whether it succeeded would depend on
// whether some edge the caller cannot read already holds that ID, and,
// through intrinsic IDs, on whether a hidden document exists at a guessed
// URI. Refusing only known server-derived prefixes would still leave other
// principals' hidden application edges as an oracle.
//
// So Connect accepts only IDs in the caller's own namespace: the prefix
// applicationEdgeIDPrefix, a digest of the caller's identity, then "/" and
// the caller's local ID. No server derivation produces that prefix, and
// another identity's namespace differs by a 128-bit digest, so the only
// registrations an accepted ID can collide with were connected under the
// same identity. Any other ID is refused with the same InvalidArgument
// whatever the catalog holds: the refusal depends on the caller's identity
// and the ID only.
//
// The digest is keyed by the catalog's identity namespace key (#721,
// namespace_key.go), so it cannot be recomputed from a guessed identity.
// The identity's unkeyed namespace from before #721 is accepted only to
// retry an edge already registered under it: whether that holds depends on
// what the caller's own identity connected, never on another identity's
// edges.
//
// The namespace binds the identity, not its current authority: a narrowed
// session of the same identity (workspace narrowing, a down-scoped token, a
// revoked label) shares it, and can still tell whether a local ID that
// identity's broader sessions connected exists. Connect has no HTTP or MCP
// route, so this is reachable by library callers only.
const applicationEdgeIDPrefix = "app-edge_"

// applicationEdgeNamespaceVersion separates the digest from any other use
// of the same components. Version 1 is the unkeyed digest of #711, which
// legacyApplicationEdgeNamespace still computes; version 2 is keyed (#721).
const (
	legacyApplicationEdgeNamespaceVersion = "shoal/authorized/application-edge-namespace/v1"
	applicationEdgeNamespaceVersion       = "shoal/authorized/application-edge-namespace/v2"
)

// identityNamespaces is one identity's namespace prefix for one namespace
// kind: the keyed one it writes in (#721), and the unkeyed one (#711, #718)
// that what it wrote before #721 is in. Both are the same identity's;
// neither is ever another identity's.
type identityNamespaces struct {
	keyed  string
	legacy string
}

// in reports which of the two namespaces value is in, with its rest after
// the prefix: (rest, legacy, true), or false for neither.
func (n identityNamespaces) in(value string) (string, bool, bool) {
	if len(value) > len(n.keyed) && strings.HasPrefix(value, n.keyed) {
		return value[len(n.keyed):], false, true
	}
	if len(value) > len(n.legacy) && strings.HasPrefix(value, n.legacy) {
		return value[len(n.legacy):], true, true
	}
	return "", false, false
}

// applicationEdgeNamespaces returns the ID prefixes of the application
// edges the decision's identity connects: applicationEdgeIDPrefix, the
// identity's digest, "/". The digest covers every component that identifies
// who is writing: the authorization domain, the subject, the acting
// application, the client, the delegation chain and the service identity. It
// leaves out what changes per request (request and correlation IDs) and what
// changes per grant (policies, generation), so a writer's retry lands in the
// same namespace.
func (c *Client) applicationEdgeNamespaces(
	ctx context.Context,
	decision auth.Decision,
) (identityNamespaces, error) {
	digest, err := c.identityNamespaceDigest(
		ctx, applicationEdgeNamespaceVersion, decision)
	if err != nil {
		return identityNamespaces{}, err
	}
	return identityNamespaces{
		keyed:  applicationEdgeIDPrefix + digest + "/",
		legacy: legacyApplicationEdgeNamespace(decision),
	}, nil
}

// legacyApplicationEdgeNamespace is the unkeyed namespace of #711.
func legacyApplicationEdgeNamespace(decision auth.Decision) string {
	return applicationEdgeIDPrefix + legacyIdentityNamespaceDigest(
		legacyApplicationEdgeNamespaceVersion, decision) + "/"
}

// identityNamespaceDigestBytes is the digest's length: 128 bits, 32 hex
// digits, as before #721.
const identityNamespaceDigestBytes = 16

// identityNamespaceDigest is the keyed hex digest, under version, of every
// component that identifies who is writing (see applicationEdgeNamespaces):
// the first 128 bits of the catalog's IdentityNamespaceMAC of
// identityNamespaceMessage. Each caller-scoped namespace (application edges,
// uploads) uses its own version, so the same identity's namespaces are
// unrelated. A catalog failure is reported as the catalog being
// unavailable, whatever its cause.
func (c *Client) identityNamespaceDigest(
	ctx context.Context,
	version string,
	decision auth.Decision,
) (string, error) {
	mac, err := c.policyStore.IdentityNamespaceMAC(
		ctx, identityNamespaceMessage(version, decision))
	if err != nil {
		return "", policyCatalogReadError(ctx, err)
	}
	if len(mac) < identityNamespaceDigestBytes {
		return "", catalogUnavailable()
	}
	return hex.EncodeToString(mac[:identityNamespaceDigestBytes]), nil
}

// legacyIdentityNamespaceDigest is the unkeyed digest of #711 and #718,
// which anyone can recompute from a guessed identity (#721). It is computed
// only to recognise what an identity wrote before #721, and only ever for
// the caller's own identity.
func legacyIdentityNamespaceDigest(version string, decision auth.Decision) string {
	digest := sha256.Sum256(identityNamespaceMessage(version, decision))
	return hex.EncodeToString(digest[:identityNamespaceDigestBytes])
}

// identityNamespaceMessage frames version and the identity's components.
func identityNamespaceMessage(version string, decision auth.Decision) []byte {
	var message []byte
	var length [8]byte
	write := func(value []byte) {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		message = append(message, length[:]...)
		message = append(message, value...)
	}
	write([]byte(version))
	write(decision.AuthorizationDomain())
	write([]byte(decision.Subject()))
	write([]byte(decision.Actor()))
	write([]byte(decision.ClientID()))
	onBehalfOf := decision.OnBehalfOf()
	// Explicit framing. Every component is length-delimited and all but the
	// chain have a fixed count, so the chain's length is already implied;
	// the count keeps the encoding self-describing if that ever changes.
	binary.BigEndian.PutUint64(length[:], uint64(len(onBehalfOf)))
	message = append(message, length[:]...)
	for _, principal := range onBehalfOf {
		write([]byte(principal))
	}
	write([]byte(decision.ServiceRole()))
	write([]byte(decision.ServiceCeilingIdentity()))
	return message
}

// applicationEdgeID places localID in the identity's keyed namespace.
func applicationEdgeID(namespaces identityNamespaces, localID shoal.ID) (shoal.ID, error) {
	if err := shoal.ValidateRequiredID("local graph edge ID", localID); err != nil {
		return "", err
	}
	id := shoal.ID(namespaces.keyed + string(localID))
	if err := shoal.ValidateRequiredID("graph edge ID", id); err != nil {
		return "", err
	}
	return id, nil
}

// requireApplicationEdgeNamespace refuses an edge ID outside the caller's
// namespaces, and reports whether it is in the caller's legacy one. The
// refusal is the same whatever the catalog or corpus holds. Connect accepts
// a legacy ID only to retry an edge already registered under it, and
// refuses any other one with this same error.
func requireApplicationEdgeNamespace(
	namespaces identityNamespaces, edgeID shoal.ID,
) (bool, error) {
	if _, legacy, ok := namespaces.in(string(edgeID)); ok {
		return legacy, nil
	}
	return false, applicationEdgeNamespaceRefused()
}

func applicationEdgeNamespaceRefused() error {
	return shoal.NewError(shoal.ErrorInvalidArgument,
		"graph edge ID is outside the caller's application edge namespace; "+
			"derive it with ApplicationEdgeID")
}

// applicationEdgeNamespacesKey carries, into the trusted edge selector
// Connect calls, the namespaces of the identity connecting.
type applicationEdgeNamespacesKey struct{}

func withApplicationEdgeNamespaces(
	ctx context.Context, namespaces identityNamespaces,
) context.Context {
	return context.WithValue(ctx, applicationEdgeNamespacesKey{}, namespaces)
}

// ApplicationEdgeLocalID returns the local ID of an application edge ID in
// the decision's namespace, and false for any other ID. A trusted
// EdgePolicySelector that selects by edge ID can use it, with the context
// Connect passed it, to key on the ID its caller chose. Since #721 the
// namespace is keyed by the catalog, so it is recognised only in that
// context: with any other context, it reports false. An ID in the identity's
// namespace from before #721 (which Connect accepts only to retry an edge
// registered under it) is recognised too.
func ApplicationEdgeLocalID(
	ctx context.Context, decision auth.Decision, edgeID shoal.ID,
) (shoal.ID, bool) {
	if ctx == nil {
		return "", false
	}
	namespaces, ok := ctx.Value(applicationEdgeNamespacesKey{}).(identityNamespaces)
	// The legacy namespace is a function of the decision's identity alone,
	// so this binds the context's namespaces to that identity.
	if !ok || namespaces.legacy != legacyApplicationEdgeNamespace(decision) {
		return "", false
	}
	local, _, ok := namespaces.in(string(edgeID))
	if !ok {
		return "", false
	}
	return shoal.ID(local), true
}

// ApplicationEdgeID returns the edge ID under which the authenticated caller
// connects an application edge it calls localID. Connect accepts no other
// new edge IDs (#699). The result depends only on the caller's identity,
// localID and the catalog's identity namespace key (#721): it is the same on
// every call and across restarts of one durable catalog, is the ID reads
// report for the edge, and cannot be recomputed from the identity without
// the key. Two different identities never share an ID, so one identity's
// edges never collide with another's; sessions of the same identity share
// its namespace whatever their grants.
func (c *Client) ApplicationEdgeID(
	ctx context.Context,
	localID shoal.ID,
) (shoal.ID, error) {
	if err := shoal.ValidateRequiredID("local graph edge ID", localID); err != nil {
		return "", err
	}
	decision, guard, _, err := c.begin(ctx, auth.OperationConnect)
	if err != nil {
		return "", err
	}
	namespaces, err := c.applicationEdgeNamespaces(ctx, decision)
	if err != nil {
		return "", err
	}
	id, err := applicationEdgeID(namespaces, localID)
	if err != nil {
		return "", err
	}
	if err := guard.Check(ctx); err != nil {
		return "", err
	}
	return id, nil
}
