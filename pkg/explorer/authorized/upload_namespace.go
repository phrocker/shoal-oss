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
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Upload source URIs are routed through the uploader's namespace (#712).
//
// The browser upload route and the MCP ingest tool name a document by a
// caller-chosen filename, as UploadSourceURIPrefix + the escaped name. Before
// #712 that URI was stored as it was, so every principal's upload of a name
// was the same document. Ingesting at a URI whose source claim the caller
// cannot read is refused (writing over a hidden document is not acceptable),
// while an absent URI succeeds, so a reader could test whether a hidden
// document exists under a guessed filename.
//
// An upload name is therefore a request, not a URI. Ingest routes it, under
// the mutation lock and before anything is written, to:
//
//  1. the caller's own namespaced URI (UploadSourceURIPrefix, "u-", a digest
//     of the caller's identity, "/", the name) when anything is stored there:
//     a claim, a registration or a base document;
//  2. otherwise the un-namespaced URI of the name, the URI every upload had
//     before #712, when the caller is authorized under every rule recorded
//     for it (its claim's rule, a pending claim's previous rule, and its
//     current registration);
//  3. otherwise the one other identity's namespaced URI of the name that is
//     stored in the base and the caller is authorized under in the same way,
//     when there is exactly one;
//  4. otherwise the caller's own namespaced URI.
//
// Steps 2 and 3 only ever choose a URI the caller is authorized under, and
// skip every other one exactly as if it were absent. So for a caller who
// cannot read what is stored under a name, the route (and so the outcome) is
// the one it would be if the name had never been uploaded: its own namespace,
// where only its own identity has written. Principals who can read a
// document still share it by name, so relabelling (#630, #680) and shared
// editing by its readers keep working, and documents uploaded before #712
// stay re-uploadable by everyone authorized under them.
//
// Namespacing only when the caller cannot read the existing claim would not
// do: an absent name would then route to the un-namespaced URI and a hidden
// one to the namespace, and the two outcomes differ. Here the default, for an
// absent name and for a hidden one alike, is the namespace.
//
// A namespaced URI is accepted as an ingest's source URI only in the caller's
// own namespace. Any other URI under UploadSourceURIPrefix that is not a bare
// name is refused with one InvalidArgument, before anything is read. Source
// URIs outside UploadSourceURIPrefix (file://, https://, a host's own
// schemes) are not routed: no HTTP or MCP route lets a caller choose one, so
// they come from trusted ingesters (operator configuration, cmd/shoal-explore,
// library hosts), and ingesting one over a hidden document is refused with
// NotFound as before.
//
// The namespace binds the identity, not its current authority (as the
// application edge namespace does, #699): a narrowed session of the same
// identity routes to the same namespace, so it can still tell whether that
// identity's broader sessions uploaded a name it cannot read.
const UploadSourceURIPrefix = "upload://workspace/"

// uploadNamespaceSegmentPrefix starts the namespace segment of a namespaced
// upload URI. An upload name never contains "/" (the upload route escapes
// it), so an un-namespaced upload URI has exactly one segment after
// UploadSourceURIPrefix, and a namespaced one has two.
const uploadNamespaceSegmentPrefix = "u-"

// uploadNamespaceDigestLength is the length of the hex identity digest in the
// namespace segment.
const uploadNamespaceDigestLength = 32

// uploadNamespaceVersion separates the upload namespace's digest from the
// application edge namespace's digest of the same identity.
const uploadNamespaceVersion = "shoal/authorized/upload-namespace/v1"

// uploadNamespace returns the URI prefix of every upload the decision's
// identity owns.
func uploadNamespace(decision auth.Decision) string {
	return UploadSourceURIPrefix + uploadNamespaceSegmentPrefix +
		identityNamespaceDigest(uploadNamespaceVersion, decision) + "/"
}

type uploadSourceKind uint8

const (
	// notUploadSource is a URI outside UploadSourceURIPrefix, stored as is.
	notUploadSource uploadSourceKind = iota
	// uploadSourceName is UploadSourceURIPrefix and a bare name, routed.
	uploadSourceName
	// uploadSourceOwn is a URI in the caller's own namespace, stored as is.
	uploadSourceOwn
)

// classifyUploadSourceURI says how Ingest treats sourceURI. For a bare
// upload name it returns the name (the URI's segment after
// UploadSourceURIPrefix). It reads nothing: a refusal depends only on the
// caller's identity and the URI it sent.
func classifyUploadSourceURI(
	decision auth.Decision, sourceURI string,
) (uploadSourceKind, string, error) {
	if !strings.HasPrefix(sourceURI, UploadSourceURIPrefix) {
		return notUploadSource, "", nil
	}
	rest := sourceURI[len(UploadSourceURIPrefix):]
	if rest != "" && !strings.Contains(rest, "/") {
		return uploadSourceName, rest, nil
	}
	own := uploadNamespace(decision)
	if len(sourceURI) > len(own) && strings.HasPrefix(sourceURI, own) &&
		!strings.Contains(sourceURI[len(own):], "/") {
		return uploadSourceOwn, "", nil
	}
	return notUploadSource, "", uploadNamespaceRefused()
}

func uploadNamespaceRefused() error {
	return shoal.NewError(shoal.ErrorInvalidArgument,
		"upload source URI is neither an upload name nor in the caller's "+
			"upload namespace; ingest "+UploadSourceURIPrefix+"<name>")
}

// namespacedUploadURI reports whether sourceURI is name's URI in some
// identity's upload namespace. routeUploadSource asks it only once the
// caller's own URI of name is known to be absent from the base. Only the
// authorized client writes upload URIs, and it refuses any namespaced one
// outside its caller's namespace, so the digest check guards only against a
// trusted direct write to the base.
func namespacedUploadURI(sourceURI, name string) bool {
	if !strings.HasPrefix(sourceURI, UploadSourceURIPrefix+uploadNamespaceSegmentPrefix) {
		return false
	}
	rest := sourceURI[len(UploadSourceURIPrefix)+len(uploadNamespaceSegmentPrefix):]
	if len(rest) != uploadNamespaceDigestLength+1+len(name) ||
		rest[uploadNamespaceDigestLength] != '/' ||
		rest[uploadNamespaceDigestLength+1:] != name {
		return false
	}
	for _, digit := range rest[:uploadNamespaceDigestLength] {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return false
		}
	}
	return true
}

// routeUploadSource returns the source URI an ingest of the upload name
// writes, as the comment on UploadSourceURIPrefix describes. It runs under
// the mutation lock, so the route cannot change before the claim is taken.
func (c *Client) routeUploadSource(
	ctx context.Context,
	decision auth.Decision,
	now time.Time,
	name string,
) (string, error) {
	own := uploadNamespace(decision) + name
	summaries, err := c.base.Documents(ctx)
	if err != nil {
		return "", err
	}
	stored, err := c.sourceURIStored(ctx, own, summaries)
	if err != nil || stored {
		return own, err
	}
	legacy := UploadSourceURIPrefix + name
	eligible, err := c.uploadRouteEligible(ctx, decision, now, legacy)
	if err != nil || eligible {
		return legacy, err
	}
	shared, count := "", 0
	seen := make(map[string]struct{})
	for _, summary := range summaries {
		uri := summary.SourceURI
		if _, done := seen[uri]; done || !namespacedUploadURI(uri, name) {
			continue
		}
		seen[uri] = struct{}{}
		eligible, err := c.uploadRouteEligible(ctx, decision, now, uri)
		if err != nil {
			return "", err
		}
		if eligible {
			shared = uri
			count++
		}
	}
	if count == 1 {
		return shared, nil
	}
	return own, nil
}

// sourceURIStored reports whether a claim, a registration or a base document
// exists for sourceURI. It is asked only of the caller's own namespace.
func (c *Client) sourceURIStored(
	ctx context.Context,
	sourceURI string,
	summaries []explorer.DocumentSummary,
) (bool, error) {
	for _, summary := range summaries {
		if summary.SourceURI == sourceURI {
			return true, nil
		}
	}
	if _, claimed, err := c.policyStore.SourceClaim(ctx, sourceURI); err != nil {
		return false, policyCatalogReadError(ctx, err)
	} else if claimed {
		return true, nil
	}
	_, registered, err := c.policyStore.CurrentRevision(
		ctx, explorer.SourceDocumentID(sourceURI))
	if err != nil {
		return false, policyCatalogReadError(ctx, err)
	}
	return registered, nil
}

// uploadRouteEligible reports whether an upload name may route to sourceURI:
// something is recorded for it and the caller is authorized to ingest under
// every rule recorded for it. Anything else, including a rule that fails to
// evaluate, is skipped as if absent, so the route never depends on a rule the
// caller is outside.
func (c *Client) uploadRouteEligible(
	ctx context.Context,
	decision auth.Decision,
	now time.Time,
	sourceURI string,
) (bool, error) {
	claim, claimed, err := c.policyStore.SourceClaim(ctx, sourceURI)
	if err != nil {
		return false, policyCatalogReadError(ctx, err)
	}
	registration, registered, err := c.policyStore.CurrentRevision(
		ctx, explorer.SourceDocumentID(sourceURI))
	if err != nil {
		return false, policyCatalogReadError(ctx, err)
	}
	var rules []AccessRule
	if claimed {
		rules = append(rules, claim.Rule)
		if claim.Pending && claim.PreviousRule != nil {
			rules = append(rules, *claim.PreviousRule)
		}
	}
	if registered {
		rules = append(rules, registration.Rule)
	}
	if len(rules) == 0 {
		return false, nil
	}
	for _, rule := range rules {
		allowed, err := ruleAllows(rule, decision, auth.OperationIngest, now)
		if err != nil || !allowed {
			return false, nil
		}
	}
	return true, nil
}

// OwnUploadSourceURI returns the source URI in the authenticated caller's
// upload namespace for the upload name (the segment after
// UploadSourceURIPrefix, escaped as the upload route escapes a filename). An
// ingest of UploadSourceURIPrefix + name by the caller writes there unless
// the caller is authorized under a document already stored under the name
// (see UploadSourceURIPrefix). The result depends only on the caller's
// identity and the name, and is the same on every call and across restarts.
func (c *Client) OwnUploadSourceURI(
	ctx context.Context,
	name string,
) (string, error) {
	if name == "" || strings.Contains(name, "/") {
		return "", shoal.NewError(shoal.ErrorInvalidArgument,
			"upload name must be one non-empty URI segment")
	}
	decision, guard, _, err := c.begin(ctx, auth.OperationIngest)
	if err != nil {
		return "", err
	}
	if err := guard.Check(ctx); err != nil {
		return "", err
	}
	return uploadNamespace(decision) + name, nil
}
