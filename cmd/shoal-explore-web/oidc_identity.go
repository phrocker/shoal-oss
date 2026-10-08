// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/golang-jwt/jwt/v5"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// A stable identity claim (#526).
//
// The approval service separates approver from requester by identity. With
// identities derived from sub, that separation is only as good as the
// issuer's promise that sub is the same for one human across clients — the
// public subject type. Entra issues pairwise subjects, and Keycloak
// advertises the pairwise type for every realm, so neither can back an
// approver mapping on sub.
//
// -oidc-identity-claim names another claim — Entra's oid, a Keycloak user ID
// exposed by a mapper — that the operator asserts is one value per human
// across every client. Code cannot prove that; the docs say per issuer how to
// choose it. What the code guarantees is that the claim is read identically
// on both branches, by stableIdentity and nothing else, into a namespace of
// its own (oidcid:) that cannot collide with the sub-derived oidc: or the
// legacy entra: identities.
const (
	// oidcStableIdentityPrefix begins every identity derived from the stable
	// identity claim. It differs from oidcIdentityPrefix ("oidc:") in its
	// first five bytes, so no identity of one form is a prefix of, or equal
	// to, one of the other.
	oidcStableIdentityPrefix = "oidcid:"
	// maxStableIdentityBytes bounds the claim value.
	maxStableIdentityBytes = 256
	// identitySchemeDigestTag domain-separates the identity scheme digest.
	identitySchemeDigestTag = "shoal-explore-web/identity-scheme/v1"
)

// mutableIdentityClaims are claims an issuer lets a human, or an
// administrator in the ordinary course, change. A changed value would be a
// new identity for the same human — and a reused one would be an old
// identity for a new human. They are refused as the last segment of the
// path, compared without regard to case.
var mutableIdentityClaims = map[string]struct{}{
	"email": {}, "preferred_username": {}, "upn": {}, "unique_name": {},
	"name": {},
}

// entraMultiTenantSegments are the issuer path segments of an Entra
// endpoint that is not one tenant's. An object ID is unique only within a
// tenant, so on these an oid would not name one human.
var entraMultiTenantSegments = map[string]struct{}{
	"common": {}, "organizations": {}, "{tenantid}": {},
}

func identityClaimInvalid(reason string) error {
	return shoal.NewError(
		shoal.ErrorInvalidArgument, "-oidc-identity-claim is invalid: "+reason)
}

// parseIdentityClaimFlag reads -oidc-identity-claim: exactly one JSON array
// of path segments, validated as the approver mapping's claim paths are. A
// dotted string is refused (it is not an array); a segment containing a dot
// names a key containing a dot. Empty means not configured.
func parseIdentityClaimFlag(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	var segments []string
	if err := decoder.Decode(&segments); err != nil {
		return nil, identityClaimInvalid(
			"it must be a JSON array of path segments, such as [\"oid\"]")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, identityClaimInvalid("it must hold one JSON array")
	}
	if len(segments) == 0 || len(segments) > auth.MaxGrantClaimPathSegments {
		return nil, identityClaimInvalid(fmt.Sprintf(
			"it must have 1 to %d path segments", auth.MaxGrantClaimPathSegments))
	}
	for _, segment := range segments {
		if !approverText(segment) {
			return nil, identityClaimInvalid("it has an invalid segment")
		}
	}
	if len(segments) == 1 && segments[0] == "sub" {
		return nil, identityClaimInvalid(
			"[\"sub\"] is the subject the stable identity replaces; leave " +
				"the flag unset to name principals by sub")
	}
	last := strings.ToLower(segments[len(segments)-1])
	if _, mutable := mutableIdentityClaims[last]; mutable {
		return nil, identityClaimInvalid(
			"it names a claim a human or administrator can change (" +
				segments[len(segments)-1] + "); choose an immutable identifier")
	}
	return segments, nil
}

// refuseIdentityClaimCombination refuses the configurations under which a
// stable identity would not be the only identity a principal carries.
//
// A non-default subject claim, an actor claim and a delegation claim each
// put a second, client-scoped identity into the decision, and the approval
// service counts every one of them as involved: the separation would again
// be judged partly in a pairwise identity space. The legacy Entra mode names
// principals entra:<oid> and is replaced by this one. An Entra issuer that is
// not one tenant's makes oid ambiguous.
func refuseIdentityClaimCombination(config oidcConfig, issuer string) error {
	if subject := strings.TrimSpace(config.subjectClaim); subject != "" && subject != "sub" {
		return identityClaimInvalid(
			"it cannot be combined with -oidc-subject-claim; the stable " +
				"identity is the subject")
	}
	if config.subjectFallbackClaim != "" || config.identityPrefix != "" ||
		config.trimIdentityValues || config.legacyTenantID != "" {
		return identityClaimInvalid(
			"it cannot be combined with the legacy Entra identity mode; " +
				"use -oidc-identity-claim '[\"oid\"]' on the tenant issuer instead")
	}
	if strings.TrimSpace(config.actorClaim) != "" {
		return identityClaimInvalid(
			"it cannot be combined with -oidc-actor-claim")
	}
	if strings.TrimSpace(config.delegationClaim) != "" {
		return identityClaimInvalid(
			"it cannot be combined with -oidc-delegation-claim")
	}
	parsed, err := url.Parse(issuer)
	if err != nil {
		return identityClaimInvalid("the issuer is not a URL")
	}
	for _, segment := range strings.Split(parsed.Path, "/") {
		if _, multiTenant := entraMultiTenantSegments[strings.ToLower(segment)]; multiTenant {
			return identityClaimInvalid(
				"the issuer path names " + segment + ", which is not one " +
					"tenant; an identifier is unique only within a tenant, so " +
					"use the tenant-specific issuer")
		}
	}
	return nil
}

// stableIdentityValue is the one derivation of the stable identity claim,
// shared by the workspace and approver branches. The claim must be present
// and a string of 1 to maxStableIdentityBytes bytes of valid UTF-8, with no
// control character and no leading or trailing whitespace. It is never
// trimmed: a padded value is refused rather than read as another value, so
// " x" can neither become nor impersonate "x". Anything else — absent, null,
// empty, a number, an object, an array — fails closed, and Authenticate
// collapses every failure to the same generic denial.
func stableIdentityValue(claims jwt.MapClaims, path []string) (string, error) {
	value, present, err := approverClaimValue(claims, path)
	if err != nil {
		return "", errMalformedClaim
	}
	if !present || value == nil {
		return "", errMissingSubject
	}
	text, ok := value.(string)
	if !ok {
		return "", errMalformedClaim
	}
	if text == "" {
		return "", errMissingSubject
	}
	if len(text) > maxStableIdentityBytes || !utf8.ValidString(text) ||
		strings.TrimSpace(text) != text {
		return "", errMalformedClaim
	}
	for _, character := range text {
		if unicode.IsControl(character) {
			return "", errMalformedClaim
		}
	}
	return text, nil
}

// stableIdentity returns oidcid:<iss>#<value> and the raw value. The issuer
// is this deployment's one configured issuer, so the value is everything
// after the first '#' following the prefix; a value containing '#' is still
// one identity.
func (a *oidcAuthenticator) stableIdentity(
	claims jwt.MapClaims,
) (shoal.ID, string, error) {
	value, err := stableIdentityValue(claims, a.identityClaim)
	if err != nil {
		return "", "", err
	}
	return shoal.ID(oidcStableIdentityPrefix + a.expectedIssuer + "#" + value),
		value, nil
}

// oidcIdentityScheme is the identity scheme this authenticator mints under.
type oidcIdentityScheme struct {
	// issuer keys the coordination store's identity-scheme row.
	issuer string
	// digest is the scheme: issuer, claim path and identity format. Every
	// replica serving the issuer must agree on it.
	digest auth.Digest
	// approvals is what the approval service is told: the zero value under
	// a sub-derived scheme, and under a stable one the scheme, its prefix,
	// and the legacy namespaces whose identities cannot be compared with it.
	approvals fleet.IdentityScheme
}

// identityScheme describes the scheme in force. Under the stable scheme the
// legacy namespaces are this issuer's oidc: identities and every entra:
// identity. entra: identities are not issuer-scoped, and a deployment that
// switches to the stable scheme has refused legacy Entra mode, so any entra:
// identity it holds is a legacy one.
func (a *oidcAuthenticator) identityScheme() oidcIdentityScheme {
	var buffer bytes.Buffer
	text := func(value string) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		buffer.Write(length[:])
		buffer.WriteString(value)
	}
	scheme := oidcIdentityScheme{issuer: a.expectedIssuer}
	if a.identityClaim != nil {
		prefix := oidcStableIdentityPrefix + a.expectedIssuer + "#"
		text("stable")
		text(a.expectedIssuer)
		text(fmt.Sprint(len(a.identityClaim)))
		for _, segment := range a.identityClaim {
			text(segment)
		}
		text(prefix)
		scheme.digest = auth.DigestBytes(identitySchemeDigestTag, buffer.Bytes())
		scheme.approvals = fleet.IdentityScheme{
			Digest: scheme.digest,
			Prefix: prefix,
			Legacy: []string{
				oidcIdentityPrefix + a.expectedIssuer + "#", legacyEntraPrefix,
			},
		}
		return scheme
	}
	text("subject")
	text(a.expectedIssuer)
	text(a.subjectClaim)
	text(a.subjectFallbackClaim)
	text(a.identityPrefix)
	text(fmt.Sprint(a.trimIdentityValues))
	scheme.digest = auth.DigestBytes(identitySchemeDigestTag, buffer.Bytes())
	return scheme
}
