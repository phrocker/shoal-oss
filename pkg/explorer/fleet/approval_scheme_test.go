// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const testIssuerFamily = "https://issuer.example#"

func testFamily() []string {
	return []string{"oidc:", "oidcid:", "entra:"}
}

// TestIdentitySchemeShape (#526): the zero scheme configures no rule; a
// configured one names a namespace in force inside its family. The stamp is
// Digest only for an identity in the namespace in force.
func TestIdentitySchemeShape(t *testing.T) {
	stable := IdentityScheme{
		Digest: auth.DigestBytes("scheme", []byte("stable")),
		Prefix: "oidcid:" + testIssuerFamily + "0123456789abcdef#",
		Family: testFamily(),
	}
	subDerived := IdentityScheme{Prefix: "oidc:" + testIssuerFamily, Family: testFamily()}
	for name, scheme := range map[string]IdentityScheme{
		"none": {}, "stable": stable, "sub-derived": subDerived,
	} {
		if err := scheme.validate(); err != nil {
			t.Fatalf("%s was refused: %v", name, err)
		}
	}
	for name, scheme := range map[string]IdentityScheme{
		"a digest without a prefix":   {Digest: stable.Digest},
		"a family without a prefix":   {Family: testFamily()},
		"a prefix outside its family": {Prefix: "mcp:", Family: testFamily()},
		"an empty namespace":          {Prefix: stable.Prefix, Family: []string{""}},
	} {
		if err := scheme.validate(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	for identity, want := range map[shoal.ID]auth.Digest{
		shoal.ID(stable.Prefix + "oid-1"):                       stable.Digest,
		"oidcid:" + testIssuerFamily + "fedcba9876543210#oid-1": {},
		"oidc:" + testIssuerFamily + "sub-1":                    {},
		"entra:oid-1":                                           {},
	} {
		if got := stable.of(identity); got != want {
			t.Errorf("%s is stamped %x, want %x", identity, got, want)
		}
		if got := subDerived.of(identity); got != (auth.Digest{}) {
			t.Errorf("under the sub-derived scheme %s is stamped %x", identity, got)
		}
	}
}

// TestIdentitySchemeOnlyTheNamespaceInForceIsComparable: within the family,
// every namespace but the one in force is foreign — under a stable scheme
// and under the default one alike, with no list of previous schemes, and
// whatever issuer minted it — and identities outside the family
// (development, service, executor and MCP principals) are never refused.
func TestIdentitySchemeOnlyTheNamespaceInForceIsComparable(t *testing.T) {
	oid := IdentityScheme{
		Digest: auth.DigestBytes("scheme", []byte("oid")),
		Prefix: "oidcid:" + testIssuerFamily + "0123456789abcdef#",
		Family: testFamily(),
	}
	uid := IdentityScheme{
		Digest: auth.DigestBytes("scheme", []byte("uid")),
		Prefix: "oidcid:" + testIssuerFamily + "fedcba9876543210#",
		Family: testFamily(),
	}
	subDerived := IdentityScheme{Prefix: "oidc:" + testIssuerFamily, Family: testFamily()}
	outside := []shoal.ID{
		"alice", "dev-principal", "mcp:client-7", "service:indexer",
		"oidcexec:https://issuer.example#runner-1", "oidcexec:bob", "gateway",
		"shoal-explore-web-oidc",
	}
	for name, probe := range map[string]struct {
		scheme  IdentityScheme
		foreign shoal.ID
		want    string
	}{
		"sub-derived identity under a stable scheme": {oid, "oidc:" + testIssuerFamily + "bob", "oidc:" + testIssuerFamily},
		"another stable path under a stable scheme":  {oid, shoal.ID(uid.Prefix + "bob"), "oidcid:" + testIssuerFamily},
		"entra under a stable scheme":                {oid, "entra:bob", "entra:"},
		"stable identity under the default scheme":   {subDerived, shoal.ID(oid.Prefix + "bob"), "oidcid:" + testIssuerFamily},
		"entra under the default scheme":             {subDerived, "entra:bob", "entra:"},
		"another issuer's sub under the default scheme": {subDerived,
			"oidc:https://old-issuer.example#bob", "oidc:https://old-issuer.example#"},
		"another issuer's stable identity under a stable scheme": {oid,
			"oidcid:https://old-issuer.example#0123456789abcdef#bob",
			"oidcid:https://old-issuer.example#"},
		"another issuer's sub under a stable scheme": {oid,
			"oidc:https://sts.windows.net/tenant/#bob", "oidc:https://sts.windows.net/tenant/#"},
	} {
		t.Run(name, func(t *testing.T) {
			current := shoal.ID(probe.scheme.Prefix + "carol")
			clean := append(append([]shoal.ID(nil), outside...), current)
			if got := probe.scheme.foreignNamespace(clean); got != "" {
				t.Fatalf("an identity outside the family, or in force, is foreign: %q", got)
			}
			if got := probe.scheme.foreignNamespace(
				append(clean, probe.foreign)); got != probe.want {
				t.Fatalf("foreign namespace = %q, want %q", got, probe.want)
			}
		})
	}
	if got := (IdentityScheme{}).foreignNamespace(
		[]shoal.ID{"oidc:" + testIssuerFamily + "bob", "entra:bob"}); got != "" {
		t.Fatalf("a host with no scheme refuses %q", got)
	}
}
