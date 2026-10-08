// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestIdentitySchemeShape (#526): the zero scheme is the legacy one and
// carries nothing; a stable scheme names its prefix, and its legacy
// namespaces are disjoint from it. An identity is under the stable scheme
// only by its prefix, and anything else — including every identity under
// the zero scheme — is legacy, which is what a record written before the
// stamp decodes as.
func TestIdentitySchemeShape(t *testing.T) {
	stable := IdentityScheme{
		Digest: auth.DigestBytes("scheme", []byte("stable")),
		Prefix: "oidcid:https://issuer.example#",
		Legacy: []string{"oidc:https://issuer.example#", "entra:"},
	}
	if err := stable.validate(); err != nil {
		t.Fatalf("a stable scheme was refused: %v", err)
	}
	if err := (IdentityScheme{}).validate(); err != nil {
		t.Fatalf("the legacy scheme was refused: %v", err)
	}
	for name, scheme := range map[string]IdentityScheme{
		"legacy with a prefix":    {Prefix: "oidcid:"},
		"legacy with namespaces":  {Legacy: []string{"oidc:"}},
		"stable without a prefix": {Digest: stable.Digest},
		"empty namespace":         {Digest: stable.Digest, Prefix: stable.Prefix, Legacy: []string{""}},
		"namespace covers prefix": {Digest: stable.Digest, Prefix: stable.Prefix, Legacy: []string{"oidc"}},
		"prefix covers namespace": {Digest: stable.Digest, Prefix: "o", Legacy: []string{"oidc:"}},
	} {
		if err := scheme.validate(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	for identity, want := range map[shoal.ID]auth.Digest{
		"oidcid:https://issuer.example#oid-1": stable.Digest,
		"oidc:https://issuer.example#sub-1":   {},
		"oidcid:https://other.example#oid-1":  {},
		"entra:oid-1":                         {},
	} {
		if got := stable.of(identity); got != want {
			t.Errorf("%s is under %x, want %x", identity, got, want)
		}
		if got := (IdentityScheme{}).of(identity); got != (auth.Digest{}) {
			t.Errorf("under the legacy scheme %s is under %x", identity, got)
		}
	}
	involved := map[shoal.ID]struct{}{
		"oidcid:https://issuer.example#oid-1": {}, "agent-7": {},
	}
	if got := stable.legacyNamespace(involved); got != "" {
		t.Fatalf("no legacy identity is involved, got %q", got)
	}
	involved["entra:oid-2"] = struct{}{}
	involved["oidc:https://issuer.example#sub-1"] = struct{}{}
	// In configured order, so the refusal names one namespace stably.
	if got := stable.legacyNamespace(involved); got != "oidc:https://issuer.example#" {
		t.Fatalf("legacy namespace = %q", got)
	}
	if got := (IdentityScheme{}).legacyNamespace(involved); got != "" {
		t.Fatalf("the legacy scheme refuses %q", got)
	}
}
