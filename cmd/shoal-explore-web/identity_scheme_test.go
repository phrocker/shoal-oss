// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
)

// TestIdentitySchemeMigrateIsOneShot (#526): -oidc-identity-scheme-migrate
// names the recorded scheme it replaces. A start proceeds only if the row
// holds exactly that scheme (then records its own) or already holds its own
// (a later replica of the same rollout, the flag still set). A flag left
// set therefore cannot move the row again, and two replicas on different
// schemes cannot flip it back and forth.
func TestIdentitySchemeMigrateIsOneShot(t *testing.T) {
	runtime, err := explorercoord.Open(explorercoord.Config{
		Directory: t.TempDir(), Domain: workspacePublicationDomain,
		Owner: workspaceRuntimeOwner,
		Authority: transaction.Authority{
			Generation: 1, Fence: 1, Holder: workspaceRuntimeOwner,
			Mode:                coordination.WriterModeEmbeddedPrimary,
			RetentionGeneration: 1, HistoryFloor: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	engine := runtime.EmbeddedEngine()
	const issuer = "https://issuer.example"
	scheme := func(name string) oidcIdentityScheme {
		return oidcIdentityScheme{
			issuer: issuer, digest: auth.DigestBytes("scheme", []byte(name)),
		}
	}
	digest := func(name string) coordination.Digest {
		return coordination.Digest(scheme(name).digest)
	}
	start := func(name string, from coordination.Digest) error {
		return stampIdentityScheme(context.Background(), engine, "",
			&identitySchemeConfig{scheme: scheme(name), migrateFrom: from})
	}
	none := coordination.Digest{}
	for _, step := range []struct {
		what string
		name string
		from coordination.Digest
		ok   bool
	}{
		{"a migration with nothing recorded", "a", digest("old"), false},
		{"the first start records its scheme", "a", none, true},
		{"a restart on the same scheme", "a", none, true},
		{"another scheme without the flag", "b", none, false},
		{"another scheme naming the wrong previous scheme", "b", digest("c"), false},
		{"another scheme naming its own digest", "b", digest("b"), false},
		{"the switch, naming the recorded scheme", "b", digest("a"), true},
		{"a later replica of the rollout, the flag still set", "b", digest("a"), true},
		{"a later restart without the flag", "b", none, true},
		{"the old scheme back, with the stale flag", "a", digest("a"), false},
		{"the old scheme back, without a flag", "a", none, false},
		{"a third scheme with the stale flag", "c", digest("a"), false},
	} {
		err := start(step.name, step.from)
		if step.ok && err != nil {
			t.Fatalf("%s: %v", step.what, err)
		}
		if !step.ok && err == nil {
			t.Fatalf("%s started", step.what)
		}
	}
	// The refusal says what to pass: the recorded digest, which is not a
	// secret, so the operator can name it.
	err = start("c", none)
	if !errors.Is(err, errIdentitySchemeMismatch) ||
		!strings.Contains(err.Error(), "-oidc-identity-scheme-migrate="+digest("b").String()) {
		t.Fatalf("mismatch refusal = %v", err)
	}
	// And the explicit switch back works, because it names what it replaces.
	if err := start("a", digest("b")); err != nil {
		t.Fatalf("an explicit switch back: %v", err)
	}
}

func TestIdentitySchemeMigrateFlagShape(t *testing.T) {
	valid := strings.Repeat("0a", 32)
	if got, err := parseIdentitySchemeMigrate(valid); err != nil || got.String() != valid {
		t.Fatalf("a valid digest = %v %v", got, err)
	}
	if got, err := parseIdentitySchemeMigrate(""); err != nil || got != (coordination.Digest{}) {
		t.Fatalf("an empty flag = %v %v", got, err)
	}
	for _, raw := range []string{
		"true", "1", strings.Repeat("0A", 32), "sha256:" + valid, valid[:62],
		valid + "00", strings.Repeat("00", 32),
	} {
		if _, err := parseIdentitySchemeMigrate(raw); err == nil {
			t.Errorf("%q was accepted", raw)
		}
	}
}
