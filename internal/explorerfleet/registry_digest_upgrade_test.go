// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/atpltest"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// The v1 registry mutation digests a build before #521 recorded for
// upgradeSpec, registered exactly as upgradeLifecycle does. They were computed
// on origin/main at 6cf88b0c by driving that build's fleet.Service through
// internal/atpltest and reading the digest its recorder was handed. They are
// literals so that nothing in this build produces the stored side: a test that
// recomputed v1 here would still pass with the version switch wired backwards.
const (
	upgradeV1Plain = "e10974c0961e69614adeecb7d7771547" +
		"49fa84eb66ef6e56c8f69af46aea6637"
	upgradeV1Approval = "1fa9069c3eeb9ab30df730b30bbf955d" +
		"07de96fc4b94682428a2abc4b62b2a47"
)

func upgradeSpec(requireApproval bool) fleet.Spec {
	return fleet.Spec{
		ID: "upgrade-agent", AuthorizationDomain: []byte(atpltest.Domain),
		Scopes: []fleet.Scope{{
			SourceID: []byte("source"), PolicyID: []byte("policy"),
		}},
		ExecutorRef: "local",
		Capabilities: []fleet.Capability{{Name: "ops", Actions: []fleet.Action{{
			Name:             "deploy",
			InputSchema:      json.RawMessage(`{"type":"object"}`),
			OutputSchema:     json.RawMessage(`{"type":"object"}`),
			Effects:          fleet.Effects{fleet.EffectMutatesExternal},
			RequiresApproval: requireApproval,
		}}}},
		LeaseExpiresAt: time.Date(2030, 1, 1, 1, 0, 0, 0, time.UTC),
	}
}

// upgradeLifecycle registers upgradeSpec through this build's real
// fleet.Service and returns the lifecycle it handed its recorder.
func upgradeLifecycle(t *testing.T, requireApproval bool) fleet.Lifecycle {
	t.Helper()
	registry := atpltest.NewRegistry(t,
		atpltest.NewClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
		atpltest.Executors{"local": atpltest.Executor{
			Max: fleet.Effects{fleet.EffectMutatesExternal},
		}},
		[]string{"source"}, []string{"policy"})
	if _, err := registry.Register(
		t, upgradeSpec(requireApproval), 0, "upgrade-key",
	); err != nil {
		t.Fatal(err)
	}
	records := registry.Recorder.Records()
	if len(records) != 1 {
		t.Fatalf("lifecycle records = %d, want 1", len(records))
	}
	return records[0]
}

func mustDigest(t *testing.T, value string) [32]byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("bad digest literal %q", value)
	}
	var digest [32]byte
	copy(digest[:], decoded)
	return digest
}

// TestRegistryDigestUpgradeReconcilesV1Receipt is the upgrade test for #521.
// A receipt the previous build wrote, carrying the literal v1 digest, must
// still be accepted as the same mutation when the request is retried after
// the upgrade, and a changed descriptor must still be refused.
func TestRegistryDigestUpgradeReconcilesV1Receipt(t *testing.T) {
	upgraded := upgradeLifecycle(t, false)
	storedV1 := mustDigest(t, upgradeV1Plain)
	// The service hands the recorder the v1 digest the old build computed,
	// and writes a different (v2) one.
	if upgraded.LegacyMutationDigest != storedV1 {
		t.Fatalf("legacy digest = %x, want the old build's %s",
			upgraded.LegacyMutationDigest, upgradeV1Plain)
	}
	if upgraded.MutationDigest == storedV1 {
		t.Fatal("the upgraded build still writes the v1 digest")
	}
	asserted, err := fleet.CallerAssertedRegistryReason(
		upgraded.ReasonCode, upgraded.ReasonDetail)
	if err != nil {
		t.Fatal(err)
	}
	// What the old build recorded for this request: the same lifecycle, with
	// the v1 digest it computed, under the identity it wrote.
	old := upgraded
	old.MutationDigest = storedV1
	old.LegacyMutationDigest = [32]byte{}

	changedSource := upgradeLifecycle(t, true)
	if changedSource.LegacyMutationDigest != mustDigest(t, upgradeV1Approval) {
		t.Fatalf("legacy digest of the changed descriptor = %x, want %s",
			changedSource.LegacyMutationDigest, upgradeV1Approval)
	}
	// The same request retried with a changed descriptor.
	changed := upgraded
	changed.MutationDigest = changedSource.MutationDigest
	changed.LegacyMutationDigest = changedSource.LegacyMutationDigest

	for _, prior := range []struct {
		name    string
		receipt interaction.Session
	}{
		{"v3", func() interaction.Session {
			receipt := trustedReceipt(old, asserted)
			receipt.ID = v3LifecycleSessionID(old)
			return receipt
		}()},
		// A v1 identity is itself derived from the v1 digest, so finding the
		// receipt at all depends on recomputing it correctly.
		{"v1", func() interaction.Session {
			receipt := trustedReceipt(old, interaction.CallerAssertedReason{})
			receipt.ID = v1LifecycleSessionID(old)
			return receipt
		}()},
	} {
		store := &reconcilingLifecycleStore{
			trustedLifecycleRecorder: trustedLifecycleRecorder{lifecycle: upgraded},
			stored:                   prior.receipt,
		}
		recorder, err := NewLifecycleRecorderWithReader(store, store)
		if err != nil {
			t.Fatal(err)
		}
		if err := recorder.RecordLifecycle(
			context.Background(), upgraded,
		); err != nil {
			t.Fatalf("%s receipt: retry across the upgrade = %v", prior.name, err)
		}
		if len(store.requests) != 0 || store.stored.ID != prior.receipt.ID {
			t.Fatalf("%s receipt: the retry wrote a new receipt instead of "+
				"reconciling with the stored one", prior.name)
		}
		err = recorder.RecordLifecycle(context.Background(), changed)
		if prior.name == "v3" {
			// The v3 identity does not depend on the digest, so the changed
			// retry finds this receipt and must conflict with it.
			if !shoal.IsErrorCode(err, shoal.ErrorConflict) ||
				len(store.requests) != 0 {
				t.Fatalf("%s receipt: changed descriptor across the upgrade "+
					"= %v", prior.name, err)
			}
			continue
		}
		// A v1 identity is derived from the digest, so a changed descriptor
		// is not a retry of that receipt at all (as before the upgrade): it
		// is not reconciled with it, and is offered as a new receipt.
		if len(store.requests) != 1 ||
			store.requests[0].ID != LifecycleReceiptID(
				changed.Operation, changed.RequestID, changed.AgentID) {
			t.Fatalf("%s receipt: changed descriptor reconciled with it: %v",
				prior.name, err)
		}
	}
}

// TestRegistryDigestUpgradeWritesV2 checks the other half: a request first
// seen after the upgrade is written under the v4 identity with the v2 digest,
// reconciles with itself, and refuses a changed descriptor.
func TestRegistryDigestUpgradeWritesV2(t *testing.T) {
	upgraded := upgradeLifecycle(t, false)
	asserted, err := fleet.CallerAssertedRegistryReason(
		upgraded.ReasonCode, upgraded.ReasonDetail)
	if err != nil {
		t.Fatal(err)
	}
	store := &reconcilingLifecycleStore{
		trustedLifecycleRecorder: trustedLifecycleRecorder{lifecycle: upgraded},
	}
	recorder, err := NewLifecycleRecorderWithReader(store, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordLifecycle(context.Background(), upgraded); err != nil {
		t.Fatal(err)
	}
	if store.stored.ID != LifecycleReceiptID(
		upgraded.Operation, upgraded.RequestID, upgraded.AgentID,
	) || store.stored.ID == v3LifecycleSessionID(upgraded) {
		t.Fatalf("new receipt identity = %s, want the v4 identity", store.stored.ID)
	}
	if store.stored.QueryDigest != lifecycleQueryDigest(upgraded, asserted) ||
		store.stored.QueryDigest == lifecycleQueryDigest(
			lifecycleWithV1RegistryDigest(upgraded), asserted) {
		t.Fatal("new receipt does not carry the v2 registry digest")
	}
	if err := recorder.RecordLifecycle(context.Background(), upgraded); err != nil {
		t.Fatalf("retry of a v4 receipt = %v", err)
	}
	changedSource := upgradeLifecycle(t, true)
	changed := upgraded
	changed.MutationDigest = changedSource.MutationDigest
	changed.LegacyMutationDigest = changedSource.LegacyMutationDigest
	err = recorder.RecordLifecycle(context.Background(), changed)
	if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("changed descriptor against a v4 receipt = %v", err)
	}
}
