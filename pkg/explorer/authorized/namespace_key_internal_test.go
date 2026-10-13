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

// The identity namespace key (#721): when it is written, how strictly it is
// read back, what an older binary does with it, and that it never leaves
// the store.

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

var namespaceKeyTestMessage = []byte("identity namespace key test message")

func namespaceMACForTest(t *testing.T, store PolicyStore) []byte {
	t.Helper()
	mac, err := store.IdentityNamespaceMAC(context.Background(), namespaceKeyTestMessage)
	if err != nil {
		t.Fatal(err)
	}
	return mac
}

// requireOldBinaryRefusesNamespaceKey checks the key row the way every
// durable binary before #721 reads it: a row under the source-claim prefix
// that is not the format floor's exact row, decoded strictly as a source
// claim, so such a binary refuses to open.
func requireOldBinaryRefusesNamespaceKey(t *testing.T, dir string) {
	t.Helper()
	value, ok := rawPolicyCell(t, dir, []byte(policyRowNamespaceKey))
	if !ok {
		t.Fatal("the catalog holds no identity namespace key")
	}
	if !strings.HasPrefix(policyRowNamespaceKey, policyRowSourceClaim) ||
		policyRowNamespaceKey == policyRowFormatFloor {
		t.Fatal("the key row is not a source-claim row other than the floor")
	}
	var claim persistedSourceClaim
	if err := decodePolicyRecord(value, policyKindSourceClaim, &claim); err == nil ||
		!strings.Contains(err.Error(), "kind") {
		t.Fatalf("an old binary would decode the key as a source claim: %v", err)
	}
	var record persistedNamespaceKey
	if err := decodePolicyRecord(value, policyKindNamespaceKey, &record); err != nil ||
		validateNamespaceKey(record) != nil {
		t.Fatalf("key record = %v", err)
	}
}

// TestNamespaceKeyIsWrittenOnlyByAReadWriteOpen: an offline listing of a
// catalog without a key writes nothing, key included, and reports the MAC
// unavailable; the first read-write open writes the key, which then is the
// same on every reopen, read-write or offline.
func TestNamespaceKeyIsWrittenOnlyByAReadWriteOpen(t *testing.T) {
	for _, test := range []struct {
		name    string
		catalog func(t *testing.T) string
	}{
		{"pre-floor catalog", preFloorCatalog},
		{"catalog with a floor and no key", func(t *testing.T) string {
			// What a binary from #695 to #721 leaves: the floor, no key.
			dir := preFloorCatalog(t)
			store, err := OpenExistingDurablePolicyStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.PutInteractionRevisions(
				context.Background(), "session", bindingTestRevisions); err != nil {
				t.Fatal(err)
			}
			closeDurableForTest(t, store)
			if _, ok := rawPolicyCell(t, dir, []byte(policyRowFormatFloor)); !ok {
				t.Fatal("no floor")
			}
			return dir
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := test.catalog(t)
			if _, ok := rawPolicyCell(t, dir, []byte(policyRowNamespaceKey)); ok {
				t.Fatal("the starting catalog already holds a key")
			}
			before := catalogSnapshot(t, dir)
			listing, err := OpenExistingDurablePolicyStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := listing.IdentityNamespaceMAC(
				context.Background(), namespaceKeyTestMessage); !shoal.IsErrorCode(
				err, shoal.ErrorUnavailable) {
				t.Fatalf("MAC of a keyless offline store = %v, want unavailable", err)
			}
			closeDurableForTest(t, listing)
			requireUnchanged(t, dir, before)

			store := openDurableForTest(t, dir)
			first := namespaceMACForTest(t, store)
			closeDurableForTest(t, store)
			requireOldBinaryRefusesNamespaceKey(t, dir)

			store = openDurableForTest(t, dir)
			if again := namespaceMACForTest(t, store); !bytes.Equal(again, first) {
				t.Fatal("the key changed across a read-write reopen")
			}
			closeDurableForTest(t, store)
			before = catalogSnapshot(t, dir)
			listing, err = OpenExistingDurablePolicyStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if again := namespaceMACForTest(t, listing); !bytes.Equal(again, first) {
				t.Fatal("an offline listing read another key")
			}
			closeDurableForTest(t, listing)
			requireUnchanged(t, dir, before)
		})
	}
}

// TestNamespaceKeysDifferPerCatalog: two catalogs, memory or durable, never
// share a key; one memory store keeps its key.
func TestNamespaceKeysDifferPerCatalog(t *testing.T) {
	memory := NewMemoryPolicyStore()
	first := namespaceMACForTest(t, memory)
	if again := namespaceMACForTest(t, memory); !bytes.Equal(again, first) {
		t.Fatal("a memory store's key changed")
	}
	durable := openDurableForTest(t, t.TempDir())
	defer closeDurableForTest(t, durable)
	other := openDurableForTest(t, t.TempDir())
	defer closeDurableForTest(t, other)
	seen := map[string]string{hex.EncodeToString(first): "memory"}
	for name, store := range map[string]PolicyStore{
		"another memory store": NewMemoryPolicyStore(),
		"zero memory store":    &MemoryPolicyStore{},
		"durable":              durable,
		"another durable":      other,
	} {
		mac := hex.EncodeToString(namespaceMACForTest(t, store))
		if previous, ok := seen[mac]; ok {
			t.Fatalf("%s shares a key with %s", name, previous)
		}
		seen[mac] = name
	}
}

// TestNamespaceKeyIsDecodedStrictly: a key of the wrong length, an all-zero
// key, a key record of another kind, a key without the format floor and two
// different keys each refuse the open as corrupt, without the key in the
// error.
func TestNamespaceKeyIsDecodedStrictly(t *testing.T) {
	key := func(seq uint64, value []byte) func(*testing.T) []byte {
		return func(t *testing.T) []byte {
			return mustEncodePolicyRecord(t, policyKindNamespaceKey,
				persistedNamespaceKey{Seq: seq, Key: value})
		}
	}
	valid := bytes.Repeat([]byte{0x5a}, namespaceKeyBytes)
	for _, test := range []struct {
		name     string
		value    func(*testing.T) []byte
		preFloor bool
		accepted bool
	}{
		{"valid key", key(900, valid), false, true},
		{"short key", key(900, valid[:namespaceKeyBytes-1]), false, false},
		{"long key", key(900, append(append([]byte(nil), valid...), 1)), false, false},
		{"empty key", key(900, nil), false, false},
		{"zero key", key(900, make([]byte, namespaceKeyBytes)), false, false},
		{"key of the wrong kind", func(t *testing.T) []byte {
			return mustEncodePolicyRecord(t, policyKindFormatFloor,
				persistedNamespaceKey{Seq: 900, Key: valid})
		}, false, false},
		{"key without the floor", key(900, valid), true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var dir string
			if test.preFloor {
				dir = preFloorCatalog(t)
			} else {
				// A catalog written by a binary from #695 to #721: the
				// floor, no key.
				dir = preFloorCatalog(t)
				store, err := OpenExistingDurablePolicyStore(dir)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.PutInteractionRevisions(
					context.Background(), "session", bindingTestRevisions); err != nil {
					t.Fatal(err)
				}
				closeDurableForTest(t, store)
			}
			corruptPolicyRow(t, dir, []byte(policyRowNamespaceKey), test.value(t))
			// The offline open reads the same rows, writing nothing.
			for _, open := range []func(string) (*DurablePolicyStore, error){
				OpenExistingDurablePolicyStore, OpenDurablePolicyStore,
			} {
				store, err := open(dir)
				if err == nil {
					if test.accepted {
						mac := namespaceMACForTest(t, store)
						want := namespaceMAC((*[namespaceKeyBytes]byte)(valid),
							namespaceKeyTestMessage)
						if !bytes.Equal(mac, want) {
							t.Fatal("the stored key was not the one used")
						}
					}
					_ = store.Close()
				}
				if (err == nil) != test.accepted {
					t.Fatalf("open = %v, want accepted=%v", err, test.accepted)
				}
				if err != nil && (!shoal.IsErrorCode(err, shoal.ErrorInternal) ||
					strings.Contains(err.Error(), hex.EncodeToString(valid)) ||
					strings.Contains(err.Error(), string(valid))) {
					t.Fatalf("open = %v, want internal without the key", err)
				}
			}
		})
	}
}

// TestNamespaceKeyIsNeverRotated: a second, different key cell for the row
// refuses the open, rather than either key silently winning.
func TestNamespaceKeyIsNeverRotated(t *testing.T) {
	dir := t.TempDir()
	store := openDurableForTest(t, dir)
	original := namespaceMACForTest(t, store)
	closeDurableForTest(t, store)
	// Rewriting the same key is harmless.
	value, ok := rawPolicyCell(t, dir, []byte(policyRowNamespaceKey))
	if !ok {
		t.Fatal("no key")
	}
	corruptPolicyRow(t, dir, []byte(policyRowNamespaceKey), value)
	store = openDurableForTest(t, dir)
	if again := namespaceMACForTest(t, store); !bytes.Equal(again, original) {
		t.Fatal("the key changed")
	}
	closeDurableForTest(t, store)
	corruptPolicyRow(t, dir, []byte(policyRowNamespaceKey),
		mustEncodePolicyRecord(t, policyKindNamespaceKey, persistedNamespaceKey{
			Seq: 1 << 40, Key: bytes.Repeat([]byte{0x33}, namespaceKeyBytes),
		}))
	if reopened, err := OpenDurablePolicyStore(dir); err == nil {
		_ = reopened.Close()
		t.Fatal("a catalog with two different keys opened")
	} else if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("open = %v, want internal", err)
	}
}

// TestNamespaceMACIsUnavailableFromAClosedOrDivergedStore: the MAC follows
// the store's own availability.
func TestNamespaceMACIsUnavailableFromAClosedOrDivergedStore(t *testing.T) {
	store := openDurableForTest(t, t.TempDir())
	namespaceMACForTest(t, store)
	store.markDiverged()
	if _, err := store.IdentityNamespaceMAC(
		context.Background(), namespaceKeyTestMessage); !shoal.IsErrorCode(
		err, shoal.ErrorUnavailable) {
		t.Fatalf("diverged MAC = %v", err)
	}
	closeDurableForTest(t, store)
	if _, err := store.IdentityNamespaceMAC(
		context.Background(), namespaceKeyTestMessage); !shoal.IsErrorCode(
		err, shoal.ErrorUnavailable) {
		t.Fatalf("closed MAC = %v", err)
	}
	var nilStore *DurablePolicyStore
	if _, err := nilStore.IdentityNamespaceMAC(
		context.Background(), namespaceKeyTestMessage); !shoal.IsErrorCode(
		err, shoal.ErrorUnavailable) {
		t.Fatalf("nil durable MAC = %v", err)
	}
	var nilMemory *MemoryPolicyStore
	if _, err := nilMemory.IdentityNamespaceMAC(
		context.Background(), namespaceKeyTestMessage); !shoal.IsErrorCode(
		err, shoal.ErrorUnavailable) {
		t.Fatalf("nil memory MAC = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewMemoryPolicyStore().IdentityNamespaceMAC(
		cancelled, namespaceKeyTestMessage); !shoal.IsErrorCode(err, shoal.ErrorCanceled) {
		t.Fatalf("cancelled MAC = %v", err)
	}
}

// TestApplicationEdgeLocalIDNeedsConnectsContext: the local ID is recognised
// in the keyed and the legacy namespace of the decision's identity, only
// with the namespaces Connect places in the selector's context, and never
// for another identity.
func TestApplicationEdgeLocalIDNeedsConnectsContext(t *testing.T) {
	now := time.Date(2026, time.October, 13, 12, 0, 0, 0, time.UTC)
	decide := func(subject shoal.ID) auth.Decision {
		decision, err := auth.NewDecision(auth.DecisionConfig{
			Subject: subject, Actor: subject + "-actor",
			AuthorizationDomain:   []byte("domain-one"),
			AllowedOperations:     []auth.Operation{auth.OperationConnect},
			PermittedSourceIDs:    [][]byte{[]byte("source-a")},
			PermittedPolicyIDs:    [][]byte{[]byte("policy-a")},
			PolicyGeneration:      1,
			AuthenticationExpires: now.Add(time.Hour),
			RequestID:             subject + "-request",
		})
		if err != nil {
			t.Fatal(err)
		}
		return decision
	}
	writer, other := decide("writer"), decide("other")
	client := &Client{policyStore: NewMemoryPolicyStore()}
	namespaces, err := client.applicationEdgeNamespaces(context.Background(), writer)
	if err != nil {
		t.Fatal(err)
	}
	if namespaces.keyed == namespaces.legacy {
		t.Fatal("the keyed namespace is the legacy one")
	}
	ctx := withApplicationEdgeNamespaces(context.Background(), namespaces)
	keyed := shoal.ID(namespaces.keyed + "edge")
	legacy := shoal.ID(namespaces.legacy + "edge")
	for _, id := range []shoal.ID{keyed, legacy} {
		if local, ok := ApplicationEdgeLocalID(ctx, writer, id); !ok || local != "edge" {
			t.Fatalf("ApplicationEdgeLocalID(%s) = %q, %v", id, local, ok)
		}
		if local, ok := ApplicationEdgeLocalID(context.Background(), writer, id); ok {
			t.Fatalf("recognised without Connect's context: %q", local)
		}
		if local, ok := ApplicationEdgeLocalID(ctx, other, id); ok {
			t.Fatalf("another identity's ID parsed as its own: %q", local)
		}
	}
	//nolint:staticcheck // A nil context is what this checks.
	if _, ok := ApplicationEdgeLocalID(nil, writer, keyed); ok {
		t.Fatal("recognised with a nil context")
	}
	for _, id := range []shoal.ID{
		"plain-edge", shoal.ID(namespaces.keyed), shoal.ID(namespaces.legacy),
	} {
		if _, ok := ApplicationEdgeLocalID(ctx, writer, id); ok {
			t.Fatalf("%q parsed", id)
		}
	}
	otherNamespaces, err := client.applicationEdgeNamespaces(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ApplicationEdgeLocalID(
		withApplicationEdgeNamespaces(context.Background(), otherNamespaces),
		writer, keyed); ok {
		t.Fatal("another identity's context recognised the writer's ID")
	}
}
