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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"log/slog"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// The identity namespace key (#721).
//
// The application edge namespace (#699, #711) and the upload namespace
// (#712, #718) name an identity by a digest of its components: domain,
// subject, actor, client, delegation chain, service role and ceiling. Every
// one of them is known or guessable, and the version strings are in the open
// source, so an unkeyed digest let anyone who reads an edge ID or a
// document's source URI group edges and uploads by author, and confirm a
// guessed author by recomputing it.
//
// The digest is therefore an HMAC-SHA256 under a random 256-bit key that
// belongs to the catalog, still truncated to the same 32 hex digits. Without
// the key the digest is a pseudorandom function of the identity: it can
// still be compared for equality (two IDs in one namespace were written by
// one identity, which the namespace exists to express), but it can no longer
// be recomputed from a guessed identity, nor linked across catalogs.
//
// The key never leaves its store. PolicyStore.IdentityNamespaceMAC returns
// a MAC of a message, never the key, and the key is in no error, log line,
// listing or metric.
//
//   - MemoryPolicyStore draws a key on first use; it lives as long as the
//     store, as its registrations do.
//   - DurablePolicyStore, opened read-write (OpenDurablePolicyStore), draws
//     one the first time a namespace is needed, under its write lock, and
//     persists it in policyRowNamespaceKey, beside the format floor (which
//     it writes first), before any MAC is returned. It is not written at
//     open: a host that opens an empty or unmounted catalog directory and
//     then refuses to serve (HasRegistrations) leaves no key there that a
//     backup copied in later would contradict. A failed write makes the
//     store fail closed (diverged) until it is reopened, so a key that may
//     have reached the disk is never replaced by another one. The key is
//     never rotated in place: rotating it would move every identity to a
//     new namespace and strand what it wrote.
//     OpenExistingDurablePolicyStore (the offline listings) never writes
//     one; a store opened that way over a catalog without a key reports
//     IdentityNamespaceMAC unavailable, and so refuses the routes that need
//     it, rather than writing from a listing.
//
// Rollback. The key row is under the source-claim prefix, as the format
// floor is, with a record kind that is not the source-claim kind. Every
// durable binary before this change decodes such a row strictly as a source
// claim (binaries since #695 match only the floor's exact row before that)
// and refuses to open the catalog ("stored policy source claim record is
// corrupt"). The row exists from the first keyed namespace on, so a binary
// that would compute only unkeyed digests never runs over a catalog whose
// identities already have keyed namespaces, and no identity ends up with
// two namespaces that its binaries disagree on. Rolling back past this
// change needs the catalog restored from a backup taken before the key was
// written; rolling forward again draws a new key, so keyed IDs and URIs
// handed out before the restore are no longer the caller's.
//
// Migration. What was written before this change keeps its unkeyed digest:
// nothing is rewritten, since a document's ID is derived from its source URI
// and an edge's ID is what its writer chose. Those digests stay linkable as
// they were. Each identity's unkeyed namespace is accepted as an alias of
// its own, never anyone else's: see applicationEdgeNamespaces and
// uploadNamespaces.
const (
	policyRowNamespaceKey  = policyRowSourceClaim + "\xffidentity-namespace-key"
	policyKindNamespaceKey = byte(16)

	// namespaceKeyBytes is the key's length: 256 bits.
	namespaceKeyBytes = 32
)

type persistedNamespaceKey struct {
	Seq uint64
	Key []byte
}

// newNamespaceKey draws a fresh key. crypto/rand.Read never fails: the
// runtime aborts the process instead.
func newNamespaceKey() *[namespaceKeyBytes]byte {
	var key [namespaceKeyBytes]byte
	_, _ = rand.Read(key[:])
	return &key
}

func namespaceMAC(key *[namespaceKeyBytes]byte, message []byte) []byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(message)
	return mac.Sum(nil)
}

// validateNamespaceKey checks a decoded key record. The error never carries
// the key.
func validateNamespaceKey(record persistedNamespaceKey) error {
	if len(record.Key) != namespaceKeyBytes {
		return fmt.Errorf("identity namespace key has %d bytes, want %d",
			len(record.Key), namespaceKeyBytes)
	}
	var zero [namespaceKeyBytes]byte
	if subtle.ConstantTimeCompare(record.Key, zero[:]) == 1 {
		return fmt.Errorf("identity namespace key is all zero")
	}
	return nil
}

// IdentityNamespaceMAC returns HMAC-SHA256 of message under this store's
// identity namespace key, drawn on first use and kept for the store's life.
func (s *MemoryPolicyStore) IdentityNamespaceMAC(
	ctx context.Context,
	message []byte,
) ([]byte, error) {
	if err := contextFailure(ctx); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, catalogUnavailable()
	}
	s.namespaceKeyOnce.Do(func() { s.namespaceKey = newNamespaceKey() })
	return namespaceMAC(s.namespaceKey, message), nil
}

// IdentityNamespaceMAC returns HMAC-SHA256 of message under the catalog's
// persisted identity namespace key. A store opened read-write draws and
// persists the key first if the catalog has none; one opened by
// OpenExistingDurablePolicyStore reports it unavailable instead.
func (s *DurablePolicyStore) IdentityNamespaceMAC(
	ctx context.Context,
	message []byte,
) ([]byte, error) {
	if err := contextFailure(ctx); err != nil {
		return nil, err
	}
	if s.memoryStore() == nil {
		return nil, catalogUnavailable()
	}
	key := s.namespaceKey.Load()
	if key == nil {
		var err error
		if key, err = s.ensureNamespaceKey(); err != nil {
			return nil, err
		}
	}
	return namespaceMAC(key, message), nil
}

// loadNamespaceKey records one decoded key cell. Every cell of the row is
// decoded, and they must all hold the same key: this binary writes the row
// once, so two different keys are corruption, never a rotation.
func (s *DurablePolicyStore) loadNamespaceKey(value []byte) (uint64, error) {
	var record persistedNamespaceKey
	if err := decodePolicyRecord(value, policyKindNamespaceKey, &record); err != nil {
		return 0, corruptPolicyRecord("identity namespace key", err)
	}
	if err := validateNamespaceKey(record); err != nil {
		return 0, corruptPolicyRecord("identity namespace key", err)
	}
	if loaded := s.namespaceKey.Load(); loaded != nil {
		if subtle.ConstantTimeCompare(loaded[:], record.Key) != 1 {
			return 0, corruptPolicyRecord("identity namespace key",
				fmt.Errorf("the catalog holds two different identity namespace keys"))
		}
		return record.Seq, nil
	}
	var key [namespaceKeyBytes]byte
	copy(key[:], record.Key)
	s.namespaceKey.Store(&key)
	return record.Seq, nil
}

// checkNamespaceKeyAtLoad refuses a key in a catalog with no format floor:
// this binary writes the floor before the key, so it never leaves one
// without the other.
func (s *DurablePolicyStore) checkNamespaceKeyAtLoad(haveFloor bool) error {
	if s.namespaceKey.Load() != nil && !haveFloor {
		return corruptPolicyRecord("identity namespace key",
			fmt.Errorf("the identity namespace key was written without the format floor"))
	}
	return nil
}

// ensureNamespaceKey draws and persists the catalog's key, the first time a
// store opened read-write needs it. It runs under s.mu, writes the format
// floor first (a read-write open already did), and publishes the key only
// once it is durable. A failed write makes the store fail closed: the key
// may have reached the disk anyway, and drawing another one would leave the
// catalog with two.
func (s *DurablePolicyStore) ensureNamespaceKey() (*[namespaceKeyBytes]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.diverged.Load() {
		return nil, catalogUnavailable()
	}
	if key := s.namespaceKey.Load(); key != nil {
		return key, nil
	}
	if s.namespaceKeyDir == "" {
		return nil, shoal.NewError(shoal.ErrorUnavailable,
			"the policy catalog has no identity namespace key yet, and a "+
				"catalog opened for offline listing never writes one")
	}
	if err := s.ensureFormatFloor(); err != nil {
		return nil, err
	}
	key := newNamespaceKey()
	if err := s.writeRowBeforeApply(
		[]byte(policyRowNamespaceKey), policyKindNamespaceKey,
		persistedNamespaceKey{Seq: s.nextSeq(), Key: key[:]},
	); err != nil {
		s.markDiverged()
		return nil, catalogUnavailable()
	}
	s.namespaceKey.Store(key)
	// The key itself is never logged.
	slog.Info("policy catalog identity namespace key written: binaries "+
		"from before #721 now refuse this catalog (\"stored policy source "+
		"claim record is corrupt\"); it is not corrupt, and rollback means "+
		"restoring a backup taken before this point, after which keyed edge "+
		"IDs and upload URIs handed out since are no longer accepted",
		"dir", s.namespaceKeyDir)
	return key, nil
}
