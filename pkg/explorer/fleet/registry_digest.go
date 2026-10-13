// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package fleet

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

// The registry mutation digest identifies one admitted registry mutation
// (register, heartbeat, revoke) in its lifecycle receipt. It is folded into
// the receipt's QueryDigest, so a retry of the same request is recognized as
// the same mutation and a retry that changes anything is a conflict.
//
// There are three encodings.
//
//   - v3 (registryMutationDigest) is the only one written. It is v2's layout
//     plus each action's declared error-code vocabulary (#740). The
//     vocabulary is part of what the descriptor authorizes — it decides
//     which codes a completion may record as the executor's word — so a
//     retry that changes it has to be a different mutation rather than a
//     deduplicated repeat.
//
//   - v2 (registryMutationDigestV2) is read, never written. Receipts under
//     fleet.lifecycle.v4 carry it.
//
//     It is an unambiguous serialization: every value is a length-prefixed
//     field, every list (scopes, capabilities, each capability's actions,
//     each action's effects) is preceded by its element count, and every
//     per-action flag is an explicit presence field that is always written.
//     Given the counts, the stream has exactly one parse, so two different
//     mutations cannot produce the same bytes. v3 keeps that property, which
//     is why the vocabulary is a counted list written for every action and
//     not an optional block appended when non-empty: an optional block makes
//     the number of fields per action depend on their contents, which is the
//     property v2 was created to establish.
//
//     Appending to v2 in place was the alternative, and it is not available.
//     Every receipt already written under fleet.lifecycle.v4 carries a v2
//     digest, and that digest is the retry identity — so a retry of a
//     request whose receipt predates the change would hash differently and
//     be answered as a conflict instead of the idempotent replay the digest
//     exists to provide. The upgrade itself would be the outage.
//
//   - v1 (registryMutationDigestV1) is read, never written. It length-prefixed
//     fields but not lists, and appended optional per-action fields only when
//     set. Fields could therefore be realigned across a list boundary, and
//     TestRegistryDigestV1CollidesAcrossAListBoundary pins two canonical
//     descriptors whose v1 digests are identical (#521). It is kept only so a
//     retry whose receipt an older build wrote still reconciles.
//
// A 32-byte digest cannot say which encoding produced it, so the version is
// carried by the receipt that stores it, not by the digest: receipt
// identities fleet.lifecycle.v1 through v3 hold v1 digests,
// fleet.lifecycle.v4 holds v2, and fleet.lifecycle.v5 holds v3. See
// internal/explorerfleet's lifecycle recorder. The two encodings are also domain-separated by their leading tag
// field, so a v1 and a v2 digest are never equal short of a SHA-256
// collision.
const (
	registryDigestTagV1 = "shoal.fleet.registry-mutation.v1"
	registryDigestTagV2 = "shoal.fleet.registry-mutation.v2"
	registryDigestTagV3 = "shoal.fleet.registry-mutation.v3"
)

// registryDigests is what a lifecycle receipt is handed for one mutation:
// the digest it is written under, and the digests a receipt written before
// each upgrade would carry. One per superseded encoding, because the
// recorder has to compare a retry against a prior receipt under that
// receipt's own version — which its identity names, and its stored digest
// cannot.
type registryDigests struct {
	current [sha256.Size]byte
	v2      [sha256.Size]byte
	v1      [sha256.Size]byte
}

func registryMutationDigests(mutation Mutation) registryDigests {
	return registryDigests{
		current: registryMutationDigest(mutation),
		v2:      registryMutationDigestV2(mutation),
		v1:      registryMutationDigestV1(mutation),
	}
}

// registryMutationDigest is the v3 encoding, the one every new receipt is
// written with.
//
// Its layout is fixed, and TestRegistryDigestV3Layout pins it field by
// field:
//
//	tag, registration key, expected generation,
//	id, generation, subject, actor, parent, authorization domain,
//	count(scopes), { source, policy }*,
//	executor ref,
//	count(capabilities), { name, count(actions), {
//	    name, count(effects), { effect }*, input schema, output schema,
//	    requires approval, requires attestation,
//	    count(error codes), { error code }* }* }*,
//	lease expiry, revocation time
//
// Every element above is one field: an eight-byte big-endian length followed
// by that many bytes. A count is an eight-byte unsigned integer field, and a
// flag is a one-byte field, 0 or 1.
//
// v1 appended an optional field only when set, so that adding the field left
// every existing digest unchanged. Under a new version there are no existing
// digests to preserve, so v2 writes every flag explicitly instead: an action
// is always the same number of fields, an absent flag is a 0 rather than a
// missing field, and an effect set is a counted list rather than a joined
// string. Nothing has to be inferred from what follows a field.
func registryMutationDigest(mutation Mutation) [sha256.Size]byte {
	digest := sha256.New()
	writeRegistryMutationV3(digest, mutation)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

// registryMutationDigestV2 reproduces, byte for byte, the encoding every
// build between #521 and #740 wrote. It is read, never written: the
// lifecycle recorder uses it only to reconcile a retry with a receipt such a
// build wrote under fleet.lifecycle.v4. Do not change it —
// TestRegistryDigestV2Layout pins its bytes, and that test is the guard, not
// this comment.
func registryMutationDigestV2(mutation Mutation) [sha256.Size]byte {
	digest := sha256.New()
	writeRegistryMutationV2(digest, mutation)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

// writeRegistryMutationV3 writes the v3 encoding. It is separate from the
// hash only so the layout test can read the bytes it writes.
//
// It duplicates writeRegistryMutationV2's body rather than sharing one
// parameterised writer, deliberately. A shared body would let an edit meant
// for v3 silently change the frozen v2 encoding, and v2 is the identity
// every existing fleet.lifecycle.v4 receipt was written under. The cost of
// the duplication is one function; the cost of the alternative is a class of
// change that cannot be reviewed locally.
func writeRegistryMutationV3(digest hash.Hash, mutation Mutation) {
	writeRegistryDigestField(digest, []byte(registryDigestTagV3))
	writeRegistryDigestField(digest, []byte(mutation.RegistrationKey))
	writeRegistryDigestInt64(digest, mutation.ExpectedGeneration)
	descriptor := mutation.Descriptor
	writeRegistryDigestField(digest, []byte(descriptor.ID))
	writeRegistryDigestInt64(digest, descriptor.Generation)
	writeRegistryDigestField(digest, []byte(descriptor.Subject))
	writeRegistryDigestField(digest, []byte(descriptor.Actor))
	writeRegistryDigestField(digest, []byte(descriptor.ParentID))
	writeRegistryDigestField(digest, descriptor.AuthorizationDomain)
	writeRegistryDigestCount(digest, len(descriptor.Scopes))
	for _, scope := range descriptor.Scopes {
		writeRegistryDigestField(digest, scope.SourceID)
		writeRegistryDigestField(digest, scope.PolicyID)
	}
	writeRegistryDigestField(digest, []byte(descriptor.ExecutorRef))
	writeRegistryDigestCount(digest, len(descriptor.Capabilities))
	for _, capability := range descriptor.Capabilities {
		writeRegistryDigestField(digest, []byte(capability.Name))
		writeRegistryDigestCount(digest, len(capability.Actions))
		for _, action := range capability.Actions {
			writeRegistryDigestField(digest, []byte(action.Name))
			// Sorted and deduplicated, as v1 did, so declaration order is
			// not part of the declaration.
			effects := action.Effects.digestOrder()
			writeRegistryDigestCount(digest, len(effects))
			for _, effect := range effects {
				writeRegistryDigestField(digest, []byte(effect))
			}
			writeRegistryDigestField(digest, action.InputSchema)
			writeRegistryDigestField(digest, action.OutputSchema)
			writeRegistryDigestBool(digest, action.RequiresApproval)
			writeRegistryDigestBool(digest, action.RequiresAttestation)
			// Written for every action, empty list included, so an action
			// is always the same number of fields. canonicalErrorCodes
			// sorts and deduplicates, so declaration order is not part of
			// the declaration and a reordering does not re-register.
			writeRegistryDigestCount(digest, len(action.ErrorCodes))
			for _, code := range action.ErrorCodes {
				writeRegistryDigestField(digest, []byte(code))
			}
		}
	}
	writeRegistryDigestInt64(digest, descriptor.LeaseExpiresAt.UnixNano())
	writeRegistryDigestInt64(digest, descriptor.RevokedAt.UnixNano())
}

// writeRegistryMutationV2 writes the v2 encoding, frozen. See
// registryMutationDigestV2.
func writeRegistryMutationV2(digest hash.Hash, mutation Mutation) {
	writeRegistryDigestField(digest, []byte(registryDigestTagV2))
	writeRegistryDigestField(digest, []byte(mutation.RegistrationKey))
	writeRegistryDigestInt64(digest, mutation.ExpectedGeneration)
	descriptor := mutation.Descriptor
	writeRegistryDigestField(digest, []byte(descriptor.ID))
	writeRegistryDigestInt64(digest, descriptor.Generation)
	writeRegistryDigestField(digest, []byte(descriptor.Subject))
	writeRegistryDigestField(digest, []byte(descriptor.Actor))
	writeRegistryDigestField(digest, []byte(descriptor.ParentID))
	writeRegistryDigestField(digest, descriptor.AuthorizationDomain)
	writeRegistryDigestCount(digest, len(descriptor.Scopes))
	for _, scope := range descriptor.Scopes {
		writeRegistryDigestField(digest, scope.SourceID)
		writeRegistryDigestField(digest, scope.PolicyID)
	}
	writeRegistryDigestField(digest, []byte(descriptor.ExecutorRef))
	writeRegistryDigestCount(digest, len(descriptor.Capabilities))
	for _, capability := range descriptor.Capabilities {
		writeRegistryDigestField(digest, []byte(capability.Name))
		writeRegistryDigestCount(digest, len(capability.Actions))
		for _, action := range capability.Actions {
			writeRegistryDigestField(digest, []byte(action.Name))
			effects := action.Effects.digestOrder()
			writeRegistryDigestCount(digest, len(effects))
			for _, effect := range effects {
				writeRegistryDigestField(digest, []byte(effect))
			}
			writeRegistryDigestField(digest, action.InputSchema)
			writeRegistryDigestField(digest, action.OutputSchema)
			writeRegistryDigestBool(digest, action.RequiresApproval)
			writeRegistryDigestBool(digest, action.RequiresAttestation)
		}
	}
	writeRegistryDigestInt64(digest, descriptor.LeaseExpiresAt.UnixNano())
	writeRegistryDigestInt64(digest, descriptor.RevokedAt.UnixNano())
}

// registryMutationDigestV1 reproduces, byte for byte, the encoding every
// build before #521 wrote. It is read, never written: the lifecycle recorder
// uses it only to reconcile a retry with a receipt an older build wrote. Do
// not change it. Its golden values were computed by those builds and are
// checked in as literals (registry_digest_test.go, approval_test.go,
// effect_test.go), not recomputed here.
func registryMutationDigestV1(mutation Mutation) [sha256.Size]byte {
	digest := sha256.New()
	writeRegistryDigestField(digest, []byte(registryDigestTagV1))
	writeRegistryDigestField(digest, []byte(mutation.RegistrationKey))
	writeRegistryDigestInt64(digest, mutation.ExpectedGeneration)
	descriptor := mutation.Descriptor
	writeRegistryDigestField(digest, []byte(descriptor.ID))
	writeRegistryDigestInt64(digest, descriptor.Generation)
	writeRegistryDigestField(digest, []byte(descriptor.Subject))
	writeRegistryDigestField(digest, []byte(descriptor.Actor))
	writeRegistryDigestField(digest, []byte(descriptor.ParentID))
	writeRegistryDigestField(digest, descriptor.AuthorizationDomain)
	for _, scope := range descriptor.Scopes {
		writeRegistryDigestField(digest, scope.SourceID)
		writeRegistryDigestField(digest, scope.PolicyID)
	}
	writeRegistryDigestField(digest, []byte(descriptor.ExecutorRef))
	for _, capability := range descriptor.Capabilities {
		writeRegistryDigestField(digest, []byte(capability.Name))
		for _, action := range capability.Actions {
			writeRegistryDigestField(digest, []byte(action.Name))
			// Appended only for a non-empty declaration, so an action that
			// declared nothing hashed exactly as it did before the field
			// existed. See Effects.digestBytes.
			if bytes := action.Effects.digestBytes(); bytes != nil {
				writeRegistryDigestField(digest, bytes)
			}
			writeRegistryDigestField(digest, action.InputSchema)
			writeRegistryDigestField(digest, action.OutputSchema)
			// Appended only when set, for the same reason, and tagged.
			if action.RequiresApproval {
				writeRegistryDigestField(
					digest, []byte("shoal.fleet.requires-approval.v1"))
			}
			if action.RequiresAttestation {
				writeRegistryDigestField(
					digest, []byte("shoal.fleet.requires-attestation.v1"))
			}
		}
	}
	writeRegistryDigestInt64(digest, descriptor.LeaseExpiresAt.UnixNano())
	writeRegistryDigestInt64(digest, descriptor.RevokedAt.UnixNano())
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func writeRegistryDigestField(digest hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = digest.Write(size[:])
	_, _ = digest.Write(value)
}

func writeRegistryDigestInt64(digest hash.Hash, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	writeRegistryDigestField(digest, encoded[:])
}

// writeRegistryDigestCount writes a list's element count, as its own field,
// ahead of the list's elements.
func writeRegistryDigestCount(digest hash.Hash, count int) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(count))
	writeRegistryDigestField(digest, encoded[:])
}

// writeRegistryDigestBool writes a flag as an explicit one-byte field, present
// whether the flag is set or not.
func writeRegistryDigestBool(digest hash.Hash, value bool) {
	encoded := []byte{0}
	if value {
		encoded[0] = 1
	}
	writeRegistryDigestField(digest, encoded)
}
