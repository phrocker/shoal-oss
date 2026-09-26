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

package explorerfleet

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

func effectDescriptor(effect fleet.Effect) fleet.Descriptor {
	schema := json.RawMessage(`{"type":"object"}`)
	return fleet.Descriptor{
		ID: "agent", Generation: 1, Subject: "owner", Actor: "operator",
		AuthorizationDomain: []byte("domain"),
		Scopes: []fleet.Scope{{
			SourceID: []byte("source"), PolicyID: []byte("policy"),
		}},
		ExecutorRef: "executor",
		Capabilities: []fleet.Capability{{Name: "deploy", Actions: []fleet.Action{{
			Name: "ship", Effect: effect,
			InputSchema: schema, OutputSchema: schema,
		}}}},
		LeaseExpiresAt: time.Unix(1, 0).UTC(),
		UpdatedAt:      time.Unix(1, 0).UTC(),
	}
}

// TestEffectSurvivesTheDurableCodec is the regression test for the bug that
// made the effect boundary bypassable by persisting it. The field was validated
// at registration and then dropped on encode, so a stored external action read
// back as evidence-only and the resolution check saw the zero value.
func TestEffectSurvivesTheDurableCodec(t *testing.T) {
	for _, effect := range []fleet.Effect{
		fleet.EffectEvidence, fleet.EffectExternal,
	} {
		descriptor := effectDescriptor(effect)
		encoded, err := encodeDescriptor(descriptor, [sha256.Size]byte{})
		if err != nil {
			t.Fatalf("encode %q: %v", effect, err)
		}
		decoded, _, err := decodeDescriptor(encoded)
		if err != nil {
			t.Fatalf("decode %q: %v", effect, err)
		}
		got := decoded.Capabilities[0].Actions[0].Effect
		if got != effect {
			t.Fatalf("effect %q did not survive the codec, got %q", effect, got)
		}
	}
}

// TestVersionOneRecordsDecodeAsEvidence proves the compatibility claim rather
// than asserting it. A record written before the effect class existed has no
// field for it, and must decode with the zero value it was written with rather
// than being rejected.
func TestVersionOneRecordsDecodeAsEvidence(t *testing.T) {
	encoded, err := encodeDescriptor(
		effectDescriptor(fleet.EffectEvidence), [sha256.Size]byte{})
	if err != nil {
		t.Fatal(err)
	}
	// A version-2 record of an evidence-only action differs from the version-1
	// layout by exactly the four zero bytes writeString emits for the empty
	// effect, plus the version word. Removing those reproduces the older
	// encoding without duplicating the whole encoder here.
	legacy := legacyVersionOne(t, encoded)
	decoded, _, err := decodeDescriptor(legacy)
	if err != nil {
		t.Fatalf("a version-1 record must still decode: %v", err)
	}
	if got := decoded.Capabilities[0].Actions[0].Effect; got != fleet.EffectEvidence {
		t.Fatalf("version-1 effect = %q, want the zero value", got)
	}
}

// legacyVersionOne turns a version-2 encoding of an evidence-only descriptor
// into the version-1 layout: version word set to 1, and the four zero bytes
// that writeString wrote for the empty effect removed.
func legacyVersionOne(t *testing.T, encoded []byte) []byte {
	t.Helper()
	// The effect immediately follows the action name, which is "ship".
	marker := append([]byte{0, 0, 0, 4}, []byte("ship")...)
	index := bytes.Index(encoded, marker)
	if index < 0 {
		t.Fatal("could not locate the action name in the encoding")
	}
	effectAt := index + len(marker)
	if !bytes.Equal(encoded[effectAt:effectAt+4], []byte{0, 0, 0, 0}) {
		t.Fatalf("expected an empty effect string at %d", effectAt)
	}
	legacy := make([]byte, 0, len(encoded)-4)
	legacy = append(legacy, encoded[:effectAt]...)
	legacy = append(legacy, encoded[effectAt+4:]...)
	binary.BigEndian.PutUint16(legacy[0:2], 1)
	return legacy
}
