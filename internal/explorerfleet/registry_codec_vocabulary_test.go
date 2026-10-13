// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// TestTheDurableCodecCarriesTheDeclaredVocabulary is #740's durable half.
//
// The registry record is a hand-rolled versioned binary encoding, so a field
// added to fleet.Action reaches storage only when this codec is extended
// too. Left out, every stored descriptor reads as declaring nothing — and an
// action that declares nothing has every reported code adjudicated once the
// vocabulary is closed, so the omission looks exactly like the feature
// working.
func TestTheDurableCodecCarriesTheDeclaredVocabulary(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	schema := json.RawMessage(`{"type":"object"}`)
	descriptor := fleet.Descriptor{
		ID: "agent", Generation: 1, Subject: "owner", Actor: "operator",
		AuthorizationDomain: []byte("domain"),
		Scopes: []fleet.Scope{{
			SourceID: []byte("source"), PolicyID: []byte("policy"),
		}},
		ExecutorRef: "exec", LeaseExpiresAt: now.Add(time.Hour),
		UpdatedAt: now,
		Capabilities: []fleet.Capability{{
			Name: "search", Actions: []fleet.Action{
				{
					Name: "query", InputSchema: schema, OutputSchema: schema,
					ErrorCodes: []string{
						"input_invalid", "upstream_refused"},
				},
				// Declares nothing, beside one that does: the version is
				// per descriptor, so this action's absent list still has
				// to round-trip as absent.
				{Name: "read", InputSchema: schema, OutputSchema: schema},
			},
		}},
	}

	encoded, err := encodeDescriptor(descriptor, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	decoded, digest, err := decodeDescriptor(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if digest != [32]byte{7} {
		t.Fatalf("registration digest = %x", digest)
	}
	actions := decoded.Capabilities[0].Actions
	if len(actions[0].ErrorCodes) != 2 ||
		actions[0].ErrorCodes[0] != "input_invalid" ||
		actions[0].ErrorCodes[1] != "upstream_refused" {
		t.Fatalf("the declared vocabulary did not survive storage: %v",
			actions[0].ErrorCodes)
	}
	if len(actions[1].ErrorCodes) != 0 {
		t.Fatalf("an action declaring nothing gained %v",
			actions[1].ErrorCodes)
	}
}

// TestADescriptorDeclaringNothingKeepsItsBytes is the rollback half, and the
// reason the version is chosen per descriptor rather than globally.
//
// A descriptor that declares no vocabulary must encode byte-for-byte as an
// earlier build wrote it, so rolling back reads it unchanged. Only a
// descriptor that declares one takes version 6, which an earlier build
// refuses outright — and that refusal is the point: reading the action and
// dropping a declaration it does not understand would leave it believing
// the action declares nothing.
func TestADescriptorDeclaringNothingKeepsItsBytes(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	schema := json.RawMessage(`{"type":"object"}`)
	plain := fleet.Descriptor{
		ID: "agent", Generation: 1, Subject: "owner", Actor: "operator",
		AuthorizationDomain: []byte("domain"),
		ExecutorRef:         "exec", LeaseExpiresAt: now.Add(time.Hour),
		UpdatedAt: now,
		Capabilities: []fleet.Capability{{
			Name: "search", Actions: []fleet.Action{
				{Name: "query", InputSchema: schema, OutputSchema: schema},
			},
		}},
	}
	encoded, err := encodeDescriptor(plain, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	if version := uint16(encoded[0])<<8 | uint16(encoded[1]); version !=
		codecVersion {
		t.Fatalf("a descriptor declaring nothing was written as version "+
			"%d, want %d; an earlier build refuses anything newer and "+
			"would stop reading records it used to read", version,
			codecVersion)
	}

	declaring := plain
	declaring.Capabilities = []fleet.Capability{{
		Name: "search", Actions: []fleet.Action{{
			Name: "query", InputSchema: schema, OutputSchema: schema,
			ErrorCodes: []string{"upstream_refused"},
		}},
	}}
	encoded, err = encodeDescriptor(declaring, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	if version := uint16(encoded[0])<<8 | uint16(encoded[1]); version !=
		codecVersionErrorCodes {
		t.Fatalf("a descriptor declaring a vocabulary was written as "+
			"version %d, want %d", version, codecVersionErrorCodes)
	}
}
