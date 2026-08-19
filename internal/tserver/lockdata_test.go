// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tserver

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// canonicalTestUUID is a UUID in the only form Java's UUID.fromString accepts.
const canonicalTestUUID = "6f1b2c8e-6a4d-4c2e-9f3b-1d5e7a9c0b21"

// nonCanonicalUUIDs are the forms Go's uuid.Parse accepts and Java's
// UUID.fromString does not, plus the near-misses either would reject. Every
// one of them is a UUID this process could otherwise publish and the manager
// could not read.
func nonCanonicalUUIDs() map[string]string {
	return map[string]string{
		"bare hex":    strings.ReplaceAll(canonicalTestUUID, "-", ""),
		"urn":         "urn:uuid:" + canonicalTestUUID,
		"braced":      "{" + canonicalTestUUID + "}",
		"truncated":   canonicalTestUUID[:len(canonicalTestUUID)-1],
		"padded":      canonicalTestUUID + "0",
		"empty":       "",
		"not a uuid":  "shoal-tablet-server-one-two-three-four",
		"right shape": "6f1b2c8e-6a4d-4c2e-9f3b-1d5e7a9c0bZZ",
	}
}

// invalidResourceGroups are names Accumulo's ResourceGroupId.of throws on.
// Publishing one is not a cosmetic problem: ServiceLockData.parse builds a
// ResourceGroupId from every descriptor's group, so the throw takes the whole
// znode with it.
func invalidResourceGroups() []string {
	return []string{
		"",            // no name at all
		"../managers", // a traversal that would clean out of the tservers tree
		"1ingest",     // must open with a letter
		"_ingest",     // must open with a letter
		"ingest_",     // a separator must be followed by something
		"ing__est",    // separators do not double
		"ing-est",     // only underscore separates
		"ing est",     // no spaces
		"ing.est",     // no dots
		"ing/est",     // no path separators
		"ingest\n",    // no trailing newline, which Go's $ would otherwise let by
		"INGEST-EAST", // still only underscore separates
	}
}

// validResourceGroups are names Accumulo accepts, kept alongside the refusals
// so the grammar is not tightened past what a real deployment configures.
func validResourceGroups() []string {
	return []string{
		DefaultResourceGroup,
		"ingest",
		"i",
		"Ingest",
		"ingest_east",
		"ingest2",
		"i_1",
		"INGEST",
	}
}

// TestResourceGroupGrammarMatchesAccumulo pins the grammar itself against
// ResourceGroupId.GROUP_NAME_PATTERN, so a later relaxation has to be
// deliberate.
func TestResourceGroupGrammarMatchesAccumulo(t *testing.T) {
	for _, group := range validResourceGroups() {
		if !validResourceGroup(group) {
			t.Errorf("validResourceGroup(%q) = false, want true", group)
		}
	}
	for _, group := range invalidResourceGroups() {
		if validResourceGroup(group) {
			t.Errorf("validResourceGroup(%q) = true, want false", group)
		}
	}
}

// TestServiceDescriptorRefusesAGroupAccumuloWouldReject covers a resource
// group arriving from configuration.
//
// The read side runs every group through ResourceGroupId.of, which throws on
// anything outside its grammar, so a group this process accepts and Accumulo
// does not is another way to publish a znode the manager cannot parse — the
// same failure a non-canonical UUID causes, reached through a different field.
func TestServiceDescriptorRefusesAGroupAccumuloWouldReject(t *testing.T) {
	for _, group := range invalidResourceGroups() {
		t.Run(group, func(t *testing.T) {
			data := ServiceLockData{Descriptors: []ServiceDescriptor{{
				UUID:    serverUUID,
				Service: ServiceClient,
				Address: testAddress,
				Group:   group,
			}}}
			if err := data.Validate(); !errors.Is(err, ErrInvalidLockData) {
				t.Fatalf("Validate: want ErrInvalidLockData, got %v", err)
			}
			encoded, err := data.Encode()
			if !errors.Is(err, ErrInvalidLockData) {
				t.Fatalf("Encode: want ErrInvalidLockData, got %v", err)
			}
			if encoded != nil {
				t.Fatalf("Encode returned %q alongside its refusal", encoded)
			}
		})
	}
}

// TestTabletServerLockDataRefusesAGroupAccumuloWouldReject covers the same
// group arriving through the constructor a tablet server actually calls.
func TestTabletServerLockDataRefusesAGroupAccumuloWouldReject(t *testing.T) {
	for _, group := range invalidResourceGroups() {
		if group == "" {
			// An empty group means DefaultResourceGroup here, which is valid.
			continue
		}
		t.Run(group, func(t *testing.T) {
			if _, err := TabletServerLockData(serverUUID, testAddress, group,
				ServiceClient); !errors.Is(err, ErrInvalidLockData) {
				t.Fatalf("want ErrInvalidLockData, got %v", err)
			}
		})
	}
	for _, group := range validResourceGroups() {
		t.Run("accepts "+group, func(t *testing.T) {
			if _, err := TabletServerLockData(serverUUID, testAddress, group,
				ServiceClient); err != nil {
				t.Fatalf("TabletServerLockData(%q): %v", group, err)
			}
		})
	}
}

// TestTabletServerLockDataRefusesAnotherRolesService keeps this constructor
// honest about what it is for.
//
// The enum covers the manager, the coordinator, the garbage collector and the
// compactors as well, and every one of them is a service Known() accepts. A
// tablet-server lock advertising MANAGER would tell a client that the manager
// endpoint lives on this process, which does not implement it — a lie the
// manager itself has no reason to look for, since a tablet server publishing
// a manager service is not a case Accumulo produces.
func TestTabletServerLockDataRefusesAnotherRolesService(t *testing.T) {
	foreign := []ThriftService{"MANAGER", "COORDINATOR", "COMPACTOR", "GC",
		"FATE_CLIENT", "FATE_WORKER", "NONE"}
	for _, service := range foreign {
		t.Run(string(service), func(t *testing.T) {
			if !service.Known() {
				t.Fatalf("%s is meant to be a service Accumulo defines", service)
			}
			_, err := TabletServerLockData(serverUUID, testAddress, testGroup, service)
			if !errors.Is(err, ErrInvalidLockData) {
				t.Fatalf("want ErrInvalidLockData, got %v", err)
			}
			if got := err.Error(); !strings.Contains(got, string(service)) {
				t.Fatalf("error %q does not name the refused service", got)
			}
		})
	}
	t.Run("alongside real ones", func(t *testing.T) {
		services := append(TabletServerServices(), "MANAGER")
		if _, err := TabletServerLockData(serverUUID, testAddress, testGroup,
			services...); !errors.Is(err, ErrInvalidLockData) {
			t.Fatalf("want ErrInvalidLockData, got %v", err)
		}
	})
	t.Run("unknown service", func(t *testing.T) {
		if _, err := TabletServerLockData(serverUUID, testAddress, testGroup,
			"SHOAL_SCAN"); !errors.Is(err, ErrInvalidLockData) {
			t.Fatalf("want ErrInvalidLockData, got %v", err)
		}
	})
}

// TestServiceDescriptorRefusesNonCanonicalUUID pins the UUID shape at the
// write side.
//
// Go's uuid.Parse is not a shape check: it also accepts a bare 32-character
// hex form, a "urn:uuid:" form, and a braced form. Java's UUID.fromString
// accepts none of them, and ServiceLockData is deserialized with it, so
// publishing one would not make the descriptor merely odd — it would make the
// whole lock znode unparseable and the server it names invisible to the
// manager. Refusing to encode is the only outcome that keeps the promise that
// validated payloads are manager-readable.
func TestServiceDescriptorRefusesNonCanonicalUUID(t *testing.T) {
	for name, value := range nonCanonicalUUIDs() {
		t.Run(name, func(t *testing.T) {
			descriptor := ServiceDescriptor{
				UUID:    value,
				Service: ServiceClient,
				Address: testAddress,
				Group:   testGroup,
			}
			err := descriptor.Validate()
			if !errors.Is(err, ErrInvalidLockData) {
				t.Fatalf("Validate(%q) = %v, want ErrInvalidLockData", value, err)
			}
			if !strings.Contains(err.Error(), "36-character dashed form") {
				t.Fatalf("Validate(%q) = %q, want it to name the shape it wants", value, err)
			}
		})
	}
	descriptor := ServiceDescriptor{
		UUID:    canonicalTestUUID,
		Service: ServiceClient,
		Address: testAddress,
		Group:   testGroup,
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("the canonical form must still validate: %v", err)
	}
}

// TestTabletServerLockDataRefusesNonCanonicalUUID checks the constructor, not
// just the descriptor it builds: a caller that never touches ServiceDescriptor
// still must not be able to publish a UUID the manager cannot read.
func TestTabletServerLockDataRefusesNonCanonicalUUID(t *testing.T) {
	for name, value := range nonCanonicalUUIDs() {
		t.Run(name, func(t *testing.T) {
			_, err := TabletServerLockData(value, testAddress, testGroup, ServiceClient)
			if !errors.Is(err, ErrInvalidLockData) {
				t.Fatalf("TabletServerLockData(%q) = %v, want ErrInvalidLockData", value, err)
			}
		})
	}
}

// TestLockIDValidRequiresTheCanonicalUUID pins the same shape on the fencing
// identity. LockID.Valid is what decides whether an identity could name a real
// lock node; a UUID Accumulo cannot parse could never appear in one, so
// treating it as fencing authority would be fencing against nothing.
func TestLockIDValidRequiresTheCanonicalUUID(t *testing.T) {
	for name, value := range nonCanonicalUUIDs() {
		t.Run(name, func(t *testing.T) {
			if (LockID{UUID: value, Sequence: 1}).Valid() {
				t.Fatalf("LockID{UUID: %q} must not be usable for fencing", value)
			}
		})
	}
	if !(LockID{UUID: canonicalTestUUID, Sequence: 1}).Valid() {
		t.Fatal("the canonical form must still be usable for fencing")
	}
}

// TestTabletServerLockDataMatchesTheAccumuloWireForm pins the exact bytes a
// Shoal tablet server writes to its lock znode.
//
// This is the compatibility contract with an unmodified manager: it reads the
// znode with Gson into ServiceLockData.ServiceDescriptorsGson, whose fields
// are uuid, service, address and group in that order, and keys the result by
// service. Field names, the descriptors wrapper, the enum spellings, and the
// group being a bare string are all load-bearing — get any of them wrong and
// the manager sees a server it cannot parse.
func TestTabletServerLockDataMatchesTheAccumuloWireForm(t *testing.T) {
	data, err := TabletServerLockData(serverUUID, testAddress, testGroup, TabletServerServices()...)
	if err != nil {
		t.Fatalf("TabletServerLockData: %v", err)
	}
	encoded, err := data.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	want := `{"descriptors":[` +
		`{"uuid":"` + serverUUID + `","service":"CLIENT","address":"` + testAddress + `","group":"default"},` +
		`{"uuid":"` + serverUUID + `","service":"TABLET_INGEST","address":"` + testAddress + `","group":"default"},` +
		`{"uuid":"` + serverUUID + `","service":"TABLET_MANAGEMENT","address":"` + testAddress + `","group":"default"},` +
		`{"uuid":"` + serverUUID + `","service":"TABLET_SCAN","address":"` + testAddress + `","group":"default"},` +
		`{"uuid":"` + serverUUID + `","service":"TSERV","address":"` + testAddress + `","group":"default"}` +
		`]}`
	if string(encoded) != want {
		t.Fatalf("lock data mismatch\n got: %s\nwant: %s", encoded, want)
	}
}

// TestEncodedLockDataIsReadableByTheLiveServerReader checks the payload
// against the shape internal/zk decodes when it enumerates live servers: a
// descriptors array whose entries carry a service name and an address. That
// reader is how a Shoal process becomes visible to Shoal's own clients, so it
// has to be able to read what this package writes.
func TestEncodedLockDataIsReadableByTheLiveServerReader(t *testing.T) {
	data, err := TabletServerLockData(serverUUID, testAddress, "ingest", ServiceClient, ServiceTabletScan)
	if err != nil {
		t.Fatalf("TabletServerLockData: %v", err)
	}
	encoded, err := data.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// The exact struct internal/zk decodes into: only service and address.
	var reader struct {
		Descriptors []struct {
			Service string `json:"service"`
			Address string `json:"address"`
		} `json:"descriptors"`
	}
	if err := json.Unmarshal(encoded, &reader); err != nil {
		t.Fatalf("decode as the live-server reader does: %v", err)
	}
	found := ""
	for _, descriptor := range reader.Descriptors {
		if descriptor.Service == "CLIENT" {
			found = descriptor.Address
		}
	}
	if found != testAddress {
		t.Fatalf("CLIENT address = %q, want %q", found, testAddress)
	}
}

func TestServiceLockDataRoundTrips(t *testing.T) {
	data, err := TabletServerLockData(serverUUID, testAddress, "", TabletServerServices()...)
	if err != nil {
		t.Fatalf("TabletServerLockData: %v", err)
	}
	encoded, err := data.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := DecodeServiceLockData(encoded)
	if err != nil {
		t.Fatalf("DecodeServiceLockData: %v", err)
	}
	if !reflect.DeepEqual(decoded.Descriptors, data.Descriptors) {
		t.Fatalf("round trip changed the descriptors:\n got %+v\nwant %+v",
			decoded.Descriptors, data.Descriptors)
	}
	for _, service := range TabletServerServices() {
		address, ok := decoded.Address(service)
		if !ok || address != testAddress {
			t.Fatalf("Address(%s) = %q, %v; want %q, true", service, address, ok, testAddress)
		}
	}
	if _, ok := decoded.Address("MANAGER"); ok {
		t.Fatal("a tablet server must not advertise the manager service")
	}
}

// TestEncodeRefusesUnusableAdvertisements checks that nothing which would
// mislead the manager can be written. A lock znode is a claim to be a live
// server; a claim the manager cannot act on is worse than staying invisible,
// because it draws work to a process that cannot do it.
func TestEncodeRefusesUnusableAdvertisements(t *testing.T) {
	good := ServiceDescriptor{
		UUID:    serverUUID,
		Service: ServiceClient,
		Address: testAddress,
		Group:   testGroup,
	}
	with := func(mutate func(*ServiceDescriptor)) ServiceLockData {
		descriptor := good
		mutate(&descriptor)
		return ServiceLockData{Descriptors: []ServiceDescriptor{descriptor}}
	}
	tests := []struct {
		name string
		data ServiceLockData
	}{
		{"no descriptors", ServiceLockData{}},
		{"not a uuid", with(func(d *ServiceDescriptor) { d.UUID = "shoal-1" })},
		{"no uuid", with(func(d *ServiceDescriptor) { d.UUID = "" })},
		{"unknown service", with(func(d *ServiceDescriptor) { d.Service = "SHOAL_SCAN" })},
		{"no service", with(func(d *ServiceDescriptor) { d.Service = "" })},
		{"no group", with(func(d *ServiceDescriptor) { d.Group = "" })},
		{"no address", with(func(d *ServiceDescriptor) { d.Address = "" })},
		{"unbound placeholder", with(func(d *ServiceDescriptor) { d.Address = placeholderAddress })},
		{"no port", with(func(d *ServiceDescriptor) { d.Address = "shoal-1.example" })},
		{"no host", with(func(d *ServiceDescriptor) { d.Address = ":9997" })},
		{"non-numeric port", with(func(d *ServiceDescriptor) { d.Address = "shoal-1.example:thrift" })},
		{"port zero", with(func(d *ServiceDescriptor) { d.Address = "shoal-1.example:0" })},
		{"port past the range", with(func(d *ServiceDescriptor) { d.Address = "shoal-1.example:70000" })},
		{"duplicate service", ServiceLockData{Descriptors: []ServiceDescriptor{good, good}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.data.Validate(); !errors.Is(err, ErrInvalidLockData) {
				t.Fatalf("Validate: want ErrInvalidLockData, got %v", err)
			}
			encoded, err := tt.data.Encode()
			if !errors.Is(err, ErrInvalidLockData) {
				t.Fatalf("Encode: want ErrInvalidLockData, got %v", err)
			}
			if encoded != nil {
				t.Fatalf("Encode returned %q alongside its refusal", encoded)
			}
		})
	}
}

// TestDuplicateServiceIsRefusedRatherThanCollapsed covers the one refusal that
// is about Accumulo's reader rather than about reachability: descriptors land
// in an EnumMap keyed by service, so a second address for a service is dropped
// and which one survives depends on iteration order. Advertising an
// arbitrarily chosen address is not something to do quietly.
func TestDuplicateServiceIsRefusedRatherThanCollapsed(t *testing.T) {
	data := ServiceLockData{Descriptors: []ServiceDescriptor{
		{UUID: serverUUID, Service: ServiceClient, Address: testAddress, Group: testGroup},
		{UUID: serverUUID, Service: ServiceClient, Address: "shoal-2.example:9997", Group: testGroup},
	}}
	err := data.Validate()
	if !errors.Is(err, ErrInvalidLockData) {
		t.Fatalf("Validate: want ErrInvalidLockData, got %v", err)
	}
	if got := err.Error(); !strings.Contains(got, "CLIENT") {
		t.Fatalf("error %q does not name the duplicated service", got)
	}
}

func TestTabletServerLockDataRefusesAnEmptyServiceSet(t *testing.T) {
	if _, err := TabletServerLockData(serverUUID, testAddress, testGroup); !errors.Is(err, ErrInvalidLockData) {
		t.Fatalf("want ErrInvalidLockData, got %v", err)
	}
}

// TestTabletServerLockDataDefaultsTheResourceGroup mirrors Accumulo, where a
// server with no configured group belongs to "default".
func TestTabletServerLockDataDefaultsTheResourceGroup(t *testing.T) {
	data, err := TabletServerLockData(serverUUID, testAddress, "", ServiceClient)
	if err != nil {
		t.Fatalf("TabletServerLockData: %v", err)
	}
	if got := data.Descriptors[0].Group; got != DefaultResourceGroup {
		t.Fatalf("group = %q, want %q", got, DefaultResourceGroup)
	}
}

// TestDecodeIsLenientAboutServicesItDoesNotKnow keeps reading and writing
// asymmetric on purpose. This process must not publish a service it cannot
// serve, but it reads znodes written by servers it does not control —
// including Java ones and newer Accumulo versions — and a descriptor it does
// not recognize is no reason to discard the ones it does.
func TestDecodeIsLenientAboutServicesItDoesNotKnow(t *testing.T) {
	raw := []byte(`{"descriptors":[` +
		`{"uuid":"` + serverUUID + `","service":"SOME_FUTURE_SERVICE","address":"shoal-9.example:1","group":"default"},` +
		`{"uuid":"` + serverUUID + `","service":"CLIENT","address":"` + testAddress + `","group":"default"}]}`)
	decoded, err := DecodeServiceLockData(raw)
	if err != nil {
		t.Fatalf("DecodeServiceLockData: %v", err)
	}
	if len(decoded.Descriptors) != 2 {
		t.Fatalf("kept %d descriptors, want 2", len(decoded.Descriptors))
	}
	if address, ok := decoded.Address(ServiceClient); !ok || address != testAddress {
		t.Fatalf("Address(CLIENT) = %q, %v; want %q, true", address, ok, testAddress)
	}
	// Leniency stops at re-publishing: what was read cannot be written back.
	if err := decoded.Validate(); !errors.Is(err, ErrInvalidLockData) {
		t.Fatalf("Validate: want ErrInvalidLockData for an unknown service, got %v", err)
	}
}

func TestDecodeRejectsUnusablePayloads(t *testing.T) {
	for _, raw := range [][]byte{nil, {}, []byte("not json"), []byte(`{"descriptors":`)} {
		if _, err := DecodeServiceLockData(raw); err == nil {
			t.Fatalf("DecodeServiceLockData(%q) succeeded", raw)
		}
	}
}

// TestTabletServerServicesIsTheJavaSet pins the descriptor set a Java tablet
// server publishes, which is the set a Shoal process has to reach before it
// can stand in for one.
func TestTabletServerServicesIsTheJavaSet(t *testing.T) {
	want := []ThriftService{"CLIENT", "TABLET_INGEST", "TABLET_MANAGEMENT", "TABLET_SCAN", "TSERV"}
	got := TabletServerServices()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TabletServerServices() = %v, want %v", got, want)
	}
	got[0] = "MANAGER"
	if again := TabletServerServices(); !reflect.DeepEqual(again, want) {
		t.Fatalf("a caller mutated the package's service set: %v", again)
	}
}

// TestAdvertisingASubsetIsAllowed is the capability story: a process that has
// scans but not the write path advertises exactly that, so the manager routes
// it what it can serve rather than what it aspires to.
func TestAdvertisingASubsetIsAllowed(t *testing.T) {
	data, err := TabletServerLockData(serverUUID, testAddress, testGroup, ServiceClient, ServiceTabletScan)
	if err != nil {
		t.Fatalf("TabletServerLockData: %v", err)
	}
	if _, err := data.Encode(); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if _, ok := data.Address(ServiceTabletIngest); ok {
		t.Fatal("a service that was not advertised must not resolve")
	}
}

func TestThriftServiceKnown(t *testing.T) {
	for _, service := range TabletServerServices() {
		if !service.Known() {
			t.Fatalf("%s must be a known service", service)
		}
	}
	for _, service := range []ThriftService{"MANAGER", "COORDINATOR", "COMPACTOR", "GC", "NONE"} {
		if !service.Known() {
			t.Fatalf("%s is part of Accumulo's enum and must parse", service)
		}
	}
	for _, service := range []ThriftService{"", "SHOAL", "tserv", "TSERV "} {
		if service.Known() {
			t.Fatalf("%q must not be treated as an Accumulo service", service)
		}
	}
}
