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

package coordination

import (
	"bytes"
	"reflect"
	"testing"
)

func TestM3CoordinationRowGoldensAndRoundTrips(t *testing.T) {
	domain := DomainID{0, 0xff, 'd'}
	lpart := LPART{'p', 0, 0xff}
	family := Family{'f', 0, 0xff}
	igen := IGEN{0, 0xff, 'i'}
	visibility := testDigest("visibility")
	tests := []struct {
		name  string
		make  func() ([]byte, error)
		parse func([]byte) (any, error)
		want  any
	}{
		{"entity_head_row_v1.bin", func() ([]byte, error) { return EntityHeadRow(domain, 1, EntityID{0, 0xff, 'e'}) }, func(b []byte) (any, error) { return ParseEntityHeadRow(b) }, EntityHeadKey{Domain: domain, Kind: 1, ID: EntityID{0, 0xff, 'e'}}},
		{"policy_copy_row_v1.bin", func() ([]byte, error) { return PolicyCopyRow(domain, lpart, 4, visibility) }, func(b []byte) (any, error) { return ParsePolicyCopyRow(b) }, PolicyCopyKey{Domain: domain, LPART: lpart, Generation: 4, VisibilityDigest: visibility}},
		{"policy_copy_map_row_v1.bin", func() ([]byte, error) { return PolicyCopyMapRow(domain, lpart, 8, visibility) }, func(b []byte) (any, error) { return ParsePolicyCopyMapRow(b) }, PolicyCopyKey{Domain: domain, LPART: lpart, Generation: 8, VisibilityDigest: visibility}},
		{"policy_generation_row_v1.bin", func() ([]byte, error) { return PolicyGenerationRow(domain, 8) }, func(b []byte) (any, error) { return ParsePolicyGenerationRow(b) }, PolicyGenerationKey{Domain: domain, Generation: 8}},
		{"index_generation_row_v1.bin", func() ([]byte, error) { return IndexGenerationRow(domain, family, igen) }, func(b []byte) (any, error) { return ParseIndexGenerationRow(b) }, IndexGenerationKey{Domain: domain, Family: family, IGEN: igen}},
		{"index_delta_row_v1.bin", func() ([]byte, error) { return IndexDeltaRow(domain, family, igen, 13, TXN{0, 0xff, 't'}) }, func(b []byte) (any, error) { return ParseIndexDeltaRow(b) }, IndexDeltaKey{IndexGenerationKey: IndexGenerationKey{Domain: domain, Family: family, IGEN: igen}, Epoch: 13, TXN: TXN{0, 0xff, 't'}}},
		{"index_activation_row_v1.bin", func() ([]byte, error) { return IndexActivationRow(domain, family, 16, igen) }, func(b []byte) (any, error) { return ParseIndexActivationRow(b) }, IndexActivationKey{Domain: domain, Family: family, ActivationEpoch: 16, IGEN: igen}},
		{"snapshot_lease_row_v1.bin", func() ([]byte, error) { return SnapshotLeaseRow(domain, LeaseID{0, 0xff, 'l'}) }, func(b []byte) (any, error) { return ParseSnapshotLeaseRow(b) }, SnapshotLeaseKey{Domain: domain, Lease: LeaseID{0, 0xff, 'l'}}},
		{"retirement_row_v1.bin", func() ([]byte, error) { return RetirementRow(domain, 2, EntityID{0, 0xff, 'r'}) }, func(b []byte) (any, error) { return ParseRetirementRow(b) }, RetirementKey{Domain: domain, Kind: 2, ID: EntityID{0, 0xff, 'r'}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row, err := test.make()
			if err != nil {
				t.Fatal(err)
			}
			row = golden(t, test.name, row)
			decoded, err := test.parse(row)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, test.want) {
				t.Fatalf("row round trip differs:\ngot  %#v\nwant %#v", decoded, test.want)
			}
			if _, err := test.parse(append(append([]byte(nil), row...), 0)); err == nil {
				t.Fatal("trailing row bytes accepted")
			}
			corrupt := append([]byte(nil), row...)
			corrupt[2] ^= 1
			if _, err := test.parse(corrupt); err == nil {
				t.Fatal("partition band mismatch accepted")
			}
		})
	}
}

func TestM3RowOrderingAndSharedAllocatorRows(t *testing.T) {
	domain := DomainID("domain")
	visibility := testDigest("visibility")
	oldMap, err := PolicyCopyMapRow(domain, LPART("part"), 7, visibility)
	if err != nil {
		t.Fatal(err)
	}
	newMap, err := PolicyCopyMapRow(domain, LPART("part"), 8, visibility)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(newMap, oldMap) >= 0 {
		t.Fatal("newer policy map does not sort first")
	}
	oldActivation, _ := IndexActivationRow(domain, Family("lexical"), 10, IGEN("g1"))
	newActivation, _ := IndexActivationRow(domain, Family("lexical"), 11, IGEN("g2"))
	if bytes.Compare(newActivation, oldActivation) >= 0 {
		t.Fatal("newer index activation does not sort first")
	}
	allocator, _ := AllocatorRow(domain)
	for name, makeRow := range map[string]func(DomainID) ([]byte, error){
		"history floor": HistoryFloorRow, "writer authority": WriterAuthorityRow,
		"backend observation": BackendObservationRow,
	} {
		row, err := makeRow(domain)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(row, allocator) {
			t.Fatalf("%s is not stored on the allocator authority row", name)
		}
	}
}
