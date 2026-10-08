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

package auth

import (
	"unicode"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	// MaxGrantProvenanceBytes bounds each text field of a GrantProvenance.
	MaxGrantProvenanceBytes = 1024
	// MaxGrantClaimPathSegments bounds the claim path a grant names.
	MaxGrantClaimPathSegments = 8
)

// GrantProvenance says which operator mapping granted a decision its
// authority, and which token claim satisfied it (#451). It is set by an
// authenticator that maps a role from a claim against an operator-supplied
// mapping, and it is empty for every other decision.
//
// It carries no token bytes: the issuer and subject identify the principal,
// the claim path and matched value say which configured value the token
// carried, and the mapping digest pins the exact mapping in force when the
// decision was minted. A consumer that must refuse once the mapping changes
// compares MappingDigest with the digest now in force.
type GrantProvenance struct {
	Issuer        string
	Subject       string
	ClaimPath     []string
	MatchedValue  string
	MappingDigest Digest
}

// Set reports whether any provenance is present.
func (p GrantProvenance) Set() bool {
	return p.Issuer != "" || p.Subject != "" || len(p.ClaimPath) > 0 ||
		p.MatchedValue != "" || p.MappingDigest != (Digest{})
}

// Validate requires a set provenance to be complete and bounded. An empty
// provenance is valid.
func (p GrantProvenance) Validate() error {
	if !p.Set() {
		return nil
	}
	invalid := func(what string) error {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "grant provenance "+what+" is invalid")
	}
	if !validProvenanceText(p.Issuer) {
		return invalid("issuer")
	}
	if !validProvenanceText(p.Subject) {
		return invalid("subject")
	}
	if len(p.ClaimPath) == 0 || len(p.ClaimPath) > MaxGrantClaimPathSegments {
		return invalid("claim path")
	}
	for _, segment := range p.ClaimPath {
		if !validProvenanceText(segment) {
			return invalid("claim path")
		}
	}
	if !validProvenanceText(p.MatchedValue) {
		return invalid("matched value")
	}
	if p.MappingDigest == (Digest{}) {
		return invalid("mapping digest")
	}
	return nil
}

// Clone returns an independent copy.
func (p GrantProvenance) Clone() GrantProvenance {
	p.ClaimPath = append([]string(nil), p.ClaimPath...)
	return p
}

// Equal reports whether two provenances are the same, field for field.
func (p GrantProvenance) Equal(other GrantProvenance) bool {
	if p.Issuer != other.Issuer || p.Subject != other.Subject ||
		p.MatchedValue != other.MatchedValue ||
		p.MappingDigest != other.MappingDigest ||
		len(p.ClaimPath) != len(other.ClaimPath) {
		return false
	}
	for index := range p.ClaimPath {
		if p.ClaimPath[index] != other.ClaimPath[index] {
			return false
		}
	}
	return true
}

func validProvenanceText(value string) bool {
	if value == "" || len(value) > MaxGrantProvenanceBytes ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

// GrantProvenance returns an independent copy of the decision's grant
// provenance, which is empty unless an operator mapping granted it.
func (d Decision) GrantProvenance() GrantProvenance {
	return d.grantProvenance.Clone()
}

// grantProvenance appends the provenance to a fingerprint. It is called
// only for a set provenance, so a decision without one keeps the fingerprint
// it had before provenance existed. The leading marker is 2, distinct from
// the selected-ontology marker 1 that may precede it, so the two optional
// sections can never be read as each other.
func (e *digestEncoder) grantProvenance(p GrantProvenance) {
	e.uint64(2)
	e.text(p.Issuer)
	e.text(p.Subject)
	e.uint64(uint64(len(p.ClaimPath)))
	for _, segment := range p.ClaimPath {
		e.text(segment)
	}
	e.text(p.MatchedValue)
	e.bytes(p.MappingDigest[:])
}
