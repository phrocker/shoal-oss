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
	"crypto/sha256"
	"sort"
	"time"
)

type IndexPin struct {
	Family Family
	IGEN   IGEN
}

func CompareIndexPins(left, right IndexPin) int {
	if order := bytes.Compare(left.Family, right.Family); order != 0 {
		return order
	}
	return bytes.Compare(left.IGEN, right.IGEN)
}

func SortIndexPins(values []IndexPin) {
	sort.Slice(values, func(i, j int) bool { return CompareIndexPins(values[i], values[j]) < 0 })
}

type SnapshotLeaseV2 struct {
	LeaseID             LeaseID
	Frontier            Epoch
	Owner               OwnerID
	Fence               Fence
	AuthorityGeneration Generation
	RetentionGeneration Generation
	PolicyGeneration    Generation
	PolicyCopyPinDigest Digest
	IndexPins           []IndexPin
	CreatedAt           time.Time
	ExpiresAt           time.Time
	RenewedAt           time.Time
	State               LeaseState
}

func (l SnapshotLeaseV2) Validate() error {
	if err := l.LeaseID.Validate(); err != nil {
		return err
	}
	if err := l.Frontier.Validate(); err != nil {
		return err
	}
	if err := l.Owner.Validate(); err != nil {
		return err
	}
	if err := l.Fence.Validate(); err != nil {
		return err
	}
	for _, value := range []Generation{
		l.AuthorityGeneration, l.RetentionGeneration, l.PolicyGeneration,
	} {
		if err := value.Validate(); err != nil {
			return err
		}
	}
	if err := l.PolicyCopyPinDigest.Validate("policy-copy pin digest"); err != nil {
		return err
	}
	if len(l.IndexPins) > MaxIndexPins {
		return invalid("snapshot lease has too many index pins")
	}
	for index, pin := range l.IndexPins {
		if err := pin.Family.Validate(); err != nil {
			return err
		}
		if err := pin.IGEN.Validate(); err != nil {
			return err
		}
		if index > 0 && CompareIndexPins(l.IndexPins[index-1], pin) >= 0 {
			return invalid("snapshot index pins must be strictly ordered")
		}
	}
	for name, value := range map[string]time.Time{
		"lease created_at": l.CreatedAt, "lease expires_at": l.ExpiresAt,
		"lease renewed_at": l.RenewedAt,
	} {
		if err := validateTime(name, value, false); err != nil {
			return err
		}
	}
	if l.RenewedAt.Before(l.CreatedAt) || l.ExpiresAt.Before(l.RenewedAt) {
		return invalid("snapshot lease times are inconsistent")
	}
	return l.State.Validate()
}

func (l SnapshotLeaseV2) ActiveAt(now time.Time) (bool, error) {
	if err := l.Validate(); err != nil {
		return false, err
	}
	if err := validateTime("lease comparison time", now, false); err != nil {
		return false, err
	}
	return l.State == LeaseStateActive && now.Before(l.ExpiresAt), nil
}

func ValidateSnapshotLeaseTransition(previous, next SnapshotLeaseV2) error {
	if err := previous.Validate(); err != nil {
		return err
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if !bytes.Equal(previous.LeaseID, next.LeaseID) ||
		!bytes.Equal(previous.Owner, next.Owner) ||
		previous.Frontier != next.Frontier ||
		previous.CreatedAt != next.CreatedAt ||
		previous.RetentionGeneration != next.RetentionGeneration ||
		previous.PolicyGeneration != next.PolicyGeneration ||
		previous.PolicyCopyPinDigest != next.PolicyCopyPinDigest ||
		!equalIndexPins(previous.IndexPins, next.IndexPins) {
		return invalid("snapshot lease identity and pins are immutable")
	}
	if previous.State != LeaseStateActive {
		return invalid("terminal snapshot lease cannot transition")
	}
	if next.Fence != previous.Fence {
		return invalid("snapshot lease renewal or release cannot change fence")
	}
	if next.AuthorityGeneration < previous.AuthorityGeneration {
		return invalid("snapshot lease authority generation must not decrease")
	}
	if next.RenewedAt.Before(previous.RenewedAt) ||
		next.ExpiresAt.Before(previous.ExpiresAt) {
		return invalid("snapshot lease renewal cannot shorten or move backward")
	}
	if next.State != LeaseStateActive && next.State != LeaseStateReleased &&
		next.State != LeaseStateExpired {
		return invalid("illegal snapshot lease transition")
	}
	return nil
}

func equalIndexPins(left, right []IndexPin) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index].Family, right[index].Family) ||
			!bytes.Equal(left[index].IGEN, right[index].IGEN) {
			return false
		}
	}
	return true
}

func encodeSnapshotLease(e *encoder, l SnapshotLeaseV2) {
	e.bytes("lease ID", l.LeaseID)
	e.u64(uint64(l.Frontier))
	e.bytes("owner", l.Owner)
	e.u64(uint64(l.Fence))
	e.u64(uint64(l.AuthorityGeneration))
	e.u64(uint64(l.RetentionGeneration))
	e.u64(uint64(l.PolicyGeneration))
	e.digest(l.PolicyCopyPinDigest)
	e.u32(uint32(len(l.IndexPins)))
	for _, pin := range l.IndexPins {
		e.bytes("index family", pin.Family)
		e.bytes("IGEN", pin.IGEN)
	}
	e.timestamp(l.CreatedAt)
	e.timestamp(l.ExpiresAt)
	e.timestamp(l.RenewedAt)
	e.byte(byte(l.State))
}

func MarshalSnapshotLeaseV2(l SnapshotLeaseV2) ([]byte, error) {
	l.IndexPins = append([]IndexPin(nil), l.IndexPins...)
	SortIndexPins(l.IndexPins)
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return marshalEnvelope(KindSnapshotLease, VersionSnapshotLeaseV2, MaxRootBytes,
		func(e *encoder) { encodeSnapshotLease(e, l) })
}

func UnmarshalSnapshotLeaseV2(data []byte) (SnapshotLeaseV2, error) {
	payload, err := verifyEnvelope(data, KindSnapshotLease, VersionSnapshotLeaseV2, MaxRootBytes)
	if err != nil {
		return SnapshotLeaseV2{}, err
	}
	d := &decoder{data: payload}
	l := SnapshotLeaseV2{
		LeaseID:             LeaseID(d.bytes("lease ID", MaxOpaqueIDBytes, true)),
		Frontier:            Epoch(d.positive("frontier")),
		Owner:               OwnerID(d.bytes("owner", MaxOwnerBytes, true)),
		Fence:               Fence(d.positive("fence")),
		AuthorityGeneration: Generation(d.positive("authority generation")),
		RetentionGeneration: Generation(d.positive("retention generation")),
		PolicyGeneration:    Generation(d.positive("policy generation")),
		PolicyCopyPinDigest: d.digest("policy-copy pin digest"),
	}
	count := d.u32("index pin count")
	if count > MaxIndexPins || uint64(count) > uint64(d.remaining())/8 {
		return SnapshotLeaseV2{}, invalid("index pin count exceeds its bound or remaining payload")
	}
	l.IndexPins = make([]IndexPin, int(count))
	for index := range l.IndexPins {
		l.IndexPins[index] = IndexPin{
			Family: Family(d.bytes("index family", MaxOpaqueIDBytes, true)),
			IGEN:   IGEN(d.bytes("IGEN", MaxOpaqueIDBytes, true)),
		}
	}
	l.CreatedAt = d.timestamp("created_at")
	l.ExpiresAt = d.timestamp("expires_at")
	l.RenewedAt = d.timestamp("renewed_at")
	l.State = LeaseState(d.byte("state"))
	if d.err != nil {
		return SnapshotLeaseV2{}, d.err
	}
	if err := l.Validate(); err != nil {
		return SnapshotLeaseV2{}, err
	}
	if err := finishDecode(d, func(e *encoder) { encodeSnapshotLease(e, l) }, MaxRootBytes); err != nil {
		return SnapshotLeaseV2{}, err
	}
	return l, nil
}

type RetirementDecisionV1 struct {
	ObjectKind          EntityKind
	ObjectID            EntityID
	ObjectGeneration    Generation
	SafeAfterFrontier   Epoch
	SafeAfterTime       time.Time
	HistoryFloor        Epoch
	ProofDigest         Digest
	AuthorityGeneration Generation
	State               RetirementState
}

func (r RetirementDecisionV1) Validate() error {
	if err := r.ObjectKind.Validate(); err != nil {
		return err
	}
	if err := r.ObjectID.Validate(); err != nil {
		return err
	}
	if err := r.ObjectGeneration.Validate(); err != nil {
		return err
	}
	if err := r.SafeAfterFrontier.Validate(); err != nil {
		return err
	}
	if err := validateTime("retirement safe-after time", r.SafeAfterTime, false); err != nil {
		return err
	}
	if err := r.HistoryFloor.Validate(); err != nil {
		return err
	}
	if r.HistoryFloor > r.SafeAfterFrontier {
		return invalid("retirement history floor exceeds safe frontier")
	}
	if err := r.ProofDigest.Validate("retirement proof digest"); err != nil {
		return err
	}
	if err := r.AuthorityGeneration.Validate(); err != nil {
		return err
	}
	return r.State.Validate()
}

func ValidateRetirementTransition(previous, next RetirementDecisionV1) error {
	if err := previous.Validate(); err != nil {
		return err
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if !bytes.Equal(previous.ObjectKind, next.ObjectKind) ||
		!bytes.Equal(previous.ObjectID, next.ObjectID) ||
		previous.ObjectGeneration != next.ObjectGeneration ||
		previous.SafeAfterFrontier != next.SafeAfterFrontier ||
		previous.SafeAfterTime != next.SafeAfterTime ||
		previous.HistoryFloor != next.HistoryFloor ||
		previous.ProofDigest != next.ProofDigest {
		return invalid("retirement identity and proof are immutable")
	}
	if previous.State != RetirementCandidate {
		if previous.State == RetirementApproved && next.State == RetirementApplied {
			return nil
		}
		return invalid("terminal retirement decision cannot transition")
	}
	if next.AuthorityGeneration < previous.AuthorityGeneration {
		return invalid("retirement authority generation must not decrease")
	}
	switch next.State {
	case RetirementApproved, RetirementRejected, RetirementPoisoned:
		return nil
	default:
		return invalid("illegal retirement transition")
	}
}

func encodeRetirementDecision(e *encoder, r RetirementDecisionV1) {
	e.bytes("object kind", r.ObjectKind)
	e.bytes("object ID", r.ObjectID)
	e.u64(uint64(r.ObjectGeneration))
	e.u64(uint64(r.SafeAfterFrontier))
	e.timestamp(r.SafeAfterTime)
	e.u64(uint64(r.HistoryFloor))
	e.digest(r.ProofDigest)
	e.u64(uint64(r.AuthorityGeneration))
	e.byte(byte(r.State))
}

func MarshalRetirementDecisionV1(r RetirementDecisionV1) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return marshalEnvelope(KindRetirementDecision, VersionRetirementDecisionV1, MaxRootBytes,
		func(e *encoder) { encodeRetirementDecision(e, r) })
}

func UnmarshalRetirementDecisionV1(data []byte) (RetirementDecisionV1, error) {
	payload, err := verifyEnvelope(data, KindRetirementDecision, VersionRetirementDecisionV1, MaxRootBytes)
	if err != nil {
		return RetirementDecisionV1{}, err
	}
	d := &decoder{data: payload}
	r := RetirementDecisionV1{
		ObjectKind:          EntityKind(d.bytes("object kind", MaxObjectKindBytes, true)),
		ObjectID:            EntityID(d.bytes("object ID", MaxOpaqueIDBytes, true)),
		ObjectGeneration:    Generation(d.positive("object generation")),
		SafeAfterFrontier:   Epoch(d.positive("safe-after frontier")),
		SafeAfterTime:       d.timestamp("safe-after time"),
		HistoryFloor:        Epoch(d.positive("history floor")),
		ProofDigest:         d.digest("proof digest"),
		AuthorityGeneration: Generation(d.positive("authority generation")),
		State:               RetirementState(d.byte("state")),
	}
	if d.err != nil {
		return RetirementDecisionV1{}, d.err
	}
	if err := r.Validate(); err != nil {
		return RetirementDecisionV1{}, err
	}
	if err := finishDecode(d, func(e *encoder) { encodeRetirementDecision(e, r) }, MaxRootBytes); err != nil {
		return RetirementDecisionV1{}, err
	}
	return r, nil
}

type HistoryFloorV1 struct {
	Floor               Epoch
	RetentionGeneration Generation
	AdvancedAt          time.Time
	PredecessorDigest   Digest
	Digest              Digest
}

func (h HistoryFloorV1) computedDigest() Digest {
	e := newPayloadEncoder(MaxRootBytes)
	e.u64(uint64(h.Floor))
	e.u64(uint64(h.RetentionGeneration))
	e.timestamp(h.AdvancedAt)
	e.digest(h.PredecessorDigest)
	return sha256.Sum256(e.data)
}

func (h HistoryFloorV1) ComputeDigest() Digest { return h.computedDigest() }

func (h HistoryFloorV1) Validate() error {
	if err := h.Floor.Validate(); err != nil {
		return err
	}
	if err := h.RetentionGeneration.Validate(); err != nil {
		return err
	}
	if err := validateTime("history-floor advanced_at", h.AdvancedAt, false); err != nil {
		return err
	}
	if h.Digest != h.computedDigest() {
		return invalid("history-floor digest mismatch")
	}
	return nil
}

func ValidateHistoryFloorAdvance(previous, next HistoryFloorV1) error {
	if err := previous.Validate(); err != nil {
		return err
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if next.Floor < previous.Floor {
		return invalid("history floor must not decrease")
	}
	if next.RetentionGeneration <= previous.RetentionGeneration {
		return invalid("retention generation must increase")
	}
	if next.AdvancedAt.Before(previous.AdvancedAt) {
		return invalid("history-floor time must not decrease")
	}
	if next.PredecessorDigest != previous.Digest {
		return invalid("history-floor predecessor digest mismatch")
	}
	return nil
}

func encodeHistoryFloor(e *encoder, h HistoryFloorV1) {
	e.u64(uint64(h.Floor))
	e.u64(uint64(h.RetentionGeneration))
	e.timestamp(h.AdvancedAt)
	e.digest(h.PredecessorDigest)
	e.digest(h.Digest)
}

func MarshalHistoryFloorV1(h HistoryFloorV1) ([]byte, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}
	return marshalEnvelope(KindHistoryFloor, VersionHistoryFloorV1, MaxRootBytes,
		func(e *encoder) { encodeHistoryFloor(e, h) })
}

func UnmarshalHistoryFloorV1(data []byte) (HistoryFloorV1, error) {
	payload, err := verifyEnvelope(data, KindHistoryFloor, VersionHistoryFloorV1, MaxRootBytes)
	if err != nil {
		return HistoryFloorV1{}, err
	}
	d := &decoder{data: payload}
	h := HistoryFloorV1{
		Floor:               Epoch(d.positive("floor")),
		RetentionGeneration: Generation(d.positive("retention generation")),
		AdvancedAt:          d.timestamp("advanced_at"),
		PredecessorDigest:   d.digest("predecessor digest"),
		Digest:              d.digest("digest"),
	}
	if d.err != nil {
		return HistoryFloorV1{}, d.err
	}
	if err := h.Validate(); err != nil {
		return HistoryFloorV1{}, err
	}
	if err := finishDecode(d, func(e *encoder) { encodeHistoryFloor(e, h) }, MaxRootBytes); err != nil {
		return HistoryFloorV1{}, err
	}
	return h, nil
}
