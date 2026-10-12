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
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Declassification audit records (#630) are append-only rows under their own
// prefix and record kind. No existing row or kind changes, and a binary that
// predates them skips the prefix on load.
const (
	policyRowDeclassification  = "audit/declassification/"
	policyKindDeclassification = byte(10)
)

type persistedDeclassification struct {
	Seq                      uint64
	AttemptID                string
	Phase                    string
	OccurredAtUnixNano       int64
	SourceURI                string
	DocumentID               string
	PriorRevisionID          string
	RevisionID               string
	PriorRules               []persistedRule
	Rule                     persistedRule
	Subject                  string
	Actor                    string
	ClientID                 string
	OnBehalfOf               []string
	RequestID                string
	CorrelationID            string
	AuthorizationFingerprint [32]byte
	ReasonCode               string
}

type sequencedDeclassification struct {
	seq    uint64
	record DeclassificationRecord
}

// orderDeclassifications restores recording order from the write sequence,
// since the engine scans rows in key order.
func orderDeclassifications(
	records []sequencedDeclassification,
) []DeclassificationRecord {
	sort.SliceStable(records, func(left, right int) bool {
		return records[left].seq < records[right].seq
	})
	ordered := make([]DeclassificationRecord, 0, len(records))
	for _, entry := range records {
		ordered = append(ordered, entry.record)
	}
	return ordered
}

func policyDeclassificationRow(record DeclassificationRecord) []byte {
	return []byte(policyRowDeclassification + string(record.AttemptID) +
		"/" + string(record.Phase))
}

// RecordDeclassification validates the record, writes it through to the
// engine, and only then makes it observable in memory, so a record that
// failed to persist is never reported as recorded.
func (s *DurablePolicyStore) RecordDeclassification(
	ctx context.Context,
	record DeclassificationRecord,
) error {
	if s == nil {
		return (*MemoryPolicyStore)(nil).RecordDeclassification(ctx, record)
	}
	if err := contextFailure(ctx); err != nil {
		return err
	}
	if err := record.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return catalogUnavailable()
	}
	// s.mu serializes every durable write, so no record for this pair can
	// appear between this check and the write below.
	if s.memory.hasDeclassification(record.AttemptID, record.Phase) {
		return duplicateDeclassification()
	}
	persisted, err := declassificationToPersisted(record, s.nextSeq())
	if err != nil {
		return catalogUnavailable()
	}
	if err := s.writeRow(
		policyDeclassificationRow(record), policyKindDeclassification,
		persisted,
	); err != nil {
		return err
	}
	return s.memory.RecordDeclassification(context.WithoutCancel(ctx), record)
}

// Declassifications is a pure read delegated to the memory store, whose
// records were reconstructed from the engine on open.
func (s *DurablePolicyStore) Declassifications(
	ctx context.Context,
) ([]DeclassificationRecord, error) {
	return s.memoryStore().Declassifications(ctx)
}

func declassificationToPersisted(
	record DeclassificationRecord,
	seq uint64,
) (persistedDeclassification, error) {
	rule, err := ruleToPersisted(record.Rule)
	if err != nil {
		return persistedDeclassification{}, err
	}
	persisted := persistedDeclassification{
		Seq:                      seq,
		AttemptID:                string(record.AttemptID),
		Phase:                    string(record.Phase),
		OccurredAtUnixNano:       record.OccurredAt.UnixNano(),
		SourceURI:                record.SourceURI,
		DocumentID:               string(record.DocumentID),
		PriorRevisionID:          string(record.PriorRevisionID),
		RevisionID:               string(record.RevisionID),
		Rule:                     rule,
		Subject:                  string(record.Subject),
		Actor:                    string(record.Actor),
		ClientID:                 string(record.ClientID),
		OnBehalfOf:               stringsFromIDs(record.OnBehalfOf),
		RequestID:                string(record.RequestID),
		CorrelationID:            string(record.CorrelationID),
		AuthorizationFingerprint: [32]byte(record.AuthorizationFingerprint),
		ReasonCode:               record.ReasonCode,
	}
	for _, prior := range record.PriorRules {
		encoded, err := ruleToPersisted(prior)
		if err != nil {
			return persistedDeclassification{}, err
		}
		persisted.PriorRules = append(persisted.PriorRules, encoded)
	}
	return persisted, nil
}

func declassificationFromPersisted(
	persisted persistedDeclassification,
) (DeclassificationRecord, error) {
	rule, err := ruleFromPersisted(persisted.Rule)
	if err != nil {
		return DeclassificationRecord{}, err
	}
	record := DeclassificationRecord{
		AttemptID:                shoal.ID(persisted.AttemptID),
		Phase:                    DeclassificationPhase(persisted.Phase),
		OccurredAt:               time.Unix(0, persisted.OccurredAtUnixNano).UTC(),
		SourceURI:                persisted.SourceURI,
		DocumentID:               shoal.ID(persisted.DocumentID),
		PriorRevisionID:          shoal.ID(persisted.PriorRevisionID),
		RevisionID:               shoal.ID(persisted.RevisionID),
		Rule:                     rule,
		Subject:                  shoal.ID(persisted.Subject),
		Actor:                    shoal.ID(persisted.Actor),
		ClientID:                 shoal.ID(persisted.ClientID),
		OnBehalfOf:               idsFromStrings(persisted.OnBehalfOf),
		RequestID:                shoal.ID(persisted.RequestID),
		CorrelationID:            shoal.ID(persisted.CorrelationID),
		AuthorizationFingerprint: auth.Fingerprint(persisted.AuthorizationFingerprint),
		ReasonCode:               persisted.ReasonCode,
	}
	for _, prior := range persisted.PriorRules {
		decoded, err := ruleFromPersisted(prior)
		if err != nil {
			return DeclassificationRecord{}, err
		}
		record.PriorRules = append(record.PriorRules, decoded)
	}
	if err := record.validate(); err != nil {
		return DeclassificationRecord{}, err
	}
	return record, nil
}
