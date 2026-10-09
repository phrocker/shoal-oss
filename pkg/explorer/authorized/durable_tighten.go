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
	"fmt"
	"os"
	"path/filepath"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	policyRowLabelMigration  = "meta/label-migration"
	policyKindLabelMigration = byte(9)
)

type persistedLabelMigration struct {
	Seq                 uint64
	Version             uint32
	Documents           int
	Unregistered        int
	Unlabelled          int
	Tightened           int
	AlreadyTightened    int
	HistoricalTightened int
	Untranslatable      []persistedUntranslatable
}

type persistedUntranslatable struct {
	DocumentID   string
	RevisionID   string
	SourceURI    string
	EscapedLabel string
	Reason       string
}

// OpenExistingDurablePolicyStore opens a durable policy catalog that already
// exists, for read-only tooling such as -list-untranslatable-labels. Unlike
// OpenDurablePolicyStore it never creates anything: a missing directory, or
// one without the catalog's table, is refused before the storage engine is
// opened (the engine would create both).
func OpenExistingDurablePolicyStore(dir string) (*DurablePolicyStore, error) {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, shoal.NewError(shoal.ErrorNotFound,
			"no policy catalog at "+dir)
	}
	table, err := os.Stat(filepath.Join(dir, policyTable))
	if err != nil || !table.IsDir() {
		return nil, shoal.NewError(shoal.ErrorNotFound,
			"no policy catalog at "+dir+": it holds no "+policyTable+" table")
	}
	return OpenDurablePolicyStore(dir)
}

// policyRow is one record of a batched durable write.
type policyRow struct {
	row   []byte
	kind  byte
	value any
}

// TightenRule tightens the in-memory catalog and persists every record it
// rewrote in ONE engine write: the policy table is a single tablet, which
// appends the whole batch to its WAL in one write before applying any of it.
// A crash can still tear that write and persist only a prefix of the batch.
// That is narrowing-only (every persisted record is tighter, none wider), and
// the label migration does not rely on more: it re-sweeps every labelled
// document on every start, and TightenRule completes whatever a torn write
// left behind.
func (s *DurablePolicyStore) TightenRule(
	ctx context.Context,
	index *TighteningIndex,
	tightening RuleTightening,
) (bool, error) {
	if s == nil {
		return (*MemoryPolicyStore)(nil).TightenRule(ctx, index, tightening)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	changes, err := s.memory.tightenRule(ctx, index, tightening)
	if err != nil {
		return false, err
	}
	if !changes.changed() {
		return false, nil
	}
	rows := make([]policyRow, 0,
		len(changes.revisions)+len(changes.nodes)+len(changes.edges)+2)
	for _, key := range changes.revisions {
		row, err := s.revisionRow(key)
		if err != nil {
			return false, err
		}
		rows = append(rows, row)
	}
	for _, nodeID := range changes.nodes {
		row, err := s.nodeRow(nodeID)
		if err != nil {
			return false, err
		}
		rows = append(rows, row)
	}
	for _, edgeID := range changes.edges {
		registration, ok := s.memory.snapshotApplicationEdge(edgeID)
		if !ok {
			return false, catalogUnavailable()
		}
		record, err := edgeToPersisted(registration, s.nextSeq(), false)
		if err != nil {
			return false, catalogUnavailable()
		}
		rows = append(rows, policyRow{policyEdgeRow(edgeID), policyKindEdge, record})
	}
	if changes.sourceClaim != "" {
		claimRows, err := s.sourceClaimRows(changes.sourceClaim)
		if err != nil {
			return false, err
		}
		rows = append(rows, claimRows...)
	}
	if err := s.writeRows(rows); err != nil {
		return false, err
	}
	return true, nil
}

// TighteningIndex is a pure read delegated to the memory store.
func (s *DurablePolicyStore) TighteningIndex(
	ctx context.Context,
) (*TighteningIndex, error) {
	return s.memoryStore().TighteningIndex(ctx)
}

// LabelMigration is a pure read delegated to the memory store, whose marker
// was reconstructed from the engine on open.
func (s *DurablePolicyStore) LabelMigration(
	ctx context.Context,
) (LabelMigrationRecord, bool, error) {
	return s.memoryStore().LabelMigration(ctx)
}

// PutLabelMigration records the marker in memory and persists it.
func (s *DurablePolicyStore) PutLabelMigration(
	ctx context.Context,
	record LabelMigrationRecord,
) error {
	if s == nil {
		return (*MemoryPolicyStore)(nil).PutLabelMigration(ctx, record)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.memory.PutLabelMigration(ctx, record); err != nil {
		return err
	}
	persisted := persistedLabelMigration{
		Seq:                 s.nextSeq(),
		Version:             record.Version,
		Documents:           record.Documents,
		Unregistered:        record.Unregistered,
		Unlabelled:          record.Unlabelled,
		Tightened:           record.Tightened,
		AlreadyTightened:    record.AlreadyTightened,
		HistoricalTightened: record.HistoricalTightened,
	}
	for _, entry := range record.Untranslatable {
		persisted.Untranslatable = append(persisted.Untranslatable,
			persistedUntranslatable{
				DocumentID:   string(entry.DocumentID),
				RevisionID:   string(entry.RevisionID),
				SourceURI:    entry.SourceURI,
				EscapedLabel: entry.EscapedLabel,
				Reason:       entry.Reason,
			})
	}
	return s.writeRow(
		[]byte(policyRowLabelMigration), policyKindLabelMigration, persisted)
}

func labelMigrationFromPersisted(
	record persistedLabelMigration,
) (LabelMigrationRecord, error) {
	if record.Version == 0 {
		return LabelMigrationRecord{}, fmt.Errorf("label migration version is zero")
	}
	migration := LabelMigrationRecord{
		Version:             record.Version,
		Documents:           record.Documents,
		Unregistered:        record.Unregistered,
		Unlabelled:          record.Unlabelled,
		Tightened:           record.Tightened,
		AlreadyTightened:    record.AlreadyTightened,
		HistoricalTightened: record.HistoricalTightened,
	}
	for _, entry := range record.Untranslatable {
		migration.Untranslatable = append(migration.Untranslatable,
			UntranslatableLabel{
				DocumentID:   shoal.ID(entry.DocumentID),
				RevisionID:   shoal.ID(entry.RevisionID),
				SourceURI:    entry.SourceURI,
				EscapedLabel: entry.EscapedLabel,
				Reason:       entry.Reason,
			})
	}
	return migration, nil
}

func (s *DurablePolicyStore) revisionRow(key revisionKey) (policyRow, error) {
	stored, ok := s.memory.snapshotRevision(key)
	if !ok {
		return policyRow{}, catalogUnavailable()
	}
	rule, err := ruleToPersisted(stored.Rule)
	if err != nil {
		return policyRow{}, catalogUnavailable()
	}
	return policyRow{policyRevisionRow(key), policyKindRevision, persistedRevision{
		Seq:            s.nextSeq(),
		DocumentID:     string(key.documentID),
		RevisionID:     string(key.revisionID),
		NodeIDs:        stringsFromIDs(stored.NodeIDs),
		IntrinsicEdges: edgesToPersisted(stored.IntrinsicEdges),
		ContentDigest:  [32]byte(stored.ContentDigest),
		Rule:           rule,
	}}, nil
}

func (s *DurablePolicyStore) nodeRow(nodeID shoal.ID) (policyRow, error) {
	registration, ok := s.memory.snapshotNode(nodeID)
	if !ok {
		return policyRow{}, catalogUnavailable()
	}
	rule, err := ruleToPersisted(registration.Rule)
	if err != nil {
		return policyRow{}, catalogUnavailable()
	}
	return policyRow{policyNodeRow(nodeID), policyKindNode, persistedNode{
		Seq:        s.nextSeq(),
		NodeID:     string(nodeID),
		DocumentID: string(registration.DocumentID),
		RevisionID: string(registration.RevisionID),
		Node:       graphNodeToPersisted(registration.Node),
		Rule:       rule,
		Kind:       uint8(registration.Kind),
	}}, nil
}

// sourceClaimRows mirrors persistSourceClaim for a present claim, as rows of
// a batch: the claim and the refreshed source-version counter.
func (s *DurablePolicyStore) sourceClaimRows(sourceURI string) ([]policyRow, error) {
	claim, present := s.memory.snapshotSourceClaim(sourceURI)
	if !present {
		return nil, catalogUnavailable()
	}
	rule, err := ruleToPersisted(claim.Rule)
	if err != nil {
		return nil, catalogUnavailable()
	}
	record := persistedSourceClaim{
		Seq:       s.nextSeq(),
		SourceURI: sourceURI,
		Rule:      rule,
		Pending:   claim.Pending,
		Version:   claim.Version,
	}
	if claim.PreviousRule != nil {
		previous, err := ruleToPersisted(*claim.PreviousRule)
		if err != nil {
			return nil, catalogUnavailable()
		}
		record.PreviousRule = &previous
	}
	version := persistedSourceVersion{
		Seq:     s.nextSeq(),
		Version: s.memory.snapshotSourceVersion(),
	}
	return []policyRow{
		{policySourceClaimRow(sourceURI), policyKindSourceClaim, record},
		{[]byte(policyRowSourceVersion), policyKindSourceVersion, version},
	}, nil
}

// writeRows encodes every row first and then writes them in one engine
// write, so an encoding failure writes nothing.
func (s *DurablePolicyStore) writeRows(rows []policyRow) error {
	mutations := make([]*cclient.Mutation, 0, len(rows))
	for _, row := range rows {
		encoded, err := encodePolicyRecord(row.kind, row.value)
		if err != nil {
			return catalogUnavailable()
		}
		mutation, err := cclient.NewMutation(row.row)
		if err != nil {
			return catalogUnavailable()
		}
		mutation.PutLatest(
			[]byte(policyRecordCF), []byte(policyRecordCQ), nil, encoded)
		mutations = append(mutations, mutation)
	}
	if err := s.engine.Write(policyTable, mutations); err != nil {
		return catalogUnavailable()
	}
	return nil
}
