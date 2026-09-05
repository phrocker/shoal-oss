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

package explorer

import (
	"context"
	"crypto/sha256"

	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// EmbeddedTableName is the durable table used by the embedded Explorer.
const EmbeddedTableName = "_shoal_explorer"

// RecordPublication is the narrow immutable-record seam used by the embedded
// Explorer. The adapter owns transaction IDs, manifests, fencing, and recovery;
// Explorer continues to own its existing record envelope and row layout.
type RecordPublication struct {
	Operation       []byte
	Token           []byte
	Table           string
	Row             []byte
	Family          []byte
	Qualifier       []byte
	Visibility      []byte
	Value           []byte
	EntityKind      byte
	EntityID        []byte
	WinnerID        []byte
	Partition       []byte
	LogicalPolicyID []byte
	ResultKind      []byte
	ResultID        []byte
}

type RecordPublicationResult struct {
	Epoch     coordination.Epoch
	Unchanged bool
}

// RecordPublicationAdapter durably publishes one immutable Explorer record.
// Implementations must persist their canonical logical intent before making
// any physical write and must resolve ambiguous commits by authoritative
// readback or return an indeterminate-commit error.
type RecordPublicationAdapter interface {
	PublishRecord(context.Context, RecordPublication) (RecordPublicationResult, error)
}

var embeddedDefaultPolicy = []byte("embedded/default")

func (e *Explorer) writeDocumentRecord(
	ctx context.Context,
	row []byte,
	record *persistedDocument,
) error {
	encoded, err := encodeEmbeddedRecord(embeddedRecordDocument, record)
	if err != nil {
		return shoal.WrapError(shoal.ErrorInternal, "encode explorer record", err)
	}
	// The adopted manifest format bounds one physical value to 1 MiB. Keep
	// larger legacy document envelopes on the established record path until a
	// chunked document encoding is defined; do not pretend they are covered by
	// the transaction fence.
	if e.publication == nil || len(encoded) > coordination.MaxManifestValueBytes {
		return e.writeEncodedRecord(row, encoded)
	}
	recordKey := sha256.Sum256(append([]byte("explorer-document-record-v1\x00"), row...))
	_, err = e.publication.PublishRecord(ctx, RecordPublication{
		Operation:       []byte("explorer-document-record-v1"),
		Token:           append([]byte(nil), recordKey[:]...),
		Table:           EmbeddedTableName,
		Row:             append([]byte(nil), row...),
		Family:          []byte(recordCF),
		Qualifier:       []byte(recordCQV2),
		Value:           encoded,
		EntityKind:      'D',
		EntityID:        append([]byte(nil), recordKey[:]...),
		WinnerID:        []byte(record.Revision.ID),
		Partition:       append([]byte(nil), recordKey[:]...),
		LogicalPolicyID: append([]byte(nil), embeddedDefaultPolicy...),
		ResultKind:      []byte("document-revision"),
		ResultID:        []byte(record.Revision.ID),
	})
	return err
}
