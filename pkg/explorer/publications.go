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
	"time"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// RevisionPublications reports when each stored revision of the requested
// documents was published (its PublishedAt, the same clock a snapshot's AsOf
// is taken from). A revision with no recorded publication time is omitted, as
// is a document with no stored revision. The authorized layer uses it to bound
// which revisions an output recorded before #695 could have been derived from.
func (e *Explorer) RevisionPublications(
	ctx context.Context, documentIDs []shoal.ID,
) (map[shoal.ID]map[shoal.ID]time.Time, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	for _, documentID := range documentIDs {
		if err := shoal.ValidateRequiredID("document ID", documentID); err != nil {
			return nil, err
		}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return nil, err
	}
	published := make(map[shoal.ID]map[shoal.ID]time.Time, len(documentIDs))
	for _, documentID := range documentIDs {
		if _, done := published[documentID]; done {
			continue
		}
		revisions := make(map[shoal.ID]time.Time, len(e.documents[documentID]))
		for revisionID, record := range e.documents[documentID] {
			if record == nil || record.PublishedAt.IsZero() {
				continue
			}
			revisions[revisionID] = record.PublishedAt.UTC()
		}
		if len(revisions) > 0 {
			published[documentID] = revisions
		}
	}
	return published, nil
}

// ConnectedEdgePublications reports, for each requested edge that an
// application connected (Connect), when it was first published. An edge that
// was never connected (an intrinsic, extracted or materialized edge), or whose
// record carries no publication time, is omitted from the result; connected
// reports which of the requested edges are connected edges at all.
func (e *Explorer) ConnectedEdgePublications(
	ctx context.Context, edgeIDs []shoal.ID,
) (published map[shoal.ID]time.Time, connected map[shoal.ID]bool, err error) {
	if err := contextError(ctx); err != nil {
		return nil, nil, err
	}
	for _, edgeID := range edgeIDs {
		if err := shoal.ValidateRequiredID("graph edge ID", edgeID); err != nil {
			return nil, nil, err
		}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return nil, nil, err
	}
	published = make(map[shoal.ID]time.Time, len(edgeIDs))
	connected = make(map[shoal.ID]bool, len(edgeIDs))
	for _, edgeID := range edgeIDs {
		record, ok := e.edges[edgeID]
		if !ok {
			continue
		}
		connected[edgeID] = true
		if !record.PublishedAt.IsZero() {
			published[edgeID] = record.PublishedAt.UTC()
		}
	}
	return published, connected, nil
}
