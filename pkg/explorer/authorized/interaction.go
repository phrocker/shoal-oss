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
	"reflect"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// EnsureInteractionSink verifies both the caller's current authorization pin
// and the base corpus's durable write path. This makes *Client directly usable
// with harness.NewGraphRecorder without bypassing the authorization wrapper.
func (c *Client) EnsureInteractionSink(ctx context.Context) error {
	writer, err := c.interactionWriter()
	if err != nil {
		return err
	}
	_, guard, _, err := c.begin(ctx, auth.OperationRetrieve)
	if err != nil {
		return err
	}
	if err := writer.EnsureInteractionSink(ctx); err != nil {
		return directBaseError(err)
	}
	return guard.Check(ctx)
}

// RecordInteraction appends one redacted interaction after verifying that its
// pinned authorization is the exact current decision and that every source
// node it retrieved or cited is still authorized. A revoked or missing source
// fails the inference instead of creating an under-authorized record.
func (c *Client) RecordInteraction(
	ctx context.Context, session interaction.Session,
) error {
	_, err := c.recordInteraction(ctx, session)
	return err
}

// RecordInteractionResult records one interaction and returns the exact
// trusted session accepted for persistence, including actor, delegation, and
// derived reason metadata supplied by the bound authorization Decision.
func (c *Client) RecordInteractionResult(
	ctx context.Context, session interaction.Session,
) (interaction.Session, error) {
	return c.recordInteraction(ctx, session)
}

func (c *Client) recordInteraction(
	ctx context.Context, session interaction.Session,
) (interaction.Session, error) {
	writer, err := c.interactionWriter()
	if err != nil {
		return interaction.Session{}, err
	}
	canonical, err := session.Canonical()
	if err != nil {
		return interaction.Session{}, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRetrieve)
	if err != nil {
		return interaction.Session{}, err
	}
	if !interactionPinMatchesDecision(canonical, decision, now) {
		return interaction.Session{}, authorizationDenied()
	}
	if isNilDependency(c.snapshotSource) {
		return interaction.Session{}, shoal.NewError(
			shoal.ErrorUnavailable,
			"trusted interaction snapshot reader is unavailable",
		)
	}
	snapshot, err := c.snapshotSource.Snapshot(ctx)
	if err != nil {
		return interaction.Session{}, directBaseError(err)
	}
	if canonical.SnapshotID != shoal.ID(snapshot.ID) ||
		!canonical.SnapshotAsOf.UTC().Equal(snapshot.AsOf.UTC()) {
		return interaction.Session{}, shoal.NewError(
			shoal.ErrorConflict,
			"interaction observed snapshot is no longer current",
		)
	}
	canonical.Actor = interaction.ActorContext{
		SubjectID:  decision.Subject(),
		ActorID:    decision.Actor(),
		ClientID:   decision.ClientID(),
		OnBehalfOf: decision.OnBehalfOf(),
	}
	canonical.Reason = interaction.Reason{}
	if decision.AuditPurpose() != "" {
		canonical.Reason, err = interaction.NewReason(
			"audit_purpose", decision.AuditPurpose())
		if err != nil {
			return interaction.Session{}, authorizationDenied()
		}
	}
	canonical, err = canonical.Canonical()
	if err != nil {
		return interaction.Session{}, err
	}
	if err := c.authorizeInteractionSources(
		ctx, canonical.TouchedNodeIDs(), decision, auth.OperationRetrieve, now,
	); err != nil {
		return interaction.Session{}, err
	}
	if err := guard.Check(ctx); err != nil {
		return interaction.Session{}, err
	}
	persisted := canonical
	if resultWriter, ok := writer.(interaction.ResultSink); ok {
		persisted, err = resultWriter.RecordInteractionResult(ctx, canonical)
	} else {
		err = writer.RecordInteraction(ctx, canonical)
	}
	if err != nil {
		return interaction.Session{}, directBaseError(err)
	}
	if err := guard.Check(ctx); err != nil {
		return interaction.Session{}, explorer.MarkCommittedInteraction(err)
	}
	if _, ok := writer.(interaction.ResultSink); ok {
		returned, canonicalErr := persisted.Canonical()
		if canonicalErr != nil || !reflect.DeepEqual(returned, canonical) {
			return interaction.Session{}, explorer.MarkCommittedInteraction(
				shoal.NewError(
					shoal.ErrorInternal,
					"durable interaction sink returned a different record",
				),
			)
		}
	}
	return canonical, nil
}

// Interactions lists only derived records whose complete current source set
// the caller may read. Tombstones have intentionally discarded their source
// edges, so they are visible only to the exact authorization projection that
// created the original record.
func (c *Client) Interactions(
	ctx context.Context,
) ([]explorer.InteractionSummary, error) {
	records, err := c.InteractionRecords(ctx)
	if err != nil {
		return nil, err
	}
	summaries := make([]explorer.InteractionSummary, len(records))
	for index, record := range records {
		summaries[index] = record.Summary
	}
	return summaries, nil
}

// InteractionRecords returns authorized interaction summaries and provenance
// in one base read and one batched policy lookup.
func (c *Client) InteractionRecords(
	ctx context.Context,
) ([]explorer.InteractionRecord, error) {
	reader, err := c.interactionReader()
	if err != nil {
		return nil, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return nil, err
	}
	records, err := reader.InteractionRecords(ctx)
	if err != nil {
		return nil, directBaseError(err)
	}
	allNodeIDs := make([]shoal.ID, 0)
	for _, record := range records {
		if !record.Summary.Deleted {
			allNodeIDs = append(allNodeIDs, record.TouchedNodeIDs...)
		}
	}
	registrations, err := c.resolveNodes(ctx, allNodeIDs)
	if err != nil {
		return nil, err
	}
	visible := make([]explorer.InteractionRecord, 0, len(records))
	for _, record := range records {
		if record.Summary.Deleted {
			if summaryFingerprintMatchesDecision(record.Summary, decision) {
				visible = append(visible, record)
			}
			continue
		}
		if len(record.TouchedNodeIDs) == 0 {
			if summaryFingerprintMatchesDecision(record.Summary, decision) {
				visible = append(visible, record)
			}
			continue
		}
		allowed, err := interactionSourcesAllow(
			registrations, record.TouchedNodeIDs,
			decision, auth.OperationRead, now,
		)
		if err != nil {
			return nil, err
		}
		if allowed {
			visible = append(visible, record)
		}
	}
	if err := guard.Check(ctx); err != nil {
		return nil, err
	}
	return visible, nil
}

// InteractionRecord returns one authorized point record without scanning the
// complete interaction history.
func (c *Client) InteractionRecord(
	ctx context.Context, sessionID shoal.ID,
) (explorer.InteractionRecord, error) {
	reader, err := c.interactionReader()
	if err != nil {
		return explorer.InteractionRecord{}, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return explorer.InteractionRecord{}, err
	}
	record, err := reader.InteractionRecord(ctx, sessionID)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnavailable) ||
			shoal.IsErrorCode(err, shoal.ErrorConflict) {
			return explorer.InteractionRecord{}, auth.ObjectNotFound()
		}
		return explorer.InteractionRecord{}, directBaseError(err)
	}
	if record.Summary.Deleted {
		if !summaryFingerprintMatchesDecision(record.Summary, decision) {
			return explorer.InteractionRecord{}, auth.ObjectNotFound()
		}
	} else if len(record.TouchedNodeIDs) == 0 {
		if !summaryFingerprintMatchesDecision(record.Summary, decision) {
			return explorer.InteractionRecord{}, auth.ObjectNotFound()
		}
	} else if err := c.authorizeInteractionSources(
		ctx, record.TouchedNodeIDs, decision, auth.OperationRead, now,
	); err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) ||
			shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			return explorer.InteractionRecord{}, auth.ObjectNotFound()
		}
		return explorer.InteractionRecord{}, err
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.InteractionRecord{}, err
	}
	return record, nil
}

// Interaction returns one authorized typed interaction. It is an explicit
// derived view and therefore cannot affect the source-only retrieval surface.
func (c *Client) Interaction(
	ctx context.Context, sessionID shoal.ID,
) (interaction.Session, error) {
	record, err := c.InteractionRecord(ctx, sessionID)
	if err != nil {
		return interaction.Session{}, err
	}
	if record.Summary.Deleted || record.Session.ID == "" {
		return interaction.Session{}, auth.ObjectNotFound()
	}
	return record.Session, nil
}

// InteractionSubgraph returns an authorized explicit graph view. Every
// touched source is re-authorized before any derived node or edge is returned.
func (c *Client) InteractionSubgraph(
	ctx context.Context, sessionID shoal.ID,
) (explorer.Neighborhood, error) {
	reader, err := c.interactionReader()
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	record, err := reader.InteractionRecord(ctx, sessionID)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnavailable) ||
			shoal.IsErrorCode(err, shoal.ErrorConflict) {
			return explorer.Neighborhood{}, auth.ObjectNotFound()
		}
		return explorer.Neighborhood{}, directBaseError(err)
	}
	if record.Summary.Deleted {
		if !summaryFingerprintMatchesDecision(record.Summary, decision) {
			return explorer.Neighborhood{}, auth.ObjectNotFound()
		}
		subgraph, readErr := reader.InteractionSubgraph(ctx, sessionID)
		if readErr != nil {
			return explorer.Neighborhood{}, directBaseError(readErr)
		}
		if err := guard.Check(ctx); err != nil {
			return explorer.Neighborhood{}, err
		}
		return subgraph, nil
	}
	if len(record.TouchedNodeIDs) == 0 &&
		!summaryFingerprintMatchesDecision(record.Summary, decision) {
		return explorer.Neighborhood{}, auth.ObjectNotFound()
	}
	if err := c.authorizeInteractionSources(
		ctx, record.TouchedNodeIDs, decision, auth.OperationRead, now,
	); err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
			return explorer.Neighborhood{}, auth.ObjectNotFound()
		}
		return explorer.Neighborhood{}, err
	}
	subgraph, err := reader.InteractionSubgraph(ctx, sessionID)
	if err != nil {
		return explorer.Neighborhood{}, directBaseError(err)
	}
	if interactionSubgraphIsTombstone(subgraph) &&
		!summaryFingerprintMatchesDecision(record.Summary, decision) {
		return explorer.Neighborhood{}, auth.ObjectNotFound()
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.Neighborhood{}, err
	}
	return subgraph, nil
}

func interactionSubgraphIsTombstone(subgraph explorer.Neighborhood) bool {
	return len(subgraph.Nodes) == 1 &&
		subgraph.Nodes[0].Kind == interaction.KindTombstone
}

func (c *Client) authorizeInteractionSources(
	ctx context.Context,
	nodeIDs []shoal.ID,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) error {
	registrations, err := c.resolveNodes(ctx, nodeIDs)
	if err != nil {
		return err
	}
	allowed, err := interactionSourcesAllow(
		registrations, nodeIDs, decision, operation, now)
	if err != nil {
		return err
	}
	if !allowed {
		return auth.ObjectNotFound()
	}
	return nil
}

func interactionSourcesAllow(
	registrations registeredNodes,
	nodeIDs []shoal.ID,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (bool, error) {
	for _, nodeID := range nodeIDs {
		registration, ok := registrations[nodeID]
		if !ok {
			return false, nil
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, operation, now)
		if err != nil {
			return false, err
		}
		if !allowed {
			return false, nil
		}
	}
	return true, nil
}

func interactionPinMatchesDecision(
	session interaction.Session, decision auth.Decision, now time.Time,
) bool {
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return false
	}
	// A runtime pin may deliberately be shorter than its enclosing credential,
	// but it must still be live now and may never outlive that credential.
	return session.AuthorizationFingerprint == shoal.ID(fingerprint.String()) &&
		now.Before(session.AuthorizationExpiresAt) &&
		!decision.AuthenticationExpires().Before(session.AuthorizationExpiresAt)
}

func summaryFingerprintMatchesDecision(
	summary explorer.InteractionSummary, decision auth.Decision,
) bool {
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return false
	}
	return summary.AuthorizationFingerprint == shoal.ID(fingerprint.String())
}

func (c *Client) interactionWriter() (explorer.InteractionWriter, error) {
	if isNilDependency(c.interactionSink) {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"trusted interaction writer is unavailable",
		)
	}
	return c.interactionSink, nil
}

func (c *Client) interactionReader() (explorer.InteractionReader, error) {
	if isNilDependency(c.interactionSource) {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"trusted interaction reader is unavailable",
		)
	}
	return c.interactionSource, nil
}

var (
	_ explorer.InteractionWriter       = (*Client)(nil)
	_ explorer.InteractionResultWriter = (*Client)(nil)
	_ explorer.InteractionReader       = (*Client)(nil)
)
