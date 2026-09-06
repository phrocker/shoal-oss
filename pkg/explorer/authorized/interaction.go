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
	if !interactionPinMatchesDecision(canonical, decision) {
		return interaction.Session{}, authorizationDenied()
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
	return persisted, nil
}

// Interactions lists only derived records whose complete current source set
// the caller may read. Tombstones have intentionally discarded their source
// edges, so they are visible only to the exact authorization projection that
// created the original record.
func (c *Client) Interactions(
	ctx context.Context,
) ([]explorer.InteractionSummary, error) {
	reader, err := c.interactionReader()
	if err != nil {
		return nil, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return nil, err
	}
	summaries, err := reader.Interactions(ctx)
	if err != nil {
		return nil, directBaseError(err)
	}
	visible := make([]explorer.InteractionSummary, 0, len(summaries))
	for _, summary := range summaries {
		if summary.Deleted {
			if summaryFingerprintMatchesDecision(summary, decision) {
				visible = append(visible, summary)
			}
			continue
		}
		session, readErr := reader.Interaction(ctx, summary.SessionID)
		if readErr != nil {
			if shoal.IsErrorCode(readErr, shoal.ErrorNotFound) ||
				shoal.IsErrorCode(readErr, shoal.ErrorUnavailable) {
				continue
			}
			return nil, directBaseError(readErr)
		}
		if err := c.authorizeInteractionSources(
			ctx, session.TouchedNodeIDs(), decision, auth.OperationRead, now,
		); err != nil {
			if shoal.IsErrorCode(err, shoal.ErrorNotFound) ||
				shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
				continue
			}
			return nil, err
		}
		visible = append(visible, summary)
	}
	if err := guard.Check(ctx); err != nil {
		return nil, err
	}
	return visible, nil
}

// Interaction returns one authorized typed interaction. It is an explicit
// derived view and therefore cannot affect the source-only retrieval surface.
func (c *Client) Interaction(
	ctx context.Context, sessionID shoal.ID,
) (interaction.Session, error) {
	reader, err := c.interactionReader()
	if err != nil {
		return interaction.Session{}, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return interaction.Session{}, err
	}
	session, err := reader.Interaction(ctx, sessionID)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnavailable) ||
			shoal.IsErrorCode(err, shoal.ErrorConflict) {
			return interaction.Session{}, auth.ObjectNotFound()
		}
		return interaction.Session{}, directBaseError(err)
	}
	if err := c.authorizeInteractionSources(
		ctx, session.TouchedNodeIDs(), decision, auth.OperationRead, now,
	); err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
			return interaction.Session{}, auth.ObjectNotFound()
		}
		return interaction.Session{}, err
	}
	if err := guard.Check(ctx); err != nil {
		return interaction.Session{}, err
	}
	return session, nil
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
	summaries, err := reader.Interactions(ctx)
	if err != nil {
		return explorer.Neighborhood{}, directBaseError(err)
	}
	var summary *explorer.InteractionSummary
	for index := range summaries {
		if summaries[index].SessionID == sessionID {
			summary = &summaries[index]
			break
		}
	}
	if summary == nil {
		return explorer.Neighborhood{}, auth.ObjectNotFound()
	}
	if summary.Deleted {
		if !summaryFingerprintMatchesDecision(*summary, decision) {
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
	session, err := reader.Interaction(ctx, sessionID)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
			return explorer.Neighborhood{}, auth.ObjectNotFound()
		}
		return explorer.Neighborhood{}, directBaseError(err)
	}
	if err := c.authorizeInteractionSources(
		ctx, session.TouchedNodeIDs(), decision, auth.OperationRead, now,
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
	if err := guard.Check(ctx); err != nil {
		return explorer.Neighborhood{}, err
	}
	return subgraph, nil
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
	for _, nodeID := range nodeIDs {
		registration, ok := registrations[nodeID]
		if !ok {
			return auth.ObjectNotFound()
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, operation, now)
		if err != nil {
			return err
		}
		if !allowed {
			return auth.ObjectNotFound()
		}
	}
	return nil
}

func interactionPinMatchesDecision(
	session interaction.Session, decision auth.Decision,
) bool {
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return false
	}
	return session.AuthorizationFingerprint == shoal.ID(fingerprint.String()) &&
		decision.AuthenticationExpires().Equal(session.AuthorizationExpiresAt)
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
	writer, ok := c.base.(explorer.InteractionWriter)
	if !ok || isNilDependency(writer) {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"underlying Explorer has no durable interaction writer",
		)
	}
	return writer, nil
}

func (c *Client) interactionReader() (explorer.InteractionReader, error) {
	reader, ok := c.base.(explorer.InteractionReader)
	if !ok || isNilDependency(reader) {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"underlying Explorer has no interaction reader",
		)
	}
	return reader, nil
}

var (
	_ explorer.InteractionWriter       = (*Client)(nil)
	_ explorer.InteractionResultWriter = (*Client)(nil)
	_ explorer.InteractionReader       = (*Client)(nil)
)
