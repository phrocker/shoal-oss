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
	"strconv"

	"github.com/phrocker/shoal-oss/internal/labelmigration"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// LabelMigrationVersion is the version of the startup label migration
// (#570). A catalog whose marker records this version or later is not
// migrated again.
const LabelMigrationVersion uint32 = 1

// maxReportedLabelBytes bounds the raw label kept in the report before it is
// escaped, so one pathological property cannot flood the operator's log.
const maxReportedLabelBytes = 512

// LabelMigrationRecord is the label-migration marker and its report. It is
// stored in the policy catalog only after every document has been migrated,
// so its presence means the run completed.
type LabelMigrationRecord struct {
	Version uint32
	// Documents is every base document examined.
	Documents int
	// Unregistered documents have no catalog registration and are already
	// readable by nobody.
	Unregistered int
	// Unlabelled documents carry no label and keep their rule.
	Unlabelled int
	// Tightened documents had at least one registration narrowed.
	Tightened int
	// AlreadyTightened documents carried their labels already (ingested
	// after labels were enforced, or migrated by an interrupted run).
	AlreadyTightened int
	// HistoricalTightened counts historical revisions narrowed by their own
	// labels, beyond the current revision's.
	HistoricalTightened int
	// Untranslatable lists every document (or historical revision) whose
	// labels could not be translated; each was conjoined with the reserved,
	// never-grantable UntranslatableLabelPolicyID.
	Untranslatable []UntranslatableLabel
}

// UntranslatableLabel is one report entry. EscapedLabel is the stored label
// value, truncated and Go-quoted (strconv.QuoteToASCII), so it is safe to
// print whatever bytes it holds.
type UntranslatableLabel struct {
	DocumentID   shoal.ID
	RevisionID   shoal.ID
	SourceURI    string
	EscapedLabel string
	Reason       string
}

func (r LabelMigrationRecord) clone() LabelMigrationRecord {
	r.Untranslatable = append([]UntranslatableLabel(nil), r.Untranslatable...)
	return r
}

// MigrateLabelledDocuments narrows the catalog rule of every document that
// was labelled before labels were enforced (#570). Such a document carries
// the bare source rule plus the free-form shoal.visibility property, so any
// holder of the source can read it. "Refuse at the next write" (#544's model)
// would leave it open until someone happens to write it, so this runs at
// startup instead, before anything is served.
//
// For each base document it reads shoal.visibility from the stored document
// node (and checks it against the revision's own metadata), parses it with
// interaction.ParseVisibility, builds LabelRule(sourcePolicy, labels), and
// calls PolicyStore.TightenRule, which rewrites every revision, the current
// projections, the extracted nodes and edges, and the source claim. Each
// historical revision is then narrowed by its own labels too, which may
// differ from the current revision's.
//
// A label set that cannot be translated (it does not parse, falls outside the
// charset, or exceeds a length, term or byte bound) is not an error: the
// document is conjoined with UntranslatablePolicy instead, so it and
// everything derived from it is readable by nobody, and it is listed in the
// report. Any store or base error aborts the run, and the caller must refuse
// to serve.
//
// The run holds the client's mutation lock and the store's mutation lease
// throughout. The marker (PutLabelMigration) is written last, so an
// interrupted run is simply repeated on the next start; every step is
// idempotent. Once the marker records LabelMigrationVersion, later calls
// return it with ran false and do nothing.
func (c *Client) MigrateLabelledDocuments(
	ctx context.Context,
	capability *labelmigration.Capability,
) (record LabelMigrationRecord, ran bool, err error) {
	if !capability.Granted() {
		return LabelMigrationRecord{}, false, shoal.NewError(
			shoal.ErrorUnauthorized,
			"the label migration requires the internal migration capability")
	}
	if err := contextFailure(ctx); err != nil {
		return LabelMigrationRecord{}, false, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	lease, err := c.policyStore.AcquireMutation(ctx)
	if err != nil {
		return LabelMigrationRecord{}, false, err
	}
	defer lease.Release()

	existing, done, err := c.policyStore.LabelMigration(ctx)
	if err != nil {
		return LabelMigrationRecord{}, false, err
	}
	if done && existing.Version >= LabelMigrationVersion {
		return existing, false, nil
	}
	summaries, err := c.base.Documents(ctx)
	if err != nil {
		return LabelMigrationRecord{}, false, err
	}
	sort.Slice(summaries, func(left, right int) bool {
		return shoal.CompareID(
			summaries[left].Document.ID, summaries[right].Document.ID) < 0
	})
	record = LabelMigrationRecord{Version: LabelMigrationVersion}
	for _, summary := range summaries {
		if err := validateSummary(summary); err != nil {
			return LabelMigrationRecord{}, false, inconsistentBase()
		}
		record.Documents++
		if err := c.migrateDocument(ctx, summary, &record); err != nil {
			return LabelMigrationRecord{}, false, err
		}
	}
	if err := c.policyStore.PutLabelMigration(ctx, record); err != nil {
		return LabelMigrationRecord{}, false, err
	}
	c.invalidateAuthorizedVectorAvailability()
	return record, true, nil
}

func (c *Client) migrateDocument(
	ctx context.Context,
	summary explorer.DocumentSummary,
	record *LabelMigrationRecord,
) error {
	documentID := summary.Document.ID
	current, registered, err := c.policyStore.CurrentRevision(ctx, documentID)
	if err != nil {
		return err
	}
	if !registered {
		record.Unregistered++
		return nil
	}
	labels, raw, reason, err := c.currentRevisionLabels(
		ctx, documentID, current.RevisionID)
	if err != nil {
		return err
	}
	outcome, err := tightenRevision(
		ctx, c.policyStore, current, summary.SourceURI, labels, reason)
	if err != nil {
		return err
	}
	if outcome.untranslatable != "" {
		record.Untranslatable = append(record.Untranslatable, UntranslatableLabel{
			DocumentID:   documentID,
			RevisionID:   current.RevisionID,
			SourceURI:    summary.SourceURI,
			EscapedLabel: escapeLabel(raw),
			Reason:       outcome.untranslatable,
		})
	}
	switch {
	case outcome.unlabelled:
		record.Unlabelled++
	case outcome.changed:
		record.Tightened++
	default:
		record.AlreadyTightened++
	}

	// Historical revisions, read after the sweep above so their rules
	// include the current revision's labels, are narrowed by their own.
	revisions, err := c.policyStore.DocumentRevisions(ctx, documentID)
	if err != nil {
		return err
	}
	for _, revision := range revisions {
		if revision.RevisionID == current.RevisionID {
			continue
		}
		view, err := c.base.Document(ctx, documentID, revision.RevisionID)
		if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			// The base no longer serves this revision, so nothing of it can
			// be read; it already carries the current labels.
			continue
		}
		if err != nil {
			return err
		}
		raw := view.Document.Metadata[interaction.PropertyVisibility]
		labels, parseErr := interaction.ParseVisibility(raw)
		reason := ""
		if parseErr != nil {
			reason = "revision visibility does not parse: " + parseErr.Error()
		}
		outcome, err := tightenRevision(ctx, c.policyStore, revision, "", labels, reason)
		if err != nil {
			return err
		}
		if outcome.changed {
			record.HistoricalTightened++
		}
		if outcome.untranslatable != "" {
			record.Untranslatable = append(record.Untranslatable, UntranslatableLabel{
				DocumentID:   documentID,
				RevisionID:   revision.RevisionID,
				SourceURI:    summary.SourceURI,
				EscapedLabel: escapeLabel(raw),
				Reason:       outcome.untranslatable,
			})
		}
	}
	return nil
}

type tightenOutcome struct {
	changed        bool
	unlabelled     bool
	untranslatable string
}

// tightenRevision derives the labelled rule for one revision registration and
// applies it through TightenRule. A non-empty reason, or a label set
// LabelRule refuses, conjoins UntranslatablePolicy instead.
//
// TightenRule is called whenever the revision carries any label policy, even
// when its own rule already has them all: the call then sweeps the derived
// registrations again from the bare source rule, which completes a durable
// write a crash tore and is otherwise a no-op.
func tightenRevision(
	ctx context.Context,
	store PolicyStore,
	revision RevisionRegistration,
	sourceURI string,
	labels []string,
	reason string,
) (tightenOutcome, error) {
	bare, anchor, sources, err := splitLabelledRule(revision.Rule)
	if err != nil {
		return tightenOutcome{}, err
	}
	var outcome tightenOutcome
	var target AccessRule
	if reason == "" && len(labels) > 0 && sources != 1 {
		reason = "the document rule has " + strconv.Itoa(sources) +
			" source policies, so a label cannot be bound to one"
	}
	if reason == "" {
		labelled, labelErr := LabelRule(anchor, labels)
		if labelErr == nil {
			target, labelErr = conjoinRules(revision.Rule, labelled)
		}
		if labelErr != nil {
			reason = "labels cannot be translated: " + labelErr.Error()
		}
	}
	if reason != "" {
		untranslatable, policyErr := UntranslatablePolicy(anchor)
		if policyErr != nil {
			return tightenOutcome{}, policyErr
		}
		components := append(revision.Rule.components(), untranslatable)
		target, err = NewAccessRule(components...)
		if err != nil {
			return tightenOutcome{}, err
		}
		outcome.untranslatable = reason
	}
	from := revision.Rule
	if target.equal(revision.Rule) {
		if bare.equal(revision.Rule) {
			outcome.unlabelled = true
			return outcome, nil
		}
		// Already labelled: sweep from the bare rule.
		from = bare
	}
	outcome.changed, err = store.TightenRule(
		ctx, revision.DocumentID, revision.RevisionID, sourceURI, from, target)
	if err != nil {
		return tightenOutcome{}, err
	}
	return outcome, nil
}

// splitLabelledRule separates a rule into its non-label components (the
// bare rule), the first of them (the anchor label policies derive from), and
// how many there are. A rule made only of label policies is refused: no
// writer produces one.
func splitLabelledRule(rule AccessRule) (AccessRule, auth.Policy, int, error) {
	var sources []auth.Policy
	for _, policy := range rule.components() {
		if auth.IsLabelPolicyID(policy.GrantPolicyID()) {
			continue
		}
		sources = append(sources, policy)
	}
	if len(sources) == 0 {
		return AccessRule{}, auth.Policy{}, 0, shoal.NewError(
			shoal.ErrorInternal,
			"a catalog rule holds no source policy, only label policies")
	}
	bare, err := NewAccessRule(sources...)
	if err != nil {
		return AccessRule{}, auth.Policy{}, 0, err
	}
	return bare, sources[0], len(sources), nil
}

// currentRevisionLabels reads the labels of the current revision from the
// stored document node, where parse.go put them, and requires the revision's
// own metadata to declare the same set. Anything that prevents a confident
// reading is returned as a reason (the document becomes untranslatable), with
// the raw value for the report; only a base failure is an error.
func (c *Client) currentRevisionLabels(
	ctx context.Context,
	documentID, revisionID shoal.ID,
) (labels []string, raw string, reason string, err error) {
	view, err := c.base.Document(ctx, documentID, revisionID)
	if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		return nil, "", "the base does not serve the registered current revision", nil
	}
	if err != nil {
		return nil, "", "", err
	}
	declared := view.Document.Metadata[interaction.PropertyVisibility]
	neighborhood, err := c.base.Neighborhood(ctx, explorer.NeighborhoodRequest{
		NodeIDs:   []shoal.ID{documentID},
		Depth:     1,
		EdgeTypes: []string{"contains"},
	})
	if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		return nil, declared, "the stored document node is missing", nil
	}
	if err != nil {
		return nil, "", "", err
	}
	for _, node := range neighborhood.Nodes {
		if node.ID != documentID {
			continue
		}
		raw = node.Properties[interaction.PropertyVisibility]
		if raw == "" {
			raw = declared
		}
		if node.Properties["revision_id"] != string(revisionID) {
			return nil, raw, "the stored document node names another revision", nil
		}
		labels, parseErr := interaction.NodeVisibility(node)
		if parseErr != nil {
			return nil, raw, "document node visibility does not parse: " +
				parseErr.Error(), nil
		}
		declaredLabels, parseErr := interaction.ParseVisibility(declared)
		if parseErr != nil {
			return nil, declared, "revision visibility does not parse: " +
				parseErr.Error(), nil
		}
		if interaction.Expression(labels) != interaction.Expression(declaredLabels) {
			return nil, raw, "the document node and the revision metadata " +
				"declare different labels", nil
		}
		return labels, raw, "", nil
	}
	return nil, declared, "the stored document node is missing", nil
}

func escapeLabel(raw string) string {
	if len(raw) > maxReportedLabelBytes {
		return strconv.QuoteToASCII(raw[:maxReportedLabelBytes]) + "..."
	}
	return strconv.QuoteToASCII(raw)
}
