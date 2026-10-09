// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

// Package evidencelabels holds the one definition of "may this reader see
// evidence carrying these visibility labels" for evidence that is *stored*
// rather than scanned.
//
// These labels are enforced nowhere else, and the reason first given for this
// package said otherwise. It said label enforcement in this plane happens at
// the scan — that a document a reader may not see does not come back from
// storage, so no service-layer filter was needed, and an evidence reference
// merely escaped that.
//
// That does not hold for the free-form labels an evidence reference carries.
// interaction.PropertyVisibility ("shoal.visibility") is written into a
// node's *properties* by explorer.setVisibility, reached from parse.go, and
// never into a cell visibility — so the scan has nothing to filter on and
// internal/visfilter never sees it. Authorized reads gate on the access
// rule's domain, source and policy, so a reader granted a source receives
// every labelled document, span and node in it, label included (#570).
//
// So Filter is not restoring a check that existed at a lower layer. It is the
// only check, which makes the nil case load-bearing rather than defensive: a
// deployment with no Visibility withholds labelled evidence precisely because
// nothing else would.
//
// An evidence reference recorded on another record — a dispatch action
// (#369), or the lifecycle event published for it (#562) — was recorded by
// that record's principal and is stored as a field of it, so it is returned
// to whoever may read the record rather than to whoever holds its labels.
// Every such path filters through Filter here, so the rule cannot drift
// between them.
//
// A leaf package on purpose: the dispatch plane and the event plane both
// import it, and neither imports the other for it.
package evidencelabels

import (
	"context"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Visibility answers whether the current reader holds the labels an evidence
// reference carries.
//
// Implemented by the host that owns the request's decision:
// authorized.LabelVisibility (#564) decides structured grant labels against
// the reader's own auth.Decision and, for a trusted service, its configured
// ceiling. A free-form label is held by nobody, which is why Translator
// exists.
type Visibility interface {
	// VisibleToReader reports whether the reader behind ctx holds the labels
	// in a visibility expression. An error is not a false answer: it is the
	// question failing, and the caller returns it rather than silently
	// withholding.
	VisibleToReader(ctx context.Context, visibility []string) (bool, error)
}

// Translator rewrites, at record time, the visibility an evidence reference
// carries into the terms Visibility can decide.
//
// An executor reports the visibility of the nodes it retrieved as their
// shoal.visibility property: free-form ingest labels such as "secret". No
// decision holds such a label, so a reference stored with one is withheld
// from every reader, holders included. Since #570 each such label is enforced
// as a structured policy per (source, label) conjoined into the node's
// AccessRule, and that policy's grant labels (d:, s:, g:) are what a reader
// can be shown to hold. Translator replaces each free-form term with them.
//
// Implemented by the host that owns the policy catalog
// (authorized.LabelTranslator). A term it cannot translate exactly is kept as
// it was, so the reference stays withheld rather than opened.
type Translator interface {
	// StructuredVisibility returns visibility with every free-form term
	// replaced by the structured terms of the label policies that enforce it
	// on nodeIDs and the endpoints of edgeIDs. An error is the catalog read
	// failing, never a refusal.
	StructuredVisibility(
		ctx context.Context, nodeIDs, edgeIDs []shoal.ID, visibility []string,
	) ([]string, error)
}

// Filter returns the values the reader behind ctx may see, in order, and
// whether anything was withheld. labels returns a value's visibility
// expression.
//
// The rule, which is the whole of #369's decided shape:
//
//   - A value with no visibility expression is kept. It carries no label to
//     lack, so withholding it would redact something nothing asked to be
//     protected — and that is what keeps this from gutting the grounding every
//     deployment without labels relies on.
//   - A nil evaluator withholds every labelled value. A plane that cannot
//     evaluate a label must not hand out the identifiers the label exists to
//     protect.
//   - A labelled value the evaluator says the reader may not see is dropped
//     whole. There is no count of what was dropped, and the caller must not
//     return one: a count that disagrees with the list is an existence oracle
//     for the rest (#398). A partial list is therefore not a completeness
//     claim — a reader cannot tell "recorded no evidence" from "recorded
//     evidence you may not see".
//   - An evaluator error is returned, not treated as a redaction, so a
//     transient fault is not indistinguishable from a permanent refusal.
//
// values is never modified. When nothing is withheld the input slice itself is
// returned with withheld false.
func Filter[T any](
	ctx context.Context, evaluator Visibility, values []T, labels func(T) []string,
) (kept []T, withheld bool, err error) {
	if len(values) == 0 {
		return values, false, nil
	}
	readable := make([]T, 0, len(values))
	for _, value := range values {
		visibility := labels(value)
		if len(visibility) == 0 {
			readable = append(readable, value)
			continue
		}
		if evaluator == nil {
			// Nothing can evaluate the label, so nothing may be shown it.
			continue
		}
		visible, err := evaluator.VisibleToReader(ctx, visibility)
		if err != nil {
			return nil, false, err
		}
		if visible {
			readable = append(readable, value)
		}
	}
	if len(readable) == len(values) {
		return values, false, nil
	}
	return readable, true, nil
}

// NodeGate answers whether the reader behind ctx may see a stored evidence
// reference's graph members under their CURRENT access rules: every node,
// every edge's effective rule, and both endpoints of every edge. It is the
// gate interaction reads already apply to their touched nodes and edges
// (#567), offered to the planes that store evidence references, so that a
// document relabelled after an action recorded evidence from it governs that
// evidence as it governs the document. Implemented by the host that owns the
// policy catalog (authorized.NodeGate).
type NodeGate interface {
	// GraphVisibleToReader reports whether the reader may see every node and
	// edge. A node or edge the catalog does not know is not visible. An
	// error is the question failing, never a refusal.
	GraphVisibleToReader(
		ctx context.Context, nodeIDs, edgeIDs []shoal.ID,
	) (bool, error)
	// PathJoins reports whether edgeIDs join nodeIDs in sequence, edge i
	// running from node i to node i+1, as the catalog records them. Used at
	// record time to refuse a graph reference whose edges are not its path.
	PathJoins(ctx context.Context, nodeIDs, edgeIDs []shoal.ID) (bool, error)
}

// FilterReferences is the per-reference rule for stored evidence (#564):
//
//   - A reference that names graph members (nodes or edges; every assertion
//     is on one of its edges) is decided by their current rules, through gate. Its
//     stored visibility is provenance only and is not consulted: after a
//     relabel it is wrong in both directions, admitting too much once a label
//     was added and withholding too much once one was removed. A nil gate
//     withholds it.
//   - A reference that names nothing has nothing current to consult, so it is
//     decided by its stored labels exactly as Filter decides them.
//
// Withheld references are dropped whole with no count, and an error is
// returned rather than read as a refusal, as for Filter. values is never
// modified, and when nothing is withheld the input slice itself is returned.
func FilterReferences[T any](
	ctx context.Context, visibility Visibility, gate NodeGate, values []T,
	labels func(T) []string, members func(T) (nodeIDs, edgeIDs []shoal.ID),
) (kept []T, withheld bool, err error) {
	if len(values) == 0 {
		return values, false, nil
	}
	readable := make([]T, 0, len(values))
	for _, value := range values {
		nodes, edges := members(value)
		if len(nodes) > 0 || len(edges) > 0 {
			if gate == nil {
				continue
			}
			visible, err := gate.GraphVisibleToReader(ctx, nodes, edges)
			if err != nil {
				return nil, false, err
			}
			if visible {
				readable = append(readable, value)
			}
			continue
		}
		// No graph member to consult: the stored labels are all there is.
		single, _, err := Filter(ctx, visibility, []T{value}, labels)
		if err != nil {
			return nil, false, err
		}
		readable = append(readable, single...)
	}
	if len(readable) == len(values) {
		return values, false, nil
	}
	return readable, true, nil
}
