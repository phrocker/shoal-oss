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

// Fleet graph evidence is bound to the registrations it was recorded over
// (#705).
//
// A fleet dispatch action (and the lifecycle event published for it) stores
// evidence references that name graph members: node and edge IDs with no
// citation. Document and root-section node IDs do not depend on the revision,
// so a new revision takes the registrations over. NodeGate used to decide such
// evidence on the CURRENT registrations alone, so evidence recorded over a
// labelled revision R1 opened to non-holders once the document was relabelled
// (the leak #695 closed for interactions).
//
// The fix is #703's, carried by the fleet record instead of the policy
// catalog:
//
//   - At record time the fleet plane calls NodeGate.BindGraphEvidence. It
//     resolves, for every node, edge and edge endpoint each reference names,
//     the (kind, document, revision) of its registration as the recorder sees
//     it (a shared extracted node or relation is projected onto the owner the
//     recorder reads it through, #700). The registrations are read before the
//     evidence is validated against the executor's pinned snapshot and again
//     after, and the union is the binding: whichever registration the catalog
//     named at either point is bound.
//   - The fleet plane stores each reference's encoded binding verbatim with the
//     reference, and passes it back on every read in evidencelabels.Graph's
//     Binding. No record time is needed: a bound reference does not use one,
//     and an unbound one is not given one (below).
//   - GraphsVisibleToReader keeps requiring every current rule it always
//     required, and adds what the binding records (below).
//
// What each recorded entry requires at read, by its kind:
//
//   - RegistrationDocument and RegistrationExtracted: revisionAllows on the
//     recorded revision, i.e. its own rule and its document's current rule.
//     An unregistered revision, or a document with no current revision,
//     denies.
//   - RegistrationMaterialized: nothing beyond the registration's own
//     (current) rule. Its (DocumentID, RevisionID) pair is a materialization
//     identity, not a document revision, so it is neither recorded nor sent
//     to the revision path: doing so would deny every reader (a team-overview
//     cliff).
//   - RegistrationApplication (edges only): nothing beyond the edge's own
//     effective rule, which already conjoins #703's Connect binding, and its
//     endpoints' rules, the endpoints being bound entries of their own.
//   - RegistrationUnspecified: denied. No catalog yields it: legacyNodeKind
//     and legacyEdgeKind map pre-kind durable records when they are loaded,
//     PutNode accepts only extracted and materialized nodes, and
//     normalization turns an unspecified edge into an application edge. A
//     member whose current registration is unspecified denies as well.
//
// Every member the graph names now (its nodes, its edges, and each edge's
// current endpoints) must be covered by at least one entry of a kind its
// current registration could have been recorded as (bindingCovers); an
// uncovered member denies.
//
// Evidence with no binding (every fleet record written before this change) is
// held to the rules of EVERY registered revision of each document its members
// now belong to, as #703 holds legacy application edges. It is not given a
// time bound like #703's legacy sessions, because no time on a fleet record
// is safe to bound by. The only candidate, the record's EvidenceSnapshotAsOf,
// is executor-supplied. The dispatch service refuses one later than the
// completion or the authorization's expiry, but nothing stops an earlier one:
//
//   - records written before #623 were never checked against a corpus
//     frontier;
//   - neither was any record written while the dispatch plane had no node gate;
//   - nor a record whose references are all document references
//     (GraphEvidenceValid checks graph references only).
//
// An understated time moves a sequence bound earlier and leaves out revisions
// published between it and the record, so a labelled revision the action did
// use would no longer be required once the document is relabelled.
// TestLegacyFleetEvidenceIgnoresAnUnderstatedRecordTime pins that.
//
// The one exception is a legacy document reference's members of the document
// it cites. They were recorded over the cited revision, which the gate already
// holds the reference to (revisionAllows on the citation), so the citation is
// their binding and no history is required for that document. That keeps
// current-revision citations open to their readers. Its residual: a reference
// citing an old revision that was public, R0, opens after the document is
// relabelled public again even if a labelled revision came between. R0's
// content is public, so what is exposed is only that the action cited it.
//
// A derived (latent) edge (#744) has no registration: it is resolved through
// its trusted derived assertion (resolveInteractionEdges), at record time
// (pathJoins, BindGraphEvidence) and at read time alike, and a reader sees it
// exactly when both of its endpoints are visible to them. It is bound by its
// endpoints alone: the binding
// records their node entries, with their (kind, document, revision), and no
// entry for the edge. The wire format is unchanged (version 1), deliberately:
//
//   - The edge has no kind, rule or revision of its own, so an entry for it
//     would record nothing a read could be held to. Everything it requires
//     is its endpoints' rules and revisions, which node entries carry, and a
//     relabel of either endpoint closes the evidence through them.
//   - It cannot change endpoints: its ID is its assertion's content-derived
//     ID, which names subject and object, and derivedInteractionEdge refuses
//     an assertion held under any other ID.
//   - A new entry kind would need version 2 (or a widened version 1 that a
//     binary built before it refuses, closing every new record it reads),
//     for no added check. Endpoint entries are what version 1 always
//     decoded, so every binding ever written still decodes the same.
//
// At read, coverage (graphMembers) takes a derived edge's endpoints as
// members, not the edge; its endpoints must be covered as any node is. An
// edge entry for an ID that now resolves only as derived denies (rules).
//
// Two consequences, both accepted:
//
//   - A derived edge's ID changes whenever its latent cell is rescored (the
//     derivation's score is part of the assertion's content). Fleet evidence
//     over the old edge then resolves nothing and goes dark for good, for
//     every reader, the holder included. That fails closed, exactly as #743
//     sessions over such an edge do.
//   - Fleet and event reads now depend on the base graph, through the
//     trusted derived assertion reader, for every batch naming an edge. A
//     sticky graph build error fails each such batch until the next
//     mutation, and the first fleet read after a latent write rebuilds the
//     graph under its write lock. Before #744 fleet reads read only the
//     policy catalog.
//
// A binding that does not decode is denied, never an error, exactly as an
// unknown member is: every denial is the same false verdict a never-recorded
// reference gets (#398).

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/evidencelabels"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// GraphBindingObject says whether a binding entry names a node or an edge.
type GraphBindingObject uint8

const (
	// GraphBindingNode names a graph node (a reference's node, or an
	// endpoint of one of its edges).
	GraphBindingNode GraphBindingObject = 1
	// GraphBindingEdge names a graph edge.
	GraphBindingEdge GraphBindingObject = 2
)

// GraphBindingEntry is one registration a graph evidence reference was
// recorded over.
type GraphBindingEntry struct {
	Object GraphBindingObject
	ID     shoal.ID
	Kind   RegistrationKind
	// DocumentID and RevisionID name the document revision the registration
	// belonged to. They are set exactly when Kind is RegistrationDocument or
	// RegistrationExtracted, and empty otherwise.
	DocumentID shoal.ID
	RevisionID shoal.ID
}

// GraphEvidenceBinding is the canonical binding set of one graph evidence
// reference: its entries validated, sorted and deduplicated. The zero value
// is the empty binding, which cannot be encoded: a reference that names no
// graph member has nothing to bind.
type GraphEvidenceBinding struct {
	entries []GraphBindingEntry
}

// graphEvidenceBindingVersion is the first byte of every encoded binding.
//
// Fleet records keep their bindings for as long as the record lives, so the
// decoder must accept EVERY version ever written: a newer encoding gets a new
// version byte and DecodeGraphEvidenceBinding keeps decoding the old ones.
// Refusing a stored version does not leak, because a binding that does not
// decode denies its reference, but it would close that evidence for good.
// TestGraphEvidenceBindingDecodesEveryVersionEverWritten pins version 1's
// exact bytes.
const graphEvidenceBindingVersion byte = 1

// maxGraphEvidenceBindingBytes bounds an encoded binding, so a corrupt
// stored length cannot make a read allocate without bound.
const maxGraphEvidenceBindingBytes = 1 << 22

// NewGraphEvidenceBinding validates entries and returns their canonical set.
func NewGraphEvidenceBinding(entries ...GraphBindingEntry) (GraphEvidenceBinding, error) {
	canonical := make([]GraphBindingEntry, 0, len(entries))
	for _, entry := range entries {
		if err := validateGraphBindingEntry(entry); err != nil {
			return GraphEvidenceBinding{}, err
		}
		canonical = append(canonical, entry)
	}
	sort.Slice(canonical, func(left, right int) bool {
		return compareGraphBindingEntries(canonical[left], canonical[right]) < 0
	})
	deduplicated := canonical[:0]
	for _, entry := range canonical {
		if len(deduplicated) > 0 && deduplicated[len(deduplicated)-1] == entry {
			continue
		}
		deduplicated = append(deduplicated, entry)
	}
	if len(deduplicated) == 0 {
		return GraphEvidenceBinding{}, nil
	}
	return GraphEvidenceBinding{entries: deduplicated}, nil
}

// Entries returns a copy of the canonical entries.
func (b GraphEvidenceBinding) Entries() []GraphBindingEntry {
	return append([]GraphBindingEntry(nil), b.entries...)
}

// Empty reports whether the binding has no entries.
func (b GraphEvidenceBinding) Empty() bool { return len(b.entries) == 0 }

// Union is the canonical union of two bindings.
func (b GraphEvidenceBinding) Union(other GraphEvidenceBinding) (GraphEvidenceBinding, error) {
	return NewGraphEvidenceBinding(append(b.Entries(), other.entries...)...)
}

// Encode returns the binding's versioned wire form, which is what a fleet
// record stores. The empty binding is refused.
//
// Layout (version 1): the version byte, a uvarint entry count, then per
// entry, in canonical order: the object byte, the kind byte, and the ID,
// document ID and revision ID, each as a uvarint length and its bytes.
func (b GraphEvidenceBinding) Encode() ([]byte, error) {
	if len(b.entries) == 0 {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument,
			"graph evidence binding is empty")
	}
	var out bytes.Buffer
	out.WriteByte(graphEvidenceBindingVersion)
	writeUvarint(&out, uint64(len(b.entries)))
	for _, entry := range b.entries {
		out.WriteByte(byte(entry.Object))
		out.WriteByte(byte(entry.Kind))
		for _, value := range []shoal.ID{entry.ID, entry.DocumentID, entry.RevisionID} {
			writeUvarint(&out, uint64(len(value)))
			out.WriteString(string(value))
		}
	}
	if out.Len() > maxGraphEvidenceBindingBytes {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument,
			"graph evidence binding is too large")
	}
	return out.Bytes(), nil
}

// DecodeGraphEvidenceBinding decodes a stored binding strictly: an unknown
// version, a truncated or trailing byte, an invalid entry, an empty set, or
// entries that are not in canonical order (sorted, no duplicates) are all
// refused, so a stored binding is exactly what Encode wrote.
func DecodeGraphEvidenceBinding(data []byte) (GraphEvidenceBinding, error) {
	corrupt := func() (GraphEvidenceBinding, error) {
		return GraphEvidenceBinding{}, shoal.NewError(shoal.ErrorInvalidArgument,
			"graph evidence binding is corrupt")
	}
	if len(data) == 0 || len(data) > maxGraphEvidenceBindingBytes ||
		data[0] != graphEvidenceBindingVersion {
		return corrupt()
	}
	reader := bytes.NewReader(data[1:])
	count, err := binary.ReadUvarint(reader)
	// Each entry takes at least five bytes, which bounds count by the input.
	if err != nil || count == 0 || count > uint64(reader.Len())/5 {
		return corrupt()
	}
	entries := make([]GraphBindingEntry, 0, count)
	for index := uint64(0); index < count; index++ {
		object, err := reader.ReadByte()
		if err != nil {
			return corrupt()
		}
		kind, err := reader.ReadByte()
		if err != nil {
			return corrupt()
		}
		var values [3]shoal.ID
		for position := range values {
			length, err := binary.ReadUvarint(reader)
			if err != nil || length > uint64(reader.Len()) {
				return corrupt()
			}
			value := make([]byte, length)
			if _, err := io.ReadFull(reader, value); err != nil {
				return corrupt()
			}
			values[position] = shoal.ID(value)
		}
		entry := GraphBindingEntry{
			Object: GraphBindingObject(object), Kind: RegistrationKind(kind),
			ID: values[0], DocumentID: values[1], RevisionID: values[2],
		}
		if err := validateGraphBindingEntry(entry); err != nil {
			return corrupt()
		}
		if len(entries) > 0 &&
			compareGraphBindingEntries(entries[len(entries)-1], entry) >= 0 {
			return corrupt()
		}
		entries = append(entries, entry)
	}
	if reader.Len() != 0 {
		return corrupt()
	}
	return GraphEvidenceBinding{entries: entries}, nil
}

func writeUvarint(out *bytes.Buffer, value uint64) {
	var scratch [binary.MaxVarintLen64]byte
	out.Write(scratch[:binary.PutUvarint(scratch[:], value)])
}

func validateGraphBindingEntry(entry GraphBindingEntry) error {
	invalid := func(message string) error {
		return shoal.NewError(shoal.ErrorInvalidArgument, message)
	}
	switch entry.Object {
	case GraphBindingNode:
		switch entry.Kind {
		case RegistrationUnspecified, RegistrationDocument,
			RegistrationExtracted, RegistrationMaterialized:
		default:
			return invalid("graph evidence binding node kind is invalid")
		}
	case GraphBindingEdge:
		switch entry.Kind {
		case RegistrationUnspecified, RegistrationDocument,
			RegistrationExtracted, RegistrationMaterialized,
			RegistrationApplication:
		default:
			return invalid("graph evidence binding edge kind is invalid")
		}
	default:
		return invalid("graph evidence binding object is invalid")
	}
	if err := shoal.ValidateRequiredID("graph evidence binding ID", entry.ID); err != nil {
		return err
	}
	if documentBound(entry.Kind) {
		if err := shoal.ValidateRequiredID(
			"graph evidence binding document ID", entry.DocumentID); err != nil {
			return err
		}
		return shoal.ValidateRequiredID(
			"graph evidence binding revision ID", entry.RevisionID)
	}
	if entry.DocumentID != "" || entry.RevisionID != "" {
		return invalid("graph evidence binding names a revision for a kind not bound to one")
	}
	return nil
}

func compareGraphBindingEntries(left, right GraphBindingEntry) int {
	switch {
	case left.Object != right.Object:
		if left.Object < right.Object {
			return -1
		}
		return 1
	}
	if compared := shoal.CompareID(left.ID, right.ID); compared != 0 {
		return compared
	}
	if left.Kind != right.Kind {
		if left.Kind < right.Kind {
			return -1
		}
		return 1
	}
	if compared := shoal.CompareID(left.DocumentID, right.DocumentID); compared != 0 {
		return compared
	}
	return shoal.CompareID(left.RevisionID, right.RevisionID)
}

// nodeBindingEntry is the entry a node registration contributes. A
// materialization identity is never recorded as a revision.
func nodeBindingEntry(id shoal.ID, registration NodeRegistration) GraphBindingEntry {
	entry := GraphBindingEntry{Object: GraphBindingNode, ID: id, Kind: registration.Kind}
	if reference, ok := nodeRevision(registration); ok {
		entry.DocumentID, entry.RevisionID = reference.DocumentID, reference.RevisionID
	} else if documentBound(registration.Kind) {
		// A document-bound kind with no revision cannot be vouched for.
		entry.Kind = RegistrationUnspecified
	}
	return entry
}

// edgeBindingEntry is nodeBindingEntry for an edge.
func edgeBindingEntry(id shoal.ID, registration EdgeRegistration) GraphBindingEntry {
	entry := GraphBindingEntry{Object: GraphBindingEdge, ID: id, Kind: registration.Kind}
	if reference, ok := edgeRevision(registration); ok {
		entry.DocumentID, entry.RevisionID = reference.DocumentID, reference.RevisionID
	} else if documentBound(registration.Kind) {
		entry.Kind = RegistrationUnspecified
	}
	return entry
}

// BindGraphEvidence is the record-time half of the graph evidence binding
// (#705), for the fleet dispatch plane. It replaces GraphEvidenceValid at
// record time: it reads the registrations every reference names, validates
// the references exactly as GraphEvidenceValid does, reads the registrations
// again, and returns, index for index with references, the encoded union of
// the two reads (nil for a reference that names no node and no edge).
//
// valid false (with a nil error and nil bindings) refuses the evidence, as
// GraphEvidenceValid's false does. An error is the question failing: the
// decision behind ctx cannot be resolved, a catalog read fails, or a reference
// names an invalid identifier.
//
// The recorder is the decision behind ctx: a shared extracted node or relation
// (#700) is bound to the owner that decision reads it through (or the first
// contributing owner when it reads none), never to another owner's revision.
// A member the catalog does not know contributes no entry, so a later read
// that finds it registered still denies the reference.
func (g *NodeGate) BindGraphEvidence(
	ctx context.Context, snapshotID shoal.ID, snapshotAsOf time.Time,
	references []interaction.EvidenceReference,
) (bindings [][]byte, valid bool, err error) {
	if g == nil || g.client == nil {
		return nil, false, shoal.NewError(
			shoal.ErrorUnavailable, "node gate is unavailable")
	}
	c := g.client
	if err := contextFailure(ctx); err != nil {
		return nil, false, err
	}
	for _, reference := range references {
		for _, id := range reference.NodeIDs {
			if err := shoal.ValidateRequiredID("evidence node ID", id); err != nil {
				return nil, false, err
			}
		}
		for _, id := range reference.EdgeIDs {
			if err := shoal.ValidateRequiredID("evidence edge ID", id); err != nil {
				return nil, false, err
			}
		}
	}
	decision, err := c.resolver.Resolve(ctx)
	if err != nil {
		return nil, false, resolverFailure(ctx, err)
	}
	recorder := readerFor(decision, auth.OperationRetrieve, c.clock())
	before, err := c.graphEvidenceBindings(ctx, references, recorder)
	if err != nil {
		return nil, false, err
	}
	valid, err = g.GraphEvidenceValid(ctx, snapshotID, snapshotAsOf, references)
	if err != nil || !valid {
		return nil, false, err
	}
	after, err := c.graphEvidenceBindings(ctx, references, recorder)
	if err != nil {
		return nil, false, err
	}
	bindings = make([][]byte, len(references))
	for index := range references {
		union, err := before[index].Union(after[index])
		if err != nil {
			return nil, false, inconsistentBase()
		}
		if union.Empty() {
			continue
		}
		encoded, err := union.Encode()
		if err != nil {
			return nil, false, err
		}
		bindings[index] = encoded
	}
	return bindings, true, nil
}

// graphEvidenceState is how one graph's recorded revisions are decided.
type graphEvidenceState uint8

const (
	// graphEvidenceLegacy: no binding was recorded; the legacy bound applies.
	graphEvidenceLegacy graphEvidenceState = iota
	// graphEvidenceBound: the recorded binding applies.
	graphEvidenceBound
	// graphEvidenceCorrupt: a binding was passed and does not decode; denied.
	graphEvidenceCorrupt
)

// graphEvidenceRevisions is what GraphsVisibleToReader requires of each graph
// beyond the current rules of its members, loaded once per batch.
type graphEvidenceRevisions struct {
	cache    *revisionRuleCache
	states   []graphEvidenceState
	bindings []GraphEvidenceBinding
}

// loadGraphEvidenceRevisions decodes every graph's binding and loads, in
// bounded batches, the revision rules they need: each bound graph's recorded
// revisions with their documents' current ones, and each legacy graph's
// documents' whole histories with their publications.
func (c *Client) loadGraphEvidenceRevisions(
	ctx context.Context,
	graphs []evidencelabels.Graph,
	nodes registeredNodes,
	edges interactionEdges,
) (*graphEvidenceRevisions, error) {
	loaded := &graphEvidenceRevisions{
		cache:    c.newRevisionRuleCache(),
		states:   make([]graphEvidenceState, len(graphs)),
		bindings: make([]GraphEvidenceBinding, len(graphs)),
	}
	var references []RevisionReference
	var documentIDs []shoal.ID
	for index, graph := range graphs {
		if len(graph.Binding) == 0 {
			loaded.states[index] = graphEvidenceLegacy
			documentIDs = append(documentIDs,
				legacyGraphDocuments(graph, nodes, edges)...)
			continue
		}
		binding, err := DecodeGraphEvidenceBinding(graph.Binding)
		if err != nil {
			loaded.states[index] = graphEvidenceCorrupt
			continue
		}
		loaded.states[index] = graphEvidenceBound
		loaded.bindings[index] = binding
		for _, entry := range binding.entries {
			if documentBound(entry.Kind) {
				references = append(references, RevisionReference{
					DocumentID: entry.DocumentID, RevisionID: entry.RevisionID,
				})
			}
		}
	}
	if err := loaded.cache.loadDocuments(ctx, documentIDs); err != nil {
		return nil, err
	}
	if err := loaded.cache.loadReferences(ctx, references); err != nil {
		return nil, err
	}
	return loaded, nil
}

// graphMember is one node or edge a graph names now: its nodes, its edges,
// and its edges' current endpoints.
type graphMember struct {
	object GraphBindingObject
	id     shoal.ID
}

// graphMembers lists a graph's members under the current registrations. The
// caller has already denied a graph naming an edge that resolves neither a
// registration nor a derived edge.
//
// A derived (latent) edge (#744) is not a member itself, only its endpoints
// are: it has no registration, so it has no kind, rule or revision a binding
// entry could record or a reader could be held to, and what it requires is
// exactly its endpoints' rules and revisions. Its endpoints are therefore
// members that must be covered by node entries, which BindGraphEvidence
// records for it. A derived edge's ID is its assertion's content-derived ID,
// never a registered edge's, so it cannot later resolve a registration
// that this would leave uncovered; if one ever did, it would be a member
// again, with no edge entry, and deny.
func graphMembers(graph evidencelabels.Graph, edges interactionEdges) []graphMember {
	members := make([]graphMember, 0, len(graph.NodeIDs)+3*len(graph.EdgeIDs))
	for _, id := range graph.NodeIDs {
		members = append(members, graphMember{GraphBindingNode, id})
	}
	for _, id := range graph.EdgeIDs {
		if registration, ok := edges.registered[id]; ok {
			members = append(members,
				graphMember{GraphBindingEdge, id},
				graphMember{GraphBindingNode, registration.Edge.From},
				graphMember{GraphBindingNode, registration.Edge.To})
			continue
		}
		if edge, ok := edges.derived[id]; ok {
			members = append(members,
				graphMember{GraphBindingNode, edge.From},
				graphMember{GraphBindingNode, edge.To})
			continue
		}
		// Unresolved: a member with no registration, which denies.
		members = append(members, graphMember{GraphBindingEdge, id})
	}
	return members
}

// memberRevision is the document revision a member's current (projected)
// registration belongs to, and its kind. ok is false for an unregistered
// member.
func memberRevision(
	member graphMember, nodes registeredNodes, edges interactionEdges,
) (kind RegistrationKind, reference RevisionReference, bound, ok bool) {
	switch member.object {
	case GraphBindingNode:
		registration, found := nodes[member.id]
		if !found {
			return 0, RevisionReference{}, false, false
		}
		reference, bound = nodeRevision(registration)
		return registration.Kind, reference, bound, true
	default:
		registration, found := edges.registered[member.id]
		if !found {
			return 0, RevisionReference{}, false, false
		}
		reference, bound = edgeRevision(registration)
		return registration.Kind, reference, bound, true
	}
}

// legacyGraphDocuments is every document a legacy graph's members now belong
// to, each shared extracted one through the owner the reader reads it by,
// except the document a document reference cites (see citesDocument).
func legacyGraphDocuments(
	graph evidencelabels.Graph, nodes registeredNodes, edges interactionEdges,
) []shoal.ID {
	var documentIDs []shoal.ID
	for _, member := range graphMembers(graph, edges) {
		if _, reference, bound, ok := memberRevision(member, nodes, edges); ok && bound &&
			!citesDocument(graph, reference.DocumentID) {
			documentIDs = append(documentIDs, reference.DocumentID)
		}
	}
	return documentIDs
}

// citesDocument reports whether graph is a document reference citing a
// revision of documentID. Its members of that document were recorded over
// the cited revision, which the gate already holds the reference to
// (revisionAllows on the citation), so a legacy reference needs no history
// for that document: the citation is its binding. This is the only legacy
// path that requires less than every registered revision. Its residual is an
// existence-of-action exposure only: a reference citing an old public
// revision opens again once the document is relabelled public, even if a
// labelled revision came between, and the cited content is public.
func citesDocument(graph evidencelabels.Graph, documentID shoal.ID) bool {
	return graph.RevisionID != "" && graph.DocumentID == documentID
}

// rules is what graph index requires beyond its members' current rules. ok
// false denies it.
//
//   - Any member whose current registration is RegistrationUnspecified denies.
//   - A corrupt binding denies.
//   - A bound graph: every member must be covered by an entry, and each entry
//     requires what its kind requires (see the comment at the top of this
//     file): revisionAllows for a document or extracted revision, nothing
//     more for a materialized or application registration, and denial for
//     an unspecified one.
//   - A legacy graph: every document a member now belongs to, other than the
//     document a document reference cites, is held to every registered
//     revision of it.
func (r *graphEvidenceRevisions) rules(
	index int, graph evidencelabels.Graph,
	nodes registeredNodes, edges interactionEdges,
) ([]AccessRule, bool) {
	members := graphMembers(graph, edges)
	for _, member := range members {
		kind, _, _, ok := memberRevision(member, nodes, edges)
		if !ok || kind == RegistrationUnspecified {
			return nil, false
		}
	}
	var required []AccessRule
	switch r.states[index] {
	case graphEvidenceBound:
		covered := make(map[graphMember][]RegistrationKind, len(r.bindings[index].entries))
		for _, entry := range r.bindings[index].entries {
			member := graphMember{entry.Object, entry.ID}
			covered[member] = append(covered[member], entry.Kind)
			// An edge recorded over a registration that now resolves only
			// as a derived edge denies: the derived resolution carries no
			// rule, so the recorded registration's rule could no longer be
			// enforced. Not reachable while derived IDs and registered IDs
			// are disjoint (graphMembers); load-bearing:
			// TestFleetEdgeRecordedRegisteredThenDerivedDenies.
			if _, derived := edges.derived[entry.ID]; derived &&
				entry.Object == GraphBindingEdge {
				return nil, false
			}
			switch {
			case documentBound(entry.Kind):
				rules, ok := r.cache.referenceRules(RevisionReference{
					DocumentID: entry.DocumentID, RevisionID: entry.RevisionID,
				})
				if !ok {
					return nil, false
				}
				required = append(required, rules...)
			case entry.Kind == RegistrationMaterialized,
				entry.Kind == RegistrationApplication:
				// The registration's own current rule decides it: a
				// materialization identity is not a document revision, and
				// an application edge's rule already carries its Connect
				// binding, its endpoints being entries of their own.
			default:
				return nil, false
			}
		}
		for _, member := range members {
			kind, _, _, _ := memberRevision(member, nodes, edges)
			if !bindingCovers(kind, covered[member]) {
				return nil, false
			}
		}
	case graphEvidenceLegacy:
		seen := make(map[shoal.ID]struct{})
		for _, member := range members {
			_, reference, bound, _ := memberRevision(member, nodes, edges)
			if !bound || citesDocument(graph, reference.DocumentID) {
				continue
			}
			if _, done := seen[reference.DocumentID]; done {
				continue
			}
			seen[reference.DocumentID] = struct{}{}
			// Bound zero: every registered revision. No record time is
			// safe to bound by (see the comment at the top of this file).
			rules, ok := r.cache.documentRules(reference.DocumentID, 0)
			if !ok {
				return nil, false
			}
			required = append(required, rules...)
		}
	default:
		return nil, false
	}
	return required, true
}

// bindingCovers reports whether a member whose current registration is of
// kind current is covered by entries of the recorded kinds. A recorded kind
// vouches only for a registration it could have become: a document's own
// node or edge is always that document's; a materialization identity vouches
// only for a materialized registration; and an application edge may since
// have been replaced by an extracted relation, which keeps its Connect
// binding (#695). An entry of another kind does not cover the member, so a
// binding can never move a document's node off the revision path.
func bindingCovers(current RegistrationKind, recorded []RegistrationKind) bool {
	for _, kind := range recorded {
		switch {
		case kind == current:
			return true
		case current == RegistrationExtracted && kind == RegistrationApplication:
			return true
		}
	}
	return false
}

// graphEvidenceBindings reads, in bounded batches, the registration of every
// node, edge and edge endpoint each reference names, projected for recorder,
// and returns each reference's binding.
func (c *Client) graphEvidenceBindings(
	ctx context.Context,
	references []interaction.EvidenceReference,
	recorder *registrationReader,
) ([]GraphEvidenceBinding, error) {
	var nodeIDs, edgeIDs []shoal.ID
	for _, reference := range references {
		nodeIDs = append(nodeIDs, reference.NodeIDs...)
		edgeIDs = append(edgeIDs, reference.EdgeIDs...)
	}
	// An edge with no registration may be a derived (latent) edge (#744). The
	// trusted reader is asked about every edge, registered or not, as
	// resolveInteractionEdges asks it, and a derived edge's endpoints are
	// read with the reference's nodes.
	derived := make(map[shoal.ID]graph.Edge)
	for start := 0; start < len(edgeIDs); {
		end := chunkEnd(start, len(edgeIDs), maxInteractionAuthorizationIDs)
		resolved, err := c.trustedDerivedEdges(ctx, edgeIDs[start:end])
		if err != nil {
			return nil, err
		}
		start = end
		for id, edge := range resolved {
			derived[id] = edge
		}
	}
	for _, edge := range derived {
		nodeIDs = append(nodeIDs, edge.From, edge.To)
	}
	nodes, edges, err := c.rawRegistrations(ctx, nodeIDs, edgeIDs)
	if err != nil {
		return nil, err
	}
	nodes, edges, err = c.projectSharedRegistrations(ctx, nodes, edges, recorder)
	if err != nil {
		return nil, err
	}
	bindings := make([]GraphEvidenceBinding, len(references))
	for index, reference := range references {
		var entries []GraphBindingEntry
		addNode := func(id shoal.ID) {
			if registration, ok := nodes[id]; ok {
				entries = append(entries, nodeBindingEntry(id, registration))
			}
		}
		for _, id := range reference.NodeIDs {
			addNode(id)
		}
		for _, id := range reference.EdgeIDs {
			if registration, ok := edges[id]; ok {
				entries = append(entries, edgeBindingEntry(id, registration))
				addNode(registration.Edge.From)
				addNode(registration.Edge.To)
				continue
			}
			// A derived edge records no entry of its own (graphMembers):
			// it is bound by its endpoints' entries, so a later relabel of
			// either endpoint holds the evidence to the revision recorded
			// here.
			if edge, ok := derived[id]; ok {
				addNode(edge.From)
				addNode(edge.To)
			}
		}
		binding, err := NewGraphEvidenceBinding(entries...)
		if err != nil {
			return nil, inconsistentBase()
		}
		bindings[index] = binding
	}
	return bindings, nil
}
