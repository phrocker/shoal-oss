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

package authorized_test

// The identity namespaces are keyed by the catalog (#721). Before, the
// application edge namespace (#711) and the upload namespace (#718) were an
// unkeyed digest of the identity, so anyone who read an edge ID or a source
// URI could recompute it for a guessed author.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	edgeNamespaceV1   = "shoal/authorized/application-edge-namespace/v1"
	uploadNamespaceV1 = "shoal/authorized/upload-namespace/v1"
)

// unkeyedNamespaceDigest is what anyone could compute before #721 from a
// guessed identity: the #711 framing of the identity under version, hashed
// with SHA-256 and truncated to 128 bits.
func unkeyedNamespaceDigest(version string, decision auth.Decision) string {
	hash := sha256.New()
	var length [8]byte
	write := func(value []byte) {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(value)
	}
	write([]byte(version))
	write(decision.AuthorizationDomain())
	write([]byte(decision.Subject()))
	write([]byte(decision.Actor()))
	write([]byte(decision.ClientID()))
	binary.BigEndian.PutUint64(length[:], uint64(len(decision.OnBehalfOf())))
	_, _ = hash.Write(length[:])
	for _, principal := range decision.OnBehalfOf() {
		write([]byte(principal))
	}
	write([]byte(decision.ServiceRole()))
	write([]byte(decision.ServiceCeilingIdentity()))
	return hex.EncodeToString(hash.Sum(nil)[:16])
}

// before721Store makes a client over the same catalog compute the namespaces
// a binary from before #721 computed: the unkeyed SHA-256 of the version 1
// message, in place of the catalog's MAC of the version 2 one. It builds the
// catalog an upgrade starts from.
type before721Store struct {
	authorized.PolicyStore
}

func (s before721Store) IdentityNamespaceMAC(
	_ context.Context, message []byte,
) ([]byte, error) {
	length := binary.BigEndian.Uint64(message[:8])
	version := string(message[8 : 8+length])
	if !strings.HasSuffix(version, "/v2") {
		panic("unexpected namespace version " + version)
	}
	legacy := append([]byte(nil), message...)
	legacy[8+length-1] = '1'
	digest := sha256.Sum256(legacy)
	return digest[:], nil
}

// edgeDigest and uploadDigest are the namespace digests of an edge ID and a
// namespaced upload URI.
func edgeDigest(t *testing.T, id shoal.ID) string {
	t.Helper()
	rest, ok := strings.CutPrefix(string(id), "app-edge_")
	if !ok || len(rest) < 33 || rest[32] != '/' {
		t.Fatalf("edge ID %q is not namespaced", id)
	}
	return rest[:32]
}

func uploadDigest(t *testing.T, uri string) string {
	t.Helper()
	rest, ok := strings.CutPrefix(uri, authorized.UploadSourceURIPrefix+"u-")
	if !ok || len(rest) < 33 || rest[32] != '/' {
		t.Fatalf("upload URI %q is not namespaced", uri)
	}
	return rest[:32]
}

// namespaceDigests are one identity's edge and upload digests through
// client.
func namespaceDigests(
	t *testing.T, client *authorized.Client, ctx context.Context,
) (string, string) {
	t.Helper()
	edge := appEdgeID(t, client, ctx, "local")
	upload, err := client.OwnUploadSourceURI(ctx, "name.md")
	if err != nil {
		t.Fatal(err)
	}
	return edgeDigest(t, edge), uploadDigest(t, upload)
}

// TestIdentityNamespacesCannotBeRecomputedWithoutTheKey: neither digest is
// the unkeyed one of either version, and both are 32 lowercase hex digits
// as before; the same identity keeps them across clients and across a
// reopen of a durable catalog, and another catalog gives other ones.
func TestIdentityNamespacesCannotBeRecomputedWithoutTheKey(t *testing.T) {
	f := newFixture(t)
	decision := f.bindingDecision(t, "keyed-writer")
	ctx := f.context(t, decision)
	selector := f.staticSelector(t, f.sourceA, f.policyA)
	clientOver := func(store authorized.PolicyStore) *authorized.Client {
		return f.labelClient(t, f.base, store, selector, selector)
	}
	memory := authorized.NewMemoryPolicyStore()
	edge, upload := namespaceDigests(t, clientOver(memory), ctx)
	for _, unkeyed := range []string{
		unkeyedNamespaceDigest(edgeNamespaceV1, decision),
		unkeyedNamespaceDigest("shoal/authorized/application-edge-namespace/v2", decision),
		unkeyedNamespaceDigest(uploadNamespaceV1, decision),
		unkeyedNamespaceDigest("shoal/authorized/upload-namespace/v2", decision),
	} {
		if edge == unkeyed || upload == unkeyed {
			t.Fatalf("a namespace digest is recomputable from the identity: %s", unkeyed)
		}
	}
	if edge == upload {
		t.Fatal("the edge and upload namespaces share a digest")
	}
	if _, err := hex.DecodeString(edge + upload); err != nil ||
		strings.ToLower(edge+upload) != edge+upload {
		t.Fatalf("digests %q, %q are not lowercase hex", edge, upload)
	}
	// The same catalog through another client: the same namespaces.
	if againEdge, againUpload := namespaceDigests(t, clientOver(memory), ctx); againEdge != edge ||
		againUpload != upload {
		t.Fatal("the namespaces moved between clients of one catalog")
	}

	dir := t.TempDir()
	durable, err := authorized.OpenDurablePolicyStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	durableEdge, durableUpload := namespaceDigests(t, clientOver(durable), ctx)
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := authorized.OpenDurablePolicyStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if againEdge, againUpload := namespaceDigests(t, clientOver(reopened), ctx); againEdge != durableEdge ||
		againUpload != durableUpload {
		t.Fatal("the namespaces moved across a reopen of the durable catalog")
	}
	other, err := authorized.OpenDurablePolicyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	seen := map[string]string{edge: "memory edge", upload: "memory upload"}
	for name, digest := range map[string]string{
		"durable edge": durableEdge, "durable upload": durableUpload,
	} {
		if previous, ok := seen[digest]; ok {
			t.Fatalf("%s shares a digest with %s", name, previous)
		}
		seen[digest] = name
	}
	for _, store := range []authorized.PolicyStore{
		authorized.NewMemoryPolicyStore(), other,
	} {
		otherEdge, otherUpload := namespaceDigests(t, clientOver(store), ctx)
		if _, ok := seen[otherEdge]; ok {
			t.Fatal("two catalogs share an edge namespace")
		}
		if _, ok := seen[otherUpload]; ok {
			t.Fatal("two catalogs share an upload namespace")
		}
		seen[otherEdge], seen[otherUpload] = "other", "other"
	}
}

// TestUploadsStoredBefore721KeepRoutingToTheirUploader: an upload made
// before #721 is in its uploader's unkeyed namespace. After the upgrade its
// name still routes the uploader to it, as the uploader's own document,
// and so does an ingest at its URI; new names go to the keyed namespace,
// and the unkeyed namespace takes no new documents. To another identity
// that document is another uploader's, refused or routed exactly as before
// #721.
func TestUploadsStoredBefore721KeepRoutingToTheirUploader(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "memory"
		if durable {
			name = "durable"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			dir := t.TempDir()
			open := func() authorized.PolicyStore {
				if !durable {
					return f.store
				}
				store, err := authorized.OpenDurablePolicyStore(dir)
				if err != nil {
					t.Fatal(err)
				}
				return store
			}
			before := open()
			old := newUploadWorld(t, f, before721Store{before}, false)
			holderDecision := old.decisions[uploadHolder]
			legacyHolder := authorized.UploadSourceURIPrefix + "u-" +
				unkeyedNamespaceDigest(uploadNamespaceV1, holderDecision) + "/"
			mine := old.ingest(old.client, uploadHolder, "mine.md", "# Mine\n\nv1\n", "secret")
			shared := old.ingest(old.client, uploadReaderTwo, "shared.md", "# Shared\n\nv1\n", "")
			if got := old.sourceURIs()[mine.Document.ID]; got != legacyHolder+"mine.md" {
				t.Fatalf("the pre-#721 upload is at %q, want %q", got, legacyHolder+"mine.md")
			}
			if durable {
				if err := before.(*authorized.DurablePolicyStore).Close(); err != nil {
					t.Fatal(err)
				}
			}
			after := open()
			if durable {
				t.Cleanup(func() { _ = after.(*authorized.DurablePolicyStore).Close() })
			}
			w := newUploadWorld(t, f, after, false)
			keyedHolder := strings.TrimSuffix(w.ownURI(uploadHolder, "x.md"), "x.md")
			if keyedHolder == legacyHolder {
				t.Fatal("the holder's namespace is still the unkeyed one")
			}
			count := len(w.sourceURIs())

			// By name: the holder's own document, reported as its own.
			again, route, err := w.client.IngestUpload(w.as(uploadHolder),
				uploadSource("mine.md", "# Mine\n\nv2\n", "secret"))
			if err != nil || again.Document.ID != mine.Document.ID ||
				again.Disposition != explorer.IngestApplied ||
				route.Kind != authorized.UploadRouteOwn ||
				route.SourceURI != legacyHolder+"mine.md" ||
				route.OtherDocuments != 0 || route.Ambiguous {
				t.Fatalf("re-upload by name = %+v, %+v (%v), want a revision of %s",
					again, route, err, mine.Document.ID)
			}
			// At its URI: accepted as the holder's own.
			source := uploadSource("mine.md", "# Mine\n\nv3\n", "secret")
			source.URI = legacyHolder + "mine.md"
			atURI, route, err := w.client.IngestUpload(w.as(uploadHolder), source)
			if err != nil || atURI.Document.ID != mine.Document.ID ||
				route.Kind != authorized.UploadRouteOwn {
				t.Fatalf("re-upload at its URI = %+v, %+v (%v)", atURI, route, err)
			}
			// A new name goes to the keyed namespace.
			fresh := w.ingest(w.client, uploadHolder, "fresh.md", "# Fresh\n", "secret")
			if got := w.sourceURIs()[fresh.Document.ID]; got != keyedHolder+"fresh.md" {
				t.Fatalf("a new upload is at %q, want the keyed namespace", got)
			}
			// The unkeyed namespace takes no new document, refused as a
			// target with nothing at it is.
			reader := w.as(uploadReader)
			absentTarget := func() error {
				source := uploadSource("absent.md", "# Absent\n", "")
				source.URI = keyedHolder + "absent.md"
				_, err := w.client.Ingest(reader, source)
				return err
			}()
			if !shoal.IsErrorCode(absentTarget, shoal.ErrorInvalidArgument) {
				t.Fatalf("absent target = %v", absentTarget)
			}
			source = uploadSource("new-in-legacy.md", "# New\n", "secret")
			source.URI = legacyHolder + "new-in-legacy.md"
			if _, err := w.client.Ingest(w.as(uploadHolder), source); err == nil ||
				err.Error() != absentTarget.Error() {
				t.Fatalf("new document in the unkeyed namespace = %v, want %v",
					err, absentTarget)
			}

			// Another identity: the holder's hidden pre-#721 document is
			// refused as a target exactly like nothing at all, in either
			// of the holder's namespaces, and its name routes the reader
			// to its own new document.
			for _, uri := range []string{
				legacyHolder + "mine.md", legacyHolder + "absent.md",
				keyedHolder + "fresh.md", keyedHolder + "absent.md",
			} {
				source := uploadSource("mine.md", "# Probe\n", "")
				source.URI = uri
				if _, err := w.client.Ingest(reader, source); err == nil ||
					err.Error() != absentTarget.Error() {
					t.Fatalf("reader at %q = %v, want %v", uri, err, absentTarget)
				}
			}
			byName, route, err := w.client.IngestUpload(reader,
				uploadSource("mine.md", "# Probe\n", ""))
			if err != nil || byName.Document.ID == mine.Document.ID ||
				route.Kind != authorized.UploadRouteOwn ||
				route.SourceURI != w.ownURI(uploadReader, "mine.md") ||
				route.OtherDocuments != 0 {
				t.Fatalf("reader upload by name = %+v, %+v (%v)", byName, route, err)
			}
			w.requireCurrent(uploadHolder, atURI, "after the reader's probes")

			// A readable pre-#721 upload of another identity is still
			// shared by name, as step 3 of #718 routes it.
			edited, route, err := w.client.IngestUpload(reader,
				uploadSource("shared.md", "# Shared\n\nv2\n", ""))
			if err != nil || edited.Document.ID != shared.Document.ID ||
				route.Kind != authorized.UploadRouteShared {
				t.Fatalf("shared pre-#721 upload = %+v, %+v (%v)", edited, route, err)
			}
			// Its own uploader reaches it as its own.
			own, route, err := w.client.IngestUpload(w.as(uploadReaderTwo),
				uploadSource("shared.md", "# Shared\n\nv3\n", ""))
			if err != nil || own.Document.ID != shared.Document.ID ||
				route.Kind != authorized.UploadRouteOwn {
				t.Fatalf("own pre-#721 upload = %+v, %+v (%v)", own, route, err)
			}
			// Two new documents: the holder's fresh one and the reader's.
			if got := len(w.sourceURIs()); got != count+2 {
				t.Fatalf("documents = %d, want %d", got, count+2)
			}
		})
	}
}

// TestUploadOwnKeyedDocumentWinsOverItsLegacyOne: when the identity has a
// document of the name in both of its namespaces, the name routes to the
// keyed one, and reports the other as left unchanged.
func TestUploadOwnKeyedDocumentWinsOverItsLegacyOne(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		f := newFixture(t)
		old := newUploadWorld(t, f, before721Store{store}, false)
		legacy := old.ingest(old.client, uploadReader, "both.md", "# Both\n\nlegacy\n", "")
		w := newUploadWorld(t, f, store, false)
		source := uploadSource("both.md", "# Both\n\nkeyed\n", "")
		source.URI = w.ownURI(uploadReader, "both.md")
		keyed, err := w.client.Ingest(w.as(uploadReader), source)
		if err != nil || keyed.Document.ID == legacy.Document.ID {
			t.Fatalf("keyed target = %+v (%v)", keyed, err)
		}
		byName, route, err := w.client.IngestUpload(w.as(uploadReader),
			uploadSource("both.md", "# Both\n\nby name\n", ""))
		if err != nil || byName.Document.ID != keyed.Document.ID ||
			route.Kind != authorized.UploadRouteOwn || route.OtherDocuments != 1 {
			t.Fatalf("by name = %+v, %+v (%v), want the keyed document", byName, route, err)
		}
	})
}

// TestConnectRetriesEdgesConnectedBefore721: an edge connected before #721
// has an ID in its writer's unkeyed namespace. Its writer's retry of it is
// still accepted, idempotently, and the selector still sees its local ID;
// a new edge in that namespace is refused as an ID outside the namespace
// is, and so is every ID in it for any other identity.
func TestConnectRetriesEdgesConnectedBefore721(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		f := newFixture(t)
		w := newBindingWorld(t, f, before721Store{store})
		oldClient := w.client
		legacyID := w.edgeID(oldClient, "pre-721-edge")
		if edgeDigest(t, legacyID) != unkeyedNamespaceDigest(edgeNamespaceV1, w.holder) {
			t.Fatalf("the pre-#721 edge ID %s is not the unkeyed one", legacyID)
		}
		edge := graph.Edge{
			ID: legacyID, From: w.open.Document.ID, To: w.open.Document.ID,
			Type: namespaceProbeEdgeType, Weight: 1,
		}
		if err := oldClient.Connect(w.as(w.holder), edge); err != nil {
			t.Fatal(err)
		}

		client := w.newClient(store)
		keyedID := w.edgeID(client, "pre-721-edge")
		if keyedID == legacyID {
			t.Fatal("ApplicationEdgeID still returns the unkeyed ID")
		}
		holder := w.as(w.holder)
		if err := client.Connect(holder, edge); err != nil {
			t.Fatalf("retry of the pre-#721 edge = %v", err)
		}
		different := edge
		different.Weight = 2
		if err := client.Connect(holder, different); !shoal.IsErrorCode(
			err, shoal.ErrorConflict) {
			t.Fatalf("a different edge under the pre-#721 ID = %v, want conflict", err)
		}
		outside := client.Connect(holder, graph.Edge{
			ID: "plain-edge", From: w.open.Document.ID, To: w.open.Document.ID,
			Type: namespaceProbeEdgeType, Weight: 1,
		})
		if !shoal.IsErrorCode(outside, shoal.ErrorInvalidArgument) {
			t.Fatalf("outside = %v", outside)
		}
		legacyNamespace := strings.TrimSuffix(string(legacyID), "pre-721-edge")
		fresh := edge
		fresh.ID = shoal.ID(legacyNamespace + "new-edge")
		if err := client.Connect(holder, fresh); err == nil ||
			err.Error() != outside.Error() {
			t.Fatalf("a new edge in the unkeyed namespace = %v, want %v", err, outside)
		}
		// The keyed namespace takes the new edge.
		fresh.ID = w.edgeID(client, "new-edge")
		if err := client.Connect(holder, fresh); err != nil {
			t.Fatalf("a new edge in the keyed namespace = %v", err)
		}
		// Another identity: the holder's IDs, existing or not, keyed or
		// not, are all refused as outside its namespace.
		reader := w.as(w.reader)
		for _, id := range []shoal.ID{
			legacyID, shoal.ID(legacyNamespace + "absent"), keyedID, fresh.ID,
		} {
			probe := edge
			probe.ID = id
			if err := client.Connect(reader, probe); err == nil ||
				err.Error() != outside.Error() {
				t.Fatalf("reader Connect(%s) = %v, want %v", id, err, outside)
			}
		}
		edges := w.probeEdges()
		if len(edges) != 2 || !edges[legacyID] || !edges[fresh.ID] {
			t.Fatalf("edges = %v", edges)
		}
	})
}

// TestConnectSelectorSeesTheLocalIDInBothNamespaces: the trusted selector
// recovers the caller's local ID with ApplicationEdgeLocalID for a new edge
// and for the retry of an edge connected before #721.
func TestConnectSelectorSeesTheLocalIDInBothNamespaces(t *testing.T) {
	f := newFixture(t)
	store := authorized.NewMemoryPolicyStore()
	static := f.staticSelector(t, f.sourceA, f.policyA)
	var locals []shoal.ID
	edges := authorized.EdgePolicySelectorFunc(func(
		ctx context.Context, decision auth.Decision, edge graph.Edge,
	) (auth.Policy, error) {
		local, ok := authorized.ApplicationEdgeLocalID(ctx, decision, edge.ID)
		if !ok {
			return auth.Policy{}, shoal.NewError(shoal.ErrorUnauthorized, "denied")
		}
		locals = append(locals, local)
		return static.SelectEdgePolicy(ctx, decision, edge)
	})
	holder := f.bindingDecision(t, "selector-holder")
	ctx := f.context(t, holder)
	oldClient := f.labelClient(t, f.base, before721Store{store}, static, edges)
	document, err := oldClient.Ingest(ctx,
		labelledSource("file:///selector/open.txt", "open", ""))
	if err != nil {
		t.Fatal(err)
	}
	edge := graph.Edge{
		ID:   appEdgeID(t, oldClient, ctx, "old"),
		From: document.Document.ID, To: document.Document.ID, Type: "links", Weight: 1,
	}
	if err := oldClient.Connect(ctx, edge); err != nil {
		t.Fatal(err)
	}
	client := f.labelClient(t, f.base, store, static, edges)
	if err := client.Connect(ctx, edge); err != nil {
		t.Fatalf("retry = %v", err)
	}
	edge.ID = appEdgeID(t, client, ctx, "new")
	if err := client.Connect(ctx, edge); err != nil {
		t.Fatalf("new edge = %v", err)
	}
	if want := []shoal.ID{"old", "old", "new"}; !equalIDs(locals, want) {
		t.Fatalf("selector saw %v, want %v", locals, want)
	}
}

func equalIDs(left, right []shoal.ID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// shortMACStore is a catalog whose MAC is shorter than a digest, as no valid
// one is.
type shortMACStore struct {
	authorized.PolicyStore
}

func (shortMACStore) IdentityNamespaceMAC(context.Context, []byte) ([]byte, error) {
	return make([]byte, 15), nil
}

// TestNamespaceRoutesRefuseAShortMAC: a MAC too short for a digest is
// refused as the catalog being unavailable, never sliced past its end.
func TestNamespaceRoutesRefuseAShortMAC(t *testing.T) {
	f := newFixture(t)
	selector := f.staticSelector(t, f.sourceA, f.policyA)
	client := f.labelClient(t, f.base, shortMACStore{authorized.NewMemoryPolicyStore()},
		selector, selector)
	ctx := f.context(t, f.bindingDecision(t, "short-mac-prober"))
	for name, call := range map[string]func() error{
		"ApplicationEdgeID": func() error {
			_, err := client.ApplicationEdgeID(ctx, "x")
			return err
		},
		"OwnUploadSourceURI": func() error {
			_, err := client.OwnUploadSourceURI(ctx, "x.md")
			return err
		},
		"Connect": func() error {
			return client.Connect(ctx, graph.Edge{
				ID:   "app-edge_" + shoal.ID(strings.Repeat("0", 32)) + "/x",
				From: "a", To: "b", Type: "t", Weight: 1,
			})
		},
		"Ingest by name": func() error {
			_, err := client.Ingest(ctx, uploadSource("x.md", "x", ""))
			return err
		},
	} {
		if err := call(); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
			t.Fatalf("%s with a short MAC = %v, want unavailable", name, err)
		}
	}
}

// TestNamespaceRoutesFailClosedWithoutTheCatalog: with the catalog closed,
// every route that needs a namespace is refused as the catalog being
// unavailable, before anything is written, and no namespace is guessed.
func TestNamespaceRoutesFailClosedWithoutTheCatalog(t *testing.T) {
	f := newFixture(t)
	selector := f.staticSelector(t, f.sourceA, f.policyA)
	durable, err := authorized.OpenDurablePolicyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client := f.labelClient(t, f.base, durable, selector, selector)
	ctx := f.context(t, f.bindingDecision(t, "closed-prober"))
	id := appEdgeID(t, client, ctx, "x")
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() error{
		"ApplicationEdgeID": func() error {
			_, err := client.ApplicationEdgeID(ctx, "x")
			return err
		},
		"OwnUploadSourceURI": func() error {
			_, err := client.OwnUploadSourceURI(ctx, "x.md")
			return err
		},
		"Connect": func() error {
			return client.Connect(ctx, graph.Edge{ID: id, From: "a", To: "b", Type: "t", Weight: 1})
		},
		"Ingest by name": func() error {
			_, err := client.Ingest(ctx, uploadSource("x.md", "x", ""))
			return err
		},
	} {
		if err := call(); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
			t.Fatalf("%s over a closed catalog = %v, want unavailable", name, err)
		}
	}
}
