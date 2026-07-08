// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package itercfg resolves the iterator stack that Java applied at a
// given compaction scope, by reading the table's configuration out of
// ZooKeeper. Layout (Accumulo 4.0):
//
//	/accumulo/<instance-id>/namespaces                          -> JSON {namespace-id: namespace-name}
//	/accumulo/<instance-id>/namespaces/<ns-id>/tables           -> JSON {table-id: table-name}
//	/accumulo/<instance-id>/tables/<id>/config                  -> versioned-props blob (binary)
//	/accumulo/<instance-id>/namespaces/<ns-id>/config           -> versioned-props blob (binary)
//	/accumulo/<instance-id>/config                              -> versioned-props blob (binary)  [site]
//
// Name resolution requires (1) parsing `name` into namespace + table
// halves (TableNameUtil.qualify: "ns.table" or just "table" for default
// namespace), (2) looking up the namespace id from /namespaces, then
// (3) looking up the table id from /namespaces/<ns-id>/tables. The
// JSON-map shape is the same as Java's NamespaceMapping.serializeMap.
//
// VERSIONED-PROPS BLOB FORMAT (Java VersionedPropGzipCodec):
//
//	int32  encoding version (currently 1)
//	bool   compressed flag (1 byte: 0x00 or 0x01)
//	UTF    timestamp string (DataOutputStream.writeUTF: int16 length || UTF-8 bytes)
//	[ gzip(  // payload, optionally gzip-compressed when the bool is true
//	    int32  number of (key, value) pairs
//	    repeated:
//	      UTF  key
//	      UTF  value
//	  ) ]
//
// Properties are MERGED across system → namespace → table levels, with
// table overriding namespace overriding system. Accumulo's actual
// compactor reads all three; an iterator can be defined at any level.
// Most installs configure table.iterator.* at the SYSTEM level so it
// applies to every table by default — the table-level znode is empty
// even when `accumulo shell config -t graph_vidx` shows the properties
// (the shell reports the merged effective view).
//
// We care about properties of the form:
//
//	table.iterator.<scope>.<name>            = "<priority>,<class>"
//	table.iterator.<scope>.<name>.opt.<k>    = "<v>"
//
// where <scope> is scan|minc|majc and <name> is the operator's nickname
// for the stack entry. The resolver groups options under their owning
// iterator, sorts by priority ascending (matching Accumulo's
// IteratorEnvironment build order), maps Java class names to shoal's
// iterrt registry, and returns the resulting []iterrt.IterSpec.
//
// Iterators whose class names aren't in the allowlist are SKIPPED with a
// WARN — operators see them flagged so they know the shadow comparison
// would be unsound for that table until the iterator is ported. The
// rest of the stack is still resolved so partial-coverage tables can
// shadow on whatever IS in the allowlist (the report records what was
// elided).
package itercfg

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/zk"
)

// defaultNamespace is the unqualified-name fallback. Accumulo's
// Namespace.DEFAULT.name() is the empty string and DEFAULT id is
// "+default" — names without a "." (e.g. "graph_vidx") resolve under
// this namespace.
const defaultNamespaceName = ""

// ClassAllowlist maps fully-qualified Java iterator class names to the
// iterrt registered identifier shoal will instantiate in their place.
// Multiple aliases for the same class are deliberate — Accumulo ships
// both o.a.a.core.iterators.user.VersioningIterator and the older
// org.apache.accumulo.core.iterators.user.VersioningIterator alias.
//
// Tables whose stack references a class NOT in this map are reported
// out of shadow coverage and the iterator is dropped from the
// reconstructed stack with a WARN. See Resolve.
var ClassAllowlist = map[string]string{
	// VersioningIterator — newest-N per coordinate.
	"org.apache.accumulo.core.iterators.user.VersioningIterator": iterrt.IterVersioning,
	"org.apache.accumulo.core.iterators.VersioningIterator":      iterrt.IterVersioning,

	// DeletingIterator — tombstone suppression.
	"org.apache.accumulo.core.iterators.system.DeletingIterator": iterrt.IterDeleting,
	"org.apache.accumulo.core.iterators.DeletingIterator":        iterrt.IterDeleting,

	// VisibilityFilter — column-visibility check (scan-only in practice).
	"org.apache.accumulo.core.iterators.system.VisibilityFilter": iterrt.IterVisibility,

	// LatentEdgeDiscoveryIterator — the graph_vidx majc emitter.
	// Lives in core/ under the `accumulo.core.graph` package.
	"org.apache.accumulo.core.graph.LatentEdgeDiscoveryIterator": iterrt.IterLatentEdgeDiscovery,
}

// ResolvedStack is the parsed table.iterator.<scope>.* config for one
// table. Stack is the in-order list (lowest priority first) of iterators
// that ARE supported; Skipped lists the entries that were dropped
// because their class isn't allowlisted.
type ResolvedStack struct {
	// TableID is the table the stack belongs to.
	TableID string
	// Scope is the compaction scope these specs apply to.
	Scope iterrt.IteratorScope
	// Stack is the resolved iterator chain in priority order (low first).
	// Empty stack is valid (no iterators configured at this scope).
	Stack []iterrt.IterSpec
	// Skipped names each iterator that was dropped because its class
	// isn't in ClassAllowlist. Operators read this list to know which
	// tables need a new iterator port before shadow comparison is
	// meaningful.
	Skipped []SkippedIter
	// LoadedAt records when the config snapshot was taken from ZK.
	// Callers use this to gate cache freshness.
	LoadedAt time.Time
}

// SkippedIter describes a table.iterator.<scope>.<name> entry that was
// dropped from the resolved stack because its Java class isn't in
// ClassAllowlist.
type SkippedIter struct {
	// Name is the entry's nickname (the third dotted component of the
	// table.iterator property).
	Name string
	// Class is the fully-qualified Java class name from the property
	// value.
	Class string
	// Priority is the integer priority parsed from the property value
	// (or -1 if the value was malformed).
	Priority int
}

// HasShoalCoverage is true when at least one iterator was successfully
// resolved AND no iterators were skipped. Operators use this to decide
// whether a table is shadow-eligible end-to-end.
func (r *ResolvedStack) HasShoalCoverage() bool {
	return len(r.Skipped) == 0
}

// Resolver loads + caches per-table iterator stacks from ZK. Safe for
// concurrent use; refreshes are coalesced per table-id.
//
// The cache TTL controls how quickly an operator-side table.iterator.*
// config change is picked up by the poller. Set 0 to disable caching
// (every Resolve hits ZK).
type Resolver struct {
	locator *zk.Locator
	ttl     time.Duration
	logger  *slog.Logger

	// Name → tableID cache. Populated lazily; refresh by clearing on
	// resolver shutdown or external signal. Names rarely change so we
	// don't TTL this.
	nameMu sync.RWMutex
	nameID map[string]string

	cacheMu sync.Mutex
	cache   map[stackKey]*ResolvedStack
}

type stackKey struct {
	tableID string
	scope   iterrt.IteratorScope
}

// NewResolver builds a Resolver bound to locator. ttl=0 disables the
// per-stack cache. logger=nil uses slog.Default().
func NewResolver(locator *zk.Locator, ttl time.Duration, logger *slog.Logger) *Resolver {
	if logger == nil {
		logger = slog.Default()
	}
	return &Resolver{
		locator: locator,
		ttl:     ttl,
		logger:  logger,
		nameID:  map[string]string{},
		cache:   map[stackKey]*ResolvedStack{},
	}
}

// ResolveTableID returns the table-id whose entry in the appropriate
// namespace JSON map equals tableName. Accepts both qualified
// ("ns.table") and unqualified ("table") forms; unqualified resolves
// against the default namespace.
//
// Caches the per-namespace JSON map for the resolver's lifetime — new
// tables added after construction won't be visible until
// InvalidateNames clears the cache.
func (r *Resolver) ResolveTableID(ctx context.Context, tableName string) (string, error) {
	r.nameMu.RLock()
	if id, ok := r.nameID[tableName]; ok {
		r.nameMu.RUnlock()
		return id, nil
	}
	r.nameMu.RUnlock()

	nsName, rawName := splitQualifiedName(tableName)

	nsID, err := r.resolveNamespaceID(ctx, nsName)
	if err != nil {
		return "", err
	}

	// Load the namespace's table map: /accumulo/<id>/namespaces/<ns-id>/tables
	// Data is a JSON object {"<table-id>": "<table-name>", ...}.
	tablesPath := path.Join(r.locator.InstancePath(), "namespaces", nsID, "tables")
	data, err := r.locator.GetRaw(ctx, tablesPath)
	if err != nil {
		return "", fmt.Errorf("get %s: %w", tablesPath, err)
	}
	idToName, err := decodeMappingJSON(data)
	if err != nil {
		return "", fmt.Errorf("decode %s: %w", tablesPath, err)
	}

	r.nameMu.Lock()
	defer r.nameMu.Unlock()
	// Refresh the cache from the loaded map. The key in the cache is
	// the operator-facing form (qualified or not, depending on what
	// was asked) so subsequent lookups hit the same key.
	for id, n := range idToName {
		if nsName == defaultNamespaceName {
			r.nameID[n] = id
		} else {
			r.nameID[nsName+"."+n] = id
		}
	}
	if id, ok := r.nameID[tableName]; ok {
		return id, nil
	}
	if id, ok := idToName[rawName]; ok {
		// Last-ditch: matched the unqualified half even if the call
		// used the qualified form (or vice versa). Cover the edge.
		return id, nil
	}
	return "", fmt.Errorf("table %q not found in namespace %q (%s)", tableName, nsName, tablesPath)
}

// resolveNamespaceID looks up a namespace by name in /accumulo/<id>/namespaces
// (JSON {ns-id: ns-name}). The default namespace's name is the empty
// string; its id is "+default".
func (r *Resolver) resolveNamespaceID(ctx context.Context, nsName string) (string, error) {
	nsRoot := path.Join(r.locator.InstancePath(), "namespaces")
	data, err := r.locator.GetRaw(ctx, nsRoot)
	if err != nil {
		return "", fmt.Errorf("get %s: %w", nsRoot, err)
	}
	idToName, err := decodeMappingJSON(data)
	if err != nil {
		return "", fmt.Errorf("decode %s: %w", nsRoot, err)
	}
	for id, n := range idToName {
		if n == nsName {
			return id, nil
		}
	}
	return "", fmt.Errorf("namespace %q not found at %s", nsName, nsRoot)
}

// splitQualifiedName mirrors TableNameUtil.qualify: a single dot
// separates namespace + table; absence = default namespace.
func splitQualifiedName(tableName string) (ns, raw string) {
	if i := strings.IndexByte(tableName, '.'); i >= 0 {
		return tableName[:i], tableName[i+1:]
	}
	return defaultNamespaceName, tableName
}

// decodeMappingJSON parses NamespaceMapping.serializeMap output: a flat
// JSON object {string: string}.
func decodeMappingJSON(data []byte) (map[string]string, error) {
	m := map[string]string{}
	if len(data) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// InvalidateNames clears the name→id cache so the next ResolveTableID
// re-scans ZK. Used by the poller after detecting a CreateTable / rename.
func (r *Resolver) InvalidateNames() {
	r.nameMu.Lock()
	r.nameID = map[string]string{}
	r.nameMu.Unlock()
}

// Resolve loads the iterator stack for tableID at scope, parses
// table.iterator.<scope>.* properties, and returns the resolved chain.
// Cache hits return the cached result iff age < ttl.
func (r *Resolver) Resolve(ctx context.Context, tableID string, scope iterrt.IteratorScope) (*ResolvedStack, error) {
	key := stackKey{tableID: tableID, scope: scope}

	r.cacheMu.Lock()
	if cached, ok := r.cache[key]; ok && r.ttl > 0 && time.Since(cached.LoadedAt) < r.ttl {
		r.cacheMu.Unlock()
		return cached, nil
	}
	r.cacheMu.Unlock()

	stack, err := r.loadStack(ctx, tableID, scope)
	if err != nil {
		return nil, err
	}

	r.cacheMu.Lock()
	r.cache[key] = stack
	r.cacheMu.Unlock()
	return stack, nil
}

// loadStack performs the ZK fetch + parse. Properties are merged in
// inheritance order: system → namespace → table. Each level's znode is
// a versioned-props blob (see decodePropBlob).
func (r *Resolver) loadStack(ctx context.Context, tableID string, scope iterrt.IteratorScope) (*ResolvedStack, error) {
	prefix := "table.iterator." + scopeString(scope) + "."

	merged := map[string]string{}

	// System level: /accumulo/<id>/config
	if err := r.mergePropsFrom(ctx, path.Join(r.locator.InstancePath(), "config"), prefix, merged); err != nil {
		r.logger.Warn("itercfg: system config read failed (continuing without site overrides)",
			slog.String("err", err.Error()))
	}

	// Namespace level. Look up the table's namespace, then read that
	// namespace's config znode.
	tableNS, err := r.tableNamespaceID(ctx, tableID)
	if err == nil && tableNS != "" {
		nsPath := path.Join(r.locator.InstancePath(), "namespaces", tableNS, "config")
		if err := r.mergePropsFrom(ctx, nsPath, prefix, merged); err != nil {
			r.logger.Debug("itercfg: namespace config read failed",
				slog.String("ns", tableNS), slog.String("err", err.Error()))
		}
	}

	// Table level: /accumulo/<id>/tables/<id>/config
	tablePath := path.Join(r.locator.InstancePath(), "tables", tableID, "config")
	if err := r.mergePropsFrom(ctx, tablePath, prefix, merged); err != nil {
		r.logger.Debug("itercfg: table config read failed",
			slog.String("table", tableID), slog.String("err", err.Error()))
	}

	return parseStack(tableID, scope, prefix, merged, r.logger), nil
}

// mergePropsFrom reads the versioned-props blob at znodePath, decodes
// it, and merges keys whose name starts with prefix into out (later
// calls override earlier). A non-existent znode is not an error — some
// installs don't have namespace-level config znodes at all.
func (r *Resolver) mergePropsFrom(ctx context.Context, znodePath, prefix string, out map[string]string) error {
	data, err := r.locator.GetRaw(ctx, znodePath)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	props, err := decodePropBlob(data)
	if err != nil {
		return fmt.Errorf("decode %s: %w", znodePath, err)
	}
	for k, v := range props {
		if strings.HasPrefix(k, prefix) {
			out[k] = v
		}
	}
	return nil
}

// tableNamespaceID returns the namespace-id znode-value stored at
// /accumulo/<id>/tables/<table-id>/namespace, or "" if not found.
func (r *Resolver) tableNamespaceID(ctx context.Context, tableID string) (string, error) {
	p := path.Join(r.locator.InstancePath(), "tables", tableID, "namespace")
	data, err := r.locator.GetRaw(ctx, p)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// decodePropBlob parses an Accumulo VersionedPropGzipCodec encoded
// property blob and returns the property map. Wire layout:
//
//	int32   encoding version (must be 1)
//	bool    compressed flag (1 byte)
//	UTF     timestamp string (DataOutputStream.writeUTF: int16 length + UTF-8 bytes)
//	payload either gzip-stream or raw:
//	  int32 count
//	  repeated: UTF key, UTF value
//
// Mirrors Java's VersionedPropCodec.fromBytes + VersionedPropGzipCodec.decodePayload.
func decodePropBlob(data []byte) (map[string]string, error) {
	r := bytes.NewReader(data)

	// EncodingOptions.fromDataStream: readInt() then readBoolean().
	var version uint32
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("read version: %w", err)
	}
	if version != 1 {
		return nil, fmt.Errorf("unsupported props encoding version %d", version)
	}
	var compressed byte
	if err := binary.Read(r, binary.BigEndian, &compressed); err != nil {
		return nil, fmt.Errorf("read compressed flag: %w", err)
	}

	// Java DataOutputStream.writeUTF: int16 (unsigned) length + UTF-8 bytes.
	if _, err := readJavaUTF(r); err != nil {
		return nil, fmt.Errorf("read timestamp: %w", err)
	}

	var payload io.Reader = r
	if compressed == 1 {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer gz.Close()
		payload = gz
	}

	// Property map: int32 count + count × (UTF key, UTF value).
	var count uint32
	if err := binary.Read(payload, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("read prop count: %w", err)
	}
	out := make(map[string]string, count)
	for i := uint32(0); i < count; i++ {
		k, err := readJavaUTF(payload)
		if err != nil {
			return nil, fmt.Errorf("read key %d: %w", i, err)
		}
		v, err := readJavaUTF(payload)
		if err != nil {
			return nil, fmt.Errorf("read value %d (key=%q): %w", i, k, err)
		}
		out[k] = v
	}
	return out, nil
}

// readJavaUTF reads a string in Java's DataOutputStream.writeUTF format:
// uint16-be length followed by that many bytes interpreted as "modified
// UTF-8". For property strings (ASCII / regular UTF-8) the modified
// encoding is identical to plain UTF-8, so we just slice the bytes.
func readJavaUTF(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// parseStack converts the prop map into an ordered ResolvedStack. Pure
// over (tableID, scope, prefix, props); covered by unit tests that
// bypass ZK entirely.
func parseStack(tableID string, scope iterrt.IteratorScope, prefix string, props map[string]string, logger *slog.Logger) *ResolvedStack {
	out := &ResolvedStack{
		TableID:  tableID,
		Scope:    scope,
		LoadedAt: time.Now(),
	}

	type rawEntry struct {
		name     string
		priority int
		class    string
		opts     map[string]string
	}
	entries := map[string]*rawEntry{} // by iterator nickname

	for k, v := range props {
		rest := strings.TrimPrefix(k, prefix)
		if rest == "" {
			continue
		}
		name, tail, hasTail := strings.Cut(rest, ".")
		entry, ok := entries[name]
		if !ok {
			entry = &rawEntry{name: name, priority: -1, opts: map[string]string{}}
			entries[name] = entry
		}
		if !hasTail {
			// Header property: "<priority>,<class>".
			pri, class, perr := splitPriorityClass(v)
			if perr != nil {
				logger.Warn("itercfg: malformed header",
					slog.String("table", tableID),
					slog.String("prop", k),
					slog.String("value", v),
					slog.String("err", perr.Error()),
				)
				continue
			}
			entry.priority = pri
			entry.class = class
			continue
		}
		// Option property: "opt.<key>".
		if optKey, ok := strings.CutPrefix(tail, "opt."); ok {
			entry.opts[optKey] = v
		}
		// Anything else (e.g. table.iterator.majc.<name>.something-else)
		// — ignored; reserved for future Accumulo extensions.
	}

	// Drop entries with no header (incomplete config — Accumulo would
	// reject this on write, but ZK can briefly land in this state during
	// an operator's two-step add). Carry them in Skipped with class="".
	for _, e := range entries {
		if e.class == "" {
			out.Skipped = append(out.Skipped, SkippedIter{
				Name: e.name, Class: "", Priority: e.priority,
			})
		}
	}
	for name, e := range entries {
		if e.class == "" {
			delete(entries, name)
		}
	}

	// Order by priority ascending (lowest priority runs LOWEST in the
	// stack — Accumulo IteratorUtil.loadIterators convention). Java
	// docs say "lowest priority first" matches "leaf side of the stack."
	keys := make([]string, 0, len(entries))
	for n := range entries {
		keys = append(keys, n)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return entries[keys[i]].priority < entries[keys[j]].priority
	})

	for _, name := range keys {
		e := entries[name]
		alias, ok := ClassAllowlist[e.class]
		if !ok {
			out.Skipped = append(out.Skipped, SkippedIter{
				Name: e.name, Class: e.class, Priority: e.priority,
			})
			logger.Warn("itercfg: class not in shoal allowlist (iterator skipped)",
				slog.String("table", tableID),
				slog.String("scope", scopeString(scope)),
				slog.String("name", e.name),
				slog.String("class", e.class),
				slog.Int("priority", e.priority),
			)
			continue
		}
		out.Stack = append(out.Stack, iterrt.IterSpec{
			Name:    alias,
			Options: e.opts,
		})
	}

	return out
}

// scopeString renders an iterator scope as the lowercase token used in
// Accumulo property names: scan|minc|majc.
func scopeString(s iterrt.IteratorScope) string {
	switch s {
	case iterrt.ScopeScan:
		return "scan"
	case iterrt.ScopeMinc:
		return "minc"
	case iterrt.ScopeMajc:
		return "majc"
	default:
		return "unknown"
	}
}

// splitPriorityClass parses "<priority>,<class>" from a table.iterator
// header property value.
func splitPriorityClass(v string) (int, string, error) {
	priS, class, ok := strings.Cut(v, ",")
	if !ok {
		return -1, "", errors.New("missing ','")
	}
	pri, err := strconv.Atoi(strings.TrimSpace(priS))
	if err != nil {
		return -1, "", fmt.Errorf("priority %q: %w", priS, err)
	}
	class = strings.TrimSpace(class)
	if class == "" {
		return pri, "", errors.New("empty class name")
	}
	return pri, class, nil
}
