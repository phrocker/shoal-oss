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

package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/inference"
)

const (
	cacheKeyVersion       = "shoal-grounded-inference-cache-v1"
	defaultMaxCacheItems  = 128
	defaultMaxCacheBytes  = 64 << 20
	defaultMaxCacheEntry  = inference.MaxInferenceResultBytes
	maxCacheIdentityBytes = 16 << 20
)

var (
	ErrCacheIdentityUnsafe = errors.New("agent harness cache identity unsafe")
	ErrCacheEntryTooLarge  = errors.New("agent harness cache entry too large")
)

// CacheKey is a non-disclosing, deterministic identity for one grounded
// inference request. It never renders raw prompts, evidence, credentials, or
// authorization grants.
type CacheKey struct {
	digest string
	set    bool
}

func (k CacheKey) Validate() error {
	if !k.set || len(k.digest) != sha256.Size*2 {
		return invalid("cache key is invalid")
	}
	if _, err := hex.DecodeString(k.digest); err != nil {
		return invalid("cache key is invalid")
	}
	return nil
}

func (k CacheKey) String() string {
	if !k.set {
		return "inference-cache:<invalid>"
	}
	return "inference-cache:" + k.digest
}

// Cache stores completed grounded inference runs. Implementations must clone
// records on both read and write.
type Cache interface {
	Get(context.Context, CacheKey) (Record, bool, error)
	Put(context.Context, CacheKey, Record) error
}

// CacheIdentityProvider supplies stable, non-secret configuration identity for
// a runner or tool host. A cached generator bypasses caching when either side
// cannot provide this identity.
type CacheIdentityProvider interface {
	CacheIdentity() (string, error)
}

// MemoryCacheConfig bounds the optional in-process cache. Zero values select
// deterministic defaults; negative values are invalid.
type MemoryCacheConfig struct {
	MaxEntries    int
	MaxBytes      int
	MaxEntryBytes int
}

// MemoryCache is a deterministic, bounded, concurrency-safe LRU cache.
type MemoryCache struct {
	mu    sync.Mutex
	cfg   MemoryCacheConfig
	items map[string]memoryCacheItem
	order []string // least recent first
	bytes int
}

type memoryCacheItem struct {
	record Record
	size   int
}

func NewMemoryCache(cfg MemoryCacheConfig) (*MemoryCache, error) {
	if cfg.MaxEntries < 0 || cfg.MaxBytes < 0 || cfg.MaxEntryBytes < 0 {
		return nil, invalid("cache bounds cannot be negative")
	}
	if cfg.MaxEntries == 0 {
		cfg.MaxEntries = defaultMaxCacheItems
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = defaultMaxCacheBytes
	}
	if cfg.MaxEntryBytes == 0 {
		cfg.MaxEntryBytes = defaultMaxCacheEntry
	}
	if cfg.MaxEntries <= 0 || cfg.MaxBytes <= 0 || cfg.MaxEntryBytes <= 0 {
		return nil, invalid("cache bounds must be positive")
	}
	if cfg.MaxEntryBytes > cfg.MaxBytes {
		cfg.MaxEntryBytes = cfg.MaxBytes
	}
	return &MemoryCache{cfg: cfg, items: make(map[string]memoryCacheItem)}, nil
}

func (c *MemoryCache) Get(ctx context.Context, key CacheKey) (Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, false, err
	}
	if c == nil {
		return Record{}, false, invalid("cache is nil")
	}
	if err := key.Validate(); err != nil {
		return Record{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key.digest]
	if !ok {
		return Record{}, false, nil
	}
	c.touchLocked(key.digest)
	return cloneRecord(item.record), true, nil
}

func (c *MemoryCache) Put(ctx context.Context, key CacheKey, record Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil {
		return invalid("cache is nil")
	}
	if err := key.Validate(); err != nil {
		return err
	}
	if unsafeRecordForCache(record) {
		return ErrCacheIdentityUnsafe
	}
	size := recordCacheBytes(record)
	if size > c.cfg.MaxEntryBytes {
		return ErrCacheEntryTooLarge
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.items[key.digest]; ok {
		c.bytes -= existing.size
		c.removeOrderLocked(key.digest)
	}
	c.items[key.digest] = memoryCacheItem{record: cloneRecord(record), size: size}
	c.order = append(c.order, key.digest)
	c.bytes += size
	c.evictLocked()
	return nil
}

func (c *MemoryCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *MemoryCache) Bytes() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes
}

func (c *MemoryCache) touchLocked(key string) {
	c.removeOrderLocked(key)
	c.order = append(c.order, key)
}

func (c *MemoryCache) removeOrderLocked(key string) {
	for i, existing := range c.order {
		if existing == key {
			copy(c.order[i:], c.order[i+1:])
			c.order = c.order[:len(c.order)-1]
			return
		}
	}
}

func (c *MemoryCache) evictLocked() {
	for len(c.items) > c.cfg.MaxEntries || c.bytes > c.cfg.MaxBytes {
		if len(c.order) == 0 {
			c.items = make(map[string]memoryCacheItem)
			c.bytes = 0
			return
		}
		victim := c.order[0]
		c.order = c.order[1:]
		if item, ok := c.items[victim]; ok {
			delete(c.items, victim)
			c.bytes -= item.size
		}
	}
}

func cacheKeyForRequest(request SessionRequest, runtimeIdentity string) (CacheKey, error) {
	if err := request.context.Validate(); err != nil {
		return CacheKey{}, err
	}
	if err := request.budgets.validate(); err != nil {
		return CacheKey{}, err
	}
	if err := request.provenance.validate(); err != nil {
		return CacheKey{}, err
	}
	if unsafeContextForCache(request.context) || unsafeProvenanceForCache(request.provenance) {
		return CacheKey{}, ErrCacheIdentityUnsafe
	}
	if unsafeCacheText(runtimeIdentity) {
		return CacheKey{}, ErrCacheIdentityUnsafe
	}
	model := request.provenance.model
	parameters := model.Parameters()
	parameterKeys := make([]string, 0, len(parameters))
	for key := range parameters {
		parameterKeys = append(parameterKeys, key)
	}
	sort.Strings(parameterKeys)
	parts := []string{
		cacheKeyVersion,
		string(request.id),
		string(request.context.ID()),
		string(request.context.Snapshot().ID()),
		cacheTime(request.context.Snapshot().AsOf()),
		string(request.context.Authorization().Fingerprint()),
		cacheTime(request.context.Authorization().ExpiresAt()),
		canonicalBudgets(request.budgets),
		request.provenance.harness,
		request.provenance.toolPolicy,
		model.Provider(),
		model.Model(),
		model.Version(),
		request.provenance.prompt.TemplateID(),
		request.provenance.prompt.Version(),
		request.provenance.prompt.Hash(),
		runtimeIdentity,
	}
	if seed, ok := model.Seed(); ok {
		parts = append(parts, "seed", strconv.FormatInt(seed, 10))
	} else {
		parts = append(parts, "no-seed")
	}
	for _, key := range parameterKeys {
		parts = append(parts, key, parameters[key])
	}
	if ontology, ok := request.context.Ontology(); ok {
		parts = append(parts, string(ontology.SchemaID()), string(ontology.VersionID()))
	}
	if unsafeIdentityParts(parts) {
		return CacheKey{}, ErrCacheIdentityUnsafe
	}
	encoded := framed(parts...)
	if len(encoded) > maxCacheIdentityBytes {
		return CacheKey{}, invalid("cache identity exceeds the byte bound")
	}
	digest := sha256.Sum256([]byte(encoded))
	return CacheKey{digest: hex.EncodeToString(digest[:]), set: true}, nil
}

func runtimeCacheIdentity(runner Runner, tools ToolHost) (string, error) {
	runnerProvider, ok := runner.(CacheIdentityProvider)
	if !ok {
		return "", ErrCacheIdentityUnsafe
	}
	toolProvider, ok := tools.(CacheIdentityProvider)
	if !ok {
		return "", ErrCacheIdentityUnsafe
	}
	runnerIdentity, err := runnerProvider.CacheIdentity()
	if err != nil {
		return "", err
	}
	toolIdentity, err := toolProvider.CacheIdentity()
	if err != nil {
		return "", err
	}
	if unsafeCacheText(runnerIdentity) || unsafeCacheText(toolIdentity) {
		return "", ErrCacheIdentityUnsafe
	}
	return framed("runtime", runnerIdentity, toolIdentity), nil
}

func validateCachedRecord(record Record, request SessionRequest, pack inference.ContextPack) error {
	if record.Request.id != request.id || record.Request.context.ID() != pack.ID() {
		return invalid("cached record identity does not match request")
	}
	if record.Request.context.Snapshot() != pack.Snapshot() ||
		record.Request.context.Authorization() != pack.Authorization() {
		return invalid("cached record pins do not match request")
	}
	if err := record.Result.ValidateFor(pack); err != nil {
		return err
	}
	return nil
}

func cloneRecord(record Record) Record {
	record.Request = cloneSessionRequest(record.Request)
	record.Transcript = cloneTranscript(record.Transcript)
	return record
}

func cloneSessionRequest(request SessionRequest) SessionRequest {
	request.context = clonePack(request.context)
	return request
}

func cacheTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func unsafeIdentityParts(parts []string) bool {
	for _, part := range parts {
		if unsafeCacheText(part) {
			return true
		}
	}
	return false
}

func unsafeProvenanceForCache(provenance Provenance) bool {
	if unsafeCacheText(provenance.harness) || unsafeCacheText(provenance.toolPolicy) {
		return true
	}
	model := provenance.model
	if unsafeCacheText(model.Provider()) || unsafeCacheText(model.Model()) ||
		unsafeCacheText(model.Version()) {
		return true
	}
	for key, value := range model.Parameters() {
		if unsafeCacheText(key) || unsafeCacheText(value) {
			return true
		}
	}
	prompt := provenance.prompt
	return unsafeCacheText(prompt.TemplateID()) || unsafeCacheText(prompt.Version()) ||
		unsafeCacheText(prompt.Hash())
}

func unsafeContextForCache(pack inference.ContextPack) bool {
	if unsafeCacheText(pack.Query()) || unsafeCacheText(string(pack.ID())) ||
		unsafeCacheText(string(pack.Snapshot().ID())) ||
		unsafeCacheText(string(pack.Authorization().Fingerprint())) {
		return true
	}
	for key, value := range pack.Metadata() {
		if unsafeCacheText(key) || unsafeCacheText(value) {
			return true
		}
	}
	for _, anchor := range pack.Evidence() {
		if unsafeAnchorForCache(anchor) {
			return true
		}
	}
	if ontology, ok := pack.Ontology(); ok {
		return unsafeCacheText(string(ontology.SchemaID())) ||
			unsafeCacheText(string(ontology.VersionID()))
	}
	return false
}

func unsafeRecordForCache(record Record) bool {
	if unsafeCacheText(string(record.Request.id)) ||
		unsafeContextForCache(record.Request.context) ||
		unsafeProvenanceForCache(record.Request.provenance) {
		return true
	}
	for _, anchor := range record.Result.EvidenceAdditions() {
		if unsafeAnchorForCache(anchor) {
			return true
		}
	}
	for _, claim := range record.Result.Claims() {
		if unsafeCacheText(string(claim.Subject())) || unsafeCacheText(string(claim.Predicate())) {
			return true
		}
		for key, value := range claim.Metadata() {
			if unsafeCacheText(key) || unsafeCacheText(value) {
				return true
			}
		}
	}
	for _, issue := range append(record.Result.Unresolved(), record.Result.Unsupported()...) {
		if unsafeCacheText(issue.Input()) || unsafeCacheText(issue.Reason()) {
			return true
		}
	}
	for _, failure := range record.Trace.Failures {
		if unsafeCacheText(failure.Operation) || unsafeCacheText(failure.Error) {
			return true
		}
	}
	return false
}

func unsafeAnchorForCache(anchor inference.EvidenceAnchor) bool {
	if unsafeCacheText(string(anchor.ID())) {
		return true
	}
	if citation, quote, ok := anchor.Document(); ok {
		return unsafeCacheText(string(citation.DocumentID)) ||
			unsafeCacheText(string(citation.RevisionID)) ||
			unsafeCacheText(string(citation.SectionID)) ||
			unsafeCacheText(string(citation.SpanID)) ||
			unsafeCacheText(quote)
	}
	if path, ok := anchor.Path(); ok {
		for _, node := range path.Nodes {
			if unsafeCacheText(string(node.ID)) || unsafeCacheText(node.Kind) {
				return true
			}
			for _, label := range node.Labels {
				if unsafeCacheText(label) {
					return true
				}
			}
			for key, value := range node.Properties {
				if unsafeCacheText(key) || unsafeCacheText(value) {
					return true
				}
			}
		}
		for _, edge := range path.Edges {
			if unsafeCacheText(string(edge.ID)) || unsafeCacheText(string(edge.From)) ||
				unsafeCacheText(string(edge.To)) || unsafeCacheText(edge.Type) {
				return true
			}
			for key, value := range edge.Properties {
				if unsafeCacheText(key) || unsafeCacheText(value) {
					return true
				}
			}
		}
	}
	return false
}

func unsafeCacheText(value string) bool {
	if !utf8.ValidString(value) {
		return true
	}
	lower := strings.ToLower(value)
	for _, marker := range []string{
		"api_key", "apikey", "access_token", "auth_token", "bearer ",
		"client_secret", "credential", "password", "private_key",
		"refresh_token", "secret",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func recordCacheBytes(record Record) int {
	total := len(record.Request.id) + len(record.Request.context.ID()) +
		len(record.Transcript.id) + len(record.Result.ID()) +
		len(record.Request.context.Query()) + len(record.Request.provenance.harness) +
		len(record.Request.provenance.toolPolicy) + 256
	total += len(canonicalBudgets(record.Request.budgets))
	total += len(modelIdentity(record.Request.provenance.model))
	total += len(record.Request.provenance.prompt.TemplateID()) +
		len(record.Request.provenance.prompt.Version()) +
		len(record.Request.provenance.prompt.Hash())
	for key, value := range record.Request.context.Metadata() {
		total += len(key) + len(value)
	}
	for _, anchor := range record.Request.context.Evidence() {
		total += anchorCacheBytes(anchor)
	}
	for _, exchange := range record.Transcript.exchanges {
		total += len(actionKey(exchange.action)) + len(exchange.action.correlation) + 32
		for _, anchor := range exchange.result.anchors {
			total += anchorCacheBytes(anchor)
		}
	}
	if record.Transcript.final != nil {
		total += len(actionKey(*record.Transcript.final)) + len(record.Transcript.final.correlation) + 32
	}
	for _, anchor := range record.Result.EvidenceAdditions() {
		total += anchorCacheBytes(anchor)
	}
	for _, claim := range record.Result.Claims() {
		total += len(claim.ID()) + len(claim.Subject()) + len(claim.Predicate()) + 64
		for _, id := range claim.EvidenceIDs() {
			total += len(id)
		}
		for key, value := range claim.Metadata() {
			total += len(key) + len(value)
		}
	}
	for _, issue := range append(record.Result.Unresolved(), record.Result.Unsupported()...) {
		total += len(issue.ID()) + len(issue.Input()) + len(issue.Reason()) + 32
		for _, id := range issue.EvidenceIDs() {
			total += len(id)
		}
	}
	total += len(record.Trace.Iterations)*96 + len(record.Trace.Failures)*96
	for _, failure := range record.Trace.Failures {
		total += len(failure.Operation) + len(failure.Error)
	}
	return total
}

func anchorCacheBytes(anchor inference.EvidenceAnchor) int {
	total := len(anchor.ID()) + len(anchor.Kind()) + 32
	if citation, quote, ok := anchor.Document(); ok {
		total += len(citation.DocumentID) + len(citation.RevisionID) +
			len(citation.SectionID) + len(citation.SpanID) + len(quote)
	}
	if path, ok := anchor.Path(); ok {
		for _, node := range path.Nodes {
			total += len(node.ID) + len(node.Kind)
			for _, label := range node.Labels {
				total += len(label)
			}
			for key, value := range node.Properties {
				total += len(key) + len(value)
			}
		}
		for _, edge := range path.Edges {
			total += len(edge.ID) + len(edge.From) + len(edge.To) + len(edge.Type) + 8
			for key, value := range edge.Properties {
				total += len(key) + len(value)
			}
		}
	}
	return total
}
