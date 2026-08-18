// Package tablenames resolves Accumulo table names and IDs from ZooKeeper.
package tablenames

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"

	nslookup "github.com/phrocker/shoal/internal/namespaces"
)

const defaultNamespaceName = ""

// ErrTableNotFound indicates that no table mapping matched a name or ID.
var ErrTableNotFound = errors.New("table not found")

// Locator is the ZooKeeper subset needed for table-name resolution.
type Locator interface {
	InstancePath() string
	GetRaw(context.Context, string) ([]byte, error)
}

// NamespaceResolver is the shared namespace lookup dependency needed for
// table-name resolution.
type NamespaceResolver interface {
	ResolveID(context.Context, string) (string, error)
	List(context.Context) (map[string]string, error)
}

// Resolver caches table name and ID mappings until explicitly invalidated.
type Resolver struct {
	locator    Locator
	namespaces NamespaceResolver

	opMu     sync.Mutex
	mu       sync.RWMutex
	nameToID map[string]string
	idToName map[string]string

	loadedNamespaces map[string]struct{}
}

// NewResolver creates a table-name resolver backed by locator and the shared
// namespace resolver.
func NewResolver(locator Locator, namespaces NamespaceResolver) *Resolver {
	if locator == nil {
		panic("tablenames.NewResolver: nil Locator")
	}
	if namespaces == nil {
		panic("tablenames.NewResolver: nil NamespaceResolver")
	}
	return &Resolver{
		locator:          locator,
		namespaces:       namespaces,
		nameToID:         map[string]string{},
		idToName:         map[string]string{},
		loadedNamespaces: map[string]struct{}{},
	}
}

// ResolveID resolves a qualified or default-namespace table name to its ID.
func (r *Resolver) ResolveID(ctx context.Context, tableName string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.opMu.Lock()
	defer r.opMu.Unlock()
	r.mu.RLock()
	if id, ok := r.nameToID[tableName]; ok {
		r.mu.RUnlock()
		return id, nil
	}
	r.mu.RUnlock()

	namespaceName, rawName := splitQualifiedName(tableName)
	namespaceID, err := r.namespaces.ResolveID(ctx, namespaceName)
	if err != nil {
		if errors.Is(err, nslookup.ErrNamespaceNotFound) {
			return "", fmt.Errorf("%w: table %q: %v", ErrTableNotFound, tableName, err)
		}
		return "", err
	}
	if err := r.loadNamespace(ctx, namespaceID, namespaceName); err != nil {
		return "", err
	}

	r.mu.RLock()
	id, ok := r.nameToID[tableName]
	r.mu.RUnlock()
	if ok {
		return id, nil
	}

	qualifiedName := rawName
	if namespaceName != defaultNamespaceName {
		qualifiedName = namespaceName + "." + rawName
	}
	r.mu.RLock()
	id, ok = r.nameToID[qualifiedName]
	r.mu.RUnlock()
	if ok {
		return id, nil
	}
	return "", fmt.Errorf("%w: table %q in namespace %q", ErrTableNotFound, tableName, namespaceName)
}

// ResolveName resolves a table ID to its qualified operator-facing name.
func (r *Resolver) ResolveName(ctx context.Context, tableID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.opMu.Lock()
	defer r.opMu.Unlock()
	r.mu.RLock()
	if name, ok := r.idToName[tableID]; ok {
		r.mu.RUnlock()
		return name, nil
	}
	r.mu.RUnlock()

	namespaces, err := r.namespaces.List(ctx)
	if err != nil {
		return "", err
	}
	for namespaceName, namespaceID := range namespaces {
		if err := r.loadNamespace(ctx, namespaceID, namespaceName); err != nil {
			return "", err
		}
		r.mu.RLock()
		name, ok := r.idToName[tableID]
		r.mu.RUnlock()
		if ok {
			return name, nil
		}
	}
	return "", fmt.Errorf("%w: table id %q", ErrTableNotFound, tableID)
}

// List returns every qualified table name mapped to its table ID.
func (r *Resolver) List(ctx context.Context) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.opMu.Lock()
	defer r.opMu.Unlock()
	namespaces, err := r.namespaces.List(ctx)
	if err != nil {
		return nil, err
	}
	for namespaceName, namespaceID := range namespaces {
		if err := r.loadNamespace(ctx, namespaceID, namespaceName); err != nil {
			return nil, err
		}
	}
	r.mu.RLock()
	tables := cloneMapping(r.nameToID)
	r.mu.RUnlock()
	return tables, nil
}

// Invalidate clears all cached table mappings. Callers that share a
// NamespaceResolver should invalidate that separately when the namespace map
// itself may have changed.
func (r *Resolver) Invalidate() {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	r.mu.Lock()
	r.nameToID = map[string]string{}
	r.idToName = map[string]string{}
	r.loadedNamespaces = map[string]struct{}{}
	r.mu.Unlock()
}

func (r *Resolver) loadNamespace(ctx context.Context, namespaceID, namespaceName string) error {
	r.mu.RLock()
	_, loaded := r.loadedNamespaces[namespaceID]
	r.mu.RUnlock()
	if loaded {
		return nil
	}

	tablesPath := path.Join(r.locator.InstancePath(), "namespaces", namespaceID, "tables")
	data, err := r.locator.GetRaw(ctx, tablesPath)
	if err != nil {
		return fmt.Errorf("get %s: %w", tablesPath, err)
	}
	idToRawName, err := decodeMappingJSON(data)
	if err != nil {
		return fmt.Errorf("decode %s: %w", tablesPath, err)
	}

	r.mu.Lock()
	for id, rawName := range idToRawName {
		name := rawName
		if namespaceName != defaultNamespaceName {
			name = namespaceName + "." + rawName
		}
		r.nameToID[name] = id
		r.idToName[id] = name
	}
	r.loadedNamespaces[namespaceID] = struct{}{}
	r.mu.Unlock()
	return nil
}

func cloneMapping(mapping map[string]string) map[string]string {
	cloned := make(map[string]string, len(mapping))
	for key, value := range mapping {
		cloned[key] = value
	}
	return cloned
}

func splitQualifiedName(tableName string) (namespace, raw string) {
	if i := strings.IndexByte(tableName, '.'); i >= 0 {
		return tableName[:i], tableName[i+1:]
	}
	return defaultNamespaceName, tableName
}

func decodeMappingJSON(data []byte) (map[string]string, error) {
	mapping := map[string]string{}
	if len(data) == 0 {
		return mapping, nil
	}
	if err := json.Unmarshal(data, &mapping); err != nil {
		return nil, err
	}
	return mapping, nil
}
