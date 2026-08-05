package tablenames

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type fakeLocator struct {
	mu    sync.Mutex
	data  map[string][]byte
	reads map[string]int
}

func (f *fakeLocator) InstancePath() string { return "/accumulo/uuid-1" }

func (f *fakeLocator) GetRaw(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads[path]++
	return append([]byte(nil), f.data[path]...), nil
}

func TestResolverNamesAndNamespaces(t *testing.T) {
	locator := &fakeLocator{
		data: map[string][]byte{
			"/accumulo/uuid-1/namespaces":                 []byte(`{"+default":"","ns1":"analytics"}`),
			"/accumulo/uuid-1/namespaces/+default/tables": []byte(`{"1":"events"}`),
			"/accumulo/uuid-1/namespaces/ns1/tables":      []byte(`{"2":"events"}`),
		},
		reads: map[string]int{},
	}
	resolver := NewResolver(locator)

	id, err := resolver.ResolveID(context.Background(), "events")
	if err != nil || id != "1" {
		t.Fatalf("ResolveID(events) = %q, %v", id, err)
	}
	id, err = resolver.ResolveID(context.Background(), "analytics.events")
	if err != nil || id != "2" {
		t.Fatalf("ResolveID(analytics.events) = %q, %v", id, err)
	}
	name, err := resolver.ResolveName(context.Background(), "2")
	if err != nil || name != "analytics.events" {
		t.Fatalf("ResolveName(2) = %q, %v", name, err)
	}
	tables, err := resolver.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 || tables["events"] != "1" || tables["analytics.events"] != "2" {
		t.Fatalf("List() = %#v", tables)
	}
	tables["events"] = "mutated"
	again, err := resolver.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if again["events"] != "1" {
		t.Fatalf("List() returned mutable cache state: %#v", again)
	}
	if _, err := resolver.ResolveName(context.Background(), "missing"); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("ResolveName(missing) error = %v, want ErrTableNotFound", err)
	}

	namespaces := "/accumulo/uuid-1/namespaces"
	defaultTables := "/accumulo/uuid-1/namespaces/+default/tables"
	analyticsTables := "/accumulo/uuid-1/namespaces/ns1/tables"
	if got := locator.reads[namespaces]; got != 1 {
		t.Fatalf("namespace map reads = %d, want 1", got)
	}
	if got := locator.reads[analyticsTables]; got != 1 {
		t.Fatalf("analytics table map reads = %d, want 1", got)
	}
	resolver.ResolveID(context.Background(), "events")
	if got := locator.reads[defaultTables]; got != 1 {
		t.Fatalf("default table map reads = %d, want 1 cache hit", got)
	}
	resolver.Invalidate()
	resolver.ResolveID(context.Background(), "events")
	if got := locator.reads[defaultTables]; got != 2 {
		t.Fatalf("default table map reads = %d, want 2 after invalidation", got)
	}
	if got := locator.reads[namespaces]; got != 2 {
		t.Fatalf("namespace map reads = %d, want 2 after invalidation", got)
	}

	locator.mu.Lock()
	locator.data[defaultTables] = []byte(`{"1":"events","3":"metrics"}`)
	locator.mu.Unlock()
	resolver.Invalidate()
	tables, err = resolver.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tables["metrics"] != "3" {
		t.Fatalf("List() after invalidation = %#v, want metrics table", tables)
	}
}

func TestResolverErrorsAndCancellation(t *testing.T) {
	locator := &fakeLocator{
		data: map[string][]byte{
			"/accumulo/uuid-1/namespaces": []byte(`{"+default":""}`),
		},
		reads: map[string]int{},
	}
	resolver := NewResolver(locator)
	if _, err := resolver.ResolveID(context.Background(), "missing"); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("ResolveID error = %v, want ErrTableNotFound", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.ResolveName(ctx, "1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveName error = %v, want context.Canceled", err)
	}
	if _, err := resolver.List(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("List error = %v, want context.Canceled", err)
	}
}
