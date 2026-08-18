package namespaces

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type fakeLocator struct {
	mu    sync.Mutex
	data  map[string][]byte
	reads map[string]int
	err   error
}

func (f *fakeLocator) InstancePath() string { return "/accumulo/uuid-1" }

func (f *fakeLocator) GetRaw(ctx context.Context, znodePath string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads[znodePath]++
	if f.err != nil {
		return nil, f.err
	}
	return append([]byte(nil), f.data[znodePath]...), nil
}

func TestResolverBuiltinsAndCopies(t *testing.T) {
	locator := &fakeLocator{
		data: map[string][]byte{
			"/accumulo/uuid-1/namespaces": []byte(`{"+accumulo":"accumulo","+default":"","ns1":"analytics"}`),
		},
		reads: map[string]int{},
	}
	resolver := NewResolver(locator)

	id, err := resolver.ResolveID(context.Background(), "")
	if err != nil || id != "+default" {
		t.Fatalf(`ResolveID("") = %q, %v`, id, err)
	}
	id, err = resolver.ResolveID(context.Background(), "accumulo")
	if err != nil || id != "+accumulo" {
		t.Fatalf(`ResolveID("accumulo") = %q, %v`, id, err)
	}
	id, err = resolver.ResolveID(context.Background(), "analytics")
	if err != nil || id != "ns1" {
		t.Fatalf(`ResolveID("analytics") = %q, %v`, id, err)
	}
	name, err := resolver.ResolveName(context.Background(), "ns1")
	if err != nil || name != "analytics" {
		t.Fatalf(`ResolveName("ns1") = %q, %v`, name, err)
	}
	name, err = resolver.ResolveName(context.Background(), "+default")
	if err != nil || name != "" {
		t.Fatalf(`ResolveName("+default") = %q, %v`, name, err)
	}

	namespaces, err := resolver.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(namespaces) != 3 || namespaces[""] != "+default" || namespaces["accumulo"] != "+accumulo" || namespaces["analytics"] != "ns1" {
		t.Fatalf("List() = %#v", namespaces)
	}
	namespaces["analytics"] = "mutated"
	again, err := resolver.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if again["analytics"] != "ns1" {
		t.Fatalf("List() returned mutable cache state: %#v", again)
	}
	if locator.reads["/accumulo/uuid-1/namespaces"] != 1 {
		t.Fatalf("namespace map reads = %d, want 1", locator.reads["/accumulo/uuid-1/namespaces"])
	}

	locator.mu.Lock()
	locator.data["/accumulo/uuid-1/namespaces"] = []byte(`{"+accumulo":"accumulo","+default":"","ns1":"analytics","ns2":"ingest"}`)
	locator.mu.Unlock()
	resolver.Invalidate()
	again, err = resolver.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if again["ingest"] != "ns2" {
		t.Fatalf("List() after invalidation = %#v, want ingest namespace", again)
	}
	if locator.reads["/accumulo/uuid-1/namespaces"] != 2 {
		t.Fatalf("namespace map reads = %d, want 2 after invalidation", locator.reads["/accumulo/uuid-1/namespaces"])
	}
}

func TestResolverRefreshesOnMiss(t *testing.T) {
	locator := &fakeLocator{
		data: map[string][]byte{
			"/accumulo/uuid-1/namespaces": []byte(`{"+accumulo":"accumulo","+default":""}`),
		},
		reads: map[string]int{},
	}
	resolver := NewResolver(locator)

	if _, err := resolver.ResolveID(context.Background(), ""); err != nil {
		t.Fatal(err)
	}

	locator.mu.Lock()
	locator.data["/accumulo/uuid-1/namespaces"] = []byte(`{"+accumulo":"accumulo","+default":"","ns1":"analytics"}`)
	locator.mu.Unlock()

	id, err := resolver.ResolveID(context.Background(), "analytics")
	if err != nil || id != "ns1" {
		t.Fatalf(`ResolveID("analytics") = %q, %v`, id, err)
	}
	if locator.reads["/accumulo/uuid-1/namespaces"] != 2 {
		t.Fatalf("namespace map reads = %d, want 2 after refresh-on-miss", locator.reads["/accumulo/uuid-1/namespaces"])
	}
}

func TestResolverErrorsAndValidation(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{
			name: "empty znode",
			data: nil,
			want: "namespace mapping znode is empty",
		},
		{
			name: "malformed json",
			data: []byte("{"),
			want: "unexpected end of JSON input",
		},
		{
			name: "null map",
			data: []byte("null"),
			want: "namespace mapping znode decoded to null",
		},
		{
			name: "missing built in",
			data: []byte(`{"+default":""}`),
			want: `missing built-in namespace ID "+accumulo"`,
		},
		{
			name: "wrong accumulo id",
			data: []byte(`{"+accumulo":"system","+default":""}`),
			want: `expected built-in namespace ID "+accumulo" to map to "accumulo", got "system"`,
		},
		{
			name: "duplicate namespace names",
			data: []byte(`{"+accumulo":"accumulo","+default":"","ns1":"analytics","ns2":"analytics"}`),
			want: `duplicates namespace name "analytics"`,
		},
		{
			name: "default name under wrong id",
			data: []byte(`{"+accumulo":"accumulo","+default":"","ns1":""}`),
			want: `assigned the default namespace name to non-default ID "ns1"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := NewResolver(&fakeLocator{
				data:  map[string][]byte{"/accumulo/uuid-1/namespaces": tt.data},
				reads: map[string]int{},
			})
			_, err := resolver.List(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("List() error = %v, want substring %q", err, tt.want)
			}
		})
	}

	resolver := NewResolver(&fakeLocator{
		data:  map[string][]byte{"/accumulo/uuid-1/namespaces": []byte(`{"+accumulo":"accumulo","+default":""}`)},
		reads: map[string]int{},
	})
	if _, err := resolver.ResolveID(context.Background(), "missing"); !errors.Is(err, ErrNamespaceNotFound) {
		t.Fatalf("ResolveID error = %v, want ErrNamespaceNotFound", err)
	}
	if _, err := resolver.ResolveName(context.Background(), "missing"); !errors.Is(err, ErrNamespaceNotFound) {
		t.Fatalf("ResolveName error = %v, want ErrNamespaceNotFound", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.ResolveID(ctx, "accumulo"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveID error = %v, want context.Canceled", err)
	}
	if _, err := resolver.List(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("List error = %v, want context.Canceled", err)
	}
}

func TestResolverConcurrentAccessAndInvalidation(t *testing.T) {
	locator := &fakeLocator{
		data: map[string][]byte{
			"/accumulo/uuid-1/namespaces": []byte(`{"+accumulo":"accumulo","+default":"","ns1":"analytics","ns2":"ingest"}`),
		},
		reads: map[string]int{},
	}
	resolver := NewResolver(locator)

	const goroutines = 8
	const iterations = 200

	errCh := make(chan error, goroutines*iterations)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				switch worker % 4 {
				case 0:
					if id, err := resolver.ResolveID(context.Background(), "analytics"); err != nil || id != "ns1" {
						errCh <- fmt.Errorf("ResolveID failed: id=%q err=%v", id, err)
					}
				case 1:
					if name, err := resolver.ResolveName(context.Background(), "+default"); err != nil || name != "" {
						errCh <- fmt.Errorf("ResolveName failed: name=%q err=%v", name, err)
					}
				case 2:
					namespaces, err := resolver.List(context.Background())
					if err != nil || namespaces["accumulo"] != "+accumulo" {
						errCh <- fmt.Errorf("List failed: namespaces=%#v err=%v", namespaces, err)
					}
				default:
					resolver.Invalidate()
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}
