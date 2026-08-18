package accumulo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal/internal/metadata"
	nslookup "github.com/phrocker/shoal/internal/namespaces"
	"github.com/phrocker/shoal/internal/tablenames"
	"github.com/phrocker/shoal/internal/zk"
)

type fakeTabletWalker struct {
	mu      sync.Mutex
	tablets map[string][]metadata.TabletInfo
	calls   int
	wait    bool
	err     error
}

func (f *fakeTabletWalker) LocateTable(ctx context.Context, tableID string) ([]metadata.TabletInfo, error) {
	if f.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return append([]metadata.TabletInfo(nil), f.tablets[tableID]...), nil
}

type fakeTableNames struct {
	mu           sync.Mutex
	byName       map[string]string
	byID         map[string]string
	resolveIDErr error
	listErr      error
	invalidates  int
	onInvalidate func()
}

func (f *fakeTableNames) ResolveID(ctx context.Context, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.resolveIDErr != nil {
		return "", f.resolveIDErr
	}
	id, ok := f.byName[name]
	if !ok {
		return "", fmt.Errorf("%w: missing fake table", tablenames.ErrTableNotFound)
	}
	return id, nil
}

func (f *fakeTableNames) ResolveName(ctx context.Context, id string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	name, ok := f.byID[id]
	if !ok {
		return "", fmt.Errorf("%w: missing fake table", tablenames.ErrTableNotFound)
	}
	return name, nil
}

func (f *fakeTableNames) List(ctx context.Context) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	tables := make(map[string]string, len(f.byName))
	for name, id := range f.byName {
		tables[name] = id
	}
	return tables, nil
}

func (f *fakeTableNames) ListNamespace(
	ctx context.Context,
	namespace string,
) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	tables := make(map[string]string)
	prefix := namespace + "."
	for name, id := range f.byName {
		if namespace == "" {
			if !strings.ContainsRune(name, '.') {
				tables[name] = id
			}
			continue
		}
		if strings.HasPrefix(name, prefix) {
			tables[name] = id
		}
	}
	return tables, nil
}

func (f *fakeTableNames) Invalidate() {
	f.mu.Lock()
	f.invalidates++
	onInvalidate := f.onInvalidate
	f.mu.Unlock()
	if onInvalidate != nil {
		onInvalidate()
	}
}

type fakeNamespaces struct {
	mu           sync.Mutex
	byName       map[string]string
	byID         map[string]string
	invalidates  int
	onInvalidate func()
}

func (f *fakeNamespaces) ResolveID(ctx context.Context, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.byName[name]
	if !ok {
		return "", fmt.Errorf("%w: missing fake namespace", nslookup.ErrNamespaceNotFound)
	}
	return id, nil
}

func (f *fakeNamespaces) ResolveName(ctx context.Context, id string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	name, ok := f.byID[id]
	if !ok {
		return "", fmt.Errorf("%w: missing fake namespace", nslookup.ErrNamespaceNotFound)
	}
	return name, nil
}

func (f *fakeNamespaces) List(ctx context.Context) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	namespaces := make(map[string]string, len(f.byName))
	for name, id := range f.byName {
		namespaces[name] = id
	}
	return namespaces, nil
}

func (f *fakeNamespaces) Invalidate() {
	f.mu.Lock()
	f.invalidates++
	onInvalidate := f.onInvalidate
	f.mu.Unlock()
	if onInvalidate != nil {
		onInvalidate()
	}
}

func newFakeNamespacesFromTables(tables *fakeTableNames) *fakeNamespaces {
	namespaces := &fakeNamespaces{
		byName: map[string]string{
			"":         "+default",
			"accumulo": "+accumulo",
		},
		byID: map[string]string{
			"+default":  "",
			"+accumulo": "accumulo",
		},
	}
	if tables == nil {
		return namespaces
	}
	tables.mu.Lock()
	defer tables.mu.Unlock()
	nextID := 1
	for fullName := range tables.byName {
		namespace := ""
		if cut := strings.IndexByte(fullName, '.'); cut >= 0 {
			namespace = fullName[:cut]
		}
		if _, ok := namespaces.byName[namespace]; ok {
			continue
		}
		id := fmt.Sprintf("ns%d", nextID)
		nextID++
		namespaces.byName[namespace] = id
		namespaces.byID[id] = namespace
	}
	return namespaces
}

func testConnectorWithDiscovery(
	t *testing.T,
	walker interface {
		LocateTable(context.Context, string) ([]metadata.TabletInfo, error)
	},
	names *fakeTableNames,
) *Connector {
	return testConnectorWithNamespaceDiscoveryAndState(
		t,
		walker,
		newFakeNamespacesFromTables(names),
		names,
		nil,
	)
}

func testConnectorWithDiscoveryAndState(
	t *testing.T,
	walker interface {
		LocateTable(context.Context, string) ([]metadata.TabletInfo, error)
	},
	names *fakeTableNames,
	state tableStateReader,
) *Connector {
	return testConnectorWithNamespaceDiscoveryAndState(
		t,
		walker,
		newFakeNamespacesFromTables(names),
		names,
		state,
	)
}

func testConnectorWithNamespaceDiscovery(
	t *testing.T,
	walker interface {
		LocateTable(context.Context, string) ([]metadata.TabletInfo, error)
	},
	namespaces *fakeNamespaces,
	names *fakeTableNames,
) *Connector {
	return testConnectorWithNamespaceDiscoveryAndState(t, walker, namespaces, names, nil)
}

func testConnectorWithNamespaceDiscoveryAndState(
	t *testing.T,
	walker interface {
		LocateTable(context.Context, string) ([]metadata.TabletInfo, error)
	},
	namespaces *fakeNamespaces,
	names *fakeTableNames,
	state tableStateReader,
) *Connector {
	t.Helper()
	instance, err := NewStaticInstance("accumulo", "uuid-1")
	if err != nil {
		t.Fatal(err)
	}

	credentials, err := PasswordCredentials("root", []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	connector, err := NewConnector(instance, credentials, ConnectorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if names == nil {
		names = &fakeTableNames{
			byName: map[string]string{},
			byID:   map[string]string{},
		}
	}
	if namespaces == nil {
		namespaces = newFakeNamespacesFromTables(names)
	}
	connector.discovery = newConnectorDiscovery(walker, namespaces, names, state)
	t.Cleanup(func() { _ = connector.Close() })
	return connector
}

func discoveryTablets() []metadata.TabletInfo {
	return []metadata.TabletInfo{
		{TableID: "1", EndRow: []byte("k"), Location: &metadata.Location{HostPort: "ts1:9997", Session: "a"}},
		{TableID: "1", PrevRow: []byte("k"), EndRow: []byte("p"), Location: &metadata.Location{HostPort: "ts2:9997", Session: "b"}},
		{TableID: "1", PrevRow: []byte("p"), Location: &metadata.Location{HostPort: "ts3:9997", Session: "c"}},
	}
}

func TestDiscoveryTableLookupAndRouting(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	names := &fakeTableNames{
		byName: map[string]string{"analytics.events": "1"},
		byID:   map[string]string{"1": "analytics.events"},
	}
	connector := testConnectorWithDiscovery(t, walker, names)

	table, err := connector.TableByName(context.Background(), "analytics.events")
	if err != nil || table != (Table{Name: "analytics.events", ID: "1"}) {
		t.Fatalf("TableByName = %+v, %v", table, err)
	}
	if got, err := connector.TableByID(context.Background(), "1"); err != nil || got != table {
		t.Fatalf("TableByID = %+v, %v", got, err)
	}

	cases := []struct {
		row  string
		host string
	}{
		{"a", "ts1:9997"},
		{"k", "ts1:9997"},
		{"k\x00", "ts2:9997"},
		{"p", "ts2:9997"},
		{"p\x00", "ts3:9997"},
	}
	for _, tc := range cases {
		tablet, err := connector.LocateTablet(context.Background(), table, []byte(tc.row))
		if err != nil {
			t.Fatalf("LocateTablet(%q): %v", tc.row, err)
		}
		if tablet.Server.HostPort != tc.host {
			t.Fatalf("LocateTablet(%q) host = %q, want %q", tc.row, tablet.Server.HostPort, tc.host)
		}
	}
	if walker.calls != 1 {
		t.Fatalf("walker calls = %d, want 1 cache population", walker.calls)
	}
}

func TestDiscoveryInvalidationAndDefensiveCopies(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{"1": discoveryTablets()}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	table := Table{Name: "events", ID: "1"}

	tablets, err := connector.Tablets(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	tablets[0].Extent.EndRow[0] = 'z'
	tablets[0].Server.HostPort = "mutated"
	again, err := connector.Tablets(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	if string(again[0].Extent.EndRow) != "k" || again[0].Server.HostPort != "ts1:9997" {
		t.Fatalf("public mutation leaked into cache: %+v", again[0])
	}

	walker.mu.Lock()
	walker.tablets["1"][0].Location = &metadata.Location{HostPort: "moved:9997", Session: "new"}
	walker.mu.Unlock()
	if err := connector.InvalidateTablet(table, []byte("a")); err != nil {
		t.Fatal(err)
	}
	tablet, err := connector.LocateTablet(context.Background(), table, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if tablet.Server.HostPort != "moved:9997" || walker.calls != 2 {
		t.Fatalf("post-invalidation tablet=%+v calls=%d", tablet, walker.calls)
	}
}

func TestDiscoveryPreservesEmptyRowBoundaries(t *testing.T) {
	walker := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{
		"1": {
			{
				TableID: "1",
				PrevRow: []byte{},
				EndRow:  []byte{},
				Location: &metadata.Location{
					HostPort: "ts1:9997",
				},
			},
		},
	}}
	connector := testConnectorWithDiscovery(t, walker, &fakeTableNames{})

	tablets, err := connector.Tablets(context.Background(), Table{ID: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if tablets[0].Extent.PrevRow == nil || tablets[0].Extent.EndRow == nil {
		t.Fatalf("empty boundaries collapsed to nil: %+v", tablets[0].Extent)
	}
	if len(tablets[0].Extent.PrevRow) != 0 || len(tablets[0].Extent.EndRow) != 0 {
		t.Fatalf("empty boundaries changed length: %+v", tablets[0].Extent)
	}
}

func TestDiscoveryErrorsAndCancellation(t *testing.T) {
	static, _ := NewStaticInstance("accumulo", "uuid-1")
	credentials, _ := PasswordCredentials("root", []byte("secret"))
	connector, err := NewConnector(static, credentials, ConnectorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	if _, err := connector.TableByName(context.Background(), "events"); !errors.Is(err, ErrDiscoveryUnavailable) {
		t.Fatalf("static discovery error = %v", err)
	}

	noLocation := &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{
		"1": {{TableID: "1"}},
	}}
	withDiscovery := testConnectorWithDiscovery(t, noLocation, &fakeTableNames{
		byName: map[string]string{"events": "1"},
		byID:   map[string]string{"1": "events"},
	})
	if _, err := withDiscovery.LocateTablet(context.Background(), Table{ID: "1"}, []byte("a")); !errors.Is(err, ErrTabletNotLocated) {
		t.Fatalf("missing location error = %v", err)
	}
	if _, err := withDiscovery.LocateTablet(context.Background(), Table{ID: "missing"}, []byte("a")); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("missing table error = %v", err)
	}
	if _, err := withDiscovery.TableByName(context.Background(), "missing"); err == nil ||
		!errors.Is(err, ErrTableNotFound) || err.Error() != `accumulo: table not found: table name "missing"` {
		t.Fatalf("missing table name error = %v", err)
	}
	withDiscovery.discovery.tablets.InvalidateAll()
	noLocation.tablets["missing"] = []metadata.TabletInfo{{TableID: "missing"}}
	if _, err := withDiscovery.TableByID(context.Background(), "missing"); err == nil ||
		!errors.Is(err, ErrTableNotFound) || err.Error() != `accumulo: table not found: table ID "missing"` {
		t.Fatalf("missing table ID error = %v", err)
	}

	gapped := testConnectorWithDiscovery(t, &fakeTabletWalker{tablets: map[string][]metadata.TabletInfo{
		"1": {{TableID: "1", PrevRow: []byte("k"), EndRow: []byte("p"), Location: &metadata.Location{HostPort: "ts:9997"}}},
	}}, &fakeTableNames{})
	if _, err := gapped.LocateTablet(context.Background(), Table{ID: "1"}, []byte("z")); !errors.Is(err, ErrNoTabletCoversRow) {
		t.Fatalf("uncovered row error = %v", err)
	}

	blocked := testConnectorWithDiscovery(t, &fakeTabletWalker{wait: true}, &fakeTableNames{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := blocked.Tablets(ctx, Table{ID: "1"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation error = %v", err)
	}
}

type lifecycleLocator struct {
	closes int
}

func (l *lifecycleLocator) InstanceID() string { return "uuid-1" }
func (l *lifecycleLocator) RootTabletLocation(context.Context) (*zk.Location, error) {
	return nil, nil
}
func (l *lifecycleLocator) InstancePath() string { return "/accumulo/uuid-1" }
func (l *lifecycleLocator) GetRaw(context.Context, string) ([]byte, error) {
	return nil, nil
}
func (l *lifecycleLocator) Children(context.Context, string) ([]string, error) { return nil, nil }

type lifecycleInstance struct {
	locator *lifecycleLocator
	closes  int
}

func (i *lifecycleInstance) Info() InstanceInfo {
	return InstanceInfo{Name: "accumulo", ID: "uuid-1"}
}
func (i *lifecycleInstance) Close() error {
	i.closes++
	i.locator.closes++
	return nil
}
func (i *lifecycleInstance) discoveryLocator() discoveryLocator { return i.locator }

func TestConnectorDiscoveryLifecycleDoesNotCloseInstance(t *testing.T) {
	instance := &lifecycleInstance{locator: &lifecycleLocator{}}
	credentials, _ := PasswordCredentials("root", []byte("secret"))
	connector, err := NewConnector(instance, credentials, ConnectorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if connector.discovery == nil {
		t.Fatal("ZooKeeper-backed instance discovery was not preserved")
	}
	if err := connector.Close(); err != nil {
		t.Fatal(err)
	}
	if instance.closes != 0 || instance.locator.closes != 0 {
		t.Fatalf("connector closed user instance: instance=%d locator=%d", instance.closes, instance.locator.closes)
	}
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
	if instance.closes != 1 || instance.locator.closes != 1 {
		t.Fatalf("instance close counts = %d/%d, want 1/1", instance.closes, instance.locator.closes)
	}
	if _, err := connector.TableByID(context.Background(), "1"); !errors.Is(err, ErrConnectorClosed) {
		t.Fatalf("closed connector error = %v", err)
	}
}

func TestDiscoveryInvalidatesNamespacesBeforeDependentTables(t *testing.T) {
	var order []string
	namespaces := &fakeNamespaces{
		byName: map[string]string{},
		byID:   map[string]string{},
		onInvalidate: func() {
			order = append(order, "namespaces")
		},
	}
	tables := &fakeTableNames{
		byName: map[string]string{},
		byID:   map[string]string{},
		onInvalidate: func() {
			order = append(order, "tables")
		},
	}
	connector := testConnectorWithNamespaceDiscovery(t, &fakeTabletWalker{}, namespaces, tables)

	if err := connector.InvalidateDiscovery(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(order, ","); got != "namespaces,tables" {
		t.Fatalf("invalidation order = %q", got)
	}
}
