package accumulo

import (
	"context"
	"errors"
	"testing"
)

func TestTableAdministrationListingAndExistence(t *testing.T) {
	names := &fakeTableNames{
		byName: map[string]string{
			"events":           "1",
			"analytics.events": "2",
			"accumulo.root":    "+r",
		},
		byID: map[string]string{},
	}
	connector := testConnectorWithDiscovery(t, &fakeTabletWalker{}, names)

	tables, err := connector.Tables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Table{
		{Name: "accumulo.root", ID: "+r"},
		{Name: "analytics.events", ID: "2"},
		{Name: "events", ID: "1"},
	}
	if len(tables) != len(want) {
		t.Fatalf("Tables() = %#v, want %#v", tables, want)
	}
	for i := range want {
		if tables[i] != want[i] {
			t.Fatalf("Tables()[%d] = %#v, want %#v", i, tables[i], want[i])
		}
	}

	exists, err := connector.TableExists(context.Background(), "analytics.events")
	if err != nil || !exists {
		t.Fatalf("TableExists(analytics.events) = %v, %v", exists, err)
	}
	exists, err = connector.TableExists(context.Background(), "missing")
	if err != nil || exists {
		t.Fatalf("TableExists(missing) = %v, %v", exists, err)
	}
	exists, err = connector.TableExists(context.Background(), "")
	if exists || !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("TableExists(empty) = %v, %v, want false/ErrTableNotFound", exists, err)
	}
	if err := connector.InvalidateDiscovery(); err != nil {
		t.Fatal(err)
	}
	if names.invalidates != 1 {
		t.Fatalf("table-name invalidations = %d, want 1", names.invalidates)
	}
}

func TestTableAdministrationLifecycleAndCancellation(t *testing.T) {
	instance, _ := NewStaticInstance("accumulo", "uuid-1")
	credentials, _ := PasswordCredentials("root", []byte("secret"))
	connector, err := NewConnector(instance, credentials, ConnectorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connector.Tables(context.Background()); !errors.Is(err, ErrDiscoveryUnavailable) {
		t.Fatalf("Tables static error = %v, want ErrDiscoveryUnavailable", err)
	}
	if _, err := connector.TableExists(context.Background(), "events"); !errors.Is(err, ErrDiscoveryUnavailable) {
		t.Fatalf("TableExists static error = %v, want ErrDiscoveryUnavailable", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := connector.Tables(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Tables canceled error = %v, want context.Canceled", err)
	}
	if _, err := connector.TableExists(ctx, "events"); !errors.Is(err, context.Canceled) {
		t.Fatalf("TableExists canceled error = %v, want context.Canceled", err)
	}

	if err := connector.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := connector.Tables(context.Background()); !errors.Is(err, ErrConnectorClosed) {
		t.Fatalf("Tables closed error = %v, want ErrConnectorClosed", err)
	}
	if _, err := connector.TableExists(context.Background(), "events"); !errors.Is(err, ErrConnectorClosed) {
		t.Fatalf("TableExists closed error = %v, want ErrConnectorClosed", err)
	}
}
