package accumulo_test

import (
	"context"
	"errors"
	"testing"

	"github.com/phrocker/shoal/accumulo"
)

func TestPublicBootstrapAPICompiles(t *testing.T) {
	instance, err := accumulo.NewStaticInstance("accumulo", "uuid-1")
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := accumulo.PasswordCredentials("root", []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	connector, err := accumulo.NewConnector(instance, credentials, accumulo.ConnectorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := connector.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPublicDiscoveryAPICompiles(t *testing.T) {
	instance, _ := accumulo.NewStaticInstance("accumulo", "uuid-1")
	credentials, _ := accumulo.PasswordCredentials("root", []byte("secret"))
	connector, err := accumulo.NewConnector(instance, credentials, accumulo.ConnectorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	table := accumulo.Table{Name: "events", ID: "1"}
	_, _ = connector.Tablets(context.Background(), table)
	_, _ = connector.LocateTablet(context.Background(), table, []byte("row"))
	_ = connector.InvalidateTablet(table, []byte("row"))
	_ = connector.InvalidateTable(table)
	_ = connector.InvalidateDiscovery()
	if _, err := connector.TableByName(context.Background(), "events"); !errors.Is(err, accumulo.ErrDiscoveryUnavailable) {
		t.Fatalf("error = %v, want ErrDiscoveryUnavailable", err)
	}
}

func TestPublicScannerAPICompiles(t *testing.T) {
	instance, _ := accumulo.NewStaticInstance("accumulo", "uuid-1")
	credentials, _ := accumulo.PasswordCredentials("root", []byte("secret"))
	connector, err := accumulo.NewConnector(instance, credentials, accumulo.ConnectorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	scanRange, err := accumulo.NewRange([]byte("a"), true, []byte("z"), false)
	if err != nil {
		t.Fatal(err)
	}
	_ = scanRange.StartRow()
	_ = scanRange.EndRow()
	_ = scanRange.StartInclusive()
	_ = scanRange.EndInclusive()
	_, _ = accumulo.NewRangeRow([]byte("row"))
	_ = accumulo.InfiniteRange()

	_, err = connector.NewScanner(accumulo.Table{Name: "events"}, accumulo.ScannerOptions{
		BatchSize:      128,
		Authorizations: [][]byte{[]byte("public")},
	})
	if !errors.Is(err, accumulo.ErrDiscoveryUnavailable) {
		t.Fatalf("error = %v, want ErrDiscoveryUnavailable", err)
	}

	var _ accumulo.Key
	var _ accumulo.KeyValue
	var _ *accumulo.Scanner
	var _ *accumulo.CleanupError
	var _ func(*accumulo.Scanner, context.Context, *accumulo.Range) ([]accumulo.KeyValue, error) = (*accumulo.Scanner).Scan
	_ = accumulo.ErrRangeSpansTablets
}
