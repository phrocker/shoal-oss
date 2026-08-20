package tserverprocess

import (
	"context"
	"errors"
	"testing"

	"github.com/phrocker/shoal/internal/metadata"
	"github.com/phrocker/shoal/internal/tabletloader"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/tserver"
)

type tableLocatorFunc func(context.Context, string) ([]metadata.TabletInfo, error)

func (f tableLocatorFunc) LocateTable(ctx context.Context, table string) ([]metadata.TabletInfo, error) {
	return f(ctx, table)
}

func TestMetadataSourceRequiresExactExtentAddressAndGeneration(t *testing.T) {
	extent := tserver.Extent{TableID: "5", PrevEndRow: []byte("a"), EndRow: []byte("m")}
	source := MetadataSource{
		Address: "shoal:9997",
		Locator: tableLocatorFunc(func(context.Context, string) ([]metadata.TabletInfo, error) {
			return []metadata.TabletInfo{{
				TableID: "5", PrevRow: []byte("a"), EndRow: []byte("m"),
				FutureLocation: &metadata.Location{HostPort: "shoal:9997", Session: "zlock#g#0000000001"},
			}}, nil
		}),
	}
	got, err := source.ReadTablet(context.Background(), extent)
	if err != nil || got.Generation != "zlock#g#0000000001" {
		t.Fatalf("ReadTablet = %#v, %v", got, err)
	}
	source.Address = "other:9997"
	if _, err := source.ReadTablet(context.Background(), extent); !errors.Is(err, tabletloader.ErrStaleGeneration) {
		t.Fatalf("stale address error = %v", err)
	}
	if _, err := source.ReadTablet(context.Background(), tserver.Extent{TableID: "5"}); !errors.Is(err, tabletloader.ErrMissingMetadata) {
		t.Fatalf("wrong extent error = %v", err)
	}
}

func TestExactCredentialsRejectsAnyMismatch(t *testing.T) {
	validator := ExactCredentials{Principal: "!SYSTEM", TokenType: "SystemToken", Token: []byte{1, 2, 3}}
	valid := &security.TCredentials{Principal: "!SYSTEM", TokenClassName: "SystemToken", Token: []byte{1, 2, 3}}
	if err := validator.Validate(context.Background(), valid, "load"); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []*security.TCredentials{
		nil,
		{Principal: "root", TokenClassName: "SystemToken", Token: []byte{1, 2, 3}},
		{Principal: "!SYSTEM", TokenClassName: "PasswordToken", Token: []byte{1, 2, 3}},
		{Principal: "!SYSTEM", TokenClassName: "SystemToken", Token: []byte{1, 2, 4}},
	} {
		if err := validator.Validate(context.Background(), invalid, "load"); err == nil {
			t.Fatalf("accepted %#v", invalid)
		}
	}
}
