package tserverprocess

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/phrocker/shoal/internal/tabletloader"
	"github.com/phrocker/shoal/internal/tserver"
	"github.com/phrocker/shoal/internal/tserverrpc"
)

type loaderFunc func(context.Context, tserver.Extent) (tabletloader.Specification, error)

func (f loaderFunc) Load(ctx context.Context, extent tserver.Extent) (tabletloader.Specification, error) {
	return f(ctx, extent)
}

func TestStorePublishesOnlySuccessfulLoadsAndUnhostsIdempotently(t *testing.T) {
	extent := tserver.Extent{TableID: "5", EndRow: []byte("m")}
	store, err := NewStore(loaderFunc(func(context.Context, tserver.Extent) (tabletloader.Specification, error) {
		return tabletloader.Specification{
			Extent: extent, Generation: "g", Directory: "t-1", Time: "M0",
			Files: []tabletloader.DataFile{{Path: "file:///a.rf"}},
		}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LocateTable(context.Background(), "5"); !errors.Is(err, ErrNotHosted) {
		t.Fatalf("pre-load LocateTable error = %v", err)
	}
	if err := store.Load(context.Background(), extent); err != nil {
		t.Fatal(err)
	}
	tablets, err := store.LocateTable(context.Background(), "5")
	if err != nil || len(tablets) != 1 || tablets[0].Files[0].Path != "file:///a.rf" {
		t.Fatalf("hosted tablets = %#v, %v", tablets, err)
	}
	if err := store.Flush(context.Background(), extent); err != nil {
		t.Fatal(err)
	}
	if err := store.Unload(context.Background(), extent, tserverrpc.UnloadUnassigned); err != nil {
		t.Fatal(err)
	}
	if err := store.Unload(context.Background(), extent, tserverrpc.UnloadUnassigned); !errors.Is(err, tserverrpc.ErrNotServing) {
		t.Fatalf("duplicate unload error = %v", err)
	}
}

func TestStoreRejectsWALAndUnsupportedIteratorBeforePublishing(t *testing.T) {
	extent := tserver.Extent{TableID: "5"}
	tests := []tabletloader.Specification{
		{Extent: extent, Logs: []tabletloader.Log{{UUID: "wal-1"}}},
		{Extent: extent, Properties: []tabletloader.Property{{
			Name: "table.iterator.scan.bad", Value: "10,example.Unknown",
		}}},
		{Extent: extent, Properties: []tabletloader.Property{
			{Name: "table.iterator.scan.vers", Value: "20,org.apache.accumulo.core.iterators.user.VersioningIterator"},
			{Name: "table.iterator.scan.vers.opt.maxVersions", Value: "2"},
		}},
	}
	for _, spec := range tests {
		store, _ := NewStore(loaderFunc(func(context.Context, tserver.Extent) (tabletloader.Specification, error) {
			return spec, nil
		}))
		if err := store.Load(context.Background(), extent); err == nil {
			t.Fatal("unsafe specification unexpectedly loaded")
		}
		if len(store.Hosted()) != 0 {
			t.Fatal("failed load became scan-visible")
		}
	}
}

func TestStoreStaleCanceledLoadCannotReplaceSuccessor(t *testing.T) {
	extent := tserver.Extent{TableID: "5"}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	call := 0
	store, _ := NewStore(loaderFunc(func(context.Context, tserver.Extent) (tabletloader.Specification, error) {
		mu.Lock()
		call++
		n := call
		mu.Unlock()
		if n == 1 {
			close(firstStarted)
			<-releaseFirst
			return tabletloader.Specification{Extent: extent, Directory: "old"}, nil
		}
		return tabletloader.Specification{Extent: extent, Directory: "new"}, nil
	}))

	firstDone := make(chan error, 1)
	go func() { firstDone <- store.Load(context.Background(), extent) }()
	<-firstStarted
	if err := store.Unload(context.Background(), extent, tserverrpc.UnloadUnassigned); !errors.Is(err, tserverrpc.ErrNotServing) {
		t.Fatalf("cancel loading unload = %v", err)
	}
	if err := store.Load(context.Background(), extent); err != nil {
		t.Fatal(err)
	}
	close(releaseFirst)
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("stale load error = %v", err)
	}
	got := store.Hosted()
	if len(got) != 1 || got[0].Directory != "new" {
		t.Fatalf("hosted = %#v", got)
	}
}

func TestStoreLoadFailureRemainsUnhosted(t *testing.T) {
	want := errors.New("metadata unavailable")
	store, _ := NewStore(loaderFunc(func(context.Context, tserver.Extent) (tabletloader.Specification, error) {
		return tabletloader.Specification{}, want
	}))
	if err := store.Load(context.Background(), tserver.Extent{TableID: "5"}); !errors.Is(err, want) {
		t.Fatalf("Load error = %v", err)
	}
	if len(store.Hosted()) != 0 {
		t.Fatal("failed tablet published")
	}
}
