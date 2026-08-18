package accumulo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/phrocker/shoal/internal/managerclient"
	"github.com/phrocker/shoal/internal/zk"
)

func TestStatisticsMapsEveryFieldAndIsCopyIsolated(t *testing.T) {
	connector := testConnectorWithDiscovery(t, &fakeTabletWalker{}, &fakeTableNames{})
	shared := completeManagerStatus()
	connector.manager = &fakeManagerAdapter{status: shared}
	connector.managerAddr = fakeManagerAddress{address: "manager:9997"}

	got, err := connector.Statistics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.State != ManagerStateUnloadMetadataTablets ||
		got.GoalState != ManagerGoalStateCleanStop ||
		got.UnassignedTablets != 31 {
		t.Fatalf("cluster scalars = %+v", got)
	}
	table := got.TableMap["1"]
	if table.Records != 100 || table.RecordsInMemory != 10 ||
		table.Tablets != 3 || table.OnlineTablets != 2 ||
		table.Rates != (TableRates{
			IngestRate: 1.1, IngestByteRate: 2.2, QueryRate: 3.3, QueryByteRate: 4.4, ScanRate: 5.5,
		}) {
		t.Fatalf("table = %+v", table)
	}
	if table.Compactions.Minors == nil ||
		*table.Compactions.Minors != (Compacting{Running: 6, Queued: 7}) ||
		table.Compactions.Scans == nil ||
		*table.Compactions.Scans != (Compacting{Running: 8, Queued: 9}) ||
		table.Compactions.Majors != nil {
		t.Fatalf("compactions = %+v", table.Compactions)
	}
	server := got.TabletServerInfo[0]
	if server.LastContact != 11 || server.Name != "ts1:9997" || server.OSLoad != 0.5 ||
		server.HoldTime != 12 || server.Lookups != 13 ||
		server.IndexCacheHits != 14 || server.IndexCacheRequests != 15 ||
		server.DataCacheHits != 16 || server.DataCacheRequests != 17 ||
		server.Flushes != 18 || server.Syncs != 19 ||
		server.Version != "4.0.0" || server.ResponseTime != 20 {
		t.Fatalf("server = %+v", server)
	}
	if !reflect.DeepEqual(server.LogSorts, []RecoveryStatus{{
		Name: "wal-1", RuntimeMillis: 21, Progress: 0.75,
	}}) {
		t.Fatalf("log sorts = %+v", server.LogSorts)
	}
	if got.BadTabletServers["bad:9997"] != TabletLoadStateLoadFailure ||
		!reflect.DeepEqual(got.ServersShuttingDown, []string{"old:9997"}) ||
		!reflect.DeepEqual(got.DeadTabletServers, []DeadServer{{
			Server: "dead:9997", LastContact: 32, Status: "LOST", ResourceGroup: "rg1",
		}}) {
		t.Fatalf("server collections = %+v", got)
	}

	got.TableMap["1"] = TableInfo{}
	got.TabletServerInfo[0].TableMap["1"] = TableInfo{}
	got.TabletServerInfo[0].LogSorts[0].Name = "changed"
	got.ServersShuttingDown[0] = "changed"
	got.DeadTabletServers[0].Server = "changed"
	got.BadTabletServers["bad:9997"] = TabletLoadStateLoaded
	second, err := connector.ClusterStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.TableMap["1"].Records != 100 ||
		second.TabletServerInfo[0].TableMap["1"].Records != 100 ||
		second.TabletServerInfo[0].LogSorts[0].Name != "wal-1" ||
		second.ServersShuttingDown[0] != "old:9997" ||
		second.DeadTabletServers[0].Server != "dead:9997" ||
		second.BadTabletServers["bad:9997"] != TabletLoadStateLoadFailure {
		t.Fatalf("caller mutation leaked into later result: %+v", second)
	}
}

func TestClusterStatusCancellationLifecycleDiscoveryAndErrors(t *testing.T) {
	connector := testConnectorWithDiscovery(t, &fakeTabletWalker{}, &fakeTableNames{})
	manager := &fakeManagerAdapter{status: completeManagerStatus()}
	connector.manager = manager
	connector.managerAddr = fakeManagerAddress{address: "manager:9997"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := connector.ClusterStatus(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled error = %v", err)
	}

	started := make(chan struct{})
	manager.statusFn = func(ctx context.Context, _ string) (managerclient.MonitorInfo, error) {
		close(started)
		<-ctx.Done()
		return managerclient.MonitorInfo{}, ctx.Err()
	}
	ctx, cancel = context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := connector.Statistics(ctx)
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("in-flight cancellation = %v", err)
	}
	manager.statusFn = nil

	connector.managerAddr = fakeManagerAddress{err: zk.ErrManagerUnavailable}
	if _, err := connector.ClusterStatus(context.Background()); !errors.Is(err, ErrManagerUnavailable) {
		t.Fatalf("manager discovery error = %v", err)
	}
	discoveryErr := errors.New("zk failed")
	connector.managerAddr = fakeManagerAddress{err: discoveryErr}
	if _, err := connector.ClusterStatus(context.Background()); !errors.Is(err, discoveryErr) {
		t.Fatalf("generic discovery error = %v", err)
	}
	connector.managerAddr = fakeManagerAddress{address: "manager:9997"}

	manager.err = &managerclient.Error{Kind: managerclient.ErrorSecurity}
	if _, err := connector.ClusterStatus(context.Background()); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("security error = %v", err)
	}
	manager.err = &managerclient.Error{Kind: managerclient.ErrorNotActive}
	if _, err := connector.ClusterStatus(context.Background()); !errors.Is(err, ErrManagerUnavailable) {
		t.Fatalf("not-active error = %v", err)
	}
	manager.err = thrift.NewTTransportExceptionFromError(errors.New("reset"))
	if _, err := connector.ClusterStatus(context.Background()); !errors.Is(err, ErrManagerUnavailable) {
		t.Fatalf("transport error = %v", err)
	}
	manager.err = nil

	instance, _ := NewStaticInstance("accumulo", "uuid-1")
	credentials, _ := PasswordCredentials("root", []byte("secret"))
	noDiscovery, err := NewConnector(instance, credentials, ConnectorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := noDiscovery.ClusterStatus(context.Background()); !errors.Is(err, ErrDiscoveryUnavailable) {
		t.Fatalf("static instance error = %v", err)
	}
	if err := noDiscovery.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := noDiscovery.Statistics(context.Background()); !errors.Is(err, ErrConnectorClosed) {
		t.Fatalf("closed connector error = %v", err)
	}
}

func TestClusterStatusRejectsUnknownAdapterEnums(t *testing.T) {
	tests := []struct {
		name   string
		change func(*managerclient.MonitorInfo)
	}{
		{name: "manager state", change: func(info *managerclient.MonitorInfo) {
			info.State = managerclient.ManagerState(99)
		}},
		{name: "goal state", change: func(info *managerclient.MonitorInfo) {
			info.GoalState = managerclient.ManagerGoalState(99)
		}},
		{name: "load state", change: func(info *managerclient.MonitorInfo) {
			info.BadTabletServers["bad:9997"] = managerclient.TabletLoadState(99)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := completeManagerStatus()
			tt.change(&info)
			if _, err := clusterStatusFromManager(info); err == nil {
				t.Fatal("expected unknown enum error")
			}
		})
	}
}

func TestPublicStatusEnumMappingsAreExhaustive(t *testing.T) {
	for raw, want := range map[managerclient.ManagerState]ManagerState{
		managerclient.ManagerStateInitial:               ManagerStateInitial,
		managerclient.ManagerStateHaveLock:              ManagerStateHaveLock,
		managerclient.ManagerStateSafeMode:              ManagerStateSafeMode,
		managerclient.ManagerStateNormal:                ManagerStateNormal,
		managerclient.ManagerStateUnloadMetadataTablets: ManagerStateUnloadMetadataTablets,
		managerclient.ManagerStateUnloadRootTablet:      ManagerStateUnloadRootTablet,
		managerclient.ManagerStateStop:                  ManagerStateStop,
	} {
		got, err := managerStateFromClient(raw)
		if err != nil || got != want {
			t.Fatalf("manager state %d = %d/%v, want %d", raw, got, err, want)
		}
	}
	for raw, want := range map[managerclient.ManagerGoalState]ManagerGoalState{
		managerclient.ManagerGoalStateCleanStop: ManagerGoalStateCleanStop,
		managerclient.ManagerGoalStateSafeMode:  ManagerGoalStateSafeMode,
		managerclient.ManagerGoalStateNormal:    ManagerGoalStateNormal,
	} {
		got, err := managerGoalStateFromClient(raw)
		if err != nil || got != want {
			t.Fatalf("goal state %d = %d/%v, want %d", raw, got, err, want)
		}
	}
	for raw, want := range map[managerclient.TabletLoadState]TabletLoadState{
		managerclient.TabletLoadStateLoaded:                  TabletLoadStateLoaded,
		managerclient.TabletLoadStateLoadFailure:             TabletLoadStateLoadFailure,
		managerclient.TabletLoadStateUnloaded:                TabletLoadStateUnloaded,
		managerclient.TabletLoadStateUnloadFailureNotServing: TabletLoadStateUnloadFailureNotServing,
		managerclient.TabletLoadStateUnloadError:             TabletLoadStateUnloadError,
	} {
		got, err := tabletLoadStateFromClient(raw)
		if err != nil || got != want {
			t.Fatalf("load state %d = %d/%v, want %d", raw, got, err, want)
		}
	}
}

func TestClusterStatusConcurrentCopyIsolation(t *testing.T) {
	connector := testConnectorWithDiscovery(t, &fakeTabletWalker{}, &fakeTableNames{})
	connector.manager = &fakeManagerAdapter{status: completeManagerStatus()}
	connector.managerAddr = fakeManagerAddress{address: "manager:9997"}

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, err := connector.ClusterStatus(context.Background())
			if err != nil {
				t.Errorf("ClusterStatus: %v", err)
				return
			}
			status.TableMap["1"] = TableInfo{Records: int64(i)}
			status.TabletServerInfo[0].LogSorts[0].Name = fmt.Sprint(i)
			status.ServersShuttingDown[0] = fmt.Sprint(i)
		}()
	}
	wg.Wait()
}

func TestClusterStatusConcurrentClose(t *testing.T) {
	connector := testConnectorWithDiscovery(t, &fakeTabletWalker{}, &fakeTableNames{})
	connector.manager = &fakeManagerAdapter{status: completeManagerStatus()}
	connector.managerAddr = fakeManagerAddress{address: "manager:9997"}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := connector.ClusterStatus(context.Background())
			if err != nil && !errors.Is(err, ErrConnectorClosed) {
				t.Errorf("ClusterStatus during close: %v", err)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := connector.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	wg.Wait()
}

func completeManagerStatus() managerclient.MonitorInfo {
	table := managerclient.TableInfo{
		Records: 100, RecordsInMemory: 10, Tablets: 3, OnlineTablets: 2,
		IngestRate: 1.1, IngestByteRate: 2.2, QueryRate: 3.3, QueryByteRate: 4.4,
		Minors:   &managerclient.Compacting{Running: 6, Queued: 7},
		Scans:    &managerclient.Compacting{Running: 8, Queued: 9},
		ScanRate: 5.5,
	}
	return managerclient.MonitorInfo{
		TableMap: map[string]managerclient.TableInfo{"1": table},
		TabletServerInfo: []managerclient.TabletServerStatus{{
			TableMap:    map[string]managerclient.TableInfo{"1": table},
			LastContact: 11, Name: "ts1:9997", OSLoad: 0.5, HoldTime: 12, Lookups: 13,
			IndexCacheHits: 14, IndexCacheRequests: 15,
			DataCacheHits: 16, DataCacheRequests: 17,
			LogSorts: []managerclient.RecoveryStatus{{Name: "wal-1", Runtime: 21, Progress: 0.75}},
			Flushes:  18, Syncs: 19, Version: "4.0.0", ResponseTime: 20,
		}},
		BadTabletServers:    map[string]managerclient.TabletLoadState{"bad:9997": managerclient.TabletLoadStateLoadFailure},
		State:               managerclient.ManagerStateUnloadMetadataTablets,
		GoalState:           managerclient.ManagerGoalStateCleanStop,
		UnassignedTablets:   31,
		ServersShuttingDown: []string{"old:9997"},
		DeadTabletServers: []managerclient.DeadServer{{
			Server: "dead:9997", LastContact: 32, Status: "LOST", ResourceGroup: "rg1",
		}},
	}
}
