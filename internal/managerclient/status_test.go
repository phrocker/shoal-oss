package managerclient

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"

	clientgen "github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/manager"
	"github.com/phrocker/shoal/internal/transportpool"
)

func TestMonitorInfoFromThriftMapsEveryFieldAndCopies(t *testing.T) {
	raw := completeThriftMonitorInfo()
	got, err := monitorInfoFromThrift(raw)
	if err != nil {
		t.Fatal(err)
	}
	table := got.TableMap["1"]
	if table.Records != 101 || table.RecordsInMemory != 12 ||
		table.Tablets != 5 || table.OnlineTablets != 4 ||
		table.IngestRate != 1.1 || table.IngestByteRate != 2.2 ||
		table.QueryRate != 3.3 || table.QueryByteRate != 4.4 ||
		table.ScanRate != 5.5 {
		t.Fatalf("table info = %+v", table)
	}
	if table.Minors == nil || *table.Minors != (Compacting{Running: 6, Queued: 7}) {
		t.Fatalf("minors = %+v", table.Minors)
	}
	if table.Scans == nil || *table.Scans != (Compacting{Running: 8, Queued: 9}) {
		t.Fatalf("scans = %+v", table.Scans)
	}
	server := got.TabletServerInfo[0]
	if server.LastContact != 10 || server.Name != "ts1:9997" || server.OSLoad != 0.75 ||
		server.HoldTime != 11 || server.Lookups != 12 ||
		server.IndexCacheHits != 13 || server.IndexCacheRequests != 14 ||
		server.DataCacheHits != 15 || server.DataCacheRequests != 16 ||
		server.Flushes != 17 || server.Syncs != 18 ||
		server.Version != "4.0.0" || server.ResponseTime != 19 {
		t.Fatalf("tablet server = %+v", server)
	}
	if !reflect.DeepEqual(server.LogSorts, []RecoveryStatus{{
		Name: "wal-1", Runtime: 20, Progress: 0.5,
	}}) {
		t.Fatalf("log sorts = %+v", server.LogSorts)
	}
	if got.BadTabletServers["bad:9997"] != TabletLoadStateUnloadError ||
		got.State != ManagerStateUnloadRootTablet ||
		got.GoalState != ManagerGoalStateSafeMode ||
		got.UnassignedTablets != 21 ||
		!reflect.DeepEqual(got.ServersShuttingDown, []string{"old:9997"}) {
		t.Fatalf("monitor info = %+v", got)
	}
	if !reflect.DeepEqual(got.DeadTabletServers, []DeadServer{{
		Server: "dead:9997", LastContact: 22, Status: "LOST", ResourceGroup: "default",
	}}) {
		t.Fatalf("dead servers = %+v", got.DeadTabletServers)
	}

	raw.TableMap["1"].Recs = -1
	raw.TServerInfo[0].TableMap["1"].Recs = -2
	raw.TServerInfo[0].LogSorts[0].Name = "changed"
	raw.ServersShuttingDown[0] = "changed"
	raw.DeadTabletServers[0].Server = "changed"
	if got.TableMap["1"].Records != 101 ||
		got.TabletServerInfo[0].TableMap["1"].Records != 101 ||
		got.TabletServerInfo[0].LogSorts[0].Name != "wal-1" ||
		got.ServersShuttingDown[0] != "old:9997" ||
		got.DeadTabletServers[0].Server != "dead:9997" {
		t.Fatalf("generated mutation leaked into result: %+v", got)
	}
}

func TestMonitorInfoFromThriftNilAndUnknownValues(t *testing.T) {
	tests := []struct {
		name string
		info *manager.ManagerMonitorInfo
		want string
	}{
		{name: "nil result", want: "nil monitor info"},
		{name: "nil table", info: func() *manager.ManagerMonitorInfo {
			info := completeThriftMonitorInfo()
			info.TableMap["1"] = nil
			return info
		}(), want: `tableMap["1"] is nil`},
		{name: "nil tablet server", info: func() *manager.ManagerMonitorInfo {
			info := completeThriftMonitorInfo()
			info.TServerInfo[0] = nil
			return info
		}(), want: "tServerInfo[0] is nil"},
		{name: "nil server table", info: func() *manager.ManagerMonitorInfo {
			info := completeThriftMonitorInfo()
			info.TServerInfo[0].TableMap["1"] = nil
			return info
		}(), want: `tServerInfo[0].tableMap["1"] is nil`},
		{name: "nil recovery", info: func() *manager.ManagerMonitorInfo {
			info := completeThriftMonitorInfo()
			info.TServerInfo[0].LogSorts[0] = nil
			return info
		}(), want: "logSorts[0] is nil"},
		{name: "nil dead server", info: func() *manager.ManagerMonitorInfo {
			info := completeThriftMonitorInfo()
			info.DeadTabletServers[0] = nil
			return info
		}(), want: "deadTabletServers[0] is nil"},
		{name: "unknown manager state", info: func() *manager.ManagerMonitorInfo {
			info := completeThriftMonitorInfo()
			info.State = manager.ManagerState(99)
			return info
		}(), want: "unknown ManagerState wire value 99"},
		{name: "unknown goal state", info: func() *manager.ManagerMonitorInfo {
			info := completeThriftMonitorInfo()
			info.GoalState = manager.ManagerGoalState(99)
			return info
		}(), want: "unknown ManagerGoalState wire value 99"},
		{name: "unknown load state", info: func() *manager.ManagerMonitorInfo {
			info := completeThriftMonitorInfo()
			info.BadTServers["bad:9997"] = 99
			return info
		}(), want: "unknown TabletLoadState wire value 99"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := monitorInfoFromThrift(tt.info)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestStatusEnumMappingsAreExhaustive(t *testing.T) {
	for raw, want := range map[manager.ManagerState]ManagerState{
		manager.ManagerState_INITIAL:                 ManagerStateInitial,
		manager.ManagerState_HAVE_LOCK:               ManagerStateHaveLock,
		manager.ManagerState_SAFE_MODE:               ManagerStateSafeMode,
		manager.ManagerState_NORMAL:                  ManagerStateNormal,
		manager.ManagerState_UNLOAD_METADATA_TABLETS: ManagerStateUnloadMetadataTablets,
		manager.ManagerState_UNLOAD_ROOT_TABLET:      ManagerStateUnloadRootTablet,
		manager.ManagerState_STOP:                    ManagerStateStop,
	} {
		got, err := managerStateFromThrift(raw)
		if err != nil || got != want {
			t.Fatalf("manager state %d = %d/%v, want %d", raw, got, err, want)
		}
	}
	for raw, want := range map[manager.ManagerGoalState]ManagerGoalState{
		manager.ManagerGoalState_CLEAN_STOP: ManagerGoalStateCleanStop,
		manager.ManagerGoalState_SAFE_MODE:  ManagerGoalStateSafeMode,
		manager.ManagerGoalState_NORMAL:     ManagerGoalStateNormal,
	} {
		got, err := managerGoalStateFromThrift(raw)
		if err != nil || got != want {
			t.Fatalf("goal state %d = %d/%v, want %d", raw, got, err, want)
		}
	}
	for raw, want := range map[manager.TabletLoadState]TabletLoadState{
		manager.TabletLoadState_LOADED:                     TabletLoadStateLoaded,
		manager.TabletLoadState_LOAD_FAILURE:               TabletLoadStateLoadFailure,
		manager.TabletLoadState_UNLOADED:                   TabletLoadStateUnloaded,
		manager.TabletLoadState_UNLOAD_FAILURE_NOT_SERVING: TabletLoadStateUnloadFailureNotServing,
		manager.TabletLoadState_UNLOAD_ERROR:               TabletLoadStateUnloadError,
	} {
		got, err := tabletLoadStateFromThrift(raw)
		if err != nil || got != want {
			t.Fatalf("load state %d = %d/%v, want %d", raw, got, err, want)
		}
	}
}

func TestPooledGetManagerStatsUsesManagerServiceAndMapsErrors(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	rpc := &fakeManagerRPC{stats: completeThriftMonitorInfo()}
	var gotKey transportpool.Key
	pooled.dial = func(_ context.Context, key transportpool.Key) (io.Closer, error) {
		gotKey = key
		return &fakeTransport{manager: rpc}, nil
	}
	pooled.newManagerClient = managerFromFakeTransport
	if _, err := pooled.GetManagerStats(context.Background(), "manager:9997"); err != nil {
		t.Fatal(err)
	}
	wantKey := transportpool.Key{
		Address: "manager:9997", Service: "mgr", InstanceID: "uuid-1",
		ProtocolVersion: "4.0.0-SNAPSHOT",
	}
	if gotKey != wantKey {
		t.Fatalf("pool key = %+v, want %+v", gotKey, wantKey)
	}

	rpc.statsErr = &clientgen.ThriftSecurityException{
		Code: clientgen.SecurityErrorCode_PERMISSION_DENIED,
	}
	_, err := pooled.GetManagerStats(context.Background(), "other:9997")
	var managerErr *Error
	if !errors.As(err, &managerErr) || managerErr.Kind != ErrorSecurity {
		t.Fatalf("security error = %#v, want ErrorSecurity", err)
	}

	rpc.statsErr = thrift.NewTTransportExceptionFromError(errors.New("reset"))
	if _, err := pooled.GetManagerStats(context.Background(), "wire:9997"); !IsRetryableEndpointError(err) {
		t.Fatalf("wire error = %v, want retryable", err)
	}
	if err := pooled.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pooled.GetManagerStats(context.Background(), "manager:9997"); err == nil {
		t.Fatal("expected closed pooled-client error")
	}
}

func completeThriftMonitorInfo() *manager.ManagerMonitorInfo {
	table := &manager.TableInfo{
		Recs: 101, RecsInMemory: 12, Tablets: 5, OnlineTablets: 4,
		IngestRate: 1.1, IngestByteRate: 2.2, QueryRate: 3.3, QueryByteRate: 4.4,
		Minors:   &manager.Compacting{Running: 6, Queued: 7},
		Scans:    &manager.Compacting{Running: 8, Queued: 9},
		ScanRate: 5.5,
	}
	return &manager.ManagerMonitorInfo{
		TableMap: map[string]*manager.TableInfo{"1": table},
		TServerInfo: []*manager.TabletServerStatus{{
			TableMap:    map[string]*manager.TableInfo{"1": table},
			LastContact: 10, Name: "ts1:9997", OsLoad: 0.75, HoldTime: 11, Lookups: 12,
			IndexCacheHits: 13, IndexCacheRequest: 14,
			DataCacheHits: 15, DataCacheRequest: 16,
			LogSorts: []*manager.RecoveryStatus{{Name: "wal-1", Runtime: 20, Progress: 0.5}},
			Flushs:   17, Syncs: 18, Version: "4.0.0", ResponseTime: 19,
		}},
		BadTServers:         map[string]int8{"bad:9997": int8(manager.TabletLoadState_UNLOAD_ERROR)},
		State:               manager.ManagerState_UNLOAD_ROOT_TABLET,
		GoalState:           manager.ManagerGoalState_SAFE_MODE,
		UnassignedTablets:   21,
		ServersShuttingDown: []string{"old:9997"},
		DeadTabletServers: []*manager.DeadServer{{
			Server: "dead:9997", LastStatus: 22, Status: "LOST", ResourceGroup: "default",
		}},
	}
}
