package accumulo

import (
	"context"
	"errors"
	"fmt"

	"github.com/phrocker/shoal/internal/managerclient"
	"github.com/phrocker/shoal/internal/zk"
)

// ManagerState is the active Accumulo Manager lifecycle state.
type ManagerState int

const (
	ManagerStateInitial ManagerState = iota
	ManagerStateHaveLock
	ManagerStateSafeMode
	ManagerStateNormal
	ManagerStateUnloadMetadataTablets
	ManagerStateUnloadRootTablet
	ManagerStateStop
)

// CoordinatorState is the Sharkbite name for ManagerState.
type CoordinatorState = ManagerState

const (
	CoordinatorStateInitial               = ManagerStateInitial
	CoordinatorStateHaveLock              = ManagerStateHaveLock
	CoordinatorStateSafeMode              = ManagerStateSafeMode
	CoordinatorStateNormal                = ManagerStateNormal
	CoordinatorStateUnloadMetadataTablets = ManagerStateUnloadMetadataTablets
	CoordinatorStateUnloadRootTablet      = ManagerStateUnloadRootTablet
	CoordinatorStateStop                  = ManagerStateStop
)

// ManagerGoalState is the lifecycle state toward which the Manager is moving.
type ManagerGoalState int

const (
	ManagerGoalStateCleanStop ManagerGoalState = iota
	ManagerGoalStateSafeMode
	ManagerGoalStateNormal
)

// CoordinatorGoalState is the Sharkbite name for ManagerGoalState.
type CoordinatorGoalState = ManagerGoalState

const (
	CoordinatorGoalStateCleanStop = ManagerGoalStateCleanStop
	CoordinatorGoalStateSafeMode  = ManagerGoalStateSafeMode
	CoordinatorGoalStateNormal    = ManagerGoalStateNormal
)

// TabletLoadState describes the last reported load operation for a bad server.
type TabletLoadState int

const (
	TabletLoadStateLoaded TabletLoadState = iota
	TabletLoadStateLoadFailure
	TabletLoadStateUnloaded
	TabletLoadStateUnloadFailureNotServing
	TabletLoadStateUnloadError
)

// Compacting contains active and queued operation counts.
type Compacting struct {
	Running int32
	Queued  int32
}

// TableRates contains all rates reported by Accumulo 4 for a table.
type TableRates struct {
	IngestRate     float64
	IngestByteRate float64
	QueryRate      float64
	QueryByteRate  float64
	ScanRate       float64
}

// TableCompactions contains compaction and scan operation counts.
//
// Majors is always nil with Accumulo 4 because its manager protocol no longer
// reports major-compaction counts. Minors and Scans are nil when the server
// omits their optional Thrift structs.
type TableCompactions struct {
	Minors *Compacting
	Majors *Compacting
	Scans  *Compacting
}

// TableInfo is the Manager's aggregate status for one table.
type TableInfo struct {
	Records         int64
	RecordsInMemory int64
	Tablets         int32
	OnlineTablets   int32
	Rates           TableRates
	Compactions     TableCompactions
}

// RecoveryStatus describes one tablet-server write-ahead-log recovery.
type RecoveryStatus struct {
	Name          string
	RuntimeMillis int32
	Progress      float64
}

// TabletServerStatus is one tablet server's status and per-table aggregates.
type TabletServerStatus struct {
	TableMap           map[string]TableInfo
	LastContact        int64
	Name               string
	OSLoad             float64
	HoldTime           int64
	Lookups            int64
	IndexCacheHits     int64
	IndexCacheRequests int64
	DataCacheHits      int64
	DataCacheRequests  int64
	LogSorts           []RecoveryStatus
	Flushes            int64
	Syncs              int64
	Version            string
	ResponseTime       int64
}

// DeadServer describes a tablet server remembered as dead by the Manager.
type DeadServer struct {
	Server        string
	LastContact   int64
	Status        string
	ResourceGroup string
}

// ClusterStatus is a caller-owned snapshot of Accumulo Manager statistics.
//
// Every map, slice, and nested pointer is detached from the generated Thrift
// response and from subsequent calls, so callers may modify the snapshot.
type ClusterStatus struct {
	TableMap            map[string]TableInfo
	TabletServerInfo    []TabletServerStatus
	BadTabletServers    map[string]TabletLoadState
	State               ManagerState
	GoalState           ManagerGoalState
	UnassignedTablets   int32
	ServersShuttingDown []string
	DeadTabletServers   []DeadServer
}

// Statistics retrieves a complete Accumulo 4 Manager status snapshot.
func (c *Connector) Statistics(ctx context.Context) (ClusterStatus, error) {
	return c.ClusterStatus(ctx)
}

// ClusterStatus retrieves a complete Accumulo 4 Manager status snapshot.
func (c *Connector) ClusterStatus(ctx context.Context) (ClusterStatus, error) {
	if err := ctx.Err(); err != nil {
		return ClusterStatus{}, err
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ClusterStatus{}, ErrConnectorClosed
	}
	resolver := c.managerAddr
	manager := c.manager
	c.mu.RUnlock()
	if resolver == nil {
		return ClusterStatus{}, ErrDiscoveryUnavailable
	}
	address, err := resolver.Address(ctx)
	if errors.Is(err, zk.ErrManagerUnavailable) {
		return ClusterStatus{}, ErrManagerUnavailable
	}
	if err != nil {
		return ClusterStatus{}, fmt.Errorf("accumulo: discover manager for cluster status: %w", err)
	}
	info, err := manager.GetManagerStats(ctx, address)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ClusterStatus{}, ctxErr
		}
		return ClusterStatus{}, mapClusterStatusError(err)
	}
	return clusterStatusFromManager(info)
}

func mapClusterStatusError(err error) error {
	var managerErr *managerclient.Error
	if errors.As(err, &managerErr) {
		switch managerErr.Kind {
		case managerclient.ErrorSecurity:
			return ErrPermissionDenied
		case managerclient.ErrorNotActive:
			return ErrManagerUnavailable
		}
	}
	if managerclient.IsRetryableEndpointError(err) {
		return fmt.Errorf("%w: %w", ErrManagerUnavailable, err)
	}
	return fmt.Errorf("accumulo: retrieve cluster status: %w", err)
}

func clusterStatusFromManager(info managerclient.MonitorInfo) (ClusterStatus, error) {
	state, err := managerStateFromClient(info.State)
	if err != nil {
		return ClusterStatus{}, err
	}
	goalState, err := managerGoalStateFromClient(info.GoalState)
	if err != nil {
		return ClusterStatus{}, err
	}
	out := ClusterStatus{
		TableMap:            publicTableMap(info.TableMap),
		TabletServerInfo:    make([]TabletServerStatus, len(info.TabletServerInfo)),
		BadTabletServers:    make(map[string]TabletLoadState, len(info.BadTabletServers)),
		State:               state,
		GoalState:           goalState,
		UnassignedTablets:   info.UnassignedTablets,
		ServersShuttingDown: append([]string(nil), info.ServersShuttingDown...),
		DeadTabletServers:   make([]DeadServer, len(info.DeadTabletServers)),
	}
	for i, server := range info.TabletServerInfo {
		out.TabletServerInfo[i] = TabletServerStatus{
			TableMap:           publicTableMap(server.TableMap),
			LastContact:        server.LastContact,
			Name:               server.Name,
			OSLoad:             server.OSLoad,
			HoldTime:           server.HoldTime,
			Lookups:            server.Lookups,
			IndexCacheHits:     server.IndexCacheHits,
			IndexCacheRequests: server.IndexCacheRequests,
			DataCacheHits:      server.DataCacheHits,
			DataCacheRequests:  server.DataCacheRequests,
			LogSorts:           make([]RecoveryStatus, len(server.LogSorts)),
			Flushes:            server.Flushes,
			Syncs:              server.Syncs,
			Version:            server.Version,
			ResponseTime:       server.ResponseTime,
		}
		for j, recovery := range server.LogSorts {
			out.TabletServerInfo[i].LogSorts[j] = RecoveryStatus{
				Name:          recovery.Name,
				RuntimeMillis: recovery.Runtime,
				Progress:      recovery.Progress,
			}
		}
	}
	for address, raw := range info.BadTabletServers {
		state, err := tabletLoadStateFromClient(raw)
		if err != nil {
			return ClusterStatus{}, fmt.Errorf("accumulo: bad tablet server %q: %w", address, err)
		}
		out.BadTabletServers[address] = state
	}
	for i, server := range info.DeadTabletServers {
		out.DeadTabletServers[i] = DeadServer{
			Server:        server.Server,
			LastContact:   server.LastContact,
			Status:        server.Status,
			ResourceGroup: server.ResourceGroup,
		}
	}
	return out, nil
}

func publicTableMap(input map[string]managerclient.TableInfo) map[string]TableInfo {
	if input == nil {
		return nil
	}
	out := make(map[string]TableInfo, len(input))
	for tableID, info := range input {
		out[tableID] = TableInfo{
			Records:         info.Records,
			RecordsInMemory: info.RecordsInMemory,
			Tablets:         info.Tablets,
			OnlineTablets:   info.OnlineTablets,
			Rates: TableRates{
				IngestRate:     info.IngestRate,
				IngestByteRate: info.IngestByteRate,
				QueryRate:      info.QueryRate,
				QueryByteRate:  info.QueryByteRate,
				ScanRate:       info.ScanRate,
			},
			Compactions: TableCompactions{
				Minors: publicCompacting(info.Minors),
				Scans:  publicCompacting(info.Scans),
			},
		}
	}
	return out
}

func publicCompacting(input *managerclient.Compacting) *Compacting {
	if input == nil {
		return nil
	}
	return &Compacting{Running: input.Running, Queued: input.Queued}
}

func managerStateFromClient(state managerclient.ManagerState) (ManagerState, error) {
	switch state {
	case managerclient.ManagerStateInitial:
		return ManagerStateInitial, nil
	case managerclient.ManagerStateHaveLock:
		return ManagerStateHaveLock, nil
	case managerclient.ManagerStateSafeMode:
		return ManagerStateSafeMode, nil
	case managerclient.ManagerStateNormal:
		return ManagerStateNormal, nil
	case managerclient.ManagerStateUnloadMetadataTablets:
		return ManagerStateUnloadMetadataTablets, nil
	case managerclient.ManagerStateUnloadRootTablet:
		return ManagerStateUnloadRootTablet, nil
	case managerclient.ManagerStateStop:
		return ManagerStateStop, nil
	default:
		return 0, fmt.Errorf("accumulo: unknown ManagerState value %d", state)
	}
}

func managerGoalStateFromClient(state managerclient.ManagerGoalState) (ManagerGoalState, error) {
	switch state {
	case managerclient.ManagerGoalStateCleanStop:
		return ManagerGoalStateCleanStop, nil
	case managerclient.ManagerGoalStateSafeMode:
		return ManagerGoalStateSafeMode, nil
	case managerclient.ManagerGoalStateNormal:
		return ManagerGoalStateNormal, nil
	default:
		return 0, fmt.Errorf("accumulo: unknown ManagerGoalState value %d", state)
	}
}

func tabletLoadStateFromClient(state managerclient.TabletLoadState) (TabletLoadState, error) {
	switch state {
	case managerclient.TabletLoadStateLoaded:
		return TabletLoadStateLoaded, nil
	case managerclient.TabletLoadStateLoadFailure:
		return TabletLoadStateLoadFailure, nil
	case managerclient.TabletLoadStateUnloaded:
		return TabletLoadStateUnloaded, nil
	case managerclient.TabletLoadStateUnloadFailureNotServing:
		return TabletLoadStateUnloadFailureNotServing, nil
	case managerclient.TabletLoadStateUnloadError:
		return TabletLoadStateUnloadError, nil
	default:
		return 0, fmt.Errorf("unknown TabletLoadState value %d", state)
	}
}
