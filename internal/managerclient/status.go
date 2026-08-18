package managerclient

import (
	"fmt"

	"github.com/phrocker/shoal/internal/thrift/gen/manager"
)

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

type ManagerGoalState int

const (
	ManagerGoalStateCleanStop ManagerGoalState = iota
	ManagerGoalStateSafeMode
	ManagerGoalStateNormal
)

type TabletLoadState int

const (
	TabletLoadStateLoaded TabletLoadState = iota
	TabletLoadStateLoadFailure
	TabletLoadStateUnloaded
	TabletLoadStateUnloadFailureNotServing
	TabletLoadStateUnloadError
)

type Compacting struct {
	Running int32
	Queued  int32
}

type TableInfo struct {
	Records         int64
	RecordsInMemory int64
	Tablets         int32
	OnlineTablets   int32
	IngestRate      float64
	IngestByteRate  float64
	QueryRate       float64
	QueryByteRate   float64
	Minors          *Compacting
	Scans           *Compacting
	ScanRate        float64
}

type RecoveryStatus struct {
	Name     string
	Runtime  int32
	Progress float64
}

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

type DeadServer struct {
	Server        string
	LastContact   int64
	Status        string
	ResourceGroup string
}

type MonitorInfo struct {
	TableMap            map[string]TableInfo
	TabletServerInfo    []TabletServerStatus
	BadTabletServers    map[string]TabletLoadState
	State               ManagerState
	GoalState           ManagerGoalState
	UnassignedTablets   int32
	ServersShuttingDown []string
	DeadTabletServers   []DeadServer
}

func monitorInfoFromThrift(info *manager.ManagerMonitorInfo) (MonitorInfo, error) {
	if info == nil {
		return MonitorInfo{}, fmt.Errorf("managerclient: getManagerStats returned nil monitor info")
	}
	state, err := managerStateFromThrift(info.State)
	if err != nil {
		return MonitorInfo{}, err
	}
	goalState, err := managerGoalStateFromThrift(info.GoalState)
	if err != nil {
		return MonitorInfo{}, err
	}
	tableMap, err := tableMapFromThrift("tableMap", info.TableMap)
	if err != nil {
		return MonitorInfo{}, err
	}
	out := MonitorInfo{
		TableMap:            tableMap,
		TabletServerInfo:    make([]TabletServerStatus, len(info.TServerInfo)),
		BadTabletServers:    make(map[string]TabletLoadState, len(info.BadTServers)),
		State:               state,
		GoalState:           goalState,
		UnassignedTablets:   info.UnassignedTablets,
		ServersShuttingDown: append([]string(nil), info.ServersShuttingDown...),
		DeadTabletServers:   make([]DeadServer, len(info.DeadTabletServers)),
	}
	for i, server := range info.TServerInfo {
		if server == nil {
			return MonitorInfo{}, fmt.Errorf("managerclient: tServerInfo[%d] is nil", i)
		}
		mapped, err := tabletServerStatusFromThrift(i, server)
		if err != nil {
			return MonitorInfo{}, err
		}
		out.TabletServerInfo[i] = mapped
	}
	for address, raw := range info.BadTServers {
		state, err := tabletLoadStateFromThrift(manager.TabletLoadState(raw))
		if err != nil {
			return MonitorInfo{}, fmt.Errorf("managerclient: badTServers[%q]: %w", address, err)
		}
		out.BadTabletServers[address] = state
	}
	for i, server := range info.DeadTabletServers {
		if server == nil {
			return MonitorInfo{}, fmt.Errorf("managerclient: deadTabletServers[%d] is nil", i)
		}
		out.DeadTabletServers[i] = DeadServer{
			Server:        server.Server,
			LastContact:   server.LastStatus,
			Status:        server.Status,
			ResourceGroup: server.ResourceGroup,
		}
	}
	return out, nil
}

func tabletServerStatusFromThrift(index int, status *manager.TabletServerStatus) (TabletServerStatus, error) {
	tableMap, err := tableMapFromThrift(fmt.Sprintf("tServerInfo[%d].tableMap", index), status.TableMap)
	if err != nil {
		return TabletServerStatus{}, err
	}
	out := TabletServerStatus{
		TableMap:           tableMap,
		LastContact:        status.LastContact,
		Name:               status.Name,
		OSLoad:             status.OsLoad,
		HoldTime:           status.HoldTime,
		Lookups:            status.Lookups,
		IndexCacheHits:     status.IndexCacheHits,
		IndexCacheRequests: status.IndexCacheRequest,
		DataCacheHits:      status.DataCacheHits,
		DataCacheRequests:  status.DataCacheRequest,
		LogSorts:           make([]RecoveryStatus, len(status.LogSorts)),
		Flushes:            status.Flushs,
		Syncs:              status.Syncs,
		Version:            status.Version,
		ResponseTime:       status.ResponseTime,
	}
	for i, recovery := range status.LogSorts {
		if recovery == nil {
			return TabletServerStatus{}, fmt.Errorf(
				"managerclient: tServerInfo[%d].logSorts[%d] is nil",
				index,
				i,
			)
		}
		out.LogSorts[i] = RecoveryStatus{
			Name:     recovery.Name,
			Runtime:  recovery.Runtime,
			Progress: recovery.Progress,
		}
	}
	return out, nil
}

func tableMapFromThrift(path string, input map[string]*manager.TableInfo) (map[string]TableInfo, error) {
	if input == nil {
		return nil, nil
	}
	out := make(map[string]TableInfo, len(input))
	for tableID, info := range input {
		if info == nil {
			return nil, fmt.Errorf("managerclient: %s[%q] is nil", path, tableID)
		}
		out[tableID] = TableInfo{
			Records:         info.Recs,
			RecordsInMemory: info.RecsInMemory,
			Tablets:         info.Tablets,
			OnlineTablets:   info.OnlineTablets,
			IngestRate:      info.IngestRate,
			IngestByteRate:  info.IngestByteRate,
			QueryRate:       info.QueryRate,
			QueryByteRate:   info.QueryByteRate,
			Minors:          compactingFromThrift(info.Minors),
			Scans:           compactingFromThrift(info.Scans),
			ScanRate:        info.ScanRate,
		}
	}
	return out, nil
}

func compactingFromThrift(input *manager.Compacting) *Compacting {
	if input == nil {
		return nil
	}
	return &Compacting{Running: input.Running, Queued: input.Queued}
}

func managerStateFromThrift(state manager.ManagerState) (ManagerState, error) {
	switch state {
	case manager.ManagerState_INITIAL:
		return ManagerStateInitial, nil
	case manager.ManagerState_HAVE_LOCK:
		return ManagerStateHaveLock, nil
	case manager.ManagerState_SAFE_MODE:
		return ManagerStateSafeMode, nil
	case manager.ManagerState_NORMAL:
		return ManagerStateNormal, nil
	case manager.ManagerState_UNLOAD_METADATA_TABLETS:
		return ManagerStateUnloadMetadataTablets, nil
	case manager.ManagerState_UNLOAD_ROOT_TABLET:
		return ManagerStateUnloadRootTablet, nil
	case manager.ManagerState_STOP:
		return ManagerStateStop, nil
	default:
		return 0, fmt.Errorf("managerclient: unknown ManagerState wire value %d", state)
	}
}

func managerGoalStateFromThrift(state manager.ManagerGoalState) (ManagerGoalState, error) {
	switch state {
	case manager.ManagerGoalState_CLEAN_STOP:
		return ManagerGoalStateCleanStop, nil
	case manager.ManagerGoalState_SAFE_MODE:
		return ManagerGoalStateSafeMode, nil
	case manager.ManagerGoalState_NORMAL:
		return ManagerGoalStateNormal, nil
	default:
		return 0, fmt.Errorf("managerclient: unknown ManagerGoalState wire value %d", state)
	}
}

func tabletLoadStateFromThrift(state manager.TabletLoadState) (TabletLoadState, error) {
	switch state {
	case manager.TabletLoadState_LOADED:
		return TabletLoadStateLoaded, nil
	case manager.TabletLoadState_LOAD_FAILURE:
		return TabletLoadStateLoadFailure, nil
	case manager.TabletLoadState_UNLOADED:
		return TabletLoadStateUnloaded, nil
	case manager.TabletLoadState_UNLOAD_FAILURE_NOT_SERVING:
		return TabletLoadStateUnloadFailureNotServing, nil
	case manager.TabletLoadState_UNLOAD_ERROR:
		return TabletLoadStateUnloadError, nil
	default:
		return 0, fmt.Errorf("unknown TabletLoadState wire value %d", state)
	}
}
