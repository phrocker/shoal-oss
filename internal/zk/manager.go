package zk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	gozk "github.com/go-zookeeper/zk"
	"github.com/google/uuid"
)

const (
	zManagerLock = "/managers/lock"
	zLockPrefix  = "zlock#"

	// ServiceLockData.ThriftService values the Accumulo 4 manager
	// publishes on its lock znode once it is fully initialized. See
	// server/manager/.../Manager.java, which replaces the bootstrap
	// ThriftService.NONE descriptor with MANAGER + COORDINATOR +
	// FATE_CLIENT descriptors, all pointing at the manager's advertise
	// address, only after upgrade and dependent services are running.
	svcManager     = "MANAGER"
	svcCoordinator = "COORDINATOR"
)

var (
	ErrManagerUnavailable       = errors.New("zk: manager unavailable")
	ErrCoordinatorUnavailable   = errors.New("zk: compaction coordinator unavailable")
	ErrClientServiceUnavailable = errors.New("zk: client service unavailable")

	// errServiceNotAdvertised is the internal "the manager lock exists but
	// does not advertise this service (yet)" signal. Exported wrappers map
	// it to their own sentinel so callers keep a stable error surface.
	errServiceNotAdvertised = errors.New("zk: service not advertised on manager lock")
)

type serviceLockData struct {
	Descriptors []serviceDescriptor `json:"descriptors"`
}

type serviceDescriptor struct {
	Service string `json:"service"`
	Address string `json:"address"`
}

// LockReader is the subset of *Locator needed to read Accumulo
// service-lock data out of ZooKeeper.
type LockReader interface {
	InstancePath() string
	GetRaw(context.Context, string) ([]byte, error)
	Children(context.Context, string) ([]string, error)
}

// ManagerAddress returns the Thrift address of the active Accumulo
// manager, or ErrManagerUnavailable when no manager currently advertises
// one.
func ManagerAddress(ctx context.Context, locator LockReader) (string, error) {
	address, err := managerLockAddress(ctx, locator, svcManager)
	if errors.Is(err, errServiceNotAdvertised) {
		return "", ErrManagerUnavailable
	}
	return address, err
}

// CoordinatorAddress returns the Thrift address of the
// CompactionCoordinator, or ErrCoordinatorUnavailable when no manager
// currently advertises one.
//
// The coordinator runs inside the manager process and is advertised on
// the manager's lock znode under ThriftService.COORDINATOR. This mirrors
// Java's ExternalCompactionUtil.findCompactionCoordinator, which reads
// the same manager lock path and maps it through
// ServiceLockData.getAddress(ThriftService.COORDINATOR).
//
// Because the address is re-read from the lowest-sequence lock node on
// every call, callers that re-resolve before each connection attempt
// follow manager failover without restarting: the new primary manager
// creates a new lock node and republishes its own coordinator address.
// Between the old manager dying and the new one finishing startup the
// lock is missing or still carries the bootstrap ThriftService.NONE
// descriptor, both of which surface as ErrCoordinatorUnavailable so
// callers can back off and retry rather than dial a dead address.
func CoordinatorAddress(ctx context.Context, locator LockReader) (string, error) {
	address, err := managerLockAddress(ctx, locator, svcCoordinator)
	if errors.Is(err, errServiceNotAdvertised) {
		return "", ErrCoordinatorUnavailable
	}
	return address, err
}

// managerLockAddress reads the current manager lock znode and returns the
// address advertised for service. ZooKeeper transport failures are
// surfaced as-is; every "nothing usable is published" case collapses to
// errServiceNotAdvertised.
func managerLockAddress(ctx context.Context, locator LockReader, service string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if locator == nil {
		return "", errors.New("zk: nil locator")
	}
	lockPath := path.Join(locator.InstancePath(), zManagerLock)
	children, err := locator.Children(ctx, lockPath)
	if err != nil {
		if errors.Is(err, gozk.ErrNoNode) {
			return "", errServiceNotAdvertised
		}
		return "", fmt.Errorf("list manager locks %s: %w", lockPath, err)
	}
	lockNode := firstLockNode(children)
	if lockNode == "" {
		return "", errServiceNotAdvertised
	}
	data, err := locator.GetRaw(ctx, path.Join(lockPath, lockNode))
	if err != nil {
		if errors.Is(err, gozk.ErrNoNode) {
			return "", errServiceNotAdvertised
		}
		return "", fmt.Errorf("get manager lock %s: %w", lockPath, err)
	}
	var lock serviceLockData
	if err := json.Unmarshal(data, &lock); err != nil {
		return "", fmt.Errorf("decode manager lock %s: %w", lockPath, err)
	}
	for _, descriptor := range lock.Descriptors {
		if descriptor.Service != service {
			continue
		}
		if descriptor.Address == "" || descriptor.Address == "0.0.0.0:0" {
			return "", errServiceNotAdvertised
		}
		return descriptor.Address, nil
	}
	return "", errServiceNotAdvertised
}

// ManagerAddresses returns every manager address advertised under the
// instance's manager lock path, ordered by lock sequence: the first element
// is the manager that currently holds the lock, and any later elements are
// queued candidates that would take over on failover. Returns
// ErrManagerUnavailable when no lock node advertises a usable MANAGER
// endpoint.
func ManagerAddresses(ctx context.Context, locator LockReader) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if locator == nil {
		return nil, errors.New("zk: nil locator")
	}
	lockPath := path.Join(locator.InstancePath(), zManagerLock)
	children, err := locator.Children(ctx, lockPath)
	if err != nil {
		if errors.Is(err, gozk.ErrNoNode) {
			return nil, ErrManagerUnavailable
		}
		return nil, fmt.Errorf("list manager locks %s: %w", lockPath, err)
	}
	lockNodes := sortedLockNodes(children)
	addresses := make([]string, 0, len(lockNodes))
	seen := make(map[string]struct{}, len(lockNodes))
	for _, lockNode := range lockNodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lockNodePath := path.Join(lockPath, lockNode)
		data, err := locator.GetRaw(ctx, lockNodePath)
		if errors.Is(err, gozk.ErrNoNode) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get manager lock %s: %w", lockNodePath, err)
		}
		var lock serviceLockData
		if err := json.Unmarshal(data, &lock); err != nil {
			return nil, fmt.Errorf("decode manager lock %s: %w", lockNodePath, err)
		}
		for _, descriptor := range lock.Descriptors {
			if descriptor.Service != svcManager ||
				descriptor.Address == "" ||
				descriptor.Address == "0.0.0.0:0" {
				continue
			}
			if _, duplicate := seen[descriptor.Address]; duplicate {
				continue
			}
			seen[descriptor.Address] = struct{}{}
			addresses = append(addresses, descriptor.Address)
		}
	}
	if len(addresses) == 0 {
		return nil, ErrManagerUnavailable
	}
	return addresses, nil
}

// ServerKind names the Accumulo 4 server role that published a
// ClientService endpoint.
type ServerKind string

const (
	// TabletServerKind is a tablet server (znode root "tservers").
	TabletServerKind ServerKind = "tserver"
	// ScanServerKind is a scan server (znode root "sservers").
	ScanServerKind ServerKind = "sserver"
	// CompactorKind is a compactor (znode root "compactors").
	CompactorKind ServerKind = "compactor"
)

// ClientService is one live ClientService endpoint together with the server
// role and resource group that published it.
type ClientService struct {
	Kind    ServerKind
	Group   string
	Address string
}

var clientServiceRoots = []struct {
	root string
	kind ServerKind
}{
	{root: "tservers", kind: TabletServerKind},
	{root: "sservers", kind: ScanServerKind},
	{root: "compactors", kind: CompactorKind},
}

// ClientServices returns the live Accumulo 4 ClientService endpoints
// advertised by tablet servers, scan servers, and compactors, each tagged
// with the publishing server role and resource group. Results are ordered by
// role (tserver, sserver, compactor), then group, then address. An address
// published by several roles or groups is reported once per (role, group).
// Returns ErrClientServiceUnavailable when nothing is advertised.
func ClientServices(ctx context.Context, locator LockReader) ([]ClientService, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if locator == nil {
		return nil, errors.New("zk: nil locator")
	}
	services := make([]ClientService, 0, 8)
	for _, serverRoot := range clientServiceRoots {
		root := path.Join(locator.InstancePath(), serverRoot.root)
		groups, err := locator.Children(ctx, root)
		if errors.Is(err, gozk.ErrNoNode) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("list client service resource groups %s: %w", root, err)
		}
		sort.Strings(groups)
		for _, group := range groups {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			groupPath := path.Join(root, group)
			servers, err := locator.Children(ctx, groupPath)
			if errors.Is(err, gozk.ErrNoNode) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("list client service servers %s: %w", groupPath, err)
			}
			sort.Strings(servers)
			groupAddresses := make(map[string]struct{}, len(servers))
			for _, server := range servers {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				serverPath := path.Join(groupPath, server)
				children, err := locator.Children(ctx, serverPath)
				if errors.Is(err, gozk.ErrNoNode) {
					continue
				}
				if err != nil {
					return nil, fmt.Errorf("list client service locks %s: %w", serverPath, err)
				}
				lockNode := firstLockNode(children)
				if lockNode == "" {
					continue
				}
				lockNodePath := path.Join(serverPath, lockNode)
				data, err := locator.GetRaw(ctx, lockNodePath)
				if errors.Is(err, gozk.ErrNoNode) {
					continue
				}
				if err != nil {
					return nil, fmt.Errorf("get client service lock %s: %w", lockNodePath, err)
				}
				var lock serviceLockData
				if err := json.Unmarshal(data, &lock); err != nil {
					return nil, fmt.Errorf("decode client service lock %s: %w", lockNodePath, err)
				}
				for _, descriptor := range lock.Descriptors {
					if descriptor.Service != "CLIENT" ||
						descriptor.Address == "" ||
						descriptor.Address == "0.0.0.0:0" {
						continue
					}
					if _, duplicate := groupAddresses[descriptor.Address]; duplicate {
						continue
					}
					groupAddresses[descriptor.Address] = struct{}{}
					services = append(services, ClientService{
						Kind:    serverRoot.kind,
						Group:   group,
						Address: descriptor.Address,
					})
				}
			}
		}
	}
	if len(services) == 0 {
		return nil, ErrClientServiceUnavailable
	}
	return services, nil
}

// ClientServiceAddresses returns the live Accumulo 4 ClientService endpoints
// advertised by tablet servers, scan servers, and compactors.
func ClientServiceAddresses(ctx context.Context, locator LockReader) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if locator == nil {
		return nil, errors.New("zk: nil locator")
	}
	addresses := make(map[string]struct{})
	for _, serverRoot := range []string{"tservers", "sservers", "compactors"} {
		root := path.Join(locator.InstancePath(), serverRoot)
		groups, err := locator.Children(ctx, root)
		if errors.Is(err, gozk.ErrNoNode) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("list client service resource groups %s: %w", root, err)
		}
		sort.Strings(groups)
		for _, group := range groups {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			groupPath := path.Join(root, group)
			servers, err := locator.Children(ctx, groupPath)
			if errors.Is(err, gozk.ErrNoNode) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("list client service servers %s: %w", groupPath, err)
			}
			sort.Strings(servers)
			for _, server := range servers {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				serverPath := path.Join(groupPath, server)
				children, err := locator.Children(ctx, serverPath)
				if errors.Is(err, gozk.ErrNoNode) {
					continue
				}
				if err != nil {
					return nil, fmt.Errorf("list client service locks %s: %w", serverPath, err)
				}
				lockNode := firstLockNode(children)
				if lockNode == "" {
					continue
				}
				lockNodePath := path.Join(serverPath, lockNode)
				data, err := locator.GetRaw(ctx, lockNodePath)
				if errors.Is(err, gozk.ErrNoNode) {
					continue
				}
				if err != nil {
					return nil, fmt.Errorf("get client service lock %s: %w", lockNodePath, err)
				}
				var lock serviceLockData
				if err := json.Unmarshal(data, &lock); err != nil {
					return nil, fmt.Errorf("decode client service lock %s: %w", lockNodePath, err)
				}
				for _, descriptor := range lock.Descriptors {
					if descriptor.Service != "CLIENT" ||
						descriptor.Address == "" ||
						descriptor.Address == "0.0.0.0:0" {
						continue
					}
					addresses[descriptor.Address] = struct{}{}
				}
			}
		}
	}
	if len(addresses) == 0 {
		return nil, ErrClientServiceUnavailable
	}
	result := make([]string, 0, len(addresses))
	for address := range addresses {
		result = append(result, address)
	}
	sort.Strings(result)
	return result, nil
}

func firstLockNode(children []string) string {
	sorted := sortedLockNodes(children)
	if len(sorted) == 0 {
		return ""
	}
	return sorted[0]
}

// sortedLockNodes returns the valid ServiceLock child names ordered by lock
// sequence, lowest first. The lowest sequence holds the lock; the rest are
// queued candidates. Names that do not match Accumulo's
// "zlock#<uuid>#<10-digit sequence>" form are ignored, mirroring
// ServiceLock.validateAndSort.
func sortedLockNodes(children []string) []string {
	type candidate struct {
		name     string
		sequence int64
	}
	valid := make([]candidate, 0, len(children))
	for _, child := range children {
		if !strings.HasPrefix(child, zLockPrefix) {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(child, zLockPrefix), "#")
		if len(parts) != 2 || len(parts[1]) != 10 {
			continue
		}
		if _, err := uuid.Parse(parts[0]); err != nil {
			continue
		}
		// Accumulo ServiceLock.validateAndSort uses Integer.parseInt.
		sequence, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			continue
		}
		valid = append(valid, candidate{name: child, sequence: sequence})
	}
	sort.Slice(valid, func(i, j int) bool {
		return valid[i].sequence < valid[j].sequence
	})
	names := make([]string, 0, len(valid))
	for _, entry := range valid {
		names = append(names, entry.name)
	}
	return names
}
