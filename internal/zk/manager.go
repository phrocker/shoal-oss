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
)

var (
	ErrManagerUnavailable       = errors.New("zk: manager unavailable")
	ErrClientServiceUnavailable = errors.New("zk: client service unavailable")
)

type serviceLockData struct {
	Descriptors []serviceDescriptor `json:"descriptors"`
}

type serviceDescriptor struct {
	Service string `json:"service"`
	Address string `json:"address"`
}

func ManagerAddress(ctx context.Context, locator interface {
	InstancePath() string
	GetRaw(context.Context, string) ([]byte, error)
	Children(context.Context, string) ([]string, error)
}) (string, error) {
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
			return "", ErrManagerUnavailable
		}
		return "", fmt.Errorf("list manager locks %s: %w", lockPath, err)
	}
	lockNode := firstLockNode(children)
	if lockNode == "" {
		return "", ErrManagerUnavailable
	}
	data, err := locator.GetRaw(ctx, path.Join(lockPath, lockNode))
	if err != nil {
		if errors.Is(err, gozk.ErrNoNode) {
			return "", ErrManagerUnavailable
		}
		return "", fmt.Errorf("get manager lock %s: %w", lockPath, err)
	}
	var lock serviceLockData
	if err := json.Unmarshal(data, &lock); err != nil {
		return "", fmt.Errorf("decode manager lock %s: %w", lockPath, err)
	}
	for _, descriptor := range lock.Descriptors {
		if descriptor.Service != "MANAGER" {
			continue
		}
		if descriptor.Address == "" || descriptor.Address == "0.0.0.0:0" {
			return "", ErrManagerUnavailable
		}
		return descriptor.Address, nil
	}
	return "", ErrManagerUnavailable
}

// ClientServiceAddresses returns the live Accumulo 4 ClientService endpoints
// advertised by tablet servers, scan servers, and compactors.
func ClientServiceAddresses(ctx context.Context, locator interface {
	InstancePath() string
	GetRaw(context.Context, string) ([]byte, error)
	Children(context.Context, string) ([]string, error)
}) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if locator == nil {
		return nil, errors.New("zk: nil locator")
	}
	addresses := make(map[string]struct{})
	for _, serverRoot := range []string{"/tservers", "/sservers", "/compactors"} {
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
				lockPath := path.Join(groupPath, server)
				children, err := locator.Children(ctx, lockPath)
				if errors.Is(err, gozk.ErrNoNode) {
					continue
				}
				if err != nil {
					return nil, fmt.Errorf("list client service locks %s: %w", lockPath, err)
				}
				lockNode := firstLockNode(children)
				if lockNode == "" {
					continue
				}
				data, err := locator.GetRaw(ctx, path.Join(lockPath, lockNode))
				if errors.Is(err, gozk.ErrNoNode) {
					continue
				}
				if err != nil {
					return nil, fmt.Errorf("get client service lock %s: %w", lockPath, err)
				}
				var lock serviceLockData
				if err := json.Unmarshal(data, &lock); err != nil {
					return nil, fmt.Errorf("decode client service lock %s: %w", lockPath, err)
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
	if len(valid) == 0 {
		return ""
	}
	return valid[0].name
}
