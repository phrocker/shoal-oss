package accumulo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/phrocker/shoal/internal/zk"
)

const defaultZooKeeperSessionTimeout = 30 * time.Second

// InstanceInfo is the resolved identity of an Accumulo instance.
type InstanceInfo struct {
	Name string
	ID   string
}

// Instance provides the identity, client configuration, cluster topology, and
// lifecycle of an Accumulo instance source.
//
// The topology accessors mirror Sharkbite's cclient::data::Instance base
// class. RootTabletLocation, ManagerLocations, and Servers resolve live state
// from ZooKeeper on every call and honor ctx cancellation. Info, ZooKeepers,
// and Root report wiring fixed at construction. Configuration is different:
// it returns the instance's own mutable Configuration, so writes through the
// returned pointer are observed by later calls, while the caller's original
// Configuration stays independent because the instance stored a clone.
type Instance interface {
	Info() InstanceInfo

	// RootTabletLocation resolves the tablet server currently hosting the
	// root tablet.
	RootTabletLocation(ctx context.Context) (TabletLocation, error)

	// ManagerLocations lists the manager addresses advertised in ZooKeeper,
	// ordered by lock sequence, active manager first.
	ManagerLocations(ctx context.Context) ([]string, error)

	// Servers lists the live tablet servers, scan servers, and compactors
	// that advertise the Accumulo client service.
	Servers(ctx context.Context) ([]ServerConnection, error)

	// ZooKeepers returns the ZooKeeper servers this instance was configured
	// with.
	ZooKeepers() []string

	// Root returns the instance's ZooKeeper root path,
	// "/accumulo/<instance-id>".
	Root() string

	// Configuration returns the instance's client configuration. It is never
	// nil.
	Configuration() *Configuration

	Close() error
}

type locator interface {
	discoveryLocator
	Close()
}

type discoveryLocator interface {
	InstanceID() string
	RootTabletLocation(context.Context) (*zk.Location, error)
	InstancePath() string
	GetRaw(context.Context, string) ([]byte, error)
	Children(context.Context, string) ([]string, error)
}

type discoveryInstance interface {
	discoveryLocator() discoveryLocator
}

type zkLocator struct {
	info          InstanceInfo
	locator       locator
	zooKeepers    []string
	configuration *Configuration
	once          sync.Once
}

// NewZooKeeperInstance resolves an Accumulo 4 instance name through ZooKeeper.
func NewZooKeeperInstance(ctx context.Context, cfg ZooKeeperConfig) (Instance, error) {
	return newZooKeeperInstance(ctx, cfg, func(cfg ZooKeeperConfig) (locator, error) {
		return zk.NewWithAuth(
			append([]string(nil), cfg.Servers...),
			cfg.InstanceName,
			cfg.SessionTimeout,
			cfg.InstanceSecret,
		)
	})
}

func newZooKeeperInstance(
	ctx context.Context,
	cfg ZooKeeperConfig,
	factory func(ZooKeeperConfig) (locator, error),
) (Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(cfg.Servers) == 0 {
		return nil, errors.New("accumulo: at least one ZooKeeper server is required")
	}
	for _, server := range cfg.Servers {
		if server == "" {
			return nil, errors.New("accumulo: ZooKeeper server must not be empty")
		}
	}
	if cfg.InstanceName == "" {
		return nil, errors.New("accumulo: instance name is required")
	}
	if cfg.SessionTimeout < 0 {
		return nil, errors.New("accumulo: ZooKeeper session timeout must not be negative")
	}
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = defaultZooKeeperSessionTimeout
	}
	cfg.Servers = append([]string(nil), cfg.Servers...)

	type result struct {
		loc locator
		err error
	}
	resultCh := make(chan result)
	go func() {
		loc, err := factory(cfg)
		select {
		case resultCh <- result{loc: loc, err: err}:
		case <-ctx.Done():
			if loc != nil {
				loc.Close()
			}
		}
	}()

	var resolved result
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resolved = <-resultCh:
	}
	if err := ctx.Err(); err != nil {
		if resolved.loc != nil {
			resolved.loc.Close()
		}
		return nil, err
	}
	if resolved.err != nil {
		return nil, fmt.Errorf("accumulo: resolve instance %q: %w", cfg.InstanceName, resolved.err)
	}
	loc := resolved.loc
	if loc == nil {
		return nil, errors.New("accumulo: ZooKeeper locator factory returned nil")
	}
	if loc.InstanceID() == "" {
		loc.Close()
		return nil, errors.New("accumulo: ZooKeeper returned an empty instance ID")
	}
	return &zkLocator{
		info: InstanceInfo{
			Name: cfg.InstanceName,
			ID:   loc.InstanceID(),
		},
		locator:       loc,
		zooKeepers:    cfg.Servers,
		configuration: cfg.Configuration.Clone(),
	}, nil
}

func (i *zkLocator) Info() InstanceInfo { return i.info }
func (i *zkLocator) discoveryLocator() discoveryLocator {
	return i.locator
}

func (i *zkLocator) Close() error {
	i.once.Do(i.locator.Close)
	return nil
}

type staticInstance struct {
	info          InstanceInfo
	configuration *Configuration
}

// NewStaticInstance creates an instance identity without ZooKeeper discovery.
func NewStaticInstance(name, id string) (Instance, error) {
	if name == "" {
		return nil, errors.New("accumulo: instance name is required")
	}
	if id == "" {
		return nil, errors.New("accumulo: instance ID is required")
	}
	return &staticInstance{
		info:          InstanceInfo{Name: name, ID: id},
		configuration: NewConfiguration(),
	}, nil
}

func (i *staticInstance) Info() InstanceInfo { return i.info }
func (i *staticInstance) Close() error       { return nil }
