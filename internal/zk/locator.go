// Package zk wraps github.com/go-zookeeper/zk and resolves the root-tablet
// tserver location from the bootstrap chain
//
//	/accumulo/instances/<name>      -> instance UUID (bytes)
//	/accumulo/<uuid>/root_tablet    -> JSON RootTabletMetadata
//
// Reference: core/.../client/clientImpl/RootClientTabletCache.java:115-148
// Schema:    core/.../metadata/schema/RootTabletMetadata.java
package zk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	gozk "github.com/go-zookeeper/zk"
)

// ZK paths. Mirror core/.../Constants.java + RootTable.java.
const (
	zRoot       = "/accumulo"
	zInstances  = "/instances"
	zRootTablet = "/root_tablet"

	// Column-family names from MetadataSchema.java.
	cfCurrentLocation = "loc"
	cfFutureLocation  = "future" //nolint:unused // reserved for future fallback

	// RootTabletMetadata.VERSION (Java: private static final int VERSION = 1).
	rootTabletMetadataVersion = 1
)

// Location is a resolved tablet location: which tserver hosts it and which
// session lock it holds.
type Location struct {
	HostPort string // e.g. "tserver-3.namespace.svc.cluster.local:9997"
	Session  string // lockId (qualifier under the "loc" column family)
}

// Locator resolves Accumulo metadata from ZooKeeper. Construct with New
// and Close when done.
type Locator struct {
	conn         *gozk.Conn
	instanceName string
	instanceID   string
}

// New connects to a ZK quorum, resolves the instance name to its instance
// UUID, and returns a Locator ready to issue further lookups.
func New(servers []string, instanceName string, sessionTimeout time.Duration) (*Locator, error) {
	return NewWithAuth(servers, instanceName, sessionTimeout, "")
}

// NewWithAuth is New plus an optional digest-auth secret. When non-empty
// the locator's ZK session adds ("digest", "accumulo:"+secret) before
// the first GetRaw / Children call, matching Accumulo's instance-secret
// scheme (core ZooSession.digestAuth). World-readable znodes (root
// tablet, namespaces JSON, instance UUID) don't need this; per-table
// /config znodes do.
func NewWithAuth(servers []string, instanceName string, sessionTimeout time.Duration, instanceSecret string) (*Locator, error) {
	conn, _, err := gozk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, fmt.Errorf("zk connect: %w", err)
	}
	if instanceSecret != "" {
		if err := conn.AddAuth("digest", []byte("accumulo:"+instanceSecret)); err != nil {
			conn.Close()
			return nil, fmt.Errorf("zk add digest auth: %w", err)
		}
	}
	l := &Locator{conn: conn, instanceName: instanceName}
	id, err := l.lookupInstanceID()
	if err != nil {
		conn.Close()
		return nil, err
	}
	l.instanceID = id
	return l, nil
}

// InstanceID returns the resolved Accumulo instance UUID.
func (l *Locator) InstanceID() string { return l.instanceID }

// Close terminates the ZK session.
func (l *Locator) Close() { l.conn.Close() }

func (l *Locator) lookupInstanceID() (string, error) {
	p := path.Join(zRoot, zInstances, l.instanceName)
	data, _, err := l.conn.Get(p)
	if err != nil {
		return "", fmt.Errorf("get %s: %w", p, err)
	}
	if len(data) == 0 {
		return "", fmt.Errorf("instance %q not found at %s", l.instanceName, p)
	}
	return string(data), nil
}

// RootTabletLocation reads the root-tablet znode and returns the current
// tserver location. Returns (nil, nil) if no current location is set —
// e.g. during tablet movement; caller should retry.
func (l *Locator) RootTabletLocation(_ context.Context) (*Location, error) {
	p := path.Join(zRoot, l.instanceID, zRootTablet)
	data, _, err := l.conn.Get(p)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", p, err)
	}
	return parseRootTabletMetadata(data)
}

// InstancePath returns the absolute ZK path for the bound instance,
// i.e. "/accumulo/<instance-id>". Useful for callers that need to walk
// instance-scoped subtrees (table config, etc.).
func (l *Locator) InstancePath() string {
	return path.Join(zRoot, l.instanceID)
}

// GetRaw fetches the raw znode bytes at an absolute path. ErrNoNode from
// the underlying ZK client is surfaced as a wrapped error so callers can
// distinguish "missing" from transport failures via errors.Is(err,
// gozk.ErrNoNode).
func (l *Locator) GetRaw(_ context.Context, znodePath string) ([]byte, error) {
	data, _, err := l.conn.Get(znodePath)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", znodePath, err)
	}
	return data, nil
}

// Children returns the child znode names of znodePath. Names are NOT
// joined with znodePath; callers join as needed. Empty slice for a
// childless znode; ErrNoNode for a missing znode (wrapped).
func (l *Locator) Children(_ context.Context, znodePath string) ([]string, error) {
	names, _, err := l.conn.Children(znodePath)
	if err != nil {
		return nil, fmt.Errorf("children %s: %w", znodePath, err)
	}
	return names, nil
}

// rootTabletJSON mirrors RootTabletMetadata.Data (Gson struct).
type rootTabletJSON struct {
	Version      int                          `json:"version"`
	ColumnValues map[string]map[string]string `json:"columnValues"`
}

// parseRootTabletMetadata extracts the current tserver location from the
// JSON stored at /accumulo/<uuid>/root_tablet. Returns (nil, nil) if no
// "loc" column-family entry is present.
//
// Reference: RootTabletMetadata.java — schema, version check, and the
// invariant that there is at most one location across "loc" + "future".
func parseRootTabletMetadata(data []byte) (*Location, error) {
	if len(data) == 0 {
		return nil, errors.New("root_tablet znode is empty")
	}

	var rt rootTabletJSON
	if err := json.Unmarshal(data, &rt); err != nil {
		return nil, fmt.Errorf("parse RootTabletMetadata json: %w", err)
	}
	if rt.Version != rootTabletMetadataVersion {
		return nil, fmt.Errorf("unsupported RootTabletMetadata version: got %d, want %d",
			rt.Version, rootTabletMetadataVersion)
	}

	loc, ok := rt.ColumnValues[cfCurrentLocation]
	if !ok || len(loc) == 0 {
		return nil, nil // no current location (likely mid-move); caller retries
	}
	if len(loc) > 1 {
		// Java enforces this invariant; report rather than guess which to use.
		return nil, fmt.Errorf("root tablet has %d current locations, expected at most 1", len(loc))
	}
	for session, hostPort := range loc {
		return &Location{HostPort: hostPort, Session: session}, nil
	}
	return nil, nil // unreachable
}
