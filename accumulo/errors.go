package accumulo

import "errors"

var (
	// ErrUnsupportedVersion indicates that the requested Accumulo wire version
	// is not implemented by this client.
	ErrUnsupportedVersion = errors.New("accumulo: unsupported version")

	// ErrDiscoveryUnavailable indicates that the Instance was created without
	// ZooKeeper-backed metadata discovery.
	ErrDiscoveryUnavailable = errors.New("accumulo: discovery unavailable")

	// ErrTableNotFound indicates that no table matches the requested name or ID.
	ErrTableNotFound = errors.New("accumulo: table not found")

	// ErrNoTabletCoversRow indicates malformed or stale metadata with no extent
	// covering the requested row.
	ErrNoTabletCoversRow = errors.New("accumulo: no tablet covers row")

	// ErrTabletNotLocated indicates that the covering tablet has no current
	// tablet-server assignment.
	ErrTabletNotLocated = errors.New("accumulo: tablet has no current location")

	// ErrConnectorClosed indicates an operation attempted on a closed Connector.
	ErrConnectorClosed = errors.New("accumulo: connector is closed")
)
