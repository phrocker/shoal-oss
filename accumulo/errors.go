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

	// ErrTableExists indicates that a requested table name is already in use.
	ErrTableExists = errors.New("accumulo: table exists")

	// ErrInvalidTableName indicates that Accumulo rejected a table name.
	ErrInvalidTableName = errors.New("accumulo: invalid table name")

	// ErrInvalidProperty indicates that Accumulo rejected a property name or
	// value.
	ErrInvalidProperty = errors.New("accumulo: invalid property")

	// ErrNamespaceNotFound indicates that a table's target namespace does not exist.
	ErrNamespaceNotFound = errors.New("accumulo: namespace not found")

	// ErrPermissionDenied indicates that the authenticated principal is not
	// authorized for the requested operation.
	ErrPermissionDenied = errors.New("accumulo: permission denied")

	// ErrManagerUnavailable indicates that no active Accumulo Manager address
	// is currently advertised in ZooKeeper.
	ErrManagerUnavailable = errors.New("accumulo: manager unavailable")

	// ErrNoTabletCoversRow indicates malformed or stale metadata with no extent
	// covering the requested row.
	ErrNoTabletCoversRow = errors.New("accumulo: no tablet covers row")

	// ErrTabletNotLocated indicates that the covering tablet has no current
	// tablet-server assignment.
	ErrTabletNotLocated = errors.New("accumulo: tablet has no current location")

	// ErrConnectorClosed indicates an operation attempted on a closed Connector.
	ErrConnectorClosed = errors.New("accumulo: connector is closed")
)
