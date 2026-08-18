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

	// ErrNamespaceNotFound indicates that no namespace matches the requested
	// name or ID, or that a table operation targeted a namespace that does not
	// exist.
	ErrNamespaceNotFound = errors.New("accumulo: namespace not found")

	// ErrPermissionDenied indicates that the authenticated principal is not
	// authorized for the requested operation.
	ErrPermissionDenied = errors.New("accumulo: permission denied")

	// ErrManagerUnavailable indicates that no active Accumulo Manager address
	// is currently advertised in ZooKeeper.
	ErrManagerUnavailable = errors.New("accumulo: manager unavailable")

	// ErrClientServiceUnavailable indicates that no live server currently
	// advertises the Accumulo ClientService API in ZooKeeper.
	ErrClientServiceUnavailable = errors.New("accumulo: client service unavailable")

	// ErrNoTabletCoversRow indicates malformed or stale metadata with no extent
	// covering the requested row.
	ErrNoTabletCoversRow = errors.New("accumulo: no tablet covers row")

	// ErrTabletNotLocated indicates that the covering tablet has no current
	// tablet-server assignment.
	ErrTabletNotLocated = errors.New("accumulo: tablet has no current location")

	// ErrConnectorClosed indicates an operation attempted on a closed Connector.
	ErrConnectorClosed = errors.New("accumulo: connector is closed")

	// ErrInvalidBulkDir indicates that a bulk-import call was given an empty
	// or otherwise unusable bulk directory path.
	ErrInvalidBulkDir = errors.New("accumulo: invalid bulk directory")

	// ErrInvalidUser indicates that a user name is empty or otherwise invalid.
	ErrInvalidUser = errors.New("accumulo: invalid user")

	// ErrUserExists indicates that a requested user already exists.
	ErrUserExists = errors.New("accumulo: user exists")

	// ErrUserNotFound indicates that a requested user does not exist.
	ErrUserNotFound = errors.New("accumulo: user not found")

	// ErrInvalidPassword indicates that a nil password was supplied.
	ErrInvalidPassword = errors.New("accumulo: invalid password")

	// ErrBadCredentials indicates rejected, invalid, or expired credentials.
	ErrBadCredentials = errors.New("accumulo: bad credentials")

	// ErrInvalidAuthorizations indicates an invalid authorization set.
	ErrInvalidAuthorizations = errors.New("accumulo: invalid authorizations")

	// ErrInvalidPermission indicates a permission outside Accumulo's wire enum.
	ErrInvalidPermission = errors.New("accumulo: invalid permission")

	// ErrInvalidNamespaceName indicates a rejected namespace name.
	ErrInvalidNamespaceName = errors.New("accumulo: invalid namespace name")

	// ErrUnsupportedOperation indicates that the server rejected an operation
	// as unsupported.
	ErrUnsupportedOperation = errors.New("accumulo: unsupported operation")

	// ErrSecurityUnavailable indicates a server-side security subsystem failure.
	ErrSecurityUnavailable = errors.New("accumulo: security subsystem unavailable")
)
