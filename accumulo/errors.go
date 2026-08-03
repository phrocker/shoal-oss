package accumulo

import "errors"

var (
	// ErrUnsupportedVersion indicates that the requested Accumulo wire version
	// is not implemented by this client.
	ErrUnsupportedVersion = errors.New("accumulo: unsupported version")
)
