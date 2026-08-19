package rfile

import "errors"

var (
	// ErrClosed reports an operation attempted after Close. Close releases the
	// file handle, so nothing can be read or written afterwards.
	ErrClosed = errors.New("rfile: closed")

	// ErrNoTop reports Top, TopKey, TopValue, or Next called while the cursor
	// has no entry: before the first Seek, or after the range was exhausted.
	// Sharkbite raises StopIteration in the same situations.
	ErrNoTop = errors.New("rfile: iterator has no top entry")

	// ErrInvalidSeekable reports a Seekable built without a range, or with
	// column-family restrictions that cannot be honored.
	ErrInvalidSeekable = errors.New("rfile: invalid seekable")

	// ErrInvalidPath reports an empty or otherwise unusable file path.
	ErrInvalidPath = errors.New("rfile: invalid path")

	// ErrUnsupportedCodec reports a WriterOptions.Codec that no registered
	// compressor implements. Create reports it before it touches the file.
	ErrUnsupportedCodec = errors.New("rfile: unsupported codec")

	// ErrOutOfOrder reports an Append whose key sorts at or before the
	// previously appended key. RFile entries must be strictly increasing.
	ErrOutOfOrder = errors.New("rfile: keys must be appended in strictly increasing order")

	// ErrLocalityGroupUnsupported reports an attempt to add a named locality
	// group. Shoal's writer emits the default locality group only, which is
	// what Accumulo 4 tablets and Shoal's own compactor read.
	ErrLocalityGroupUnsupported = errors.New("rfile: named locality groups are not supported by this writer")
)
