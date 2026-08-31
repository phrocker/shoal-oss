package metadata

import "github.com/phrocker/shoal-oss/internal/embeddingspace"

// TabletInfo is the resolved metadata for a single tablet: which tserver
// hosts it, the row range it covers, and the RFiles backing it.
type TabletInfo struct {
	TableID string
	EndRow  []byte // nil for the last (default) tablet of a table
	PrevRow []byte // nil if first tablet
	// PrevRowSet distinguishes a valid first tablet from a partial/corrupt
	// metadata row that omitted the required ~tab:~pr column.
	PrevRowSet bool
	Location   *Location
	// FutureLocation is the manager assignment not yet promoted to loc.
	FutureLocation *Location
	Directory      string
	Time           string
	ServerLock     string
	Files          []FileEntry
	// Logs are the tablet's unflushed WAL segments, parsed from the
	// metadata "log:" column family. Empty for a fully flushed tablet.
	// The WAL-merged read path (scanserver, Phase W2) drains these on
	// top of Files so a scan sees writes not yet flushed to an RFile.
	Logs []LogEntry

	fileEmbeddings map[string]embeddingspace.FileState
}

// LogEntry is one WAL segment referenced by a tablet's "log:" column
// family. It carries enough to re-open the segment from the quorum WAL
// sidecar: the segment UUID, its path, and the peer addresses that hold
// replicas.
type LogEntry struct {
	UUID         string   // segment identifier (filename component)
	Path         string   // WAL path encoded after the qualifier's "-/" prefix
	WALPath      string   // path passed to the sidecar opener
	Server       string   // host:port decoded from the path's host+port component
	Peers        []string // optional replicas supplied by a later resolver
	RawQualifier []byte   // exact metadata qualifier for mutations
}

// Location pairs a tserver host:port with the lock session it holds for a
// given tablet. The session is sent on subsequent Thrift calls so the
// server can detect stale assignments.
type Location struct {
	HostPort string
	Session  string // hex-encoded eid component of the lockID
}

// FileEntry is one RFile backing a tablet, plus its DataFileValue stats.
//
// In Accumulo 4.0+ the column qualifier under "file:" is a JSON object
// (StoredTabletFile) with path + startRow + endRow. Path is the absolute
// URI; StartRow/EndRow define a range inside the file (file slicing — most
// files use empty range, meaning "whole file").
type FileEntry struct {
	Path       string // URI from the StoredTabletFile JSON
	StartRow   string // empty for whole-file entries (the common case)
	EndRow     string // empty for whole-file entries
	Size       int64
	NumEntries int64
	Time       int64 // -1 when unset
	Embedding  embeddingspace.FileState

	// RawQualifier preserves the exact JSON bytes as they appeared in the
	// metadata table. Required if you ever want to delete or update this
	// entry — Accumulo enforces byte-exact qualifier match for mutations.
	RawQualifier []byte
}
