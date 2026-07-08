package index

import (
	"bytes"
	"fmt"

	"github.com/phrocker/shoal/internal/rfile/wire"
)

// Entries is a random-access view of the IndexEntries in one IndexBlock.
// We don't decode every entry up front — most callers (binary search,
// targeted seek) touch only log(N) entries. Decoding happens lazily on
// At(i).
//
// The view is read-only and shares storage with block.Data; callers
// must not mutate the block while iterating.
type Entries struct {
	block *IndexBlock
}

// EntriesOf returns a random-access view over block's entries.
func EntriesOf(block *IndexBlock) *Entries { return &Entries{block: block} }

// Len returns the number of entries in the underlying IndexBlock.
func (e *Entries) Len() int {
	if e.block == nil {
		return 0
	}
	return len(e.block.Offsets)
}

// At returns the i'th IndexEntry (0-indexed). Each call decodes from
// the preserved Data bytes — repeated calls re-decode, so callers
// hot-pathing the same index should hold onto the result.
func (e *Entries) At(i int) (*IndexEntry, error) {
	if i < 0 || i >= e.Len() {
		return nil, fmt.Errorf("Entries.At: index %d out of range [0, %d)", i, e.Len())
	}
	start := int(e.block.Offsets[i])
	if start < 0 || start > len(e.block.Data) {
		return nil, fmt.Errorf("Entries.At: offset %d out of range [0, %d]", start, len(e.block.Data))
	}
	// We don't slice with an explicit end — ReadIndexEntry consumes
	// exactly the bytes one entry occupies, so giving it the tail
	// slice is fine. If readers ever start needing the end position
	// (for sub-slicing into a separate buffer), we can compute it
	// from Offsets[i+1] / len(Data).
	r := bytes.NewReader(e.block.Data[start:])
	return ReadIndexEntry(r)
}

// KeyAt is a perf-oriented helper for binary search: only reads the
// Key portion of entry i, skipping the entries/offsets/sizes tail.
// Equivalent to At(i).Key but ~2× faster for typical key sizes.
func (e *Entries) KeyAt(i int) (*wire.Key, error) {
	if i < 0 || i >= e.Len() {
		return nil, fmt.Errorf("Entries.KeyAt: index %d out of range [0, %d)", i, e.Len())
	}
	start := int(e.block.Offsets[i])
	if start < 0 || start > len(e.block.Data) {
		return nil, fmt.Errorf("Entries.KeyAt: offset %d out of range [0, %d]", start, len(e.block.Data))
	}
	r := bytes.NewReader(e.block.Data[start:])
	return wire.ReadKey(r)
}
