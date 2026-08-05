package cclient

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/phrocker/shoal/internal/rfile/wire"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
)

const mutationValueCopyCutoff = 1 << 15

// MutationLatestTimestamp is the Go model's sentinel for omitting a timestamp
// from the serialized mutation so the tablet server assigns one.
const MutationLatestTimestamp int64 = 9223372036854775807

// MutationEntry is one Put or Delete in a mutation.
type MutationEntry struct {
	ColFamily     []byte
	ColQualifier  []byte
	ColVisibility []byte
	Timestamp     int64
	Value         []byte
	Deleted       bool
}

// Mutation is a row plus an ordered list of column updates.
type Mutation struct {
	row     []byte
	entries []MutationEntry
}

// NewMutation allocates a Mutation for the given non-empty row.
func NewMutation(row []byte) (*Mutation, error) {
	if len(row) == 0 {
		return nil, errors.New("cclient: Mutation row must be non-empty")
	}
	rowCopy := make([]byte, len(row))
	copy(rowCopy, row)
	return &Mutation{row: rowCopy}, nil
}

// Row returns the row this mutation targets.
func (m *Mutation) Row() []byte { return m.row }

// Entries returns the ordered list of column entries.
func (m *Mutation) Entries() []MutationEntry { return m.entries }

// Put appends a Put entry. Inputs are defensively copied.
func (m *Mutation) Put(cf, cq, cv []byte, timestamp int64, value []byte) {
	m.entries = append(m.entries, MutationEntry{
		ColFamily:     cloneBytes(cf),
		ColQualifier:  cloneBytes(cq),
		ColVisibility: cloneBytes(cv),
		Timestamp:     timestamp,
		Value:         cloneBytes(value),
		Deleted:       false,
	})
}

// PutLatest is the convenience form: timestamp = MutationLatestTimestamp.
func (m *Mutation) PutLatest(cf, cq, cv, value []byte) {
	m.Put(cf, cq, cv, MutationLatestTimestamp, value)
}

// Delete appends a tombstone entry. Inputs are defensively copied.
func (m *Mutation) Delete(cf, cq, cv []byte, timestamp int64) {
	m.entries = append(m.entries, MutationEntry{
		ColFamily:     cloneBytes(cf),
		ColQualifier:  cloneBytes(cq),
		ColVisibility: cloneBytes(cv),
		Timestamp:     timestamp,
		Value:         nil,
		Deleted:       true,
	})
}

// DeleteLatest is the convenience form: timestamp = MutationLatestTimestamp.
func (m *Mutation) DeleteLatest(cf, cq, cv []byte) {
	m.Delete(cf, cq, cv, MutationLatestTimestamp)
}

// Size returns the number of column entries — matches sharkbite's
// `Mutation::size()` (Mutation.h:76-78).
func (m *Mutation) Size() int { return len(m.entries) }

// Cell is one column entry of a Mutation projected onto a wire.Key plus its
// value — the sortable shape a key-ordered merger (internal/memtable)
// consumes. Key.Row aliases the Mutation's row slice and the cf/cq/cv/value
// slices alias the MutationEntry's, so a Cell is only valid while its parent
// Mutation is retained; callers that outlive it must Clone the Key and copy
// Value.
type Cell struct {
	Key   wire.Key
	Value []byte
}

// Cells projects every MutationEntry onto a (wire.Key, value) Cell. The WAL
// is append-order, not key-sorted, so the merger sorts these on insert.
//
// Ordering note carried from wire.Key: a MutationEntry with Deleted=true
// becomes a Key that sorts before the matching live cell, and the timestamp
// sorts descending — both handled by wire.Key.Compare, not here.
//
// Slices are aliased, not copied (see Cell). The result slice is allocated
// once at the exact entry count.
func (m *Mutation) Cells() []Cell {
	if len(m.entries) == 0 {
		return nil
	}
	cells := make([]Cell, len(m.entries))
	for i := range m.entries {
		e := &m.entries[i]
		cells[i] = Cell{
			Key: wire.Key{
				Row:              m.row,
				ColumnFamily:     e.ColFamily,
				ColumnQualifier:  e.ColQualifier,
				ColumnVisibility: e.ColVisibility,
				Timestamp:        e.Timestamp,
				Deleted:          e.Deleted,
			},
			Value: e.Value,
		}
	}
	return cells
}

// Serialize encodes the column-update stream stored in TMutation.Data.
func (m *Mutation) Serialize() ([]byte, error) {
	serialized, err := m.serialize()
	if err != nil {
		return nil, err
	}
	return serialized.data, nil
}

// ToThrift converts the mutation to the internal Accumulo 4 wire type.
func (m *Mutation) ToThrift() (*data.TMutation, error) {
	serialized, err := m.serialize()
	if err != nil {
		return nil, err
	}
	return &data.TMutation{
		Row:     cloneBytes(m.row),
		Data:    serialized.data,
		Values:  serialized.values,
		Entries: int32(len(m.entries)),
	}, nil
}

type serializedMutation struct {
	data   []byte
	values [][]byte
}

func (m *Mutation) serialize() (serializedMutation, error) {
	if m == nil {
		return serializedMutation{}, errors.New("cclient: nil Mutation")
	}
	if len(m.row) == 0 {
		return serializedMutation{}, errors.New("cclient: Mutation row must be non-empty")
	}
	if len(m.entries) > math.MaxInt32 {
		return serializedMutation{}, errors.New("cclient: Mutation has too many entries")
	}

	var encoded bytes.Buffer
	var values [][]byte
	for index := range m.entries {
		entry := &m.entries[index]
		if err := writeMutationBytes(&encoded, entry.ColFamily); err != nil {
			return serializedMutation{}, fmt.Errorf("cclient: encode column family: %w", err)
		}
		if err := writeMutationBytes(&encoded, entry.ColQualifier); err != nil {
			return serializedMutation{}, fmt.Errorf("cclient: encode column qualifier: %w", err)
		}
		if err := writeMutationBytes(&encoded, entry.ColVisibility); err != nil {
			return serializedMutation{}, fmt.Errorf("cclient: encode column visibility: %w", err)
		}

		hasTimestamp := entry.Timestamp != MutationLatestTimestamp
		if err := encoded.WriteByte(boolByte(hasTimestamp)); err != nil {
			return serializedMutation{}, err
		}
		if hasTimestamp {
			if _, err := wire.WriteVLong(&encoded, entry.Timestamp); err != nil {
				return serializedMutation{}, fmt.Errorf("cclient: encode timestamp: %w", err)
			}
		}
		if err := encoded.WriteByte(boolByte(entry.Deleted)); err != nil {
			return serializedMutation{}, err
		}

		if len(entry.Value) < mutationValueCopyCutoff {
			if err := writeMutationBytes(&encoded, entry.Value); err != nil {
				return serializedMutation{}, fmt.Errorf("cclient: encode value: %w", err)
			}
			continue
		}
		values = append(values, cloneBytes(entry.Value))
		if _, err := wire.WriteVLong(&encoded, -int64(len(values))); err != nil {
			return serializedMutation{}, fmt.Errorf("cclient: encode large value reference: %w", err)
		}
	}
	return serializedMutation{data: encoded.Bytes(), values: values}, nil
}

func writeMutationBytes(buffer *bytes.Buffer, value []byte) error {
	if _, err := wire.WriteVLong(buffer, int64(len(value))); err != nil {
		return err
	}
	_, err := buffer.Write(value)
	return err
}

func boolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
