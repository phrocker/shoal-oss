package cclient

import (
	"bytes"
	"testing"
)

func TestNewMutation_RejectsEmptyRow(t *testing.T) {
	if _, err := NewMutation(nil); err == nil {
		t.Error("nil row should error")
	}
	if _, err := NewMutation([]byte{}); err == nil {
		t.Error("empty row should error")
	}
}

func TestNewMutation_DefensiveRowCopy(t *testing.T) {
	row := []byte("row-1")
	m, err := NewMutation(row)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	row[0] = 'X'
	if !bytes.Equal(m.Row(), []byte("row-1")) {
		t.Errorf("row not defensively copied: %q", m.Row())
	}
}

func TestMutation_PutAndDelete(t *testing.T) {
	m, _ := NewMutation([]byte("row"))
	m.Put([]byte("cf"), []byte("cq"), []byte("cv"), 1234, []byte("value"))
	m.PutLatest([]byte("cf"), []byte("cq2"), nil, []byte("v2"))
	m.Delete([]byte("cf"), []byte("cq"), nil, 5678)
	m.DeleteLatest([]byte("cf"), []byte("cq3"), nil)

	if m.Size() != 4 {
		t.Fatalf("Size = %d, want 4", m.Size())
	}
	es := m.Entries()
	// Put #1
	if es[0].Deleted || !bytes.Equal(es[0].Value, []byte("value")) || es[0].Timestamp != 1234 {
		t.Errorf("entry[0] = %+v", es[0])
	}
	// PutLatest
	if es[1].Deleted || es[1].Timestamp != MutationLatestTimestamp || !bytes.Equal(es[1].Value, []byte("v2")) {
		t.Errorf("entry[1] = %+v", es[1])
	}
	// Delete (no value)
	if !es[2].Deleted || es[2].Value != nil || es[2].Timestamp != 5678 {
		t.Errorf("entry[2] = %+v", es[2])
	}
	// DeleteLatest
	if !es[3].Deleted || es[3].Timestamp != MutationLatestTimestamp {
		t.Errorf("entry[3] = %+v", es[3])
	}
}

func TestMutation_LatestTimestampIsLongMax(t *testing.T) {
	// Mutation.h:54 — sharkbite's `9223372036854775807L`. Java uses Long.MAX_VALUE.
	if MutationLatestTimestamp != 9223372036854775807 {
		t.Errorf("MutationLatestTimestamp = %d, want 9223372036854775807", MutationLatestTimestamp)
	}
}

func TestMutation_SerializeNotImplemented(t *testing.T) {
	m, _ := NewMutation([]byte("row"))
	m.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("v"))
	if _, err := m.Serialize(); err == nil {
		t.Error("Serialize should error until write path lands")
	}
}
