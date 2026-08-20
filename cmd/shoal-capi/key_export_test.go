package main

import (
	"bytes"
	"sync"
	"testing"

	"github.com/phrocker/shoal/accumulo"
)

func TestOwnedKeyStateConcurrentGetSet(t *testing.T) {
	state := &ownedKeyState{key: accumulo.NewKeyWithColumns(
		[]byte("start"), []byte("start"), nil, nil, accumulo.DefaultKeyTimestamp,
	)}
	const workers = 24
	const iterations = 500

	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := range workers {
		go func() {
			defer wait.Done()
			for iteration := range iterations {
				if worker%2 == 0 {
					value := []byte{byte(worker), byte(iteration)}
					state.mu.Lock()
					state.key.SetRow(value)
					state.key.SetColumnFamily(value)
					state.mu.Unlock()
					value[0] ^= 0xff
				} else {
					state.mu.RLock()
					snapshot := state.key.Clone()
					state.mu.RUnlock()
					if len(snapshot.Row) != len(snapshot.ColumnFamily) ||
						!bytes.Equal(snapshot.Row, snapshot.ColumnFamily) {
						t.Errorf("torn key snapshot: row=%v family=%v", snapshot.Row, snapshot.ColumnFamily)
						return
					}
				}
			}
		}()
	}
	wait.Wait()
}

func TestOwnedKeySnapshotIsIndependent(t *testing.T) {
	state := &ownedKeyState{key: accumulo.NewKeyWithColumns(
		[]byte("row"), []byte("cf"), []byte("cq"), []byte("A"), 9,
	)}
	state.mu.RLock()
	snapshot := state.key.Clone()
	state.mu.RUnlock()

	state.mu.Lock()
	state.key.SetRow([]byte("changed"))
	state.mu.Unlock()

	if string(snapshot.Row) != "row" {
		t.Fatalf("snapshot row = %q, want row", snapshot.Row)
	}
}
