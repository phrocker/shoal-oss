package main

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/phrocker/shoal/accumulo"
)

func TestOwnedDataDescriptorConstructionIsConcurrent(t *testing.T) {
	start := &accumulo.Key{
		Row:              []byte{'s', 0, 'r'},
		ColumnFamily:     []byte("cf"),
		ColumnQualifier:  []byte("cq"),
		ColumnVisibility: []byte("A&B"),
		Timestamp:        17,
	}
	end := &accumulo.Key{
		Row:              []byte{'t', 0, 'r'},
		ColumnFamily:     []byte("ef"),
		ColumnQualifier:  []byte("eq"),
		ColumnVisibility: []byte("C|D"),
		Timestamp:        23,
	}
	scanRange, err := accumulo.NewKeyRange(start, true, end, false)
	if err != nil {
		t.Fatal(err)
	}
	setting, err := accumulo.NewIteratorSetting(
		"age", "com.example.Age", 19,
		map[string]string{"zeta": "last", "alpha": "first"},
	)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 64)
	for worker := 0; worker < 64; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rangeResult, err := buildRangeResult(scanRange, rangeBoundKey, rangeBoundKey)
			if err != nil {
				errors <- err
				return
			}
			rangeSnapshot, err := snapshotRangeResult(rangeResult)
			freeBuiltRangeResult(rangeResult)
			if err != nil {
				errors <- err
				return
			}
			if rangeSnapshot.startKind != rangeBoundKey ||
				rangeSnapshot.endKind != rangeBoundKey ||
				!rangeSnapshot.hasStart || !rangeSnapshot.hasEnd ||
				!rangeSnapshot.startInclusive || rangeSnapshot.endInclusive ||
				!bytes.Equal(rangeSnapshot.start.Row, start.Row) ||
				!bytes.Equal(rangeSnapshot.start.ColumnFamily, start.ColumnFamily) ||
				!bytes.Equal(rangeSnapshot.start.ColumnQualifier, start.ColumnQualifier) ||
				!bytes.Equal(rangeSnapshot.start.ColumnVisibility, start.ColumnVisibility) ||
				rangeSnapshot.start.Timestamp != start.Timestamp ||
				!bytes.Equal(rangeSnapshot.end.Row, end.Row) ||
				!bytes.Equal(rangeSnapshot.end.ColumnFamily, end.ColumnFamily) ||
				!bytes.Equal(rangeSnapshot.end.ColumnQualifier, end.ColumnQualifier) ||
				!bytes.Equal(rangeSnapshot.end.ColumnVisibility, end.ColumnVisibility) ||
				rangeSnapshot.end.Timestamp != end.Timestamp {
				errors <- fmt.Errorf("unexpected range snapshot: %#v", rangeSnapshot)
				return
			}

			iteratorResult, err := buildIteratorSettingResult(setting)
			if err != nil {
				errors <- err
				return
			}
			iteratorSnapshot, err := snapshotIteratorSettingResult(iteratorResult)
			freeBuiltIteratorSettingResult(iteratorResult)
			if err != nil {
				errors <- err
				return
			}
			if iteratorSnapshot.name != "age" ||
				iteratorSnapshot.className != "com.example.Age" ||
				iteratorSnapshot.priority != 19 ||
				iteratorSnapshot.options["alpha"] != "first" ||
				iteratorSnapshot.options["zeta"] != "last" {
				errors <- fmt.Errorf("unexpected iterator snapshot: %#v", iteratorSnapshot)
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
