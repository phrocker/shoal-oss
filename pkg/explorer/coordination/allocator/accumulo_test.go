package allocator

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/phrocker/shoal-oss/accumulo"
)

type fakeAccumuloScanner struct {
	values []accumulo.KeyValue
	ranges []*accumulo.Range
}

func (s *fakeAccumuloScanner) Scan(_ context.Context, scanRange *accumulo.Range) ([]accumulo.KeyValue, error) {
	s.ranges = append(s.ranges, scanRange)
	return append([]accumulo.KeyValue(nil), s.values...), nil
}

type fakeAccumuloWriter struct {
	status   accumulo.ConditionalStatus
	err      error
	mutation *accumulo.ConditionalMutation
}

func (w *fakeAccumuloWriter) Write(_ context.Context, mutation *accumulo.ConditionalMutation) (accumulo.ConditionalStatus, error) {
	w.mutation = mutation
	return w.status, w.err
}

func TestAccumuloStoreExactReadUsesTrustedCoordinates(t *testing.T) {
	row := []byte("allocator-row")
	visibility := []byte("CONTROL")
	scanner := &fakeAccumuloScanner{values: []accumulo.KeyValue{
		{Key: accumulo.Key{Row: row, ColumnFamily: []byte("q"), ColumnQualifier: []byte("head"), ColumnVisibility: visibility}, Value: []byte("head-value")},
		{Key: accumulo.Key{Row: row, ColumnFamily: []byte("r"), ColumnQualifier: []byte("other"), ColumnVisibility: visibility}, Value: []byte("ignored")},
	}}
	store := &AccumuloStore{scanner: scanner}
	coordinate := Coordinate{Row: row, Family: []byte("q"), Qualifier: []byte("head"), Visibility: visibility}
	cells, err := store.ReadExact(context.Background(), []Coordinate{coordinate})
	if err != nil || len(cells) != 1 || !cells[0].Coordinate.equal(coordinate) ||
		!bytes.Equal(cells[0].Value, []byte("head-value")) {
		t.Fatalf("ReadExact = %#v, %v", cells, err)
	}
	if len(scanner.ranges) != 1 || !bytes.Equal(scanner.ranges[0].StartRow(), row) ||
		!bytes.Equal(scanner.ranges[0].EndRow(), row) {
		t.Fatalf("exact row range = %#v", scanner.ranges)
	}
}

func TestAccumuloStoreConditionalMappingAndUnknownClassification(t *testing.T) {
	row := []byte("allocator-row")
	visibility := []byte("CONTROL")
	writer := &fakeAccumuloWriter{
		status: accumulo.ConditionalUnknown,
		err:    errors.Join(accumulo.ErrConditionalUnknown, errors.New("response lost")),
	}
	store := &AccumuloStore{writer: writer}
	request := Mutation{
		Row: row,
		Conditions: []Condition{
			{Coordinate: Coordinate{Row: row, Family: []byte("q"), Qualifier: []byte("head"), Visibility: visibility}, Value: []byte("before")},
			{Coordinate: Coordinate{Row: row, Family: []byte("r"), Qualifier: []byte{0, 0, 0, 1}, Visibility: visibility}, Absent: true},
		},
		Updates: []Update{
			{Coordinate: Coordinate{Row: row, Family: []byte("q"), Qualifier: []byte("head"), Visibility: visibility}, Value: []byte("after"), Timestamp: 2},
			{Coordinate: Coordinate{Row: row, Family: []byte("r"), Qualifier: []byte{0, 0, 0, 1}, Visibility: visibility}, Delete: true, Timestamp: 1},
		},
	}
	status, err := store.CompareAndMutate(context.Background(), request)
	if status != StatusUnknown || !errors.Is(err, ErrConditionalUnknown) {
		t.Fatalf("CompareAndMutate = %v, %v", status, err)
	}
	if writer.mutation == nil || !bytes.Equal(writer.mutation.Row(), row) {
		t.Fatal("conditional mutation did not retain exact row")
	}
	value := reflect.ValueOf(writer.mutation).Elem()
	conditions := value.FieldByName("conditions")
	if conditions.Len() != 2 {
		t.Fatalf("condition count = %d", conditions.Len())
	}
	assertConditionShape(t, conditions.Index(0), "q", "head", "CONTROL", "before", true)
	assertConditionShape(t, conditions.Index(1), "r", string([]byte{0, 0, 0, 1}), "CONTROL", "", false)
	wire := value.FieldByName("mutation").Elem()
	if got := int(wire.FieldByName("Entries").Int()); got != 2 {
		t.Fatalf("update count = %d", got)
	}
	values := wire.FieldByName("Values")
	if values.Len() > 1 || values.Len() == 1 && string(values.Index(0).Bytes()) != "after" {
		t.Fatalf("put values = %#v", values)
	}
	data := wire.FieldByName("Data").Bytes()
	for _, component := range [][]byte{[]byte("q"), []byte("head"), visibility, []byte("after"), []byte("r"), {0, 0, 0, 1}} {
		if !bytes.Contains(data, component) {
			t.Fatalf("encoded update data omits %x: %x", component, data)
		}
	}
}

func assertConditionShape(t *testing.T, value reflect.Value, family, qualifier, visibility, expected string, valueSet bool) {
	t.Helper()
	if string(value.FieldByName("columnFamily").Bytes()) != family ||
		string(value.FieldByName("columnQualifier").Bytes()) != qualifier ||
		string(value.FieldByName("columnVisibility").Bytes()) != visibility ||
		value.FieldByName("valueSet").Bool() != valueSet ||
		string(value.FieldByName("value").Bytes()) != expected {
		t.Fatalf("condition shape differs: %#v", value)
	}
}
