package ingestservice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/phrocker/shoal/internal/ingestrouter"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
)

const setEncodingIteratorClass = "org.apache.accumulo.server.metadata.iterators.SetEncodingIterator"

type conditionIterator struct {
	class   string
	options map[string]string
}

type vintReader struct {
	data   []byte
	offset int
}

func decodeConditionIterators(encoded []byte, symbols []string) ([]conditionIterator, error) {
	reader := vintReader{data: encoded}
	count, err := reader.read()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, errors.New("negative iterator count")
	}
	iterators := make([]conditionIterator, 0, count)
	for range count {
		if _, err := reader.symbol(symbols); err != nil {
			return nil, fmt.Errorf("iterator name: %w", err)
		}
		class, err := reader.symbol(symbols)
		if err != nil {
			return nil, fmt.Errorf("iterator class: %w", err)
		}
		if _, err := reader.read(); err != nil {
			return nil, fmt.Errorf("iterator priority: %w", err)
		}
		optionCount, err := reader.read()
		if err != nil || optionCount < 0 {
			return nil, errors.New("invalid iterator option count")
		}
		options := make(map[string]string, optionCount)
		for range optionCount {
			key, err := reader.symbol(symbols)
			if err != nil {
				return nil, fmt.Errorf("iterator option key: %w", err)
			}
			value, err := reader.symbol(symbols)
			if err != nil {
				return nil, fmt.Errorf("iterator option value: %w", err)
			}
			options[key] = value
		}
		iterators = append(iterators, conditionIterator{class: class, options: options})
	}
	if reader.offset != len(reader.data) {
		return nil, errors.New("trailing condition iterator bytes")
	}
	return iterators, nil
}

func validateConditionIterators(encoded []byte, symbols []string) (bool, error) {
	if len(encoded) == 0 {
		return false, nil
	}
	iterators, err := decodeConditionIterators(encoded, symbols)
	if err != nil {
		return false, err
	}
	if len(iterators) == 0 {
		return false, nil
	}
	if len(iterators) != 1 || iterators[0].class != setEncodingIteratorClass {
		return false, fmt.Errorf("unsupported condition iterators %#v", iterators)
	}
	if _, err := strconv.ParseBool(iterators[0].options["concat.value"]); err != nil {
		return false, errors.New("invalid SetEncodingIterator concat.value")
	}
	return true, nil
}

func (r *vintReader) symbol(symbols []string) (string, error) {
	index, err := r.read()
	if err != nil {
		return "", err
	}
	if index < 0 || index >= len(symbols) {
		return "", fmt.Errorf("symbol index %d out of range", index)
	}
	return symbols[index], nil
}

func (r *vintReader) read() (int, error) {
	if r.offset >= len(r.data) {
		return 0, errors.New("truncated vint")
	}
	first := int8(r.data[r.offset])
	r.offset++
	length := hadoopVIntSize(first)
	if length == 1 {
		return int(first), nil
	}
	if r.offset+length-1 > len(r.data) {
		return 0, errors.New("truncated vint")
	}
	var value uint64
	for range length - 1 {
		value = value<<8 | uint64(r.data[r.offset])
		r.offset++
	}
	if first < -120 {
		value = ^value
	}
	return int(int64(value)), nil
}

func hadoopVIntSize(first int8) int {
	if first >= -112 {
		return 1
	}
	if first < -120 {
		return -119 - int(first)
	}
	return -111 - int(first)
}

func iteratorConditionMatches(
	row []byte,
	condition *data.TCondition,
	cells []ingestrouter.Cell,
	symbols []string,
) (bool, error) {
	iterators, err := decodeConditionIterators(condition.Iterators, symbols)
	if err != nil {
		return false, err
	}
	concat, err := strconv.ParseBool(iterators[0].options["concat.value"])
	if err != nil {
		return false, errors.New("invalid SetEncodingIterator concat.value")
	}
	latest := make(map[string]ingestrouter.Cell)
	for _, cell := range cells {
		if !bytes.Equal(cell.Row, row) ||
			!bytes.Equal(cell.ColumnFamily, condition.Cf) ||
			!bytes.Equal(cell.ColumnVisibility, condition.Cv) {
			continue
		}
		key := string(cell.ColumnQualifier) + "\x00" + string(cell.ColumnVisibility)
		if previous, ok := latest[key]; !ok || cell.Timestamp >= previous.Timestamp {
			latest[key] = cell
		}
	}
	entries := make([][]byte, 0, len(latest))
	for _, cell := range latest {
		if cell.Deleted {
			continue
		}
		entry := append([]byte(nil), cell.ColumnQualifier...)
		if concat {
			entry = append(entry, 0)
			entry = append(entry, cell.Value...)
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i], entries[j]) < 0
	})
	var encoded bytes.Buffer
	for _, entry := range entries {
		_ = binary.Write(&encoded, binary.BigEndian, int32(len(entry)))
		_, _ = encoded.Write(entry)
	}
	_ = binary.Write(&encoded, binary.BigEndian, int32(len(entries)))
	return bytes.Equal(encoded.Bytes(), condition.Val), nil
}
