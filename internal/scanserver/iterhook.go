package scanserver

import (
	"fmt"
	"sort"

	"github.com/phrocker/shoal/internal/ivfpq"
	"github.com/phrocker/shoal/internal/rfile/wire"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
)

// cellPostProcessor consumes cells from the heap-merge in the order
// they would have been emitted, then produces the final result list.
// Used when a server-side iterator can be replicated in Go and
// REPLACES the streaming output (e.g., IvfPqDistanceIterator emits
// top-K in score order, not the original key order).
type cellPostProcessor interface {
	// offer feeds one heap-merge output cell to the iterator. Caller
	// MUST not retain the wire.Key beyond this call (the iterator
	// copies whatever it needs internally).
	offer(k *wire.Key, v []byte)
	// drain returns the iterator's final output as a list of TKeyValue
	// in the order they should be wire-shipped. Calling this also
	// resets the iterator's internal state.
	drain() []*data.TKeyValue
}

// buildPostProcessor inspects the request's iterator settings and
// returns a Go-native processor when one matches a class shoal knows
// how to run. Unknown iterators yield (nil, nil) — the caller falls
// back to the standard streaming path.
//
// Currently recognized:
//   - IvfPqDistanceIterator: replicated by internal/ivfpq.
//
// Multiple iterators in ssiList: V1 only supports a single recognized
// iterator (the Java handler only wires one for /vector/search).
// If more than one shoal-known iterator is configured we error out
// rather than guess at the composition order.
func buildPostProcessor(ssiList []*data.IterInfo, ssio map[string]map[string]string) (cellPostProcessor, error) {
	if len(ssiList) == 0 {
		return nil, nil
	}
	// Sort by priority ascending so we report the lowest-priority
	// recognized iterator first if the request has multiple. (Java's
	// SortedKeyValueIterator stack runs lowest-priority first.)
	sorted := make([]*data.IterInfo, len(ssiList))
	copy(sorted, ssiList)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})

	var picked cellPostProcessor
	for _, info := range sorted {
		if info == nil {
			continue
		}
		switch info.ClassName {
		case ivfpq.IteratorClassName:
			if picked != nil {
				return nil, fmt.Errorf("scanserver: multiple shoal-recognized iterators not supported in V1")
			}
			opts := ssio[info.IterName]
			it, err := ivfpq.NewFromOptions(opts)
			if err != nil {
				return nil, fmt.Errorf("scanserver: build ivfpq iterator (%s): %w", info.IterName, err)
			}
			picked = &ivfpqProcessor{it: it}
		default:
			// Unknown iterator. Java would error if a class wasn't on
			// the classpath; shoal mirrors this rather than silently
			// giving wrong answers. The Java caller is expected to
			// only set iterators shoal knows about (currently just
			// IvfPqDistanceIterator); anything else means a routing
			// mistake and the caller should fall back to tserver.
			return nil, fmt.Errorf("scanserver: unsupported iterator class %q (priority=%d, name=%s)",
				info.ClassName, info.Priority, info.IterName)
		}
	}
	return picked, nil
}

// ivfpqProcessor adapts ivfpq.Iterator to the cellPostProcessor
// interface. Cells flowing in must be the (row, V:_pq) pairs the
// iterator expects; the heap-merge typically already filters to those
// via the column pushdown layer, but the iterator silently drops
// wrong-length values either way.
type ivfpqProcessor struct {
	it *ivfpq.Iterator
}

func (p *ivfpqProcessor) offer(k *wire.Key, v []byte) {
	// Clone the row + CF + CQ + CV bytes here — the heap-merge owns
	// these slices and can recycle them on the next iteration.
	p.it.Offer(
		cloneBytes(k.Row),
		cloneBytes(k.ColumnFamily),
		cloneBytes(k.ColumnQualifier),
		cloneBytes(k.ColumnVisibility),
		k.Timestamp,
		v, // Drain owns the score bytes; the input value is not retained.
	)
}

func (p *ivfpqProcessor) drain() []*data.TKeyValue {
	cells := p.it.Drain()
	out := make([]*data.TKeyValue, 0, len(cells))
	for _, c := range cells {
		out = append(out, &data.TKeyValue{
			Key: &data.TKey{
				Row:           c.Row,
				ColFamily:     c.CF,
				ColQualifier:  c.CQ,
				ColVisibility: c.CV,
				Timestamp:     c.TS,
			},
			Value: c.Value, // 4-byte big-endian float32 score
		})
	}
	return out
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
