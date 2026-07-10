// Package enginebackend adapts a *engine.Engine to the shoalql.Backend seam,
// so the SQL layer can run against real shoal tables. It is deliberately a
// separate package: the core shoalql parser/planner/executor never import the
// engine, which keeps them unit-testable with in-memory fakes.
package enginebackend

import (
	"context"

	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/shoalql"
)

// Backend wraps an *engine.Engine as a shoalql.Backend.
type Backend struct {
	eng *engine.Engine
}

// New builds a Backend over eng.
func New(eng *engine.Engine) *Backend { return &Backend{eng: eng} }

var _ shoalql.Backend = (*Backend)(nil)

// Scan implements shoalql.Backend. The pushdown stack is hosted above a
// whole-table merge (ScanHosted) so re-seeking iterators such as VectorKNN
// and TermIndex see every cell regardless of tablet boundaries. *engine.Scanner
// already satisfies shoalql.RowStream because iterrt.Key aliases wire.Key.
func (b *Backend) Scan(_ context.Context, table string, r iterrt.Range, req shoalql.ScanRequest) (shoalql.RowStream, error) {
	opts := engine.ScanOptions{
		ColumnFamilies:          req.ColumnFamilies,
		ColumnFamiliesInclusive: req.CFInclusive,
	}
	return b.eng.ScanHosted(table, r, opts, req.Stack)
}

// LookupRows implements shoalql.Backend, buffering the engine's visitor
// callback into copied cells (KNN hydration sets are small).
func (b *Backend) LookupRows(_ context.Context, table string, rows [][]byte, req shoalql.ScanRequest) ([]shoalql.Cell, error) {
	opts := engine.ScanOptions{
		ColumnFamilies:          req.ColumnFamilies,
		ColumnFamiliesInclusive: req.CFInclusive,
	}
	var out []shoalql.Cell
	err := b.eng.LookupRows(table, rows, opts, func(_ int, key *iterrt.Key, value []byte) {
		out = append(out, shoalql.Cell{
			Key:   key.Clone(),
			Value: append([]byte(nil), value...),
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Neighbors implements shoalql.Backend.
func (b *Backend) Neighbors(_ context.Context, table string, rows [][]byte, edgeCF []byte) ([][]shoalql.Neighbor, error) {
	raw, err := b.eng.Neighbors(table, rows, edgeCF, engine.ScanOptions{})
	if err != nil {
		return nil, err
	}
	out := make([][]shoalql.Neighbor, len(raw))
	for i, ns := range raw {
		if len(ns) == 0 {
			continue
		}
		conv := make([]shoalql.Neighbor, len(ns))
		for j, n := range ns {
			conv[j] = shoalql.Neighbor{Target: n.Target, Value: n.Value}
		}
		out[i] = conv
	}
	return out, nil
}
