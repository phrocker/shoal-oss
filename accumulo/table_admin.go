package accumulo

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/phrocker/shoal/internal/tablenames"
)

// Tables lists every table visible through the Accumulo 4 ZooKeeper table
// mappings, sorted by qualified name.
func (c *Connector) Tables(ctx context.Context) ([]Table, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	discovery, err := c.discoveryState()
	if err != nil {
		return nil, err
	}
	mapping, err := discovery.names.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("accumulo: list tables: %w", err)
	}
	tables := make([]Table, 0, len(mapping))
	for name, id := range mapping {
		tables = append(tables, Table{Name: name, ID: id})
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})
	return tables, nil
}

// TableExists reports whether a qualified or default-namespace table name is
// present in the Accumulo 4 ZooKeeper table mappings.
func (c *Connector) TableExists(ctx context.Context, name string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if name == "" {
		return false, fmt.Errorf("%w: empty table name", ErrTableNotFound)
	}
	discovery, err := c.discoveryState()
	if err != nil {
		return false, err
	}
	_, err = discovery.names.ResolveID(ctx, name)
	if errors.Is(err, tablenames.ErrTableNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("accumulo: check table %q: %w", name, err)
	}
	return true, nil
}
