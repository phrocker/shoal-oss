package accumulo

import (
	"context"
	"errors"
	"fmt"

	"github.com/phrocker/shoal/internal/cache"
	"github.com/phrocker/shoal/internal/metadata"
	"github.com/phrocker/shoal/internal/tablenames"
)

// Namespace is an Accumulo namespace identity.
type Namespace struct {
	Name string
	ID   string
}

// VersionedProperties is an Accumulo property snapshot and its persistent
// property-store version.
type VersionedProperties struct {
	Version    int64
	Properties map[string]string
}

// Table is an Accumulo table identity.
type Table struct {
	Name string
	ID   string
}

// TabletExtent identifies an Accumulo tablet's (PrevRow, EndRow] range.
// Nil PrevRow means negative infinity; nil EndRow means positive infinity.
type TabletExtent struct {
	TableID string
	PrevRow []byte
	EndRow  []byte
}

// TabletServer is a tablet's current server assignment.
type TabletServer struct {
	HostPort string
	Session  string
}

// Tablet is the public, Go-native discovery view of an Accumulo tablet.
// Server is nil while the tablet has no current assignment.
type Tablet struct {
	Extent TabletExtent
	Server *TabletServer
}

type namespaceResolver interface {
	ResolveID(context.Context, string) (string, error)
	ResolveName(context.Context, string) (string, error)
	List(context.Context) (map[string]string, error)
	Invalidate()
}

type tableNameResolver interface {
	ResolveID(context.Context, string) (string, error)
	ResolveName(context.Context, string) (string, error)
	List(context.Context) (map[string]string, error)
	ListNamespace(context.Context, string) (map[string]string, error)
	Invalidate()
}

type connectorDiscovery struct {
	tablets    *cache.LocatorCache
	namespaces namespaceResolver
	tables     tableNameResolver
}

func newConnectorDiscovery(
	tablets cache.TableLocator,
	namespaces namespaceResolver,
	tables tableNameResolver,
) *connectorDiscovery {
	return &connectorDiscovery{
		tablets:    cache.New(tablets),
		namespaces: namespaces,
		tables:     tables,
	}
}

func (d *connectorDiscovery) close() {
	d.invalidateAll()
}

func (d *connectorDiscovery) invalidateNames() {
	d.namespaces.Invalidate()
	d.tables.Invalidate()
}

func (d *connectorDiscovery) invalidateAll() {
	d.namespaces.Invalidate()
	d.tablets.InvalidateAll()
	d.tables.Invalidate()
}

// TableByName resolves a qualified or default-namespace table name.
func (c *Connector) TableByName(ctx context.Context, name string) (Table, error) {
	if err := ctx.Err(); err != nil {
		return Table{}, err
	}
	if name == "" {
		return Table{}, fmt.Errorf("%w: empty table name", ErrTableNotFound)
	}
	discovery, err := c.discoveryState()
	if err != nil {
		return Table{}, err
	}
	id, err := discovery.tables.ResolveID(ctx, name)
	if err != nil {
		if errors.Is(err, tablenames.ErrTableNotFound) {
			return Table{}, fmt.Errorf("%w: table name %q", ErrTableNotFound, name)
		}
		return Table{}, fmt.Errorf("accumulo: resolve table name %q: %w", name, err)
	}
	return Table{Name: name, ID: id}, nil
}

// TableByID validates a table ID and resolves its qualified name.
func (c *Connector) TableByID(ctx context.Context, id string) (Table, error) {
	if err := ctx.Err(); err != nil {
		return Table{}, err
	}
	if id == "" {
		return Table{}, fmt.Errorf("%w: empty table ID", ErrTableNotFound)
	}
	discovery, err := c.discoveryState()
	if err != nil {
		return Table{}, err
	}
	if _, err := discovery.tablets.LocateTable(ctx, id); err != nil {
		return Table{}, mapTabletDiscoveryError(id, nil, err)
	}
	name, err := discovery.tables.ResolveName(ctx, id)
	if err != nil {
		if errors.Is(err, tablenames.ErrTableNotFound) {
			return Table{}, fmt.Errorf("%w: table ID %q", ErrTableNotFound, id)
		}
		return Table{}, fmt.Errorf("accumulo: resolve table ID %q: %w", id, err)
	}
	return Table{Name: name, ID: id}, nil
}

// Tablets lists the current tablet extents and assignments for table.
func (c *Connector) Tablets(ctx context.Context, table Table) ([]Tablet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if table.ID == "" {
		return nil, fmt.Errorf("%w: empty table ID", ErrTableNotFound)
	}
	discovery, err := c.discoveryState()
	if err != nil {
		return nil, err
	}
	tablets, err := discovery.tablets.LocateTable(ctx, table.ID)
	if err != nil {
		return nil, mapTabletDiscoveryError(table.ID, nil, err)
	}
	out := make([]Tablet, len(tablets))
	for i := range tablets {
		out[i] = publicTablet(tablets[i])
	}
	return out, nil
}

// LocateTablet returns the tablet whose (PrevRow, EndRow] range contains row.
func (c *Connector) LocateTablet(ctx context.Context, table Table, row []byte) (Tablet, error) {
	if err := ctx.Err(); err != nil {
		return Tablet{}, err
	}
	if table.ID == "" {
		return Tablet{}, fmt.Errorf("%w: empty table ID", ErrTableNotFound)
	}
	discovery, err := c.discoveryState()
	if err != nil {
		return Tablet{}, err
	}
	tablet, err := discovery.tablets.Locate(ctx, table.ID, row)
	if err != nil {
		return Tablet{}, mapTabletDiscoveryError(table.ID, row, err)
	}
	if tablet.Location == nil || tablet.Location.HostPort == "" {
		return Tablet{}, fmt.Errorf("%w: table=%s row=%q", ErrTabletNotLocated, table.ID, row)
	}
	return publicTablet(tablet), nil
}

// InvalidateTablet invalidates the cached assignment covering row.
func (c *Connector) InvalidateTablet(table Table, row []byte) error {
	if table.ID == "" {
		return fmt.Errorf("%w: empty table ID", ErrTableNotFound)
	}
	discovery, err := c.discoveryState()
	if err != nil {
		return err
	}
	discovery.tablets.Invalidate(table.ID, row)
	return nil
}

// InvalidateTable invalidates every cached tablet for table.
func (c *Connector) InvalidateTable(table Table) error {
	if table.ID == "" {
		return fmt.Errorf("%w: empty table ID", ErrTableNotFound)
	}
	discovery, err := c.discoveryState()
	if err != nil {
		return err
	}
	discovery.tablets.InvalidateTable(table.ID)
	return nil
}

// InvalidateDiscovery clears all tablet, table-name, and namespace discovery caches.
func (c *Connector) InvalidateDiscovery() error {
	discovery, err := c.discoveryState()
	if err != nil {
		return err
	}
	discovery.invalidateAll()
	return nil
}

func (c *Connector) discoveryState() (*connectorDiscovery, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrConnectorClosed
	}
	if c.discovery == nil {
		return nil, ErrDiscoveryUnavailable
	}
	return c.discovery, nil
}

func publicTablet(tablet metadata.TabletInfo) Tablet {
	out := Tablet{
		Extent: TabletExtent{
			TableID: tablet.TableID,
			PrevRow: cloneRow(tablet.PrevRow),
			EndRow:  cloneRow(tablet.EndRow),
		},
	}
	if tablet.Location != nil {
		out.Server = &TabletServer{
			HostPort: tablet.Location.HostPort,
			Session:  tablet.Location.Session,
		}
	}
	return out
}

func cloneRow(row []byte) []byte {
	if row == nil {
		return nil
	}
	return append([]byte{}, row...)
}

func mapTabletDiscoveryError(tableID string, row []byte, err error) error {
	switch {
	case errors.Is(err, cache.ErrTableNotFound):
		return fmt.Errorf("%w: table=%s: %w", ErrTableNotFound, tableID, err)
	case errors.Is(err, cache.ErrNoTabletCovers):
		return fmt.Errorf("%w: table=%s row=%q: %w", ErrNoTabletCoversRow, tableID, row, err)
	default:
		return fmt.Errorf("accumulo: discover tablets for table %s: %w", tableID, err)
	}
}
