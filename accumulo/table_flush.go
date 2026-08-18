package accumulo

import (
	"context"
	"errors"
	"fmt"

	"github.com/phrocker/shoal/internal/managerclient"
	"github.com/phrocker/shoal/internal/tablenames"
	"github.com/phrocker/shoal/internal/zk"
)

// FlushTable requests a full-table flush. When wait is true, the call waits
// until Accumulo reports the flush complete or ctx is canceled.
func (c *Connector) FlushTable(ctx context.Context, tableName string, wait bool) error {
	if tableName == "" {
		return fmt.Errorf("%w: empty table name", ErrInvalidTableName)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrConnectorClosed
	}
	discovery := c.discovery
	resolver := c.managerAddr
	manager := c.manager
	c.mu.RUnlock()
	if discovery == nil || resolver == nil {
		return ErrDiscoveryUnavailable
	}

	tableID, err := discovery.tables.ResolveID(ctx, tableName)
	if errors.Is(err, tablenames.ErrTableNotFound) {
		return fmt.Errorf("%w: table name %q", ErrTableNotFound, tableName)
	}
	if err != nil {
		return fmt.Errorf("accumulo: resolve table name %q: %w", tableName, err)
	}

	address, err := resolver.Address(ctx)
	if errors.Is(err, zk.ErrManagerUnavailable) {
		return ErrManagerUnavailable
	}
	if err != nil {
		return fmt.Errorf("accumulo: discover manager: %w", err)
	}
	if err := manager.FlushTable(ctx, address, tableID, wait); err != nil {
		var managerErr *managerclient.Error
		if errors.As(err, &managerErr) {
			switch managerErr.Kind {
			case managerclient.ErrorNamespaceNotFound:
				discovery.invalidateNames()
			case managerclient.ErrorTableNotFound:
				discovery.tables.Invalidate()
			}
		}
		return mapManagerFlushError(tableName, err)
	}
	return nil
}

func mapManagerFlushError(tableName string, err error) error {
	var managerErr *managerclient.Error
	if !errors.As(err, &managerErr) {
		return fmt.Errorf("accumulo: flush table %q: %w", tableName, err)
	}
	switch managerErr.Kind {
	case managerclient.ErrorTableNotFound:
		return fmt.Errorf("%w: %q", ErrTableNotFound, tableName)
	case managerclient.ErrorNamespaceNotFound:
		return fmt.Errorf("%w: %q", ErrNamespaceNotFound, tableName)
	case managerclient.ErrorSecurity:
		return fmt.Errorf("%w: table %q", ErrPermissionDenied, tableName)
	case managerclient.ErrorNotActive:
		return ErrManagerUnavailable
	default:
		return fmt.Errorf("accumulo: flush table %q: %w", tableName, managerErr)
	}
}
