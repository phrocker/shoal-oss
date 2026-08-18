//go:build shoal_capi_test

package main

/*
#cgo CFLAGS: -I${SRCDIR}/../../capi/include -I${SRCDIR}/../../capi/tests
#include "bridge.h"
#include "test_seam.h"
*/
import "C"

import (
	"context"
	"sort"
	"strconv"
	"sync"

	"github.com/phrocker/shoal/accumulo"
)

type testAdminConnector struct {
	mu         sync.Mutex
	nextID     int
	tables     map[string]string
	properties map[string]map[string]string
}

func newTestAdminConnector() *testAdminConnector {
	return &testAdminConnector{
		nextID: 3,
		tables: map[string]string{
			"analytics.orders": "2",
			"events":           "1",
		},
		properties: map[string]map[string]string{
			"events": {
				"table.custom.mode":  "stream",
				"table.custom.empty": "",
			},
		},
	}
}

func (c *testAdminConnector) Close() error {
	return nil
}

func (c *testAdminConnector) NewScanner(
	accumulo.Table,
	accumulo.ScannerOptions,
) (*accumulo.Scanner, error) {
	return nil, accumulo.ErrDiscoveryUnavailable
}

func (c *testAdminConnector) NewBatchScanner(
	accumulo.Table,
	accumulo.ScannerOptions,
) (*accumulo.BatchScanner, error) {
	return nil, accumulo.ErrDiscoveryUnavailable
}

func (c *testAdminConnector) NewBatchWriter(
	accumulo.Table,
	accumulo.BatchWriterOptions,
) (*accumulo.BatchWriter, error) {
	return nil, accumulo.ErrDiscoveryUnavailable
}

func (c *testAdminConnector) Tables(ctx context.Context) ([]accumulo.Table, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	tables := make([]accumulo.Table, 0, len(names))
	for _, name := range names {
		tables = append(tables, accumulo.Table{Name: name, ID: c.tables[name]})
	}
	return tables, nil
}

func (c *testAdminConnector) TableExists(ctx context.Context, name string) (bool, error) {
	if err := c.maybeTableError(ctx, name, nil); err != nil {
		return false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.tables[name]
	return ok, nil
}

func (c *testAdminConnector) CreateTable(ctx context.Context, name string) error {
	if err := c.maybeTableError(ctx, name, accumulo.ErrManagerUnavailable); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tables[name]; exists {
		return accumulo.ErrTableExists
	}
	c.tables[name] = strconv.Itoa(c.nextID)
	c.nextID++
	return nil
}

func (c *testAdminConnector) DeleteTable(ctx context.Context, name string) error {
	if err := c.maybeTableError(ctx, name, accumulo.ErrManagerUnavailable); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tables[name]; !exists {
		return accumulo.ErrTableNotFound
	}
	delete(c.tables, name)
	delete(c.properties, name)
	return nil
}

func (c *testAdminConnector) RenameTable(ctx context.Context, oldName, newName string) error {
	if err := c.maybeTableError(ctx, oldName, accumulo.ErrManagerUnavailable); err != nil {
		return err
	}
	if err := c.maybeTableError(ctx, newName, accumulo.ErrManagerUnavailable); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id, exists := c.tables[oldName]
	if !exists {
		return accumulo.ErrTableNotFound
	}
	if _, exists := c.tables[newName]; exists {
		return accumulo.ErrTableExists
	}
	c.tables[newName] = id
	delete(c.tables, oldName)
	if props, ok := c.properties[oldName]; ok {
		c.properties[newName] = cloneProperties(props)
		delete(c.properties, oldName)
	}
	return nil
}

func (c *testAdminConnector) FlushTable(
	ctx context.Context,
	name string,
	_ bool,
) error {
	if err := c.maybeTableError(ctx, name, accumulo.ErrManagerUnavailable); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tables[name]; !exists {
		return accumulo.ErrTableNotFound
	}
	return nil
}

func (c *testAdminConnector) SetTableProperty(
	ctx context.Context,
	tableName, property, value string,
) error {
	if property == "invalid" {
		return accumulo.ErrInvalidProperty
	}
	if err := c.maybeTableError(ctx, tableName, accumulo.ErrManagerUnavailable); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tables[tableName]; !exists {
		return accumulo.ErrTableNotFound
	}
	if c.properties[tableName] == nil {
		c.properties[tableName] = make(map[string]string)
	}
	c.properties[tableName][property] = value
	return nil
}

func (c *testAdminConnector) RemoveTableProperty(
	ctx context.Context,
	tableName, property string,
) error {
	if property == "invalid" {
		return accumulo.ErrInvalidProperty
	}
	if err := c.maybeTableError(ctx, tableName, accumulo.ErrManagerUnavailable); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tables[tableName]; !exists {
		return accumulo.ErrTableNotFound
	}
	if c.properties[tableName] != nil {
		delete(c.properties[tableName], property)
	}
	return nil
}

func (c *testAdminConnector) EffectiveTableProperties(
	ctx context.Context,
	tableName string,
) (map[string]string, error) {
	if err := c.maybeTableError(ctx, tableName, accumulo.ErrClientServiceUnavailable); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tables[tableName]; !exists {
		return nil, accumulo.ErrTableNotFound
	}
	return cloneProperties(c.properties[tableName]), nil
}

func (c *testAdminConnector) maybeTableError(
	ctx context.Context,
	tableName string,
	unavailable error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch tableName {
	case "block":
		<-ctx.Done()
		return ctx.Err()
	case "denied":
		return accumulo.ErrPermissionDenied
	case "down":
		if unavailable != nil {
			return unavailable
		}
	}
	return ctx.Err()
}

func cloneProperties(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

type testAdminInstance struct{}

func (testAdminInstance) Info() accumulo.InstanceInfo {
	return accumulo.InstanceInfo{Name: "test", ID: "test-id"}
}

func (testAdminInstance) Close() error {
	return nil
}

//export shoal_test_connector_create
func shoal_test_connector_create(outConnector **C.shoal_connector) C.int {
	if outConnector == nil {
		return 0
	}
	*outConnector = nil
	owned := newOwnedConnector(newTestAdminConnector(), testAdminInstance{})
	id, ok := connectors.add(owned)
	if !ok {
		return 0
	}
	handle := C.shoal_bridge_connector_alloc(C.uint64_t(id))
	if handle == nil {
		connectors.remove(id)
		return 0
	}
	*outConnector = handle
	return 1
}

//export shoal_test_string_alloc_fail_after
func shoal_test_string_alloc_fail_after(successfulAllocations C.size_t) {
	C.shoal_bridge_test_string_alloc_fail_after(successfulAllocations)
}

//export shoal_test_string_alloc_reset
func shoal_test_string_alloc_reset() {
	C.shoal_bridge_test_string_alloc_reset()
}
