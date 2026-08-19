//go:build shoal_capi_test

package main

/*
#cgo CFLAGS: -I${SRCDIR}/../../capi/include -I${SRCDIR}/../../capi/tests
#include "bridge.h"
#include "test_seam.h"
*/
import "C"

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/phrocker/shoal/accumulo"
)

type testAdminConnector struct {
	mu            sync.Mutex
	nextID        int
	tables        map[string]string
	properties    map[string]map[string]string
	namespaces    map[string]string
	nsProps       map[string]map[string]string
	users         map[string][]byte
	auths         map[string][][]byte
	permissions   map[string]bool
	splits        map[string][][]byte
	flushFalse    int
	flushTrue     int
	identityBlock atomic.Bool
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
		namespaces: map[string]string{"": "+default", "analytics": "a"},
		nsProps: map[string]map[string]string{
			"analytics": {"table.custom.namespace": "enabled"},
		},
		users:       map[string][]byte{"root": []byte("secret")},
		auths:       map[string][][]byte{"root": {[]byte("A")}},
		permissions: make(map[string]bool),
		splits:      map[string][][]byte{"events": {[]byte("m")}},
	}
}

func (c *testAdminConnector) Close() error {
	return nil
}

func (c *testAdminConnector) Principal() string {
	return "root"
}

func (c *testAdminConnector) capiConnectorIdentity(ctx context.Context) (accumulo.InstanceInfo, string, error) {
	if c.identityBlock.Load() {
		<-ctx.Done()
		return accumulo.InstanceInfo{}, "", ctx.Err()
	}
	return accumulo.InstanceInfo{Name: "test", ID: "test-id"}, c.Principal(), nil
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
	delete(c.splits, name)
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
	if splits, ok := c.splits[oldName]; ok {
		c.splits[newName] = cloneRows(splits)
		delete(c.splits, oldName)
	}
	return nil
}

func (c *testAdminConnector) FlushTable(
	ctx context.Context,
	name string,
	wait bool,
) error {
	if err := c.maybeTableError(ctx, name, accumulo.ErrManagerUnavailable); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tables[name]; !exists {
		return accumulo.ErrTableNotFound
	}
	if wait {
		c.flushTrue++
	} else {
		c.flushFalse++
	}
	return nil
}

func (c *testAdminConnector) flushWaitCount(wait bool) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if wait {
		return c.flushTrue
	}
	return c.flushFalse
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

func (c *testAdminConnector) Namespaces(ctx context.Context) ([]accumulo.Namespace, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.namespaces))
	for name := range c.namespaces {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]accumulo.Namespace, 0, len(names))
	for _, name := range names {
		result = append(result, accumulo.Namespace{Name: name, ID: c.namespaces[name]})
	}
	return result, nil
}

func (c *testAdminConnector) NamespaceExists(ctx context.Context, name string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.namespaces[name]
	return ok, nil
}

func (c *testAdminConnector) CreateNamespace(ctx context.Context, name string) error {
	if name == "block" {
		<-ctx.Done()
		return ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.namespaces[name]; ok {
		return accumulo.ErrNamespaceExists
	}
	c.namespaces[name] = "n" + strconv.Itoa(len(c.namespaces))
	return nil
}

func (c *testAdminConnector) DeleteNamespace(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.namespaces[name]; !ok {
		return accumulo.ErrNamespaceNotFound
	}
	for table := range c.tables {
		namespace := ""
		if separator := strings.IndexByte(table, '.'); separator >= 0 {
			namespace = table[:separator]
		}
		if namespace == name {
			return accumulo.ErrNamespaceNotEmpty
		}
	}
	delete(c.namespaces, name)
	delete(c.nsProps, name)
	return nil
}

func (c *testAdminConnector) RenameNamespace(ctx context.Context, oldName, newName string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.namespaces[oldName]
	if !ok {
		return accumulo.ErrNamespaceNotFound
	}
	if _, ok := c.namespaces[newName]; ok {
		return accumulo.ErrNamespaceExists
	}
	delete(c.namespaces, oldName)
	c.namespaces[newName] = id
	c.nsProps[newName] = cloneProperties(c.nsProps[oldName])
	delete(c.nsProps, oldName)
	return nil
}

func (c *testAdminConnector) SetNamespaceProperty(ctx context.Context, namespace, property, value string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.namespaces[namespace]; !ok {
		return accumulo.ErrNamespaceNotFound
	}
	if c.nsProps[namespace] == nil {
		c.nsProps[namespace] = make(map[string]string)
	}
	c.nsProps[namespace][property] = value
	return nil
}

func (c *testAdminConnector) RemoveNamespaceProperty(ctx context.Context, namespace, property string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.namespaces[namespace]; !ok {
		return accumulo.ErrNamespaceNotFound
	}
	delete(c.nsProps[namespace], property)
	return nil
}

func (c *testAdminConnector) EffectiveNamespaceProperties(ctx context.Context, namespace string) (map[string]string, error) {
	return c.NamespaceProperties(ctx, namespace)
}

func (c *testAdminConnector) NamespaceProperties(ctx context.Context, namespace string) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.namespaces[namespace]; !ok {
		return nil, accumulo.ErrNamespaceNotFound
	}
	return cloneProperties(c.nsProps[namespace]), nil
}

func (c *testAdminConnector) VersionedNamespaceProperties(ctx context.Context, namespace string) (accumulo.VersionedProperties, error) {
	properties, err := c.NamespaceProperties(ctx, namespace)
	return accumulo.VersionedProperties{Version: 7, Properties: properties}, err
}

func (c *testAdminConnector) CreateUser(ctx context.Context, user string, password []byte) error {
	if user == "block" {
		<-ctx.Done()
		return ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.users[user]; ok {
		return accumulo.ErrUserExists
	}
	c.users[user] = append([]byte(nil), password...)
	return nil
}

func (c *testAdminConnector) DropUser(ctx context.Context, user string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.users[user]; !ok {
		return accumulo.ErrUserNotFound
	}
	delete(c.users, user)
	delete(c.auths, user)
	for key := range c.permissions {
		parts := strings.Split(key, "\x00")
		if len(parts) == 4 && parts[1] == user {
			delete(c.permissions, key)
		}
	}
	return nil
}

func (c *testAdminConnector) ChangePassword(ctx context.Context, user string, password []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.users[user]; !ok {
		return &accumulo.SecurityError{User: user, Code: "USER_DOESNT_EXIST", Err: accumulo.ErrUserNotFound}
	}
	c.users[user] = append([]byte(nil), password...)
	return nil
}

func (c *testAdminConnector) ChangeUserAuthorizations(ctx context.Context, user string, auths [][]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.users[user]; !ok {
		return accumulo.ErrUserNotFound
	}
	c.auths[user] = cloneRows(auths)
	return nil
}

func (c *testAdminConnector) GetUserAuthorizations(ctx context.Context, user string) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.users[user]; !ok {
		return nil, accumulo.ErrUserNotFound
	}
	return cloneRows(c.auths[user]), nil
}

func permissionKey(kind, user, target string, permission int8) string {
	return kind + "\x00" + user + "\x00" + target + "\x00" + strconv.Itoa(int(permission))
}

func (c *testAdminConnector) permission(ctx context.Context, key string, set *bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if set != nil {
		c.permissions[key] = *set
	}
	return c.permissions[key], nil
}

func (c *testAdminConnector) HasSystemPermission(ctx context.Context, user string, permission accumulo.SystemPermission) (bool, error) {
	return c.permission(ctx, permissionKey("system", user, "", int8(permission)), nil)
}
func (c *testAdminConnector) HasTablePermission(ctx context.Context, user, table string, permission accumulo.TablePermission) (bool, error) {
	return c.permission(ctx, permissionKey("table", user, table, int8(permission)), nil)
}
func (c *testAdminConnector) HasNamespacePermission(ctx context.Context, user, namespace string, permission accumulo.NamespacePermission) (bool, error) {
	return c.permission(ctx, permissionKey("namespace", user, namespace, int8(permission)), nil)
}
func (c *testAdminConnector) GrantSystemPermission(ctx context.Context, user string, permission accumulo.SystemPermission) error {
	value := true
	_, err := c.permission(ctx, permissionKey("system", user, "", int8(permission)), &value)
	return err
}
func (c *testAdminConnector) RevokeSystemPermission(ctx context.Context, user string, permission accumulo.SystemPermission) error {
	value := false
	_, err := c.permission(ctx, permissionKey("system", user, "", int8(permission)), &value)
	return err
}
func (c *testAdminConnector) GrantTablePermission(ctx context.Context, user, table string, permission accumulo.TablePermission) error {
	value := true
	_, err := c.permission(ctx, permissionKey("table", user, table, int8(permission)), &value)
	return err
}
func (c *testAdminConnector) RevokeTablePermission(ctx context.Context, user, table string, permission accumulo.TablePermission) error {
	value := false
	_, err := c.permission(ctx, permissionKey("table", user, table, int8(permission)), &value)
	return err
}
func (c *testAdminConnector) GrantNamespacePermission(ctx context.Context, user, namespace string, permission accumulo.NamespacePermission) error {
	value := true
	_, err := c.permission(ctx, permissionKey("namespace", user, namespace, int8(permission)), &value)
	return err
}
func (c *testAdminConnector) RevokeNamespacePermission(ctx context.Context, user, namespace string, permission accumulo.NamespacePermission) error {
	value := false
	_, err := c.permission(ctx, permissionKey("namespace", user, namespace, int8(permission)), &value)
	return err
}

func (c *testAdminConnector) ListTableSplits(ctx context.Context, table string) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[table]; !ok {
		return nil, accumulo.ErrTableNotFound
	}
	return cloneRows(c.splits[table]), nil
}

func (c *testAdminConnector) AddTableSplits(ctx context.Context, table string, splits [][]byte) error {
	if table == "block" {
		<-ctx.Done()
		return ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[table]; !ok {
		return accumulo.ErrTableNotFound
	}
	merged := append(cloneRows(c.splits[table]), cloneRows(splits)...)
	sort.Slice(merged, func(i, j int) bool { return bytes.Compare(merged[i], merged[j]) < 0 })
	deduped := merged[:0]
	for _, split := range merged {
		if len(deduped) == 0 || !bytes.Equal(deduped[len(deduped)-1], split) {
			deduped = append(deduped, split)
		}
	}
	c.splits[table] = deduped
	return nil
}

func cloneRows(rows [][]byte) [][]byte {
	result := make([][]byte, len(rows))
	for i := range rows {
		result[i] = append([]byte(nil), rows[i]...)
	}
	return result
}

func cloneProperties(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

type testAdminInstance struct{ accumulo.NoTopology }

func (*testAdminInstance) Info() accumulo.InstanceInfo {
	return accumulo.InstanceInfo{Name: "test", ID: "test-id"}
}

func (*testAdminInstance) Close() error {
	return nil
}

//export shoal_test_connector_create
func shoal_test_connector_create(outConnector **C.shoal_connector) C.int {
	if outConnector == nil {
		return 0
	}
	*outConnector = nil
	owned := newOwnedConnector(newTestAdminConnector(), &testAdminInstance{})
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

//export shoal_test_connector_flush_wait_count
func shoal_test_connector_flush_wait_count(
	handle *C.shoal_connector,
	wait C.uint8_t,
) C.size_t {
	connector, err := lookupConnector(handle)
	if err != nil {
		return 0
	}
	admin, ok := connector.connector.(*testAdminConnector)
	if !ok {
		return 0
	}
	waitForCompletion, err := boolFlag(wait, "wait")
	if err != nil {
		return 0
	}
	return C.size_t(admin.flushWaitCount(waitForCompletion))
}

//export shoal_test_connector_identity_block
func shoal_test_connector_identity_block(
	handle *C.shoal_connector,
	block C.uint8_t,
) C.int {
	connector, err := lookupConnector(handle)
	if err != nil {
		return 0
	}
	admin, ok := connector.connector.(*testAdminConnector)
	if !ok {
		return 0
	}
	value, err := boolFlag(block, "block")
	if err != nil {
		return 0
	}
	admin.identityBlock.Store(value)
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

//export shoal_test_result_alloc_fail_after
func shoal_test_result_alloc_fail_after(successfulAllocations C.size_t) {
	C.shoal_bridge_test_result_alloc_fail_after(successfulAllocations)
}

//export shoal_test_result_alloc_reset
func shoal_test_result_alloc_reset() {
	C.shoal_bridge_test_result_alloc_reset()
}

//export shoal_test_error_alloc_fail_after
func shoal_test_error_alloc_fail_after(successfulAllocations C.size_t) {
	C.shoal_bridge_test_error_alloc_fail_after(successfulAllocations)
}

//export shoal_test_error_alloc_reset
func shoal_test_error_alloc_reset() {
	C.shoal_bridge_test_error_alloc_reset()
}

//export shoal_test_error_message_alloc_fail_after
func shoal_test_error_message_alloc_fail_after(successfulAllocations C.size_t) {
	C.shoal_bridge_test_error_message_alloc_fail_after(successfulAllocations)
}

//export shoal_test_error_message_alloc_reset
func shoal_test_error_message_alloc_reset() {
	C.shoal_bridge_test_error_message_alloc_reset()
}
