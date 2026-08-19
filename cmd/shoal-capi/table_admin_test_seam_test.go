//go:build shoal_capi_test

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/phrocker/shoal/accumulo"
)

func TestTestAdminConnectorDeleteNamespaceRejectsNamespacesWithTables(t *testing.T) {
	t.Parallel()
	connector := newTestAdminConnector()

	for _, namespace := range []string{"", "analytics"} {
		if err := connector.DeleteNamespace(context.Background(), namespace); !errors.Is(err, accumulo.ErrNamespaceNotEmpty) {
			t.Fatalf("DeleteNamespace(%q) error = %v, want ErrNamespaceNotEmpty", namespace, err)
		}
	}

	if err := connector.CreateNamespace(context.Background(), "scratch"); err != nil {
		t.Fatalf("CreateNamespace(scratch) error = %v", err)
	}
	if err := connector.DeleteNamespace(context.Background(), "scratch"); err != nil {
		t.Fatalf("DeleteNamespace(scratch) error = %v", err)
	}
}

func TestTestAdminConnectorRenameAndDeleteTableKeepSplitsInSync(t *testing.T) {
	t.Parallel()
	connector := newTestAdminConnector()
	ctx := context.Background()

	if err := connector.CreateTable(ctx, "scratch"); err != nil {
		t.Fatalf("CreateTable(scratch) error = %v", err)
	}
	if err := connector.AddTableSplits(ctx, "scratch", [][]byte{[]byte("b"), []byte("d")}); err != nil {
		t.Fatalf("AddTableSplits(scratch) error = %v", err)
	}
	if err := connector.RenameTable(ctx, "scratch", "events"); !errors.Is(err, accumulo.ErrTableExists) {
		t.Fatalf("RenameTable(scratch, events) error = %v, want ErrTableExists", err)
	}
	if splits, err := connector.ListTableSplits(ctx, "scratch"); err != nil || len(splits) != 2 {
		t.Fatalf("ListTableSplits(scratch) after failed rename = %q, %v; want 2 splits", splits, err)
	}
	if err := connector.RenameTable(ctx, "scratch", "scratch_renamed"); err != nil {
		t.Fatalf("RenameTable(scratch, scratch_renamed) error = %v", err)
	}
	if _, err := connector.ListTableSplits(ctx, "scratch"); !errors.Is(err, accumulo.ErrTableNotFound) {
		t.Fatalf("ListTableSplits(scratch) after rename error = %v, want ErrTableNotFound", err)
	}
	if splits, err := connector.ListTableSplits(ctx, "scratch_renamed"); err != nil || len(splits) != 2 {
		t.Fatalf("ListTableSplits(scratch_renamed) = %q, %v; want 2 splits", splits, err)
	}
	if err := connector.DeleteTable(ctx, "scratch_renamed"); err != nil {
		t.Fatalf("DeleteTable(scratch_renamed) error = %v", err)
	}
	if err := connector.CreateTable(ctx, "scratch_renamed"); err != nil {
		t.Fatalf("CreateTable(scratch_renamed) error = %v", err)
	}
	splits, err := connector.ListTableSplits(ctx, "scratch_renamed")
	if err != nil {
		t.Fatalf("ListTableSplits(scratch_renamed) after recreate error = %v", err)
	}
	if len(splits) != 0 {
		t.Fatalf("ListTableSplits(scratch_renamed) after recreate = %q, want empty", splits)
	}
}

func TestTestAdminConnectorDropUserClearsPermissionsAndAuthorizations(t *testing.T) {
	t.Parallel()
	connector := newTestAdminConnector()
	ctx := context.Background()

	if err := connector.CreateUser(ctx, "alice", []byte("pw")); err != nil {
		t.Fatalf("CreateUser(alice) error = %v", err)
	}
	if err := connector.ChangeUserAuthorizations(ctx, "alice", [][]byte{[]byte("A")}); err != nil {
		t.Fatalf("ChangeUserAuthorizations(alice) error = %v", err)
	}
	if err := connector.GrantSystemPermission(ctx, "alice", accumulo.SystemPermissionCreateTable); err != nil {
		t.Fatalf("GrantSystemPermission(alice) error = %v", err)
	}
	if err := connector.GrantTablePermission(ctx, "alice", "events", accumulo.TablePermissionRead); err != nil {
		t.Fatalf("GrantTablePermission(alice) error = %v", err)
	}
	if err := connector.GrantNamespacePermission(ctx, "alice", "analytics", accumulo.NamespacePermissionRead); err != nil {
		t.Fatalf("GrantNamespacePermission(alice) error = %v", err)
	}
	if err := connector.DropUser(ctx, "alice"); err != nil {
		t.Fatalf("DropUser(alice) error = %v", err)
	}
	if err := connector.CreateUser(ctx, "alice", []byte("pw2")); err != nil {
		t.Fatalf("CreateUser(alice) after drop error = %v", err)
	}
	auths, err := connector.GetUserAuthorizations(ctx, "alice")
	if err != nil {
		t.Fatalf("GetUserAuthorizations(alice) after recreate error = %v", err)
	}
	if len(auths) != 0 {
		t.Fatalf("GetUserAuthorizations(alice) after recreate = %q, want empty", auths)
	}
	if has, err := connector.HasSystemPermission(ctx, "alice", accumulo.SystemPermissionCreateTable); err != nil || has {
		t.Fatalf("HasSystemPermission(alice) after recreate = %v, %v; want false, nil", has, err)
	}
	if has, err := connector.HasTablePermission(ctx, "alice", "events", accumulo.TablePermissionRead); err != nil || has {
		t.Fatalf("HasTablePermission(alice) after recreate = %v, %v; want false, nil", has, err)
	}
	if has, err := connector.HasNamespacePermission(ctx, "alice", "analytics", accumulo.NamespacePermissionRead); err != nil || has {
		t.Fatalf("HasNamespacePermission(alice) after recreate = %v, %v; want false, nil", has, err)
	}
}
