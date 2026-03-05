package auth

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestAllowlistStore_DeleteRenameAndHelpers(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()

	store := NewAllowlistStore(eng)
	if err := store.SaveRoleDatabases(ctx, "analyst", []string{"neo4j", "system"}); err != nil {
		t.Fatalf("save analyst allowlist failed: %v", err)
	}
	if err := store.RenameRoleInAllowlist(ctx, "analyst", "scientist"); err != nil {
		t.Fatalf("rename allowlist role failed: %v", err)
	}
	al := store.Allowlist()
	if _, ok := al["analyst"]; ok {
		t.Fatalf("old role should be removed after rename")
	}
	if !reflect.DeepEqual(al["scientist"], []string{"neo4j", "system"}) {
		t.Fatalf("unexpected copied databases after rename: %#v", al["scientist"])
	}

	if err := store.DeleteRoleDatabases(ctx, "scientist"); err != nil {
		t.Fatalf("delete role allowlist failed: %v", err)
	}
	if err := store.DeleteRoleDatabases(ctx, "scientist"); err != nil {
		t.Fatalf("delete missing role allowlist should be no-op, got %v", err)
	}

	has, err := store.HasAllowlistData(ctx)
	if err != nil {
		t.Fatalf("has allowlist data failed: %v", err)
	}
	if has {
		t.Fatalf("expected no allowlist data after deletion")
	}

	if got := roleFromNodeID("role_db_access:myrole"); got != "myrole" {
		t.Fatalf("unexpected role parse result: %q", got)
	}
	if got := roleFromNodeID("bad"); got != "" {
		t.Fatalf("expected empty role for invalid ID, got %q", got)
	}
	if got := databasesFromProperties(nil); got != nil {
		t.Fatalf("expected nil databases for nil properties, got %#v", got)
	}
	if got := databasesFromProperties(map[string]any{"databases": `[
		"neo4j",
		"system"
	]`}); !reflect.DeepEqual(got, []string{"neo4j", "system"}) {
		t.Fatalf("unexpected parsed databases: %#v", got)
	}
}

func TestAllowlistDatabaseAccessMode_NormalizationPaths(t *testing.T) {
	mode := NewAllowlistDatabaseAccessMode(
		map[string][]string{"viewer": {"neo4j"}, "editor": {}},
		[]string{" role_viewer "},
	)
	if !mode.CanSeeDatabase("neo4j") || mode.CanAccessDatabase("system") {
		t.Fatalf("expected normalized role_viewer access only to neo4j")
	}

	mode2 := NewAllowlistDatabaseAccessMode(
		map[string][]string{"viewer": {"neo4j"}, "editor": {}},
		[]string{"editor"},
	)
	if !mode2.CanAccessDatabase("anything") {
		t.Fatalf("empty role allowlist entry should mean all databases")
	}
}
