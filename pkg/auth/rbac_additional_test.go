package auth

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestEntitlementsCatalogAndRolePermissionStrings(t *testing.T) {
	all := AllEntitlements()
	if len(all) != 11 {
		t.Fatalf("expected 11 entitlements, got %d", len(all))
	}

	global := 0
	perDB := 0
	for _, e := range all {
		if e.ID == "" || e.Name == "" {
			t.Fatalf("entitlement should have id/name: %#v", e)
		}
		switch e.Category {
		case EntitlementCategoryGlobal:
			global++
		case EntitlementCategoryPerDatabase:
			perDB++
		default:
			t.Fatalf("unexpected entitlement category %q", e.Category)
		}
	}
	if global != 7 || perDB != 4 {
		t.Fatalf("unexpected entitlement counts: global=%d perDB=%d", global, perDB)
	}

	if got := GlobalEntitlementIDs(); len(got) != 7 {
		t.Fatalf("expected 7 global IDs, got %d", len(got))
	}
	if got := PerDatabaseEntitlementIDs(); len(got) != 4 {
		t.Fatalf("expected 4 per-db IDs, got %d", len(got))
	}

	rolePerms := RolePermissionsAsStrings()
	if len(rolePerms) == 0 {
		t.Fatalf("expected role permissions map")
	}
	if _, ok := rolePerms[string(RoleAdmin)]; !ok {
		t.Fatalf("expected admin role in permissions map")
	}
}

func TestRequestRBACContextRoundTrip(t *testing.T) {
	base := context.Background()
	if got := RequestPrincipalRolesFromContext(base); got != nil {
		t.Fatalf("expected nil roles without context, got %#v", got)
	}
	if got := RequestDatabaseAccessModeFromContext(base); got != nil {
		t.Fatalf("expected nil db access mode without context, got %#v", got)
	}
	if got := RequestResolvedAccessResolverFromContext(base); got != nil {
		t.Fatalf("expected nil resolver without context, got non-nil resolver")
	}

	ctx := WithRequestPrincipalRoles(base, []string{"admin", "editor"})
	ctx = WithRequestDatabaseAccessMode(ctx, FullDatabaseAccessMode)
	ctx = WithRequestResolvedAccessResolver(ctx, func(db string) ResolvedAccess {
		return ResolvedAccess{Read: db != "forbidden", Write: db == "system"}
	})

	roles := RequestPrincipalRolesFromContext(ctx)
	if !reflect.DeepEqual(roles, []string{"admin", "editor"}) {
		t.Fatalf("unexpected roles from context: %#v", roles)
	}
	mode := RequestDatabaseAccessModeFromContext(ctx)
	if !mode.CanAccessDatabase("anything") {
		t.Fatalf("expected full access mode from context")
	}
	resolver := RequestResolvedAccessResolverFromContext(ctx)
	if resolver == nil {
		t.Fatalf("expected resolver in context")
	}
	got := resolver("system")
	if !got.Read || !got.Write {
		t.Fatalf("unexpected resolved access from resolver: %#v", got)
	}
}

func TestRoleEntitlementsStore_SetLoadAndPermissions(t *testing.T) {
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	ctx := context.Background()

	store := NewRoleEntitlementsStore(eng)
	if err := store.Set(ctx, "", []string{"read"}); err != ErrInvalidRoleName {
		t.Fatalf("expected ErrInvalidRoleName, got %v", err)
	}

	if err := store.Set(ctx, "Admin", []string{"read", "write"}); err != nil {
		t.Fatalf("set admin override: %v", err)
	}
	got := store.Get("admin")
	if !reflect.DeepEqual(got, []string{"read", "write"}) {
		t.Fatalf("unexpected stored entitlements: %#v", got)
	}

	// Ensure Get returns copy.
	got[0] = "mutated"
	got2 := store.Get("admin")
	if got2[0] != "read" {
		t.Fatalf("store should return copy, got %#v", got2)
	}

	all := store.All()
	if !reflect.DeepEqual(all["admin"], []string{"read", "write"}) {
		t.Fatalf("unexpected all entitlements map: %#v", all)
	}

	loaded := NewRoleEntitlementsStore(eng)
	if err := loaded.Load(ctx); err != nil {
		t.Fatalf("load from storage failed: %v", err)
	}
	if !reflect.DeepEqual(loaded.Get("admin"), []string{"read", "write"}) {
		t.Fatalf("loaded entitlements mismatch: %#v", loaded.Get("admin"))
	}

	if err := loaded.Set(ctx, "admin", nil); err != nil {
		t.Fatalf("clearing admin override failed: %v", err)
	}
	if ids := loaded.Get("admin"); ids != nil {
		t.Fatalf("expected nil override after delete, got %#v", ids)
	}

	// Fallback behavior without override.
	viewer := PermissionsForRole("viewer", loaded)
	if len(viewer) == 0 {
		t.Fatalf("expected built-in fallback permissions for viewer")
	}
	if custom := PermissionsForRole("custom-role", loaded); custom != nil {
		t.Fatalf("expected nil for unknown custom role, got %#v", custom)
	}

	union := PermissionsForRoles([]string{"viewer", "editor", "viewer"}, loaded)
	if len(union) == 0 {
		t.Fatalf("expected merged permissions for multiple roles")
	}
}

func TestRoleEntitlementParsingHelpers(t *testing.T) {
	if got := roleFromEntitlementNodeID("bad"); got != "" {
		t.Fatalf("expected empty role for invalid ID, got %q", got)
	}
	if got := roleFromEntitlementNodeID("role_entitlement:  AdMin "); got != "admin" {
		t.Fatalf("expected normalized role 'admin', got %q", got)
	}

	if got := entitlementIDsFromProperties(nil); got != nil {
		t.Fatalf("expected nil entitlements for nil props, got %#v", got)
	}
	if got := entitlementIDsFromProperties(map[string]any{"entitlements": `["read","write"]`}); !reflect.DeepEqual(got, []string{"read", "write"}) {
		t.Fatalf("unexpected json string entitlements: %#v", got)
	}
	if got := entitlementIDsFromProperties(map[string]any{"entitlements": []any{" READ ", "", "Write"}}); !reflect.DeepEqual(got, []string{"read", "write"}) {
		t.Fatalf("unexpected []any entitlements normalization: %#v", got)
	}
	if got := entitlementIDsFromProperties(map[string]any{"entitlements": 123}); got != nil {
		t.Fatalf("expected nil for unsupported entitlement type, got %#v", got)
	}
}

func TestPrivilegesStore_SaveLoadResolveAndPutMatrix(t *testing.T) {
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	ctx := context.Background()

	store := NewPrivilegesStore(eng)
	if err := store.SavePrivilege(ctx, " Viewer ", "neo4j", true, false); err != nil {
		t.Fatalf("save privilege failed: %v", err)
	}

	resolved := store.Resolve([]string{"ROLE_viewer"}, "neo4j")
	if !resolved.Read || resolved.Write {
		t.Fatalf("unexpected resolved access from matrix: %#v", resolved)
	}

	// Non-matrix DB for built-in editor should fall back to role permissions.
	fallback := store.Resolve([]string{"editor"}, "otherdb")
	if !fallback.Read || !fallback.Write {
		t.Fatalf("expected editor fallback read+write, got %#v", fallback)
	}

	matrix := store.Matrix()
	if len(matrix) != 1 || matrix[0].Role != "viewer" || matrix[0].Database != "neo4j" {
		t.Fatalf("unexpected matrix contents: %#v", matrix)
	}

	loaded := NewPrivilegesStore(eng)
	if err := loaded.Load(ctx); err != nil {
		t.Fatalf("load privileges failed: %v", err)
	}
	resolvedLoaded := loaded.Resolve([]string{"viewer"}, "neo4j")
	if !resolvedLoaded.Read || resolvedLoaded.Write {
		t.Fatalf("unexpected loaded resolved access: %#v", resolvedLoaded)
	}

	entries := []struct {
		Role     string `json:"role"`
		Database string `json:"database"`
		Read     bool   `json:"read"`
		Write    bool   `json:"write"`
	}{
		{Role: "admin", Database: "system", Read: true, Write: true},
		{Role: "viewer", Database: "analytics", Read: true, Write: false},
	}
	if err := loaded.PutMatrix(ctx, entries); err != nil {
		t.Fatalf("put matrix failed: %v", err)
	}
	if got := loaded.Resolve([]string{"viewer"}, "neo4j"); got.Write || !got.Read {
		t.Fatalf("expected fallback read-only on neo4j after matrix replace, got %#v", got)
	}
	if got := loaded.Resolve([]string{"viewer"}, "analytics"); !got.Read || got.Write {
		t.Fatalf("expected explicit analytics privilege, got %#v", got)
	}
}

func TestPrivilegesParsingHelpers(t *testing.T) {
	if role, db := roleDbFromNodeID("bad"); role != "" || db != "" {
		t.Fatalf("expected empty role/db for invalid id, got role=%q db=%q", role, db)
	}
	if role, db := roleDbFromNodeID("db_priv:viewer:neo4j"); role != "viewer" || db != "neo4j" {
		t.Fatalf("unexpected parse result role=%q db=%q", role, db)
	}

	if got := privFromProperties(map[string]any{"read": true, "write": false}); !got.Read || got.Write {
		t.Fatalf("unexpected bool property conversion: %#v", got)
	}
	if got := privFromProperties(map[string]any{"read": float64(0), "write": float64(1)}); got.Read || !got.Write {
		t.Fatalf("unexpected numeric property conversion: %#v", got)
	}
}
