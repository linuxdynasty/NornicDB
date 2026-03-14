package fabric

import (
	"sort"
	"testing"
)

func TestCatalog_RegisterAndResolve(t *testing.T) {
	c := NewCatalog()
	c.Register("mydb", &LocationLocal{DBName: "mydb"})

	loc, err := c.Resolve("mydb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	local, ok := loc.(*LocationLocal)
	if !ok {
		t.Fatalf("expected LocationLocal, got %T", loc)
	}
	if local.DBName != "mydb" {
		t.Errorf("expected mydb, got %s", local.DBName)
	}
}

func TestCatalog_ResolveIsCaseInsensitive(t *testing.T) {
	c := NewCatalog()
	c.Register("MyDB", &LocationLocal{DBName: "mydb"})

	loc, err := c.Resolve("mydb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loc.DatabaseName() != "mydb" {
		t.Errorf("expected mydb, got %s", loc.DatabaseName())
	}

	loc2, err := c.Resolve("MYDB")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loc2.DatabaseName() != "mydb" {
		t.Errorf("expected mydb, got %s", loc2.DatabaseName())
	}
}

func TestCatalog_ResolveNotFound(t *testing.T) {
	c := NewCatalog()
	_, err := c.Resolve("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent graph")
	}
}

func TestCatalog_Unregister(t *testing.T) {
	c := NewCatalog()
	c.Register("mydb", &LocationLocal{DBName: "mydb"})
	c.Unregister("mydb")

	_, err := c.Resolve("mydb")
	if err == nil {
		t.Fatal("expected error after unregister")
	}
}

func TestCatalog_UnregisterCaseInsensitive(t *testing.T) {
	c := NewCatalog()
	c.Register("MyDB", &LocationLocal{DBName: "mydb"})
	c.Unregister("MYDB")

	_, err := c.Resolve("mydb")
	if err == nil {
		t.Fatal("expected error after case-insensitive unregister")
	}
}

func TestCatalog_OverwriteRegistration(t *testing.T) {
	c := NewCatalog()
	c.Register("mydb", &LocationLocal{DBName: "mydb"})
	c.Register("mydb", &LocationRemote{DBName: "remote_mydb", URI: "bolt://remote:7687"})

	loc, err := c.Resolve("mydb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	remote, ok := loc.(*LocationRemote)
	if !ok {
		t.Fatalf("expected LocationRemote after overwrite, got %T", loc)
	}
	if remote.URI != "bolt://remote:7687" {
		t.Errorf("expected bolt://remote:7687, got %s", remote.URI)
	}
}

func TestCatalog_ListGraphs(t *testing.T) {
	c := NewCatalog()
	c.Register("db1", &LocationLocal{DBName: "db1"})
	c.Register("db2", &LocationLocal{DBName: "db2"})
	c.Register("comp.shard_a", &LocationRemote{DBName: "shard_a", URI: "bolt://a:7687"})

	names := c.ListGraphs()
	sort.Strings(names)

	expected := []string{"comp.shard_a", "db1", "db2"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d graphs, got %d: %v", len(expected), len(names), names)
	}
	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected %s at index %d, got %s", expected[i], i, name)
		}
	}
}

func TestCatalog_DottedCompositeConstituent(t *testing.T) {
	c := NewCatalog()
	c.Register("nornic", &LocationLocal{DBName: "nornic"})
	c.Register("nornic.tr", &LocationRemote{
		DBName:   "nornic_tr",
		URI:      "bolt://shard-a:7687",
		AuthMode: "oidc_forwarding",
	})
	c.Register("nornic.txt", &LocationRemote{
		DBName:   "nornic_txt",
		URI:      "bolt://shard-b:7687",
		AuthMode: "user_password",
		User:     "svc",
		Password: "pass",
	})

	// Resolve composite itself.
	loc, err := c.Resolve("nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := loc.(*LocationLocal); !ok {
		t.Errorf("expected LocationLocal for composite, got %T", loc)
	}

	// Resolve constituent with OIDC forwarding.
	loc, err = c.Resolve("nornic.tr")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	remote, ok := loc.(*LocationRemote)
	if !ok {
		t.Fatalf("expected LocationRemote, got %T", loc)
	}
	if remote.DBName != "nornic_tr" {
		t.Errorf("expected nornic_tr, got %s", remote.DBName)
	}
	if remote.AuthMode != "oidc_forwarding" {
		t.Errorf("expected oidc_forwarding, got %s", remote.AuthMode)
	}

	// Resolve constituent with user/password.
	loc, err = c.Resolve("nornic.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	remote2, ok := loc.(*LocationRemote)
	if !ok {
		t.Fatalf("expected LocationRemote, got %T", loc)
	}
	if remote2.AuthMode != "user_password" {
		t.Errorf("expected user_password, got %s", remote2.AuthMode)
	}
	if remote2.User != "svc" {
		t.Errorf("expected svc, got %s", remote2.User)
	}
}
