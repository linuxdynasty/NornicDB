package fabric

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// mockCypherExecutor is a test double for CypherExecutor.
type mockCypherExecutor struct {
	results map[string]*ResultStream // query -> result
	err     error
}

func (m *mockCypherExecutor) ExecuteQuery(_ context.Context, _ storage.Engine, query string, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	result, ok := m.results[query]
	if !ok {
		return nil, nil, fmt.Errorf("unexpected query: %s", query)
	}
	return result.Columns, result.Rows, nil
}

// mockEngine is a minimal storage.Engine for testing.
type mockEngine struct {
	storage.Engine
}

func newTestLocalExecutor(mock *mockCypherExecutor) *LocalFragmentExecutor {
	engines := map[string]storage.Engine{
		"db1":    &mockEngine{},
		"shard1": &mockEngine{},
		"shard2": &mockEngine{},
	}
	return NewLocalFragmentExecutor(mock, func(name string) (storage.Engine, error) {
		if e, ok := engines[name]; ok {
			return e, nil
		}
		return nil, fmt.Errorf("database '%s' not found", name)
	})
}

func TestFabricExecutor_SimpleExec(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id": {
				Columns: []string{"n.id"},
				Rows:    [][]interface{}{{"id1"}, {"id2"}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	fragment := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "MATCH (n) RETURN n.id",
		GraphName: "db1",
		Columns:   []string{"n.id"},
	}

	result, err := exec.Execute(ctx, nil, fragment, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount())
	}
	if result.Columns[0] != "n.id" {
		t.Errorf("expected column n.id, got %s", result.Columns[0])
	}
}

func TestFabricExecutor_Init(t *testing.T) {
	exec := NewFabricExecutor(NewCatalog(), nil, nil)
	ctx := context.Background()

	init := &FragmentInit{Columns: []string{"x"}}
	result, err := exec.Execute(ctx, nil, init, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 1 {
		t.Errorf("expected 1 row, got %d", result.RowCount())
	}
}

func TestFabricExecutor_UnknownGraph(t *testing.T) {
	catalog := NewCatalog()
	exec := NewFabricExecutor(catalog, nil, nil)
	ctx := context.Background()

	fragment := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "MATCH (n) RETURN n",
		GraphName: "nonexistent",
	}

	_, err := exec.Execute(ctx, nil, fragment, nil, "")
	if err == nil {
		t.Fatal("expected error for unknown graph")
	}
}

func TestFabricExecutor_TransactionWriteConstraint(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("shard1", &LocationLocal{DBName: "shard1"})
	catalog.Register("shard2", &LocationLocal{DBName: "shard2"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"CREATE (n:Test)": {
				Columns: []string{"n"},
				Rows:    [][]interface{}{{"created"}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()
	tx := NewFabricTransaction("tx-test")

	// Write to shard1 — succeeds.
	frag1 := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "CREATE (n:Test)",
		GraphName: "shard1",
		IsWrite:   true,
	}
	_, err := exec.Execute(ctx, tx, frag1, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Write to shard2 — must fail with second write shard error.
	frag2 := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "CREATE (n:Test)",
		GraphName: "shard2",
		IsWrite:   true,
	}
	_, err = exec.Execute(ctx, tx, frag2, nil, "")
	if err != ErrSecondWriteShard {
		t.Errorf("expected ErrSecondWriteShard, got: %v", err)
	}
}

func TestFabricExecutor_Apply(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"a"}, {"b"}},
			},
			"MATCH (m) WHERE m.ref = $id RETURN m.name AS name": {
				Columns: []string{"name"},
				Rows:    [][]interface{}{{"result"}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	outer := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "MATCH (n) RETURN n.id AS id",
		GraphName: "db1",
		Columns:   []string{"id"},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"id"}, ImportColumns: []string{"id"}},
		Query:     "MATCH (m) WHERE m.ref = $id RETURN m.name AS name",
		GraphName: "db1",
		Columns:   []string{"name"},
	}
	apply := &FragmentApply{
		Input:   outer,
		Inner:   inner,
		Columns: []string{"id", "name"},
	}

	result, err := exec.Execute(ctx, nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 2 outer rows x 1 inner row each = 2 combined rows
	if result.RowCount() != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount())
	}
}

func TestFabricExecutor_Union(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"MATCH (n:A) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"a1"}, {"a2"}},
			},
			"MATCH (n:B) RETURN n.id AS id": {
				Columns: []string{"id"},
				Rows:    [][]interface{}{{"b1"}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	union := &FragmentUnion{
		Init: &FragmentInit{},
		LHS: &FragmentExec{
			Input:     &FragmentInit{},
			Query:     "MATCH (n:A) RETURN n.id AS id",
			GraphName: "db1",
			Columns:   []string{"id"},
		},
		RHS: &FragmentExec{
			Input:     &FragmentInit{},
			Query:     "MATCH (n:B) RETURN n.id AS id",
			GraphName: "db1",
			Columns:   []string{"id"},
		},
		Distinct: false,
		Columns:  []string{"id"},
	}

	result, err := exec.Execute(ctx, nil, union, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 3 {
		t.Errorf("expected 3 rows, got %d", result.RowCount())
	}
}

func TestFabricExecutor_UnionDistinct(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"RETURN 1 AS x": {
				Columns: []string{"x"},
				Rows:    [][]interface{}{{int64(1)}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	union := &FragmentUnion{
		Init: &FragmentInit{},
		LHS: &FragmentExec{
			Input: &FragmentInit{}, Query: "RETURN 1 AS x", GraphName: "db1",
		},
		RHS: &FragmentExec{
			Input: &FragmentInit{}, Query: "RETURN 1 AS x", GraphName: "db1",
		},
		Distinct: true,
		Columns:  []string{"x"},
	}

	result, err := exec.Execute(ctx, nil, union, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 1 {
		t.Errorf("expected 1 row after distinct, got %d", result.RowCount())
	}
}

func TestFabricExecutor_LeafFragment(t *testing.T) {
	exec := NewFabricExecutor(NewCatalog(), nil, nil)
	ctx := context.Background()

	leaf := &FragmentLeaf{
		Input:   &FragmentInit{},
		Clauses: "MATCH (n)",
		Columns: []string{"n"},
	}

	_, err := exec.Execute(ctx, nil, leaf, nil, "")
	if err == nil {
		t.Fatal("expected error for unresolved FragmentLeaf")
	}
}

func TestFabricExecutor_NilLocalExecutor(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	exec := NewFabricExecutor(catalog, nil, nil)
	ctx := context.Background()

	frag := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n", GraphName: "db1",
	}

	_, err := exec.Execute(ctx, nil, frag, nil, "")
	if err == nil {
		t.Fatal("expected error when local executor is nil")
	}
}

func TestFabricExecutor_NilRemoteExecutor(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("remote.shard", &LocationRemote{DBName: "shard", URI: "bolt://r:7687"})

	exec := NewFabricExecutor(catalog, nil, nil)
	ctx := context.Background()

	frag := &FragmentExec{
		Input: &FragmentInit{}, Query: "MATCH (n) RETURN n", GraphName: "remote.shard",
	}

	_, err := exec.Execute(ctx, nil, frag, nil, "")
	if err == nil {
		t.Fatal("expected error when remote executor is nil")
	}
}

func TestMergeRowParams(t *testing.T) {
	params := map[string]interface{}{"existing": "val"}
	columns := []string{"col1", "col2"}
	row := []interface{}{"v1", "v2"}

	merged := mergeRowParams(params, columns, row)
	if merged["existing"] != "val" {
		t.Error("expected existing param preserved")
	}
	if merged["col1"] != "v1" {
		t.Errorf("expected col1=v1, got %v", merged["col1"])
	}
	if merged["col2"] != "v2" {
		t.Errorf("expected col2=v2, got %v", merged["col2"])
	}
}

func TestCombineColumns(t *testing.T) {
	outer := []string{"a", "b"}
	inner := []string{"b", "c"}

	combined := combineColumns(outer, inner)
	// Should be [a, b, c] — b is deduplicated.
	if len(combined) != 3 {
		t.Fatalf("expected 3 columns, got %d: %v", len(combined), combined)
	}
	if combined[0] != "a" || combined[1] != "b" || combined[2] != "c" {
		t.Errorf("expected [a b c], got %v", combined)
	}
}

func TestDeduplicateRows(t *testing.T) {
	rows := [][]interface{}{
		{int64(1), "a"},
		{int64(2), "b"},
		{int64(1), "a"},
	}

	deduped := deduplicateRows(rows)
	if len(deduped) != 2 {
		t.Errorf("expected 2 rows after dedup, got %d", len(deduped))
	}
}
