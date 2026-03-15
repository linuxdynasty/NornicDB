package fabric

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// mockCypherExecutor is a test double for CypherExecutor.
type mockCypherExecutor struct {
	results map[string]*ResultStream // query -> result
	err     error
	calls   map[string]int
}

func (m *mockCypherExecutor) ExecuteQuery(_ context.Context, _ string, _ storage.Engine, query string, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	if m.calls != nil {
		m.calls[query]++
	}
	result, ok := m.results[query]
	if !ok {
		return nil, nil, fmt.Errorf("unexpected query: %s", query)
	}
	return result.Columns, result.Rows, nil
}

func (m *mockCypherExecutor) ExecuteQueryWithRecord(_ context.Context, _ string, _ storage.Engine, query string, _ map[string]interface{}, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	if m.calls != nil {
		m.calls[query]++
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

func TestFabricExecutor_ApplyBatchesCorrelatedCollectLookup(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	outerQuery := "MATCH (n) RETURN n.textKey128 AS textKey128"
	batchedInnerQuery := "MATCH (tt:MongoDocument) WHERE tt.translationId IN $__fabric_apply_keys RETURN tt.translationId AS __fabric_apply_key, collect(tt) AS texts"

	mock := &mockCypherExecutor{
		calls: map[string]int{},
		results: map[string]*ResultStream{
			outerQuery: {
				Columns: []string{"textKey128"},
				Rows:    [][]interface{}{{"k1"}, {"k2"}},
			},
			batchedInnerQuery: {
				Columns: []string{"__fabric_apply_key", "texts"},
				Rows:    [][]interface{}{{"k1", []interface{}{"hit-1", "hit-2"}}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	outer := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     outerQuery,
		GraphName: "db1",
		Columns:   []string{"textKey128"},
	}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"textKey128"}, ImportColumns: []string{"textKey128"}},
		Query:     "WITH textKey128 MATCH (tt:MongoDocument) WHERE tt.translationId = textKey128 RETURN collect(tt) AS texts",
		GraphName: "db1",
		Columns:   []string{"texts"},
	}
	apply := &FragmentApply{
		Input:   outer,
		Inner:   inner,
		Columns: []string{"textKey128", "texts"},
	}

	result, err := exec.Execute(ctx, nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RowCount() != 2 {
		t.Fatalf("expected 2 rows, got %d", result.RowCount())
	}
	if got := mock.calls[batchedInnerQuery]; got != 1 {
		t.Fatalf("expected single batched inner lookup, got %d calls", got)
	}
	if got := mock.calls["WITH textKey128 MATCH (tt:MongoDocument) WHERE tt.translationId = textKey128 RETURN collect(tt) AS texts"]; got != 0 {
		t.Fatalf("expected no per-row correlated inner calls, got %d", got)
	}
	// Row 1 matched.
	if v, ok := result.Rows[0][1].([]interface{}); !ok || len(v) != 2 {
		t.Fatalf("expected first row texts to contain 2 items, got %#v", result.Rows[0][1])
	}
	// Row 2 unmatched -> deterministic empty list.
	if v, ok := result.Rows[1][1].([]interface{}); !ok || len(v) != 0 {
		t.Fatalf("expected second row texts to be empty list, got %#v", result.Rows[1][1])
	}
}

func TestFabricExecutor_ApplyBatchesCorrelatedCollectLookup_WithUseAndExtraPredicate(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	outerQuery := "MATCH (n) RETURN n.textKey128 AS textKey128"
	batchedInnerQuery := "USE translations.txr MATCH (tt:MongoDocument) WHERE tt.translationId IN $__fabric_apply_keys AND tt.language = 'es' RETURN tt.translationId AS __fabric_apply_key, collect(tt) AS texts"

	mock := &mockCypherExecutor{
		calls: map[string]int{},
		results: map[string]*ResultStream{
			outerQuery: {
				Columns: []string{"textKey128"},
				Rows:    [][]interface{}{{"k1"}, {"k2"}},
			},
			batchedInnerQuery: {
				Columns: []string{"__fabric_apply_key", "texts"},
				Rows:    [][]interface{}{{"k2", []interface{}{"es-hit"}}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	outer := &FragmentExec{Input: &FragmentInit{}, Query: outerQuery, GraphName: "db1", Columns: []string{"textKey128"}}
	innerQuery := "WITH textKey128 USE translations.txr MATCH (tt:MongoDocument) WHERE tt.language = 'es' AND tt.translationId = textKey128 RETURN collect(tt) AS texts"
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"textKey128"}, ImportColumns: []string{"textKey128"}},
		Query:     innerQuery,
		GraphName: "db1",
		Columns:   []string{"texts"},
	}
	apply := &FragmentApply{Input: outer, Inner: inner, Columns: []string{"textKey128", "texts"}}

	result, err := exec.Execute(ctx, nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := mock.calls[batchedInnerQuery]; got != 1 {
		t.Fatalf("expected single batched inner lookup, got %d calls", got)
	}
	if v, ok := result.Rows[0][1].([]interface{}); !ok || len(v) != 0 {
		t.Fatalf("expected first row empty list, got %#v", result.Rows[0][1])
	}
	if v, ok := result.Rows[1][1].([]interface{}); !ok || len(v) != 1 {
		t.Fatalf("expected second row one hit, got %#v", result.Rows[1][1])
	}
}

func TestFabricExecutor_ApplyBatchesCorrelatedCollectLookup_ReversedEquality(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	outerQuery := "MATCH (n) RETURN n.textKey128 AS textKey128"
	batchedInnerQuery := "MATCH (tt:MongoDocument) WHERE tt.translationId IN $__fabric_apply_keys RETURN tt.translationId AS __fabric_apply_key, collect(tt) AS texts"

	mock := &mockCypherExecutor{
		calls: map[string]int{},
		results: map[string]*ResultStream{
			outerQuery: {
				Columns: []string{"textKey128"},
				Rows:    [][]interface{}{{"k1"}},
			},
			batchedInnerQuery: {
				Columns: []string{"__fabric_apply_key", "texts"},
				Rows:    [][]interface{}{{"k1", []interface{}{"hit"}}},
			},
		},
	}

	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	ctx := context.Background()

	outer := &FragmentExec{Input: &FragmentInit{}, Query: outerQuery, GraphName: "db1", Columns: []string{"textKey128"}}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: []string{"textKey128"}, ImportColumns: []string{"textKey128"}},
		Query:     "WITH textKey128 MATCH (tt:MongoDocument) WHERE textKey128 = tt.translationId RETURN collect(tt) AS texts",
		GraphName: "db1",
		Columns:   []string{"texts"},
	}
	apply := &FragmentApply{Input: outer, Inner: inner, Columns: []string{"textKey128", "texts"}}

	result, err := exec.Execute(ctx, nil, apply, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := mock.calls[batchedInnerQuery]; got != 1 {
		t.Fatalf("expected single batched inner lookup, got %d calls", got)
	}
	if v, ok := result.Rows[0][1].([]interface{}); !ok || len(v) != 1 {
		t.Fatalf("expected one hit, got %#v", result.Rows[0][1])
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

func TestDeduplicateRows_MapValuesDeterministic(t *testing.T) {
	rows := [][]interface{}{
		{map[string]interface{}{"b": int64(2), "a": int64(1)}},
		{map[string]interface{}{"a": int64(1), "b": int64(2)}},
	}
	deduped := deduplicateRows(rows)
	if len(deduped) != 1 {
		t.Fatalf("expected map rows to dedupe deterministically, got %d", len(deduped))
	}
}

func TestCombineRowsByIndexes_InnerOverridesOuter(t *testing.T) {
	resultCols := []string{"id", "outerOnly", "innerOnly", "shared"}
	outerIdx := buildColumnIndex([]string{"id", "outerOnly", "shared"})
	innerIdx := buildColumnIndex([]string{"innerOnly", "shared"})
	out := combineRowsByIndexes(
		resultCols,
		outerIdx, []interface{}{"a1", "outer", "outerShared"},
		innerIdx, []interface{}{"inner", "innerShared"},
	)
	if len(out) != 4 {
		t.Fatalf("expected 4 combined values, got %d", len(out))
	}
	if out[0] != "a1" || out[1] != "outer" || out[2] != "inner" || out[3] != "innerShared" {
		t.Fatalf("unexpected combined row: %#v", out)
	}
}

func TestFragmentContainsWrite(t *testing.T) {
	t.Run("exec write", func(t *testing.T) {
		if !fragmentContainsWrite(&FragmentExec{IsWrite: true}) {
			t.Fatal("expected write fragment to be detected")
		}
	})
	t.Run("exec read", func(t *testing.T) {
		if fragmentContainsWrite(&FragmentExec{IsWrite: false}) {
			t.Fatal("expected read fragment to be non-write")
		}
	})
	t.Run("nested apply", func(t *testing.T) {
		f := &FragmentApply{
			Input: &FragmentExec{IsWrite: false},
			Inner: &FragmentExec{IsWrite: true},
		}
		if !fragmentContainsWrite(f) {
			t.Fatal("expected nested write to be detected")
		}
	})
	t.Run("nested union", func(t *testing.T) {
		f := &FragmentUnion{
			LHS: &FragmentExec{IsWrite: false},
			RHS: &FragmentApply{
				Input: &FragmentInit{},
				Inner: &FragmentExec{IsWrite: true},
			},
		}
		if !fragmentContainsWrite(f) {
			t.Fatal("expected nested union write to be detected")
		}
	})
}

func TestRewriteLeadingWithImports_DeterministicClauseBoundary(t *testing.T) {
	query := "WITH id, size([x IN [1,2,3] WHERE x > 1]) AS c MATCH (n) WHERE n.id = id RETURN n"
	rewritten := rewriteLeadingWithImports(query, []string{"id"})
	expected := "WITH $id AS id MATCH (n) WHERE n.id = id RETURN n"
	if rewritten != expected {
		t.Fatalf("expected %q, got %q", expected, rewritten)
	}
}

func TestRewriteLeadingWithImports_RespectsQuotedKeywords(t *testing.T) {
	query := "WITH 'MATCH as text' AS txt, id RETURN txt, id"
	rewritten := rewriteLeadingWithImports(query, []string{"id"})
	expected := "WITH $id AS id RETURN txt, id"
	if rewritten != expected {
		t.Fatalf("expected %q, got %q", expected, rewritten)
	}
}

func TestRewriteLeadingWithImports_NoLeadingWith(t *testing.T) {
	query := "MATCH (n) RETURN n"
	rewritten := rewriteLeadingWithImports(query, []string{"id"})
	if rewritten != query {
		t.Fatalf("expected query unchanged, got %q", rewritten)
	}
}

func TestRewriteLeadingWithImports_NoLeadingWithReturn(t *testing.T) {
	query := "RETURN id, name"
	rewritten := rewriteLeadingWithImports(query, []string{"id", "name"})
	expected := "WITH $id AS id, $name AS name RETURN id, name"
	if rewritten != expected {
		t.Fatalf("expected %q, got %q", expected, rewritten)
	}
}

func TestExecuteApplyInMemoryProjection_WithCollectMapReturnAlias(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"textKey", "textKey128"},
		Rows: [][]interface{}{
			{"k1", "h1"},
			{"k2", "h2"},
		},
	}
	query := "WITH collect({textKey: textKey, textKey128: textKey128}) AS rows\nRETURN rows"
	res, handled := executeApplyInMemoryProjection(input, query)
	if !handled {
		t.Fatal("expected projection to be handled")
	}
	if res == nil {
		t.Fatal("expected non-nil projection result")
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one aggregated row, got %d", len(res.Rows))
	}
}

func TestExecuteApplyInMemoryProjection_ReturnOrderBySkipLimit(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"textKey128", "texts"},
		Rows: [][]interface{}{
			{"a2", []interface{}{}},
			{"a1", []interface{}{"ORD-001"}},
			{"a3", []interface{}{"ORD-003"}},
		},
	}
	query := "RETURN textKey128, texts ORDER BY textKey128 SKIP 1 LIMIT 1"
	res, handled := executeApplyInMemoryProjection(input, query)
	if !handled {
		t.Fatal("expected ordered projection to be handled")
	}
	if res == nil {
		t.Fatal("expected non-nil projection result")
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after SKIP/LIMIT, got %d", len(res.Rows))
	}
	if got := res.Rows[0][0]; got != "a2" {
		t.Fatalf("expected middle ordered row a2, got %#v", got)
	}
	texts, ok := res.Rows[0][1].([]interface{})
	if !ok || len(texts) != 0 {
		t.Fatalf("expected empty texts slice for a2, got %#v", res.Rows[0][1])
	}
}

func TestMergeBindings(t *testing.T) {
	parent := map[string]interface{}{"rows": []interface{}{1, 2}, "x": "parent"}
	row := map[string]interface{}{"x": "row", "k": "v"}
	got := mergeBindings(parent, row)
	if got["rows"] == nil || got["k"] != "v" || got["x"] != "row" {
		t.Fatalf("unexpected merged bindings: %#v", got)
	}
}

func TestSynthesizeEmptyCollectOnlyReturn(t *testing.T) {
	cols, row, ok := synthesizeEmptyCollectOnlyReturn("MATCH (n) RETURN collect(n.id) AS ids")
	if !ok {
		t.Fatal("expected collect-only return to synthesize empty row")
	}
	if len(cols) != 1 || cols[0] != "ids" {
		t.Fatalf("unexpected columns: %#v", cols)
	}
	values, ok := row[0].([]interface{})
	if !ok || len(values) != 0 {
		t.Fatalf("expected empty list value, got %#v", row[0])
	}
}

func BenchmarkDeduplicateRows(b *testing.B) {
	rows := make([][]interface{}, 0, 10000)
	for i := 0; i < 5000; i++ {
		row := []interface{}{int64(i), "v-" + strconv.Itoa(i), map[string]interface{}{"k": int64(i)}}
		rows = append(rows, row, row)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = deduplicateRows(rows)
	}
}

func BenchmarkCombineRowsByIndexes(b *testing.B) {
	resultCols := []string{"a", "b", "c", "d", "e", "f"}
	outerCols := []string{"a", "b", "c", "d"}
	innerCols := []string{"c", "e", "f"}
	outerIdx := buildColumnIndex(outerCols)
	innerIdx := buildColumnIndex(innerCols)
	outerRow := []interface{}{1, 2, 3, 4}
	innerRow := []interface{}{30, 50, 60}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = combineRowsByIndexes(resultCols, outerIdx, outerRow, innerIdx, innerRow)
	}
}
