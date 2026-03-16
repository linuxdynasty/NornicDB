package fabric

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

type slowBatchingCypherExecutor struct {
	outerQuery string
	outerRows  [][]interface{}
	active     atomic.Int32
	maxActive  atomic.Int32
	batchCalls atomic.Int32
}

func (s *slowBatchingCypherExecutor) ExecuteQuery(_ context.Context, _ string, _ storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	if query == s.outerQuery {
		return []string{"textKey128", "textKey", "originalText"}, s.outerRows, nil
	}
	if strings.Contains(query, "IN $__fabric_apply_keys") && strings.Contains(query, "__fabric_apply_key") {
		now := s.active.Add(1)
		for {
			max := s.maxActive.Load()
			if now <= max {
				break
			}
			if s.maxActive.CompareAndSwap(max, now) {
				break
			}
		}
		s.batchCalls.Add(1)
		time.Sleep(35 * time.Millisecond)
		keysAny, _ := params["__fabric_apply_keys"].([]interface{})
		rows := make([][]interface{}, 0, len(keysAny))
		for _, k := range keysAny {
			rows = append(rows, []interface{}{k, "es", fmt.Sprintf("t-%v", k)})
		}
		s.active.Add(-1)
		return []string{"__fabric_apply_key", "language", "translatedText"}, rows, nil
	}
	return nil, nil, fmt.Errorf("unexpected query: %s", query)
}

func (s *slowBatchingCypherExecutor) ExecuteQueryWithRecord(ctx context.Context, dbName string, eng storage.Engine, query string, params map[string]interface{}, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	return s.ExecuteQuery(ctx, dbName, eng, query, params)
}

type pipelineCaptureCypherExecutor struct {
	lastQuery  string
	lastParams map[string]interface{}
}

func (p *pipelineCaptureCypherExecutor) ExecuteQuery(_ context.Context, _ string, _ storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	p.lastQuery = query
	p.lastParams = params
	return []string{"k", "v"}, [][]interface{}{{"x", "y"}}, nil
}

func (p *pipelineCaptureCypherExecutor) ExecuteQueryWithRecord(ctx context.Context, dbName string, eng storage.Engine, query string, params map[string]interface{}, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	return p.ExecuteQuery(ctx, dbName, eng, query, params)
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

func TestLocalFragmentExecutor_Execute_WrapperPath(t *testing.T) {
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"RETURN 1 AS one": {
				Columns: []string{"one"},
				Rows:    [][]interface{}{{int64(1)}},
			},
		},
	}
	local := newTestLocalExecutor(mock)
	res, err := local.Execute(context.Background(), &LocationLocal{DBName: "db1"}, "RETURN 1 AS one", nil)
	if err != nil {
		t.Fatalf("unexpected execute error: %v", err)
	}
	if len(res.Columns) != 1 || res.Columns[0] != "one" {
		t.Fatalf("unexpected columns: %#v", res.Columns)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 || res.Rows[0][0] != int64(1) {
		t.Fatalf("unexpected rows: %#v", res.Rows)
	}
}

func TestSplitTopLevelEquality_AndApplyLookupKeyString(t *testing.T) {
	lhs, rhs, ok := splitTopLevelEquality(`tt.translationId = textKey128`)
	if !ok || lhs != "tt.translationId" || rhs != "textKey128" {
		t.Fatalf("unexpected equality parse: ok=%v lhs=%q rhs=%q", ok, lhs, rhs)
	}
	// Must ignore non-equality operators.
	if _, _, ok := splitTopLevelEquality(`tt.translationId >= textKey128`); ok {
		t.Fatalf("expected non-equality operator to be ignored")
	}
	// Must ignore nested equals inside string literal.
	if _, _, ok := splitTopLevelEquality(`tt.expr = "a=b"`); !ok {
		t.Fatalf("expected top-level equality to be detected")
	}

	if got := applyLookupKeyString([]byte(`"abc"`)); got != "s:abc" {
		t.Fatalf("expected byte-slice quoted key normalized to s:abc, got %q", got)
	}
	if got := applyLookupKeyString(`"xyz"`); got != "s:xyz" {
		t.Fatalf("expected quoted key normalized to s:xyz, got %q", got)
	}
	if got := applyLookupKeyString(int64(42)); got != "i64:42" {
		t.Fatalf("expected numeric key stringified to i64:42, got %q", got)
	}
}

func TestProjectSimpleReturnFromRow(t *testing.T) {
	cols, row, ok := projectSimpleReturnFromRow(
		"RETURN textKey AS key, textKey128 AS key128",
		[]string{"textKey", "textKey128"},
		[]interface{}{"k1", "h1"},
	)
	if !ok {
		t.Fatalf("expected simple return projection to be supported")
	}
	if len(cols) != 2 || cols[0] != "key" || cols[1] != "key128" {
		t.Fatalf("unexpected projected columns: %#v", cols)
	}
	if len(row) != 2 || row[0] != "k1" || row[1] != "h1" {
		t.Fatalf("unexpected projected row: %#v", row)
	}

	// Non-simple expression is intentionally rejected by this fast-path helper.
	if _, _, ok := projectSimpleReturnFromRow("RETURN coalesce(textKey, textKey128) AS key", []string{"textKey"}, []interface{}{"k"}); ok {
		t.Fatalf("expected non-simple return expression to bypass helper")
	}
}

func TestExecuteUnionSequential_Direct(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{
			"CREATE (n:Row)": {
				Columns: []string{"n"},
				Rows:    [][]interface{}{{"lhs"}},
			},
			"RETURN 1 AS one": {
				Columns: []string{"one"},
				Rows:    [][]interface{}{{int64(1)}},
			},
		},
	}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)

	union := &FragmentUnion{
		LHS: &FragmentExec{
			Input:     &FragmentInit{},
			Query:     "CREATE (n:Row)",
			GraphName: "db1",
			Columns:   []string{"n"},
			IsWrite:   true,
		},
		RHS: &FragmentExec{
			Input:     &FragmentInit{},
			Query:     "RETURN 1 AS one",
			GraphName: "db1",
			Columns:   []string{"one"},
		},
		Columns: []string{"v"},
	}

	res, err := exec.executeUnionSequential(context.Background(), nil, union, nil, "")
	if err != nil {
		t.Fatalf("unexpected sequential union error: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows from sequential union, got %d", len(res.Rows))
	}
}

func TestExecuteApplyInMemoryProjection_Shapes(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"textKey", "textKey128"},
		Rows: [][]interface{}{
			{"k1", "h1"},
			{"k2", "h2"},
		},
	}

	ret, ok := executeApplyInMemoryProjection(input, "RETURN textKey AS key, textKey128 AS hash")
	if !ok {
		t.Fatalf("expected simple RETURN projection to hit in-memory path")
	}
	if len(ret.Columns) != 2 || ret.Columns[0] != "key" || ret.Columns[1] != "hash" {
		t.Fatalf("unexpected columns: %#v", ret.Columns)
	}

	collectOnly, ok := executeApplyInMemoryProjection(
		input,
		"WITH collect({textKey: textKey, textKey128: textKey128}) AS rows RETURN rows",
	)
	if !ok || len(collectOnly.Rows) != 1 {
		t.Fatalf("expected collect-map-only projection result, got ok=%v rows=%#v", ok, collectOnly.Rows)
	}

	distinctKeys, ok := executeApplyInMemoryProjection(
		input,
		"WITH collect({textKey128: textKey128}) AS rows UNWIND rows AS r WITH collect(DISTINCT r.textKey128) AS keys RETURN keys",
	)
	if !ok || len(distinctKeys.Rows) != 1 {
		t.Fatalf("expected distinct-keys projection result, got ok=%v rows=%#v", ok, distinctKeys.Rows)
	}
}

func TestExecuteApplyAsPipeline_RewritesAndExecutes(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	capture := &pipelineCaptureCypherExecutor{}
	local := NewLocalFragmentExecutor(capture, func(name string) (storage.Engine, error) {
		if name != "db1" {
			return nil, fmt.Errorf("unexpected db %s", name)
		}
		return &mockEngine{}, nil
	})
	exec := NewFabricExecutor(catalog, local, nil)

	input := &ResultStream{
		Columns: []string{"textKey128", "textKey", "originalText"},
		Rows: [][]interface{}{
			{"h1", "k1", "o1"},
			{"h2", "k2", "o2"},
		},
	}

	inner := &FragmentExec{
		Input:     &FragmentInit{},
		Query:     "WITH textKey128 AS hash MATCH (tt) WHERE tt.translationId = hash RETURN tt.language AS language, tt.translatedText AS translatedText",
		GraphName: "db1",
		Columns:   []string{"language", "translatedText"},
	}

	res, used, err := exec.executeApplyAsPipeline(context.Background(), nil, input, inner, map[string]interface{}{"p": 1}, "")
	if err != nil {
		t.Fatalf("unexpected pipeline error: %v", err)
	}
	if !used {
		t.Fatalf("expected pipeline path to be used")
	}
	if res == nil || len(res.Rows) != 1 {
		t.Fatalf("expected mocked execution result, got %#v", res)
	}
	if !strings.Contains(capture.lastQuery, "UNWIND $__fabric_apply_rows AS __fabric_row") {
		t.Fatalf("expected query rewrite to UNWIND apply rows, got %q", capture.lastQuery)
	}
	rowsParam, ok := capture.lastParams["__fabric_apply_rows"].([]map[string]interface{})
	if !ok || len(rowsParam) != 2 {
		t.Fatalf("expected __fabric_apply_rows param with 2 rows, got %#v", capture.lastParams["__fabric_apply_rows"])
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

func TestInferReturnColumnsFromQuery(t *testing.T) {
	query := `CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
}
RETURN coalesce(textKey, textKey128) AS textKey, textKey128, count(*) AS c
ORDER BY textKey128
LIMIT 25`
	got := inferReturnColumnsFromQuery(query)
	if len(got) != 3 || got[0] != "textKey" || got[1] != "textKey128" || got[2] != "c" {
		t.Fatalf("unexpected inferred columns: %#v", got)
	}
}

func TestExecuteRowsGroupedJoinProjection_CoversShapes(t *testing.T) {
	input := &ResultStream{
		Columns: []string{"rows", "k", "texts"},
		Rows: [][]interface{}{
			{
				[]interface{}{
					map[string]interface{}{"textKey": "k1", "textKey128": "h1"},
					map[string]interface{}{"textKey": "k2", "textKey128": "h2"},
				},
				"h1",
				[]interface{}{"t1"},
			},
			{
				[]interface{}{},
				"h2",
				nil,
			},
		},
	}
	got := executeRowsGroupedJoinProjection(input)
	if len(got.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got.Rows))
	}
	if got.Rows[0][0] != "k1" || got.Rows[0][1] != "h1" {
		t.Fatalf("unexpected first row: %#v", got.Rows[0])
	}
	if texts, ok := got.Rows[0][2].([]interface{}); !ok || len(texts) != 1 {
		t.Fatalf("expected one grouped text in first row, got %#v", got.Rows[0][2])
	}
	if got.Rows[1][0] != "k2" || got.Rows[1][1] != "h2" {
		t.Fatalf("unexpected second row: %#v", got.Rows[1])
	}
	if texts, ok := got.Rows[1][2].([]interface{}); !ok || len(texts) != 0 {
		t.Fatalf("expected empty texts fallback for second row, got %#v", got.Rows[1][2])
	}
}

func TestExecuteRowsGroupedJoinProjection_InvalidInput(t *testing.T) {
	got := executeRowsGroupedJoinProjection(&ResultStream{
		Columns: []string{"rows"},
		Rows:    [][]interface{}{{[]interface{}{}}},
	})
	if len(got.Rows) != 0 {
		t.Fatalf("expected empty projection rows, got %#v", got.Rows)
	}
}

func TestFabricImportRewriteHelpers(t *testing.T) {
	init := &FragmentInit{Columns: []string{"a"}, ImportColumns: []string{"a"}}
	execFrag := &FragmentExec{
		Input:   init,
		Query:   "WITH a MATCH (n) WHERE n.id = a RETURN n.id AS id",
		Columns: []string{"id"},
	}
	union := &FragmentUnion{
		LHS:     execFrag,
		RHS:     &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1 AS one", Columns: []string{"one"}},
		Columns: []string{"id"},
	}
	apply := &FragmentApply{Input: &FragmentInit{}, Inner: union, Columns: []string{"id"}}

	if cols := importColumnsFromFragment(init); len(cols) != 1 || cols[0] != "a" {
		t.Fatalf("unexpected import columns: %#v", cols)
	}
	if !fragmentHasStaticImports(apply) {
		t.Fatal("expected fragmentHasStaticImports to detect static imports")
	}
	rewritten := rewriteFragmentWithImports(apply)
	rewrittenApply, ok := rewritten.(*FragmentApply)
	if !ok {
		t.Fatalf("expected rewritten apply, got %T", rewritten)
	}
	innerUnion, ok := rewrittenApply.Inner.(*FragmentUnion)
	if !ok {
		t.Fatalf("expected rewritten union, got %T", rewrittenApply.Inner)
	}
	lhsExec, ok := innerUnion.LHS.(*FragmentExec)
	if !ok {
		t.Fatalf("expected rewritten lhs exec, got %T", innerUnion.LHS)
	}
	if !strings.Contains(lhsExec.Query, "$a") {
		t.Fatalf("expected rewritten query to contain parameterized import, got %q", lhsExec.Query)
	}

	rewrittenRuntime := rewriteFragmentWithRuntimeImports(&FragmentExec{
		Input:   &FragmentInit{ImportColumns: []string{"a"}},
		Query:   "RETURN a, b",
		Columns: []string{"a", "b"},
	}, []string{"b", "a", "b"})
	rExec, ok := rewrittenRuntime.(*FragmentExec)
	if !ok {
		t.Fatalf("expected rewritten runtime exec, got %T", rewrittenRuntime)
	}
	if !strings.HasPrefix(strings.TrimSpace(rExec.Query), "WITH ") {
		t.Fatalf("expected runtime rewrite to prepend WITH imports, got %q", rExec.Query)
	}
	merged := mergeImportColumns([]string{"a", "b"}, []string{"b", "c"})
	if len(merged) != 3 || merged[0] != "a" || merged[1] != "b" || merged[2] != "c" {
		t.Fatalf("unexpected merged import columns: %#v", merged)
	}
}

func TestSubstituteAndReplaceStandaloneVar(t *testing.T) {
	query := `MATCH (tt) WHERE tt.translationId = textKey128 AND tt.note = "textKey128" RETURN textKey128, tt.translationId`
	sub := substituteVarsWithParams(query, []string{"textKey128"})
	if !strings.Contains(sub, "tt.translationId = $textKey128") {
		t.Fatalf("expected identifier substitution in predicate, got %q", sub)
	}
	if !strings.Contains(sub, `"textKey128"`) {
		t.Fatalf("expected string literal untouched, got %q", sub)
	}
}

func TestCombineRowsByColumnsWrapper(t *testing.T) {
	resultCols := []string{"a", "b", "c"}
	outerCols := []string{"a", "b"}
	innerCols := []string{"c"}
	outerRow := []interface{}{1, 2}
	innerRow := []interface{}{3}
	got := combineRowsByColumns(resultCols, outerCols, outerRow, innerCols, innerRow)
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("unexpected combined row: %#v", got)
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
