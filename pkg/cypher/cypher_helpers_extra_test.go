package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/vectorspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type typedResultFixture struct {
	Name      string    `cypher:"name"`
	Age       int       `json:"age"`
	CreatedAt time.Time `json:"created_at"`
	Score     float64
}

func TestCypherHelpers_DecodeMapAndAssignValue(t *testing.T) {
	m := map[string]interface{}{
		"name":       "alice",
		"age":        float64(32),
		"created_at": "2024-01-02T03:04:05Z",
		"score":      int64(7),
	}
	var out typedResultFixture
	err := decodeMap(m, reflect.ValueOf(&out).Elem())
	require.NoError(t, err)
	assert.Equal(t, "alice", out.Name)
	assert.Equal(t, 32, out.Age)
	assert.WithinDuration(t, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), out.CreatedAt, time.Second)
	assert.Equal(t, 7.0, out.Score)

	// assignValue error branch (unsupported conversion)
	var i int
	err = assignValue(reflect.ValueOf(&i).Elem(), "not-a-number")
	assert.Error(t, err)
}

func TestCypherHelpers_ExtractorsAndEnsureLabel(t *testing.T) {
	assert.Equal(t, "myGraph", extractGraphNameFromReturn("RETURN gds.graph.project('myGraph', ['A'], ['R'])"))
	assert.Equal(t, "", extractGraphNameFromReturn("RETURN 1"))
	assert.Equal(t, 0.75, extractFloatArg("{dampingFactor: 0.75, iterations: 20}", "dampingFactor"))
	assert.Equal(t, 0.0, extractFloatArg("{iterations: 20}", "dampingFactor"))

	labels := ensureLabel([]string{"A"}, "B")
	assert.ElementsMatch(t, []string{"A", "B"}, labels)
	labels2 := ensureLabel([]string{"A", "B"}, "B")
	assert.ElementsMatch(t, []string{"A", "B"}, labels2)
}

func TestCypherHelpers_ProcedureCatalogNodeID(t *testing.T) {
	id := procedureCatalogNodeID("  Db.Labels  ")
	assert.Equal(t, storage.NodeID(procedureCatalogPrefix+"db.labels"), id)
}

func TestCypherHelpers_NodeMapAndEmbeddingSummary(t *testing.T) {
	exec := &StorageExecutor{}

	nodePending := &storage.Node{
		ID:         "n-pending",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"name": "pending"},
	}
	pending := exec.nodeToMap(nodePending)
	require.Equal(t, "n-pending", pending["_nodeId"])
	require.Equal(t, "n-pending", pending["id"]) // fallback to storage ID when user id absent
	pEmb, ok := pending["embedding"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "pending", pEmb["status"])

	nodeReady := &storage.Node{
		ID:              "n-ready",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 2, 3, 4}},
		EmbedMeta: map[string]interface{}{
			"embedding_model": "bge-m3",
		},
		Properties: map[string]interface{}{"id": "user-id", "name": "ready"},
	}
	ready := exec.nodeToMap(nodeReady)
	require.Equal(t, "user-id", ready["id"]) // preserve user-provided id
	rEmb, ok := ready["embedding"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "ready", rEmb["status"])
	assert.Equal(t, 4, rEmb["dimensions"])
	assert.Equal(t, "bge-m3", rEmb["model"])
}

func TestCypherHelpers_ProcedureRegistryAndPatternNames(t *testing.T) {
	reg := NewProcedureRegistry()
	err := reg.RegisterBuiltIn(ProcedureSpec{Name: "db.labels", MinArgs: 0, MaxArgs: 0}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	})
	require.NoError(t, err)
	err = reg.RegisterUser(ProcedureSpec{Name: "custom.proc", MinArgs: 0, MaxArgs: 1}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	})
	require.NoError(t, err)

	builtins := reg.ListBuiltIns()
	require.Len(t, builtins, 1)
	assert.Equal(t, "db.labels", builtins[0].Name)

	assert.Equal(t, "Generic", PatternGeneric.String())
	assert.Equal(t, "MutualRelationship", PatternMutualRelationship.String())
	assert.Equal(t, "IncomingCountAgg", PatternIncomingCountAgg.String())
	assert.Equal(t, "OutgoingCountAgg", PatternOutgoingCountAgg.String())
	assert.Equal(t, "EdgePropertyAgg", PatternEdgePropertyAgg.String())
	assert.Equal(t, "LargeResultSet", PatternLargeResultSet.String())
}

func TestCypherHelpers_LooksNumericAndSkipString(t *testing.T) {
	assert.True(t, looksNumeric("42"))
	assert.True(t, looksNumeric("3.1415"))
	assert.False(t, looksNumeric("x42"))
	assert.Equal(t, "17", ExtractSkipString("MATCH (n) RETURN n SKIP 17 LIMIT 5"))
	assert.Equal(t, "", ExtractSkipString("MATCH (n) RETURN n"))
}

func TestCypherHelpers_ClauseAndMapLiterals(t *testing.T) {
	assert.True(t, prevWordEqualsIgnoreCase("name STARTS WITH 'a'", strings.Index("name STARTS WITH 'a'", "WITH"), "STARTS"))
	assert.True(t, prevWordEqualsIgnoreCase("name ENDS WITH 'z'", strings.Index("name ENDS WITH 'z'", "WITH"), "ENDS"))
	assert.False(t, prevWordEqualsIgnoreCase("WITH n", 0, "STARTS"))

	m := normalizeInterfaceMap(map[interface{}]interface{}{"k": 1, 2: "v"})
	assert.Equal(t, 1, m["k"])
	assert.Equal(t, "v", m["2"])

	assert.Equal(t, "'x'", valueToCypherLiteral("x"))
	assert.Equal(t, "true", valueToCypherLiteral(true))
	assert.Equal(t, "3.5", valueToCypherLiteral(3.5))
	assert.Equal(t, "null", valueToCypherLiteral(nil))
	assert.Equal(t, "[1, 'a']", valueToCypherLiteral([]interface{}{1, "a"}))
	assert.Contains(t, valueToCypherLiteral(map[string]interface{}{"a": 1}), "a: 1")
}

func TestCypherHelpers_DurationAndFunctionMatcherHelpers(t *testing.T) {
	d := &CypherDuration{Days: 1, Hours: 2, Minutes: 3, Seconds: 4, Nanos: 5}
	want := 24*time.Hour + 2*time.Hour + 3*time.Minute + 4*time.Second + 5*time.Nanosecond
	assert.Equal(t, want, d.ToTimeDuration())

	args, idx := extractFuncArgsLen("COUNT (n)", "count")
	assert.Equal(t, "n", args)
	assert.GreaterOrEqual(t, idx, 0)

	args, idx = extractFuncArgsLen("count(n) + 1", "count")
	assert.Equal(t, "", args)
	assert.Equal(t, -1, idx)
}

func TestCypherHelpers_CreatePipelineAndCreateHelpers(t *testing.T) {
	assert.True(t, containsString([]string{"a", "b"}, "a"))
	assert.False(t, containsString([]string{"a", "b"}, "c"))

	nodes := []*storage.Node{
		{ID: "2", Properties: map[string]interface{}{"orderId": 2}},
		{ID: "1", Properties: map[string]interface{}{"orderId": 1}},
		{ID: "3", Properties: map[string]interface{}{"other": 9}},
		nil,
	}
	sortNodesByProperty(nodes, "orderId")
	assert.Equal(t, "1", string(nodes[0].ID))
	assert.Equal(t, 1, getNodeProp(nodes[0], "orderId"))
	assert.Nil(t, getNodeProp(nil, "orderId"))
	assert.Nil(t, getNodeProp(&storage.Node{}, "orderId"))

	keys := getKeys(map[string]*storage.Node{"b": {}, "a": {}})
	assert.Len(t, keys, 2)
	assert.Contains(t, keys, "a")
	assert.Contains(t, keys, "b")
}

func TestCypherHelpers_NodeLookupCacheHelpers(t *testing.T) {
	exec := &StorageExecutor{
		nodeLookupCache: make(map[string]*storage.Node, 1000),
	}
	key := makeLookupKey("Person", map[string]interface{}{"name": "alice", "age": 30})
	assert.True(t, strings.HasPrefix(key, "Person:"))
	assert.Contains(t, key, "name=alice")
	assert.Contains(t, key, "age=30")
	assert.Equal(t, "Person", makeLookupKey("Person", nil))

	n := &storage.Node{ID: "n1"}
	exec.cacheNodeLookup("Person", map[string]interface{}{"name": "alice"}, n)
	got := exec.lookupCachedNode("Person", map[string]interface{}{"name": "alice"})
	require.NotNil(t, got)
	assert.Equal(t, storage.NodeID("n1"), got.ID)

	// eviction branch
	exec.nodeLookupCache = make(map[string]*storage.Node, 10002)
	for i := 0; i < 10002; i++ {
		exec.nodeLookupCache[makeLookupKey("X", map[string]interface{}{"i": i})] = &storage.Node{ID: storage.NodeID("x")}
	}
	exec.cacheNodeLookup("Person", map[string]interface{}{"name": "bob"}, &storage.Node{ID: "n2"})
	assert.LessOrEqual(t, len(exec.nodeLookupCache), 1001)
	exec.invalidateNodeLookupCache()
	assert.Len(t, exec.nodeLookupCache, 0)
}

func TestCypherHelpers_MatchRowsAndTransactionProjection(t *testing.T) {
	exec := &StorageExecutor{}
	nodes := map[string]*storage.Node{
		"n": {ID: "n1", Properties: map[string]interface{}{"name": "alice", "age": int64(42)}},
	}
	co := exec.evaluateCoalesceInContext("COALESCE(n.missing, n.name, 'fallback')", nodes, nil, map[string]interface{}{})
	assert.Equal(t, "alice", co)
	co2 := exec.evaluateCoalesceInContext("COALESCE(missing, 'fallback')", nodes, nil, map[string]interface{}{})
	assert.Equal(t, "fallback", co2)
	assert.Nil(t, exec.evaluateCoalesceInContext("COALESCE(", nodes, nil, nil))

	assert.True(t, exec.nodeMatchesWhereClause(nodes["n"], "n.age >= 40", "n"))
	assert.False(t, exec.nodeMatchesWhereClause(nodes["n"], "n.age < 40", "n"))

	input := &ExecuteResult{
		Columns: []string{"x", "n"},
		Rows: [][]interface{}{
			{int64(7), nodes["n"]},
		},
	}
	out, err := exec.projectTransactionReturn(input, "x AS val, n.name AS name")
	require.NoError(t, err)
	require.Equal(t, []string{"val", "name"}, out.Columns)
	require.Len(t, out.Rows, 1)
	require.Equal(t, int64(7), out.Rows[0][0])
	require.Equal(t, "alice", out.Rows[0][1])

	empty, err := exec.projectTransactionReturn(input, "")
	require.NoError(t, err)
	require.Equal(t, []string{"*"}, empty.Columns)
	require.Len(t, empty.Rows, 1)
	require.Equal(t, "*", empty.Rows[0][0])
}

func TestCypherHelpers_ToStringAnyMapAndSubstringSet(t *testing.T) {
	m, ok := toStringAnyMap(map[string]interface{}{"a": 1})
	require.True(t, ok)
	assert.Equal(t, 1, m["a"])

	m, ok = toStringAnyMap(map[interface{}]interface{}{"a": 1})
	require.True(t, ok)
	assert.Equal(t, 1, m["a"])

	_, ok = toStringAnyMap(map[interface{}]interface{}{1: "x"})
	assert.False(t, ok)
	_, ok = toStringAnyMap([]int{1, 2})
	assert.False(t, ok)

	exec := &StorageExecutor{}
	assert.Equal(t, "bc", exec.evaluateSubstringForSet("substring('abc', 1, 2)"))
	assert.Equal(t, "c", exec.evaluateSubstringForSet("substring('abc', 2)"))
	assert.Equal(t, "", exec.evaluateSubstringForSet("substring('abc', 9, 1)"))
	assert.Equal(t, "ab", exec.evaluateSubstringForSet("substring('abc', bad, 2)"))
	assert.Equal(t, "", exec.evaluateSubstringForSet("substring('abc')"))
}

func TestCypherHelpers_FindNodeByProperties_AndRangeIndex(t *testing.T) {
	base := storage.NewMemoryEngine()
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice", "age": int64(30)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob", "age": int64(40)}})
	require.NoError(t, err)

	n := exec.findNodeByProperties(map[string]interface{}{"name": "alice"})
	require.NotNil(t, n)
	assert.Equal(t, storage.NodeID("p1"), n.ID)
	assert.Nil(t, exec.findNodeByProperties(map[string]interface{}{"name": "nobody"}))

	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_age IF NOT EXISTS FOR (n:Person) ON (n.age)")
	require.NoError(t, err)
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX FOR (n:Person) ON (n.age)")
	require.NoError(t, err)
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_bad FOR (n:Person) ON (n.a, n.b)")
	require.Error(t, err)
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX nonsense")
	require.Error(t, err)
}

func TestCypherHelpers_ExecuteMatchWithPipelineToRows(t *testing.T) {
	base := storage.NewMemoryEngine()
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	// Seed OrderStatus and Pharmacy data expected by pipeline helper.
	_, err := eng.CreateNode(&storage.Node{ID: "o1", Labels: []string{"OrderStatus"}, Properties: map[string]interface{}{"orderId": int64(1)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o2", Labels: []string{"OrderStatus"}, Properties: map[string]interface{}{"orderId": int64(2)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "ph1", Labels: []string{"Pharmacy"}, Properties: map[string]interface{}{"id": int64(10)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "ph2", Labels: []string{"Pharmacy"}, Properties: map[string]interface{}{"id": int64(20)}})
	require.NoError(t, err)

	matchPart := "MATCH (o:OrderStatus) WITH collect(o) AS orders UNWIND range(0, size(orders)-1) AS i WITH orders[i] AS o, i MATCH (ph:Pharmacy) WITH o, i, ph ORDER BY ph.id WITH o, i, collect(ph) AS pharmacies WITH o, pharmacies[i % size(pharmacies)] AS pharmacy"
	rows, err := exec.executeMatchWithPipelineToRows(ctx, matchPart, []string{"o", "pharmacy"}, eng)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	for _, row := range rows {
		_, okO := row["o"].(*storage.Node)
		_, okP := row["pharmacy"].(*storage.Node)
		assert.True(t, okO)
		assert.True(t, okP)
	}

	// Missing WITH should error.
	_, err = exec.executeMatchWithPipelineToRows(ctx, "MATCH (o:OrderStatus)", []string{"o"}, eng)
	require.Error(t, err)
}

func TestCypherHelpers_ParserMarkersAndUnwindHelpers(t *testing.T) {
	// Clause marker coverage.
	var _ Clause = &MatchClause{}
	var _ Clause = &CreateClause{}
	var _ Clause = &ReturnClause{}
	var _ Clause = &WhereClause{}
	var _ Clause = &SetClause{}
	var _ Clause = &DeleteClause{}
	for _, c := range []Clause{
		&MatchClause{}, &CreateClause{}, &ReturnClause{}, &WhereClause{}, &SetClause{}, &DeleteClause{},
	} {
		c.clauseMarker()
	}

	// Expression marker coverage.
	var _ Expression = &PropertyAccess{}
	var _ Expression = &Comparison{}
	var _ Expression = &Literal{}
	var _ Expression = &Parameter{}
	var _ Expression = &FunctionCall{}
	for _, ex := range []Expression{
		&PropertyAccess{}, &Comparison{}, &Literal{}, &Parameter{}, &FunctionCall{},
	} {
		ex.exprMarker()
	}

	assert.True(t, hasOuterParens("(a)"))
	assert.True(t, hasOuterParens("((a+b))"))
	assert.False(t, hasOuterParens("(a)+b"))
	assert.Equal(t, "$x", normalizeUnwindExpression(" ( ($x) ) "))

	assert.Nil(t, coerceToUnwindItems(nil))
	assert.Equal(t, []interface{}{"a", "b"}, coerceToUnwindItems([]string{"a", "b"}))
	assert.Equal(t, []interface{}{1, 2}, coerceToUnwindItems([]int{1, 2}))
	assert.Equal(t, []interface{}{int64(1), int64(2)}, coerceToUnwindItems([]int64{1, 2}))
	assert.Equal(t, []interface{}{"x"}, coerceToUnwindItems("x"))
}

func TestCypherHelpers_CartesianMatchAndAggregation(t *testing.T) {
	base := storage.NewMemoryEngine()
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "a1"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "a2", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "a2"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"name": "b1"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "b2", Labels: []string{"B"}, Properties: map[string]interface{}{"name": "b2"}})
	require.NoError(t, err)

	result := &ExecuteResult{
		Columns: []string{"aName", "bName"},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	items := []returnItem{
		{expr: "a.name", alias: "aName"},
		{expr: "b.name", alias: "bName"},
	}
	_, err = exec.executeCartesianProductMatch(
		ctx,
		"MATCH (a:A), (b:B) RETURN a.name AS aName, b.name AS bName ORDER BY aName SKIP 1 LIMIT 2",
		"(a:A), (b:B)",
		[]string{"(a:A)", "(b:B)"},
		-1,
		strings.Index("MATCH (a:A), (b:B) RETURN a.name AS aName, b.name AS bName ORDER BY aName SKIP 1 LIMIT 2", "RETURN"),
		items,
		false,
		false,
		result,
	)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)

	// Aggregation without grouping.
	aggResult := &ExecuteResult{Columns: []string{"cnt"}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
	aggItems := []returnItem{{expr: "COUNT(*)", alias: "cnt"}}
	_, err = exec.executeCartesianProductMatch(
		ctx,
		"MATCH (a:A), (b:B) RETURN COUNT(*) AS cnt",
		"(a:A), (b:B)",
		[]string{"(a:A)", "(b:B)"},
		-1,
		strings.Index("MATCH (a:A), (b:B) RETURN COUNT(*) AS cnt", "RETURN"),
		aggItems,
		true,
		false,
		aggResult,
	)
	require.NoError(t, err)
	require.Len(t, aggResult.Rows, 1)
	assert.Equal(t, int64(4), aggResult.Rows[0][0])

	// Aggregation with grouping path.
	allMatches := []map[string]*storage.Node{
		{"a": {ID: "a1", Properties: map[string]interface{}{"name": "x"}}},
		{"a": {ID: "a2", Properties: map[string]interface{}{"name": "x"}}},
		{"a": {ID: "a3", Properties: map[string]interface{}{"name": "y"}}},
	}
	groupRes := &ExecuteResult{Columns: []string{"name", "cnt"}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
	_, err = exec.executeCartesianAggregation(allMatches, []returnItem{{expr: "a.name", alias: "name"}, {expr: "COUNT(*)", alias: "cnt"}}, groupRes)
	require.NoError(t, err)
	require.Len(t, groupRes.Rows, 2)
}

func TestCypherHelpers_TraversalAndShortestPathHelpers(t *testing.T) {
	base := storage.NewMemoryEngine()
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	_, err := eng.CreateNode(&storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "alpha"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "a2", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "beta"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"name": "bee"}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{
		ID:        "e1",
		StartNode: "a1",
		EndNode:   "b1",
		Type:      "KNOWS",
	})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{
		ID:        "e2",
		StartNode: "a2",
		EndNode:   "b1",
		Type:      "KNOWS",
	})
	require.NoError(t, err)

	// Invalid pattern branch for executeMatchWithRelationships.
	_, err = exec.executeMatchWithRelationships("this is not a pattern", "", []returnItem{{expr: "a", alias: "a"}})
	require.Error(t, err)

	// Valid traversal branch.
	r, err := exec.executeMatchWithRelationships("(a:A)-[r:KNOWS]->(b:B)", "", []returnItem{
		{expr: "a.name", alias: "aName"},
		{expr: "b.name", alias: "bName"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, r.Rows)

	// Cover traverseGraphParallel directly with explicit worker config.
	match := exec.parseTraversalPattern("(a:A)-[r:KNOWS]->(b:B)")
	require.NotNil(t, match)
	startNodes, err := eng.GetNodesByLabel("A")
	require.NoError(t, err)
	paths := exec.traverseGraphParallel(match, startNodes, ParallelConfig{
		Enabled:      true,
		MaxWorkers:   2,
		MinBatchSize: 1,
	})
	require.Len(t, paths, 2)

	// Cover evaluatePathExpression helper.
	path := PathResult{
		Nodes:  []*storage.Node{{ID: "s1", Properties: map[string]interface{}{"name": "start"}}, {ID: "e1", Properties: map[string]interface{}{"name": "end"}}},
		Length: 1,
	}
	q := &ShortestPathQuery{
		startNode: nodePatternInfo{variable: "s"},
		endNode:   nodePatternInfo{variable: "e"},
	}
	assert.Equal(t, "start", exec.evaluatePathExpression("s.name", path, q))

	// Sanity to ensure shortest path helper still identifies syntax.
	assert.True(t, isShortestPathQuery("MATCH p=shortestPath((a)-[*]->(b)) RETURN p"))
	assert.True(t, isShortestPathQuery("MATCH p=allShortestPaths((a)-[*]->(b)) RETURN p"))
	assert.False(t, isShortestPathQuery("MATCH (a) RETURN a"))
}

func TestCypherHelpers_ExecuteCallFallbackDispatch(t *testing.T) {
	base := storage.NewMemoryEngine()
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "n1", Labels: []string{"L1"}, Properties: map[string]interface{}{"name": "x"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "n2", Labels: []string{"L2"}, Properties: map[string]interface{}{"name": "y"}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{
		ID:         "rel1",
		StartNode:  "n1",
		EndNode:    "n2",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"text": "hello world"},
	})
	require.NoError(t, err)

	// Force executeCall to use legacy switch fallback (instead of registry-first dispatch)
	// so we can validate and cover those branches while preserving production behavior.
	origRegistry := globalProcedureRegistry
	origOnce := builtinProcedureRegistryOnce
	doneOnce := sync.Once{}
	doneOnce.Do(func() {})
	globalProcedureRegistry = NewProcedureRegistry()
	builtinProcedureRegistryOnce = doneOnce
	defer func() {
		globalProcedureRegistry = origRegistry
		builtinProcedureRegistryOnce = origOnce
	}()

	cases := []struct {
		query     string
		expectErr bool
	}{
		{query: "CALL db.labels()", expectErr: false},
		{query: "CALL db.relationshipTypes()", expectErr: false},
		{query: "CALL db.schema.visualization()", expectErr: false},
		{query: "CALL db.schema.nodeProperties()", expectErr: false},
		{query: "CALL db.schema.relProperties()", expectErr: false},
		{query: "CALL db.indexes()", expectErr: false},
		{query: "CALL db.index.stats()", expectErr: false},
		{query: "CALL db.constraints()", expectErr: false},
		{query: "CALL db.propertyKeys()", expectErr: false},
		{query: "CALL db.info()", expectErr: false},
		{query: "CALL db.ping()", expectErr: false},
		{query: "CALL dbms.info()", expectErr: false},
		{query: "CALL dbms.listConfig()", expectErr: false},
		{query: "CALL dbms.clientConfig()", expectErr: false},
		{query: "CALL dbms.listConnections()", expectErr: false},
		{query: "CALL dbms.components()", expectErr: false},
		{query: "CALL dbms.procedures()", expectErr: false},
		{query: "CALL dbms.functions()", expectErr: false},
		{query: "CALL db.index.fulltext.listAvailableAnalyzers()", expectErr: false},
		{query: "CALL db.index.fulltext.queryRelationships('idx','hello')", expectErr: false},
		{query: "CALL db.stats.status()", expectErr: false},
		{query: "CALL db.clearQueryCaches()", expectErr: false},
		{query: "CALL tx.setMetaData({app:'x'})", expectErr: true}, // requires active tx
		{query: "CALL db.notARealProcedure()", expectErr: true},
	}

	for _, tc := range cases {
		res, err := exec.executeCall(ctx, tc.query)
		if tc.expectErr {
			require.Error(t, err, tc.query)
		} else {
			require.NoError(t, err, tc.query)
			require.NotNil(t, res, tc.query)
		}
	}

	expectSuccess := []string{
		"CALL nornicdb.version()",
		"CALL nornicdb.stats()",
		"CALL nornicdb.decay.info()",
		"CALL db.awaitIndexes()",
		"CALL db.awaitIndex('idx')",
		"CALL db.resampleIndex('idx')",
		"CALL db.stats.retrieveAllAnTheStats()",
		"CALL db.stats.retrieve('QUERIES')",
		"CALL db.stats.collect('QUERIES')",
		"CALL db.stats.clear()",
		"CALL db.stats.stop()",
	}
	for _, q := range expectSuccess {
		res, err := exec.executeCall(ctx, q)
		require.NoErrorf(t, err, "expected success for query: %s", q)
		require.NotNilf(t, res, "expected non-nil result for query: %s", q)
	}

	expectError := []string{
		"CALL apoc.load.jsonarray('file:///missing.json')",
		"CALL apoc.load.json('file:///missing.json')",
		"CALL apoc.load.csv('file:///missing.csv')",
		"CALL apoc.import.json('file:///missing.json')",
		"CALL db.txlog.entries(1, 10)",
		"CALL db.txlog.byTxId('tx-1', 10)",
	}
	for _, q := range expectError {
		_, err := exec.executeCall(ctx, q)
		require.Errorf(t, err, "expected error for query: %s", q)
	}

	// Environment-dependent calls: keep separate from strict success/error assertions.
	allowEither := []string{
		"CALL apoc.path.subgraphNodes('n1', {maxLevel: 1})",
		"CALL apoc.path.expand('n1', 'KNOWS>', '', 1, 2)",
		"CALL apoc.path.spanningTree('n1', {maxLevel: 1})",
		"CALL apoc.algo.dijkstra('n1','n2','KNOWS','weight')",
		"CALL apoc.algo.aStar('n1','n2','KNOWS','lat','lon')",
		"CALL apoc.algo.allSimplePaths('n1','n2','KNOWS', 3)",
		"CALL apoc.algo.pageRank({iterations: 2})",
		"CALL apoc.algo.betweenness()",
		"CALL apoc.algo.closeness()",
		"CALL apoc.algo.louvain()",
		"CALL apoc.algo.labelPropagation()",
		"CALL apoc.algo.wcc()",
		"CALL apoc.neighbors.tohop('n1','KNOWS',1)",
		"CALL apoc.neighbors.byhop('n1','KNOWS',2)",
		"CALL apoc.export.json.all('file:///tmp.json', {})",
		"CALL apoc.export.json.query('MATCH (n) RETURN n', 'file:///tmp.json', {})",
		"CALL apoc.export.csv.all('file:///tmp.csv', {})",
		"CALL apoc.export.csv.query('MATCH (n) RETURN n', 'file:///tmp.csv', {})",
		"CALL db.retrieve({query:'x'})",
		"CALL db.rretrieve({query:'x'})",
		"CALL db.rerank({query:'x', candidates: []})",
		"CALL db.infer({prompt:'x'})",
		"CALL gds.version()",
		"CALL gds.graph.list()",
		"CALL gds.graph.drop('missing')",
		"CALL gds.graph.project('g', ['A'], ['R'])",
		"CALL gds.fastRP.stream('g', {})",
		"CALL gds.fastRP.stats('g', {})",
		"CALL db.index.vector.queryRelationships('idx', 2, [0.1, 0.2])",
		"CALL db.index.vector.embed('hello')",
		"CALL db.index.vector.createNodeIndex('v_idx', 'Doc', 'embedding', 2, 'cosine')",
		"CALL db.index.vector.createRelationshipIndex('vr_idx', 'REL', 'embedding', 2, 'cosine')",
		"CALL db.index.fulltext.createNodeIndex('ft_idx', ['Doc'], ['content'])",
		"CALL db.index.fulltext.createRelationshipIndex('ftr_idx', ['REL'], ['content'])",
		"CALL db.index.fulltext.drop('ft_idx')",
		"CALL db.index.vector.drop('v_idx')",
		"CALL db.create.setNodeVectorProperty('n1','embedding',[0.1,0.2])",
		"CALL db.create.setRelationshipVectorProperty('rel1','embedding',[0.1,0.2])",
		"CALL db.temporal.assertNoOverlap('Fact','k','vf','vt','id','2024-01-01','2024-01-02')",
		"CALL db.temporal.asOf('Fact','k','id','vf','vt','2024-01-01')",
		"CALL apoc.cypher.run('RETURN 1', {})",
		"CALL apoc.cypher.doItAll('RETURN 1', {})",
		"CALL apoc.cypher.runMany('RETURN 1; RETURN 2', {})",
		"CALL apoc.periodic.iterate('RETURN 1 AS x', 'RETURN x', {})",
		"CALL apoc.periodic.commit('RETURN 1', {})",
		"CALL apoc.periodic.rock_n_roll('RETURN 1 AS x', 'RETURN x', {})",
	}
	for _, q := range allowEither {
		res, err := exec.executeCall(ctx, q)
		require.Truef(t, res != nil || err != nil, "executeCall returned nil result and nil error for query: %s", q)
	}
}

func TestCypherHelpers_CallCompatRelationshipQueries(t *testing.T) {
	base := storage.NewMemoryEngine()
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "a"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "b"}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{
		ID:         "e1",
		StartNode:  "n1",
		EndNode:    "n2",
		Type:       "RELATED",
		Properties: map[string]interface{}{"content": "searchable text"},
	})
	require.NoError(t, err)

	// Fulltext relationship query: empty query branch.
	res, err := exec.callDbIndexFulltextQueryRelationships("CALL db.index.fulltext.queryRelationships('idx','')")
	require.NoError(t, err)
	require.Empty(t, res.Rows)

	// Fulltext relationship query: match path.
	res, err = exec.callDbIndexFulltextQueryRelationships("CALL db.index.fulltext.queryRelationships('idx','searchable')")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	// Vector relationship query: parse error.
	_, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx', 2)")
	require.Error(t, err)

	// Vector relationship query: string input without embedder.
	_, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx', 2, 'hello')")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no embedder configured")

	// Vector relationship query: parameter without params context => empty rows.
	res, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx', 2, $q)")
	require.NoError(t, err)
	require.Empty(t, res.Rows)

	// Vector relationship query: missing parameter.
	ctxMissing := context.WithValue(ctx, paramsKey, map[string]interface{}{"x": []float32{0.1, 0.2}})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxMissing, "CALL db.index.vector.queryRelationships('idx', 2, $q)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "parameter $q not provided")

	// Vector relationship query: unsupported parameter element type.
	ctxBadElem := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": []interface{}{1, "bad"}})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxBadElem, "CALL db.index.vector.queryRelationships('idx', 2, $q)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-numeric value")

	// Vector relationship query: unsupported parameter type and missing query vector branch.
	ctxBadType := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": true})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxBadType, "CALL db.index.vector.queryRelationships('idx', 2, $q)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")
}

func TestCypherHelpers_ComparisonConversionAndTemporalHelpers(t *testing.T) {
	assert.False(t, compareForSort(nil, nil))
	assert.True(t, compareForSort(nil, "x"))
	assert.False(t, compareForSort("x", nil))
	assert.True(t, compareForSort(int64(1), int64(2)))
	assert.True(t, compareForSort(int64(1), float64(2)))
	assert.True(t, compareForSort(1, 2))
	assert.True(t, compareForSort(1, int64(2)))
	assert.True(t, compareForSort(float64(1.5), float64(2.5)))
	assert.True(t, compareForSort(float64(1.5), int64(2)))
	assert.True(t, compareForSort("a", "b"))
	assert.True(t, compareForSort(struct{ X int }{X: 1}, struct{ X int }{X: 2}))

	assert.Equal(t, int64(3), toInt64(3))
	assert.Equal(t, int64(4), toInt64(int64(4)))
	assert.Equal(t, int64(5), toInt64(float64(5.9)))
	assert.Equal(t, int64(6), toInt64("6"))
	assert.Equal(t, int64(0), toInt64("bad"))
	assert.Equal(t, int64(0), toInt64(true))

	ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{"p": "v", "n": int64(7)})
	assert.Nil(t, resolveTemporalArg(ctx, ""))
	assert.Nil(t, resolveTemporalArg(ctx, "NULL"))
	assert.Equal(t, "v", resolveTemporalArg(ctx, "$p"))
	assert.Nil(t, resolveTemporalArg(ctx, "$missing"))
	assert.Equal(t, "s", resolveTemporalArg(ctx, "'s'"))
	assert.Equal(t, "d", resolveTemporalArg(ctx, "\"d\""))
	assert.Equal(t, int64(9), resolveTemporalArg(ctx, "9"))
	assert.Equal(t, float64(3.5), resolveTemporalArg(ctx, "3.5"))
	assert.Equal(t, "raw", resolveTemporalArg(ctx, "raw"))

	_, err := coerceStringArg(nil, "arg")
	require.Error(t, err)
	_, err = coerceStringArg("   ", "arg")
	require.Error(t, err)
	s, err := coerceStringArg(12, "arg")
	require.NoError(t, err)
	assert.Equal(t, "12", s)

	now := time.Now().UTC().Truncate(time.Second)
	tm, ok := coerceDateTime(now)
	require.True(t, ok)
	assert.Equal(t, now, tm)
	tm, ok = coerceDateTime("2024-01-02T03:04:05Z")
	require.True(t, ok)
	assert.Equal(t, int64(1704164645), tm.Unix())
	tm, ok = coerceDateTime(int64(1700000000))
	require.True(t, ok)
	assert.Equal(t, int64(1700000000), tm.Unix())
	tm, ok = coerceDateTime(float64(1700000001))
	require.True(t, ok)
	assert.Equal(t, int64(1700000001), tm.Unix())
	_, ok = coerceDateTime(struct{}{})
	assert.False(t, ok)

	_, ok = coerceDateTimeOptional(nil)
	assert.False(t, ok)

	aStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	aEnd := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	bStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	bEnd := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, intervalsOverlap(aStart, aEnd, true, bStart, bEnd, true))
	assert.False(t, intervalsOverlap(time.Time{}, aEnd, true, bStart, bEnd, true))
	assert.False(t, intervalsOverlap(aStart, aEnd, true, bEnd, bEnd, true))
	assert.False(t, intervalsOverlap(bEnd, bEnd, true, aStart, aEnd, true))

	assert.True(t, valuesEqual(int64(1), "1"))
	assert.False(t, valuesEqual(1, 2))
	assert.False(t, isTruthy(nil))
	assert.False(t, isTruthy(false))
	assert.False(t, isTruthy(int64(0)))
	assert.False(t, isTruthy(""))
	assert.True(t, isTruthy(true))
	assert.True(t, isTruthy(2))
	assert.True(t, isTruthy("x"))
	assert.True(t, isTruthy(struct{}{}))
}

func TestCypherHelpers_DatabaseNameAndRemoveNodeFromSearch(t *testing.T) {
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "tenant_cov")
	exec := NewStorageExecutor(ns)

	assert.Equal(t, "tenant_cov", exec.databaseName())

	execNoNS := NewStorageExecutor(base)
	t.Setenv("NORNICDB_DEFAULT_DATABASE", "db_env_cov")
	assert.Equal(t, "db_env_cov", execNoNS.databaseName())

	// removeNodeFromSearch should early-return on empty id / nil search.
	execNoNS.removeNodeFromSearch("")
	execNoNS.removeNodeFromSearch("plain-id")

	// With search service configured, prefixed IDs should be unprefixed before removal.
	svc := search.NewService(base)
	execNoNS.SetSearchService(svc)
	execNoNS.removeNodeFromSearch("tenant_cov:node-1")
	execNoNS.removeNodeFromSearch("node-2")
}

func TestCypherHelpers_SchemaVectorAndApocPathHelpers(t *testing.T) {
	// parsePropertyType coverage.
	pt, err := parsePropertyType("STRING")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeString, pt)
	pt, err = parsePropertyType("INT")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeInteger, pt)
	pt, err = parsePropertyType("FLOAT")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeFloat, pt)
	pt, err = parsePropertyType("BOOL")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeBoolean, pt)
	pt, err = parsePropertyType("DATE")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeDate, pt)
	pt, err = parsePropertyType("ZONED DATETIME")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeZonedDateTime, pt)
	pt, err = parsePropertyType("LOCALDATETIME")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeLocalDateTime, pt)
	_, err = parsePropertyType("UNSUPPORTED")
	require.Error(t, err)

	// toDistanceMetric coverage.
	dist, err := toDistanceMetric("")
	require.NoError(t, err)
	assert.Equal(t, vectorspace.DistanceCosine, dist)
	dist, err = toDistanceMetric("dot")
	require.NoError(t, err)
	assert.Equal(t, vectorspace.DistanceDot, dist)
	dist, err = toDistanceMetric("euclidean")
	require.NoError(t, err)
	assert.Equal(t, vectorspace.DistanceEuclidean, dist)
	_, err = toDistanceMetric("chebyshev")
	require.Error(t, err)

	// isTerminateNode coverage.
	node := &storage.Node{ID: "n1", Labels: []string{"A", "B"}}
	assert.True(t, isTerminateNode(node, []string{"B"}))
	assert.False(t, isTerminateNode(node, []string{"C"}))
	assert.False(t, isTerminateNode(node, nil))
}

func TestCypherHelpers_CreateAndDropConstraintVariants(t *testing.T) {
	validCreate := []string{
		"CREATE CONSTRAINT c_nodekey IF NOT EXISTS FOR (n:Person) REQUIRE (n.id, n.tenant) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE (n.id, n.tenant) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT (n.id, n.tenant) IS NODE KEY",
		"CREATE CONSTRAINT c_temporal IF NOT EXISTS FOR (n:Fact) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Fact) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL",
		"CREATE CONSTRAINT c_exists IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT exists(n.name)",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.name IS NOT NULL",
		"CREATE CONSTRAINT c_type IF NOT EXISTS FOR (n:Person) REQUIRE n.age IS :: INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.age IS :: INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.age IS :: INTEGER",
		"CREATE CONSTRAINT c_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.email IS UNIQUE",
	}

	for i, q := range validCreate {
		t.Run(fmt.Sprintf("create_variant_%d", i), func(t *testing.T) {
			base := storage.NewMemoryEngine()
			eng := storage.NewNamespacedEngine(base, "test")
			exec := NewStorageExecutor(eng)
			_, err := exec.executeCreateConstraint(context.Background(), q)
			require.NoError(t, err, q)
		})
	}

	base := storage.NewMemoryEngine()
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	_, err := exec.executeCreateConstraint(context.Background(), "CREATE CONSTRAINT bad syntax")
	require.Error(t, err)

	_, err = exec.executeCreateConstraint(context.Background(), "CREATE CONSTRAINT c_type_bad IF NOT EXISTS FOR (n:Person) REQUIRE n.age IS :: BOGUS")
	require.Error(t, err)

	// Drop existing constraint path.
	_, err = exec.executeCreateConstraint(context.Background(), "CREATE CONSTRAINT c_drop IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE")
	require.NoError(t, err)
	_, err = exec.executeDropConstraint(context.Background(), "DROP CONSTRAINT c_drop")
	require.NoError(t, err)

	// IF EXISTS swallow path.
	_, err = exec.executeDropConstraint(context.Background(), "DROP CONSTRAINT c_missing IF EXISTS")
	require.NoError(t, err)

	// Invalid drop syntax.
	_, err = exec.executeDropConstraint(context.Background(), "DROP CONSTRAINT")
	require.Error(t, err)
}

func TestCypherHelpers_CallPluginHandlerSignatures(t *testing.T) {
	type tc struct {
		name      string
		handler   interface{}
		args      []interface{}
		expect    interface{}
		expectErr bool
	}

	cases := []tc{
		{name: "nil", handler: nil, expectErr: true},
		{name: "noargs_interface", handler: func() interface{} { return "ok" }, expect: "ok"},
		{name: "noargs_string", handler: func() string { return "s" }, expect: "s"},
		{name: "noargs_int64", handler: func() int64 { return 7 }, expect: int64(7)},
		{name: "noargs_float64", handler: func() float64 { return 1.5 }, expect: 1.5},
		{name: "one_interface", handler: func(v interface{}) interface{} { return v }, args: []interface{}{"x"}, expect: "x"},
		{name: "one_list_interface", handler: func(v []interface{}) interface{} { return len(v) }, args: []interface{}{[]interface{}{1, 2}}, expect: 2},
		{name: "one_list_float", handler: func(v []interface{}) float64 { return float64(len(v)) }, args: []interface{}{[]interface{}{1, 2, 3}}, expect: 3.0},
		{name: "one_string", handler: func(s string) string { return s + "!" }, args: []interface{}{"a"}, expect: "a!"},
		{name: "one_float", handler: func(f float64) float64 { return f * 2 }, args: []interface{}{int64(3)}, expect: 6.0},
		{name: "one_float_slice", handler: func(v []float64) []float64 { return append(v, 9) }, args: []interface{}{[]interface{}{1.0, 2.0}}, expect: []float64{1, 2, 9}},
		{name: "two_interface", handler: func(a, b interface{}) interface{} { return fmt.Sprintf("%v-%v", a, b) }, args: []interface{}{"a", "b"}, expect: "a-b"},
		{name: "two_list_item", handler: func(v []interface{}, x interface{}) bool { return len(v) == 1 && x == "k" }, args: []interface{}{[]interface{}{1}, "k"}, expect: true},
		{name: "two_lists", handler: func(a, b []interface{}) []interface{} { return append(a, b...) }, args: []interface{}{[]interface{}{1}, []interface{}{2}}, expect: []interface{}{1, 2}},
		{name: "two_strings", handler: func(a, b string) string { return a + b }, args: []interface{}{"a", "b"}, expect: "ab"},
		{name: "two_strings_int", handler: func(a, b string) int { return len(a) + len(b) }, args: []interface{}{"a", "bb"}, expect: 3},
		{name: "two_strings_float", handler: func(a, b string) float64 { return float64(len(a) * len(b)) }, args: []interface{}{"aa", "bbb"}, expect: 6.0},
		{name: "two_float_slices", handler: func(a, b []float64) float64 { return a[0] + b[0] }, args: []interface{}{[]interface{}{1.0}, []interface{}{2.0}}, expect: 3.0},
		{name: "three_strings", handler: func(a, b, c string) string { return a + b + c }, args: []interface{}{"a", "b", "c"}, expect: "abc"},
		{name: "unsupported", handler: func(int) int { return 1 }, args: []interface{}{1}, expectErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := callPluginHandler(c.handler, c.args)
			if c.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.expect, got)
		})
	}
}

func TestCypherHelpers_SubstituteNodeAndExecuteSetMerge(t *testing.T) {
	base := storage.NewMemoryEngine()
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	node := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}}
	_, err := eng.CreateNode(node)
	require.NoError(t, err)

	// substituteNodeInSubquery
	sub := exec.substituteNodeInSubquery("MATCH (n)-[:KNOWS]->(m) RETURN n.name", "n", node)
	assert.Contains(t, sub, "(n1)-[:KNOWS]->")
	sub = exec.substituteNodeInSubquery("MATCH (n:Person)-[:KNOWS]->(m) RETURN n.name", "n", node)
	assert.Contains(t, sub, "(n1:Person)-[:KNOWS]->")

	matchResult := &ExecuteResult{
		Columns: []string{"n", "props"},
		Rows:    [][]interface{}{{node, map[string]interface{}{"city": "NYC"}}},
	}
	out := &ExecuteResult{Stats: &QueryStats{}}
	_, err = exec.executeSetMerge(ctx, matchResult, "n += {country: 'US'}", out, "", -1)
	require.NoError(t, err)
	assert.Equal(t, "US", node.Properties["country"])

	// map variable path
	out = &ExecuteResult{Stats: &QueryStats{}}
	_, err = exec.executeSetMerge(ctx, matchResult, "n += props", out, "", -1)
	require.NoError(t, err)
	assert.Equal(t, "NYC", node.Properties["city"])

	// parameter map path
	ctxWithParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"p": map[string]interface{}{"age": int(41)}})
	out = &ExecuteResult{Stats: &QueryStats{}}
	_, err = exec.executeSetMerge(ctxWithParams, matchResult, "n += $p", out, "", -1)
	require.NoError(t, err)
	assert.Equal(t, int64(41), node.Properties["age"])

	// Error branches.
	_, err = exec.executeSetMerge(ctx, matchResult, "n = {x:1}", &ExecuteResult{Stats: &QueryStats{}}, "", -1)
	require.Error(t, err)
	_, err = exec.executeSetMerge(ctx, matchResult, "n += $", &ExecuteResult{Stats: &QueryStats{}}, "", -1)
	require.Error(t, err)
	_, err = exec.executeSetMerge(ctx, matchResult, "n += $missing", &ExecuteResult{Stats: &QueryStats{}}, "", -1)
	require.Error(t, err)
	_, err = exec.executeSetMerge(ctx, &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{node}}}, "n += props", &ExecuteResult{Stats: &QueryStats{}}, "", -1)
	require.Error(t, err)

	// normalizePropsMap / normalizePropValue branches
	props, err := normalizePropsMap(map[interface{}]interface{}{"a": int(1), "b": uint8(2), "c": float32(3.5), "d": []interface{}{int8(1), uint16(2)}}, "var props")
	require.NoError(t, err)
	assert.Equal(t, int64(1), props["a"])
	assert.Equal(t, int64(2), props["b"])
	assert.Equal(t, float64(3.5), props["c"])
	_, err = normalizePropsMap(map[interface{}]interface{}{1: "x"}, "bad")
	require.Error(t, err)
	_, err = normalizePropsMap("not-map", "bad")
	require.Error(t, err)
}
