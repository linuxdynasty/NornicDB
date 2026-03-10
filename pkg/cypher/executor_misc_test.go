// Package cypher provides tests for the Cypher executor.
package cypher

import (
	"context"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallDbLabelsWithError(t *testing.T) {
	// This is tricky - MemoryEngine doesn't error on AllNodes
	// Just verify normal behavior
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "label-test",
		Labels:     []string{"TestLabel", "SecondLabel"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestResolveReturnItemWithCount(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "count-ri",
		Labels:     []string{"CountRI"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// This triggers resolveReturnItem with COUNT prefix in non-aggregation path
	result, err := exec.Execute(ctx, "MATCH (n:CountRI) RETURN count(*)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Rows[0][0])
}

// Tests for toFloat64 type coverage
func TestToFloat64TypeCoverage(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test float32 through comparison
	node1 := &storage.Node{
		ID:         "f32-test",
		Labels:     []string{"Float32Test"},
		Properties: map[string]interface{}{"val": float32(3.14)},
	}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:Float32Test) WHERE n.val > 3.0 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test int through SUM aggregation
	node2 := &storage.Node{
		ID:         "int-test",
		Labels:     []string{"IntTest"},
		Properties: map[string]interface{}{"val": int(100)},
	}
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:IntTest) RETURN sum(n.val)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(100), result.Rows[0][0])

	// Test int32 through AVG
	node3 := &storage.Node{
		ID:         "i32-test",
		Labels:     []string{"Int32Test"},
		Properties: map[string]interface{}{"val": int32(50)},
	}
	_, err = store.CreateNode(node3)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:Int32Test) RETURN avg(n.val)", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(50), result.Rows[0][0])

	// Test string value - Neo4j ignores non-numeric values in SUM
	node4 := &storage.Node{
		ID:         "str-num",
		Labels:     []string{"StrNumTest"},
		Properties: map[string]interface{}{"val": "42.5"},
	}
	_, err = store.CreateNode(node4)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:StrNumTest) RETURN sum(n.val)", nil)
	require.NoError(t, err)
	// String values are not numeric, so SUM ignores them and returns 0
	assert.Equal(t, int64(0), result.Rows[0][0])
}

// Test Parser MERGE clause
func TestParseMerge(t *testing.T) {
	parser := NewParser()
	query, err := parser.Parse("MERGE (n:Person {name: 'Alice'})")
	require.NoError(t, err)
	assert.NotNil(t, query)
	// MERGE is currently parsed but treated as CREATE internally
}

// Test all WHERE operators exercise evaluateWhere fully
func TestEvaluateWhereFullCoverage(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:     "where-full",
		Labels: []string{"WhereFull"},
		Properties: map[string]interface{}{
			"name":   "Alice Smith",
			"age":    float64(30),
			"active": true,
			"score":  float64(85.5),
		},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Test >= operator
	result, err := exec.Execute(ctx, "MATCH (n:WhereFull) WHERE n.age >= 30 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test <= operator
	result, err = exec.Execute(ctx, "MATCH (n:WhereFull) WHERE n.age <= 30 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// Test edge cases in splitNodePatterns
func TestSplitNodePatternsEdgeCases(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Empty pattern after splitting
	result, err := exec.Execute(ctx, "CREATE (a:A)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

// Test evaluateStringOp edge cases
func TestEvaluateStringOpEdgeCases(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "str-edge",
		Labels:     []string{"StrEdge"},
		Properties: map[string]interface{}{"text": "hello world"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// CONTAINS that matches
	result, err := exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text CONTAINS 'world' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// CONTAINS that doesn't match
	result, err = exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text CONTAINS 'xyz' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// STARTS WITH match
	result, err = exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text STARTS WITH 'hello' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// ENDS WITH match
	result, err = exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text ENDS WITH 'world' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// Test evaluateInOp edge cases
func TestEvaluateInOpMatch(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "in-match",
		Labels:     []string{"InMatch"},
		Properties: map[string]interface{}{"status": "pending"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// IN with matching value
	result, err := exec.Execute(ctx, "MATCH (n:InMatch) WHERE n.status IN ['active', 'pending'] RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// IN with literal on the left and list property on the right
	node2 := &storage.Node{
		ID:     "in-list-prop",
		Labels: []string{"InMatch"},
		Properties: map[string]interface{}{
			"file_tags": []interface{}{"github", "argoCD"},
		},
	}
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:InMatch) WHERE 'github' IN n.file_tags RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// Test Parser default case in Parse
func TestParserDefaultCase(t *testing.T) {
	parser := NewParser()

	// Query with tokens that aren't recognized keywords
	query, err := parser.Parse("MATCH (n) RETURN n")
	require.NoError(t, err)
	assert.NotNil(t, query)
}

func TestApocDynamicRunAndRunMany_Direct(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:         "apoc-dyn-1",
		Labels:     []string{"Dyn"},
		Properties: map[string]interface{}{"name": "a"},
	})
	require.NoError(t, err)

	_, err = exec.callApocCypherRun(ctx, "CALL apoc.cypher.bogus('RETURN 1', {})")
	require.Error(t, err)
	_, err = exec.callApocCypherRun(ctx, "CALL apoc.cypher.run")
	require.Error(t, err)
	_, err = exec.callApocCypherRun(ctx, "CALL apoc.cypher.run('RETURN 1', {}")
	require.Error(t, err)

	res, err := exec.callApocCypherRun(ctx, "CALL apoc.cypher.run('MATCH (n:Dyn) RETURN count(n) AS c', {})")
	require.NoError(t, err)
	require.Equal(t, []string{"value"}, res.Columns)
	require.Len(t, res.Rows, 1)
	valueMap, ok := res.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	_, hasC := valueMap["c"]
	require.True(t, hasC)

	_, err = exec.callApocCypherRunMany(ctx, "CALL apoc.cypher.bogusMany('RETURN 1', {})")
	require.Error(t, err)
	_, err = exec.callApocCypherRunMany(ctx, "CALL apoc.cypher.runMany")
	require.Error(t, err)

	res, err = exec.callApocCypherRunMany(ctx, "CALL apoc.cypher.runMany('RETURN 1 AS n; INVALID CYPHER; RETURN 2 AS n', {})")
	require.NoError(t, err)
	require.Equal(t, []string{"row", "result"}, res.Columns)
	require.NotEmpty(t, res.Rows)
}

func TestApocPeriodicIterateAndCommit_Direct(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.callApocPeriodicIterate(ctx, "CALL apoc.periodic.nope('RETURN 1','RETURN 1',{})")
	require.Error(t, err)
	_, err = exec.callApocPeriodicIterate(ctx, "CALL apoc.periodic.iterate")
	require.Error(t, err)
	_, err = exec.callApocPeriodicIterate(ctx, "CALL apoc.periodic.iterate('RETURN 1','RETURN 1',{}")
	require.Error(t, err)

	res, err := exec.callApocPeriodicIterate(ctx, "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS i RETURN i','CREATE (:Iter {v: i})',{batchSize:2})")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(3), res.Rows[0][1]) // total

	res, err = exec.callApocPeriodicIterate(ctx, "CALL apoc.periodic.rock_n_roll('UNWIND [4,5] AS i RETURN i','CREATE (:Iter {v: i})',{batchSize:1})")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	_, err = exec.callApocPeriodicCommit(ctx, "CALL apoc.periodic.nope('MATCH (n) RETURN n', {})")
	require.Error(t, err)
	_, err = exec.callApocPeriodicCommit(ctx, "CALL apoc.periodic.commit")
	require.Error(t, err)
	_, err = exec.callApocPeriodicCommit(ctx, "CALL apoc.periodic.commit('MATCH (n) RETURN n', {}")
	require.Error(t, err)

	res, err = exec.callApocPeriodicCommit(ctx, "CALL apoc.periodic.commit('MATCH (n:Nothing) RETURN n', {limit: 10})")
	require.NoError(t, err)
	require.Equal(t, []string{"updates", "executions", "runtime", "batches"}, res.Columns)
	require.Len(t, res.Rows, 1)
}

func TestDbTxlogProcedures_ErrorBranches(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.callDbTxlogEntries(ctx, "CALL db.txlog.bad(1,2)")
	require.Error(t, err)
	_, err = exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(1,2")
	require.Error(t, err)
	_, err = exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(abc,2)")
	require.Error(t, err)
	_, err = exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(0,2)")
	require.Error(t, err)
	_, err = exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(2,1)")
	require.Error(t, err)
	_, err = exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(1,2)")
	require.Error(t, err) // memory engine -> WAL not available

	_, err = exec.callDbTxlogByTxID(ctx, "CALL db.txlog.bad('x',10)")
	require.Error(t, err)
	_, err = exec.callDbTxlogByTxID(ctx, "CALL db.txlog.byTxId('x',10")
	require.Error(t, err)
	_, err = exec.callDbTxlogByTxID(ctx, "CALL db.txlog.byTxId('',10)")
	require.Error(t, err)
	_, err = exec.callDbTxlogByTxID(ctx, "CALL db.txlog.byTxId('tx-1',10)")
	require.Error(t, err) // memory engine -> WAL not available
}

func TestDbTxlogProcedures_WithWALStack(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cypher-txlog-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	badger, err := storage.NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badger.Close()

	wal, err := storage.NewWAL(tmpDir+"/wal", nil)
	require.NoError(t, err)
	defer wal.Close()

	walEngine := storage.NewWALEngine(badger, wal)
	asyncEngine := storage.NewAsyncEngine(walEngine, nil)
	defer asyncEngine.Close()

	store := storage.NewNamespacedEngine(asyncEngine, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	beginSeq, err := wal.AppendTxBegin("test", "tx-test-1", map[string]string{"app": "cypher-test"})
	require.NoError(t, err)
	_, err = wal.AppendTxCommit("test", "tx-test-1", 2)
	require.NoError(t, err)
	require.Greater(t, beginSeq, uint64(0))
	require.NoError(t, wal.Sync())

	_, err = exec.Execute(ctx, `CREATE (n:TxLog {name: "one"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:TxLog {name: "two"})`, nil)
	require.NoError(t, err)

	entriesRes, err := exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(1, 0)")
	require.NoError(t, err)
	require.NotEmpty(t, entriesRes.Rows)
	var txID string
	for _, row := range entriesRes.Rows {
		if len(row) > 3 {
			if s, ok := row[3].(string); ok && s != "" {
				txID = s
				break
			}
		}
	}
	require.NotEmpty(t, txID)

	byTxIDRes, err := exec.callDbTxlogByTxID(ctx, "CALL db.txlog.byTxId('tx-test-1', 10)")
	require.NoError(t, err)
	require.NotEmpty(t, byTxIDRes.Rows)
	for _, row := range byTxIDRes.Rows {
		require.GreaterOrEqual(t, len(row), 4)
		assert.Equal(t, "tx-test-1", row[3])
	}
}

// =============================================================================
// Tests for Parameter Substitution (substituteParams and valueToLiteral)
// =============================================================================

func TestSubstituteParamsBasic(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name     string
		query    string
		params   map[string]interface{}
		expected string
	}{
		{
			name:     "string parameter",
			query:    "MATCH (n {name: $name}) RETURN n",
			params:   map[string]interface{}{"name": "Alice"},
			expected: "MATCH (n {name: 'Alice'}) RETURN n",
		},
		{
			name:     "integer parameter",
			query:    "MATCH (n) WHERE n.age = $age RETURN n",
			params:   map[string]interface{}{"age": 25},
			expected: "MATCH (n) WHERE n.age = 25 RETURN n",
		},
		{
			name:     "float parameter",
			query:    "MATCH (n) WHERE n.score > $score RETURN n",
			params:   map[string]interface{}{"score": 85.5},
			expected: "MATCH (n) WHERE n.score > 85.5 RETURN n",
		},
		{
			name:     "boolean parameter true",
			query:    "MATCH (n) WHERE n.active = $active RETURN n",
			params:   map[string]interface{}{"active": true},
			expected: "MATCH (n) WHERE n.active = true RETURN n",
		},
		{
			name:     "boolean parameter false",
			query:    "MATCH (n) WHERE n.active = $active RETURN n",
			params:   map[string]interface{}{"active": false},
			expected: "MATCH (n) WHERE n.active = false RETURN n",
		},
		{
			name:     "null parameter",
			query:    "MATCH (n) WHERE n.value = $value RETURN n",
			params:   map[string]interface{}{"value": nil},
			expected: "MATCH (n) WHERE n.value = null RETURN n",
		},
		{
			name:     "multiple parameters",
			query:    "MATCH (n {name: $name, age: $age}) RETURN n",
			params:   map[string]interface{}{"name": "Bob", "age": 30},
			expected: "MATCH (n {name: 'Bob', age: 30}) RETURN n",
		},
		{
			name:     "missing parameter unchanged",
			query:    "MATCH (n {name: $name}) RETURN n",
			params:   map[string]interface{}{},
			expected: "MATCH (n {name: $name}) RETURN n",
		},
		{
			name:     "empty params",
			query:    "MATCH (n) RETURN n",
			params:   nil,
			expected: "MATCH (n) RETURN n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.substituteParams(tt.query, tt.params)
			assert.Equal(t, tt.expected, result)
		})
	}

	// Verify queries execute correctly after substitution
	_, err := exec.Execute(ctx, "CREATE (n:ParamTest {name: 'Test'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:ParamTest {name: $name}) RETURN n", map[string]interface{}{"name": "Test"})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestSubstituteParamsStringEscaping(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{
			name:     "single quote escaping",
			value:    "O'Connor",
			expected: "'O''Connor'",
		},
		{
			name:     "backslash escaping",
			value:    "path\\to\\file",
			expected: "'path\\\\to\\\\file'",
		},
		{
			name:     "both quotes and backslashes",
			value:    "It's a\\path",
			expected: "'It''s a\\\\path'",
		},
		{
			name:     "empty string",
			value:    "",
			expected: "''",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.valueToLiteral(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToLiteralArrays(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "string array",
			value:    []string{"a", "b", "c"},
			expected: "['a', 'b', 'c']",
		},
		{
			name:     "int array",
			value:    []int{1, 2, 3},
			expected: "[1, 2, 3]",
		},
		{
			name:     "int64 array",
			value:    []int64{100, 200, 300},
			expected: "[100, 200, 300]",
		},
		{
			name:     "float64 array",
			value:    []float64{1.5, 2.5, 3.5},
			expected: "[1.5, 2.5, 3.5]",
		},
		{
			name:     "interface array",
			value:    []interface{}{"hello", 42, true},
			expected: "['hello', 42, true]",
		},
		{
			name:     "empty array",
			value:    []interface{}{},
			expected: "[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.valueToLiteral(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToLiteralMaps(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// Map with single key (deterministic)
	result := exec.valueToLiteral(map[string]interface{}{"name": "Alice"})
	assert.Equal(t, "{name: 'Alice'}", result)

	// Empty map
	result = exec.valueToLiteral(map[string]interface{}{})
	assert.Equal(t, "{}", result)
}

func TestValueToLiteralIntegerTypes(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"int", int(42), "42"},
		{"int8", int8(8), "8"},
		{"int16", int16(16), "16"},
		{"int32", int32(32), "32"},
		{"int64", int64(64), "64"},
		{"uint", uint(100), "100"},
		{"uint8", uint8(8), "8"},
		{"uint16", uint16(16), "16"},
		{"uint32", uint32(32), "32"},
		{"uint64", uint64(64), "64"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.valueToLiteral(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToLiteralFloats(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// float32
	result := exec.valueToLiteral(float32(3.14))
	assert.Contains(t, result, "3.14")

	// float64
	result = exec.valueToLiteral(float64(2.718281828))
	assert.Contains(t, result, "2.718")
}

// =============================================================================
// Tests for RETURN Clause Parsing
// =============================================================================

func TestParseReturnClauseBasic(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	node := &storage.Node{
		ID:         "ret-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}

	tests := []struct {
		name            string
		returnClause    string
		varName         string
		expectedCols    []string
		expectedValFunc func([]interface{}) bool
	}{
		{
			name:         "return star",
			returnClause: "*",
			varName:      "n",
			expectedCols: []string{"n"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] != nil
			},
		},
		{
			name:         "return property with alias",
			returnClause: "n.name AS personName",
			varName:      "n",
			expectedCols: []string{"personName"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] == "Alice"
			},
		},
		{
			name:         "return property without alias",
			returnClause: "n.age",
			varName:      "n",
			expectedCols: []string{"age"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] == float64(30)
			},
		},
		{
			name:         "return id function",
			returnClause: "id(n) AS node_id",
			varName:      "n",
			expectedCols: []string{"node_id"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] == "ret-1"
			},
		},
		{
			name:         "return multiple expressions",
			returnClause: "n.name AS name, n.age AS age, id(n) AS id",
			varName:      "n",
			expectedCols: []string{"name", "age", "id"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 3 && vals[0] == "Alice" && vals[1] == float64(30) && vals[2] == "ret-1"
			},
		},
		{
			name:         "return variable only",
			returnClause: "n",
			varName:      "n",
			expectedCols: []string{"n"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] != nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cols, vals := exec.parseReturnClause(tt.returnClause, tt.varName, node)
			assert.Equal(t, tt.expectedCols, cols)
			assert.True(t, tt.expectedValFunc(vals), "Value validation failed for %v", vals)
		})
	}
}

func TestSplitReturnExpressions(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		clause   string
		expected []string
	}{
		{
			name:     "single expression",
			clause:   "n.name",
			expected: []string{"n.name"},
		},
		{
			name:     "multiple simple expressions",
			clause:   "n.name, n.age, n.city",
			expected: []string{"n.name", " n.age", " n.city"},
		},
		{
			name:     "expression with function",
			clause:   "id(n), n.name",
			expected: []string{"id(n)", " n.name"},
		},
		{
			name:     "nested parentheses",
			clause:   "count(n), sum(n.age)",
			expected: []string{"count(n)", " sum(n.age)"},
		},
		{
			name:     "complex function call",
			clause:   "collect(n.name), count(*)",
			expected: []string{"collect(n.name)", " count(*)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.splitReturnExpressions(tt.clause)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExpressionToAlias(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		expr     string
		expected string
	}{
		{"property access", "n.name", "name"},
		{"nested property", "n.address.city", "city"},
		{"function call", "id(n)", "id(n)"},
		{"simple variable", "n", "n"},
		{"literal", "'hello'", "'hello'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.expressionToAlias(tt.expr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluateExpression(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	node := &storage.Node{
		ID:     "eval-1",
		Labels: []string{"Test"},
		Properties: map[string]interface{}{
			"name":   "Test Node",
			"count":  float64(42),
			"active": true,
		},
	}

	tests := []struct {
		name     string
		expr     string
		varName  string
		expected interface{}
	}{
		{"id function", "id(n)", "n", "eval-1"},
		{"id function with spaces", "id( n )", "n", "eval-1"},
		{"property access", "n.name", "n", "Test Node"},
		{"numeric property", "n.count", "n", float64(42)},
		{"boolean property", "n.active", "n", true},
		{"missing property", "n.missing", "n", nil},
		{"string literal", "'hello'", "n", "hello"},
		{"integer literal", "42", "n", int64(42)},
		{"float literal", "3.14", "n", float64(3.14)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.evaluateExpression(tt.expr, tt.varName, node)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Tests for MERGE with ON CREATE SET / ON MATCH SET
// =============================================================================

func TestMergeWithOnCreateSet(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MERGE a new node with ON CREATE SET
	result, err := exec.Execute(ctx, `
		MERGE (n:Person {name: 'Alice'})
		ON CREATE SET n.created = 'yes', n.age = 25
		RETURN n
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Len(t, result.Rows, 1)

	// Verify node was created with properties
	matchResult, err := exec.Execute(ctx, "MATCH (n:Person {name: 'Alice'}) RETURN n.created, n.age", nil)
	require.NoError(t, err)
	assert.Len(t, matchResult.Rows, 1)
}

func TestMergeWithOnMatchSet(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First create a node
	_, err := exec.Execute(ctx, "CREATE (n:Person {name: 'Bob', visits: 0})", nil)
	require.NoError(t, err)

	// MERGE existing node with ON MATCH SET
	result, err := exec.Execute(ctx, `
		MERGE (n:Person {name: 'Bob'})
		ON MATCH SET n.visits = 1
		RETURN n
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Stats.NodesCreated) // Should not create new node
}

func TestMergeRouting(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MERGE with ON CREATE SET should NOT be routed to executeSet
	result, err := exec.Execute(ctx, `
		MERGE (n:File {path: '/test/file.txt'})
		ON CREATE SET n.created = 'true'
		RETURN n.path AS path
	`, nil)
	require.NoError(t, err)
	assert.Len(t, result.Columns, 1)
	assert.Equal(t, "path", result.Columns[0])
}

func TestMergeWithParameterSubstitution(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"path":      "/app/docs/README.md",
		"name":      "README.md",
		"size":      int64(1024),
		"extension": ".md",
	}

	result, err := exec.Execute(ctx, `
		MERGE (f:File {path: $path})
		ON CREATE SET f.name = $name, f.size = $size, f.extension = $extension
		RETURN f.path AS path, f.name AS name
	`, params)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Len(t, result.Columns, 2)
	assert.Contains(t, result.Columns, "path")
	assert.Contains(t, result.Columns, "name")
}

func TestMergeReturnIdFunction(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		MERGE (f:File {path: '/test.txt'})
		RETURN f.path AS path, id(f) AS node_id
	`, nil)
	require.NoError(t, err)
	assert.Len(t, result.Columns, 2)
	assert.Equal(t, "path", result.Columns[0])
	assert.Equal(t, "node_id", result.Columns[1])

	// node_id should be a string
	if len(result.Rows) > 0 {
		assert.IsType(t, "", result.Rows[0][1])
	}
}

// =============================================================================
// Tests for Extract Helper Functions
// =============================================================================

func TestExtractVarName(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{"simple var with label", "(n:Person)", "n"},
		{"var with multiple labels", "(f:File:Node)", "f"},
		{"var with properties", "(n:Person {name: 'Alice'})", "n"},
		{"var only", "(n)", "n"},
		{"no var, label only", "(:Person)", "n"}, // Default
		{"empty pattern", "()", "n"},             // Default
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.extractVarName(tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractLabels(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		pattern  string
		expected []string
	}{
		{"single label", "(n:Person)", []string{"Person"}},
		{"multiple labels", "(f:File:Node)", []string{"File", "Node"}},
		{"label with properties", "(n:Person {name: 'Alice'})", []string{"Person"}},
		{"no label", "(n)", []string{}},
		{"no var, label only", "(:Person)", []string{"Person"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.extractLabels(tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Tests for DROP INDEX (No-op)
// =============================================================================

func TestDropIndexNoOp(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// DROP INDEX should be treated as no-op (returns empty result, no error)
	result, err := exec.Execute(ctx, "DROP INDEX file_path IF EXISTS", nil)
	require.NoError(t, err)
	assert.Empty(t, result.Columns)
	assert.Empty(t, result.Rows)
}

// =============================================================================
// Tests for Edge Cases in Parameter Substitution
// =============================================================================

func TestSubstituteParamsEdgeCases(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name   string
		query  string
		params map[string]interface{}
	}{
		{
			name:   "parameter at start",
			query:  "$name is the name",
			params: map[string]interface{}{"name": "test"},
		},
		{
			name:   "parameter at end",
			query:  "The name is $name",
			params: map[string]interface{}{"name": "test"},
		},
		{
			name:   "adjacent parameters",
			query:  "Values: $a$b",
			params: map[string]interface{}{"a": "x", "b": "y"},
		},
		{
			name:   "underscore in param name",
			query:  "Path: $host_path",
			params: map[string]interface{}{"host_path": "/app/docs"},
		},
		{
			name:   "number in param name",
			query:  "Value: $param123",
			params: map[string]interface{}{"param123": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.substituteParams(tt.query, tt.params)
			// Should not contain the original parameter placeholders
			for key := range tt.params {
				assert.NotContains(t, result, "$"+key)
			}
		})
	}
}

func TestSubstituteParamsComplexQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test with a complex MERGE query like Mimir uses
	query := `
		MERGE (f:File:Node {path: $path})
		ON CREATE SET f.id = 'file-123',
			f.host_path = $host_path,
			f.name = $name,
			f.extension = $extension,
			f.size_bytes = $size_bytes,
			f.content = $content
		RETURN f.path AS path, f.size_bytes AS size_bytes, id(f) AS node_id
	`

	params := map[string]interface{}{
		"path":       "/app/docs/README.md",
		"host_path":  "/Users/dev/docs/README.md",
		"name":       "README.md",
		"extension":  ".md",
		"size_bytes": int64(2048),
		"content":    "# Hello World\n\nThis is a test file.",
	}

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	assert.Len(t, result.Columns, 3)
	assert.Contains(t, result.Columns, "path")
	assert.Contains(t, result.Columns, "size_bytes")
	assert.Contains(t, result.Columns, "node_id")

	if len(result.Rows) > 0 {
		assert.Equal(t, "/app/docs/README.md", result.Rows[0][0])
	}
}

// TestRelationshipCountAggregation tests that COUNT(r) properly aggregates
// all relationships instead of returning 1 (the bug that was fixed)
func TestRelationshipCountAggregation(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a small graph with multiple relationships
	// 3 products, 2 categories, 2 suppliers
	// Each product has 2 relationships (PART_OF category, SUPPLIES from supplier)
	// Total: 6 relationships

	// Create categories
	cat1 := &storage.Node{ID: "cat1", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryID": int64(1), "name": "Beverages"}}
	cat2 := &storage.Node{ID: "cat2", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryID": int64(2), "name": "Condiments"}}
	_, err := store.CreateNode(cat1)
	require.NoError(t, err)
	_, err = store.CreateNode(cat2)
	require.NoError(t, err)

	// Create suppliers
	sup1 := &storage.Node{ID: "sup1", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"supplierID": int64(1), "name": "Exotic Liquids"}}
	sup2 := &storage.Node{ID: "sup2", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"supplierID": int64(2), "name": "New Orleans"}}
	_, err = store.CreateNode(sup1)
	require.NoError(t, err)
	_, err = store.CreateNode(sup2)
	require.NoError(t, err)

	// Create products
	prod1 := &storage.Node{ID: "prod1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productID": int64(1), "name": "Chai"}}
	prod2 := &storage.Node{ID: "prod2", Labels: []string{"Product"}, Properties: map[string]interface{}{"productID": int64(2), "name": "Chang"}}
	prod3 := &storage.Node{ID: "prod3", Labels: []string{"Product"}, Properties: map[string]interface{}{"productID": int64(3), "name": "Aniseed Syrup"}}
	_, err = store.CreateNode(prod1)
	require.NoError(t, err)
	_, err = store.CreateNode(prod2)
	require.NoError(t, err)
	_, err = store.CreateNode(prod3)
	require.NoError(t, err)

	// Create relationships
	// Product 1: Beverages category, Supplier 1
	edge1 := &storage.Edge{ID: "e1", StartNode: "prod1", EndNode: "cat1", Type: "PART_OF"}
	edge2 := &storage.Edge{ID: "e2", StartNode: "sup1", EndNode: "prod1", Type: "SUPPLIES"}
	require.NoError(t, store.CreateEdge(edge1))
	require.NoError(t, store.CreateEdge(edge2))

	// Product 2: Beverages category, Supplier 1
	edge3 := &storage.Edge{ID: "e3", StartNode: "prod2", EndNode: "cat1", Type: "PART_OF"}
	edge4 := &storage.Edge{ID: "e4", StartNode: "sup1", EndNode: "prod2", Type: "SUPPLIES"}
	require.NoError(t, store.CreateEdge(edge3))
	require.NoError(t, store.CreateEdge(edge4))

	// Product 3: Condiments category, Supplier 2
	edge5 := &storage.Edge{ID: "e5", StartNode: "prod3", EndNode: "cat2", Type: "PART_OF"}
	edge6 := &storage.Edge{ID: "e6", StartNode: "sup2", EndNode: "prod3", Type: "SUPPLIES"}
	require.NoError(t, store.CreateEdge(edge5))
	require.NoError(t, store.CreateEdge(edge6))

	// Test 1: Count all relationships
	t.Run("count all relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1, "Should return single aggregated row")
		require.Len(t, result.Rows[0], 1, "Should return single column")

		count := result.Rows[0][0]
		assert.Equal(t, int64(6), count, "Should count all 6 relationships, not return 1")
	})

	// Test 2: Count relationships by type
	t.Run("count PART_OF relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r:PART_OF]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(3), count, "Should count 3 PART_OF relationships")
	})

	t.Run("count SUPPLIES relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r:SUPPLIES]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(3), count, "Should count 3 SUPPLIES relationships")
	})

	// Test 3: Count with wildcard (COUNT(*))
	t.Run("count with wildcard", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(*) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(6), count, "COUNT(*) should count all 6 relationships")
	})

	// Test 4: Verify non-aggregation still works (should return all rows)
	t.Run("non-aggregation returns all relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN type(r) as relType", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 6, "Non-aggregation should return all 6 relationship rows")
	})

	// Test 5: Count with GROUP BY (implicit grouping by type)
	t.Run("count grouped by type", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN type(r) as relType, count(*) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		// Should group by type: PART_OF (3) and SUPPLIES (3) = 2 groups
		assert.Len(t, result.Rows, 2, "Should return 2 groups (PART_OF and SUPPLIES)")

		// Verify counts
		for _, row := range result.Rows {
			relType := row[0].(string)
			count := row[1].(int64)
			assert.Equal(t, int64(3), count, "Each type should have count of 3")
			assert.Contains(t, []string{"PART_OF", "SUPPLIES"}, relType)
		}
	})

	// Test 6: Empty result aggregation
	t.Run("count with no matches", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r:NONEXISTENT]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(0), count, "COUNT should return 0 for no matches")
	})
}

// ========================================
// SET Label Tests - SET n:Label syntax
// ========================================

func TestSetLabelSyntax(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node without the target label
	node := &storage.Node{
		ID:         "label-test-1",
		Labels:     []string{"File"},
		Properties: map[string]interface{}{"path": "/test/file.txt"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	t.Run("SET single label", func(t *testing.T) {
		// Add Node label using SET n:Label syntax
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file.txt'})
			SET f:Node
			RETURN f
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.LabelsAdded)

		// Verify the node now has both labels
		updatedNode, err := store.GetNode("label-test-1")
		require.NoError(t, err)
		assert.Contains(t, updatedNode.Labels, "File")
		assert.Contains(t, updatedNode.Labels, "Node")
	})

	t.Run("SET label idempotent", func(t *testing.T) {
		// Adding same label again should not increase count
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file.txt'})
			SET f:Node
			RETURN f
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, result.Stats.LabelsAdded, "Should not add duplicate label")
	})
}

func TestSetLabelWithPropertyAssignment(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "combo-test-1",
		Labels:     []string{"Document"},
		Properties: map[string]interface{}{"name": "readme"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	t.Run("SET label and property together", func(t *testing.T) {
		// SET f:Node, f.type = 'file' - combining label and property
		result, err := exec.Execute(ctx, `
			MATCH (d:Document {name: 'readme'})
			SET d:Indexed, d.type = 'file'
			RETURN d
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.LabelsAdded)
		assert.Equal(t, 1, result.Stats.PropertiesSet)

		// Verify both label and property were set
		updatedNode, err := store.GetNode("combo-test-1")
		require.NoError(t, err)
		assert.Contains(t, updatedNode.Labels, "Document")
		assert.Contains(t, updatedNode.Labels, "Indexed")
		assert.Equal(t, "file", updatedNode.Properties["type"])
	})
}

func TestSetMultipleLabels(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "multi-label-1",
		Labels:     []string{"Base"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	t.Run("SET multiple labels in sequence", func(t *testing.T) {
		// First add one label
		_, err := exec.Execute(ctx, `MATCH (n:Base) SET n:First RETURN n`, nil)
		require.NoError(t, err)

		// Then add another
		result, err := exec.Execute(ctx, `MATCH (n:Base) SET n:Second RETURN n`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.LabelsAdded)

		// Verify all labels present
		updatedNode, err := store.GetNode("multi-label-1")
		require.NoError(t, err)
		assert.Contains(t, updatedNode.Labels, "Base")
		assert.Contains(t, updatedNode.Labels, "First")
		assert.Contains(t, updatedNode.Labels, "Second")
	})
}

// ========================================
// WHERE Label Check Tests - WHERE n:Label and WHERE NOT n:Label
// ========================================

func TestWhereLabelCheck(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test nodes with different labels
	nodes := []*storage.Node{
		{ID: "person-1", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: "person-2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
		{ID: "company-1", Labels: []string{"Company"}, Properties: map[string]interface{}{"name": "Acme"}},
	}
	for _, n := range nodes {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	t.Run("WHERE n:Label filters correctly", func(t *testing.T) {
		// Match only Person nodes that are also Employees
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE p:Employee
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Alice", result.Rows[0][0])
	})

	t.Run("WHERE NOT n:Label excludes correctly", func(t *testing.T) {
		// Match Person nodes that are NOT Employees
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE NOT p:Employee
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Bob", result.Rows[0][0])
	})
}

func TestWhereNotLabelMigrationPattern(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// This tests the exact pattern from the Mimir schema initialization:
	// MATCH (f:File) WHERE NOT f:Node SET f:Node, f.type = 'file'

	// Create File nodes - some with Node label, some without
	nodes := []*storage.Node{
		{ID: "file-1", Labels: []string{"File"}, Properties: map[string]interface{}{"path": "/a.txt"}},
		{ID: "file-2", Labels: []string{"File", "Node"}, Properties: map[string]interface{}{"path": "/b.txt"}},
		{ID: "file-3", Labels: []string{"File"}, Properties: map[string]interface{}{"path": "/c.txt"}},
	}
	for _, n := range nodes {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	t.Run("migration adds label only to nodes missing it", func(t *testing.T) {
		// Run the migration pattern
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WHERE NOT f:Node
			SET f:Node, f.type = 'file'
		`, nil)
		require.NoError(t, err)
		// Should add Node label to file-1 and file-3, not file-2
		assert.Equal(t, 2, result.Stats.LabelsAdded)
		assert.Equal(t, 2, result.Stats.PropertiesSet)

		// Verify file-1 now has Node label
		file1, err := store.GetNode("file-1")
		require.NoError(t, err)
		assert.Contains(t, file1.Labels, "Node")
		assert.Equal(t, "file", file1.Properties["type"])

		// Verify file-2 unchanged (already had Node label)
		file2, err := store.GetNode("file-2")
		require.NoError(t, err)
		assert.Contains(t, file2.Labels, "Node")
		assert.Nil(t, file2.Properties["type"]) // type not set because it wasn't matched

		// Verify file-3 now has Node label
		file3, err := store.GetNode("file-3")
		require.NoError(t, err)
		assert.Contains(t, file3.Labels, "Node")
		assert.Equal(t, "file", file3.Properties["type"])
	})

	t.Run("running migration again has no effect", func(t *testing.T) {
		// Second run should do nothing since all File nodes now have Node label
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WHERE NOT f:Node
			SET f:Node, f.type = 'file'
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, result.Stats.LabelsAdded)
		assert.Equal(t, 0, result.Stats.PropertiesSet)
	})
}

func TestWhereLabelWithAndCondition(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test nodes
	nodes := []*storage.Node{
		{ID: "emp-1", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "Alice", "age": int64(30)}},
		{ID: "emp-2", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "Bob", "age": int64(25)}},
		{ID: "cust-1", Labels: []string{"Person", "Customer"}, Properties: map[string]interface{}{"name": "Charlie", "age": int64(35)}},
	}
	for _, n := range nodes {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	t.Run("WHERE label AND property condition", func(t *testing.T) {
		// Match Employees over 28
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE p:Employee AND p.age > 28
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Alice", result.Rows[0][0])
	})

	t.Run("WHERE NOT label AND property condition", func(t *testing.T) {
		// Match non-Employees over 30
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE NOT p:Employee AND p.age > 30
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Charlie", result.Rows[0][0])
	})
}

// TestUseCommand tests :USE command handling (Neo4j browser/shell compatibility)
func TestUseCommand(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run(":USE command with CREATE statements", func(t *testing.T) {
		// Test :USE command followed by multiple CREATE statements
		// This mimics Neo4j browser behavior where :USE switches database context
		query := `:USE test_db_a
CREATE (alice:Person {name: "Alice", id: "a1", db: "test_db_a"})
CREATE (bob:Person {name: "Bob", id: "a2", db: "test_db_a"})
CREATE (company:Company {name: "Acme Corp", id: "a3", db: "test_db_a"})
CREATE (alice)-[:WORKS_FOR]->(company)
CREATE (bob)-[:WORKS_FOR]->(company)
RETURN alice, bob, company`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, ":USE command should be stripped and query should execute")
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1, "should return one row with alice, bob, company")

		// Verify nodes were created
		countResult, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) as count", nil)
		require.NoError(t, err)
		require.Len(t, countResult.Rows, 1)
		assert.Equal(t, int64(3), countResult.Rows[0][0], "should have 3 nodes (alice, bob, company)")

		// Verify relationships were created
		relResult, err := exec.Execute(ctx, "MATCH ()-[r:WORKS_FOR]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.Len(t, relResult.Rows, 1)
		assert.Equal(t, int64(2), relResult.Rows[0][0], "should have 2 WORKS_FOR relationships")
	})

	t.Run(":USE command alone returns success", func(t *testing.T) {
		// :USE without any query should return success (database switching handled at API layer)
		result, err := exec.Execute(ctx, ":USE test_db", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, []string{"database"}, result.Columns)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "switched", result.Rows[0][0])
	})

	t.Run(":USE with lowercase", func(t *testing.T) {
		// Test :use (lowercase) is also recognized
		result, err := exec.Execute(ctx, `:use test_db
CREATE (n:Test {name: "test"})
RETURN n.name`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "test", result.Rows[0][0])
	})

	t.Run(":USE with whitespace", func(t *testing.T) {
		// Test :USE with extra whitespace
		result, err := exec.Execute(ctx, `:USE  test_db  
CREATE (n:Test {name: "test2"})
RETURN n.name`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "test2", result.Rows[0][0])
	})
}

// TestPropertyAccessInMatch verifies that property access works correctly in MATCH queries
func TestPropertyAccessInMatch(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First, test parseReturnItems directly
	returnClause := "n.order_id as order_id, n.amount as amount"
	returnItems := exec.parseReturnItems(returnClause)
	t.Logf("parseReturnItems returned %d items for %q", len(returnItems), returnClause)
	for i, item := range returnItems {
		t.Logf("  Item %d: expr=%q, alias=%q", i, item.expr, item.alias)
	}
	require.Len(t, returnItems, 2, "should parse 2 return items")
	require.Equal(t, "n.order_id", returnItems[0].expr, "first item expression should be n.order_id")
	require.Equal(t, "order_id", returnItems[0].alias, "first item alias should be order_id")
	require.Equal(t, "n.amount", returnItems[1].expr, "second item expression should be n.amount")
	require.Equal(t, "amount", returnItems[1].alias, "second item alias should be amount")

	// Create a node with properties
	_, err := exec.Execute(ctx, `CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "test_db"}) RETURN order`, nil)
	require.NoError(t, err)

	// Verify node was created with properties
	verifyResult, err := exec.Execute(ctx, `MATCH (n:Order) RETURN n, properties(n) as props`, nil)
	require.NoError(t, err)
	require.NotNil(t, verifyResult)
	require.Len(t, verifyResult.Rows, 1, "should find the Order node")
	if len(verifyResult.Rows) > 0 && len(verifyResult.Rows[0]) >= 2 {
		if props, ok := verifyResult.Rows[0][1].(map[string]interface{}); ok {
			t.Logf("Order node properties: %+v", props)
			require.Contains(t, props, "order_id", "Order node should have order_id property")
			require.Contains(t, props, "amount", "Order node should have amount property")
		}
	}

	// Test property access in MATCH query
	query := `MATCH (n:Order) RETURN n.order_id as order_id, n.amount as amount`
	t.Logf("Testing query: %q", query)
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	t.Logf("Result columns: %+v", result.Columns)
	t.Logf("Result rows: %+v", result.Rows)
	require.Len(t, result.Columns, 2, "should have 2 columns: order_id and amount")
	require.Len(t, result.Rows, 1, "should have 1 row")
	if len(result.Rows) > 0 {
		require.Len(t, result.Rows[0], 2, "row should have 2 values")
		t.Logf("Returned row: %+v", result.Rows[0])
	}

	// Verify properties are accessible
	orderID, ok := result.Rows[0][0].(string)
	require.True(t, ok, "order_id should be a string")
	assert.Equal(t, "ORD-001", orderID, "order_id should be ORD-001")

	// Amount can be int64 or float64 depending on how it was parsed
	var amountValue interface{}
	var amountFloat float64
	var amountInt int64
	if f, ok := result.Rows[0][1].(float64); ok {
		amountValue = f
		amountFloat = f
	} else if i, ok := result.Rows[0][1].(int64); ok {
		amountValue = i
		amountInt = i
	} else {
		t.Fatalf("amount should be int64 or float64, got %T: %v", result.Rows[0][1], result.Rows[0][1])
	}
	require.NotNil(t, amountValue, "amount should not be nil")
	if amountFloat > 0 {
		assert.Equal(t, float64(1000), amountFloat, "amount should be 1000")
	} else {
		assert.Equal(t, int64(1000), amountInt, "amount should be 1000")
	}
}

// TestMultipleCreatesPropertyAccess verifies that properties are correctly accessible after multiple CREATE statements
func TestMultipleCreatesPropertyAccess(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with multiple CREATE statements (like user's query)
	_, err := exec.Execute(ctx, `
		CREATE (charlie:Person {name: "Charlie", id: "b1", db: "test_db_b"})
		CREATE (diana:Person {name: "Diana", id: "b2", db: "test_db_b"})
		CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "test_db_b"})
		RETURN charlie, diana, order
	`, nil)
	require.NoError(t, err)

	// Query nodes and verify properties are accessible
	result, err := exec.Execute(ctx, `
		MATCH (n)
		RETURN n.name as name, n.order_id as order_id, labels(n) as labels, n.db as db
		ORDER BY n.name
	`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 3, "should have 3 nodes")

	// Verify properties are set correctly
	foundCharlie := false
	foundDiana := false
	foundOrder := false

	for _, row := range result.Rows {
		require.Len(t, row, 4, "each row should have 4 columns: name, order_id, labels, db")

		name := row[0]
		orderID := row[1]
		db := row[3]

		// Check db property
		if dbVal, ok := db.(string); ok {
			assert.Equal(t, "test_db_b", dbVal, "db property should be test_db_b")
		}

		// Check Person nodes
		if nameVal, ok := name.(string); ok && nameVal == "Charlie" {
			foundCharlie = true
			assert.Nil(t, orderID, "Charlie should not have order_id property")
		} else if nameVal, ok := name.(string); ok && nameVal == "Diana" {
			foundDiana = true
			assert.Nil(t, orderID, "Diana should not have order_id property")
		}

		// Check Order node
		if orderIDVal, ok := orderID.(string); ok && orderIDVal == "ORD-001" {
			foundOrder = true
			assert.Nil(t, name, "Order should not have name property")
		}
	}

	assert.True(t, foundCharlie, "should find Charlie node")
	assert.True(t, foundDiana, "should find Diana node")
	assert.True(t, foundOrder, "should find Order node")
}
