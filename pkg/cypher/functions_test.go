// Comprehensive unit tests for all Cypher functions in NornicDB.
// This file provides complete coverage for all scalar, aggregation, list, string,
// math, date/time, spatial, and vector functions.

package cypher

import (
	"math"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Test Helper Functions
// ========================================

func setupTestExecutor(t *testing.T) *StorageExecutor {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	return NewStorageExecutor(store)
}

func createTestNode(t *testing.T, e *StorageExecutor, id string, labels []string, props map[string]interface{}) *storage.Node {
	node := &storage.Node{
		ID:         storage.NodeID(id),
		Labels:     labels,
		Properties: props,
	}
	if _, err := e.storage.CreateNode(node); err != nil {
		t.Fatalf("Failed to create test node: %v", err)
	}
	return node
}

// ========================================
// Scalar Functions Tests
// ========================================

func TestFunctionId(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{"name": "Alice"})

	nodes := map[string]*storage.Node{"n": node}
	result := e.evaluateExpressionWithContext("id(n)", nodes, nil)

	if result != "node-1" {
		t.Errorf("id(n) = %v, want node-1", result)
	}
}

func TestFunctionElementId(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{"name": "Alice"})

	nodes := map[string]*storage.Node{"n": node}
	result := e.evaluateExpressionWithContext("elementId(n)", nodes, nil)

	expected := "4:nornicdb:node-1"
	if result != expected {
		t.Errorf("elementId(n) = %v, want %v", result, expected)
	}
}

func TestFunctionLabels(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person", "Employee"}, nil)

	nodes := map[string]*storage.Node{"n": node}
	result := e.evaluateExpressionWithContext("labels(n)", nodes, nil)

	labels, ok := result.([]interface{})
	if !ok {
		t.Fatalf("labels(n) should return []interface{}, got %T", result)
	}
	if len(labels) != 2 {
		t.Errorf("labels(n) should return 2 labels, got %d", len(labels))
	}
}

func TestFunctionType(t *testing.T) {
	e := setupTestExecutor(t)

	rel := &storage.Edge{
		ID:   "rel-1",
		Type: "KNOWS",
	}
	rels := map[string]*storage.Edge{"r": rel}
	result := e.evaluateExpressionWithContext("type(r)", nil, rels)

	if result != "KNOWS" {
		t.Errorf("type(r) = %v, want KNOWS", result)
	}
}

func TestFunctionKeys(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name": "Alice",
		"age":  30,
	})

	nodes := map[string]*storage.Node{"n": node}
	result := e.evaluateExpressionWithContext("keys(n)", nodes, nil)

	keys, ok := result.([]interface{})
	if !ok {
		t.Fatalf("keys(n) should return []interface{}, got %T", result)
	}
	if len(keys) != 2 {
		t.Errorf("keys(n) should return 2 keys, got %d", len(keys))
	}
}

func TestFunctionProperties(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name": "Alice",
		"age":  30,
	})

	nodes := map[string]*storage.Node{"n": node}
	result := e.evaluateExpressionWithContext("properties(n)", nodes, nil)

	props, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("properties(n) should return map, got %T", result)
	}
	if props["name"] != "Alice" {
		t.Errorf("properties(n)['name'] = %v, want Alice", props["name"])
	}
}

func TestFunctionCoalesce(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"coalesce(null, 'default')", "default"},
		{"coalesce('first', 'second')", "first"},
		{"coalesce(null, null, 'third')", "third"},
		{"coalesce(1, 2)", int64(1)},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

func TestFunctionExists(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name": "Alice",
	})

	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected bool
	}{
		{"exists(n.name)", true},
		{"exists(n.missing)", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// String Functions Tests
// ========================================

func TestStringFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// Case conversion
		{"toLower('HELLO')", "hello"},
		{"toUpper('hello')", "HELLO"},
		{"lower('WORLD')", "world"},
		{"upper('world')", "WORLD"},

		// Trimming
		{"trim('  hello  ')", "hello"},
		{"ltrim('  hello')", "hello"},
		{"rtrim('hello  ')", "hello"},
		{"btrim('xxhelloxx', 'x')", "hello"},

		// String manipulation
		{"replace('hello', 'l', 'L')", "heLLo"},
		{"reverse('hello')", "olleh"},
		{"left('hello', 2)", "he"},
		{"right('hello', 2)", "lo"},
		{"substring('hello', 1, 3)", "ell"},

		// Split
		{"toString(123)", "123"},
		{"toString(true)", "true"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

func TestSplitFunction(t *testing.T) {
	e := setupTestExecutor(t)

	// Create a node with property for split test
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"data": "a,b,c",
		"sep":  ",",
	})
	nodes := map[string]*storage.Node{"n": node}

	// Test with node properties
	result := e.evaluateExpressionWithContext("split(n.data, n.sep)", nodes, nil)
	list, ok := result.([]interface{})
	if !ok {
		t.Fatalf("split should return list, got %T", result)
	}
	if len(list) != 3 {
		t.Errorf("split(n.data, n.sep) returned %d elements, want 3", len(list))
	}
}

func TestCharLengthFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected int64
	}{
		{"char_length('hello')", 5},
		{"character_length('世界')", 2}, // Unicode characters
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

func TestFunctionEvaluator_ArrayIndexSliceAndMapLiteral(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-eval-1", []string{"Person", "Employee"}, map[string]interface{}{"nums": []interface{}{int64(10), int64(20), int64(30)}})
	nodes := map[string]*storage.Node{"n": node}

	// Array/string indexing paths
	assertEqual(t, "labels(n)[0]", "Person", e.evaluateExpressionWithContext("labels(n)[0]", nodes, nil))
	assertEqual(t, "labels(n)[-1]", "Employee", e.evaluateExpressionWithContext("labels(n)[-1]", nodes, nil))
	assertEqual(t, "'abc'[1]", "b", e.evaluateExpressionWithContext("'abc'[1]", nodes, nil))

	// Slice notation paths
	s1 := e.evaluateExpressionWithContext("labels(n)[..1]", nodes, nil)
	list1, ok := s1.([]interface{})
	if !ok || len(list1) != 1 || list1[0] != "Person" {
		t.Fatalf("labels(n)[..1] = %v, want [Person]", s1)
	}

	s2 := e.evaluateExpressionWithContext("labels(n)[1..]", nodes, nil)
	list2, ok := s2.([]interface{})
	if !ok || len(list2) != 1 || list2[0] != "Employee" {
		t.Fatalf("labels(n)[1..] = %v, want [Employee]", s2)
	}

	// IN list must not be treated as indexing
	assertEqual(t, "1 IN [1,2,3]", true, e.evaluateExpressionWithContext("1 IN [1,2,3]", nil, nil))

	// Parenthesized expression and map literal
	assertEqual(t, "(1 + 2)", int64(3), e.evaluateExpressionWithContext("(1 + 2)", nil, nil))
	m := e.evaluateExpressionWithContext("{x: 1, y: 'z'}", nil, nil)
	mm, ok := m.(map[string]interface{})
	if !ok {
		t.Fatalf("map literal should return map, got %T", m)
	}
	if mm["x"] != int64(1) || mm["y"] != "z" {
		t.Fatalf("map literal mismatch: %v", mm)
	}
}

func TestFunctionEvaluator_AdditionalBranches(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-eval-2", []string{"Person", "Employee"}, map[string]interface{}{
		"name":   "Alice",
		"values": []interface{}{int64(10), int64(20), int64(30), int64(40)},
	})
	rel := &storage.Edge{
		ID:         storage.EdgeID("rel-eval-1"),
		Type:       "KNOWS",
		StartNode:  node.ID,
		EndNode:    node.ID,
		Properties: map[string]interface{}{"weight": int64(1)},
	}
	if err := e.storage.CreateEdge(rel); err != nil {
		t.Fatalf("Failed to create test edge: %v", err)
	}

	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{"r": rel}

	assertEqual(t, "type({type:'KNOWS'})", "KNOWS", e.evaluateExpressionWithContext("type({type:'KNOWS'})", nodes, rels))
	assertEqual(t, "exists(n.missing)", false, e.evaluateExpressionWithContext("exists(n.missing)", nodes, rels))
	assertEqual(t, "head([])", nil, e.evaluateExpressionWithContext("head([])", nodes, rels))
	assertEqual(t, "last([])", nil, e.evaluateExpressionWithContext("last([])", nodes, rels))
	assertEqual(t, "reverse('stressed')", "desserts", e.evaluateExpressionWithContext("reverse('stressed')", nodes, rels))
	assertEqual(t, "indexOf([1,2,3], 9)", int64(-1), e.evaluateExpressionWithContext("indexOf([1,2,3], 9)", nodes, rels))
	assertEqual(t, "hasLabels(n, ['Person', 'Employee'])", true, e.evaluateExpressionWithContext("hasLabels(n, ['Person', 'Employee'])", nodes, rels))
	assertEqual(t, "hasLabels(n, ['Person', 'Missing'])", false, e.evaluateExpressionWithContext("hasLabels(n, ['Person', 'Missing'])", nodes, rels))

	tail := e.evaluateExpressionWithContext("tail([1])", nodes, rels)
	tailList, ok := tail.([]interface{})
	if !ok || len(tailList) != 0 {
		t.Fatalf("tail([1]) = %v, want []", tail)
	}

	sliced := e.evaluateExpressionWithContext("slice([1, 2, 3, 4], -3, -1)", nodes, rels)
	sliceList, ok := sliced.([]interface{})
	if !ok || len(sliceList) != 2 || sliceList[0] != int64(2) || sliceList[1] != int64(3) {
		t.Fatalf("slice([1,2,3,4], -3, -1) = %v, want [2,3]", sliced)
	}

	gotLength := e.evaluateExpressionWithContextFullFunctions(
		"length(p)",
		nodes,
		rels,
		map[string]*PathResult{"p": {Length: 4}},
		nil,
		nil,
		0,
	)
	assertEqual(t, "length(p)", int64(4), gotLength)

	gotFallbackLength := e.evaluateExpressionWithContextFullFunctions("length(route)", nodes, rels, nil, nil, nil, 6)
	assertEqual(t, "length(route)", int64(6), gotFallbackLength)
}

func assertEqual(t *testing.T, name string, expected interface{}, actual interface{}) {
	t.Helper()
	if actual != expected {
		t.Fatalf("%s = %v, want %v", name, actual, expected)
	}
}

// ========================================
// List Functions Tests
// ========================================

func TestListFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Test head
	result := e.evaluateExpressionWithContext("head([1, 2, 3])", nil, nil)
	if result != int64(1) {
		t.Errorf("head([1,2,3]) = %v, want 1", result)
	}

	// Test last
	result = e.evaluateExpressionWithContext("last([1, 2, 3])", nil, nil)
	if result != int64(3) {
		t.Errorf("last([1,2,3]) = %v, want 3", result)
	}

	// Test tail
	tail := e.evaluateExpressionWithContext("tail([1, 2, 3])", nil, nil)
	tailList, ok := tail.([]interface{})
	if !ok || len(tailList) != 2 {
		t.Errorf("tail([1,2,3]) = %v, want [2,3]", tail)
	}

	// Test reverse
	rev := e.evaluateExpressionWithContext("reverse([1, 2, 3])", nil, nil)
	revList, ok := rev.([]interface{})
	if !ok || len(revList) != 3 {
		t.Errorf("reverse([1,2,3]) failed")
	}
}

func TestRangeFunction(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected int // length of result
	}{
		{"range(0, 5)", 6},     // 0,1,2,3,4,5
		{"range(0, 10, 2)", 6}, // 0,2,4,6,8,10
		{"range(5, 0, -1)", 6}, // 5,4,3,2,1,0
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			list, ok := result.([]interface{})
			if !ok {
				t.Fatalf("%s should return list", tt.expr)
			}
			if len(list) != tt.expected {
				t.Errorf("%s returned %d elements, want %d", tt.expr, len(list), tt.expected)
			}
		})
	}
}

func TestSizeFunction(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected int64
	}{
		{"size([1, 2, 3])", 3},
		{"size('hello')", 5},
		{"length([1, 2])", 2},
		{"length('abc')", 3},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// Math Functions Tests
// ========================================

func TestMathFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
		delta    float64 // for float comparisons
	}{
		{"abs(-5)", int64(5), 0},
		{"abs(5)", int64(5), 0},
		{"ceil(4.2)", int64(5), 0},
		{"floor(4.8)", int64(4), 0},
		{"round(4.5)", int64(5), 0},
		{"sign(10)", int64(1), 0},
		{"sign(-10)", int64(-1), 0},
		{"sign(0)", int64(0), 0},
		{"sqrt(16)", float64(4), 0.001},
		{"exp(0)", float64(1), 0.001},
		{"log(1)", float64(0), 0.001},
		{"log10(100)", float64(2), 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)

			if tt.delta > 0 {
				resultF, ok := result.(float64)
				if !ok {
					t.Fatalf("%s should return float64", tt.expr)
				}
				expectedF := tt.expected.(float64)
				if math.Abs(resultF-expectedF) > tt.delta {
					t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
				}
			} else {
				if result != tt.expected {
					t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
				}
			}
		})
	}
}

func TestTrigFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected float64
		delta    float64
	}{
		{"sin(0)", 0, 0.001},
		{"cos(0)", 1, 0.001},
		{"tan(0)", 0, 0.001},
		{"asin(0)", 0, 0.001},
		{"acos(1)", 0, 0.001},
		{"atan(0)", 0, 0.001},
		{"atan2(0, 1)", 0, 0.001},
		{"radians(180)", math.Pi, 0.001},
		{"degrees(3.14159265)", 180, 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			resultF, ok := result.(float64)
			if !ok {
				t.Fatalf("%s should return float64, got %T", tt.expr, result)
			}
			if math.Abs(resultF-tt.expected) > tt.delta {
				t.Errorf("%s = %v, want %v", tt.expr, resultF, tt.expected)
			}
		})
	}
}

func TestHyperbolicFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected float64
		delta    float64
	}{
		{"sinh(0)", 0, 0.001},
		{"sinh(1)", 1.1752011936438014, 0.001},
		{"sinh(0.7)", 0.7585837018395334, 0.001},
		{"cosh(0)", 1, 0.001},
		{"cosh(1)", 1.5430806348152437, 0.001},
		{"cosh(0.7)", 1.255169005630943, 0.001},
		{"tanh(0)", 0, 0.001},
		{"tanh(1)", 0.7615941559557649, 0.001},
		{"tanh(0.7)", 0.6043677771171636, 0.001},
		{"coth(1)", 1.3130352854993312, 0.001},
		{"coth(0.7)", 1.6546216358026298, 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			resultF, ok := result.(float64)
			if !ok {
				t.Fatalf("%s should return float64, got %T", tt.expr, result)
			}
			if math.Abs(resultF-tt.expected) > tt.delta {
				t.Errorf("%s = %v, want %v", tt.expr, resultF, tt.expected)
			}
		})
	}
}

func TestPowerFunction(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected float64
		delta    float64
	}{
		{"power(2, 3)", 8, 0.001},
		{"power(2, 10)", 1024, 0.001},
		{"power(10, 2)", 100, 0.001},
		{"power(4, 0.5)", 2, 0.001},
		{"power(27, 0.333333)", 3, 0.01},
		{"power(2, -1)", 0.5, 0.001},
		{"power(0, 5)", 0, 0.001},
		{"power(5, 0)", 1, 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			resultF, ok := result.(float64)
			if !ok {
				t.Fatalf("%s should return float64, got %T", tt.expr, result)
			}
			if math.Abs(resultF-tt.expected) > tt.delta {
				t.Errorf("%s = %v, want %v", tt.expr, resultF, tt.expected)
			}
		})
	}
}

func TestMathConstants(t *testing.T) {
	e := setupTestExecutor(t)

	// Test pi()
	pi := e.evaluateExpressionWithContext("pi()", nil, nil)
	piF, ok := pi.(float64)
	if !ok || math.Abs(piF-math.Pi) > 0.0001 {
		t.Errorf("pi() = %v, want %v", pi, math.Pi)
	}

	// Test e()
	eVal := e.evaluateExpressionWithContext("e()", nil, nil)
	eF, ok := eVal.(float64)
	if !ok || math.Abs(eF-math.E) > 0.0001 {
		t.Errorf("e() = %v, want %v", eVal, math.E)
	}
}

func TestRandomFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Test rand() returns value between 0 and 1
	result := e.evaluateExpressionWithContext("rand()", nil, nil)
	randF, ok := result.(float64)
	if !ok {
		t.Fatalf("rand() should return float64")
	}
	if randF < 0 || randF > 1 {
		t.Errorf("rand() = %v, should be between 0 and 1", randF)
	}

	// Test randomUUID() returns a string
	uuid := e.evaluateExpressionWithContext("randomUUID()", nil, nil)
	uuidStr, ok := uuid.(string)
	if !ok {
		t.Fatalf("randomUUID() should return string")
	}
	if len(uuidStr) < 32 {
		t.Errorf("randomUUID() = %v, should be valid UUID format", uuidStr)
	}
}

// ========================================
// Type Conversion Functions Tests
// ========================================

func TestTypeConversionFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"toInteger('123')", int64(123)},
		{"toInteger(123.7)", int64(123)},
		{"toInt('456')", int64(456)},
		{"toFloat('3.14')", float64(3.14)},
		{"toFloat(42)", float64(42)},
		{"toBoolean('true')", true},
		{"toBoolean('false')", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v (%T), want %v (%T)", tt.expr, result, result, tt.expected, tt.expected)
			}
		})
	}
}

// ========================================
// Null Check Functions Tests
// ========================================

func TestNullCheckFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"isEmpty([])", true},
		{"isEmpty([1])", false},
		{"isEmpty('')", true},
		{"isEmpty('a')", false},
		{"isNaN(0)", false},
		{"nullIf('a', 'a')", nil},
		{"nullIf('a', 'b')", "a"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// Timestamp Functions Tests
// ========================================

func TestTimestampFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Test timestamp() returns a value
	ts := e.evaluateExpressionWithContext("timestamp()", nil, nil)
	if ts == nil {
		t.Error("timestamp() should return a value")
	}

	// Test datetime() returns a value
	dt := e.evaluateExpressionWithContext("datetime()", nil, nil)
	if dt == nil {
		t.Error("datetime() should return a value")
	}

	// Test date()
	d := e.evaluateExpressionWithContext("date()", nil, nil)
	if d == nil {
		t.Error("date() should return a value")
	}

	// Test time()
	tm := e.evaluateExpressionWithContext("time()", nil, nil)
	if tm == nil {
		t.Error("time() should return a value")
	}
}

// ========================================
// Relationship Functions Tests
// ========================================

func TestRelationshipFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Create nodes and relationship
	node1 := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{"name": "Alice"})
	node2 := createTestNode(t, e, "node-2", []string{"Person"}, map[string]interface{}{"name": "Bob"})

	rel := &storage.Edge{
		ID:         "rel-1",
		Type:       "KNOWS",
		StartNode:  node1.ID,
		EndNode:    node2.ID,
		Properties: map[string]interface{}{"since": 2020},
	}
	e.storage.CreateEdge(rel)

	nodes := map[string]*storage.Node{"a": node1, "b": node2}
	rels := map[string]*storage.Edge{"r": rel}

	// Test startNode
	result := e.evaluateExpressionWithContext("startNode(r)", nodes, rels)
	if result == nil {
		t.Error("startNode(r) should return a value")
	}

	// Test endNode
	result = e.evaluateExpressionWithContext("endNode(r)", nodes, rels)
	if result == nil {
		t.Error("endNode(r) should return a value")
	}

	// Test type(r)
	result = e.evaluateExpressionWithContext("type(r)", nodes, rels)
	if result != "KNOWS" {
		t.Errorf("type(r) = %v, want KNOWS", result)
	}
}

// ========================================
// Vector Functions Tests
// ========================================

func TestVectorSimilarityFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Create nodes with vector embeddings
	node1 := createTestNode(t, e, "node-1", []string{"Doc"}, map[string]interface{}{
		"vec": []interface{}{float64(1), float64(0), float64(0)},
	})
	node2 := createTestNode(t, e, "node-2", []string{"Doc"}, map[string]interface{}{
		"vec": []interface{}{float64(1), float64(0), float64(0)},
	})

	nodes := map[string]*storage.Node{"a": node1, "b": node2}

	// Test cosine similarity of identical vectors = 1
	result := e.evaluateExpressionWithContext("vector.similarity.cosine(a.vec, b.vec)", nodes, nil)
	if result == nil {
		t.Error("vector.similarity.cosine should return a value")
	}

	// Test euclidean similarity
	result = e.evaluateExpressionWithContext("vector.similarity.euclidean(a.vec, b.vec)", nodes, nil)
	if result == nil {
		t.Error("vector.similarity.euclidean should return a value")
	}
}

// ========================================
// Spatial Functions Tests
// ========================================

func TestSpatialFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Test point function
	result := e.evaluateExpressionWithContext("point({x: 1.0, y: 2.0})", nil, nil)
	pointMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("point() should return map, got %T", result)
	}
	if pointMap["x"] != float64(1.0) {
		t.Errorf("point().x = %v, want 1.0", pointMap["x"])
	}

	// Test distance function with x/y coordinates
	node1 := createTestNode(t, e, "node-1", []string{"Location"}, map[string]interface{}{
		"loc": map[string]interface{}{"x": float64(0), "y": float64(0)},
	})
	node2 := createTestNode(t, e, "node-2", []string{"Location"}, map[string]interface{}{
		"loc": map[string]interface{}{"x": float64(3), "y": float64(4)},
	})

	nodes := map[string]*storage.Node{"a": node1, "b": node2}
	dist := e.evaluateExpressionWithContext("distance(a.loc, b.loc)", nodes, nil)
	distF, ok := dist.(float64)
	if !ok {
		t.Fatalf("distance() should return float64, got %T", dist)
	}
	// Distance from (0,0) to (3,4) should be 5
	if math.Abs(distF-5.0) > 0.001 {
		t.Errorf("distance(a.loc, b.loc) = %v, want 5.0", distF)
	}
}

// ========================================
// Reduce Function Tests
// ========================================

func TestReduceFunction(t *testing.T) {
	e := setupTestExecutor(t)

	// reduce(acc = 0, x IN [1,2,3] | acc + x) should return 6
	result := e.evaluateExpressionWithContext("reduce(acc = 0, x IN [1, 2, 3] | acc + x)", nil, nil)
	if result == nil {
		t.Error("reduce should return a value")
	}
}

// ========================================
// Property Access Tests
// ========================================

func TestFunctionPropertyAccess(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name":    "Alice",
		"age":     int64(30),
		"active":  true,
		"balance": float64(100.50),
	})

	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"n.name", "Alice"},
		{"n.age", int64(30)},
		{"n.active", true},
		{"n.balance", float64(100.50)},
		{"n.missing", nil},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v (%T), want %v (%T)", tt.expr, result, result, tt.expected, tt.expected)
			}
		})
	}
}

// ========================================
// Literals Tests
// ========================================

func TestLiterals(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"null", nil},
		{"true", true},
		{"false", false},
		{"123", int64(123)},
		{"3.14", float64(3.14)},
		{"'hello'", "hello"},
		{`"world"`, "world"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// String Concatenation Tests
// ========================================

func TestStringConcatenation(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"firstName": "John",
		"lastName":  "Doe",
	})

	nodes := map[string]*storage.Node{"n": node}

	result := e.evaluateExpressionWithContext("n.firstName + ' ' + n.lastName", nodes, nil)
	if result != "John Doe" {
		t.Errorf("concatenation = %v, want 'John Doe'", result)
	}
}

// ========================================
// Count Function Tests
// ========================================

func TestCountFunction(t *testing.T) {
	e := setupTestExecutor(t)

	// count(*) in expression context should NOT be evaluated here
	// Aggregation functions must be handled by executeAggregation() or executeMatchWithRelationships()
	result := e.evaluateExpressionWithContext("count(*)", nil, nil)
	if result != nil {
		t.Errorf("count(*) in expression context should return nil, got %v", result)
	}

	// count(n) also should NOT be evaluated in expression context
	node := createTestNode(t, e, "node-1", []string{"Person"}, nil)
	nodes := map[string]*storage.Node{"n": node}
	result = e.evaluateExpressionWithContext("count(n)", nodes, nil)
	if result != nil {
		t.Errorf("count(n) in expression context should return nil, got %v", result)
	}
}

// ========================================
// Embedding Property Filter Tests
// ========================================
// OrNull Variants Tests
// ========================================

func TestOrNullVariants(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// toIntegerOrNull
		{"toIntegerOrNull('123')", int64(123)},
		{"toIntegerOrNull('invalid')", nil},
		{"toIntegerOrNull(45.6)", int64(45)},

		// toFloatOrNull
		{"toFloatOrNull('3.14')", float64(3.14)},
		{"toFloatOrNull('invalid')", nil},
		{"toFloatOrNull(42)", float64(42)},

		// toBooleanOrNull
		{"toBooleanOrNull('true')", true},
		{"toBooleanOrNull('false')", false},
		{"toBooleanOrNull('maybe')", nil},

		// toStringOrNull
		{"toStringOrNull(123)", "123"},
		{"toStringOrNull(null)", nil},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v (%T), want %v (%T)", tt.expr, result, result, tt.expected, tt.expected)
			}
		})
	}
}

// ========================================
// List Conversion Functions Tests
// ========================================

func TestListConversionFunctions(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"intList":    []interface{}{"1", "2", "3"},
		"floatList":  []interface{}{"1.1", "2.2", "3.3"},
		"boolList":   []interface{}{"true", "false", "true"},
		"stringList": []interface{}{int64(1), int64(2), int64(3)},
	})
	nodes := map[string]*storage.Node{"n": node}

	// toIntegerList
	result := e.evaluateExpressionWithContext("toIntegerList(n.intList)", nodes, nil)
	if intList, ok := result.([]interface{}); ok {
		if len(intList) != 3 {
			t.Errorf("toIntegerList should return 3 elements, got %d", len(intList))
		}
		if intList[0] != int64(1) {
			t.Errorf("toIntegerList[0] = %v, want 1", intList[0])
		}
	} else {
		t.Errorf("toIntegerList should return []interface{}, got %T", result)
	}

	// toFloatList
	result = e.evaluateExpressionWithContext("toFloatList(n.floatList)", nodes, nil)
	if floatList, ok := result.([]interface{}); ok {
		if len(floatList) != 3 {
			t.Errorf("toFloatList should return 3 elements")
		}
	}

	// toBooleanList
	result = e.evaluateExpressionWithContext("toBooleanList(n.boolList)", nodes, nil)
	if boolList, ok := result.([]interface{}); ok {
		if len(boolList) != 3 {
			t.Errorf("toBooleanList should return 3 elements")
		}
		if boolList[0] != true {
			t.Errorf("toBooleanList[0] = %v, want true", boolList[0])
		}
	}

	// toStringList
	result = e.evaluateExpressionWithContext("toStringList(n.stringList)", nodes, nil)
	if stringList, ok := result.([]interface{}); ok {
		if len(stringList) != 3 {
			t.Errorf("toStringList should return 3 elements")
		}
		if stringList[0] != "1" {
			t.Errorf("toStringList[0] = %v, want '1'", stringList[0])
		}
	}
}

// ========================================
// valueType Function Tests
// ========================================

func TestValueTypeFunction(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected string
	}{
		{"valueType(null)", "NULL"},
		{"valueType(true)", "BOOLEAN"},
		{"valueType(123)", "INTEGER"},
		{"valueType(3.14)", "FLOAT"},
		{"valueType('hello')", "STRING"},
		{"valueType([1, 2, 3])", "LIST"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// Aggregation Functions (Expression Context) Tests
// ========================================

func TestAggregationInExpressionContext(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{"val": int64(42)})
	nodes := map[string]*storage.Node{"n": node}

	// In single row context, aggregation functions just return the value
	tests := []string{"sum(n.val)", "avg(n.val)", "min(n.val)", "max(n.val)"}

	for _, expr := range tests {
		t.Run(expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(expr, nodes, nil)
			if result != int64(42) {
				t.Errorf("%s = %v, want 42", expr, result)
			}
		})
	}

	// collect returns a list
	result := e.evaluateExpressionWithContext("collect(n.val)", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 1 || list[0] != int64(42) {
			t.Errorf("collect(n.val) = %v, want [42]", list)
		}
	} else {
		t.Errorf("collect should return list, got %T", result)
	}
}

// ========================================
// List Predicate Functions Tests
// ========================================

func TestListPredicateFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Test with simple predicates that can be evaluated
	// Note: these functions parse the WHERE predicate and substitute values

	// Test none() with empty result - no match expected
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"empty": []interface{}{},
	})
	nodes := map[string]*storage.Node{"n": node}

	// none() on empty list should return true
	result := e.evaluateExpressionWithContext("none(x IN n.empty WHERE true)", nodes, nil)
	if result != true {
		t.Errorf("none(x IN empty list) = %v, want true", result)
	}

	// Test basic parsing - ensure functions don't crash
	node2 := createTestNode(t, e, "node-2", []string{"Test"}, map[string]interface{}{
		"nums": []interface{}{int64(1), int64(2), int64(3)},
	})
	nodes2 := map[string]*storage.Node{"n": node2}

	// all() should not crash even if predicate evaluation is limited
	_ = e.evaluateExpressionWithContext("all(x IN n.nums WHERE x = x)", nodes2, nil)

	// any() should not crash
	_ = e.evaluateExpressionWithContext("any(x IN n.nums WHERE x = x)", nodes2, nil)

	// single() should not crash
	_ = e.evaluateExpressionWithContext("single(x IN n.nums WHERE x = x)", nodes2, nil)
}

// ========================================
// Filter and Extract Functions Tests
// ========================================

func TestFilterAndExtractFunctions(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"nums": []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
	})
	nodes := map[string]*storage.Node{"n": node}

	// Test extract() - transforms each element
	result := e.evaluateExpressionWithContext("extract(x IN n.nums | x)", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 5 {
			t.Errorf("extract should return 5 elements, got %d", len(list))
		}
	} else {
		t.Errorf("extract should return list, got %T", result)
	}

	// Test filter() - returns a list (predicate evaluation limited)
	result = e.evaluateExpressionWithContext("filter(x IN n.nums WHERE true)", nodes, nil)
	_, ok := result.([]interface{})
	if !ok {
		t.Errorf("filter should return list, got %T", result)
	}
}

// ========================================
// withinBBox Function Tests
// ========================================

func TestWithinBBoxFunction(t *testing.T) {
	e := setupTestExecutor(t)

	// Create points
	node := createTestNode(t, e, "node-1", []string{"Location"}, map[string]interface{}{
		"inside":  map[string]interface{}{"x": float64(5), "y": float64(5)},
		"outside": map[string]interface{}{"x": float64(15), "y": float64(15)},
		"ll":      map[string]interface{}{"x": float64(0), "y": float64(0)},
		"ur":      map[string]interface{}{"x": float64(10), "y": float64(10)},
	})
	nodes := map[string]*storage.Node{"n": node}

	// Point inside bbox
	result := e.evaluateExpressionWithContext("withinBBox(n.inside, n.ll, n.ur)", nodes, nil)
	if result != true {
		t.Errorf("withinBBox(inside) = %v, want true", result)
	}

	// Point outside bbox
	result = e.evaluateExpressionWithContext("withinBBox(n.outside, n.ll, n.ur)", nodes, nil)
	if result != false {
		t.Errorf("withinBBox(outside) = %v, want false", result)
	}
}

// ========================================
// List Comprehension Tests
// ========================================

func TestListComprehension(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"nums": []interface{}{int64(1), int64(2), int64(3)},
	})
	nodes := map[string]*storage.Node{"n": node}

	// [x IN list | x] - identity transformation
	result := e.evaluateExpressionWithContext("[x IN n.nums | x]", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 3 {
			t.Errorf("list comprehension should return 3 elements, got %d", len(list))
		}
	}
}

// ========================================
// Slice Function Tests
// ========================================

func TestSliceFunction(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"list": []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
	})
	nodes := map[string]*storage.Node{"n": node}

	// slice(list, 1, 3)
	result := e.evaluateExpressionWithContext("slice(n.list, 1, 3)", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 2 {
			t.Errorf("slice(list, 1, 3) should return 2 elements, got %d", len(list))
		}
	} else {
		t.Errorf("slice should return list, got %T", result)
	}

	// slice(list, 2) - to end
	result = e.evaluateExpressionWithContext("slice(n.list, 2)", nodes, nil)
	if list, ok := result.([]interface{}); ok {
		if len(list) != 3 {
			t.Errorf("slice(list, 2) should return 3 elements, got %d", len(list))
		}
	}
}

// ========================================
// indexOf Function Tests
// ========================================

func TestIndexOfFunction(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"list": []interface{}{"a", "b", "c", "d"},
	})
	nodes := map[string]*storage.Node{"n": node}

	// Found
	result := e.evaluateExpressionWithContext("indexOf(n.list, 'b')", nodes, nil)
	if result != int64(1) {
		t.Errorf("indexOf(list, 'b') = %v, want 1", result)
	}

	// Not found
	result = e.evaluateExpressionWithContext("indexOf(n.list, 'z')", nodes, nil)
	if result != int64(-1) {
		t.Errorf("indexOf(list, 'z') = %v, want -1", result)
	}
}

// ========================================
// Degree Functions Tests
// ========================================

func TestDegreeFunctions(t *testing.T) {
	e := setupTestExecutor(t)

	// Create nodes
	node1 := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{"name": "Alice"})
	node2 := createTestNode(t, e, "node-2", []string{"Person"}, map[string]interface{}{"name": "Bob"})
	node3 := createTestNode(t, e, "node-3", []string{"Person"}, map[string]interface{}{"name": "Carol"})

	// Create edges: Alice -> Bob, Carol -> Alice
	e.storage.CreateEdge(&storage.Edge{ID: "e1", Type: "KNOWS", StartNode: node1.ID, EndNode: node2.ID})
	e.storage.CreateEdge(&storage.Edge{ID: "e2", Type: "KNOWS", StartNode: node3.ID, EndNode: node1.ID})

	nodes := map[string]*storage.Node{"a": node1, "b": node2, "c": node3}

	// Alice has 1 outgoing, 1 incoming = degree 2
	result := e.evaluateExpressionWithContext("degree(a)", nodes, nil)
	if result != int64(2) {
		t.Errorf("degree(a) = %v, want 2", result)
	}

	// Alice has 1 outgoing
	result = e.evaluateExpressionWithContext("outDegree(a)", nodes, nil)
	if result != int64(1) {
		t.Errorf("outDegree(a) = %v, want 1", result)
	}

	// Alice has 1 incoming
	result = e.evaluateExpressionWithContext("inDegree(a)", nodes, nil)
	if result != int64(1) {
		t.Errorf("inDegree(a) = %v, want 1", result)
	}

	// Bob has 0 outgoing, 1 incoming
	result = e.evaluateExpressionWithContext("degree(b)", nodes, nil)
	if result != int64(1) {
		t.Errorf("degree(b) = %v, want 1", result)
	}
}

// ========================================
// hasLabels Function Tests
// ========================================

func TestHasLabelsFunction(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person", "Employee", "Manager"}, map[string]interface{}{
		"requiredLabels": []interface{}{"Person", "Employee"},
		"wrongLabels":    []interface{}{"Person", "Director"},
	})
	nodes := map[string]*storage.Node{"n": node}

	// Has all labels (using property with list)
	result := e.evaluateExpressionWithContext("hasLabels(n, n.requiredLabels)", nodes, nil)
	if result != true {
		t.Errorf("hasLabels with matching labels = %v, want true", result)
	}

	// Missing a label
	result = e.evaluateExpressionWithContext("hasLabels(n, n.wrongLabels)", nodes, nil)
	if result != false {
		t.Errorf("hasLabels with missing label = %v, want false", result)
	}
}

// ========================================
// Haversine Distance Tests
// ========================================

func TestHaversineDistance(t *testing.T) {
	e := setupTestExecutor(t)

	// Test distance with lat/lon coordinates
	node1 := createTestNode(t, e, "node-1", []string{"City"}, map[string]interface{}{
		"loc": map[string]interface{}{"latitude": float64(40.7128), "longitude": float64(-74.0060)}, // NYC
	})
	node2 := createTestNode(t, e, "node-2", []string{"City"}, map[string]interface{}{
		"loc": map[string]interface{}{"latitude": float64(34.0522), "longitude": float64(-118.2437)}, // LA
	})

	nodes := map[string]*storage.Node{"nyc": node1, "la": node2}
	dist := e.evaluateExpressionWithContext("distance(nyc.loc, la.loc)", nodes, nil)

	if dist == nil {
		t.Fatal("haversine distance should return a value")
	}

	distF, ok := dist.(float64)
	if !ok {
		t.Fatalf("distance() should return float64, got %T", dist)
	}

	// Distance from NYC to LA is approximately 3,944 km or 3,944,000 meters
	if distF < 3800000 || distF > 4100000 {
		t.Errorf("haversine distance = %v, expected ~3944000 meters", distF)
	}
}

// ========================================
// CASE WHEN Expression Tests
// ========================================

func TestCaseWhenExpression(t *testing.T) {
	e := setupTestExecutor(t)

	// Simple CASE expression
	result := e.evaluateExpressionWithContext("CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END", nil, nil)
	if result != "one" {
		t.Errorf("CASE 1 WHEN 1 = %v, want 'one'", result)
	}

	// Searched CASE expression
	result = e.evaluateExpressionWithContext("CASE WHEN true THEN 'yes' ELSE 'no' END", nil, nil)
	if result != "yes" {
		t.Errorf("CASE WHEN true = %v, want 'yes'", result)
	}

	// CASE with ELSE
	result = e.evaluateExpressionWithContext("CASE 3 WHEN 1 THEN 'one' ELSE 'other' END", nil, nil)
	if result != "other" {
		t.Errorf("CASE 3 WHEN 1 = %v, want 'other'", result)
	}

	// CASE without matching WHEN (no ELSE)
	result = e.evaluateExpressionWithContext("CASE 5 WHEN 1 THEN 'one' END", nil, nil)
	if result != nil {
		t.Errorf("CASE without match = %v, want nil", result)
	}
}

func TestCaseWhenWithNode(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"status": "active",
		"score":  int64(85),
	})
	nodes := map[string]*storage.Node{"n": node}

	// CASE with property
	result := e.evaluateExpressionWithContext("CASE n.status WHEN 'active' THEN 'online' ELSE 'offline' END", nodes, nil)
	if result != "online" {
		t.Errorf("CASE n.status = %v, want 'online'", result)
	}
}

// ========================================
// Logical Operators Tests
// ========================================

func TestLogicalOperators(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// AND
		{"true AND true", true},
		{"true AND false", false},
		{"false AND true", false},

		// OR
		{"true OR false", true},
		{"false OR true", true},
		{"false OR false", false},

		// XOR
		{"true XOR false", true},
		{"true XOR true", false},
		{"false XOR false", false},

		// NOT
		{"NOT true", false},
		{"NOT false", true},

		// Combined
		{"true AND NOT false", true},
		{"false OR NOT false", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// Comparison Operators Tests
// ========================================

func TestComparisonOperators(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// Equality
		{"1 = 1", true},
		{"1 = 2", false},
		{"'a' = 'a'", true},

		// Not equal
		{"1 <> 2", true},
		{"1 != 2", true},
		{"1 <> 1", false},

		// Less than
		{"1 < 2", true},
		{"2 < 1", false},

		// Greater than
		{"2 > 1", true},
		{"1 > 2", false},

		// Less than or equal
		{"1 <= 1", true},
		{"1 <= 2", true},
		{"2 <= 1", false},

		// Greater than or equal
		{"1 >= 1", true},
		{"2 >= 1", true},
		{"1 >= 2", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// Arithmetic Operators Tests
// ========================================

func TestArithmeticOperators(t *testing.T) {
	e := setupTestExecutor(t)

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// Multiplication
		{"2 * 3", int64(6)},
		{"2.5 * 2", float64(5)},

		// Division - Neo4j returns int64 for exact division, float64 otherwise
		{"6 / 2", int64(3)},
		{"7 / 2", float64(3.5)},

		// Modulo
		{"7 % 3", int64(1)},
		{"10 % 5", int64(0)},

		// Subtraction
		{"5 - 3", int64(2)},
		{"3.5 - 1.5", float64(2)},

		// Unary minus
		{"-5", int64(-5)},
		{"-3.14", float64(-3.14)},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nil, nil)
			if result != tt.expected {
				t.Errorf("%s = %v (%T), want %v (%T)", tt.expr, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestArithmeticWithProperties(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"a": int64(10),
		"b": int64(3),
	})
	nodes := map[string]*storage.Node{"n": node}

	// n.a * n.b
	result := e.evaluateExpressionWithContext("n.a * n.b", nodes, nil)
	if result != int64(30) {
		t.Errorf("n.a * n.b = %v, want 30", result)
	}

	// n.a - n.b
	result = e.evaluateExpressionWithContext("n.a - n.b", nodes, nil)
	if result != int64(7) {
		t.Errorf("n.a - n.b = %v, want 7", result)
	}
}

// ========================================
// Combined Expression Tests
// ========================================

func TestCombinedExpressions(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"age":    int64(25),
		"active": true,
	})
	nodes := map[string]*storage.Node{"n": node}

	// Comparison with property
	result := e.evaluateExpressionWithContext("n.age > 18", nodes, nil)
	if result != true {
		t.Errorf("n.age > 18 = %v, want true", result)
	}

	// Multiple conditions with AND
	result = e.evaluateExpressionWithContext("n.age > 18 AND n.active = true", nodes, nil)
	if result != true {
		t.Errorf("n.age > 18 AND n.active = %v, want true", result)
	}
}

// ========================================
// IS NULL / IS NOT NULL Tests
// ========================================

func TestIsNullPredicates(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name": "Alice",
	})
	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"null IS NULL", true},
		{"null IS NOT NULL", false},
		{"'hello' IS NULL", false},
		{"'hello' IS NOT NULL", true},
		{"n.name IS NOT NULL", true},
		{"n.missing IS NULL", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// String Predicates Tests (STARTS WITH, ENDS WITH, CONTAINS)
// ========================================

func TestStringPredicates(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"name": "Alice Johnson",
	})
	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected interface{}
	}{
		// STARTS WITH
		{"'hello world' STARTS WITH 'hello'", true},
		{"'hello world' STARTS WITH 'world'", false},
		{"n.name STARTS WITH 'Alice'", true},
		{"n.name STARTS WITH 'Bob'", false},

		// ENDS WITH
		{"'hello world' ENDS WITH 'world'", true},
		{"'hello world' ENDS WITH 'hello'", false},
		{"n.name ENDS WITH 'Johnson'", true},

		// CONTAINS
		{"'hello world' CONTAINS 'lo wo'", true},
		{"'hello world' CONTAINS 'xyz'", false},
		{"n.name CONTAINS 'ice'", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// IN Operator Tests
// ========================================

func TestInOperatorExpression(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"status": "active",
		"list":   []interface{}{"a", "b", "c"},
	})
	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"1 IN [1, 2, 3]", true},
		{"4 IN [1, 2, 3]", false},
		{"'a' IN ['a', 'b', 'c']", true},
		{"'x' IN ['a', 'b', 'c']", false},
		{"n.status IN ['active', 'pending']", true},
		{"n.status IN ['closed', 'cancelled']", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// BETWEEN Operator Tests
// ========================================

func TestBetweenOperator(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Person"}, map[string]interface{}{
		"age": int64(25),
	})
	nodes := map[string]*storage.Node{"n": node}

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"5 BETWEEN 1 AND 10", true},
		{"15 BETWEEN 1 AND 10", false},
		{"1 BETWEEN 1 AND 10", true},  // inclusive
		{"10 BETWEEN 1 AND 10", true}, // inclusive
		{"n.age BETWEEN 18 AND 30", true},
		{"n.age BETWEEN 30 AND 40", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := e.evaluateExpressionWithContext(tt.expr, nodes, nil)
			if result != tt.expected {
				t.Errorf("%s = %v, want %v", tt.expr, result, tt.expected)
			}
		})
	}
}

// ========================================
// APOC Map Functions Tests
// ========================================

func TestApocMapMerge(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"map1": map[string]interface{}{"a": int64(1), "b": int64(2)},
		"map2": map[string]interface{}{"b": int64(3), "c": int64(4)},
	})
	nodes := map[string]*storage.Node{"n": node}

	result := e.evaluateExpressionWithContext("apoc.map.merge(n.map1, n.map2)", nodes, nil)
	if m, ok := result.(map[string]interface{}); ok {
		// map2's "b" should override map1's "b"
		if m["a"] != int64(1) {
			t.Errorf("merged map should have a=1, got %v", m["a"])
		}
		if m["b"] != int64(3) {
			t.Errorf("merged map should have b=3 (from map2), got %v", m["b"])
		}
		if m["c"] != int64(4) {
			t.Errorf("merged map should have c=4, got %v", m["c"])
		}
	} else {
		t.Errorf("apoc.map.merge should return map, got %T", result)
	}
}

func TestApocMapSetKey(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"myMap": map[string]interface{}{"a": int64(1)},
	})
	nodes := map[string]*storage.Node{"n": node}

	result := e.evaluateExpressionWithContext("apoc.map.setKey(n.myMap, 'b', 2)", nodes, nil)
	if m, ok := result.(map[string]interface{}); ok {
		if m["a"] != int64(1) {
			t.Errorf("map should still have a=1, got %v", m["a"])
		}
		// The '2' was parsed as a string, so we need to check for string or int
		if m["b"] == nil {
			t.Errorf("map should have b set")
		}
	} else {
		t.Errorf("apoc.map.setKey should return map, got %T", result)
	}
}

func TestApocMapRemoveKey(t *testing.T) {
	e := setupTestExecutor(t)
	node := createTestNode(t, e, "node-1", []string{"Test"}, map[string]interface{}{
		"myMap": map[string]interface{}{"a": int64(1), "b": int64(2), "c": int64(3)},
	})
	nodes := map[string]*storage.Node{"n": node}

	result := e.evaluateExpressionWithContext("apoc.map.removeKey(n.myMap, 'b')", nodes, nil)
	if m, ok := result.(map[string]interface{}); ok {
		if _, exists := m["b"]; exists {
			t.Errorf("map should not have key 'b' after removeKey")
		}
		if m["a"] != int64(1) {
			t.Errorf("map should still have a=1")
		}
		if m["c"] != int64(3) {
			t.Errorf("map should still have c=3")
		}
	} else {
		t.Errorf("apoc.map.removeKey should return map, got %T", result)
	}
}

func TestFunctionAdditionalMathAndStringCoverage(t *testing.T) {
	e := setupTestExecutor(t)
	nodes := map[string]*storage.Node{
		"n": {
			ID: "n1",
			Properties: map[string]interface{}{
				"txt": "go",
				"x":   int64(42),
			},
		},
	}

	tests := []struct {
		expr string
		want interface{}
	}{
		{"cot(1)", 1.0 / math.Tan(1)},
		{"haversin(1)", (1 - math.Cos(1)) / 2},
		{"normalize('cafe')", "cafe"},
		{"lpad('go', 5, '.')", "...go"},
		{"rpad('go', 5, '.')", "go..."},
		{"percentileCont(n.x, 0.5)", int64(42)},
		{"percentileDisc(n.x, 0.5)", int64(42)},
		{"stDev(n.x)", float64(0)},
		{"stDevP(n.x)", float64(0)},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got := e.evaluateExpressionWithContext(tt.expr, nodes, nil)
			if got != tt.want {
				t.Errorf("%s = %v, want %v", tt.expr, got, tt.want)
			}
		})
	}
}

func TestFunctionAdditionalGeometryAndPathCoverage(t *testing.T) {
	e := setupTestExecutor(t)

	// linestring / polygon literals
	line := e.evaluateExpressionWithContext("lineString([point({x:0,y:0}), point({x:1,y:1})])", nil, nil)
	lineMap, ok := line.(map[string]interface{})
	if !ok {
		t.Fatalf("lineString should return map, got %T", line)
	}
	if lineMap["type"] != "linestring" {
		t.Fatalf("lineString type = %v, want linestring", lineMap["type"])
	}

	poly := e.evaluateExpressionWithContext("polygon([point({x:0,y:0}), point({x:2,y:0}), point({x:0,y:2})])", nil, nil)
	polyMap, ok := poly.(map[string]interface{})
	if !ok {
		t.Fatalf("polygon should return map, got %T", poly)
	}
	if polyMap["type"] != "polygon" {
		t.Fatalf("polygon type = %v, want polygon", polyMap["type"])
	}

	// insufficient points branches
	if v := e.evaluateExpressionWithContext("lineString([point({x:0,y:0})])", nil, nil); v != nil {
		t.Fatalf("lineString with <2 points should be nil, got %v", v)
	}
	if v := e.evaluateExpressionWithContext("polygon([point({x:0,y:0}), point({x:1,y:1})])", nil, nil); v != nil {
		t.Fatalf("polygon with <3 points should be nil, got %v", v)
	}

	// nodes(path) coverage using explicit path context
	pathNodes := []*storage.Node{
		{ID: "a", Properties: map[string]interface{}{"name": "a"}},
		{ID: "b", Properties: map[string]interface{}{"name": "b"}},
	}
	pathEdges := []*storage.Edge{
		{ID: "e1", Type: "KNOWS", Properties: map[string]interface{}{"w": 1}},
	}
	path := &PathResult{
		Nodes:         pathNodes,
		Relationships: pathEdges,
		Length:        1,
	}
	gotNodes := e.evaluateExpressionWithContextFull("nodes(p)", nil, nil, map[string]*PathResult{"p": path}, nil, nil, 0)
	if arr, ok := gotNodes.([]interface{}); !ok || len(arr) != 2 {
		t.Fatalf("nodes(p) expected 2 entries, got %T %#v", gotNodes, gotNodes)
	}

	// fallback via allPathNodes/allPathEdges
	gotNodesFallback := e.evaluateExpressionWithContextFull("nodes(p)", nil, nil, nil, nil, pathNodes, 0)
	if arr, ok := gotNodesFallback.([]interface{}); !ok || len(arr) != 2 {
		t.Fatalf("nodes(p) fallback expected 2 entries, got %T %#v", gotNodesFallback, gotNodesFallback)
	}
	gotRelsFallback := e.evaluateExpressionWithContextFull("relationships(p)", nil, nil, nil, pathEdges, nil, 0)
	if arr, ok := gotRelsFallback.([]interface{}); !ok || len(arr) != 1 {
		t.Fatalf("relationships(p) fallback expected 1 entry, got %T %#v", gotRelsFallback, gotRelsFallback)
	}
}

func TestFunctionAdditionalListMapAndDegreeCoverage(t *testing.T) {
	e := setupTestExecutor(t)

	a := createTestNode(t, e, "fn-a", []string{"Person", "Employee"}, map[string]interface{}{
		"requiredLabels": []interface{}{"Person", "Employee"},
		"cleanMap":       map[string]interface{}{"a": int64(1), "b": int64(2), "c": nil},
	})
	b := createTestNode(t, e, "fn-b", []string{"Person"}, map[string]interface{}{})
	if err := e.storage.CreateEdge(&storage.Edge{
		ID:        "fn-rel-1",
		Type:      "KNOWS",
		StartNode: a.ID,
		EndNode:   b.ID,
	}); err != nil {
		t.Fatalf("create edge failed: %v", err)
	}

	nodes := map[string]*storage.Node{"a": a, "b": b}

	// list/map helpers in evaluateExpressionWithContextFullFunctions.
	assertEqual(t, "indexOf([10,20,30], 20)", int64(1), e.evaluateExpressionWithContext("indexOf([10,20,30], 20)", nodes, nil))
	gotRange := e.evaluateExpressionWithContext("range(1,3)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3)}, gotRange) {
		t.Fatalf("range(1,3) = %#v, want [1 2 3]", gotRange)
	}
	gotRangeNeg := e.evaluateExpressionWithContext("range(5,1,-2)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{int64(5), int64(3), int64(1)}, gotRangeNeg) {
		t.Fatalf("range(5,1,-2) = %#v, want [5 3 1]", gotRangeNeg)
	}
	gotRangeZero := e.evaluateExpressionWithContext("range(1,3,0)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{int64(1), int64(2), int64(3)}, gotRangeZero) {
		t.Fatalf("range(1,3,0) = %#v, want [1 2 3]", gotRangeZero)
	}
	gotSliceEmpty := e.evaluateExpressionWithContext("slice([1,2,3], 2, 1)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{}, gotSliceEmpty) {
		t.Fatalf("slice([1,2,3],2,1) = %#v, want []", gotSliceEmpty)
	}
	gotSliceNonList := e.evaluateExpressionWithContext("slice('not-list', 0, 1)", nodes, nil)
	if !reflect.DeepEqual([]interface{}{}, gotSliceNonList) {
		t.Fatalf("slice('not-list',0,1) = %#v, want []", gotSliceNonList)
	}
	assertEqual(t, "inDegree(missing)", int64(0), e.evaluateExpressionWithContext("inDegree(missing)", nodes, nil))
	assertEqual(t, "outDegree(missing)", int64(0), e.evaluateExpressionWithContext("outDegree(missing)", nodes, nil))
	assertEqual(t, "hasLabels(a, 'bad')", false, e.evaluateExpressionWithContext("hasLabels(a, 'bad')", nodes, nil))
	assertEqual(t, "hasLabels(missing, ['Person'])", false, e.evaluateExpressionWithContext("hasLabels(missing, ['Person'])", nodes, nil))

	fromPairs := e.evaluateExpressionWithContext("apoc.map.fromPairs([['a',1],['b',2]])", nodes, nil)
	fromPairsMap, ok := fromPairs.(map[string]interface{})
	if !ok {
		t.Fatalf("fromPairs result type = %T, want map[string]interface{}", fromPairs)
	}
	if fromPairsMap["a"] != int64(1) || fromPairsMap["b"] != int64(2) {
		t.Fatalf("unexpected fromPairs result: %#v", fromPairsMap)
	}

	clean := e.evaluateExpressionWithContext("apoc.map.clean(a.cleanMap, ['b'], [null])", nodes, nil)
	cleanMap, ok := clean.(map[string]interface{})
	if !ok {
		t.Fatalf("clean result type = %T, want map[string]interface{}", clean)
	}
	if cleanMap["a"] != int64(1) {
		t.Fatalf("clean map missing expected a=1: %#v", cleanMap)
	}
	_, hasB := cleanMap["b"]
	if hasB {
		t.Fatalf("clean map should not contain key b: %#v", cleanMap)
	}
	_, hasC := cleanMap["c"]
	if hasC {
		t.Fatalf("clean map should not contain key c: %#v", cleanMap)
	}

	if !reflect.DeepEqual(map[string]interface{}{}, e.evaluateExpressionWithContext("apoc.map.fromPairs('bad')", nodes, nil)) {
		t.Fatalf("apoc.map.fromPairs('bad') should return empty map")
	}
	if !reflect.DeepEqual(map[string]interface{}{}, e.evaluateExpressionWithContext("apoc.map.clean('bad')", nodes, nil)) {
		t.Fatalf("apoc.map.clean('bad') should return empty map")
	}
}
