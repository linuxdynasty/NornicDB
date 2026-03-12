package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateExpression_FunctionUtilityBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test"))
	nodes := map[string]*storage.Node{
		"n": {
			ID:     "n1",
			Labels: []string{"T"},
			Properties: map[string]interface{}{
				"name": "  Alice  ",
				"list": []interface{}{"a", nil, 1},
			},
		},
	}

	// toStringList
	assert.Equal(t, []interface{}{"a", nil, "1"}, exec.evaluateExpressionWithContext("toStringList(n.list)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext("toStringList(n.name)", nodes, nil))

	// valueType
	assert.Equal(t, "NULL", exec.evaluateExpressionWithContext("valueType(null)", nodes, nil))
	assert.Equal(t, "BOOLEAN", exec.evaluateExpressionWithContext("valueType(true)", nodes, nil))
	assert.Equal(t, "INTEGER", exec.evaluateExpressionWithContext("valueType(1)", nodes, nil))
	assert.Equal(t, "FLOAT", exec.evaluateExpressionWithContext("valueType(1.5)", nodes, nil))
	assert.Equal(t, "STRING", exec.evaluateExpressionWithContext("valueType('x')", nodes, nil))
	assert.Equal(t, "LIST", exec.evaluateExpressionWithContext("valueType([1,2])", nodes, nil))
	assert.Equal(t, "MAP", exec.evaluateExpressionWithContext("valueType({a:1})", nodes, nil))

	// aggregation passthrough in expression context
	assert.EqualValues(t, 7, exec.evaluateExpressionWithContext("sum(7)", nodes, nil))
	assert.EqualValues(t, 8, exec.evaluateExpressionWithContext("avg(8)", nodes, nil))
	assert.EqualValues(t, 9, exec.evaluateExpressionWithContext("min(9)", nodes, nil))
	assert.EqualValues(t, 10, exec.evaluateExpressionWithContext("max(10)", nodes, nil))
	assert.Equal(t, []interface{}{}, exec.evaluateExpressionWithContext("collect(null)", nodes, nil))
	assert.Equal(t, []interface{}{int64(1)}, exec.evaluateExpressionWithContext("collect(1)", nodes, nil))

	// aliases
	assert.Equal(t, "alice", exec.evaluateExpressionWithContext("lower('ALICE')", nodes, nil))
	assert.Equal(t, "ALICE", exec.evaluateExpressionWithContext("upper('alice')", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext("lower(1)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext("upper(1)", nodes, nil))

	// trim family
	assert.Equal(t, "Alice", exec.evaluateExpressionWithContext("trim(n.name)", nodes, nil))
	assert.Equal(t, "Alice  ", exec.evaluateExpressionWithContext("ltrim(n.name)", nodes, nil))
	assert.Equal(t, "  Alice", exec.evaluateExpressionWithContext("rtrim(n.name)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext("trim(42)", nodes, nil))

	// replace / split
	assert.Equal(t, "a-c", exec.evaluateExpressionWithContext("replace('a-b','b','c')", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext("replace('a','b')", nodes, nil))
	splitVal := exec.evaluateExpressionWithContext("split('a,b,c',',')", nodes, nil)
	splitList, ok := splitVal.([]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{"a", "b", "c"}, splitList)
	assert.Nil(t, exec.evaluateExpressionWithContext("split('abc')", nodes, nil))

	// substring / left / right
	assert.Equal(t, "bc", exec.evaluateExpressionWithContext("substring('abcd',1,2)", nodes, nil))
	assert.Equal(t, "", exec.evaluateExpressionWithContext("substring('abcd',99)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext("substring('abcd')", nodes, nil))
	assert.Equal(t, "ab", exec.evaluateExpressionWithContext("left('abcd',2)", nodes, nil))
	assert.Equal(t, "abcd", exec.evaluateExpressionWithContext("left('abcd',99)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext("left('abcd')", nodes, nil))
	assert.Equal(t, "cd", exec.evaluateExpressionWithContext("right('abcd',2)", nodes, nil))
	assert.Equal(t, "abcd", exec.evaluateExpressionWithContext("right('abcd',99)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext("right('abcd')", nodes, nil))

	// lpad / rpad
	assert.Equal(t, "  ab", exec.evaluateExpressionWithContext("lpad('ab',4)", nodes, nil))
	assert.Equal(t, "xxab", exec.evaluateExpressionWithContext("lpad('ab',4,'x')", nodes, nil))
	assert.Equal(t, "ab  ", exec.evaluateExpressionWithContext("rpad('ab',4)", nodes, nil))
	assert.Equal(t, "abxx", exec.evaluateExpressionWithContext("rpad('ab',4,'x')", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext("lpad('ab','bad')", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext("rpad('ab','bad')", nodes, nil))
}
