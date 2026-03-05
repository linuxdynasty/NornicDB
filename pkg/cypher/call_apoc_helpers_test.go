package cypher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApocHelpers_FindMatchingParen(t *testing.T) {
	e := &StorageExecutor{}
	assert.Equal(t, -1, e.findMatchingParen("abc", 0))
	assert.Equal(t, -1, e.findMatchingParen("(abc", 1))
	assert.Equal(t, 5, e.findMatchingParen("(a(b))", 0))
	assert.Equal(t, 8, e.findMatchingParen("('x)';(a))", 6))
}

func TestApocHelpers_ExtractQuotedString(t *testing.T) {
	e := &StorageExecutor{}
	v, rem, err := e.extractQuotedString(" 'hello' ,x")
	require.NoError(t, err)
	assert.Equal(t, "hello", v)
	assert.Equal(t, " ,x", rem)

	v, rem, err = e.extractQuotedString("\"a\\\"b\" tail")
	require.NoError(t, err)
	assert.Equal(t, "a\\\"b", v)
	assert.Equal(t, " tail", rem)

	_, _, err = e.extractQuotedString("")
	assert.Error(t, err)
	_, _, err = e.extractQuotedString("abc")
	assert.Error(t, err)
	_, _, err = e.extractQuotedString("'abc")
	assert.Error(t, err)
}

func TestApocHelpers_FindMatchingBrace(t *testing.T) {
	e := &StorageExecutor{}
	assert.Equal(t, -1, e.findMatchingBrace("abc", 0))
	assert.Equal(t, 6, e.findMatchingBrace("{a:{b}}", 0))
	assert.Equal(t, 12, e.findMatchingBrace("{a:'x}y',b:1}", 0))
}

func TestApocHelpers_ParseApocCypherRunArgs(t *testing.T) {
	e := &StorageExecutor{}
	q, p, err := e.parseApocCypherRunArgs("'MATCH (n) RETURN n', {limit: 10, enabled: true}")
	require.NoError(t, err)
	assert.Equal(t, "MATCH (n) RETURN n", q)
	assert.Equal(t, int64(10), p["limit"])
	assert.Equal(t, true, p["enabled"])

	q, p, err = e.parseApocCypherRunArgs("'RETURN 1', NULL")
	require.NoError(t, err)
	assert.Equal(t, "RETURN 1", q)
	assert.Empty(t, p)

	_, _, err = e.parseApocCypherRunArgs("no-quote")
	assert.Error(t, err)
	_, _, err = e.parseApocCypherRunArgs("'unclosed")
	assert.Error(t, err)
}

func TestApocHelpers_ParseApocPeriodicIterateArgs(t *testing.T) {
	e := &StorageExecutor{}
	it, act, cfg, err := e.parseApocPeriodicIterateArgs("'MATCH (n) RETURN n', 'SET n.x=1', {batchSize: 1000, parallel: true}")
	require.NoError(t, err)
	assert.Equal(t, "MATCH (n) RETURN n", it)
	assert.Equal(t, "SET n.x=1", act)
	assert.Equal(t, int64(1000), cfg["batchSize"])
	assert.Equal(t, true, cfg["parallel"])

	_, _, _, err = e.parseApocPeriodicIterateArgs("'MATCH (n)', 'SET n.x=1'")
	require.NoError(t, err)

	_, _, _, err = e.parseApocPeriodicIterateArgs("'MATCH (n)' 'SET n.x=1'")
	assert.Error(t, err)
}

func TestApocHelpers_SplitBySemicolon(t *testing.T) {
	e := &StorageExecutor{}
	parts := e.splitBySemicolon("RETURN 1; RETURN ';'; RETURN 3")
	assert.Equal(t, []string{"RETURN 1", " RETURN ';'", " RETURN 3"}, parts)
	assert.Equal(t, []string{"RETURN 1"}, e.splitBySemicolon("RETURN 1"))
}

func TestApocHelpers_ExtractProcedureName(t *testing.T) {
	assert.Equal(t, "db.labels", extractProcedureName("CALL db.labels()"))
	assert.Equal(t, "apoc.cypher.run", extractProcedureName("call apoc.cypher.run('RETURN 1',{})"))
	long := "THIS TEXT HAS NO PROC KEYWORD AND SHOULD FALL BACK TO TRUNCATED QUERY STRING BEHAVIOR"
	got := extractProcedureName(long)
	assert.Len(t, got, 63)
	assert.Equal(t, long[:60]+"...", got)
}
