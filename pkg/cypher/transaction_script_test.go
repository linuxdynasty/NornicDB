package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionScriptBeginTransactionCommit(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:User {id: 'u-10', age: 21, last_seen: null})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CREATE OR REPLACE PROCEDURE nornic.touchUser($id, $ts)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.last_seen = $ts
RETURN u
`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
BEGIN TRANSACTION
CALL nornic.touchUser('u-10', datetime())
YIELD u
RETURN u.id, u.last_seen
COMMIT
`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "u-10", result.Rows[0][0])
	require.NotNil(t, result.Rows[0][1])
}

func TestTransactionScriptBeginShorthandEquivalent(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:User {id: 'u-11', age: 21, last_seen: null})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CREATE OR REPLACE PROCEDURE nornic.touchUser($id, $ts)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.last_seen = $ts
RETURN u
`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
BEGIN
CALL nornic.touchUser('u-11', datetime())
YIELD u
RETURN u.id, u.last_seen
COMMIT
`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "u-11", result.Rows[0][0])
	require.NotNil(t, result.Rows[0][1])
}

func TestTransactionScriptCaseRollback(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:User {id: 'u-12', age: 10, last_seen: null})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CREATE OR REPLACE PROCEDURE nornic.touchUser($id, $ts)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.last_seen = $ts
RETURN u
`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
BEGIN TRANSACTION
CALL nornic.touchUser('u-12', datetime())
YIELD u
CASE
  WHEN u.age < 18 THEN ROLLBACK
  ELSE
    RETURN u.id, u.last_seen
COMMIT
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"status"}, result.Columns)
	require.Equal(t, "Transaction rolled back", result.Rows[0][0])

	check, err := exec.Execute(ctx, "MATCH (u:User {id: 'u-12'}) RETURN u.last_seen", nil)
	require.NoError(t, err)
	require.Len(t, check.Rows, 1)
	require.Nil(t, check.Rows[0][0])
}

func TestTransactionScriptHelpers_ParseAndMapBuilders(t *testing.T) {
	body, ok := stripBeginTransactionPrefix("BEGIN TRANSACTION RETURN 1 COMMIT")
	require.True(t, ok)
	assert.Equal(t, "RETURN 1 COMMIT", body)

	body, ok = stripBeginTransactionPrefix("BEGIN RETURN 1 COMMIT")
	require.True(t, ok)
	assert.Equal(t, "RETURN 1 COMMIT", body)

	_, ok = stripBeginTransactionPrefix("MATCH (n) RETURN n")
	assert.False(t, ok)

	q, action, ok := splitTransactionScriptTailAction("RETURN 1 COMMIT")
	require.True(t, ok)
	assert.Equal(t, "RETURN 1", q)
	assert.Equal(t, "COMMIT", action)

	q, action, ok = splitTransactionScriptTailAction("RETURN 1 ROLLBACK")
	require.True(t, ok)
	assert.Equal(t, "RETURN 1", q)
	assert.Equal(t, "ROLLBACK", action)

	_, _, ok = splitTransactionScriptTailAction("COMMIT")
	assert.False(t, ok)

	n := &storage.Node{ID: "n1", Labels: []string{"Node"}, Properties: map[string]interface{}{"x": 1}}
	e := &storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n1", Type: "LOOP"}
	nodes, rels := buildRowGraphContext([]string{"n", "r", "value"}, []interface{}{n, e, 42})
	assert.Contains(t, nodes, "n")
	assert.Contains(t, rels, "r")
	assert.Equal(t, n, nodes["n"])
	assert.Equal(t, e, rels["r"])

	vals := buildRowValueMap([]string{"a", "b", "c"}, []interface{}{1, "two"})
	assert.Equal(t, 1, vals["a"])
	assert.Equal(t, "two", vals["b"])
	_, exists := vals["c"]
	assert.False(t, exists)
}
