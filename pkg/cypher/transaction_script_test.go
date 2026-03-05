package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
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
