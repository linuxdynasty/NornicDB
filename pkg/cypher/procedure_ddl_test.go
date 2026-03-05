package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateCallDropProcedureDDL(t *testing.T) {
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

	result, err := exec.Execute(ctx, "CALL nornic.touchUser('u-10', datetime()) YIELD u RETURN u.id, u.last_seen", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"u.id", "u.last_seen"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "u-10", result.Rows[0][0])
	require.NotNil(t, result.Rows[0][1])

	_, err = exec.Execute(ctx, "DROP PROCEDURE nornic.touchUser", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CALL nornic.touchUser('u-10', datetime())", nil)
	require.Error(t, err)
}

func TestPersistedProcedurePrecompiledOnStartup(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "test")
	exec1 := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec1.Execute(ctx, "CREATE (:User {id: 'u-1', active: false})", nil)
	require.NoError(t, err)

	_, err = exec1.Execute(ctx, `
CREATE OR REPLACE PROCEDURE nornic.activateUser($id)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.active = true
RETURN u
`, nil)
	require.NoError(t, err)

	// Simulate process restart/runtime reset.
	ClearUserProcedures()

	exec2 := NewStorageExecutor(store)
	result, err := exec2.Execute(ctx, "CALL nornic.activateUser('u-1') YIELD u RETURN u.active", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, true, result.Rows[0][0])
}

func TestProcedureCreateRejectedInsideActiveTransaction(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "BEGIN TRANSACTION", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE PROCEDURE nornic.bad() MODE READ AS RETURN 1 AS value", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not allowed inside an active transaction")

	_, _ = exec.Execute(ctx, "ROLLBACK", nil)
}
