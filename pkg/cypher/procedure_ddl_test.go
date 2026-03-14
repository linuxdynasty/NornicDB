package cypher

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCreateCallDropProcedureDDL(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
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

	base := newTestMemoryEngine(t)
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

	base := newTestMemoryEngine(t)
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

func TestLoadPersistedProcedures_BranchCoverage(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// no record property -> ignored
	_, err := store.CreateNode(&storage.Node{
		ID:         procedureCatalogNodeID("nornic.norecord"),
		Labels:     []string{procedureCatalogLabel},
		Properties: map[string]interface{}{"name": "nornic.norecord"},
	})
	require.NoError(t, err)

	// invalid base64 -> ignored
	_, err = store.CreateNode(&storage.Node{
		ID:     procedureCatalogNodeID("nornic.badb64"),
		Labels: []string{procedureCatalogLabel},
		Properties: map[string]interface{}{
			"record": "not-valid-base64",
		},
	})
	require.NoError(t, err)

	// invalid msgpack payload -> ignored
	_, err = store.CreateNode(&storage.Node{
		ID:     procedureCatalogNodeID("nornic.badpack"),
		Labels: []string{procedureCatalogLabel},
		Properties: map[string]interface{}{
			"record": base64.StdEncoding.EncodeToString([]byte("bad-msgpack")),
		},
	})
	require.NoError(t, err)

	// compile failure (invalid mode) -> ignored
	invalidMode := persistedProcedureRecord{
		Name:      "nornic.invalidMode",
		Mode:      "NOPE",
		Body:      "RETURN 1 AS value",
		Signature: buildProcedureSignature("nornic.invalidMode", nil),
	}
	invalidBlob, err := msgpack.Marshal(invalidMode)
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     procedureCatalogNodeID("nornic.invalidMode"),
		Labels: []string{procedureCatalogLabel},
		Properties: map[string]interface{}{
			"record": base64.StdEncoding.EncodeToString(invalidBlob),
		},
	})
	require.NoError(t, err)

	valid := persistedProcedureRecord{
		Name:      "nornic.loadedProc",
		Mode:      "READ",
		Body:      "RETURN 42 AS value",
		Signature: buildProcedureSignature("nornic.loadedProc", nil),
	}
	validBlob, err := msgpack.Marshal(valid)
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     procedureCatalogNodeID("nornic.loadedProc"),
		Labels: []string{procedureCatalogLabel},
		Properties: map[string]interface{}{
			"record": base64.StdEncoding.EncodeToString(validBlob),
		},
	})
	require.NoError(t, err)

	validSecond := persistedProcedureRecord{
		Name:      "nornic.loadedProcTwo",
		Mode:      "READ",
		Body:      "RETURN 7 AS value",
		Signature: buildProcedureSignature("nornic.loadedProcTwo", nil),
	}
	validSecondBlob, err := msgpack.Marshal(validSecond)
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     procedureCatalogNodeID("nornic.loadedProcTwo"),
		Labels: []string{procedureCatalogLabel},
		Properties: map[string]interface{}{
			"record": base64.StdEncoding.EncodeToString(validSecondBlob),
		},
	})
	require.NoError(t, err)

	ClearUserProcedures()
	require.NoError(t, exec.loadPersistedProcedures())

	res, err := exec.Execute(ctx, "CALL nornic.loadedProc()", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(42), res.Rows[0][0])

	res, err = exec.Execute(ctx, "CALL nornic.loadedProcTwo()", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(7), res.Rows[0][0])

	_, err = exec.Execute(ctx, "CALL nornic.invalidMode()", nil)
	require.Error(t, err)
}

func TestDropProcedure_ErrorBranches(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.executeDropProcedure(ctx, "DROP PROCEDURE")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid DROP PROCEDURE syntax")

	_, err = exec.executeDropProcedure(ctx, "DROP PROCEDURE nornic.missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to drop procedure")

	_, err = exec.Execute(ctx, "BEGIN TRANSACTION", nil)
	require.NoError(t, err)
	_, err = exec.executeDropProcedure(ctx, "DROP PROCEDURE nornic.any")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not allowed inside an active transaction")
	_, _ = exec.Execute(ctx, "ROLLBACK", nil)
}
