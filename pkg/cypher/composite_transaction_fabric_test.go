package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeExplicitTx_SecondWriteShardRejected(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("shard_a"))
	require.NoError(t, mgr.CreateDatabase("shard_b"))
	require.NoError(t, mgr.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "shard_a", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "shard_b", Type: "local", AccessMode: "read_write"},
	}))

	cmpStore, err := mgr.GetStorage("cmp")
	require.NoError(t, err)

	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()
	t.Logf("composite executor storage type: %T", exec.storage)

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)
	require.IsType(t, &fabric.FabricTransaction{}, exec.txContext.tx)

	firstWrite := "CALL { USE cmp.a CREATE (n:W {id: '1'}) RETURN count(n) AS c } RETURN c"
	secondWrite := "CALL { USE cmp.b CREATE (n:W {id: '2'}) RETURN count(n) AS c } RETURN c"
	require.Equal(t, "cmp.a", extractFirstUseGraph(firstWrite))
	require.Equal(t, "cmp.b", extractFirstUseGraph(secondWrite))

	_, err = exec.Execute(ctx, firstWrite, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, secondWrite, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Writing to more than one database per transaction is not allowed")

	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
}

func TestCompositeExplicitTx_BeginCommitLifecycle(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("shard_c"))
	require.NoError(t, mgr.CreateCompositeDatabase("cmp2", []multidb.ConstituentRef{
		{Alias: "c", DatabaseName: "shard_c", Type: "local", AccessMode: "read_write"},
	}))

	cmpStore, err := mgr.GetStorage("cmp2")
	require.NoError(t, err)

	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CALL { USE cmp2.c CREATE (n:T {id: 'ok'}) RETURN count(n) AS c } RETURN c", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}

func TestCompositeExplicitTx_DocumentationExamples(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("caremark_tr"))
	require.NoError(t, mgr.CreateDatabase("caremark_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("caremark", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "caremark_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txt", DatabaseName: "caremark_txt", Type: "local", AccessMode: "read_write"},
	}))

	cmpStore, err := mgr.GetStorage("caremark")
	require.NoError(t, err)
	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	// Docs example: one write shard per explicit transaction succeeds.
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CALL {
  USE caremark.tr
  CREATE (t:Translation {id: "tr-tx-1", textKey: "ORDERS_WHERE"})
  RETURN count(t) AS c
}
RETURN c
`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	// Docs example: second write shard in same explicit transaction is rejected.
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CALL {
  USE caremark.tr
  CREATE (t:Translation {id: "tr-tx-2"})
  RETURN count(t) AS c
}
RETURN c
`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CALL {
  USE caremark.txt
  CREATE (tt:TranslationText {translationId: "tr-tx-2", locale: "en-US", value: "Where are my orders?"})
  RETURN count(tt) AS c
}
RETURN c
`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Writing to more than one database per transaction is not allowed")
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
}
