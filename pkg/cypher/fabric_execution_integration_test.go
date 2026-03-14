package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecute_FabricCallUseChain_OnCompositeConstituents(t *testing.T) {
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

	trStore, err := mgr.GetStorage("caremark_tr")
	require.NoError(t, err)
	txtStore, err := mgr.GetStorage("caremark_txt")
	require.NoError(t, err)

	_, err = trStore.CreateNode(&storage.Node{ID: "t-1", Labels: []string{"Translation"}, Properties: map[string]interface{}{
		"id":      "t-1",
		"textKey": "orders.where",
	}})
	require.NoError(t, err)
	_, err = txtStore.CreateNode(&storage.Node{ID: "tt-1", Labels: []string{"TranslationText"}, Properties: map[string]interface{}{
		"translationId": "t-1",
		"text":          "Where are my orders?",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE caremark
CALL {
  USE caremark.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId, t.textKey AS textKey
}
CALL {
  USE caremark.txt
  MATCH (tt:TranslationText)
  RETURN count(tt) AS textCount
}
RETURN translationId, textKey, textCount
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Rows)

	found := false
	for _, row := range res.Rows {
		if len(row) < 3 {
			continue
		}
		if id, ok := row[0].(string); ok && id == "t-1" {
			found = true
			require.Equal(t, "orders.where", row[1])
			// count(tt) can come back as int64/int depending execution path.
			switch v := row[2].(type) {
			case int64:
				require.Equal(t, int64(1), v)
			case int:
				require.Equal(t, 1, v)
			default:
				t.Fatalf("unexpected textCount type: %T (%v)", row[2], row[2])
			}
		}
	}
	require.True(t, found, "expected row for translation t-1")
}
