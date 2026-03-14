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

func TestExecute_FabricNestedCallUseChain_OnCompositeConstituents(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("caremark_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("caremark", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "caremark_tr", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("caremark_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "t-2", Labels: []string{"Translation"}, Properties: map[string]interface{}{
		"id":      "t-2",
		"textKey": "nested.use",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE caremark
CALL {
  CALL {
    USE caremark.tr
    MATCH (t:Translation {id: "t-2"})
    RETURN t.id AS translationId, t.textKey AS textKey
  }
  RETURN translationId, textKey
}
RETURN translationId, textKey
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Rows)

	found := false
	for _, row := range res.Rows {
		if len(row) < 2 {
			continue
		}
		if id, ok := row[0].(string); ok && id == "t-2" {
			found = true
			require.Equal(t, "nested.use", row[1])
		}
	}
	require.True(t, found, "expected row for nested translation t-2")
}

func TestExecute_FabricCallUseSubquery_OutOfScopeUseFails(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("caremark_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("caremark", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "caremark_tr", Type: "local", AccessMode: "read_write"},
	}))
	require.NoError(t, mgr.CreateDatabase("other_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("other", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "other_tr", Type: "local", AccessMode: "read_write"},
	}))

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE caremark
CALL {
  USE other.tr
  RETURN 1 AS x
}
RETURN x
`

	_, err = exec.Execute(context.Background(), query, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid USE target")
}

func TestExecute_FabricCorrelatedCallUseChain_EmptyOuterRowsStillHasColumns(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("caremark_tr"))
	require.NoError(t, mgr.CreateDatabase("caremark_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "caremark_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "caremark_txt", Type: "local", AccessMode: "read_write"},
	}))

	txtStore, err := mgr.GetStorage("caremark_txt")
	require.NoError(t, err)
	_, err = txtStore.CreateNode(&storage.Node{ID: "tt-1", Labels: []string{"TranslationText"}, Properties: map[string]interface{}{
		"translationId": "missing",
		"text":          "orphan",
	}})
	require.NoError(t, err)

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	query := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId, t.textKey AS textKey, t.textKey128 AS textKey128
}
CALL {
  WITH translationId
  USE translations.txr
  MATCH (tt:TranslationText)
  WHERE tt.translationId = translationId
  RETURN collect(tt) AS texts
}
RETURN translationId, textKey, textKey128, texts;
`

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []string{"translationId", "textKey", "textKey128", "texts"}, res.Columns)
	require.Empty(t, res.Rows)
}
