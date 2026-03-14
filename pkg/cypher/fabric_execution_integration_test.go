package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecute_FabricCallUseChain_OnCompositeConstituents(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("nornic_cmp_a", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txt", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	txtStore, err := mgr.GetStorage("nornic_txt")
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
USE nornic_cmp_a
CALL {
  USE nornic_cmp_a.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId, t.textKey AS textKey
}
CALL {
  USE nornic_cmp_a.txt
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

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("nornic_cmp_b", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
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
USE nornic_cmp_b
CALL {
  CALL {
    USE nornic_cmp_b.tr
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

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateCompositeDatabase("nornic_cmp_c", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
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
USE nornic_cmp_c
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

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	txtStore, err := mgr.GetStorage("nornic_txt")
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

func TestExecute_FabricCorrelatedCallUseChain_AppliesWherePerOuterRow(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-1", Labels: []string{"Translation"}, Properties: map[string]interface{}{
		"textKey":    "k1",
		"textKey128": "h1",
	}})
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-2", Labels: []string{"Translation"}, Properties: map[string]interface{}{
		"textKey":    "k2",
		"textKey128": "h2",
	}})
	require.NoError(t, err)

	txrStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-1", Labels: []string{"TranslationText"}, Properties: map[string]interface{}{
		"textKey128": "h1",
		"text":       "one",
	}})
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-2", Labels: []string{"TranslationText"}, Properties: map[string]interface{}{
		"textKey128": "h2",
		"text":       "two",
	}})
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-x", Labels: []string{"TranslationText"}, Properties: map[string]interface{}{
		"textKey128": "hx",
		"text":       "other",
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
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
  LIMIT 2
}
CALL {
  WITH textKey128
  USE translations.txr
  MATCH (tt:TranslationText)
  WHERE tt.textKey128 = textKey128
  RETURN collect(tt) AS texts
}
RETURN textKey, textKey128, texts
`

	catalog, err := exec.buildFabricCatalog()
	require.NoError(t, err)
	frag, err := fabric.NewFabricPlanner(catalog).Plan(query, "nornic")
	require.NoError(t, err)
	var fragmentExecs []*fabric.FragmentExec
	var walk func(fabric.Fragment)
	walk = func(f fabric.Fragment) {
		switch n := f.(type) {
		case *fabric.FragmentExec:
			fragmentExecs = append(fragmentExecs, n)
		case *fabric.FragmentApply:
			walk(n.Input)
			walk(n.Inner)
		case *fabric.FragmentUnion:
			walk(n.LHS)
			walk(n.RHS)
		}
	}
	walk(frag)
	hasTXR := false
	hasTrailing := false
	for _, ex := range fragmentExecs {
		if ex.GraphName == "translations.txr" {
			hasTXR = true
			require.NotContains(t, ex.Query, "USE translations.txr")
		}
		if ex.GraphName == "translations" && strings.HasPrefix(strings.TrimSpace(strings.ToUpper(ex.Query)), "RETURN ") {
			hasTrailing = true
		}
	}
	require.True(t, hasTXR, "planner must route second CALL block to translations.txr")
	require.True(t, hasTrailing, "planner must keep trailing RETURN as separate fragment")

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"textKey", "textKey128", "texts"}, res.Columns)
	require.Len(t, res.Rows, 2)
	byKey := make(map[string][]interface{}, len(res.Rows))
	for _, row := range res.Rows {
		require.Len(t, row, 3)
		textKey, _ := row[0].(string)
		textKey128, _ := row[1].(string)
		require.NotEqual(t, "coalesce(textKey, textKey128)", textKey)
		require.NotEqual(t, "textKey128", textKey128)
		texts, ok := row[2].([]interface{})
		require.True(t, ok, "texts should be a list")
		byKey[textKey128] = texts
	}
	require.Contains(t, byKey, "h1")
	require.Contains(t, byKey, "h2")
	require.Len(t, byKey["h1"], 1)
	require.Len(t, byKey["h2"], 1)
}

func TestExecute_FabricCorrelatedCallUseChain_ListComprehensionJoin(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("translations", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txr", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	trStore, err := mgr.GetStorage("nornic_tr")
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-1", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"textKey":    "k1",
		"textKey128": "h1",
	}})
	require.NoError(t, err)
	_, err = trStore.CreateNode(&storage.Node{ID: "tr-2", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"textKey":    "k2",
		"textKey128": "h2",
	}})
	require.NoError(t, err)

	txrStore, err := mgr.GetStorage("nornic_txt")
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-1", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"textKey128": "h1",
		"text":       "one",
	}})
	require.NoError(t, err)
	_, err = txrStore.CreateNode(&storage.Node{ID: "txr-2", Labels: []string{"MongoDocument"}, Properties: map[string]interface{}{
		"textKey128": "h2",
		"text":       "two",
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
  MATCH (t:MongoDocument)
  WHERE t.textKey128 IS NOT NULL
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
  ORDER BY t.textKey128
  LIMIT 25
}
WITH collect({textKey: textKey, textKey128: textKey128}) AS rows
CALL {
  WITH rows
  UNWIND rows AS r
  WITH collect(DISTINCT r.textKey128) AS keys
  USE translations.txr
  MATCH (tt:MongoDocument)
  WHERE tt.textKey128 IN keys
  RETURN tt.textKey128 AS k, collect(tt) AS texts
}
WITH rows, collect({k: k, texts: texts}) AS grouped
UNWIND rows AS r
WITH r, [g IN grouped WHERE g.k = r.textKey128][0] AS hit
RETURN r.textKey AS textKey, r.textKey128 AS textKey128, coalesce(hit.texts, []) AS texts
`

	keysProbe := `
USE translations
CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  WHERE t.textKey128 IS NOT NULL
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
  ORDER BY t.textKey128
  LIMIT 25
}
WITH collect({textKey: textKey, textKey128: textKey128}) AS rows
UNWIND rows AS r
WITH collect(DISTINCT r.textKey128) AS keys
RETURN keys
`
	keysRes, err := exec.Execute(context.Background(), keysProbe, nil)
	require.NoError(t, err)
	require.Len(t, keysRes.Rows, 1)
	keysList, ok := keysRes.Rows[0][0].([]interface{})
	require.True(t, ok, "unexpected keys type=%T value=%#v", keysRes.Rows[0][0], keysRes.Rows[0][0])
	require.ElementsMatch(t, []interface{}{"h1", "h2"}, keysList)

	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"textKey", "textKey128", "texts"}, res.Columns)
	require.Len(t, res.Rows, 2)
	for _, row := range res.Rows {
		require.Len(t, row, 3)
		require.NotEmpty(t, row[0])
		require.NotEmpty(t, row[1])
		texts, ok := row[2].([]interface{})
		require.True(t, ok, "unexpected texts type=%T value=%#v", row[2], row[2])
		require.Len(t, texts, 1, "expected exactly one matching collected row per key")
	}
}
