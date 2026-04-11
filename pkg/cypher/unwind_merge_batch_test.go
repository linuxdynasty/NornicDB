package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindCollectDistinctProjection_UsesHelperRoute(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	rows := []map[string]interface{}{
		{"textKey128": "k1"},
		{"textKey128": "k2"},
		{"textKey128": "k1"},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
WITH collect(DISTINCT row.textKey128) AS keys
RETURN keys
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"keys"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	values, ok := res.Rows[0][0].([]interface{})
	require.True(t, ok)
	require.Len(t, values, 2)
	require.Equal(t, "k1", values[0])
	require.Equal(t, "k2", values[1])
}

func TestParseUnwindCollectDistinctProjection(t *testing.T) {
	exec := &StorageExecutor{}
	plan, ok := exec.parseUnwindCollectDistinctProjection("WITH collect(DISTINCT row.textKey128) AS keys RETURN keys")
	require.True(t, ok)
	require.Equal(t, "row", plan.srcVar)
	require.Equal(t, "textKey128", plan.prop)
	require.Equal(t, "keys", plan.alias)

	_, ok = exec.parseUnwindCollectDistinctProjection("WITH collect(DISTINCT row.textKey128) AS keys RETURN other")
	require.False(t, ok)
}

func TestUnwindMergeBatch_HopUpsertShape(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	hops := make([]map[string]interface{}, 0, 72)
	for row := 0; row < 12; row++ {
		for depth := 1; depth <= 6; depth++ {
			hops = append(hops, map[string]interface{}{
				"hopId": fmt.Sprintf("benchhop-%03d:%d", row, depth),
				"runID": "bench-deep-hop-v1",
			})
		}
	}

	res, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": hops})
	require.NoError(t, err)
	require.Equal(t, []string{"prepared"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(72), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("BenchmarkHop")
	require.NoError(t, err)
	require.Len(t, nodes, 72)
	for _, n := range nodes {
		require.Equal(t, "bench-deep-hop-v1", n.Properties["benchmarkRun"])
	}
}

func TestUnwindMergeBatch_HopUpsertUpdatesExisting(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	first := []map[string]interface{}{
		{"hopId": "benchhop-000:1", "runID": "v1"},
		{"hopId": "benchhop-000:2", "runID": "v1"},
	}
	_, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": first})
	require.NoError(t, err)

	second := []map[string]interface{}{
		{"hopId": "benchhop-000:1", "runID": "v2"},
		{"hopId": "benchhop-000:2", "runID": "v2"},
	}
	res, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": second})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("BenchmarkHop")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, n := range nodes {
		require.Equal(t, "v2", n.Properties["benchmarkRun"])
	}
}

func TestUnwindMergeBatch_HopUpsertDuplicateKeys_LastRowWinsAndCountPreserved(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Ensure index-aware path can be used for existing lookups.
	require.NoError(t, store.GetSchema().AddPropertyIndex("idx_benchhop_hopid", "BenchmarkHop", []string{"hopId"}))

	rows := []map[string]interface{}{
		{"hopId": "benchhop-dupe:1", "runID": "v1"},
		{"hopId": "benchhop-dupe:2", "runID": "v1"},
		{"hopId": "benchhop-dupe:1", "runID": "v2"}, // duplicate key, should win
	}

	res, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": rows})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	// Cypher semantics: count(h) in this shape counts input rows, including duplicates.
	require.Equal(t, int64(3), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("BenchmarkHop")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, n := range nodes {
		if n.Properties["hopId"] == "benchhop-dupe:1" {
			require.Equal(t, "v2", n.Properties["benchmarkRun"])
		}
	}
}

func TestUnwindMergeBatch_MultiPropertyMerge_UsesHotPath(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddCompositeIndex("idx_translated_key_lang", "TranslatedText", []string{"translationId", "language"}))

	rows := []map[string]interface{}{
		{"translationId": "src-1", "language": "es", "translatedText": "hola"},
		{"translationId": "src-2", "language": "fr", "translatedText": "bonjour"},
		{"translationId": "src-1", "language": "es", "translatedText": "hola-2"},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (t:TranslatedText {translationId: row.translationId, language: row.language})
ON CREATE SET t.translatedText = row.translatedText
ON MATCH SET t.translatedText = row.translatedText
RETURN count(t) AS prepared
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"prepared"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(3), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("TranslatedText")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, n := range nodes {
		if n.Properties["translationId"] == "src-1" && n.Properties["language"] == "es" {
			require.Equal(t, "hola-2", n.Properties["translatedText"])
		}
	}
}

func TestUnwindMergeBatch_MultiPropertyMerge_DistinctTypesDoNotCollapse(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddCompositeIndex("idx_type_safe_translation_key_lang", "TranslatedText", []string{"translationId", "language"}))

	rows := []map[string]interface{}{
		{"translationId": 1, "language": "es", "translatedText": "numeric-one"},
		{"translationId": "1", "language": "es", "translatedText": "string-one"},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (t:TranslatedText {translationId: row.translationId, language: row.language})
ON CREATE SET t.translatedText = row.translatedText
ON MATCH SET t.translatedText = row.translatedText
RETURN count(t) AS prepared
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"prepared"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("TranslatedText")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	seenNumeric := false
	seenString := false
	for _, n := range nodes {
		if n.Properties["language"] != "es" {
			continue
		}
		switch v := n.Properties["translationId"].(type) {
		case int:
			if v == 1 {
				seenNumeric = true
				require.Equal(t, "numeric-one", n.Properties["translatedText"])
			}
		case int64:
			if v == 1 {
				seenNumeric = true
				require.Equal(t, "numeric-one", n.Properties["translatedText"])
			}
		case string:
			if v == "1" {
				seenString = true
				require.Equal(t, "string-one", n.Properties["translatedText"])
			}
		}
	}
	require.True(t, seenNumeric, "expected numeric translationId row to remain distinct")
	require.True(t, seenString, "expected string translationId row to remain distinct")
}

func TestUnwindMergeBatch_MultiPropertyMerge_NestedMapValuesDoNotCollapse(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddCompositeIndex("idx_nested_map_translation_key_lang", "TranslatedText", []string{"translationId", "language"}))

	rows := []map[string]interface{}{
		{
			"translationId":  map[string]interface{}{"id": 1, "meta": map[string]interface{}{"kind": "n"}},
			"language":       "es",
			"translatedText": "nested-numeric",
		},
		{
			"translationId":  map[string]interface{}{"id": "1", "meta": map[string]interface{}{"kind": "n"}},
			"language":       "es",
			"translatedText": "nested-string",
		},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (t:TranslatedText {translationId: row.translationId, language: row.language})
ON CREATE SET t.translatedText = row.translatedText
ON MATCH SET t.translatedText = row.translatedText
RETURN count(t) AS prepared
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch)

	nodes, err := store.GetNodesByLabel("TranslatedText")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	seenNumeric := false
	seenString := false
	for _, n := range nodes {
		m, ok := n.Properties["translationId"].(map[string]interface{})
		if !ok {
			continue
		}
		if inner, ok := m["id"]; ok {
			switch v := inner.(type) {
			case int:
				if v == 1 {
					seenNumeric = true
					require.Equal(t, "nested-numeric", n.Properties["translatedText"])
				}
			case int64:
				if v == 1 {
					seenNumeric = true
					require.Equal(t, "nested-numeric", n.Properties["translatedText"])
				}
			case string:
				if v == "1" {
					seenString = true
					require.Equal(t, "nested-string", n.Properties["translatedText"])
				}
			}
		}
	}
	require.True(t, seenNumeric, "expected nested numeric map key to remain distinct")
	require.True(t, seenString, "expected nested string map key to remain distinct")
}

func TestUnwindMergeBatch_MultiPropertyMerge_NestedSliceAndNilValuesDoNotCollapse(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddCompositeIndex("idx_nested_slice_translation_key_lang", "TranslatedText", []string{"translationId", "language"}))

	rows := []map[string]interface{}{
		{
			"translationId":  []interface{}{1, "alpha", nil, map[string]interface{}{"flag": true}},
			"language":       "es",
			"translatedText": "slice-numeric",
		},
		{
			"translationId":  []interface{}{"1", "alpha", nil, map[string]interface{}{"flag": true}},
			"language":       "es",
			"translatedText": "slice-string",
		},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (t:TranslatedText {translationId: row.translationId, language: row.language})
ON CREATE SET t.translatedText = row.translatedText
ON MATCH SET t.translatedText = row.translatedText
RETURN count(t) AS prepared
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch)

	nodes, err := store.GetNodesByLabel("TranslatedText")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	seenNumeric := false
	seenString := false
	for _, n := range nodes {
		list, ok := n.Properties["translationId"].([]interface{})
		if !ok || len(list) != 4 {
			continue
		}
		switch v := list[0].(type) {
		case int:
			if v == 1 {
				seenNumeric = true
				require.Nil(t, list[2])
				require.Equal(t, "slice-numeric", n.Properties["translatedText"])
			}
		case int64:
			if v == 1 {
				seenNumeric = true
				require.Nil(t, list[2])
				require.Equal(t, "slice-numeric", n.Properties["translatedText"])
			}
		case string:
			if v == "1" {
				seenString = true
				require.Nil(t, list[2])
				require.Equal(t, "slice-string", n.Properties["translatedText"])
			}
		}
	}
	require.True(t, seenNumeric, "expected nested slice numeric key to remain distinct")
	require.True(t, seenString, "expected nested slice string key to remain distinct")
}

func TestGenericMerge_MultiPropertyLookup_UsesCompositeSchemaPath(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	_, err := eng.CreateNode(&storage.Node{
		ID:     "nornic:translated-1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"translationId":  "src-1",
			"language":       "es",
			"translatedText": "hola",
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(eng)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_tt_translation_id FOR (n:TranslatedText) ON (n.translationId)", nil)
	require.NoError(t, err)
	require.NoError(t, eng.GetSchema().AddCompositeIndex("idx_tt_translation_lang", "TranslatedText", []string{"translationId", "language"}))
	eng.forbidScan = true

	res, err := exec.Execute(context.Background(), `
MERGE (t:TranslatedText {translationId: 'src-1', language: 'es'})
ON MATCH SET t.translatedText = 'hola-2'
RETURN t.translatedText AS translatedText
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"translatedText"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "hola-2", res.Rows[0][0])
}
