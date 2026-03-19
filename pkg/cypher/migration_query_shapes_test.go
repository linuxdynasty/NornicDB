package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func seedTranslationJoinNodes(t *testing.T, store storage.Engine) {
	t.Helper()
	nodes := []*storage.Node{
		{
			ID:     "orig-k1",
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k1",
			},
		},
		{
			ID:     "orig-k2",
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k2",
			},
		},
		{
			ID:     "orig-k3",
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k3",
			},
		},
		{
			ID:     "tr-k1",
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k1",
			},
		},
		{
			ID:     "tr-k2",
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k2",
			},
		},
		{
			ID:     "tr-k3",
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"__tmpJoinKey": "k3",
			},
		},
	}

	for _, n := range nodes {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}
}

func TestMigrationShape_UnwindMatchMerge_ReturnCountAggregates(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	result, err := exec.Execute(ctx, `
UNWIND $keys AS k
MATCH (o:OriginalText {__tmpJoinKey: k})
MATCH (t:TranslatedText {__tmpJoinKey: k})
MERGE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS merged_pairs
`, map[string]interface{}{
		"keys": []interface{}{"k1", "k2"},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "count(*) should aggregate to one row")
	require.Len(t, result.Rows[0], 1)
	require.Equal(t, int64(2), result.Rows[0][0])

	verify, err := exec.Execute(
		ctx,
		"MATCH (:OriginalText)-[r:TRANSLATES_TO]->(:TranslatedText) RETURN count(r) AS c",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, int64(2), verify.Rows[0][0])
}

func TestMigrationShape_UnwindMatchCreate_CreatesBatchEdges(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	_, err := exec.Execute(ctx, `
UNWIND $keys AS k
MATCH (o:OriginalText {__tmpJoinKey: k})
MATCH (t:TranslatedText {__tmpJoinKey: k})
CREATE (o)-[:TRANSLATES_TO]->(t)
`, map[string]interface{}{"keys": []interface{}{"k1", "k2"}})
	require.NoError(t, err)

	verify, err := exec.Execute(
		ctx,
		"MATCH (:OriginalText)-[r:TRANSLATES_TO]->(:TranslatedText) RETURN count(r) AS c",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, int64(2), verify.Rows[0][0])
}

func TestMigrationShape_UnwindMatchRemoveProperty_Batched(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	// Baseline: direct REMOVE with property map should work.
	_, err := exec.Execute(ctx, "MATCH (n:OriginalText {__tmpJoinKey: 'k1'}) REMOVE n.__tmpJoinKey", nil)
	require.NoError(t, err)

	beforeBatch, err := exec.Execute(ctx, `
MATCH (n:OriginalText)
WHERE n.__tmpJoinKey IS NOT NULL
RETURN count(n) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, beforeBatch.Rows, 1)
	require.Equal(t, int64(2), beforeBatch.Rows[0][0], "k1 should already be removed")

	_, err = exec.Execute(ctx, `
UNWIND $keys AS k
MATCH (n:OriginalText {__tmpJoinKey: k})
REMOVE n.__tmpJoinKey
`, map[string]interface{}{
		"keys": []interface{}{"k2", "k3"},
	})
	require.NoError(t, err)

	remaining, err := exec.Execute(ctx, `
MATCH (n:OriginalText)
WHERE n.__tmpJoinKey IS NOT NULL
RETURN count(n) AS c
	`, nil)
	require.NoError(t, err)
	require.Len(t, remaining.Rows, 1)
	require.Equal(t, int64(0), remaining.Rows[0][0], "all keys should be removed")
}

func TestMigrationShape_MatchWhereInRemoveProperty_Batched(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	_, err := exec.Execute(ctx, `
MATCH (n:OriginalText)
WHERE n.__tmpJoinKey IN $keys
REMOVE n.__tmpJoinKey
`, map[string]interface{}{"keys": []interface{}{"k1", "k2", "k3"}})
	require.NoError(t, err)

	remaining, err := exec.Execute(ctx, `
MATCH (n:OriginalText)
WHERE n.__tmpJoinKey IS NOT NULL
RETURN count(n) AS c
	`, nil)
	require.NoError(t, err)
	require.Len(t, remaining.Rows, 1)
	require.Equal(t, int64(0), remaining.Rows[0][0], "all keys should be removed")
}

func TestMigrationShape_MatchInCreate_ReturnCountAlias(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.__tmpJoinKey IN $keys
  AND t.__tmpJoinKey = o.__tmpJoinKey
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS created_pairs
`, map[string]interface{}{"keys": []interface{}{"k1", "k2"}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(2), res.Rows[0][0])
}

func TestMigrationShape_MatchInCreate_WithNotRelationshipModifier(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	first, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.__tmpJoinKey IN $keys
  AND t.__tmpJoinKey = o.__tmpJoinKey
  AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS created_pairs
`, map[string]interface{}{"keys": []interface{}{"k1", "k2"}})
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.Equal(t, int64(2), first.Rows[0][0])

	second, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.__tmpJoinKey IN $keys
  AND t.__tmpJoinKey = o.__tmpJoinKey
  AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS created_pairs
`, map[string]interface{}{"keys": []interface{}{"k1", "k2"}})
	require.NoError(t, err)
	require.Len(t, second.Rows, 1)
	require.Equal(t, int64(0), second.Rows[0][0])
}

func TestMigrationShape_MatchInMerge_ReturnCountAlias(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedTranslationJoinNodes(t, store)

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText), (t:TranslatedText)
WHERE o.__tmpJoinKey IN $keys
  AND t.__tmpJoinKey = o.__tmpJoinKey
MERGE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS matched_pairs
`, map[string]interface{}{"keys": []interface{}{"k1", "k2"}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(2), res.Rows[0][0])
}

func TestMigrationDDL_CreateIndexVariants_ParseAndApply(t *testing.T) {
	cases := []struct {
		name  string
		query string
	}{
		{
			name:  "named_if_not_exists",
			query: "CREATE INDEX original_tmp_join_idx IF NOT EXISTS FOR (o:OriginalText) ON (o.__tmpJoinKey)",
		},
		{
			name:  "unnamed_if_not_exists",
			query: "CREATE INDEX IF NOT EXISTS FOR (t:TranslatedText) ON (t.__tmpJoinKey)",
		},
		{
			name:  "named_plain",
			query: "CREATE INDEX translated_tmp_join_idx FOR (t:TranslatedText) ON (t.__tmpJoinKey)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseStore := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			_, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err, tc.query)

			showRes, err := exec.Execute(ctx, "SHOW INDEXES", nil)
			require.NoError(t, err)
			require.NotNil(t, showRes)
			require.NotEmpty(t, showRes.Rows)
		})
	}
}
