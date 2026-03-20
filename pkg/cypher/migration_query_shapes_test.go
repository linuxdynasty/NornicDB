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

func TestMigrationShape_UnwindMatchMergeSetMap_ComplexRowsAndClauses(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (f1:File {path: '/repo/a.py'})
CREATE (f2:File {path: '/repo/b.py'})
`, nil)
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{
			"file_path":   "/repo/a.py",
			"line_number": int64(10),
			"name":        "alpha",
			"props": map[string]interface{}{
				"lang":             "python",
				"context":          "_find_classes",
				"class_context":    "TypescriptTreeSitterParser",
				"is_dependency":    false,
				"text_with_braces": "payload {'a':1, 'b':{'c':[1,2,3]}} with (parentheses) should stay literal",
			},
		},
		{
			"file_path":   "/repo/a.py",
			"line_number": int64(11),
			"name":        "beta",
			"props": map[string]interface{}{
				"lang":          "python",
				"context":       "_find_classes",
				"is_dependency": true,
			},
		},
		{
			"file_path":   "/repo/b.py",
			"line_number": int64(3),
			"name":        "gamma",
			"props": map[string]interface{}{
				"lang":               "typescript",
				"context":            "walk (ast)",
				"is_dependency":      false,
				"parenthetical_text": "example(value) and nested(call(arg))",
			},
		},
	}

	shape := `
UNWIND $rows AS row
MATCH (f:File {path: row.file_path})
MERGE (n:Variable {name: row.name, path: row.file_path, line_number: row.line_number})
SET n += row.props
MERGE (f)-[:CONTAINS]->(n)
WITH f, n
RETURN f.path AS file_path, n.name AS variable_name, n.line_number AS line_number, n.lang AS lang
`
	result, err := exec.Execute(ctx, shape, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"file_path", "variable_name", "line_number", "lang"}, result.Columns)
	require.Len(t, result.Rows, 3)

	ordered, err := exec.Execute(ctx, `
MATCH (f:File)-[:CONTAINS]->(n:Variable)
RETURN f.path AS file_path, n.name AS variable_name, n.line_number AS line_number, n.lang AS lang
ORDER BY file_path, line_number, variable_name
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"file_path", "variable_name", "line_number", "lang"}, ordered.Columns)
	require.Len(t, ordered.Rows, 3)
	require.Equal(t, []interface{}{"/repo/a.py", "alpha", int64(10), "python"}, ordered.Rows[0])
	require.Equal(t, []interface{}{"/repo/a.py", "beta", int64(11), "python"}, ordered.Rows[1])
	require.Equal(t, []interface{}{"/repo/b.py", "gamma", int64(3), "typescript"}, ordered.Rows[2])

	verifyParens, err := exec.Execute(ctx, `
MATCH (n:Variable {name: 'gamma'})
RETURN n.context AS context, n.parenthetical_text AS parenthetical_text
`, nil)
	require.NoError(t, err)
	require.Len(t, verifyParens.Rows, 1)
	require.Equal(t, "walk (ast)", verifyParens.Rows[0][0])
	require.Equal(t, "example(value) and nested(call(arg))", verifyParens.Rows[0][1])

	counts, err := exec.Execute(ctx, `
MATCH (f:File)-[r:CONTAINS]->(n:Variable)
RETURN count(f) AS files_joined, count(r) AS contains_edges, count(n) AS variable_nodes
`, nil)
	require.NoError(t, err)
	require.Len(t, counts.Rows, 1)
	require.Equal(t, int64(3), counts.Rows[0][0])
	require.Equal(t, int64(3), counts.Rows[0][1])
	require.Equal(t, int64(3), counts.Rows[0][2])
}

func TestMigrationShape_UnwindMatchMergeSetMap_IdempotentAndPaginatedProjection(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (f:File {path: '/repo/a.py'})
`, nil)
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{
			"file_path":   "/repo/a.py",
			"line_number": int64(10),
			"name":        "alpha",
			"props": map[string]interface{}{
				"lang":  "python",
				"notes": "alpha(value)",
			},
		},
		{
			"file_path":   "/repo/a.py",
			"line_number": int64(20),
			"name":        "beta",
			"props": map[string]interface{}{
				"lang":  "python",
				"notes": "beta(value)",
			},
		},
		{
			"file_path":   "/repo/a.py",
			"line_number": int64(30),
			"name":        "gamma",
			"props": map[string]interface{}{
				"lang":  "python",
				"notes": "gamma(value)",
			},
		},
	}

	query := `
UNWIND $rows AS row
MATCH (f:File {path: row.file_path})
MERGE (n:Variable {name: row.name, path: row.file_path, line_number: row.line_number})
SET n += row.props
MERGE (f)-[:CONTAINS]->(n)
RETURN count(*) AS processed_rows
`
	first, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.Equal(t, int64(3), first.Rows[0][0])

	second, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Len(t, second.Rows, 1)
	require.Equal(t, int64(3), second.Rows[0][0])

	edgeCount, err := exec.Execute(ctx, `
MATCH (:File)-[r:CONTAINS]->(:Variable)
RETURN count(r) AS edge_count
`, nil)
	require.NoError(t, err)
	require.Len(t, edgeCount.Rows, 1)
	require.Equal(t, int64(3), edgeCount.Rows[0][0], "MERGE should keep edge creation idempotent")

	paginated, err := exec.Execute(ctx, `
MATCH (f:File)-[:CONTAINS]->(n:Variable)
WITH f.path AS file_path, n.name AS variable_name, n.line_number AS line_number
RETURN file_path, variable_name, line_number
ORDER BY line_number ASC, variable_name ASC
SKIP 1
LIMIT 1
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"file_path", "variable_name", "line_number"}, paginated.Columns)
	require.Len(t, paginated.Rows, 1)
	require.Equal(t, []interface{}{"/repo/a.py", "beta", int64(20)}, paginated.Rows[0])

	notes, err := exec.Execute(ctx, `
MATCH (n:Variable)
WHERE n.notes = 'beta(value)'
RETURN count(n) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, notes.Rows, 1)
	require.Equal(t, int64(1), notes.Rows[0][0])
}

func TestMigrationDDL_OpenCypherCompatibleVariants_FullStatements(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE INDEX original_text_idx IF NOT EXISTS
FOR (o:OriginalText)
ON (o.originalText)
`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
CREATE INDEX translated_lang_idx IF NOT EXISTS
FOR (t:TranslatedText)
ON (t.language)
`, nil)
	require.NoError(t, err)

	beforeDrop, err := exec.Execute(ctx, "SHOW INDEXES", nil)
	require.NoError(t, err)
	require.NotNil(t, beforeDrop)
	require.GreaterOrEqual(t, len(beforeDrop.Rows), 2)

	showRes, err := exec.Execute(ctx, `
SHOW INDEXES
YIELD name, state, type, entityType, labelsOrTypes, properties
RETURN name, state, type, entityType, labelsOrTypes, properties
ORDER BY name
`, nil)
	require.NoError(t, err)
	require.NotEmpty(t, showRes.Rows)

	_, err = exec.Execute(ctx, "DROP INDEX translated_lang_idx IF EXISTS", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "DROP INDEX original_text_idx IF EXISTS", nil)
	require.NoError(t, err)

	afterDrop, err := exec.Execute(ctx, "SHOW INDEXES", nil)
	require.NoError(t, err)
	require.NotNil(t, afterDrop)
	require.LessOrEqual(t, len(afterDrop.Rows), len(beforeDrop.Rows))
}
