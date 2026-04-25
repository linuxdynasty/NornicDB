package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindMatchRelationshipSetBatch_PCGSQLRelationshipShape(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, stmt := range []string{
		"CREATE CONSTRAINT sql_table_uid_unique IF NOT EXISTS FOR (n:SqlTable) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT sql_view_uid_unique IF NOT EXISTS FOR (n:SqlView) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT sql_column_uid_unique IF NOT EXISTS FOR (n:SqlColumn) REQUIRE n.uid IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}

	for _, stmt := range []string{
		"CREATE (:SqlTable {uid: 'table:users', name: 'users'})",
		"CREATE (:SqlView {uid: 'view:active_users', name: 'active_users'})",
		"CREATE (:SqlColumn {uid: 'column:users.id', name: 'id'})",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MATCH (source:SqlTable|SqlView|SqlFunction|SqlTrigger|SqlIndex|SqlColumn {uid: row.source_entity_id})
MATCH (target:SqlTable|SqlView|SqlFunction|SqlTrigger|SqlIndex|SqlColumn {uid: row.target_entity_id})
MERGE (source)-[rel:REFERENCES_TABLE]->(target)
SET rel.confidence = 0.95,
    rel.reason = 'SQL entity metadata resolved a table reference edge',
    rel.evidence_source = row.evidence_source
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"source_entity_id": "view:active_users",
				"target_entity_id": "table:users",
				"evidence_source":  "parser-sql",
			},
			{
				"source_entity_id": "column:users.id",
				"target_entity_id": "table:users",
				"evidence_source":  "parser-sql",
			},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch)
	require.True(t, exec.LastHotPathTrace().MergeSchemaLookupUsed)
	require.False(t, exec.LastHotPathTrace().MergeScanFallbackUsed)

	res, err := exec.Execute(ctx, `
MATCH (source)-[rel:REFERENCES_TABLE]->(target)
RETURN source.uid, target.uid, rel.confidence, rel.evidence_source
ORDER BY source.uid
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"column:users.id", "table:users", 0.95, "parser-sql"},
		{"view:active_users", "table:users", 0.95, "parser-sql"},
	}, res.Rows)
}

func TestUnwindMatchReadDoesNotUseMutationBatchPath(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:SqlTable {uid: 'table:users'})", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MATCH (n:SqlTable {uid: row.uid})
RETURN count(n) AS matched
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{"uid": "table:users"},
			{"uid": "table:missing"},
		},
	})
	require.NoError(t, err)
	require.False(t, exec.LastHotPathTrace().UnwindMergeChainBatch)
	require.Equal(t, [][]interface{}{{int64(1)}}, res.Rows)
}
