package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindMatchSetBatch_UsesIndexedLookupWithoutCreatingMissingNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	schema := store.GetSchema()
	require.NotNil(t, schema)
	require.NoError(t, schema.AddPropertyIndex("idx_function_uid", "Function", []string{"uid"}))

	existingID, err := store.CreateNode(&storage.Node{
		ID:     "fn-1",
		Labels: []string{"Function"},
		Properties: map[string]interface{}{
			"uid":  "content-entity:e_1",
			"name": "before",
		},
	})
	require.NoError(t, err)
	require.NoError(t, schema.PropertyIndexInsert("Function", "uid", existingID, "content-entity:e_1"))

	exec := NewStorageExecutor(&noScanMergeLookupEngine{Engine: store})
	_, err = exec.Execute(context.Background(), `
UNWIND $rows AS row
MATCH (n:Function {uid: row.entity_id})
SET n.name = row.name,
    n.semantic_kind = row.semantic_kind
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id":     "content-entity:e_1",
				"name":          "after",
				"semantic_kind": "function",
			},
			{
				"entity_id":     "content-entity:e_missing",
				"name":          "missing",
				"semantic_kind": "function",
			},
		},
	})
	require.NoError(t, err)

	node, err := store.GetNode(existingID)
	require.NoError(t, err)
	require.Equal(t, "after", node.Properties["name"])
	require.Equal(t, "function", node.Properties["semantic_kind"])

	missing := schema.PropertyIndexLookup("Function", "uid", "content-entity:e_missing")
	require.Empty(t, missing, "MATCH/SET hot path must not create nodes for missing matches")
}
