package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindMatchMergeRelationshipSet_UsesChainBatchHotPath(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Function {uid: 'fn-source'}), (:Function {uid: 'fn-target'})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (source:Function {uid: row.caller_entity_id})
MATCH (target:Function {uid: row.callee_entity_id})
MERGE (source)-[rel:CALLS]->(target)
SET rel.confidence = 0.95,
    rel.reason = 'Parser and symbol analysis resolved a code call edge',
    rel.evidence_source = row.evidence_source,
    rel.call_kind = row.call_kind
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"caller_entity_id": "fn-source",
				"callee_entity_id": "fn-target",
				"evidence_source":  "parser",
				"call_kind":        "function",
			},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch, "expected PCG code-call shape to use chain batch hot path")

	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, "CALLS", edges[0].Type)
	require.Equal(t, 0.95, edges[0].Properties["confidence"])
	require.Equal(t, "parser", edges[0].Properties["evidence_source"])
	require.Equal(t, "function", edges[0].Properties["call_kind"])
}

func BenchmarkUnwindMatchMergeRelationshipSet_CodeCallShape(b *testing.B) {
	const rowCount = 512
	ctx := context.Background()
	rows := make([]map[string]interface{}, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		rows = append(rows, map[string]interface{}{
			"caller_entity_id": fmt.Sprintf("fn-source-%03d", i),
			"callee_entity_id": fmt.Sprintf("fn-target-%03d", i),
			"evidence_source":  "parser",
			"call_kind":        "function",
		})
	}

	query := `
UNWIND $rows AS row
MATCH (source:Function {uid: row.caller_entity_id})
MATCH (target:Function {uid: row.callee_entity_id})
MERGE (source)-[rel:CALLS]->(target)
SET rel.confidence = 0.95,
    rel.reason = 'Parser and symbol analysis resolved a code call edge',
    rel.evidence_source = row.evidence_source,
    rel.call_kind = row.call_kind
`

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		base := newTestMemoryEngine(b)
		store := storage.NewNamespacedEngine(base, fmt.Sprintf("bench-%d", i))
		exec := NewStorageExecutor(store)
		require.NoError(b, store.GetSchema().AddPropertyIndex("idx_function_uid", "Function", []string{"uid"}))
		for _, row := range rows {
			_, err := store.CreateNode(&storage.Node{
				ID:         storage.NodeID(fmt.Sprintf("%s-source", row["caller_entity_id"])),
				Labels:     []string{"Function"},
				Properties: map[string]interface{}{"uid": row["caller_entity_id"]},
			})
			require.NoError(b, err)
			_, err = store.CreateNode(&storage.Node{
				ID:         storage.NodeID(fmt.Sprintf("%s-target", row["callee_entity_id"])),
				Labels:     []string{"Function"},
				Properties: map[string]interface{}{"uid": row["callee_entity_id"]},
			})
			require.NoError(b, err)
		}

		b.StartTimer()
		_, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
		b.StopTimer()
		require.NoError(b, err)
		b.StartTimer()
	}
}
