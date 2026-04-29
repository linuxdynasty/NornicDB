package cypher

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
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
	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMergeChainBatch, "expected PCG code-call shape to use chain batch hot path")
	require.True(t, trace.UnwindMergeChainBulkRels, "expected PCG code-call shape to bulk-create new relationships")

	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, "CALLS", edges[0].Type)
	require.Equal(t, 0.95, edges[0].Properties["confidence"])
	require.Equal(t, "parser", edges[0].Properties["evidence_source"])
	require.Equal(t, "function", edges[0].Properties["call_kind"])
}

func TestUnwindMatchMergeRelationshipSet_ProfilesChainBatchWhenEnabled(t *testing.T) {
	var logs bytes.Buffer
	originalWriter := log.Writer()
	originalFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(originalWriter)
		log.SetFlags(originalFlags)
	})
	t.Setenv("NORNICDB_PCG_CHAIN_PROFILE", "1")

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

	output := logs.String()
	require.Contains(t, output, "pcg_unwind_merge_chain_profile")
	require.Contains(t, output, "input_rows=1")
	require.Contains(t, output, "processed_rows=1")
	require.Contains(t, output, "bulk_create_rows=1")
	require.Contains(t, output, "rel_lookup_misses=1")
	require.False(t, strings.Contains(output, "\n\n"), "profile logging should emit compact single-line records")
}

func TestUnwindMatchMergeRelationshipSet_BulkPathDeduplicatesPendingRelationships(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Function {uid: 'fn-source'}), (:Function {uid: 'fn-target'})`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (source:Function {uid: row.caller_entity_id})
MATCH (target:Function {uid: row.callee_entity_id})
MERGE (source)-[rel:CALLS]->(target)
SET rel.confidence = row.confidence,
    rel.evidence_source = row.evidence_source,
    rel.call_kind = row.call_kind
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"caller_entity_id": "fn-source",
				"callee_entity_id": "fn-target",
				"confidence":       0.5,
				"evidence_source":  "parser",
				"call_kind":        "first",
			},
			{
				"caller_entity_id": "fn-source",
				"callee_entity_id": "fn-target",
				"confidence":       0.95,
				"evidence_source":  "parser",
				"call_kind":        "second",
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, result.Stats.RelationshipsCreated)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBulkRels, "expected duplicate pending edge to flush through bulk path")

	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, 0.95, edges[0].Properties["confidence"])
	require.Equal(t, "second", edges[0].Properties["call_kind"])
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
		require.NoError(b, store.GetSchema().AddUniqueConstraint("function_uid_unique", "Function", "uid"))
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

func BenchmarkUnwindMatchMergeRelationshipSet_CodeCallShapeBadgerAccumulating(b *testing.B) {
	const (
		batchSize  = 1000
		batchCount = 5
		rowCount   = batchSize * batchCount
	)
	ctx := context.Background()
	rows := make([]map[string]interface{}, 0, rowCount)
	nodes := make([]*storage.Node, 0, rowCount*2)
	for i := 0; i < rowCount; i++ {
		callerID := fmt.Sprintf("fn-source-%05d", i)
		calleeID := fmt.Sprintf("fn-target-%05d", i)
		rows = append(rows, map[string]interface{}{
			"caller_entity_id": callerID,
			"callee_entity_id": calleeID,
			"evidence_source":  "parser",
			"call_kind":        "function",
		})
		nodes = append(nodes,
			&storage.Node{
				ID:         storage.NodeID(callerID + "-node"),
				Labels:     []string{"Function"},
				Properties: map[string]interface{}{"uid": callerID},
			},
			&storage.Node{
				ID:         storage.NodeID(calleeID + "-node"),
				Labels:     []string{"Function"},
				Properties: map[string]interface{}{"uid": calleeID},
			},
		)
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

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		base, err := storage.NewBadgerEngine(b.TempDir())
		require.NoError(b, err)
		store := storage.NewNamespacedEngine(base, fmt.Sprintf("bench-%d", i))
		exec := NewStorageExecutor(store)
		require.NoError(b, store.GetSchema().AddUniqueConstraint("function_uid_unique", "Function", "uid"))
		for _, node := range nodes {
			_, err := store.CreateNode(node)
			require.NoError(b, err)
		}
		b.Cleanup(func() {
			require.NoError(b, store.Close())
		})
		b.StartTimer()

		for start := 0; start < len(rows); start += batchSize {
			end := start + batchSize
			if end > len(rows) {
				end = len(rows)
			}
			_, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows[start:end]})
			require.NoError(b, err)
		}

		b.StopTimer()
		edgeCount, err := store.EdgeCount()
		require.NoError(b, err)
		require.Equal(b, int64(rowCount), edgeCount)
	}
}
