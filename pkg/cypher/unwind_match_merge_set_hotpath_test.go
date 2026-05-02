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

func TestUnwindMatchNodeSet_UsesChainBatchHotPathWithoutCreatingMissingNodes(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddPropertyIndex("idx_function_uid", "Function", []string{"uid"}))
	for _, uid := range []string{"fn-existing-1", "fn-existing-2"} {
		_, err := exec.Execute(ctx, fmt.Sprintf(`
CREATE (:Function {uid: '%s', repo_id: 'repo-before', language: 'go'})
`, uid), nil)
		require.NoError(t, err)
	}
	if got := store.GetSchema().PropertyIndexLookup("Function", "uid", "fn-existing-1"); len(got) != 1 {
		nodes, nodeErr := store.GetNodesByLabel("Function")
		require.NoError(t, nodeErr)
		observed := make([]map[string]interface{}, 0, len(nodes))
		for _, node := range nodes {
			observed = append(observed, map[string]interface{}{
				"id":         node.ID,
				"labels":     node.Labels,
				"properties": node.Properties,
			})
		}
		t.Fatalf("expected Function uid index to be populated, lookup=%v nodes=%#v", got, observed)
	}

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (n:Function {uid: row.entity_id})
SET n.repo_id = row.repo_id,
    n.language = row.language,
    n.semantic_kind = row.semantic_kind
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"entity_id": "fn-existing-1", "repo_id": "repo-after", "language": "typescript", "semantic_kind": "function"},
			{"entity_id": "fn-missing", "repo_id": "repo-after", "language": "typescript", "semantic_kind": "function"},
			{"entity_id": "fn-existing-2", "repo_id": "repo-after", "language": "typescript", "semantic_kind": "function"},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch, "expected PCG semantic MATCH/SET shape to use chain batch hot path")

	nodes, err := store.GetNodesByLabel("Function")
	require.NoError(t, err)
	require.Len(t, nodes, 2, "MATCH/SET must not create missing canonical semantic nodes")
	for _, node := range nodes {
		require.Equal(t, "repo-after", node.Properties["repo_id"])
		require.Equal(t, "typescript", node.Properties["language"])
		require.Equal(t, "function", node.Properties["semantic_kind"])
	}
}

func TestUnwindMergeNodeSet_EvaluatesRowExpressionIdentity(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddPropertyIndex("idx_function_uid", "Function", []string{"uid"}))
	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (n:Function {uid: row.entity_id})
SET n.repo_id = row.repo_id,
    n.language = row.language,
    n.semantic_kind = row.semantic_kind
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"entity_id": "fn-created-1", "repo_id": "repo-1", "language": "go", "semantic_kind": "function"},
			{"entity_id": "fn-created-2", "repo_id": "repo-2", "language": "typescript", "semantic_kind": "function"},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch, "expected PCG canonical semantic MERGE/SET shape to use chain batch hot path")

	nodes, err := store.GetNodesByLabel("Function")
	require.NoError(t, err)
	require.Len(t, nodes, 2, "each row identity must create a distinct Function node")

	byUID := make(map[interface{}]*storage.Node, len(nodes))
	for _, node := range nodes {
		byUID[node.Properties["uid"]] = node
	}
	require.NotContains(t, byUID, "row.entity_id", "row expression must not be stored as a literal uid")
	require.Equal(t, "repo-1", byUID["fn-created-1"].Properties["repo_id"])
	require.Equal(t, "go", byUID["fn-created-1"].Properties["language"])
	require.Equal(t, "repo-2", byUID["fn-created-2"].Properties["repo_id"])
	require.Equal(t, "typescript", byUID["fn-created-2"].Properties["language"])
}

func TestUnwindMergeNodeSetMap_EvaluatesRowExpressionIdentity(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddPropertyIndex("idx_function_uid", "Function", []string{"uid"}))
	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (n:Function {uid: row.entity_id})
SET n += row.props
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id": "fn-created-1",
				"props": map[string]interface{}{
					"id":       "fn-created-1",
					"repo_id":  "repo-1",
					"language": "go",
				},
			},
			{
				"entity_id": "fn-created-2",
				"props": map[string]interface{}{
					"id":       "fn-created-2",
					"repo_id":  "repo-2",
					"language": "typescript",
				},
			},
		},
	})
	require.NoError(t, err)

	nodes, err := store.GetNodesByLabel("Function")
	require.NoError(t, err)
	require.Len(t, nodes, 2, "canonical entity rows must create a distinct node per row identity")

	byUID := make(map[interface{}]*storage.Node, len(nodes))
	for _, node := range nodes {
		byUID[node.Properties["uid"]] = node
	}
	require.NotContains(t, byUID, "row.entity_id", "fallback path must not store row expression as a literal uid")
	require.Equal(t, "repo-1", byUID["fn-created-1"].Properties["repo_id"])
	require.Equal(t, "go", byUID["fn-created-1"].Properties["language"])
	require.Equal(t, "repo-2", byUID["fn-created-2"].Properties["repo_id"])
	require.Equal(t, "typescript", byUID["fn-created-2"].Properties["language"])
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
