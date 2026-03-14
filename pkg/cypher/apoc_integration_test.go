package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAPOCFunctionsIntegration tests APOC functions work end-to-end
func TestAPOCFunctionsIntegration(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30})
		CREATE (b:Person {name: 'Bob', age: 25})
		CREATE (c:Person {name: 'Carol', age: 35})
		CREATE (a)-[:KNOWS]->(b)
		CREATE (b)-[:KNOWS]->(c)
		CREATE (a)-[:KNOWS]->(c)
	`, nil)
	require.NoError(t, err)

	t.Run("nornicdb.version", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL nornicdb.version()", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		t.Logf("Version: %v", result.Rows[0])
	})

	t.Run("nornicdb.stats", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL nornicdb.stats()", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		t.Logf("Stats: %v", result.Rows[0])
	})

	t.Run("db.labels", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.labels()", nil)
		require.NoError(t, err)
		assert.True(t, len(result.Rows) > 0)
		t.Logf("Labels: %v", result.Rows)
	})

	t.Run("db.relationshipTypes", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.relationshipTypes()", nil)
		require.NoError(t, err)
		t.Logf("Relationship Types: %v", result.Rows)
	})

	t.Run("apoc.algo.pageRank", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person)
			WITH collect(n) as nodes
			CALL apoc.algo.pageRank(nodes, {iterations: 20})
			YIELD node, score
			RETURN node.name as name, score
			ORDER BY score DESC
		`, nil)
		require.NoError(t, err)
		t.Logf("PageRank results: %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v: score=%v", row[0], row[1])
		}
	})

	t.Run("apoc.algo.betweenness", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person)
			WITH collect(n) as nodes
			CALL apoc.algo.betweenness(nodes)
			YIELD node, score
			RETURN node.name as name, score
		`, nil)
		require.NoError(t, err)
		t.Logf("Betweenness results: %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v: score=%v", row[0], row[1])
		}
	})

	t.Run("apoc.neighbors.tohop", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'})
			CALL apoc.neighbors.tohop(start, 'KNOWS>', 2)
			YIELD node
			RETURN node.name as neighbor
		`, nil)
		require.NoError(t, err)
		t.Logf("Neighbors within 2 hops: %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v", row[0])
		}
	})

	t.Run("dbms.procedures", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL dbms.procedures()", nil)
		require.NoError(t, err)
		t.Logf("Available procedures: %d", len(result.Rows))
		// Show first 10
		for i, row := range result.Rows {
			if i >= 10 {
				t.Logf("  ... and %d more", len(result.Rows)-10)
				break
			}
			t.Logf("  %v", row[0])
		}
	})
}

func TestAPOCPathHelpers_BranchCoverage(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err := store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Root"}, Properties: map[string]interface{}{"id": "root"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Keep"}, Properties: map[string]interface{}{"id": "keep"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "n3", Labels: []string{"Archive"}, Properties: map[string]interface{}{"id": "arch"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "REL"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e2", StartNode: "n2", EndNode: "n3", Type: "OTHER"}))

	types, dir := parseRelationshipFilter(">REL|OTHER")
	assert.ElementsMatch(t, []string{"REL", "OTHER"}, types)
	assert.Equal(t, "outgoing", dir)
	inc, exc, term := parseLabelFilter("+Keep|-Archive|/Root")
	assert.Equal(t, []string{"Keep"}, inc)
	assert.Equal(t, []string{"Archive"}, exc)
	assert.Equal(t, []string{"Root"}, term)

	assert.Equal(t, "root", exec.extractStartNodeID("MATCH (n {id: 'root'}) CALL apoc.path.subgraphNodes(n,{})"))
	assert.Equal(t, "keep", exec.extractStartNodeID("MATCH (n) WHERE n.id = 'keep' CALL apoc.path.subgraphNodes(n,{})"))
	assert.Equal(t, "", exec.extractStartNodeID("CALL apoc.path.subgraphNodes($nodeId,{})"))
	assert.Equal(t, "*", exec.extractStartNodeID("CALL apoc.path.subgraphNodes(n,{})"))

	assert.True(t, passesLabelFilter(&storage.Node{Labels: []string{"Keep"}}, []string{"Keep"}, nil))
	assert.False(t, passesLabelFilter(&storage.Node{Labels: []string{"Archive"}}, nil, []string{"Archive"}))
	assert.True(t, isTerminateNode(&storage.Node{Labels: []string{"Root"}}, []string{"Root"}))

	// Parameterized start node branch returns empty by design.
	res, err := exec.callApocPathSubgraphNodes("CALL apoc.path.subgraphNodes($nodeId,{maxLevel: 2})")
	require.NoError(t, err)
	require.Empty(t, res.Rows)

	// Traverse-all branch and config parsing branch.
	res, err = exec.callApocPathSubgraphNodes("CALL apoc.path.subgraphNodes(n,{maxLevel:2,minLevel:1,relationshipFilter:'REL|OTHER',labelFilter:'+Keep|-Archive',limit:10,bfs:false})")
	require.NoError(t, err)
	require.NotNil(t, res)

	startNode, err := store.GetNode("n1")
	require.NoError(t, err)
	require.NotNil(t, startNode)

	cfg := apocPathConfig{
		maxLevel:          3,
		minLevel:          0,
		relationshipTypes: []string{"REL", "OTHER"},
		direction:         "both",
		includeLabels:     []string{},
		excludeLabels:     []string{"Archive"},
	}
	dfsEdges := exec.dfsSpanningTree(startNode, cfg)
	require.NotEmpty(t, dfsEdges)

	// Missing explicit start node id should produce deterministic empty results.
	res, err = exec.callApocPathSubgraphNodes("MATCH (n {id: 'missing'}) CALL apoc.path.subgraphNodes(n,{maxLevel:2})")
	require.NoError(t, err)
	require.Empty(t, res.Rows)
}

func TestAPOCPathHelpers_SubgraphNodes_AllNodesError(t *testing.T) {
	failStore := &failingNodeLookupEngine{
		Engine:      storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"),
		allNodesErr: assert.AnError,
	}
	exec := NewStorageExecutor(failStore)

	_, err := exec.callApocPathSubgraphNodes("CALL apoc.path.subgraphNodes(n,{maxLevel:2})")
	require.Error(t, err)
	assert.Contains(t, err.Error(), assert.AnError.Error())
}
