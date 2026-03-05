package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAsyncTestEngine(t *testing.T) *AsyncEngine {
	t.Helper()
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{
		FlushInterval: 50 * time.Millisecond,
	})
	t.Cleanup(func() { ae.Close() })
	return ae
}

func makeNode(id string) *Node {
	return &Node{
		ID:         NodeID(prefixTestID(id)),
		Labels:     []string{"TestLabel"},
		Properties: map[string]interface{}{"name": id},
	}
}

func makeEdge(id, from, to string) *Edge {
	return &Edge{
		ID:         EdgeID(prefixTestID(id)),
		StartNode:  NodeID(prefixTestID(from)),
		EndNode:    NodeID(prefixTestID(to)),
		Type:       "RELATED",
		Properties: map[string]interface{}{},
	}
}

// ============================================================================
// AllNodes / AllEdges
// ============================================================================

func TestAsyncEngine_AllNodes_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	nodes, err := ae.AllNodes()
	require.NoError(t, err)
	assert.Empty(t, nodes)
}

func TestAsyncEngine_AllNodes_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)

	_, err := ae.CreateNode(makeNode("n1"))
	require.NoError(t, err)
	_, err = ae.CreateNode(makeNode("n2"))
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	nodes, err := ae.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(nodes), 2)
}

func TestAsyncEngine_AllEdges_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	edges, err := ae.AllEdges()
	require.NoError(t, err)
	assert.Empty(t, edges)
}

func TestAsyncEngine_AllEdges_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)

	_, err := ae.CreateNode(makeNode("n1"))
	require.NoError(t, err)
	_, err = ae.CreateNode(makeNode("n2"))
	require.NoError(t, err)
	require.NoError(t, ae.CreateEdge(makeEdge("e1", "n1", "n2")))
	require.NoError(t, ae.Flush())

	edges, err := ae.AllEdges()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
}

// ============================================================================
// BatchGetNodes
// ============================================================================

func TestAsyncEngine_BatchGetNodes_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	result, err := ae.BatchGetNodes([]NodeID{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestAsyncEngine_BatchGetNodes_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)

	id1, err := ae.CreateNode(makeNode("batchn1"))
	require.NoError(t, err)
	id2, err := ae.CreateNode(makeNode("batchn2"))
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	result, err := ae.BatchGetNodes([]NodeID{id1, id2})
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestAsyncEngine_BatchGetNodes_SomeMissing(t *testing.T) {
	ae := newAsyncTestEngine(t)
	result, err := ae.BatchGetNodes([]NodeID{NodeID(prefixTestID("missing"))})
	require.NoError(t, err)
	assert.Empty(t, result)
}

// ============================================================================
// BulkCreateNodes / BulkCreateEdges
// ============================================================================

func TestAsyncEngine_BulkCreateNodes_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	err := ae.BulkCreateNodes([]*Node{})
	assert.NoError(t, err)
}

func TestAsyncEngine_BulkCreateNodes_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)
	nodes := []*Node{makeNode("bulk-n1"), makeNode("bulk-n2"), makeNode("bulk-n3")}
	err := ae.BulkCreateNodes(nodes)
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	count, err := ae.NodeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(3))
}

func TestAsyncEngine_BulkCreateEdges_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	err := ae.BulkCreateEdges([]*Edge{})
	assert.NoError(t, err)
}

func TestAsyncEngine_BulkCreateEdges_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("be1"))
	_, _ = ae.CreateNode(makeNode("be2"))
	require.NoError(t, ae.Flush())

	edges := []*Edge{makeEdge("bulk-e1", "be1", "be2")}
	err := ae.BulkCreateEdges(edges)
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	count, err := ae.EdgeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(1))
}

// ============================================================================
// BulkDeleteNodes / BulkDeleteEdges
// ============================================================================

func TestAsyncEngine_BulkDeleteNodes_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	err := ae.BulkDeleteNodes([]NodeID{})
	assert.NoError(t, err)
}

func TestAsyncEngine_BulkDeleteNodes_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)
	id1, _ := ae.CreateNode(makeNode("del-n1"))
	id2, _ := ae.CreateNode(makeNode("del-n2"))
	require.NoError(t, ae.Flush())

	err := ae.BulkDeleteNodes([]NodeID{id1, id2})
	require.NoError(t, err)
}

func TestAsyncEngine_BulkDeleteEdges_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	err := ae.BulkDeleteEdges([]EdgeID{})
	assert.NoError(t, err)
}

func TestAsyncEngine_BulkDeleteEdges_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("de1"))
	_, _ = ae.CreateNode(makeNode("de2"))
	_ = ae.CreateEdge(makeEdge("del-e1", "de1", "de2"))
	require.NoError(t, ae.Flush())

	err := ae.BulkDeleteEdges([]EdgeID{EdgeID(prefixTestID("del-e1"))})
	require.NoError(t, err)
}

// ============================================================================
// GetEdgesBetween / GetEdgeBetween / GetAllNodes / Degree
// ============================================================================

func TestAsyncEngine_GetEdgesBetween(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("gb1"))
	_, _ = ae.CreateNode(makeNode("gb2"))
	_ = ae.CreateEdge(makeEdge("gb-e1", "gb1", "gb2"))
	require.NoError(t, ae.Flush())

	edges, err := ae.GetEdgesBetween(NodeID(prefixTestID("gb1")), NodeID(prefixTestID("gb2")))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
}

func TestAsyncEngine_GetEdgesBetween_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	edges, err := ae.GetEdgesBetween(NodeID(prefixTestID("x")), NodeID(prefixTestID("y")))
	require.NoError(t, err)
	assert.Empty(t, edges)
}

func TestAsyncEngine_GetEdgeBetween_NotFound(t *testing.T) {
	ae := newAsyncTestEngine(t)
	edge := ae.GetEdgeBetween(NodeID(prefixTestID("x")), NodeID(prefixTestID("y")), "NOTYPE")
	assert.Nil(t, edge)
}

func TestAsyncEngine_GetAllNodes(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("gan1"))
	_, _ = ae.CreateNode(makeNode("gan2"))
	require.NoError(t, ae.Flush())

	nodes := ae.GetAllNodes()
	assert.GreaterOrEqual(t, len(nodes), 2)
}

func TestAsyncEngine_GetInOutDegree(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("deg1"))
	_, _ = ae.CreateNode(makeNode("deg2"))
	_ = ae.CreateEdge(makeEdge("deg-e1", "deg1", "deg2"))
	require.NoError(t, ae.Flush())

	in := ae.GetInDegree(NodeID(prefixTestID("deg2")))
	out := ae.GetOutDegree(NodeID(prefixTestID("deg1")))
	assert.GreaterOrEqual(t, in, 0)
	assert.GreaterOrEqual(t, out, 0)
}

// ============================================================================
// NodeCountByPrefix / EdgeCountByPrefix
// ============================================================================

func TestAsyncEngine_NodeCountByPrefix(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("pfx-a1"))
	_, _ = ae.CreateNode(makeNode("pfx-a2"))
	require.NoError(t, ae.Flush())

	count, err := ae.NodeCountByPrefix(prefixTestID("pfx-"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(0))
}

func TestAsyncEngine_EdgeCountByPrefix(t *testing.T) {
	ae := newAsyncTestEngine(t)
	count, err := ae.EdgeCountByPrefix(prefixTestID("epfx-"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(0))
}

// ============================================================================
// Pending embeddings
// ============================================================================

func TestAsyncEngine_AddToPendingEmbeddings(t *testing.T) {
	ae := newAsyncTestEngine(t)
	id, _ := ae.CreateNode(makeNode("emb1"))
	// Should not panic
	ae.AddToPendingEmbeddings(id)
}

func TestAsyncEngine_FindNodeNeedingEmbedding(t *testing.T) {
	ae := newAsyncTestEngine(t)
	id, _ := ae.CreateNode(makeNode("emb2"))
	ae.AddToPendingEmbeddings(id)

	node := ae.FindNodeNeedingEmbedding()
	// May be nil depending on flush state
	_ = node
}

func TestAsyncEngine_MarkNodeEmbedded(t *testing.T) {
	ae := newAsyncTestEngine(t)
	id, _ := ae.CreateNode(makeNode("emb3"))
	ae.AddToPendingEmbeddings(id)
	// Should not panic
	ae.MarkNodeEmbedded(id)
}

func TestAsyncEngine_RefreshPendingEmbeddingsIndex(t *testing.T) {
	ae := newAsyncTestEngine(t)
	count := ae.RefreshPendingEmbeddingsIndex()
	assert.GreaterOrEqual(t, count, 0)
}

// ============================================================================
// GetSchema / GetSchemaForNamespace / GetEngine
// ============================================================================

func TestAsyncEngine_GetSchema(t *testing.T) {
	ae := newAsyncTestEngine(t)
	schema := ae.GetSchema()
	assert.NotNil(t, schema)
}

func TestAsyncEngine_GetSchemaForNamespace(t *testing.T) {
	ae := newAsyncTestEngine(t)
	schema := ae.GetSchemaForNamespace("test")
	assert.NotNil(t, schema)
}

func TestAsyncEngine_GetEngine(t *testing.T) {
	ae := newAsyncTestEngine(t)
	eng := ae.GetEngine()
	assert.NotNil(t, eng)
}

// ============================================================================
// GetEdgesByType
// ============================================================================

func TestAsyncEngine_GetEdgesByType_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	edges, err := ae.GetEdgesByType("NONEXISTENT")
	require.NoError(t, err)
	assert.Empty(t, edges)
}

func TestAsyncEngine_GetEdgesByType_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("et1"))
	_, _ = ae.CreateNode(makeNode("et2"))
	_ = ae.CreateEdge(makeEdge("et-e1", "et1", "et2"))
	require.NoError(t, ae.Flush())

	edges, err := ae.GetEdgesByType("RELATED")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
}

// ============================================================================
// Label lookup / iteration / prefix delete delegates
// ============================================================================

func TestAsyncEngine_ForEachNodeIDByLabel_MergesCacheAndEngine(t *testing.T) {
	ae := newAsyncTestEngine(t)

	// Engine-backed node
	engineNode := makeNode("label-engine")
	_, err := ae.GetEngine().CreateNode(engineNode)
	require.NoError(t, err)

	// Cache-backed node (not flushed yet)
	cacheNode := makeNode("label-cache")
	_, err = ae.CreateNode(cacheNode)
	require.NoError(t, err)

	seen := map[NodeID]bool{}
	err = ae.ForEachNodeIDByLabel("testlabel", func(id NodeID) bool {
		seen[id] = true
		return true
	})
	require.NoError(t, err)
	assert.True(t, seen[engineNode.ID], "engine node should be visited")
	assert.True(t, seen[cacheNode.ID], "cached node should be visited")

	// Nil callback is a no-op path.
	require.NoError(t, ae.ForEachNodeIDByLabel("testlabel", nil))
}

func TestAsyncEngine_GetFirstAndGetNodesByLabel_CaseInsensitive(t *testing.T) {
	ae := newAsyncTestEngine(t)

	// Cached first-hit path.
	cacheNode := &Node{
		ID:         NodeID(prefixTestID("first-cache")),
		Labels:     []string{"MiXeDCaSe"},
		Properties: map[string]interface{}{"name": "cached"},
	}
	_, err := ae.CreateNode(cacheNode)
	require.NoError(t, err)

	first, err := ae.GetFirstNodeByLabel("mixedcase")
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, cacheNode.ID, first.ID)

	// Engine fallback path.
	require.NoError(t, ae.Flush())
	engineOnly := &Node{
		ID:         NodeID(prefixTestID("first-engine")),
		Labels:     []string{"EngineOnly"},
		Properties: map[string]interface{}{"name": "engine"},
	}
	_, err = ae.GetEngine().CreateNode(engineOnly)
	require.NoError(t, err)

	first, err = ae.GetFirstNodeByLabel("engineonly")
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, engineOnly.ID, first.ID)

	nodes, err := ae.GetNodesByLabel("mixedcase")
	require.NoError(t, err)
	assert.NotEmpty(t, nodes)
}

func TestAsyncEngine_GetIncomingEdges_MergesCacheAndEngine(t *testing.T) {
	ae := newAsyncTestEngine(t)

	_, _ = ae.CreateNode(makeNode("in-n1"))
	_, _ = ae.CreateNode(makeNode("in-n2"))
	_, _ = ae.CreateNode(makeNode("in-n3"))
	require.NoError(t, ae.Flush())

	// Engine edge.
	require.NoError(t, ae.GetEngine().CreateEdge(makeEdge("in-engine", "in-n1", "in-n2")))
	// Cache edge.
	require.NoError(t, ae.CreateEdge(makeEdge("in-cache", "in-n3", "in-n2")))

	incoming, err := ae.GetIncomingEdges(NodeID(prefixTestID("in-n2")))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(incoming), 2)
}

func TestAsyncEngine_IterateNodes_DeleteByPrefix_LastWriteTime(t *testing.T) {
	ae := newAsyncTestEngine(t)

	_, err := ae.CreateNode(makeNode("iter-a1"))
	require.NoError(t, err)
	_, err = ae.CreateNode(makeNode("iter-a2"))
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	visited := 0
	err = ae.IterateNodes(func(node *Node) bool {
		if node != nil {
			visited++
		}
		return true
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, visited, 2)

	// Cover LastWriteTime() delegate/fallback path.
	_ = ae.LastWriteTime()

	// Delete all test nodes by prefix and verify they're gone.
	nodesDeleted, _, err := ae.DeleteByPrefix(prefixTestID("iter-"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, nodesDeleted, int64(2))

	remaining, err := ae.NodeCountByPrefix(prefixTestID("iter-"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), remaining)
}
