package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionStorageWrapper_PrefixHelpers(t *testing.T) {
	w := &transactionStorageWrapper{namespace: "tenant", separator: ":"}
	assert.Equal(t, storage.NodeID("tenant:n1"), w.prefixNodeID("n1"))
	assert.Equal(t, storage.NodeID("n1"), w.unprefixNodeID("tenant:n1"))
	assert.Equal(t, storage.NodeID("n1"), w.unprefixNodeID("n1"))
	assert.Equal(t, storage.EdgeID("tenant:e1"), w.prefixEdgeID("e1"))
	assert.Equal(t, storage.EdgeID("e1"), w.unprefixEdgeID("tenant:e1"))
	assert.Equal(t, storage.EdgeID("e1"), w.unprefixEdgeID("e1"))

	w2 := &transactionStorageWrapper{}
	assert.Equal(t, storage.NodeID("n1"), w2.prefixNodeID("n1"))
	assert.Equal(t, storage.EdgeID("e1"), w2.prefixEdgeID("e1"))
}

func TestTransactionStorageWrapper_CreateGetDelete_WithNamespace(t *testing.T) {
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	w := &transactionStorageWrapper{tx: tx, underlying: eng, namespace: "tenant", separator: ":"}

	n := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}}
	createdID, err := w.CreateNode(n)
	require.NoError(t, err)
	assert.Equal(t, storage.NodeID("n1"), createdID)

	got, err := w.GetNode("n1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, storage.NodeID("n1"), got.ID)

	err = w.UpdateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)

	err = w.DeleteNode("n1")
	require.NoError(t, err)
}

func TestTransactionStorageWrapper_BulkOps_AndCounts(t *testing.T) {
	eng := storage.NewMemoryEngine()
	defer eng.Close()

	// Seed underlying storage first so the transaction snapshot can read/delete them.
	_, err := eng.CreateNode(&storage.Node{ID: "test:r1", Labels: []string{"Person"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "test:r2", Labels: []string{"Person"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{ID: "test:re1", StartNode: "test:r1", EndNode: "test:r2", Type: "REL", Properties: map[string]interface{}{}})
	require.NoError(t, err)

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	w := &transactionStorageWrapper{tx: tx, underlying: eng, namespace: "", separator: ":"}

	// Cover bulk-create transaction paths (writes are staged in tx, not immediately visible
	// through underlying delegated read methods).
	nodes := []*storage.Node{
		{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]interface{}{}},
		{ID: "test:n2", Labels: []string{"Person"}, Properties: map[string]interface{}{}},
	}
	require.NoError(t, w.BulkCreateNodes(nodes))

	edges := []*storage.Edge{{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "REL", Properties: map[string]interface{}{}}}
	require.NoError(t, w.BulkCreateEdges(edges))

	// BatchGetNodes and delegate methods
	gotMap, err := w.BatchGetNodes([]storage.NodeID{"test:r1", "test:r2"})
	require.NoError(t, err)
	assert.Len(t, gotMap, 2)

	allNodes, err := w.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(allNodes), 2)

	allEdges, err := w.AllEdges()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(allEdges), 1)

	firstByLabel, err := w.GetFirstNodeByLabel("Person")
	require.NoError(t, err)
	assert.NotNil(t, firstByLabel)

	edgesBetween, err := w.GetEdgesBetween("test:r1", "test:r2")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edgesBetween), 1)

	edgesByType, err := w.GetEdgesByType("REL")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edgesByType), 1)

	assert.GreaterOrEqual(t, len(w.GetAllNodes()), 2)
	assert.GreaterOrEqual(t, w.GetOutDegree("test:r1"), 0)
	assert.GreaterOrEqual(t, w.GetInDegree("test:r2"), 0)
	assert.NotNil(t, w.GetSchema())

	_, err = w.NodeCount()
	require.NoError(t, err)
	_, err = w.EdgeCount()
	require.NoError(t, err)

	require.NoError(t, w.BulkDeleteEdges([]storage.EdgeID{"test:re1"}))
	require.NoError(t, w.BulkDeleteNodes([]storage.NodeID{"test:r1", "test:r2"}))

	err = w.Close()
	require.NoError(t, err)

	_, _, err = w.DeleteByPrefix("test:")
	assert.Error(t, err)
}

func TestTransactionStorageWrapper_ToUserNode_NilSafe(t *testing.T) {
	w := &transactionStorageWrapper{namespace: "tenant", separator: ":"}
	assert.Nil(t, w.toUserNode(nil))

	n := &storage.Node{ID: "tenant:n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"x": 1}}
	out := w.toUserNode(n)
	require.NotNil(t, out)
	assert.Equal(t, storage.NodeID("n1"), out.ID)
	assert.EqualValues(t, 1, out.Properties["x"])
}
