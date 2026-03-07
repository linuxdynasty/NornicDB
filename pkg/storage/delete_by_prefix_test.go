package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_DeleteByPrefix_DropsOnlyMatchingNamespace(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	_, err := engine.CreateNode(&Node{ID: "db1:n1", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db2:n2", Labels: []string{"Person"}})
	require.NoError(t, err)

	require.NoError(t, engine.CreateEdge(&Edge{ID: "db1:e1", StartNode: "db1:n1", EndNode: "db1:n1", Type: "KNOWS"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "db2:e2", StartNode: "db2:n2", EndNode: "db2:n2", Type: "KNOWS"}))

	// Warm caches to ensure DeleteByPrefix invalidates them.
	nodes, err := engine.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	edges, err := engine.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.Len(t, edges, 2)

	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(1), nodesDeleted)
	require.Equal(t, int64(1), edgesDeleted)

	_, err = engine.GetNode("db1:n1")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge("db1:e1")
	require.ErrorIs(t, err, ErrNotFound)

	_, err = engine.GetNode("db2:n2")
	require.NoError(t, err)
	_, err = engine.GetEdge("db2:e2")
	require.NoError(t, err)

	// Label index must not return dropped nodes.
	nodes, err = engine.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, NodeID("db2:n2"), nodes[0].ID)

	// Edge type cache must not return dropped edges.
	edges, err = engine.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, EdgeID("db2:e2"), edges[0].ID)
}

func TestBadgerEngine_DeleteByPrefix_EdgeCases(t *testing.T) {
	t.Run("empty prefix rejected", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })

		_, _, err := engine.DeleteByPrefix("")
		require.ErrorContains(t, err, "prefix cannot be empty")
	})

	t.Run("missing namespace returns zero counts", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })

		_, err := engine.CreateNode(&Node{ID: "db1:n1", Labels: []string{"Person"}})
		require.NoError(t, err)

		nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("missing:")
		require.NoError(t, err)
		require.Zero(t, nodesDeleted)
		require.Zero(t, edgesDeleted)

		_, err = engine.GetNode("db1:n1")
		require.NoError(t, err)
	})

	t.Run("closed engine returns storage closed", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.Close())

		_, _, err := engine.DeleteByPrefix("db1:")
		require.ErrorIs(t, err, ErrStorageClosed)
	})
}
