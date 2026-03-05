package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_PrefixStatsAreO1AndCorrect(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	_, err = engine.CreateNode(&Node{ID: "db1:n1"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db1:n2"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db2:n1"})
	require.NoError(t, err)

	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(3), count)

	count, err = engine.NodeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	count, err = engine.NodeCountByPrefix("db2:")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	err = engine.DeleteNode("db1:n1")
	require.NoError(t, err)

	count, err = engine.NodeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "db1:n3"})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	count, err = engine.NodeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

func TestBadgerEngine_EdgePrefixStatsAreCorrect_TransactionalAndNonTransactional(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	_, err = engine.CreateNode(&Node{ID: "db1:a"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db1:b"})
	require.NoError(t, err)

	err = engine.CreateEdge(&Edge{
		ID:        "db1:e1",
		StartNode: "db1:a",
		EndNode:   "db1:b",
		Type:      "R",
	})
	require.NoError(t, err)

	count, err := engine.EdgeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{
		ID:        "db1:e2",
		StartNode: "db1:a",
		EndNode:   "db1:b",
		Type:      "R",
	}))
	require.NoError(t, tx.Commit())

	count, err = engine.EdgeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	require.NoError(t, engine.BulkDeleteEdges([]EdgeID{"db1:e1", "db1:e2"}))

	count, err = engine.EdgeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}
