package nornicdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRecoverBadgerFromSnapshotAndWAL_AllowsWALOnlyWhenNoSnapshots(t *testing.T) {
	// Create a fake "data dir" with ONLY a WAL (no snapshots directory/files).
	dataDir, err := os.MkdirTemp("", "nornicdb-auto-recover-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })

	walDir := filepath.Join(dataDir, "wal")
	wal, err := storage.NewWAL(walDir, nil)
	require.NoError(t, err)

	// Write a simple WAL-only history.
	require.NoError(t, wal.AppendWithDatabase(storage.OpCreateNode, storage.WALNodeData{
		Node: &storage.Node{
			ID:     storage.NodeID("node-1"), // unprefixed; database is carried in entry.Database
			Labels: []string{"Test"},
			Properties: map[string]any{
				"key": "value",
			},
		},
	}, "test"))
	require.NoError(t, wal.Close())

	// Invoke recovery. This will rename dataDir → dataDir.corrupted-* and rebuild.
	badgerOpts := storage.BadgerOptions{DataDir: dataDir}
	recovered, backupDir, err := recoverBadgerFromSnapshotAndWAL(dataDir, badgerOpts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = recovered.Close() })
	t.Cleanup(func() { _ = os.RemoveAll(backupDir) })

	// Verify the recovered Badger store contains the node.
	node, err := recovered.GetNode(storage.NodeID("test:node-1"))
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Equal(t, "value", node.Properties["key"])

	// Sanity: run a small transactional read as well (exercise Badger reads).
	tx, err := recovered.BeginTransaction()
	require.NoError(t, err)
	require.NotNil(t, tx)
	require.NoError(t, tx.Rollback())
}
