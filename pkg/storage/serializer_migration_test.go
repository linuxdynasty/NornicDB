package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestMigrateBadgerSerializer_GobToMsgpack(t *testing.T) {
	prev := currentStorageSerializer()
	t.Cleanup(func() {
		_ = SetStorageSerializer(prev)
	})

	dir := t.TempDir()
	base, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:    dir,
		Serializer: StorageSerializerGob,
	})
	require.NoError(t, err)

	engine := NewNamespacedEngine(base, "test")
	_, err = engine.CreateNode(&Node{
		ID:     NodeID("node-1"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Alice",
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{
		ID:     NodeID("node-2"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Bob",
		},
	})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{
		ID:        EdgeID("edge-1"),
		StartNode: NodeID("node-1"),
		EndNode:   NodeID("node-2"),
		Type:      "KNOWS",
	})
	require.NoError(t, err)

	_, err = engine.CreateNode(&Node{
		ID:              NodeID("embed-1"),
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{make([]float32, 20000)},
	})
	require.NoError(t, err)

	stats, err := MigrateBadgerSerializerWithDB(base.db, dir, StorageSerializerMsgpack, SerializerMigrationOptions{
		BatchSize: 10,
	})
	require.NoError(t, err)
	require.True(t, stats.HasData)
	require.Equal(t, StorageSerializerGob, stats.Source)
	require.Equal(t, StorageSerializerMsgpack, stats.Target)
	require.Greater(t, stats.NodesConverted+stats.EdgesConverted, 0)

	require.NoError(t, base.Close())

	base2, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:    dir,
		Serializer: StorageSerializerMsgpack,
	})
	require.NoError(t, err)

	engine2 := NewNamespacedEngine(base2, "test")
	node, err := engine2.GetNode(NodeID("node-1"))
	require.NoError(t, err)
	require.Equal(t, NodeID("node-1"), node.ID)

	edge, err := engine2.GetEdge(EdgeID("edge-1"))
	require.NoError(t, err)
	require.Equal(t, "KNOWS", edge.Type)

	require.NoError(t, base2.Close())

	stats2, err := MigrateBadgerSerializer(dir, StorageSerializerMsgpack, SerializerMigrationOptions{
		BatchSize: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 0, stats2.NodesConverted+stats2.EdgesConverted+stats2.EmbeddingsConverted)
	require.Greater(t, stats2.SkippedExisting, 0)
}

func TestMigrateBadgerSerializerWithDB_EdgeCases(t *testing.T) {
	t.Run("invalid target and nil db fail fast", func(t *testing.T) {
		dir := t.TempDir()
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		stats, err := MigrateBadgerSerializerWithDB(db, dir, StorageSerializer("bogus"), SerializerMigrationOptions{})
		require.Error(t, err)
		require.Equal(t, dir, stats.DataDir)
		require.Equal(t, StorageSerializer("bogus"), stats.Target)

		_, err = MigrateBadgerSerializerWithDB(nil, dir, StorageSerializerMsgpack, SerializerMigrationOptions{})
		require.ErrorContains(t, err, "nil badger db")
	})

	t.Run("empty database reports no data", func(t *testing.T) {
		dir := t.TempDir()
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		stats, err := MigrateBadgerSerializerWithDB(db, dir, StorageSerializerMsgpack, SerializerMigrationOptions{})
		require.NoError(t, err)
		require.False(t, stats.HasData)
		require.Equal(t, 0, stats.TotalScanned)
	})

	t.Run("dry run counts conversions without rewriting payloads", func(t *testing.T) {
		prev := currentStorageSerializer()
		t.Cleanup(func() {
			_ = SetStorageSerializer(prev)
		})

		dir := t.TempDir()
		base, err := NewBadgerEngineWithOptions(BadgerOptions{
			DataDir:    dir,
			Serializer: StorageSerializerGob,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = base.Close() })

		engine := NewNamespacedEngine(base, "test")
		_, err = engine.CreateNode(&Node{
			ID:         NodeID("dry-run-node"),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"name": "draft"},
		})
		require.NoError(t, err)

		before, hasData, err := detectStoredSerializer(base.db)
		require.NoError(t, err)
		require.True(t, hasData)
		require.Equal(t, StorageSerializerGob, before)

		stats, err := MigrateBadgerSerializerWithDB(base.db, dir, StorageSerializerMsgpack, SerializerMigrationOptions{
			DryRun: true,
		})
		require.NoError(t, err)
		require.True(t, stats.HasData)
		require.Greater(t, stats.NodesConverted, 0)
		require.Equal(t, 0, stats.SkippedExisting)

		after, hasData, err := detectStoredSerializer(base.db)
		require.NoError(t, err)
		require.True(t, hasData)
		require.Equal(t, StorageSerializerGob, after)
	})
}

func TestDetectStoredSerializer(t *testing.T) {
	t.Run("nil db and empty db", func(t *testing.T) {
		_, _, err := detectStoredSerializer(nil)
		require.ErrorContains(t, err, "nil badger db")

		dir := t.TempDir()
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		serializer, hasData, err := detectStoredSerializer(db)
		require.NoError(t, err)
		require.False(t, hasData)
		require.Empty(t, serializer)
	})

	t.Run("detects gob and msgpack payloads", func(t *testing.T) {
		gobDir := t.TempDir()
		gobEngine, err := NewBadgerEngineWithOptions(BadgerOptions{
			DataDir:    gobDir,
			Serializer: StorageSerializerGob,
		})
		require.NoError(t, err)
		_, err = gobEngine.CreateNode(&Node{ID: "detect:gob", Labels: []string{"Test"}})
		require.NoError(t, err)

		serializer, hasData, err := detectStoredSerializer(gobEngine.db)
		require.NoError(t, err)
		require.True(t, hasData)
		require.Equal(t, StorageSerializerGob, serializer)
		require.NoError(t, gobEngine.Close())

		msgpackDir := t.TempDir()
		msgpackEngine, err := NewBadgerEngineWithOptions(BadgerOptions{
			DataDir:    msgpackDir,
			Serializer: StorageSerializerMsgpack,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = msgpackEngine.Close() })
		_, err = msgpackEngine.CreateNode(&Node{ID: "detect:msgpack", Labels: []string{"Test"}})
		require.NoError(t, err)

		serializer, hasData, err = detectStoredSerializer(msgpackEngine.db)
		require.NoError(t, err)
		require.True(t, hasData)
		require.Equal(t, StorageSerializerMsgpack, serializer)
	})
}

func TestMigrateBadgerSerializer_DryRun(t *testing.T) {
	dir := t.TempDir()

	// Create engine with gob serializer and some data
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:    dir,
		Serializer: StorageSerializerGob,
	})
	require.NoError(t, err)

	_, err = engine.CreateNode(&Node{ID: "test:dry1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	// Dry run should report what would change without modifying data
	stats, err := MigrateBadgerSerializer(dir, StorageSerializerMsgpack, SerializerMigrationOptions{DryRun: true, BatchSize: 10})
	require.NoError(t, err)
	require.True(t, stats.HasData)
	require.Greater(t, stats.NodesConverted, 0)
}

func TestMigrateBadgerSerializer_WithEmbeddings(t *testing.T) {
	dir := t.TempDir()

	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:    dir,
		Serializer: StorageSerializerGob,
	})
	require.NoError(t, err)

	// Create node with large embeddings (stored separately, triggering embedding prefix migration)
	node := &Node{
		ID:              "test:emb1",
		Labels:          []string{"Document"},
		Properties:      map[string]interface{}{"title": "test"},
		ChunkEmbeddings: makeLargeChunkEmbeddings(),
	}
	_, err = engine.CreateNode(node)
	require.NoError(t, err)

	// Also create an edge
	_, err = engine.CreateNode(&Node{ID: "test:emb2", Labels: []string{"Doc"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:ee1", StartNode: "test:emb1", EndNode: "test:emb2", Type: "REF", Properties: map[string]interface{}{}}))
	require.NoError(t, engine.Close())

	stats, err := MigrateBadgerSerializer(dir, StorageSerializerMsgpack, SerializerMigrationOptions{BatchSize: 2})
	require.NoError(t, err)
	require.True(t, stats.HasData)
	require.Greater(t, stats.NodesConverted, 0)
	require.Greater(t, stats.EdgesConverted, 0)
	require.Greater(t, stats.EmbeddingsConverted, 0)
}

func TestMigrateBadgerSerializer_InvalidDir(t *testing.T) {
	_, err := MigrateBadgerSerializer("/nonexistent/badger/dir", StorageSerializerMsgpack, SerializerMigrationOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "open badger")
}

func TestMigrateBadgerSerializerWithDB_InvalidSerializer(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	defer db.Close()

	_, err = MigrateBadgerSerializerWithDB(db, dir, StorageSerializer("invalid"), SerializerMigrationOptions{})
	require.Error(t, err)
}
