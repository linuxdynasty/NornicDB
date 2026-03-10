package nornicdb

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDBWrapperHelpers_StorageAccess(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	namespaced := storage.NewNamespacedEngine(base, "nornic")
	db := &DB{storage: namespaced}

	require.Same(t, namespaced, db.GetStorage())
	require.Same(t, base, db.GetBaseStorageForManager())

	db.storage = base
	require.Panics(t, func() {
		_ = db.GetBaseStorageForManager()
	})
}

func TestDBWrapperHelpers_EmbedConfigRegistration(t *testing.T) {
	mock := newMockEmbedder()
	db := &DB{}

	db.SetDefaultEmbedConfig(nil)
	require.Nil(t, db.embedderRegistry)

	db.embedQueue = &EmbedQueue{embedder: mock}
	cfg := &embed.Config{
		Provider:   "local",
		Model:      "test-model",
		Dimensions: mock.Dimensions(),
		GPULayers:  0, // Normalized to -1 in key generation for local models
	}

	db.SetDefaultEmbedConfig(cfg)
	key := embedConfigKey(cfg)
	require.Equal(t, key, db.defaultEmbedKey)
	require.NotNil(t, db.embedderRegistry)
	require.Same(t, mock, db.embedderRegistry[key])

	var calledDB string
	resolver := func(dbName string) (*embed.Config, error) {
		calledDB = dbName
		return cfg, nil
	}
	db.SetEmbedConfigForDB(resolver)

	embedder, err := db.getOrCreateEmbedderForDB("tenant_a")
	require.NoError(t, err)
	require.Same(t, mock, embedder)
	require.Equal(t, "tenant_a", calledDB)
}

func TestDBWrapperHelpers_QueueAndExecutorAccess(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	queueCfg := DefaultEmbedQueueConfig()
	queueCfg.DeferWorkerStart = true
	queue := NewEmbedQueue(newMockEmbedder(), engine, queueCfg)
	t.Cleanup(queue.Close)

	exec := &cypher.StorageExecutor{}
	db := &DB{
		embedQueue:           queue,
		cypherExecutor:       exec,
		allDatabasesProvider: nil,
	}

	require.Same(t, exec, db.GetCypherExecutor())
	require.Same(t, queue, db.GetEmbedQueue())

	stats := db.EmbedQueueStats()
	require.NotNil(t, stats)
	require.False(t, stats.Running)
	require.Equal(t, 0, stats.Processed)
	require.Equal(t, 0, stats.Failed)

	db.SetAllDatabasesProvider(func() []DatabaseAndStorage {
		return []DatabaseAndStorage{{Name: "nornic", Storage: engine}}
	})
	require.NotNil(t, db.allDatabasesProvider)

	db.SetDbConfigResolver(func(dbName string) (int, float64, string) {
		if dbName == "tenant_a" {
			return 768, 0.42, "v2"
		}
		return 0, 0, ""
	})
	require.NotNil(t, db.dbConfigResolver)

	count, err := db.EmbedExisting(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, count)

	require.NoError(t, db.ResetEmbedWorker())

	db.StopEmbedQueue()
	require.Nil(t, db.embedQueue)
	require.Nil(t, db.EmbedQueueStats())

	_, err = db.EmbedExisting(context.Background())
	require.Error(t, err)
	require.Error(t, db.ResetEmbedWorker())
}

func TestDBWrapperHelpers_VectorDimensionsHelpers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 7
	db, err := Open("", cfg)
	require.NoError(t, err)

	require.Equal(t, 7, db.VectorIndexDimensions())
	require.Equal(t, 7, db.VectorIndexDimensionsCached())

	require.NoError(t, db.Close())
	require.Equal(t, 0, db.VectorIndexDimensions())
}
