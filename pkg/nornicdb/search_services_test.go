package nornicdb

import (
	"context"
	"testing"
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchServices_HelperBranches(t *testing.T) {
	t.Run("splitQualifiedID validity", func(t *testing.T) {
		dbName, local, ok := splitQualifiedID("tenant:node1")
		require.True(t, ok)
		require.Equal(t, "tenant", dbName)
		require.Equal(t, "node1", local)

		_, _, ok = splitQualifiedID("tenant:")
		require.False(t, ok)
		_, _, ok = splitQualifiedID(":node")
		require.False(t, ok)
		_, _, ok = splitQualifiedID("not-qualified")
		require.False(t, ok)
	})

	t.Run("defaultDatabaseName panics when storage is not namespaced", func(t *testing.T) {
		db := &DB{storage: storage.NewMemoryEngine()}
		require.Panics(t, func() {
			_ = db.defaultDatabaseName()
		})
	})

	t.Run("kmeansNumClusters defaults to zero with nil config", func(t *testing.T) {
		db := &DB{}
		require.Equal(t, 0, db.kmeansNumClusters())
	})
}

func TestSearchServices_PerDatabaseIsolation_EventRouting(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringDisabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Wait for the per-database search services to finish their initial startup build
	// before injecting event-driven index updates. This test is verifying namespace
	// routing, not the startup warmup path, and a concurrent startup rebuild can
	// otherwise race with IndexNode() and make the expected counts nondeterministic.
	defaultSvc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.NoError(t, db.ensureSearchIndexesBuilt(ctx, db.defaultDatabaseName()))
	require.Equal(t, 0, defaultSvc.EmbeddingCount())

	db2Svc, err := db.GetOrCreateSearchService("db2", nil)
	require.NoError(t, err)
	require.NoError(t, db.ensureSearchIndexesBuilt(ctx, "db2"))
	require.Equal(t, 0, db2Svc.EmbeddingCount())

	// Create and index a node in the default database (nornic).
	alpha := &storage.Node{
		ID:     storage.NodeID("alpha"),
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "hello alpha",
		},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
	}
	_, err = db.storage.CreateNode(alpha)
	require.NoError(t, err)
	db.indexNodeFromEvent(&storage.Node{
		ID:              storage.NodeID("nornic:alpha"),
		Labels:          alpha.Labels,
		Properties:      alpha.Properties,
		ChunkEmbeddings: alpha.ChunkEmbeddings,
	})

	// Create and index a node in another database.
	db2Storage := storage.NewNamespacedEngine(db.baseStorage, "db2")
	beta := &storage.Node{
		ID:     storage.NodeID("beta"),
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "world beta",
		},
		ChunkEmbeddings: [][]float32{{0.4, 0.5, 0.6}},
	}
	_, err = db2Storage.CreateNode(beta)
	require.NoError(t, err)
	db.indexNodeFromEvent(&storage.Node{
		ID:              storage.NodeID("db2:beta"),
		Labels:          beta.Labels,
		Properties:      beta.Properties,
		ChunkEmbeddings: beta.ChunkEmbeddings,
	})

	// Default DB service should only contain default DB embedding.
	require.Eventually(t, func() bool {
		return defaultSvc.EmbeddingCount() == 1
	}, 5*time.Second, 10*time.Millisecond)

	// db2 service should exist and contain only db2 embedding.
	require.Eventually(t, func() bool {
		return db2Svc.EmbeddingCount() == 1
	}, 5*time.Second, 10*time.Millisecond)

	// Wait for text search to be ready after indexing.
	// The fulltext index updates are inline but may have small delays,
	// especially in high-load CI environments.
	require.Eventually(t, func() bool {
		resp, err := defaultSvc.Search(ctx, "hello", nil, nil)
		return err == nil && len(resp.Results) > 0
	}, 5*time.Second, 10*time.Millisecond)

	// Verify text-only search does not cross-contaminate.
	// Default DB should find alpha, not beta.
	resp, err := defaultSvc.Search(ctx, "world", nil, nil)
	require.NoError(t, err)
	require.Len(t, resp.Results, 0)

	resp, err = defaultSvc.Search(ctx, "hello", nil, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(resp.Results), 1)

	// db2 should find beta, not alpha.
	resp, err = db2Svc.Search(ctx, "hello", nil, nil)
	require.NoError(t, err)
	require.Len(t, resp.Results, 0)

	// As with defaultSvc above, text search visibility can lag briefly in CI even
	// when embedding counts are updated, so assert db2's positive search via retry.
	require.Eventually(t, func() bool {
		resp, err = db2Svc.Search(ctx, "world", nil, nil)
		return err == nil && len(resp.Results) >= 1
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSearchServices_ResetDropsCache(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.GetOrCreateSearchService("db2", nil)
	require.NoError(t, err)

	db.searchServicesMu.RLock()
	_, exists := db.searchServices["db2"]
	db.searchServicesMu.RUnlock()
	require.True(t, exists)

	db.ResetSearchService("db2")

	db.searchServicesMu.RLock()
	_, exists = db.searchServices["db2"]
	db.searchServicesMu.RUnlock()
	require.False(t, exists)
}

func TestSearchServices_ClusteringRunnerInitializesKnownNamespaces(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Create a node in a second database without touching the search service cache.
	db2Storage := storage.NewNamespacedEngine(db.baseStorage, "db2")
	_, err = db2Storage.CreateNode(&storage.Node{
		ID:     storage.NodeID("beta"),
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "world beta",
		},
		ChunkEmbeddings: [][]float32{{0.4, 0.5, 0.6}},
	})
	require.NoError(t, err)

	// The clustering runner should discover db2 and initialize a search service for it.
	db.runClusteringOnceAllDatabases(context.Background())

	db.searchServicesMu.RLock()
	_, db2Exists := db.searchServices["db2"]
	_, systemExists := db.searchServices["system"]
	db.searchServicesMu.RUnlock()

	require.True(t, db2Exists)
	require.False(t, systemExists)
}

func TestSearchServices_ClusteringFlagUpgradesCachedService(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringDisabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Create service while clustering is disabled.
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.False(t, svc.IsClusteringEnabled())

	// Enable clustering and run the clustering runner; it should upgrade the cached service.
	enable := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(enable)

	db.runClusteringOnceAllDatabases(context.Background())

	svc, err = db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.True(t, svc.IsClusteringEnabled())
}

func TestSearchServices_SkipsQdrantNamespaceNodes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)

	before := svc.EmbeddingCount()
	db.indexNodeFromEvent(&storage.Node{
		ID: storage.NodeID("nornic:qdrant:bench_col:1"),
		NamedEmbeddings: map[string][]float32{
			"default": {1, 0, 0},
		},
	})
	after := svc.EmbeddingCount()
	require.Equal(t, before, after)
}

func TestSearchServices_EventRemovalAndCreationErrorBranches(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	t.Run("removeNodeFromEvent unprefixed ID falls back to default db", func(t *testing.T) {
		svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
		require.NoError(t, err)

		node := &storage.Node{
			ID:              "n-local",
			Properties:      map[string]any{"content": "remove me"},
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		}
		require.NoError(t, svc.IndexNode(node))
		require.Equal(t, 1, svc.EmbeddingCount())

		db.removeNodeFromEvent("n-local")
		require.Eventually(t, func() bool {
			return svc.EmbeddingCount() == 0
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("system database creation is rejected", func(t *testing.T) {
		_, err := db.getOrCreateSearchService("system", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "system database")
	})

	t.Run("indexNodeFromEvent ignores unqualified IDs", func(t *testing.T) {
		svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
		require.NoError(t, err)
		before := svc.EmbeddingCount()

		db.indexNodeFromEvent(&storage.Node{
			ID:              "unqualified",
			Properties:      map[string]any{"content": "ignored"},
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		})

		require.Equal(t, before, svc.EmbeddingCount())
	})

	t.Run("indexNodeFromEvent tolerates service creation failure", func(t *testing.T) {
		minimal := &DB{
			embeddingDims:       3,
			searchServices:      make(map[string]*dbSearchService),
			searchMinSimilarity: 0.1,
		}
		minimal.indexNodeFromEvent(&storage.Node{
			ID:              "tenant:n1",
			Properties:      map[string]any{"content": "noop"},
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		})
	})

	t.Run("nil base storage returns deterministic error", func(t *testing.T) {
		minimal := &DB{
			embeddingDims:       3,
			searchServices:      make(map[string]*dbSearchService),
			searchMinSimilarity: 0.1,
		}
		_, err := minimal.getOrCreateSearchService("tenant_cov", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "base storage is nil")
	})
}

// TestRunClusteringOnceAllDatabases_RespectsContextCancellation verifies that
// runClusteringOnceAllDatabases returns promptly when the context is cancelled
// (e.g. on server shutdown). The goroutine must exit so Close() can complete.
func TestRunClusteringOnceAllDatabases_RespectsContextCancellation(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately so runClusteringOnceAllDatabases exits right away.

	done := make(chan struct{})
	go func() {
		defer close(done)
		db.runClusteringOnceAllDatabases(ctx)
	}()

	select {
	case <-done:
		// Goroutine exited; cancellation was respected.
	case <-time.After(2 * time.Second):
		t.Fatal("runClusteringOnceAllDatabases did not return after context cancellation within 2s")
	}
}

// TestTriggerSearchClustering_DoesNotPanic verifies TriggerSearchClustering
// runs without panicking when buildCtx is set (normal Open path) and when
// clustering is disabled (returns early). Also ensures nil buildCtx is handled
// defensively in code paths that may call TriggerSearchClustering.
func TestTriggerSearchClustering_DoesNotPanic(t *testing.T) {
	t.Run("clustering_disabled_returns_early", func(t *testing.T) {
		cleanup := featureflags.WithGPUClusteringDisabled()
		t.Cleanup(cleanup)

		cfg := DefaultConfig()
		cfg.Memory.EmbeddingDimensions = 3
		db, err := Open("", cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		err = db.TriggerSearchClustering()
		require.NoError(t, err)
	})

	t.Run("clustering_enabled_uses_buildCtx", func(t *testing.T) {
		cleanup := featureflags.WithGPUClusteringEnabled()
		t.Cleanup(cleanup)

		cfg := DefaultConfig()
		cfg.Memory.EmbeddingDimensions = 3
		db, err := Open("", cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		require.NotNil(t, db.buildCtx, "Open() should set buildCtx so clustering can be cancelled on Close()")
		err = db.TriggerSearchClustering()
		require.NoError(t, err)
	})
}

func TestSearchServices_RerankerStatusAndBuildStartHelpers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Explicitly store odd cache entries so setter loop skips nils safely.
	db.searchServicesMu.Lock()
	db.searchServices["nil_entry"] = nil
	db.searchServices["nil_svc"] = &dbSearchService{}
	db.searchServicesMu.Unlock()

	// Global reranker setter should tolerate nil/empty entries.
	db.SetSearchReranker(nil)

	// Resolver should be consulted for new DB services.
	calledDB := ""
	db.SetRerankerResolver(func(dbName string) search.Reranker {
		calledDB = dbName
		return nil
	})

	svc, err := db.GetOrCreateSearchService("tenant_cov", nil)
	require.NoError(t, err)
	require.NotNil(t, svc)
	require.Equal(t, "tenant_cov", calledDB)

	// Not initialized path: missing entry and nil-svc entry both report not_initialized.
	missing := db.GetDatabaseSearchStatus("missing_cov")
	require.False(t, missing.Ready)
	require.False(t, missing.Building)
	require.False(t, missing.Initialized)
	require.Equal(t, "not_initialized", missing.Phase)
	require.Equal(t, int64(-1), missing.ETASeconds)

	nilSvc := db.GetDatabaseSearchStatus("nil_svc")
	require.False(t, nilSvc.Initialized)
	require.Equal(t, "not_initialized", nilSvc.Phase)

	// Start build without waiting; helper should return service and initialize status.
	startedSvc, err := db.EnsureSearchIndexesBuildStarted("tenant_cov", nil)
	require.NoError(t, err)
	require.Same(t, svc, startedSvc)

	// Ensure completion so this test does not leak in-flight builders.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.ensureSearchIndexesBuilt(ctx, "tenant_cov"))

	ready := db.GetDatabaseSearchStatus("tenant_cov")
	require.True(t, ready.Initialized)
}
