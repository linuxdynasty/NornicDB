package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/stretchr/testify/require"
)

type chunkingTestEmbedder struct {
	mu sync.Mutex

	dims int

	embedCalls     int
	embedBatchCall int
	maxTextLen     int
	maxTokens      int
}

func (e *chunkingTestEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.mu.Lock()
	e.embedCalls++
	if len(text) > e.maxTextLen {
		e.maxTextLen = len(text)
	}
	if tok := util.CountApproxTokens(text); tok > e.maxTokens {
		e.maxTokens = tok
	}
	dims := e.dims
	e.mu.Unlock()

	// Simulate a tokenizer limit.
	if util.CountApproxTokens(text) > 512 {
		return nil, fmt.Errorf("simulated tokenizer overflow for tokens=%d", util.CountApproxTokens(text))
	}
	if dims <= 0 {
		dims = 4
	}
	vec := make([]float32, dims)
	vec[0] = 1
	return vec, nil
}

func (e *chunkingTestEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	e.mu.Lock()
	e.embedBatchCall++
	for _, t := range texts {
		if len(t) > e.maxTextLen {
			e.maxTextLen = len(t)
		}
		if tok := util.CountApproxTokens(t); tok > e.maxTokens {
			e.maxTokens = tok
		}
	}
	dims := e.dims
	e.mu.Unlock()

	if dims <= 0 {
		dims = 4
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		if util.CountApproxTokens(t) > 512 {
			return nil, fmt.Errorf("chunk too long: tokens=%d", util.CountApproxTokens(t))
		}
		vec := make([]float32, dims)
		vec[0] = 1
		out[i] = vec
	}
	return out, nil
}

func (e *chunkingTestEmbedder) Dimensions() int { return e.dims }
func (e *chunkingTestEmbedder) Model() string   { return "chunking-test-embedder" }

func loadLargeDocQuery(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "plans", "sharding-base-plan.md")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	query := string(data)
	require.Greater(t, util.CountApproxTokens(query), 512)
	return query
}

func TestDB_EmbedQuery_ShortQuery_UsesEmbed(t *testing.T) {
	emb := &chunkingTestEmbedder{dims: 4}
	db := &DB{embedQueue: &EmbedQueue{embedder: emb}}

	vec, err := db.EmbedQuery(context.Background(), "hello world")
	require.NoError(t, err)
	require.Len(t, vec, 4)

	emb.mu.Lock()
	embedCalls := emb.embedCalls
	batchCalls := emb.embedBatchCall
	emb.mu.Unlock()

	require.Equal(t, 1, embedCalls)
	require.Equal(t, 0, batchCalls)
}

func TestDB_EmbedQuery_LongQuery_UsesEmbedBatchOnChunks(t *testing.T) {
	emb := &chunkingTestEmbedder{dims: 4}
	db := &DB{embedQueue: &EmbedQueue{embedder: emb}}

	longQuery := loadLargeDocQuery(t)
	vec, err := db.EmbedQuery(context.Background(), longQuery)
	require.NoError(t, err)
	require.Len(t, vec, 4)

	emb.mu.Lock()
	embedCalls := emb.embedCalls
	batchCalls := emb.embedBatchCall
	maxLen := emb.maxTextLen
	maxTokens := emb.maxTokens
	emb.mu.Unlock()

	require.Equal(t, 0, embedCalls, "expected long query path to avoid single-text embedding")
	require.Equal(t, 1, batchCalls, "expected long query path to batch-embed chunks once")
	require.Greater(t, maxLen, 0)
	require.LessOrEqual(t, maxTokens, 512, "expected all embedded chunks to be <= 512 tokens")
}

func TestDB_EmbedQueryForDB_NoResolver_ReturnsSameAsEmbedQuery(t *testing.T) {
	emb := &chunkingTestEmbedder{dims: 8}
	db := &DB{embedQueue: &EmbedQueue{embedder: emb}}

	vec, err := db.EmbedQueryForDB(context.Background(), "mydb", "hello")
	require.NoError(t, err)
	require.Len(t, vec, 8)
}

func TestDB_EmbedQueryForDB_ResolverMatchesDims_Success(t *testing.T) {
	emb := &chunkingTestEmbedder{dims: 8}
	db := &DB{
		embedQueue: &EmbedQueue{embedder: emb},
		dbConfigResolver: func(dbName string) (embeddingDims int, searchMinSimilarity float64, bm25Engine string) {
			return 8, 0.5, ""
		},
	}

	vec, err := db.EmbedQueryForDB(context.Background(), "mydb", "hello")
	require.NoError(t, err)
	require.Len(t, vec, 8)
}

func TestDB_EmbedQueryForDB_ResolverMismatchDims_ReturnsErrQueryEmbeddingDimensionMismatch(t *testing.T) {
	emb := &chunkingTestEmbedder{dims: 8}
	db := &DB{
		embedQueue: &EmbedQueue{embedder: emb},
		dbConfigResolver: func(dbName string) (embeddingDims int, searchMinSimilarity float64, bm25Engine string) {
			return 768, 0.5, "" // index is 768-d, query will be 8-d from global embedder
		},
	}

	vec, err := db.EmbedQueryForDB(context.Background(), "mydb", "hello")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrQueryEmbeddingDimensionMismatch))
	require.Nil(t, vec)
	require.Contains(t, err.Error(), "index dims 768")
	require.Contains(t, err.Error(), "query dims 8")
}

func TestEmbedConfigKey_LocalZeroAndAutoLayers_AreEquivalent(t *testing.T) {
	cfgZero := &embed.Config{
		Provider:   "local",
		Model:      "bge-m3",
		Dimensions: 1024,
		ModelsDir:  "models",
		GPULayers:  0,
	}
	cfgAuto := &embed.Config{
		Provider:   "local",
		Model:      "bge-m3",
		Dimensions: 1024,
		ModelsDir:  "models",
		GPULayers:  -1,
	}
	require.Equal(t, embedConfigKey(cfgZero), embedConfigKey(cfgAuto))
}

func TestDB_GetOrCreateEmbedderForDB_LocalEquivalentConfig_AliasesDefaultEmbedder(t *testing.T) {
	defaultEmbedder := &chunkingTestEmbedder{dims: 1024}
	defaultCfg := &embed.Config{
		Provider:   "local",
		Model:      "bge-m3",
		Dimensions: 1024,
		ModelsDir:  "models",
		GPULayers:  0,
	}
	db := &DB{
		embedQueue: &EmbedQueue{embedder: defaultEmbedder},
		embedderRegistry: map[string]embed.Embedder{
			embedConfigKey(defaultCfg): defaultEmbedder,
		},
		defaultEmbedKey: embedConfigKey(defaultCfg),
	}
	db.embedConfigForDB = func(dbName string) (*embed.Config, error) {
		return &embed.Config{
			Provider:   "local",
			Model:      "bge-m3",
			Dimensions: 1024,
			ModelsDir:  "models",
			// Equivalent local default expressed as -1 instead of 0.
			GPULayers: -1,
		}, nil
	}

	got, err := db.getOrCreateEmbedderForDB("translations")
	require.NoError(t, err)
	require.Same(t, defaultEmbedder, got)

	aliasKey := embedConfigKey(&embed.Config{
		Provider:   "local",
		Model:      "bge-m3",
		Dimensions: 1024,
		ModelsDir:  "models",
		GPULayers:  -1,
	})

	db.embedderRegistryMu.RLock()
	aliased := db.embedderRegistry[aliasKey]
	db.embedderRegistryMu.RUnlock()
	require.Same(t, defaultEmbedder, aliased)
}

type factoryTestEmbedder struct {
	dims int
}

func (e *factoryTestEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	vec := make([]float32, e.dims)
	if e.dims > 0 {
		vec[0] = 1
	}
	return vec, nil
}
func (e *factoryTestEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i], _ = e.Embed(ctx, texts[i])
	}
	return out, nil
}
func (e *factoryTestEmbedder) Dimensions() int { return e.dims }
func (e *factoryTestEmbedder) Model() string   { return "factory-test" }

func TestDB_GetOrCreateEmbedderForDB_SingleFlightAndNoStatsBlocking(t *testing.T) {
	fallback := &chunkingTestEmbedder{dims: 8}
	db := &DB{
		embedQueue: &EmbedQueue{embedder: fallback},
	}
	db.embedConfigForDB = func(dbName string) (*embed.Config, error) {
		return &embed.Config{
			Provider:   "openai",
			Model:      "test-model",
			Dimensions: 8,
			APIURL:     "https://example.invalid",
			APIKey:     "k",
		}, nil
	}

	var createCalls atomic.Int64
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	db.embedderFactory = func(cfg *embed.Config) (embed.Embedder, error) {
		createCalls.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return &factoryTestEmbedder{dims: cfg.Dimensions}, nil
	}

	const workers = 6
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			_, err := db.getOrCreateEmbedderForDB("translations")
			errCh <- err
		}()
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("embedder factory did not start")
	}

	// While embedder creation is in-flight, stats paths should remain responsive.
	statsDone := make(chan struct{}, 1)
	go func() {
		_ = db.EmbeddingCountCached()
		_ = db.PendingEmbeddingsCount()
		statsDone <- struct{}{}
	}()
	select {
	case <-statsDone:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("stats calls blocked while embedder creation in-flight")
	}

	close(release)

	for i := 0; i < workers; i++ {
		require.NoError(t, <-errCh)
	}
	require.Equal(t, int64(1), createCalls.Load(), "expected single-flight embedder creation")
}

func TestDB_GetOrCreateEmbedderForDB_FallbackBranches(t *testing.T) {
	t.Run("no queue configured returns nil embedder without error", func(t *testing.T) {
		db := &DB{}
		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("nil resolver returns active queue embedder", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 6}
		db := &DB{embedQueue: &EmbedQueue{embedder: fallback}}
		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, fallback, got)
	})

	t.Run("resolver error falls back to active queue embedder", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 6}
		db := &DB{
			embedQueue: &EmbedQueue{embedder: fallback},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return nil, errors.New("resolver-failed")
			},
		}
		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, fallback, got)
	})

	t.Run("factory failure falls back to active queue embedder", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 8}
		db := &DB{
			embedQueue: &EmbedQueue{embedder: fallback},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return &embed.Config{
					Provider:   "openai",
					Model:      "test-model",
					Dimensions: 8,
					APIURL:     "https://example.invalid",
					APIKey:     "k",
				}, nil
			},
			embedderFactory: func(cfg *embed.Config) (embed.Embedder, error) {
				return nil, errors.New("factory failed")
			},
		}

		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, fallback, got)

		key := embedConfigKey(&embed.Config{
			Provider:   "openai",
			Model:      "test-model",
			Dimensions: 8,
			APIURL:     "https://example.invalid",
			APIKey:     "k",
		})
		db.embedderRegistryMu.RLock()
		_, exists := db.embedderRegistry[key]
		db.embedderRegistryMu.RUnlock()
		require.False(t, exists)
	})

	t.Run("single-flight waiter falls back when creation fails", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 8}
		db := &DB{
			embedQueue: &EmbedQueue{embedder: fallback},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return &embed.Config{
					Provider:   "openai",
					Model:      "singleflight-failure",
					Dimensions: 8,
					APIURL:     "https://example.invalid",
					APIKey:     "k",
				}, nil
			},
		}

		started := make(chan struct{}, 1)
		release := make(chan struct{})
		var createCalls atomic.Int64
		db.embedderFactory = func(cfg *embed.Config) (embed.Embedder, error) {
			createCalls.Add(1)
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return nil, errors.New("factory failed")
		}

		type callResult struct {
			embedder embed.Embedder
			err      error
		}
		resCh1 := make(chan callResult, 1)
		resCh2 := make(chan callResult, 1)
		go func() {
			e, err := db.getOrCreateEmbedderForDB("tenant_a")
			resCh1 <- callResult{embedder: e, err: err}
		}()
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("factory did not start")
		}
		go func() {
			e, err := db.getOrCreateEmbedderForDB("tenant_a")
			resCh2 <- callResult{embedder: e, err: err}
		}()

		require.Eventually(t, func() bool {
			db.embedderCreateMu.Lock()
			inflight := len(db.embedderCreate)
			db.embedderCreateMu.Unlock()
			return createCalls.Load() == 1 && inflight == 1
		}, time.Second, 10*time.Millisecond)

		close(release)
		r1 := <-resCh1
		r2 := <-resCh2
		require.NoError(t, r1.err)
		require.NoError(t, r2.err)
		require.Same(t, fallback, r1.embedder)
		require.Same(t, fallback, r2.embedder)
		require.Equal(t, int64(1), createCalls.Load(), "failed creation should still be single-flight")
	})
}
