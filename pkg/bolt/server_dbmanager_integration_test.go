package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSessionGetExecutorForDatabase_WiresDatabaseManagerCommands(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("tenant_a"))

	s := &Session{server: &Server{dbManager: mgr}}
	exec, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)

	res, err := exec.Execute(context.Background(), "SHOW DATABASES", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Rows)
}

type fixedEmbedder struct {
	dims int
}

func (f *fixedEmbedder) Embed(_ context.Context, _ string) ([]float32, error) {
	out := make([]float32, f.dims)
	if f.dims > 0 {
		out[0] = 1
	}
	return out, nil
}

func (f *fixedEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	for range texts {
		v, _ := f.Embed(ctx, "")
		out = append(out, v)
	}
	return out, nil
}

func (f *fixedEmbedder) Dimensions() int { return f.dims }
func (f *fixedEmbedder) Model() string   { return "fixed" }

func (f *fixedEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return []string{text}, nil
}

type providerBackedExecutor struct {
	base *cypher.StorageExecutor
}

func (p *providerBackedExecutor) Execute(_ context.Context, _ string, _ map[string]any) (*QueryResult, error) {
	return &QueryResult{}, nil
}

func (p *providerBackedExecutor) BaseCypherExecutor() *cypher.StorageExecutor {
	return p.base
}

func (p *providerBackedExecutor) ConfigureDatabaseExecutor(exec *cypher.StorageExecutor, _ string, _ storage.Engine) {
	if p == nil || p.base == nil || exec == nil {
		return
	}
	if emb := p.base.GetEmbedder(); emb != nil {
		exec.SetEmbedder(emb)
	}
}

func TestSessionGetExecutorForDatabase_InheritsEmbedder_ForStringVectorQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "nornic")
	require.NoError(t, store.GetSchema().AddVectorIndex("idx_doc", "Doc", "embedding", 3, "cosine"))
	_, err := store.CreateNode(&storage.Node{
		ID:     "d1",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"embedding": []float32{1, 0, 0},
		},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
	})
	require.NoError(t, err)

	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": store,
		},
		defaultDB: "nornic",
	}

	baseExec := cypher.NewStorageExecutor(store)
	var emb embed.Embedder = &fixedEmbedder{dims: 3}
	baseExec.SetEmbedder(emb)

	s := &Session{
		server: &Server{
			dbManager: mgr,
			executor:  &boltQueryExecutorAdapter{executor: baseExec},
		},
	}

	exec, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)

	// Ensure the DB-scoped Bolt executor inherits the base embedder so string
	// query inputs over Bolt are accepted.
	adapter, ok := exec.(*boltQueryExecutorAdapter)
	require.True(t, ok)
	require.NotNil(t, adapter.executor.GetEmbedder())

	res, err := exec.Execute(context.Background(),
		"CALL db.index.vector.queryNodes('idx_doc', 1, $q) YIELD node, score RETURN score",
		map[string]any{"q": "hello world"},
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Columns)
}

func TestSessionGetExecutorForDatabase_InheritsEmbedder_FromBaseExecutorProvider(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "nornic")
	require.NoError(t, store.GetSchema().AddVectorIndex("idx_doc", "Doc", "embedding", 3, "cosine"))
	_, err := store.CreateNode(&storage.Node{
		ID:     "d1",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"embedding": []float32{1, 0, 0},
		},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
	})
	require.NoError(t, err)

	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": store,
		},
		defaultDB: "nornic",
	}

	baseExec := cypher.NewStorageExecutor(store)
	var emb embed.Embedder = &fixedEmbedder{dims: 3}
	baseExec.SetEmbedder(emb)

	s := &Session{
		server: &Server{
			dbManager: mgr,
			executor:  &providerBackedExecutor{base: baseExec},
		},
	}

	exec, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)

	adapter, ok := exec.(*boltQueryExecutorAdapter)
	require.True(t, ok)
	require.NotNil(t, adapter.executor.GetEmbedder())

	res, err := exec.Execute(context.Background(),
		"CALL db.index.vector.queryNodes('idx_doc', 1, $q) YIELD node, score RETURN score",
		map[string]any{"q": "hello world"},
	)
	require.NoError(t, err)
	require.NotNil(t, res)
}
