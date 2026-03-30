package heimdall

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/textchunk"
	"github.com/stretchr/testify/require"
)

func countTestTokens(text string) (int, error) {
	return len(strings.Fields(text)), nil
}

func chunkTestText(text string, maxTokens, overlap int) ([]string, error) {
	return textchunk.ChunkByTokenCount(text, maxTokens, overlap, countTestTokens)
}

type stubQueryDB struct{}

func (s *stubQueryDB) Query(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	return nil, nil
}

func (s *stubQueryDB) Stats() interface{} { return nil }
func (s *stubQueryDB) NodeCount() (int64, error) {
	return 0, nil
}
func (s *stubQueryDB) EdgeCount() (int64, error) {
	return 0, nil
}

type testEmbedder struct {
	mu sync.Mutex

	failIfTokensGreater int
	calls               int
	maxLen              int
	maxTokens           int
}

func (e *testEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.mu.Lock()
	e.calls++
	if len(text) > e.maxLen {
		e.maxLen = len(text)
	}
	tokens, err := countTestTokens(text)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if tokens > e.maxTokens {
		e.maxTokens = tokens
	}
	fail := e.failIfTokensGreater > 0 && tokens > e.failIfTokensGreater
	e.mu.Unlock()

	if fail {
		return nil, fmt.Errorf("simulated tokenizer overflow for tokens=%d", tokens)
	}
	return []float32{0.1, 0.2}, nil
}

func (e *testEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

type testSearcher struct {
	mu sync.Mutex

	hybridCalls []string
	searchCalls []string
}

func (s *testSearcher) HybridSearch(ctx context.Context, query string, queryEmbedding []float32, labels []string, limit int) ([]*SemanticSearchResult, error) {
	s.mu.Lock()
	s.hybridCalls = append(s.hybridCalls, query)
	s.mu.Unlock()

	return []*SemanticSearchResult{
		{
			ID:         "node-1",
			Labels:     []string{"Memory"},
			Properties: map[string]interface{}{"title": "hello"},
			Score:      0.03,
		},
	}, nil
}

func (s *testSearcher) Search(ctx context.Context, query string, labels []string, limit int) ([]*SemanticSearchResult, error) {
	s.mu.Lock()
	s.searchCalls = append(s.searchCalls, query)
	s.mu.Unlock()
	return nil, nil
}

func (s *testSearcher) Neighbors(ctx context.Context, nodeID string) ([]string, error) {
	return nil, nil
}
func (s *testSearcher) GetEdgesForNode(ctx context.Context, nodeID string) ([]*GraphEdge, error) {
	return nil, nil
}
func (s *testSearcher) GetNode(ctx context.Context, nodeID string) (*NodeData, error) {
	return nil, nil
}

func loadLargeDocQuery(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "features", "gpu-acceleration.md")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	query := string(data)
	tokens, err := countTestTokens(query)
	require.NoError(t, err)
	require.Greater(t, tokens, 512)
	return query
}

func TestQueryExecutor_Discover_ChunksLongQueriesForVectorSearch(t *testing.T) {
	emb := &testEmbedder{failIfTokensGreater: 512}
	searcher := &testSearcher{}
	exec := NewQueryExecutorWithSearch(&stubQueryDB{}, searcher, emb, 5*time.Second)

	longQuery := loadLargeDocQuery(t)
	res, err := exec.Discover(context.Background(), longQuery, nil, 10, 1)
	require.NoError(t, err)
	require.Equal(t, "vector", res.Method)
	require.NotEmpty(t, res.Results)

	emb.mu.Lock()
	calls := emb.calls
	maxTokens := emb.maxTokens
	emb.mu.Unlock()
	require.GreaterOrEqual(t, calls, 2, "expected multiple chunk embeddings")
	require.LessOrEqual(t, maxTokens, 512, "expected no embedding call on query chunks > 512 tokens")

	searcher.mu.Lock()
	hybridCalls := append([]string(nil), searcher.hybridCalls...)
	searchCalls := append([]string(nil), searcher.searchCalls...)
	searcher.mu.Unlock()
	require.GreaterOrEqual(t, len(hybridCalls), 2, "expected multiple per-chunk hybrid searches")
	require.Empty(t, searchCalls, "expected no text-only fallback when chunked vector search succeeds")

	maxQTokens := 0
	for _, q := range hybridCalls {
		tok, err := countTestTokens(q)
		require.NoError(t, err)
		if tok > maxQTokens {
			maxQTokens = tok
		}
	}
	require.LessOrEqual(t, maxQTokens, 512, "expected no hybrid search call on query chunks > 512 tokens")
}
