package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type stubInferenceManager struct{}
type stubVectorEmbedder struct {
	vec []float32
}

func (s *stubVectorEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return s.vec, nil
}

func (s *stubInferenceManager) Generate(ctx context.Context, prompt string, params heimdall.GenerateParams) (string, error) {
	return "generated: " + prompt, nil
}

func (s *stubInferenceManager) Chat(ctx context.Context, req heimdall.ChatRequest) (*heimdall.ChatResponse, error) {
	return &heimdall.ChatResponse{
		Model: "stub-model",
		Choices: []heimdall.ChatChoice{
			{
				Message:      &heimdall.ChatMessage{Role: "assistant", Content: "chat-response"},
				FinishReason: "stop",
			},
		},
		Usage: &heimdall.ChatUsage{
			PromptTokens:     3,
			CompletionTokens: 2,
			TotalTokens:      5,
		},
	}, nil
}

func TestCallDbRetrieveAndRerank(t *testing.T) {
	ctx := context.Background()
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err := store.CreateNode(&storage.Node{
		ID:         storage.NodeID("doc-1"),
		Labels:     []string{"Document"},
		Properties: map[string]interface{}{"content": "alpha retrieval test"},
	})
	require.NoError(t, err)

	svc := search.NewService(store)
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)

	retrieveRes, err := exec.Execute(ctx, "CALL db.retrieve({query: 'alpha', limit: 5})", nil)
	require.NoError(t, err)
	require.NotEmpty(t, retrieveRes.Columns)
	assert.Equal(t, "node", retrieveRes.Columns[0])
	require.GreaterOrEqual(t, len(retrieveRes.Rows), 1)

	rretrieveRes, err := exec.Execute(ctx, "CALL db.rretrieve({query: 'alpha', limit: 5})", nil)
	require.NoError(t, err)
	require.NotEmpty(t, rretrieveRes.Columns)
	assert.Equal(t, "node", rretrieveRes.Columns[0])
	require.GreaterOrEqual(t, len(rretrieveRes.Rows), 1)
}

func TestCallDbInfer(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	exec.SetInferenceManager(&stubInferenceManager{})

	genRes, err := exec.Execute(ctx, "CALL db.infer({prompt: 'hello world', max_tokens: 32, temperature: 0.2})", nil)
	require.NoError(t, err)
	require.Len(t, genRes.Rows, 1)
	assert.Equal(t, "generated: hello world", genRes.Rows[0][0])
	assert.Equal(t, "stop", genRes.Rows[0][5])

	chatRes, err := exec.Execute(ctx, "CALL db.infer({messages: [{role: 'user', content: 'hi'}], model: 'stub-model'})", nil)
	require.NoError(t, err)
	require.Len(t, chatRes.Rows, 1)
	assert.Equal(t, "chat-response", chatRes.Rows[0][0])
	assert.Equal(t, "stub-model", chatRes.Rows[0][2])
	usage, ok := chatRes.Rows[0][3].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, 5, usage["total_tokens"])
}

func TestCallDbRerankCandidates(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test"))

	res, err := exec.Execute(ctx, "CALL db.rerank({query: 'alpha', candidates: [{id: 'a', content: 'alpha text', score: 0.9}, {id: 'b', content: 'beta text', score: 0.4}]})", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "content", "original_rank", "new_rank", "bi_score", "cross_score", "final_score"}, res.Columns)
	require.Len(t, res.Rows, 2)
	assert.Equal(t, "a", res.Rows[0][0])
}

func TestCallDbIndexVectorEmbed(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test"))
	exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{0.1, 0.2, 0.3, 0.4}})

	res, err := exec.Execute(ctx, "CALL db.index.vector.embed('hello world') YIELD embedding", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"embedding"}, res.Columns)
	require.Len(t, res.Rows, 1)
	embedding, ok := res.Rows[0][0].([]float32)
	require.True(t, ok)
	assert.Equal(t, []float32{0.1, 0.2, 0.3, 0.4}, embedding)
}
