package localllm

import (
	"context"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/textchunk"
)

func wordTokenCount(text string) (int, error) {
	fields := strings.Fields(text)
	return len(fields), nil
}

// skipOnConstrainedEnv skips tests in memory-constrained environments
func skipOnConstrainedEnv(t testing.TB) {
	t.Helper()
	if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		t.Skip("Skipping model loading test in CI environment")
	}
	if runtime.GOOS == "windows" {
		t.Skip("Skipping model loading test on Windows due to memory constraints")
	}
}

// TestDefaultOptions verifies default options are reasonable
func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions("/tmp/test.gguf")

	if opts.ModelPath != "/tmp/test.gguf" {
		t.Errorf("ModelPath = %q, want /tmp/test.gguf", opts.ModelPath)
	}
	if opts.ContextSize != 0 {
		t.Errorf("ContextSize = %d, want 0 (auto)", opts.ContextSize)
	}
	if opts.BatchSize != 0 {
		t.Errorf("BatchSize = %d, want 0 (auto)", opts.BatchSize)
	}
	if opts.Threads < 1 {
		t.Errorf("Threads = %d, want >= 1", opts.Threads)
	}
	if opts.Threads > 8 {
		t.Errorf("Threads = %d, want <= 8", opts.Threads)
	}
	if opts.GPULayers != -1 {
		t.Errorf("GPULayers = %d, want -1 (auto)", opts.GPULayers)
	}
}

func TestResolveEmbeddingContextAndBatch(t *testing.T) {
	tests := []struct {
		name      string
		opts      Options
		trainCtx  int
		wantCtx   int
		wantBatch int
	}{
		{
			name:      "auto uses train context capped",
			opts:      Options{},
			trainCtx:  8192,
			wantCtx:   8192,
			wantBatch: 8192,
		},
		{
			name:      "auto clamps to smaller model context",
			opts:      Options{},
			trainCtx:  4096,
			wantCtx:   4096,
			wantBatch: 4096,
		},
		{
			name:      "explicit context clamps to model context",
			opts:      Options{ContextSize: 12000, BatchSize: 12000},
			trainCtx:  8192,
			wantCtx:   8192,
			wantBatch: 8192,
		},
		{
			name:      "batch clamps to effective context",
			opts:      Options{ContextSize: 6000, BatchSize: 7000},
			trainCtx:  0,
			wantCtx:   6000,
			wantBatch: 6000,
		},
		{
			name:      "fallback when train context unknown",
			opts:      Options{},
			trainCtx:  0,
			wantCtx:   defaultEmbeddingContextCap,
			wantBatch: defaultEmbeddingContextCap,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCtx, gotBatch := resolveEmbeddingContextAndBatch(tt.opts, tt.trainCtx)
			if gotCtx != tt.wantCtx {
				t.Fatalf("ctx=%d want=%d", gotCtx, tt.wantCtx)
			}
			if gotBatch != tt.wantBatch {
				t.Fatalf("batch=%d want=%d", gotBatch, tt.wantBatch)
			}
		})
	}
}

func TestResolveGenerationContextAndBatch(t *testing.T) {
	tests := []struct {
		name      string
		opts      GenerationOptions
		trainCtx  int
		wantCtx   int
		wantBatch int
	}{
		{
			name:      "defaults use train context and default batch",
			opts:      GenerationOptions{},
			trainCtx:  8192,
			wantCtx:   8192,
			wantBatch: 512,
		},
		{
			name:      "defaults fall back when train context unknown",
			opts:      GenerationOptions{},
			trainCtx:  0,
			wantCtx:   2048,
			wantBatch: 512,
		},
		{
			name:      "explicit context clamps to model context",
			opts:      GenerationOptions{ContextSize: 12000, BatchSize: 1024},
			trainCtx:  8192,
			wantCtx:   8192,
			wantBatch: 1024,
		},
		{
			name:      "batch clamps to effective context",
			opts:      GenerationOptions{ContextSize: 384, BatchSize: 1024},
			trainCtx:  0,
			wantCtx:   384,
			wantBatch: 384,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCtx, gotBatch := resolveGenerationContextAndBatch(tt.opts, tt.trainCtx)
			if gotCtx != tt.wantCtx {
				t.Fatalf("ctx=%d want=%d", gotCtx, tt.wantCtx)
			}
			if gotBatch != tt.wantBatch {
				t.Fatalf("batch=%d want=%d", gotBatch, tt.wantBatch)
			}
		})
	}
}

func TestChunkTextByTokenCount_DeterministicLimits(t *testing.T) {
	text := strings.Join([]string{
		"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
	}, " ")

	chunks, err := textchunk.ChunkByTokenCount(text, 4, 1, wordTokenCount)
	if err != nil {
		t.Fatalf("chunkTextByTokenCount failed: %v", err)
	}
	if len(chunks) < 3 {
		t.Fatalf("expected multiple chunks, got %v", chunks)
	}
	for i, chunk := range chunks {
		tok, err := wordTokenCount(chunk)
		if err != nil {
			t.Fatalf("count tokens for chunk %d: %v", i, err)
		}
		if tok > 4 {
			t.Fatalf("chunk %d exceeds token cap: got %d tokens in %q", i, tok, chunk)
		}
	}
	if chunks[0] != "one two three four" {
		t.Fatalf("unexpected first chunk: %q", chunks[0])
	}
	if chunks[1] != "four five six seven" {
		t.Fatalf("unexpected overlapped second chunk: %q", chunks[1])
	}
}

func TestChunkTextByTokenCount_OverlapClamped(t *testing.T) {
	text := "alpha beta gamma delta"
	chunks, err := textchunk.ChunkByTokenCount(text, 2, 5, wordTokenCount)
	if err != nil {
		t.Fatalf("chunkTextByTokenCount failed: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatal("expected chunks")
	}
	for i, chunk := range chunks {
		tok, err := wordTokenCount(chunk)
		if err != nil {
			t.Fatalf("count tokens for chunk %d: %v", i, err)
		}
		if tok > 2 {
			t.Fatalf("chunk %d exceeds clamped cap: got %d tokens in %q", i, tok, chunk)
		}
	}
}

// TestLoadModel_FileNotFound verifies error on missing model file
func TestLoadModel_FileNotFound(t *testing.T) {
	t.Skip("Skipping: requires llama.cpp static library")

	opts := DefaultOptions("/nonexistent/model.gguf")
	_, err := LoadModel(opts)
	if err == nil {
		t.Error("Expected error for non-existent model file")
	}
}

// TestModel_Integration is an integration test requiring actual model
func TestModel_Integration(t *testing.T) {
	skipOnConstrainedEnv(t)
	modelPath := os.Getenv("TEST_GGUF_MODEL")
	if modelPath == "" {
		t.Skip("Skipping: TEST_GGUF_MODEL not set")
	}

	opts := DefaultOptions(modelPath)
	opts.GPULayers = 0 // Force CPU for CI

	model, err := LoadModel(opts)
	if err != nil {
		t.Fatalf("LoadModel failed: %v", err)
	}
	defer model.Close()

	t.Logf("Model loaded: %d dimensions", model.Dimensions())

	// Test single embedding
	ctx := context.Background()
	vec, err := model.Embed(ctx, "hello world")
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}

	if len(vec) != model.Dimensions() {
		t.Errorf("Embedding length = %d, want %d", len(vec), model.Dimensions())
	}
	if model.MaxTokens() < 1 {
		t.Fatalf("MaxTokens = %d, want > 0", model.MaxTokens())
	}

	// Verify normalization
	var sumSq float32
	for _, v := range vec {
		sumSq += v * v
	}
	if sumSq < 0.99 || sumSq > 1.01 {
		t.Errorf("Embedding not normalized: sum of squares = %f", sumSq)
	}

	// Regression guard: verify we can tokenize/embed beyond legacy 512-token limit
	// when the model context supports it.
	if model.MaxTokens() > 640 {
		longText := strings.Repeat("embedding token ", 640)
		if _, err := model.Embed(ctx, longText); err != nil {
			t.Fatalf("Embed longText failed with MaxTokens=%d: %v", model.MaxTokens(), err)
		}
	}
}

// TestModel_BatchEmbedding tests batch embedding
func TestModel_BatchEmbedding(t *testing.T) {
	skipOnConstrainedEnv(t)
	modelPath := os.Getenv("TEST_GGUF_MODEL")
	if modelPath == "" {
		t.Skip("Skipping: TEST_GGUF_MODEL not set")
	}

	opts := DefaultOptions(modelPath)
	opts.GPULayers = 0

	model, err := LoadModel(opts)
	if err != nil {
		t.Fatalf("LoadModel failed: %v", err)
	}
	defer model.Close()

	texts := []string{"hello", "world", "test"}
	ctx := context.Background()

	vecs, err := model.EmbedBatch(ctx, texts)
	if err != nil {
		t.Fatalf("EmbedBatch failed: %v", err)
	}

	if len(vecs) != len(texts) {
		t.Errorf("Got %d embeddings, want %d", len(vecs), len(texts))
	}
}

// BenchmarkEmbed measures embedding performance
func BenchmarkEmbed(b *testing.B) {
	skipOnConstrainedEnv(b)
	modelPath := os.Getenv("TEST_GGUF_MODEL")
	if modelPath == "" {
		b.Skip("Skipping: TEST_GGUF_MODEL not set")
	}

	opts := DefaultOptions(modelPath)
	model, err := LoadModel(opts)
	if err != nil {
		b.Fatalf("LoadModel failed: %v", err)
	}
	defer model.Close()

	ctx := context.Background()
	text := "The quick brown fox jumps over the lazy dog"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := model.Embed(ctx, text)
		if err != nil {
			b.Fatal(err)
		}
	}
}
