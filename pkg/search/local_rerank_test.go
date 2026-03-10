package search

import (
	"context"
	"testing"
)

// mockScorer returns fixed scores for testing.
type mockScorer map[string]float32

func (m mockScorer) Score(ctx context.Context, query, document string) (float32, error) {
	key := query + "|" + document
	if s, ok := m[key]; ok {
		return s, nil
	}
	return 0.5, nil
}

func TestLocalReranker_Enabled(t *testing.T) {
	scorer := mockScorer{}
	cfg := DefaultLocalRerankerConfig()
	cfg.Enabled = false
	r := NewLocalReranker(scorer, cfg)
	if r.Enabled() {
		t.Error("expected disabled when config.Enabled=false")
	}
	cfg.Enabled = true
	r = NewLocalReranker(scorer, cfg)
	if !r.Enabled() {
		t.Error("expected enabled when config.Enabled=true and scorer non-nil")
	}
	r = NewLocalReranker(nil, cfg)
	if r.Enabled() {
		t.Error("expected disabled when scorer is nil")
	}
}

func TestLocalReranker_Name(t *testing.T) {
	r := NewLocalReranker(mockScorer{}, nil)
	if got := r.Name(); got != "local_gguf" {
		t.Errorf("Name() = %q, want local_gguf", got)
	}
}

func TestLocalReranker_IsAvailable(t *testing.T) {
	cfg := DefaultLocalRerankerConfig()
	cfg.Enabled = true

	okReranker := NewLocalReranker(mockScorer{
		"health|check": 0.9,
	}, cfg)
	if !okReranker.IsAvailable(context.Background()) {
		t.Fatal("expected IsAvailable true when health check scoring succeeds")
	}

	badReranker := NewLocalReranker(failingScorer{}, cfg)
	if badReranker.IsAvailable(context.Background()) {
		t.Fatal("expected IsAvailable false when health check scoring fails")
	}
}

func TestLocalReranker_Rerank_EmptyCandidates(t *testing.T) {
	r := NewLocalReranker(mockScorer{}, DefaultLocalRerankerConfig())
	got, err := r.Rerank(context.Background(), "q", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("Rerank(nil) = %v, want nil", got)
	}
	got, _ = r.Rerank(context.Background(), "q", []RerankCandidate{})
	if len(got) != 0 {
		t.Errorf("Rerank(empty) = %d results, want 0", len(got))
	}
}

func TestLocalReranker_Rerank_ReordersByScore(t *testing.T) {
	scorer := mockScorer{
		"q|docA": 0.2,
		"q|docB": 0.9,
		"q|docC": 0.5,
	}
	cfg := DefaultLocalRerankerConfig()
	cfg.Enabled = true
	r := NewLocalReranker(scorer, cfg)

	candidates := []RerankCandidate{
		{ID: "a", Content: "docA", Score: 0.1},
		{ID: "b", Content: "docB", Score: 0.2},
		{ID: "c", Content: "docC", Score: 0.3},
	}
	results, err := r.Rerank(context.Background(), "q", candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	// Order should be B (0.9), C (0.5), A (0.2)
	eps := 1e-5
	if results[0].ID != "b" || results[0].FinalScore < 0.9-eps || results[0].FinalScore > 0.9+eps {
		t.Errorf("first result: id=%s score=%.4f, want id=b score=0.90", results[0].ID, results[0].FinalScore)
	}
	if results[1].ID != "c" || results[1].FinalScore < 0.5-eps || results[1].FinalScore > 0.5+eps {
		t.Errorf("second result: id=%s score=%.4f, want id=c score=0.50", results[1].ID, results[1].FinalScore)
	}
	if results[2].ID != "a" || results[2].FinalScore < 0.2-eps || results[2].FinalScore > 0.2+eps {
		t.Errorf("third result: id=%s score=%.4f, want id=a score=0.20", results[2].ID, results[2].FinalScore)
	}
}

func TestLocalReranker_Rerank_ScorerError_PassThrough(t *testing.T) {
	scorer := &failingScorer{}
	cfg := DefaultLocalRerankerConfig()
	cfg.Enabled = true
	r := NewLocalReranker(scorer, cfg)
	candidates := []RerankCandidate{
		{ID: "a", Content: "docA", Score: 0.5},
	}
	results, err := r.Rerank(context.Background(), "q", candidates)
	if err != nil {
		t.Fatal(err)
	}
	// Fail-open: pass-through order
	if len(results) != 1 || results[0].ID != "a" {
		t.Errorf("expected pass-through on scorer error: got %v", results)
	}
}

type failingScorer struct{}

func (failingScorer) Score(context.Context, string, string) (float32, error) {
	return 0, context.DeadlineExceeded
}
