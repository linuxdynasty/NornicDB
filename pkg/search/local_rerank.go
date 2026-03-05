// Package search provides local GGUF-based Stage-2 reranking.
//
// LocalReranker uses a BGE-style reranker model (e.g. BGE-Reranker-v2-m3) loaded
// via localllm, scoring (query, document) pairs and reordering candidates by
// relevance. It is independent of Heimdall and dedicated to search like the
// embedding model.

package search

import (
	"context"
	"sort"
	"strings"
	"time"
)

// RerankScorer scores a (query, document) pair for relevance.
// Implementations are typically *localllm.RerankerModel (loaded from GGUF).
type RerankScorer interface {
	Score(ctx context.Context, query, document string) (float32, error)
}

// LocalRerankerConfig configures local GGUF reranker behavior.
type LocalRerankerConfig struct {
	Enabled bool

	Timeout time.Duration

	// MaxCandidates caps how many candidates are scored per query.
	MaxCandidates int

	// MaxDocChars truncates each candidate's content before scoring.
	MaxDocChars int

	// MinScore filters out candidates below this score (0 = no filter).
	MinScore float64
}

// DefaultLocalRerankerConfig returns sensible defaults for BGE-style reranking.
func DefaultLocalRerankerConfig() *LocalRerankerConfig {
	return &LocalRerankerConfig{
		Enabled:       true,
		Timeout:       15 * time.Second,
		MaxCandidates: 50,
		MaxDocChars:   512,
		MinScore:      0.0,
	}
}

// LocalReranker performs Stage-2 reranking using a local GGUF reranker model.
type LocalReranker struct {
	scorer RerankScorer
	cfg    *LocalRerankerConfig
}

// NewLocalReranker creates a reranker that uses the given scorer (e.g. *localllm.RerankerModel).
func NewLocalReranker(scorer RerankScorer, cfg *LocalRerankerConfig) *LocalReranker {
	if cfg == nil {
		cfg = DefaultLocalRerankerConfig()
	}
	return &LocalReranker{scorer: scorer, cfg: cfg}
}

func (r *LocalReranker) Name() string { return "local_gguf" }

func (r *LocalReranker) Enabled() bool {
	return r != nil && r.scorer != nil && r.cfg != nil && r.cfg.Enabled
}

func (r *LocalReranker) IsAvailable(ctx context.Context) bool {
	if !r.Enabled() {
		return false
	}
	_, err := r.scorer.Score(ctx, "health", "check")
	return err == nil
}

// Rerank scores each candidate with the scorer and returns results sorted by score (desc).
// Fail-open: on error returns original order.
func (r *LocalReranker) Rerank(ctx context.Context, query string, candidates []RerankCandidate) ([]RerankResult, error) {
	if !r.Enabled() {
		return r.passThrough(candidates), nil
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	maxCandidates := r.cfg.MaxCandidates
	if maxCandidates <= 0 {
		maxCandidates = 50
	}
	if len(candidates) > maxCandidates {
		candidates = candidates[:maxCandidates]
	}

	maxDocChars := r.cfg.MaxDocChars
	if maxDocChars <= 0 {
		maxDocChars = 512
	}

	callCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()

	q := strings.TrimSpace(query)
	type scored struct {
		idx   int
		score float64
	}
	scores := make([]scored, 0, len(candidates))

	for i, c := range candidates {
		doc := strings.TrimSpace(c.Content)
		if len(doc) > maxDocChars {
			doc = doc[:maxDocChars]
		}
		score, err := r.scorer.Score(callCtx, q, doc)
		if err != nil {
			return r.passThrough(candidates), nil
		}
		s := float64(score)
		if r.cfg.MinScore > 0 && s < r.cfg.MinScore {
			continue
		}
		scores = append(scores, scored{idx: i, score: s})
	}

	if len(scores) == 0 {
		return r.passThrough(candidates), nil
	}

	sort.Slice(scores, func(i, j int) bool { return scores[i].score > scores[j].score })

	results := make([]RerankResult, 0, len(scores))
	for rank, s := range scores {
		c := candidates[s.idx]
		results = append(results, RerankResult{
			ID:           c.ID,
			Content:      c.Content,
			OriginalRank: s.idx + 1,
			NewRank:      rank + 1,
			BiScore:      c.Score,
			CrossScore:   s.score,
			FinalScore:   s.score,
		})
	}
	return results, nil
}

func (r *LocalReranker) passThrough(candidates []RerankCandidate) []RerankResult {
	out := make([]RerankResult, len(candidates))
	for i, c := range candidates {
		out[i] = RerankResult{
			ID:           c.ID,
			Content:      c.Content,
			OriginalRank: i + 1,
			NewRank:      i + 1,
			BiScore:      c.Score,
			CrossScore:   c.Score,
			FinalScore:   c.Score,
		}
	}
	return out
}
