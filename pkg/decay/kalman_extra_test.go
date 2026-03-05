package decay

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newKalmanAdapter() *KalmanAdapter {
	m := New(nil)
	return NewKalmanAdapter(m, DefaultKalmanAdapterConfig())
}

func sampleMemory(id string, score float64) *MemoryInfo {
	now := time.Now()
	return &MemoryInfo{
		ID:           id,
		Tier:         TierEpisodic,
		CreatedAt:    now.Add(-24 * time.Hour),
		LastAccessed: now.Add(-1 * time.Hour),
		AccessCount:  5,
	}
}

func TestKalmanAdapter_GetSmoothedScore_Miss(t *testing.T) {
	ka := newKalmanAdapter()
	result := ka.GetSmoothedScore("nonexistent")
	assert.Nil(t, result)
}

func TestKalmanAdapter_GetSmoothedScore_Hit(t *testing.T) {
	ka := newKalmanAdapter()
	m := sampleMemory("n1", 0.5)

	// First calculate to populate cache
	score := ka.CalculateScore(m)
	assert.Greater(t, score, 0.0)

	// Now GetSmoothedScore should return the cached value
	cached := ka.GetSmoothedScore("n1")
	require.NotNil(t, cached)
	assert.Greater(t, cached.Smoothed, 0.0)
}

func TestKalmanAdapter_ShouldArchive_HighScore(t *testing.T) {
	ka := newKalmanAdapter()
	// A node with recent access and high count → high score → should NOT archive
	m := &MemoryInfo{
		ID:           "active",
		Tier:         TierEpisodic,
		CreatedAt:    time.Now().Add(-1 * time.Hour),
		LastAccessed: time.Now(),
		AccessCount:  100,
	}
	// Calculate score first to populate cache
	_ = ka.CalculateScore(m)
	result := ka.ShouldArchive(m)
	assert.False(t, result)
}

func TestKalmanAdapter_ShouldArchive_LowScore(t *testing.T) {
	ka := newKalmanAdapter()
	// Very old, rarely accessed → low score → likely archive
	m := &MemoryInfo{
		ID:           "stale",
		Tier:         TierEpisodic,
		CreatedAt:    time.Now().Add(-365 * 24 * time.Hour),
		LastAccessed: time.Now().Add(-180 * 24 * time.Hour),
		AccessCount:  1,
	}
	// No panic regardless of outcome
	result := ka.ShouldArchive(m)
	_ = result
}

func TestKalmanAdapter_ShouldArchive_Uncached(t *testing.T) {
	ka := newKalmanAdapter()
	// Memory not in cache at all – triggers fallback path
	m := &MemoryInfo{
		ID:           "unknown-node",
		Tier:         TierEpisodic,
		CreatedAt:    time.Now().Add(-10 * 24 * time.Hour),
		LastAccessed: time.Now().Add(-9 * 24 * time.Hour),
		AccessCount:  2,
	}
	result := ka.ShouldArchive(m)
	_ = result // no panic required
}

func TestKalmanAdapter_GetArchivalCandidates_Empty(t *testing.T) {
	ka := newKalmanAdapter()
	result := ka.GetArchivalCandidates([]*MemoryInfo{}, 10)
	assert.Empty(t, result)
}

func TestKalmanAdapter_GetArchivalCandidates_NoCachedScores(t *testing.T) {
	ka := newKalmanAdapter()
	m1 := sampleMemory("m1", 0.5)
	m2 := sampleMemory("m2", 0.3)
	// Scores not yet cached → no candidates
	result := ka.GetArchivalCandidates([]*MemoryInfo{m1, m2}, 10)
	assert.Empty(t, result)
}

func TestKalmanAdapter_RunDecayCycle_Empty(t *testing.T) {
	ka := newKalmanAdapter()
	ctx := context.Background()
	err := ka.RunDecayCycle(ctx, []*MemoryInfo{})
	assert.NoError(t, err)
}

func TestKalmanAdapter_RunDecayCycle_WithMemories(t *testing.T) {
	ka := newKalmanAdapter()
	ctx := context.Background()
	memories := []*MemoryInfo{
		sampleMemory("a", 0.5),
		sampleMemory("b", 0.3),
	}
	err := ka.RunDecayCycle(ctx, memories)
	assert.NoError(t, err)
}

func TestKalmanAdapter_RunDecayCycle_ContextCancelled(t *testing.T) {
	ka := newKalmanAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	memories := make([]*MemoryInfo, 5)
	for i := range memories {
		memories[i] = sampleMemory("node", 0.5)
	}
	err := ka.RunDecayCycle(ctx, memories)
	assert.Error(t, err)
}
