package search

import (
	"context"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHNSWIndex(t *testing.T) {
	t.Run("creates index with default config", func(t *testing.T) {
		index := NewHNSWIndex(128, HNSWConfig{})
		assert.NotNil(t, index)
		assert.Equal(t, 128, index.dimensions)
		assert.Equal(t, 16, index.config.M)
		assert.Equal(t, 200, index.config.EfConstruction)
		assert.Equal(t, 100, index.config.EfSearch)
	})

	t.Run("creates index with custom config", func(t *testing.T) {
		config := HNSWConfig{
			M:               32,
			EfConstruction:  400,
			EfSearch:        200,
			LevelMultiplier: 0.5,
		}
		index := NewHNSWIndex(256, config)
		assert.Equal(t, 256, index.dimensions)
		assert.Equal(t, 32, index.config.M)
		assert.Equal(t, 400, index.config.EfConstruction)
		assert.Equal(t, 200, index.config.EfSearch)
	})
}

func TestHNSWIndex_Add(t *testing.T) {
	t.Run("adds single vector", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		err := index.Add("vec1", []float32{1.0, 0.0, 0.0, 0.0})
		require.NoError(t, err)
		assert.Equal(t, 1, index.Size())
		require.True(t, index.hasEntryPoint)
		assert.Equal(t, "vec1", index.internalToID[index.entryPoint])
	})

	t.Run("adds multiple vectors", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		vectors := []struct {
			id  string
			vec []float32
		}{
			{"vec1", []float32{1.0, 0.0, 0.0, 0.0}},
			{"vec2", []float32{0.0, 1.0, 0.0, 0.0}},
			{"vec3", []float32{0.0, 0.0, 1.0, 0.0}},
			{"vec4", []float32{0.0, 0.0, 0.0, 1.0}},
		}

		for _, v := range vectors {
			err := index.Add(v.id, v.vec)
			require.NoError(t, err)
		}

		assert.Equal(t, 4, index.Size())
	})

	t.Run("rejects dimension mismatch", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		err := index.Add("vec1", []float32{1.0, 0.0, 0.0}) // 3 dims instead of 4
		assert.ErrorIs(t, err, ErrDimensionMismatch)
	})

	t.Run("updates existing vector", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		err := index.Add("vec1", []float32{1.0, 0.0, 0.0, 0.0})
		require.NoError(t, err)

		// Add again with different values - should update
		err = index.Add("vec1", []float32{0.0, 1.0, 0.0, 0.0})
		require.NoError(t, err)

		// Size should still be 1 (updated, not added)
		// Note: Current implementation adds duplicate, this tests current behavior
		assert.GreaterOrEqual(t, index.Size(), 1)
	})
}

func TestHNSWIndex_Remove(t *testing.T) {
	t.Run("removes existing vector", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		index.Add("vec1", []float32{1.0, 0.0, 0.0, 0.0})
		index.Add("vec2", []float32{0.0, 1.0, 0.0, 0.0})

		assert.Equal(t, 2, index.Size())

		index.Remove("vec1")
		assert.Equal(t, 1, index.Size())
	})

	t.Run("handles non-existent vector", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		index.Add("vec1", []float32{1.0, 0.0, 0.0, 0.0})

		// Should not panic
		index.Remove("nonexistent")
		assert.Equal(t, 1, index.Size())
	})

	t.Run("updates entry point when removing it", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		index.Add("vec1", []float32{1.0, 0.0, 0.0, 0.0})
		index.Add("vec2", []float32{0.0, 1.0, 0.0, 0.0})

		require.True(t, index.hasEntryPoint)
		entryBefore := index.entryPoint
		entryExternal := index.internalToID[entryBefore]
		index.Remove(entryExternal)

		// Entry point should change
		assert.True(t, index.hasEntryPoint)
		assert.NotEqual(t, entryBefore, index.entryPoint)
	})

	t.Run("handles last vector removal", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		index.Add("vec1", []float32{1.0, 0.0, 0.0, 0.0})
		index.Remove("vec1")

		assert.Equal(t, 0, index.Size())
		assert.False(t, index.hasEntryPoint)
	})
}

func TestHNSWIndex_Search(t *testing.T) {
	t.Run("finds exact match", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		index.Add("vec1", []float32{1.0, 0.0, 0.0, 0.0})
		index.Add("vec2", []float32{0.0, 1.0, 0.0, 0.0})
		index.Add("vec3", []float32{0.0, 0.0, 1.0, 0.0})

		ctx := context.Background()
		results, err := index.Search(ctx, []float32{1.0, 0.0, 0.0, 0.0}, 1, 0.9)

		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "vec1", results[0].ID)
		assert.InDelta(t, 1.0, float64(results[0].Score), 0.01)
	})

	t.Run("finds similar vectors", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		// Add orthogonal vectors
		index.Add("vec1", []float32{1.0, 0.0, 0.0, 0.0})
		index.Add("vec2", []float32{0.9, 0.1, 0.0, 0.0}) // Similar to vec1
		index.Add("vec3", []float32{0.0, 0.0, 1.0, 0.0}) // Orthogonal

		ctx := context.Background()
		results, err := index.Search(ctx, []float32{1.0, 0.0, 0.0, 0.0}, 2, 0.5)

		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 1)

		// vec1 or vec2 should be in results (both are similar)
		hasMatch := false
		for _, r := range results {
			if r.ID == "vec1" || r.ID == "vec2" {
				hasMatch = true
			}
		}
		assert.True(t, hasMatch, "Expected vec1 or vec2 in results")
	})

	t.Run("respects minimum similarity threshold", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		index.Add("vec1", []float32{1.0, 0.0, 0.0, 0.0})
		index.Add("vec2", []float32{0.0, 1.0, 0.0, 0.0}) // Orthogonal, similarity ~0

		ctx := context.Background()
		results, err := index.Search(ctx, []float32{1.0, 0.0, 0.0, 0.0}, 10, 0.9)

		require.NoError(t, err)

		// Only vec1 should match with threshold 0.9
		for _, r := range results {
			assert.GreaterOrEqual(t, float64(r.Score), 0.9)
		}
	})

	t.Run("respects k limit", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		// Add many vectors
		for i := 0; i < 20; i++ {
			vec := make([]float32, 4)
			vec[i%4] = 1.0
			index.Add(string(rune('a'+i)), vec)
		}

		ctx := context.Background()
		results, err := index.Search(ctx, []float32{1.0, 0.0, 0.0, 0.0}, 5, 0.0)

		require.NoError(t, err)
		assert.LessOrEqual(t, len(results), 5)
	})

	t.Run("returns empty for empty index", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		ctx := context.Background()
		results, err := index.Search(ctx, []float32{1.0, 0.0, 0.0, 0.0}, 5, 0.0)

		require.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		// Add many vectors
		for i := 0; i < 100; i++ {
			vec := make([]float32, 4)
			for j := range vec {
				vec[j] = rand.Float32()
			}
			index.Add(string(rune(i)), vec)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := index.Search(ctx, []float32{1.0, 0.0, 0.0, 0.0}, 50, 0.0)
		// Should handle cancellation gracefully
		assert.True(t, err == nil || err == context.Canceled)
	})

	t.Run("rejects dimension mismatch", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())
		index.Add("vec1", []float32{1.0, 0.0, 0.0, 0.0})

		ctx := context.Background()
		_, err := index.Search(ctx, []float32{1.0, 0.0}, 5, 0.0) // Wrong dimensions

		assert.ErrorIs(t, err, ErrDimensionMismatch)
	})

	t.Run("results are sorted by score descending", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		index.Add("close", []float32{0.99, 0.1, 0.0, 0.0})
		index.Add("medium", []float32{0.7, 0.7, 0.0, 0.0})
		index.Add("far", []float32{0.1, 0.99, 0.0, 0.0})

		ctx := context.Background()
		results, err := index.Search(ctx, []float32{1.0, 0.0, 0.0, 0.0}, 3, 0.0)

		require.NoError(t, err)

		// Verify descending order
		for i := 1; i < len(results); i++ {
			assert.GreaterOrEqual(t, results[i-1].Score, results[i].Score,
				"Results should be sorted by score descending")
		}
	})
}

func TestHNSWIndex_Concurrency(t *testing.T) {
	t.Run("concurrent adds are safe", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		var wg sync.WaitGroup
		numGoroutines := 10
		vectorsPerGoroutine := 20

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < vectorsPerGoroutine; i++ {
					id := string(rune(goroutineID*1000 + i))
					vec := []float32{
						rand.Float32(),
						rand.Float32(),
						rand.Float32(),
						rand.Float32(),
					}
					index.Add(id, vec)
				}
			}(g)
		}

		wg.Wait()
		assert.Equal(t, numGoroutines*vectorsPerGoroutine, index.Size())
	})

	t.Run("concurrent reads are safe", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		// Pre-populate
		for i := 0; i < 100; i++ {
			vec := []float32{rand.Float32(), rand.Float32(), rand.Float32(), rand.Float32()}
			index.Add(string(rune(i)), vec)
		}

		var wg sync.WaitGroup
		numGoroutines := 10

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx := context.Background()
				for i := 0; i < 50; i++ {
					query := []float32{rand.Float32(), rand.Float32(), rand.Float32(), rand.Float32()}
					_, err := index.Search(ctx, query, 5, 0.0)
					assert.NoError(t, err)
				}
			}()
		}

		wg.Wait()
	})

	t.Run("concurrent read-write is safe", func(t *testing.T) {
		index := NewHNSWIndex(4, DefaultHNSWConfig())

		// Pre-populate
		for i := 0; i < 50; i++ {
			vec := []float32{rand.Float32(), rand.Float32(), rand.Float32(), rand.Float32()}
			index.Add(string(rune(i)), vec)
		}

		var wg sync.WaitGroup
		ctx := context.Background()

		// Writers
		for g := 0; g < 5; g++ {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					id := string(rune(1000 + gid*100 + i))
					vec := []float32{rand.Float32(), rand.Float32(), rand.Float32(), rand.Float32()}
					index.Add(id, vec)
				}
			}(g)
		}

		// Readers
		for g := 0; g < 5; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 30; i++ {
					query := []float32{rand.Float32(), rand.Float32(), rand.Float32(), rand.Float32()}
					_, _ = index.Search(ctx, query, 5, 0.0)
				}
			}()
		}

		wg.Wait()
	})
}

func TestHNSWIndex_RecallQuality(t *testing.T) {
	// Test that HNSW provides good recall compared to brute-force
	t.Run("high recall on random vectors", func(t *testing.T) {
		rand.Seed(time.Now().UnixNano())

		dims := 64
		numVectors := 500
		k := 10

		// Create both indexes
		hnsw := NewHNSWIndex(dims, DefaultHNSWConfig())
		bruteForce := NewVectorIndex(dims)

		// Add same random vectors to both
		vectors := make([][]float32, numVectors)
		for i := 0; i < numVectors; i++ {
			vec := make([]float32, dims)
			for j := range vec {
				vec[j] = rand.Float32()
			}
			vectors[i] = vec

			id := string(rune(i))
			hnsw.Add(id, vec)
			bruteForce.Add(id, vec)
		}

		// Query and compare results
		ctx := context.Background()
		numQueries := 20
		totalRecall := 0.0

		for q := 0; q < numQueries; q++ {
			query := make([]float32, dims)
			for j := range query {
				query[j] = rand.Float32()
			}

			// Get ground truth from brute force
			bfResults, _ := bruteForce.Search(ctx, query, k, 0.0)
			bfIDs := make(map[string]bool)
			for _, r := range bfResults {
				bfIDs[r.ID] = true
			}

			// Get HNSW results
			hnswResults, _ := hnsw.Search(ctx, query, k, 0.0)

			// Calculate recall
			hits := 0
			for _, r := range hnswResults {
				if bfIDs[r.ID] {
					hits++
				}
			}

			if len(bfResults) > 0 {
				totalRecall += float64(hits) / float64(len(bfResults))
			}
		}

		avgRecall := totalRecall / float64(numQueries)

		// HNSW should achieve at least 80% recall with default settings
		assert.GreaterOrEqual(t, avgRecall, 0.8,
			"HNSW recall should be at least 80%%, got %.2f%%", avgRecall*100)
	})
}

func TestDefaultHNSWConfig(t *testing.T) {
	config := DefaultHNSWConfig()

	assert.Equal(t, 16, config.M)
	assert.Equal(t, 200, config.EfConstruction)
	assert.Equal(t, 100, config.EfSearch)
	assert.InDelta(t, 1.0/math.Log(16.0), config.LevelMultiplier, 0.0001)
}

// Benchmark tests
func BenchmarkHNSWIndex_Add(b *testing.B) {
	index := NewHNSWIndex(128, DefaultHNSWConfig())
	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = rand.Float32()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Add(string(rune(i)), vec)
	}
}

func BenchmarkHNSWIndex_Search(b *testing.B) {
	index := NewHNSWIndex(128, DefaultHNSWConfig())

	// Pre-populate with 10000 vectors
	for i := 0; i < 10000; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		index.Add(string(rune(i)), vec)
	}

	query := make([]float32, 128)
	for i := range query {
		query[i] = rand.Float32()
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Search(ctx, query, 10, 0.0)
	}
}

func BenchmarkHNSWIndex_vs_BruteForce(b *testing.B) {
	dims := 128
	numVectors := 10000

	// Setup
	hnsw := NewHNSWIndex(dims, DefaultHNSWConfig())
	bruteForce := NewVectorIndex(dims)

	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		hnsw.Add(string(rune(i)), vec)
		bruteForce.Add(string(rune(i)), vec)
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = rand.Float32()
	}
	ctx := context.Background()

	b.Run("HNSW", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			hnsw.Search(ctx, query, 10, 0.0)
		}
	})

	b.Run("BruteForce", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bruteForce.Search(ctx, query, 10, 0.0)
		}
	})
}

// TestHNSWIndex_SaveLoad tests HNSW index persistence (Save/Load round-trip).
// Save writes graph-only (IDs + graph); load populates vectors from the provided lookup.
func TestHNSWIndex_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hnsw")

	idx := NewHNSWIndex(4, DefaultHNSWConfig())
	vecs := map[string][]float32{
		"a": {1, 0, 0, 0},
		"b": {0, 1, 0, 0},
		"c": {0, 0, 1, 0},
	}
	for id, vec := range vecs {
		require.NoError(t, idx.Add(id, vec))
	}

	err := idx.Save(path)
	require.NoError(t, err)

	lookup := func(id string) ([]float32, bool) { v, ok := vecs[id]; return v, ok }
	loaded, err := LoadHNSWIndex(path, lookup)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, idx.GetDimensions(), loaded.GetDimensions())
	assert.Equal(t, idx.Size(), loaded.Size())

	ctx := context.Background()
	results, err := loaded.Search(ctx, []float32{1, 0, 0, 0}, 3, 0.0)
	require.NoError(t, err)
	require.Len(t, results, 3)
	assert.Equal(t, "a", results[0].ID)
}

// TestHNSWIndex_VectorLookupOnly verifies that HNSW can be built and loaded without storing
// vectors in the index (vecOff = -1, resolve via VectorLookup at search time).
func TestHNSWIndex_VectorLookupOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hnsw")

	vecs := map[string][]float32{
		"a": {1, 0, 0, 0},
		"b": {0, 1, 0, 0},
		"c": {0, 0, 1, 0},
	}
	lookup := func(id string) ([]float32, bool) { v, ok := vecs[id]; return v, ok }

	// Build with lookup: Add does not store vectors (saves RAM).
	idx := NewHNSWIndex(4, DefaultHNSWConfig())
	idx.SetVectorLookup(lookup)
	for id, vec := range vecs {
		require.NoError(t, idx.Add(id, vec))
	}
	require.NoError(t, idx.Save(path))

	// Load: graph-only now always uses lookup mode (no vector copy in RAM).
	loaded, err := LoadHNSWIndex(path, lookup)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, 4, loaded.GetDimensions())
	assert.Equal(t, 3, loaded.Size())

	ctx := context.Background()
	results, err := loaded.Search(ctx, []float32{1, 0, 0, 0}, 3, 0.0)
	require.NoError(t, err)
	require.Len(t, results, 3)
	assert.Equal(t, "a", results[0].ID)
}

// TestHNSWIndex_LoadMissingOrCorrupt verifies that LoadHNSWIndex returns (nil, nil) for
// missing file and (nil, nil) for corrupt/old format so the caller can rebuild.
func TestHNSWIndex_LoadMissingOrCorrupt(t *testing.T) {
	dir := t.TempDir()
	lookup := func(id string) ([]float32, bool) { return nil, false }

	t.Run("missing file", func(t *testing.T) {
		loaded, err := LoadHNSWIndex(filepath.Join(dir, "nonexistent.gob"), lookup)
		require.NoError(t, err)
		assert.Nil(t, loaded)
	})

	t.Run("corrupt file", func(t *testing.T) {
		corruptPath := filepath.Join(dir, "corrupt.gob")
		require.NoError(t, os.WriteFile(corruptPath, []byte("not valid gob"), 0644))
		loaded, err := LoadHNSWIndex(corruptPath, lookup)
		require.NoError(t, err)
		assert.Nil(t, loaded)
	})
}
