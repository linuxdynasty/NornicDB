package dbconfig

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolve_GlobalOnly(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_BM25_ENGINE", "v2")
	global := config.LoadDefaults()
	global.Memory.EmbeddingDimensions = 1536
	global.Memory.SearchMinSimilarity = 0.6
	r := Resolve(global, nil)
	require.NotNil(t, r)
	assert.Equal(t, 1536, r.EmbeddingDimensions)
	assert.Equal(t, 0.6, r.SearchMinSimilarity)
	assert.Equal(t, "v2", r.BM25Engine)
	assert.NotEmpty(t, r.Effective["NORNICDB_EMBEDDING_DIMENSIONS"])
}

func TestResolve_Overrides(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_BM25_ENGINE", "v2")
	global := config.LoadDefaults()
	global.Memory.EmbeddingDimensions = 1024
	global.Memory.SearchMinSimilarity = 0.5
	overrides := map[string]string{
		"NORNICDB_EMBEDDING_DIMENSIONS":  "768",
		"NORNICDB_SEARCH_MIN_SIMILARITY": "0.8",
		"NORNICDB_SEARCH_BM25_ENGINE":    "v2",
	}
	r := Resolve(global, overrides)
	require.NotNil(t, r)
	assert.Equal(t, 768, r.EmbeddingDimensions)
	assert.Equal(t, 0.8, r.SearchMinSimilarity)
	assert.Equal(t, "v2", r.BM25Engine)
	assert.Equal(t, "768", r.Effective["NORNICDB_EMBEDDING_DIMENSIONS"])
	assert.Equal(t, "0.8", r.Effective["NORNICDB_SEARCH_MIN_SIMILARITY"])
	assert.Equal(t, "v2", r.Effective["NORNICDB_SEARCH_BM25_ENGINE"])
}

func TestResolve_DefaultDimensionsAndIgnoredOverrides(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_BM25_ENGINE", "unexpected")
	global := config.LoadDefaults()
	global.Memory.EmbeddingDimensions = 0
	global.Memory.SearchMinSimilarity = 0.55

	r := Resolve(global, map[string]string{
		"NOT_ALLOWED":                    "ignored",
		"NORNICDB_EMBEDDING_DIMENSIONS":  "-5",
		"NORNICDB_SEARCH_BM25_ENGINE":    "v1",
		"NORNICDB_SEARCH_MIN_SIMILARITY": "bad",
	})
	require.NotNil(t, r)
	assert.Equal(t, 1024, r.EmbeddingDimensions)
	assert.Equal(t, 0.55, r.SearchMinSimilarity)
	assert.Equal(t, "v1", r.BM25Engine)
	_, ok := r.Effective["NOT_ALLOWED"]
	assert.False(t, ok)
}

func TestApplyOverride(t *testing.T) {
	r := &ResolvedDbConfig{
		EmbeddingDimensions: 1024,
		SearchMinSimilarity: 0.5,
		BM25Engine:          "v2",
		Effective:           map[string]string{},
	}

	applyOverride(r, "NORNICDB_EMBEDDING_DIMENSIONS", " 2048 ")
	assert.Equal(t, 2048, r.EmbeddingDimensions)

	applyOverride(r, "NORNICDB_EMBEDDING_DIMENSIONS", "0")
	assert.Equal(t, 1024, r.EmbeddingDimensions)

	applyOverride(r, "NORNICDB_SEARCH_MIN_SIMILARITY", "0.75")
	assert.Equal(t, 0.75, r.SearchMinSimilarity)

	applyOverride(r, "NORNICDB_SEARCH_MIN_SIMILARITY", "0")
	assert.Equal(t, 0.0, r.SearchMinSimilarity)

	applyOverride(r, "NORNICDB_SEARCH_BM25_ENGINE", "V1")
	assert.Equal(t, "v1", r.BM25Engine)

	applyOverride(r, "NORNICDB_EMBEDDING_ENABLED", "1")
	assert.Equal(t, "v1", r.BM25Engine)

	applyOverride(r, "UNKNOWN_KEY", "value")
	assert.Equal(t, 1024, r.EmbeddingDimensions)
}

func TestIsAllowedKey(t *testing.T) {
	assert.True(t, IsAllowedKey("NORNICDB_EMBEDDING_MODEL"))
	assert.True(t, IsAllowedKey("NORNICDB_SEARCH_MIN_SIMILARITY"))
	assert.True(t, IsAllowedKey("NORNICDB_EMBEDDING_API_KEY"))
	assert.False(t, IsAllowedKey("UNKNOWN_KEY"))
}
