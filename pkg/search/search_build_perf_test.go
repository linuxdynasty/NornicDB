package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPropertyToString_SkipsDenseNumericVectors(t *testing.T) {
	got := propertyToString([]float32{0.1, 0.2, 0.3})
	require.Equal(t, "", got)

	got = propertyToString([]float64{0.1, 0.2, 0.3})
	require.Equal(t, "", got)

	denseAny := make([]any, 64)
	for i := range denseAny {
		denseAny[i] = float64(i) / 10.0
	}
	got = propertyToString(denseAny)
	require.Equal(t, "", got)

	smallAny := []any{1.0, 2.0, 3.0}
	got = propertyToString(smallAny)
	require.Equal(t, "1 2 3", got)
}

func TestVectorFromPropertyValue_DimensionAware(t *testing.T) {
	vec, ok := vectorFromPropertyValue([]float32{1, 2, 3}, 3)
	require.True(t, ok)
	require.Equal(t, []float32{1, 2, 3}, vec)

	_, ok = vectorFromPropertyValue([]float32{1, 2}, 3)
	require.False(t, ok)

	vec, ok = vectorFromPropertyValue([]any{1.0, 2.0, 3.0}, 3)
	require.True(t, ok)
	require.Equal(t, []float32{1, 2, 3}, vec)
}

func TestVectorFromPropertyValue_AdditionalBranches(t *testing.T) {
	_, ok := vectorFromPropertyValue([]float32{1, 2, 3}, 0)
	require.False(t, ok)

	vec, ok := vectorFromPropertyValue([]float64{1, 2, 3}, 3)
	require.True(t, ok)
	require.Equal(t, []float32{1, 2, 3}, vec)

	_, ok = vectorFromPropertyValue([]float64{1, 2}, 3)
	require.False(t, ok)

	vec, ok = vectorFromPropertyValue([]any{float32(1), int(2), int64(3)}, 3)
	require.True(t, ok)
	require.Equal(t, []float32{1, 2, 3}, vec)

	_, ok = vectorFromPropertyValue([]any{1, "bad", 3}, 3)
	require.False(t, ok)

	_, ok = vectorFromPropertyValue("not-a-vector", 3)
	require.False(t, ok)
}

func TestPropertyToString_AdditionalBranches(t *testing.T) {
	require.Equal(t, "hello", propertyToString("hello"))
	require.Equal(t, "a b", propertyToString([]string{"a", "b"}))
	require.Equal(t, "42", propertyToString(42))
	require.Equal(t, "true", propertyToString(true))
	require.Equal(t, "false", propertyToString(false))
	require.Equal(t, "", propertyToString(map[string]any{"k": "v"}))
}

func TestLooksLikeDenseNumericSlice_Branches(t *testing.T) {
	require.False(t, looksLikeDenseNumericSlice([]any{1, 2, 3}))

	mixed := make([]any, 40)
	for i := 0; i < 30; i++ {
		mixed[i] = float64(i)
	}
	for i := 30; i < len(mixed); i++ {
		mixed[i] = "x"
	}
	require.False(t, looksLikeDenseNumericSlice(mixed))

	dense := make([]any, 40)
	for i := range dense {
		dense[i] = float64(i)
	}
	require.True(t, looksLikeDenseNumericSlice(dense))
}

func TestBuildIndexes_IndexesNamedChunkAndPropertyVectors(t *testing.T) {
	t.Parallel()

	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 3)

	node := &storage.Node{
		ID:     "nornic:doc-vectors",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"title":     "vectorized doc",
			"customVec": []float32{0, 1, 0},
		},
		NamedEmbeddings: map[string][]float32{
			"titleVec": {1, 0, 0},
		},
		ChunkEmbeddings: [][]float32{
			{0, 0, 1},
			{0, 1, 0},
		},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	require.NoError(t, svc.BuildIndexes(context.Background()))

	// Expected vectors:
	// - named: "nornic:doc-vectors-named-titleVec"
	// - chunks: main id + chunk-0 + chunk-1
	// - custom property vector: "nornic:doc-vectors-prop-customVec"
	require.Equal(t, 5, svc.EmbeddingCount())

	named := svc.nodeNamedVector["nornic:doc-vectors"]
	require.NotNil(t, named)
	require.Equal(t, "nornic:doc-vectors-named-titleVec", named["titleVec"])

	props := svc.nodePropVector["nornic:doc-vectors"]
	require.NotNil(t, props)
	require.Equal(t, "nornic:doc-vectors-prop-customVec", props["customVec"])

	chunks := svc.nodeChunkVectors["nornic:doc-vectors"]
	require.Contains(t, chunks, "nornic:doc-vectors")
	require.Contains(t, chunks, "nornic:doc-vectors-chunk-0")
	require.Contains(t, chunks, "nornic:doc-vectors-chunk-1")
}
