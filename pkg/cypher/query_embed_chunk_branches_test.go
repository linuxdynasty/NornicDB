package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmbedQueryChunked_Branches_MaxChunkCapAndDimMismatch(t *testing.T) {
	ctx := context.Background()

	// Force many chunks to exercise maxQueryChunks cap.
	longText := strings.Repeat("alpha beta gamma delta epsilon ", 5000)
	capped := &sequenceEmbedder{}
	_, err := embedQueryChunked(ctx, capped, longText)
	require.NoError(t, err)
	assert.LessOrEqual(t, capped.calls, 32, "embedder should not be called above max chunk cap")

	// Mixed dimensions: mismatched vectors should be skipped, matching vectors averaged.
	// Here first and third are 2D, second is 3D (ignored).
	dimMismatch := &sequenceEmbedder{
		embs: [][]float32{
			{1, 0},
			{9, 9, 9},
			{0, 1},
		},
	}
	emb, err := embedQueryChunked(ctx, dimMismatch, strings.Repeat("chunk ", 900))
	require.NoError(t, err)
	require.Len(t, emb, 2)
	// Average of [1,0] and [0,1], normalized.
	assert.InDelta(t, 0.707, emb[0], 0.01)
	assert.InDelta(t, 0.707, emb[1], 0.01)
}
