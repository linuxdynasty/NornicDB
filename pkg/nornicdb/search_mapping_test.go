package nornicdb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMapSearchResponse(t *testing.T) {
	resp := &search.SearchResponse{
		Results: []search.SearchResult{
			{
				ID:         "n1",
				NodeID:     storage.NodeID("n1"),
				Labels:     []string{"Doc"},
				Properties: map[string]any{"title": "hello"},
				Score:      0.9,
				RRFScore:   0.7,
				VectorRank: 1,
				BM25Rank:   2,
			},
		},
	}

	got := MapSearchResponse(resp)
	require.Len(t, got, 1)
	require.NotNil(t, got[0].Node)
	require.Equal(t, "n1", got[0].Node.ID)
	require.Equal(t, []string{"Doc"}, got[0].Node.Labels)
	require.Equal(t, 0.9, got[0].Score)
	require.Equal(t, 0.7, got[0].RRFScore)
	require.Equal(t, 1, got[0].VectorRank)
	require.Equal(t, 2, got[0].BM25Rank)
}
