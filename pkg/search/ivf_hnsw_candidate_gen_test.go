package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIVFHNSW_UsedAfterClustering_CPUOnly(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", "1")

	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(1)

	// Two obvious groups, large enough to use an ANN pipeline (>= NSmallMax).
	for i := 0; i < 3000; i++ {
		n := &storage.Node{
			ID:              storage.NodeID("a-" + itoa(i)),
			Labels:          []string{"Doc"},
			ChunkEmbeddings: [][]float32{{1, 0, 0, 0}},
		}
		require.NoError(t, svc.IndexNode(n))
	}
	for i := 0; i < 3000; i++ {
		n := &storage.Node{
			ID:              storage.NodeID("b-" + itoa(i)),
			Labels:          []string{"Doc"},
			ChunkEmbeddings: [][]float32{{0, 1, 0, 0}},
		}
		require.NoError(t, svc.IndexNode(n))
	}

	require.NoError(t, svc.TriggerClustering(context.Background()))

	pipeline, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)

	// In CPU-only mode, centroid routing should select IVF-HNSW once per-cluster
	// indexes have been built.
	_, ok := pipeline.candidateGen.(*IVFHNSWCandidateGen)
	require.True(t, ok)

	got, err := pipeline.Search(context.Background(), []float32{1, 0, 0, 0}, 5, 0.0)
	require.NoError(t, err)
	require.NotEmpty(t, got)
}

func TestIVFHNSWCandidateGen_ErrorAndFallbackBranches(t *testing.T) {
	query := []float32{1, 0, 0}

	// Not clustered path.
	gen := NewIVFHNSWCandidateGen(nil, nil, 1)
	_, err := gen.SearchCandidates(context.Background(), query, 5, 0.0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cluster index not clustered")
	require.Equal(t, 1, gen.numClustersToSearch)

	// Non-positive cluster count should normalize to default.
	genDefault := NewIVFHNSWCandidateGen(nil, nil, 0)
	require.Equal(t, 3, genDefault.numClustersToSearch)

	embCfg := gpu.DefaultEmbeddingIndexConfig(3)
	embCfg.GPUEnabled = true
	embCfg.AutoSync = true
	clusterIndex := gpu.NewClusterIndex(nil, embCfg, &gpu.KMeansConfig{
		NumClusters:   1,
		MaxIterations: 5,
		Tolerance:     0.001,
		InitMethod:    "kmeans++",
	})
	require.NoError(t, clusterIndex.Add("d1", []float32{1, 0, 0}))
	require.NoError(t, clusterIndex.Add("d2", []float32{0.9, 0.1, 0}))
	require.NoError(t, clusterIndex.Cluster())

	// Missing per-cluster lookup path.
	gen = NewIVFHNSWCandidateGen(clusterIndex, nil, 1)
	_, err = gen.SearchCandidates(context.Background(), query, 5, 0.0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "lookup not configured")

	// Missing per-cluster HNSW falls back to exact clustered search.
	gen = NewIVFHNSWCandidateGen(clusterIndex, func(clusterID int) *HNSWIndex { return nil }, 1)
	gen.SetClusterSelector(func(context.Context, []float32, int) []int { return []int{0} })
	cands, err := gen.SearchCandidates(context.Background(), query, 5, -1.0)
	require.NoError(t, err)
	require.NotEmpty(t, cands)

	// Canceled context is propagated from per-cluster HNSW search path.
	hidx := NewHNSWIndex(3, DefaultHNSWConfig())
	require.NoError(t, hidx.Add("d1", []float32{1, 0, 0}))
	gen = NewIVFHNSWCandidateGen(clusterIndex, func(clusterID int) *HNSWIndex { return hidx }, 1)
	gen.SetClusterSelector(func(context.Context, []float32, int) []int { return []int{0} })
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = gen.SearchCandidates(cancelled, query, 5, -1.0)
	require.ErrorIs(t, err, context.Canceled)
}
