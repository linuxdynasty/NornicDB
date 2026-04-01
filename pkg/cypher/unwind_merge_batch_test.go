package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindMergeBatch_HopUpsertShape(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	hops := make([]map[string]interface{}, 0, 72)
	for row := 0; row < 12; row++ {
		for depth := 1; depth <= 6; depth++ {
			hops = append(hops, map[string]interface{}{
				"hopId": fmt.Sprintf("benchhop-%03d:%d", row, depth),
				"runID": "bench-deep-hop-v1",
			})
		}
	}

	res, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": hops})
	require.NoError(t, err)
	require.Equal(t, []string{"prepared"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(72), toInt64ForTest(t, res.Rows[0][0]))

	nodes, err := store.GetNodesByLabel("BenchmarkHop")
	require.NoError(t, err)
	require.Len(t, nodes, 72)
	for _, n := range nodes {
		require.Equal(t, "bench-deep-hop-v1", n.Properties["benchmarkRun"])
	}
}

func TestUnwindMergeBatch_HopUpsertUpdatesExisting(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	first := []map[string]interface{}{
		{"hopId": "benchhop-000:1", "runID": "v1"},
		{"hopId": "benchhop-000:2", "runID": "v1"},
	}
	_, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": first})
	require.NoError(t, err)

	second := []map[string]interface{}{
		{"hopId": "benchhop-000:1", "runID": "v2"},
		{"hopId": "benchhop-000:2", "runID": "v2"},
	}
	res, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": second})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))

	nodes, err := store.GetNodesByLabel("BenchmarkHop")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, n := range nodes {
		require.Equal(t, "v2", n.Properties["benchmarkRun"])
	}
}

func TestUnwindMergeBatch_HopUpsertDuplicateKeys_LastRowWinsAndCountPreserved(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Ensure index-aware path can be used for existing lookups.
	require.NoError(t, store.GetSchema().AddPropertyIndex("idx_benchhop_hopid", "BenchmarkHop", []string{"hopId"}))

	rows := []map[string]interface{}{
		{"hopId": "benchhop-dupe:1", "runID": "v1"},
		{"hopId": "benchhop-dupe:2", "runID": "v1"},
		{"hopId": "benchhop-dupe:1", "runID": "v2"}, // duplicate key, should win
	}

	res, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": rows})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	// Cypher semantics: count(h) in this shape counts input rows, including duplicates.
	require.Equal(t, int64(3), toInt64ForTest(t, res.Rows[0][0]))

	nodes, err := store.GetNodesByLabel("BenchmarkHop")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, n := range nodes {
		if n.Properties["hopId"] == "benchhop-dupe:1" {
			require.Equal(t, "v2", n.Properties["benchmarkRun"])
		}
	}
}
