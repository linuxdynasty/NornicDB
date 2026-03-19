package cypher

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type limitedLabelScanEngine struct {
	*storage.MemoryEngine
	maxCalls int
	calls    int
}

func (e *limitedLabelScanEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	e.calls++
	if e.maxCalls > 0 && e.calls > e.maxCalls {
		return nil, fmt.Errorf("GetNodesByLabel called too many times: %d > %d", e.calls, e.maxCalls)
	}
	return e.MemoryEngine.GetNodesByLabel(label)
}

// BUG REGRESSION:
// Queries that contain UNWIND and multiple MATCH clauses should be routed through
// UNWIND-aware execution, not the multi-MATCH cartesian handler.
func TestBug_UnwindWithMultipleMatchClauses_RoutesAndExecutes(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "test:o1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"joinKey": "k1",
		},
	})
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     "test:t1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"joinKey": "k1",
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
MATCH (o:OriginalText)
UNWIND ['k1'] AS k
MATCH (t:TranslatedText)
WHERE o.joinKey = k AND t.joinKey = k
RETURN count(*) AS c
`, nil)
	require.NoError(t, err)
}

// BUG REGRESSION:
// Top-level UNWIND followed by multiple MATCH clauses should execute.
// Current behavior can misroute and return "expected multiple MATCH clauses".
func TestBug_TopLevelUnwindThenMultipleMatchClauses_Executes(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "test:o2",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"joinKey": "k2",
		},
	})
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     "test:t2",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"joinKey": "k2",
		},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
UNWIND ['k2'] AS joinKey
MATCH (o:OriginalText)
WHERE o.joinKey = joinKey
MATCH (t:TranslatedText)
WHERE t.joinKey = joinKey
RETURN count(*) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(1), res.Rows[0][0])
}

func TestNormalizeMultiMatchWhereClauses(t *testing.T) {
	in := "MATCH (o:OriginalText) WHERE o.joinKey = joinKey MATCH (t:TranslatedText) WHERE t.joinKey = joinKey RETURN count(*) AS c"
	got := normalizeMultiMatchWhereClauses(in)
	require.Equal(
		t,
		"MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE o.joinKey = joinKey AND t.joinKey = joinKey RETURN count(*) AS c",
		got,
	)
}

func TestRewriteUnwindCorrelationToIn(t *testing.T) {
	in := "MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE o.joinKey = joinKey AND t.joinKey = joinKey CREATE (o)-[:TRANSLATES_TO]->(t) RETURN count(*) AS c"
	got, ok := rewriteUnwindCorrelationToIn(in, "joinKey", "__unwind_items")
	require.True(t, ok)
	require.True(t, regexp.MustCompile(`o\.joinKey\s+IN\s+\$__unwind_items`).MatchString(got))
	require.True(t, regexp.MustCompile(`t\.joinKey\s*=\s*o\.joinKey`).MatchString(got))
}

func TestRewriteTopLevelMultiMatchToCartesianMatch(t *testing.T) {
	in := "MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE o.joinKey IN $__unwind_items AND t.joinKey = o.joinKey RETURN count(*) AS c"
	got := rewriteTopLevelMultiMatchToCartesianMatch(in)
	require.Equal(
		t,
		"MATCH (o:OriginalText), (t:TranslatedText) WHERE o.joinKey IN $__unwind_items AND t.joinKey = o.joinKey RETURN count(*) AS c",
		got,
	)
}

func TestBug_UnwindMatchCount_RewriteAvoidsPerItemLabelScans(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &limitedLabelScanEngine{MemoryEngine: base, maxCalls: 3}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("k%d", i)
		_, err := eng.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:o%d", i)), Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"joinKey": k}})
		require.NoError(t, err)
		_, err = eng.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:t%d", i)), Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"joinKey": k}})
		require.NoError(t, err)
	}

	res, err := exec.Execute(ctx, `
	UNWIND ['k1','k2','k3','k4','k5'] AS joinKey
	MATCH (o:OriginalText)
	WHERE o.joinKey = joinKey
	MATCH (t:TranslatedText)
	WHERE t.joinKey = joinKey
	RETURN count(*) AS c
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(5), res.Rows[0][0])
	require.LessOrEqual(t, eng.calls, 3)
}

func TestBug_UnwindMatchCreate_CorrelatedJoinKeepsCardinality(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("k%d", i)
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:o%d", i)), Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"joinKey": k}})
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:t%d", i)), Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"joinKey": k}})
		require.NoError(t, err)
	}

	res, err := exec.Execute(ctx, `
UNWIND ['k1','k2','k3','k4','k5'] AS joinKey
MATCH (o:OriginalText)
WHERE o.joinKey = joinKey
MATCH (t:TranslatedText)
WHERE t.joinKey = joinKey AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS c
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	require.Equal(t, int64(5), res.Rows[0][0])

	edges, err := store.AllEdges()
	require.NoError(t, err)
	translatesTo := 0
	for _, edge := range edges {
		if edge != nil && edge.Type == "TRANSLATES_TO" {
			translatesTo++
		}
	}
	require.Equal(t, 5, translatesTo)

	res2, err := exec.Execute(ctx, `
UNWIND ['k1','k2','k3','k4','k5'] AS joinKey
MATCH (o:OriginalText)
WHERE o.joinKey = joinKey
MATCH (t:TranslatedText)
WHERE t.joinKey = joinKey AND NOT (o)-[:TRANSLATES_TO]->(t)
CREATE (o)-[:TRANSLATES_TO]->(t)
RETURN count(*) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, res2.Rows, 1)
	require.Len(t, res2.Rows[0], 1)
	require.Equal(t, int64(0), res2.Rows[0][0])
}
