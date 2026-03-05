package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMergeChain_OptionalMatchForeach_CreatesOnlyWhenMatched(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (a:TypeA {name: 'A1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c:TypeC {name: 'C1'})`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MERGE (e:Entity {id: 'opt1'})
		WITH e
		OPTIONAL MATCH (a:TypeA {name: 'A1'})
		FOREACH (x IN CASE WHEN a IS NOT NULL THEN [1] ELSE [] END |
			MERGE (e)-[:REL_A]->(a)
		)
		WITH e
		OPTIONAL MATCH (b:TypeB {name: 'NONEXISTENT'})
		FOREACH (x IN CASE WHEN b IS NOT NULL THEN [1] ELSE [] END |
			MERGE (e)-[:REL_B]->(b)
		)
		WITH e
		OPTIONAL MATCH (c:TypeC {name: 'C1'})
		FOREACH (x IN CASE WHEN c IS NOT NULL THEN [1] ELSE [] END |
			MERGE (e)-[:REL_C]->(c)
		)
		RETURN e.id
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "opt1", result.Rows[0][0])

	relACount, err := exec.Execute(ctx, `MATCH (e:Entity {id: 'opt1'})-[:REL_A]->(:TypeA) RETURN count(*) as c`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), relACount.Rows[0][0])

	relBCount, err := exec.Execute(ctx, `MATCH (e:Entity {id: 'opt1'})-[:REL_B]->(:TypeB) RETURN count(*) as c`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), relBCount.Rows[0][0])

	relCCount, err := exec.Execute(ctx, `MATCH (e:Entity {id: 'opt1'})-[:REL_C]->(:TypeC) RETURN count(*) as c`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), relCCount.Rows[0][0])
}

func TestForeach_ReplacesLoopVariable_NotMapKeys(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Regression: naive strings.ReplaceAll would turn "{i: i}" into "{1: 1}".
	_, err := exec.Execute(ctx, `FOREACH (i IN [1] | CREATE (:Item {i: i}))`, nil)
	require.NoError(t, err)

	got, err := exec.Execute(ctx, `MATCH (n:Item) RETURN n.i`, nil)
	require.NoError(t, err)
	require.Len(t, got.Rows, 1)
	require.Equal(t, int64(1), got.Rows[0][0])
}
