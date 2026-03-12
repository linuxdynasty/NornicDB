package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryFastSingleHopAgg_GroupValue(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	exec := NewStorageExecutor(store)

	c1, err := store.CreateNode(&storage.Node{ID: "c1", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryName": "Beverages"}})
	require.NoError(t, err)
	c2, err := store.CreateNode(&storage.Node{ID: "c2", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryName": "Condiments"}})
	require.NoError(t, err)

	p1, err := store.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Chai", "unitPrice": 18.0}})
	require.NoError(t, err)
	p2, err := store.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Chang", "unitPrice": 19.0}})
	require.NoError(t, err)
	p3, err := store.CreateNode(&storage.Node{ID: "p3", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Aniseed Syrup", "unitPrice": 10.0}})
	require.NoError(t, err)

	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", Type: "PART_OF", StartNode: p1, EndNode: c1, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e2", Type: "PART_OF", StartNode: p2, EndNode: c1, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e3", Type: "PART_OF", StartNode: p3, EndNode: c2, Properties: map[string]interface{}{}}))

	matches := exec.parseTraversalPattern("(c:Category)<-[:PART_OF]-(p:Product)")
	require.NotNil(t, matches)

	items := []returnItem{
		{expr: "c.categoryName"},
		{expr: "count(p)", alias: "productCount"},
	}

	rows, ok, err := exec.tryFastRelationshipAggregations(matches, items)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 2)
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r[0].(string)] = true
	}
	require.True(t, seen["Beverages"])
}

func TestTryFastRelationshipAggregations_EarlyGuards(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	exec := NewStorageExecutor(store)

	items := []returnItem{{expr: "c.categoryName"}, {expr: "count(p)"}}

	matches := exec.parseTraversalPattern("(c:Category)<-[:PART_OF*1..2]-(p:Product)")
	require.NotNil(t, matches)
	_, ok, err := exec.tryFastRelationshipAggregations(matches, items)
	require.NoError(t, err)
	assert.False(t, ok)

	chained := &TraversalMatch{
		IsChained: true,
		Segments:  []TraversalSegment{{}, {}, {}, {}},
		Relationship: RelationshipPattern{
			MinHops: 1,
			MaxHops: 1,
		},
	}
	_, ok, err = exec.tryFastRelationshipAggregations(chained, items)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestTryFastSingleHopAgg_AggregateVariants(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	exec := NewStorageExecutor(store)

	catID, err := store.CreateNode(&storage.Node{
		ID:     "cat",
		Labels: []string{"Category"},
		Properties: map[string]interface{}{
			"categoryName": "Beverages",
		},
	})
	require.NoError(t, err)
	p1, err := store.CreateNode(&storage.Node{
		ID:     "p1",
		Labels: []string{"Product"},
		Properties: map[string]interface{}{
			"unitPrice": int64(10),
			"name":      "Tea",
		},
	})
	require.NoError(t, err)
	p2, err := store.CreateNode(&storage.Node{
		ID:     "p2",
		Labels: []string{"Product"},
		Properties: map[string]interface{}{
			"unitPrice": float64(20),
			"name":      "Coffee",
		},
	})
	require.NoError(t, err)

	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:        "e1",
		Type:      "PART_OF",
		StartNode: p1,
		EndNode:   catID,
		Properties: map[string]interface{}{
			"weight": int64(2),
		},
	}))
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:        "e2",
		Type:      "PART_OF",
		StartNode: p2,
		EndNode:   catID,
		Properties: map[string]interface{}{
			"weight": float64(3.5),
		},
	}))

	matches := exec.parseTraversalPattern("(c:Category)<-[r:PART_OF]-(p:Product)")
	require.NotNil(t, matches)

	rows, ok, err := exec.tryFastRelationshipAggregations(matches, []returnItem{
		{expr: "c.categoryName"},
		{expr: "sum(r.weight)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "Beverages", rows[0][0])
	assert.Equal(t, float64(5.5), rows[0][1])

	rows, ok, err = exec.tryFastRelationshipAggregations(matches, []returnItem{
		{expr: "c.categoryName"},
		{expr: "avg(p.unitPrice)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, float64(15), rows[0][1])

	rows, ok, err = exec.tryFastRelationshipAggregations(matches, []returnItem{
		{expr: "c.categoryName"},
		{expr: "collect(p.name)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	names, ok := rows[0][1].([]interface{})
	require.True(t, ok)
	assert.ElementsMatch(t, []interface{}{"Tea", "Coffee"}, names)
}
