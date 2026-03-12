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

func TestTryFastChainedAgg_SupplierCategoryAndDistinctOrders(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	exec := NewStorageExecutor(store)

	customerID, err := store.CreateNode(&storage.Node{ID: "cust-1", Labels: []string{"Customer"}, Properties: map[string]interface{}{"companyName": "CustCo"}})
	require.NoError(t, err)
	orderID, err := store.CreateNode(&storage.Node{ID: "ord-1", Labels: []string{"Order"}, Properties: map[string]interface{}{"orderNo": "001"}})
	require.NoError(t, err)
	supplierID, err := store.CreateNode(&storage.Node{ID: "sup-1", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"companyName": "SupCo"}})
	require.NoError(t, err)
	productID, err := store.CreateNode(&storage.Node{ID: "prod-1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "Chai"}})
	require.NoError(t, err)
	categoryID, err := store.CreateNode(&storage.Node{ID: "cat-1", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryName": "Beverages"}})
	require.NoError(t, err)

	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-purchased", Type: "PURCHASED", StartNode: customerID, EndNode: orderID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-orders", Type: "ORDERS", StartNode: orderID, EndNode: productID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-supplies", Type: "SUPPLIES", StartNode: supplierID, EndNode: productID, Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-partof", Type: "PART_OF", StartNode: productID, EndNode: categoryID, Properties: map[string]interface{}{}}))

	supplierCategory := exec.parseTraversalPattern("(s:Supplier)-[:SUPPLIES]->(p:Product)-[:PART_OF]->(c:Category)")
	require.NotNil(t, supplierCategory)
	rows, ok, err := exec.tryFastRelationshipAggregations(supplierCategory, []returnItem{
		{expr: "s.companyName"},
		{expr: "c.categoryName"},
		{expr: "count(p)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "SupCo", rows[0][0])
	assert.Equal(t, "Beverages", rows[0][1])
	assert.Equal(t, int64(1), rows[0][2])

	customerCategory := exec.parseTraversalPattern("(c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)")
	require.NotNil(t, customerCategory)
	rows, ok, err = exec.tryFastRelationshipAggregations(customerCategory, []returnItem{
		{expr: "c.companyName"},
		{expr: "cat.categoryName"},
		{expr: "count(DISTINCT o)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "CustCo", rows[0][0])
	assert.Equal(t, "Beverages", rows[0][1])
	assert.Equal(t, int64(1), rows[0][2])

	customerSupplier := exec.parseTraversalPattern("(c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)<-[:SUPPLIES]-(s:Supplier)")
	require.NotNil(t, customerSupplier)
	rows, ok, err = exec.tryFastRelationshipAggregations(customerSupplier, []returnItem{
		{expr: "c.companyName"},
		{expr: "s.companyName"},
		{expr: "count(DISTINCT o)"},
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, rows, 1)
	assert.Equal(t, "CustCo", rows[0][0])
	assert.Equal(t, "SupCo", rows[0][1])
	assert.Equal(t, int64(1), rows[0][2])

	// Shape mismatch guard in chained fast path.
	_, ok, err = exec.tryFastRelationshipAggregations(customerSupplier, []returnItem{
		{expr: "c.companyName"},
		{expr: "s.companyName"},
		{expr: "count(o)"},
	})
	require.NoError(t, err)
	assert.False(t, ok)
}
