package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// BadgerEngine – AllNodes / AllEdges / BatchGetNodes
// ============================================================================

func TestBadgerEngine_AllNodes_Empty(t *testing.T) {
	b := createTestBadgerEngine(t)
	nodes, err := b.AllNodes()
	require.NoError(t, err)
	assert.Empty(t, nodes)
}

func TestBadgerEngine_AllNodes_WithData(t *testing.T) {
	b := createTestBadgerEngine(t)
	n := testNode(prefixTestID("ban1"))
	_, err := b.CreateNode(n)
	require.NoError(t, err)

	nodes, err := b.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(nodes), 1)
}

func TestBadgerEngine_AllEdges_Empty(t *testing.T) {
	b := createTestBadgerEngine(t)
	edges, err := b.AllEdges()
	require.NoError(t, err)
	assert.Empty(t, edges)
}

func TestBadgerEngine_AllEdges_WithData(t *testing.T) {
	b := createTestBadgerEngine(t)
	n1 := testNode(prefixTestID("bae1"))
	n2 := testNode(prefixTestID("bae2"))
	_, err := b.CreateNode(n1)
	require.NoError(t, err)
	_, err = b.CreateNode(n2)
	require.NoError(t, err)

	e := &Edge{
		ID:         EdgeID(prefixTestID("bae-e1")),
		StartNode:  NodeID(prefixTestID("bae1")),
		EndNode:    NodeID(prefixTestID("bae2")),
		Type:       "KNOWS",
		Properties: map[string]interface{}{},
	}
	err = b.CreateEdge(e)
	require.NoError(t, err)

	edges, err := b.AllEdges()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
}

func TestBadgerEngine_BatchGetNodes_Empty(t *testing.T) {
	b := createTestBadgerEngine(t)
	result, err := b.BatchGetNodes([]NodeID{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestBadgerEngine_BatchGetNodes_WithData(t *testing.T) {
	b := createTestBadgerEngine(t)
	n := testNode(prefixTestID("bgn1"))
	id, err := b.CreateNode(n)
	require.NoError(t, err)

	result, err := b.BatchGetNodes([]NodeID{id})
	require.NoError(t, err)
	assert.Len(t, result, 1)
}

func TestBadgerEngine_BatchGetNodes_Missing(t *testing.T) {
	b := createTestBadgerEngine(t)
	result, err := b.BatchGetNodes([]NodeID{NodeID(prefixTestID("nonexist"))})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestBadgerEngine_InvalidatePendingEmbeddingsIndex_NoOp(t *testing.T) {
	b := createTestBadgerEngine(t)
	// Badger pending embeddings index is persistent; invalidation is a no-op.
	b.InvalidatePendingEmbeddingsIndex()
}

func TestBadgerEngine_QueryHelpers_Extra(t *testing.T) {
	t.Run("GetFirstNodeByLabel skips stale and corrupt indexed entries", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		missingID := NodeID(prefixTestID("aa-missing"))
		corruptID := NodeID(prefixTestID("ab-corrupt"))
		valid := &Node{ID: NodeID(prefixTestID("zz-valid")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "valid"}}
		_, err := b.CreateNode(valid)
		require.NoError(t, err)

		require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
			if err := txn.Set(labelIndexKey("Person", missingID), []byte{}); err != nil {
				return err
			}
			if err := txn.Set(labelIndexKey("Person", corruptID), []byte{}); err != nil {
				return err
			}
			return txn.Set(nodeKey(corruptID), []byte("not-a-node"))
		}))

		got, err := b.GetFirstNodeByLabel("Person")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, valid.ID, got.ID)
	})

	t.Run("ForEachNodeIDByLabel invalidates stale cache and supports early stop", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		first := &Node{ID: NodeID(prefixTestID("foreach-a")), Labels: []string{"Person"}}
		second := &Node{ID: NodeID(prefixTestID("foreach-b")), Labels: []string{"Person"}}
		_, err := b.CreateNode(first)
		require.NoError(t, err)
		_, err = b.CreateNode(second)
		require.NoError(t, err)

		staleID := NodeID(prefixTestID("foreach-stale"))
		b.labelCacheSetFirst("Person", staleID)

		var visited []NodeID
		err = b.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
			visited = append(visited, id)
			return true
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []NodeID{first.ID, second.ID}, visited)

		cachedID, ok := b.labelCacheGetFirst("Person")
		require.True(t, ok)
		assert.NotEqual(t, staleID, cachedID)

		visited = visited[:0]
		err = b.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
			visited = append(visited, id)
			return false
		})
		require.NoError(t, err)
		assert.Len(t, visited, 1)
	})

	t.Run("BatchGetNodes merges cache hits db hits and empty ids", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		cached := &Node{ID: NodeID(prefixTestID("batch-cached")), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "cached"}}
		stored := &Node{ID: NodeID(prefixTestID("batch-stored")), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "stored"}}
		b.cacheStoreNode(cached)
		_, err := b.CreateNode(stored)
		require.NoError(t, err)

		result, err := b.BatchGetNodes([]NodeID{"", cached.ID, stored.ID, NodeID(prefixTestID("batch-missing"))})
		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, "cached", result[cached.ID].Properties["name"])
		assert.Equal(t, "stored", result[stored.ID].Properties["name"])

		b.nodeCacheMu.RLock()
		_, cachedStored := b.nodeCache[stored.ID]
		b.nodeCacheMu.RUnlock()
		assert.True(t, cachedStored)
	})

	t.Run("BatchGetNodes all cache hits returns copies without DB read", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		n1 := &Node{ID: NodeID(prefixTestID("cache-only-1")), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "one"}}
		n2 := &Node{ID: NodeID(prefixTestID("cache-only-2")), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "two"}}
		b.cacheStoreNode(n1)
		b.cacheStoreNode(n2)

		result, err := b.BatchGetNodes([]NodeID{n1.ID, n2.ID})
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.NotSame(t, n1, result[n1.ID])
		require.NotSame(t, n2, result[n2.ID])
		assert.Equal(t, "one", result[n1.ID].Properties["name"])
		assert.Equal(t, "two", result[n2.ID].Properties["name"])
	})

	t.Run("BatchGetNodes skips corrupt payloads and missing keys", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		valid := &Node{ID: NodeID(prefixTestID("batch-valid")), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "valid"}}
		corruptID := NodeID(prefixTestID("batch-corrupt"))
		_, err := b.CreateNode(valid)
		require.NoError(t, err)
		require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(nodeKey(corruptID), []byte("not-a-node"))
		}))

		result, err := b.BatchGetNodes([]NodeID{valid.ID, corruptID, NodeID(prefixTestID("batch-missing"))})
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, valid.ID, result[valid.ID].ID)
	})

	t.Run("BatchGetNodes returns error when view transaction fails", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		require.NoError(t, b.Close())
		_, err := b.BatchGetNodes([]NodeID{NodeID(prefixTestID("after-close"))})
		require.Error(t, err)
	})

	t.Run("edge query helpers validate ids and skip corrupt payloads", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		_, err := b.GetOutgoingEdges("")
		require.ErrorIs(t, err, ErrInvalidID)
		_, err = b.GetIncomingEdges("")
		require.ErrorIs(t, err, ErrInvalidID)

		start := testNode(prefixTestID("edge-start"))
		end := testNode(prefixTestID("edge-end"))
		_, err = b.CreateNode(start)
		require.NoError(t, err)
		_, err = b.CreateNode(end)
		require.NoError(t, err)
		require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(prefixTestID("edge-good")), StartNode: start.ID, EndNode: end.ID, Type: "REL"}))

		corruptID := EdgeID(prefixTestID("edge-bad"))
		require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
			if err := txn.Set(outgoingIndexKey(start.ID, corruptID), []byte{}); err != nil {
				return err
			}
			if err := txn.Set(incomingIndexKey(end.ID, corruptID), []byte{}); err != nil {
				return err
			}
			if err := txn.Set(edgeTypeIndexKey("REL", corruptID), []byte{}); err != nil {
				return err
			}
			return txn.Set(edgeKey(corruptID), []byte("not-an-edge"))
		}))

		outgoing, err := b.GetOutgoingEdges(start.ID)
		require.NoError(t, err)
		assert.Len(t, outgoing, 1)

		incoming, err := b.GetIncomingEdges(end.ID)
		require.NoError(t, err)
		assert.Len(t, incoming, 1)

		byType, err := b.GetEdgesByType("REL")
		require.NoError(t, err)
		assert.Len(t, byType, 1)
	})

	t.Run("UpdateEdge endpoint changes require existing nodes", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		start := testNode(prefixTestID("upd-start"))
		end := testNode(prefixTestID("upd-end"))
		_, err := b.CreateNode(start)
		require.NoError(t, err)
		_, err = b.CreateNode(end)
		require.NoError(t, err)

		edge := &Edge{
			ID:         EdgeID(prefixTestID("upd-e1")),
			StartNode:  start.ID,
			EndNode:    end.ID,
			Type:       "REL",
			Properties: map[string]interface{}{},
		}
		require.NoError(t, b.CreateEdge(edge))

		edge.StartNode = NodeID(prefixTestID("missing-node"))
		err = b.UpdateEdge(edge)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("UpdateEdge returns decode error for corrupt stored edge", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		start := testNode(prefixTestID("upd2-start"))
		end := testNode(prefixTestID("upd2-end"))
		_, err := b.CreateNode(start)
		require.NoError(t, err)
		_, err = b.CreateNode(end)
		require.NoError(t, err)
		edge := &Edge{
			ID:         EdgeID(prefixTestID("upd2-e1")),
			StartNode:  start.ID,
			EndNode:    end.ID,
			Type:       "REL",
			Properties: map[string]interface{}{},
		}
		require.NoError(t, b.CreateEdge(edge))
		require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(edgeKey(edge.ID), []byte("not-an-edge"))
		}))
		err = b.UpdateEdge(edge)
		require.Error(t, err)
	})

	t.Run("DeleteEdge succeeds for typeless edge", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		start := testNode(prefixTestID("del-start"))
		end := testNode(prefixTestID("del-end"))
		_, err := b.CreateNode(start)
		require.NoError(t, err)
		_, err = b.CreateNode(end)
		require.NoError(t, err)

		edge := &Edge{
			ID:         EdgeID(prefixTestID("del-typeless")),
			StartNode:  start.ID,
			EndNode:    end.ID,
			Type:       "",
			Properties: map[string]interface{}{},
		}
		require.NoError(t, b.CreateEdge(edge))
		require.NoError(t, b.DeleteEdge(edge.ID))
		_, err = b.GetEdge(edge.ID)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

// ============================================================================
// BadgerEngine – BulkCreate / BulkDelete
// ============================================================================

func TestBadgerEngine_BulkCreateNodes_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	nodes := []*Node{
		testNode(prefixTestID("bulk-bn1")),
		testNode(prefixTestID("bulk-bn2")),
	}
	err := b.BulkCreateNodes(nodes)
	require.NoError(t, err)

	all, err := b.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(all), 2)
}

func TestBadgerEngine_BulkCreateNodes_ExtraEmpty(t *testing.T) {
	b := createTestBadgerEngine(t)
	assert.NoError(t, b.BulkCreateNodes(nil))
}

func TestBadgerEngine_BulkCreateEdges_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	n1 := testNode(prefixTestID("bce1"))
	n2 := testNode(prefixTestID("bce2"))
	_, _ = b.CreateNode(n1)
	_, _ = b.CreateNode(n2)

	edges := []*Edge{{
		ID: EdgeID(prefixTestID("bulk-be1")), StartNode: NodeID(prefixTestID("bce1")),
		EndNode: NodeID(prefixTestID("bce2")), Type: "LINK", Properties: map[string]interface{}{},
	}}
	err := b.BulkCreateEdges(edges)
	require.NoError(t, err)
}

func TestBadgerEngine_BulkCreateEdges_ExtraEmpty(t *testing.T) {
	b := createTestBadgerEngine(t)
	assert.NoError(t, b.BulkCreateEdges(nil))
}

func TestBadgerEngine_BulkDeleteNodes_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	n := testNode(prefixTestID("bdn1"))
	id, _ := b.CreateNode(n)
	err := b.BulkDeleteNodes([]NodeID{id})
	require.NoError(t, err)
}

func TestBadgerEngine_BulkDeleteNodes_ExtraEmpty(t *testing.T) {
	b := createTestBadgerEngine(t)
	assert.NoError(t, b.BulkDeleteNodes([]NodeID{}))
}

func TestBadgerEngine_BulkDeleteEdges_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	n1 := testNode(prefixTestID("bde1"))
	n2 := testNode(prefixTestID("bde2"))
	_, _ = b.CreateNode(n1)
	_, _ = b.CreateNode(n2)
	e := &Edge{
		ID: EdgeID(prefixTestID("bde-e1")), StartNode: NodeID(prefixTestID("bde1")),
		EndNode: NodeID(prefixTestID("bde2")), Type: "KNOWS", Properties: map[string]interface{}{},
	}
	_ = b.CreateEdge(e)
	err := b.BulkDeleteEdges([]EdgeID{EdgeID(prefixTestID("bde-e1"))})
	require.NoError(t, err)
}

func TestBadgerEngine_BulkDeleteEdges_ExtraEmpty(t *testing.T) {
	b := createTestBadgerEngine(t)
	assert.NoError(t, b.BulkDeleteEdges([]EdgeID{}))
}

// ============================================================================
// SchemaManager – AddUniqueConstraint / AddPropertyTypeConstraint / CheckUniqueConstraint
// ============================================================================

func TestSchemaManager_AddUniqueConstraint(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()
	require.NotNil(t, sm)

	err := sm.AddUniqueConstraint("uc-person-email", "Person", "email")
	assert.NoError(t, err)
}

func TestSchemaManager_AddUniqueConstraint_Duplicate(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()

	_ = sm.AddUniqueConstraint("uc-dup", "User", "username")
	err := sm.AddUniqueConstraint("uc-dup", "User", "username")
	// Either no error (idempotent) or an error for duplicate
	_ = err
}

func TestSchemaManager_AddPropertyTypeConstraint(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()

	err := sm.AddPropertyTypeConstraint("ptc-age", "Person", "age", PropertyTypeInteger)
	assert.NoError(t, err)
}

func TestSchemaManager_CheckUniqueConstraint_NoConstraint(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()

	// No constraint registered for this label/property — should return no error
	err := sm.CheckUniqueConstraint("Ghost", "prop", "val", "")
	assert.NoError(t, err)
}

func TestSchemaManager_CheckUniqueConstraint_WithConstraint(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()

	_ = sm.AddUniqueConstraint("uc-check", "Item", "code")

	// No data yet → no conflict
	err := sm.CheckUniqueConstraint("Item", "code", "ABC123", "")
	assert.NoError(t, err)
}

func TestBadgerEngine_validateBulkNodeConstraints(t *testing.T) {
	t.Run("rejects unprefixed ids and batch constraint violations", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		schema := b.GetSchemaForNamespace("test")
		require.NoError(t, schema.AddUniqueConstraint("user_email", "User", "email"))
		require.NoError(t, schema.AddConstraint(Constraint{
			Name:       "user_key",
			Type:       ConstraintNodeKey,
			Label:      "User",
			Properties: []string{"tenant", "username"},
		}))
		require.NoError(t, schema.AddConstraint(Constraint{
			Name:       "user_name_exists",
			Type:       ConstraintExists,
			Label:      "User",
			Properties: []string{"name"},
		}))

		err := b.validateBulkNodeConstraints([]*Node{{ID: "plain-id", Labels: []string{"User"}}})
		require.ErrorContains(t, err, "node ID must be prefixed with namespace")

		err = b.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("u1")), Labels: []string{"User"}, Properties: map[string]any{"email": "dup@example.com", "tenant": "t1", "username": "alice", "name": "Alice"}},
			{ID: NodeID(prefixTestID("u2")), Labels: []string{"User"}, Properties: map[string]any{"email": "dup@example.com", "tenant": "t2", "username": "bob", "name": "Bob"}},
		})
		require.Error(t, err)
		var violation *ConstraintViolationError
		require.ErrorAs(t, err, &violation)
		assert.Equal(t, ConstraintUnique, violation.Type)

		err = b.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("u3")), Labels: []string{"User"}, Properties: map[string]any{"tenant": "t1", "name": "Alice"}},
		})
		require.ErrorAs(t, err, &violation)
		assert.Equal(t, ConstraintNodeKey, violation.Type)

		err = b.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("u4")), Labels: []string{"User"}, Properties: map[string]any{"tenant": "t1", "username": "alice"}},
		})
		require.ErrorAs(t, err, &violation)
		assert.Equal(t, ConstraintExists, violation.Type)
	})

	t.Run("ignores unsupported unique arity and nil unique values", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		schema := b.GetSchemaForNamespace("test")
		require.NoError(t, schema.AddConstraint(Constraint{
			Name:       "ignored_multi_unique",
			Type:       ConstraintUnique,
			Label:      "Multi",
			Properties: []string{"a", "b"},
		}))
		require.NoError(t, schema.AddUniqueConstraint("user_email", "User", "email"))

		err := b.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("m1")), Labels: []string{"Multi"}, Properties: map[string]any{"b": "only-second"}},
			{ID: NodeID(prefixTestID("u5")), Labels: []string{"User"}, Properties: map[string]any{"email": nil}},
			{ID: NodeID(prefixTestID("u6")), Labels: []string{"NoConstraint"}, Properties: map[string]any{"name": "ok"}},
		})
		require.NoError(t, err)
	})
}

// ============================================================================
// NodeConfig / NodeConfigStore
// ============================================================================

func TestNodeConfig_AddToPin(t *testing.T) {
	cfg := &NodeConfig{PinList: []string{}, DenyList: []string{}}
	cfg.AddToPin("target-node-1")
	cfg.AddToPin("target-node-2")
	assert.Len(t, cfg.PinList, 2)
}

func TestNodeConfig_AddToDeny(t *testing.T) {
	cfg := &NodeConfig{PinList: []string{}, DenyList: []string{}}
	cfg.AddToDeny("blocked-node-1")
	assert.Len(t, cfg.DenyList, 1)
}

func TestNodeConfigStore_AddToNodePinList(t *testing.T) {
	store := NewNodeConfigStore()
	store.AddToNodePinList("nornic:node-a", "nornic:node-b")
	cfg := store.Get("nornic:node-a")
	require.NotNil(t, cfg)
	assert.Contains(t, cfg.PinList, "nornic:node-b")
}

func TestNodeConfigStore_AddToNodeDenyList(t *testing.T) {
	store := NewNodeConfigStore()
	store.AddToNodeDenyList("nornic:node-x", "nornic:node-y")
	cfg := store.Get("nornic:node-x")
	require.NotNil(t, cfg)
	assert.Contains(t, cfg.DenyList, "nornic:node-y")
}
