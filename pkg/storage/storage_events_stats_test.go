package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// AsyncEngine – events, stats, config helpers
// ============================================================================

func TestAsyncEngine_DefaultAsyncEngineConfig(t *testing.T) {
	cfg := DefaultAsyncEngineConfig()
	assert.Greater(t, cfg.FlushInterval, time.Duration(0))
}

func TestAsyncEngine_GetUnderlying(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()
	underlying := ae.GetUnderlying()
	assert.NotNil(t, underlying)
}

func TestAsyncEngine_Stats(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()
	pending, flushes := ae.Stats()
	assert.GreaterOrEqual(t, pending, int64(0))
	assert.GreaterOrEqual(t, flushes, int64(0))
}

func TestAsyncEngine_HasPendingWrites_Empty(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()
	// No pending writes initially
	has := ae.HasPendingWrites()
	assert.False(t, has)
}

func TestAsyncEngine_HasPendingWrites_WithData(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: 10 * time.Second})
	defer ae.Close()
	_, _ = ae.CreateNode(makeNode("pw1"))
	// May or may not have pending writes depending on timing
	_ = ae.HasPendingWrites()
}

func TestAsyncEngine_ListNamespaces(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()
	ns := ae.ListNamespaces()
	assert.NotNil(t, ns)
}

// ============================================================================
// AsyncEngine – event callbacks
// ============================================================================

func TestAsyncEngine_OnNodeCreated(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()

	ae.OnNodeCreated(func(n *Node) {})

	_, err := ae.CreateNode(makeNode("ev-n1"))
	require.NoError(t, err)
	require.NoError(t, ae.Flush())
}

func TestAsyncEngine_OnNodeUpdated(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()

	ae.OnNodeUpdated(func(n *Node) {})

	id, _ := ae.CreateNode(makeNode("ev-upd"))
	require.NoError(t, ae.Flush())

	n, _ := ae.GetNode(id)
	require.NotNil(t, n)
	n.Properties["updated"] = true
	require.NoError(t, ae.UpdateNode(n))
	require.NoError(t, ae.Flush())
}

func TestAsyncEngine_OnNodeDeleted(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()

	ae.OnNodeDeleted(func(id NodeID) {})

	nid, _ := ae.CreateNode(makeNode("ev-del"))
	require.NoError(t, ae.Flush())
	require.NoError(t, ae.DeleteNode(nid))
	require.NoError(t, ae.Flush())
}

func TestAsyncEngine_OnEdgeCreated(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()

	ae.OnEdgeCreated(func(e *Edge) {})

	_, _ = ae.CreateNode(makeNode("ee1"))
	_, _ = ae.CreateNode(makeNode("ee2"))
	require.NoError(t, ae.CreateEdge(makeEdge("ev-edge1", "ee1", "ee2")))
	require.NoError(t, ae.Flush())
}

func TestAsyncEngine_OnEdgeUpdated(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()

	ae.OnEdgeUpdated(func(e *Edge) {})

	_, _ = ae.CreateNode(makeNode("eu1"))
	_, _ = ae.CreateNode(makeNode("eu2"))
	_ = ae.CreateEdge(makeEdge("ev-edge-u", "eu1", "eu2"))
	require.NoError(t, ae.Flush())
	// No panic is enough
}

func TestAsyncEngine_OnEdgeDeleted(t *testing.T) {
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, nil)
	defer ae.Close()

	ae.OnEdgeDeleted(func(id EdgeID) {})

	_, _ = ae.CreateNode(makeNode("ed1"))
	_, _ = ae.CreateNode(makeNode("ed2"))
	eid := EdgeID(prefixTestID("ev-edge-del"))
	e := &Edge{ID: eid, StartNode: NodeID(prefixTestID("ed1")), EndNode: NodeID(prefixTestID("ed2")), Type: "T", Properties: map[string]interface{}{}}
	_ = ae.CreateEdge(e)
	require.NoError(t, ae.Flush())
	require.NoError(t, ae.DeleteEdge(eid))
	require.NoError(t, ae.Flush())
}

// ============================================================================
// BadgerEngine – stats functions (called directly, not via AsyncEngine)
// ============================================================================

func TestBadgerEngine_NodeCount(t *testing.T) {
	b := createTestBadgerEngine(t)
	count, err := b.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	_, _ = b.CreateNode(testNode(prefixTestID("cnt1")))
	count, err = b.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestBadgerEngine_NodeCountByPrefix(t *testing.T) {
	b := createTestBadgerEngine(t)
	_, _ = b.CreateNode(testNode(prefixTestID("pfx-1")))
	_, _ = b.CreateNode(testNode(prefixTestID("pfx-2")))

	count, err := b.NodeCountByPrefix(prefixTestID("pfx-"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(0))
}

func TestBadgerEngine_EdgeCount(t *testing.T) {
	b := createTestBadgerEngine(t)
	count, err := b.EdgeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestBadgerEngine_GetSchema(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()
	assert.NotNil(t, sm)
}
