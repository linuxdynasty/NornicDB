package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplySetMergeToCreated_Branches(t *testing.T) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{ID: "n1", Labels: []string{"N"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	edge := &storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n1", Type: "REL", Properties: map[string]interface{}{}}
	require.NoError(t, store.CreateEdge(edge))

	t.Run("invalid syntax missing +=", func(t *testing.T) {
		err := exec.applySetMergeToCreated(ctx, "n = {x:1}", map[string]*storage.Node{"n": node}, nil, &ExecuteResult{Stats: &QueryStats{}}, store)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid SET += syntax")
	})

	t.Run("parameter name missing after $", func(t *testing.T) {
		err := exec.applySetMergeToCreated(ctx, "n += $", map[string]*storage.Node{"n": node}, nil, &ExecuteResult{Stats: &QueryStats{}}, store)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "valid parameter name")
	})

	t.Run("parameter context missing", func(t *testing.T) {
		err := exec.applySetMergeToCreated(ctx, "n += $props", map[string]*storage.Node{"n": node}, nil, &ExecuteResult{Stats: &QueryStats{}}, store)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires parameters")
	})

	t.Run("parameter not found", func(t *testing.T) {
		ctxParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"other": map[string]interface{}{"x": 1}})
		err := exec.applySetMergeToCreated(ctxParams, "n += $props", map[string]*storage.Node{"n": node}, nil, &ExecuteResult{Stats: &QueryStats{}}, store)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("inline map parse failure", func(t *testing.T) {
		err := exec.applySetMergeToCreated(ctx, "n += ", map[string]*storage.Node{"n": node}, nil, &ExecuteResult{Stats: &QueryStats{}}, store)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse properties")
	})

	t.Run("inline map trailing comma failure", func(t *testing.T) {
		err := exec.applySetMergeToCreated(ctx, "n += {a: 1,}", map[string]*storage.Node{"n": node}, nil, &ExecuteResult{Stats: &QueryStats{}}, store)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse properties")
	})

	t.Run("unknown variable", func(t *testing.T) {
		err := exec.applySetMergeToCreated(ctx, "x += {a: 1}", map[string]*storage.Node{"n": node}, map[string]*storage.Edge{"r": edge}, &ExecuteResult{Stats: &QueryStats{}}, store)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown variable")
	})

	t.Run("node update from inline map", func(t *testing.T) {
		res := &ExecuteResult{Stats: &QueryStats{}}
		err := exec.applySetMergeToCreated(ctx, "n += {age: 30, city: 'NY'}", map[string]*storage.Node{"n": node}, nil, res, store)
		require.NoError(t, err)
		assert.EqualValues(t, 30, node.Properties["age"])
		assert.Equal(t, "NY", node.Properties["city"])
		assert.Equal(t, 2, res.Stats.PropertiesSet)
	})

	t.Run("edge update from params map", func(t *testing.T) {
		ctxParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"props": map[string]interface{}{"weight": int64(7)}})
		res := &ExecuteResult{Stats: &QueryStats{}}
		err := exec.applySetMergeToCreated(ctxParams, "r += $props", nil, map[string]*storage.Edge{"r": edge}, res, store)
		require.NoError(t, err)
		assert.EqualValues(t, 7, edge.Properties["weight"])
		assert.Equal(t, 1, res.Stats.PropertiesSet)
	})
}

func TestParseSetMergeMapLiteralStrict_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())

	t.Run("missing braces", func(t *testing.T) {
		_, err := exec.parseSetMergeMapLiteralStrict("a:1")
		require.Error(t, err)
	})

	t.Run("empty map", func(t *testing.T) {
		props, err := exec.parseSetMergeMapLiteralStrict("{}")
		require.NoError(t, err)
		require.Empty(t, props)
	})

	t.Run("trailing comma entry", func(t *testing.T) {
		_, err := exec.parseSetMergeMapLiteralStrict("{a: 1,}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty map entry")
	})

	t.Run("missing colon", func(t *testing.T) {
		_, err := exec.parseSetMergeMapLiteralStrict("{a}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid map entry")
	})

	t.Run("empty key", func(t *testing.T) {
		_, err := exec.parseSetMergeMapLiteralStrict("{: 1}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid map entry")
	})

	t.Run("empty value", func(t *testing.T) {
		_, err := exec.parseSetMergeMapLiteralStrict("{a: }")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid map entry")
	})

	t.Run("quoted key and nested value", func(t *testing.T) {
		props, err := exec.parseSetMergeMapLiteralStrict("{'a': 1, b: {x: 2}, c: [1,2]}")
		require.NoError(t, err)
		assert.EqualValues(t, int64(1), props["a"])
		assert.NotNil(t, props["b"])
		assert.NotNil(t, props["c"])
	})
}
