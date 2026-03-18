package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type allNodesForbiddenEngine struct {
	*storage.MemoryEngine
	forbidScan bool
}

func (e *allNodesForbiddenEngine) AllNodes() ([]*storage.Node, error) {
	if !e.forbidScan {
		return e.MemoryEngine.AllNodes()
	}
	return nil, fmt.Errorf("AllNodes should not be called for indexed equality lookup")
}

func (e *allNodesForbiddenEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if !e.forbidScan {
		return e.MemoryEngine.GetNodesByLabel(label)
	}
	return nil, fmt.Errorf("GetNodesByLabel should not be called for indexed fast path")
}

func TestMatchUsesPropertyIndexForUnlabeledEquality(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	_, err := eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-1",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"textKey128": "k-1",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-2",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"textKey128": "k-2",
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(eng)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_text_key_128 FOR (n:MongoDocument) ON (n.textKey128)", nil)
	require.NoError(t, err)
	eng.forbidScan = true
	res, err := exec.Execute(context.Background(), "MATCH (n) WHERE n.textKey128 = 'k-2' RETURN n.textKey128 AS key", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"key"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "k-2", res.Rows[0][0])
}

func TestMatchUsesPropertyIndexForFabricRecordBindingEquality(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	_, err := eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-a",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"textKey128": "h-a",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "nornic:doc-b",
		Labels: []string{"MongoDocument"},
		Properties: map[string]interface{}{
			"textKey128": "h-b",
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(eng)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_text_key_128 FOR (n:MongoDocument) ON (n.textKey128)", nil)
	require.NoError(t, err)
	eng.forbidScan = true
	exec.fabricRecordBindings = map[string]interface{}{
		"textKey128": "h-a",
	}

	res, err := exec.Execute(context.Background(), "MATCH (n) WHERE n.textKey128 = textKey128 RETURN n.textKey128 AS key", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"key"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "h-a", res.Rows[0][0])
}

func TestMatchUsesPropertyIndexForIsNotNullOrderByLimit(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	for i := 0; i < 10; i++ {
		_, err := eng.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("nornic:doc-%d", i)),
			Labels: []string{"MongoDocument"},
			Properties: map[string]interface{}{
				"sourceId": fmt.Sprintf("src-%03d", i),
			},
		})
		require.NoError(t, err)
	}

	exec := NewStorageExecutor(eng)
	_, err := exec.Execute(context.Background(), "CREATE INDEX idx_source_id FOR (n:MongoDocument) ON (n.sourceId)", nil)
	require.NoError(t, err)
	eng.forbidScan = true

	res, err := exec.Execute(context.Background(), "MATCH (n:MongoDocument) WHERE n.sourceId IS NOT NULL RETURN n.sourceId AS sourceId ORDER BY n.sourceId LIMIT 3", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"sourceId"}, res.Columns)
	require.Len(t, res.Rows, 3)
	require.Equal(t, "src-000", res.Rows[0][0])
	require.Equal(t, "src-001", res.Rows[1][0])
	require.Equal(t, "src-002", res.Rows[2][0])
}
