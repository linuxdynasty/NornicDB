package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatchWhereIDEquality_UsesDirectLookupWithoutAllNodesScan(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(&failingNodeLookupEngine{
		Engine:      ns,
		allNodesErr: errors.New("all-nodes scan must not be used for id seek"),
	})
	ctx := context.Background()

	_, err := ns.CreateNode(&storage.Node{
		ID:         "target-id",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "prompt text"},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (n)
		WHERE id(n) = "target-id"
		RETURN n.text AS text
		LIMIT 1
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"text"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "prompt text", res.Rows[0][0])
}

func TestMatchWhereElementIDEquality_UsesDirectLookupWithoutAllNodesScan(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(&failingNodeLookupEngine{
		Engine:      ns,
		allNodesErr: errors.New("all-nodes scan must not be used for elementId seek"),
	})
	ctx := context.Background()

	_, err := ns.CreateNode(&storage.Node{
		ID:         "target-elem-id",
		Labels:     []string{"SystemPrompt"},
		Properties: map[string]interface{}{"text": "prompt text"},
	})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (n)
		WHERE elementId(n) = "4:nornicdb:target-elem-id"
		RETURN n.text AS text
		LIMIT 1
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"text"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "prompt text", res.Rows[0][0])
}
