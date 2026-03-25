package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type collectNodesLabelProbeEngine struct {
	storage.Engine
	streamCalls      int
	labelCalls       int
	labelLookupCalls int
	labelIDs         []storage.NodeID
}

func (e *collectNodesLabelProbeEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	e.labelCalls++
	return nil, assert.AnError
}

func (e *collectNodesLabelProbeEngine) StreamNodes(_ context.Context, _ func(node *storage.Node) error) error {
	e.streamCalls++
	return assert.AnError
}

func (e *collectNodesLabelProbeEngine) StreamEdges(_ context.Context, _ func(edge *storage.Edge) error) error {
	return nil
}

func (e *collectNodesLabelProbeEngine) StreamNodeChunks(_ context.Context, _ int, _ func(nodes []*storage.Node) error) error {
	return nil
}

func (e *collectNodesLabelProbeEngine) ForEachNodeIDByLabel(label string, visit func(storage.NodeID) bool) error {
	e.labelLookupCalls++
	for _, id := range e.labelIDs {
		if !visit(id) {
			return nil
		}
	}
	return nil
}

func TestCollectNodesWithStreaming_LabelLimitPrefersLabelLookup(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	for i := 0; i < 100; i++ {
		label := "Other"
		if i%20 == 0 {
			label = "SystemPrompt"
		}
		id := storage.NodeID(fmt.Sprintf("nornic:n-%d", i))
		_, err := base.CreateNode(&storage.Node{
			ID:     id,
			Labels: []string{label},
			Properties: map[string]any{
				"idx": i,
			},
		})
		require.NoError(t, err)
	}

	probe := &collectNodesLabelProbeEngine{
		Engine: base,
		labelIDs: []storage.NodeID{
			"nornic:n-0", "nornic:n-20", "nornic:n-40", "nornic:n-60", "nornic:n-80",
		},
	}
	exec := NewStorageExecutor(probe)

	nodes, err := exec.collectNodesWithStreaming(context.Background(), []string{"SystemPrompt"}, nil, "n", "", 3)
	require.NoError(t, err)
	require.Len(t, nodes, 3)
	assert.GreaterOrEqual(t, probe.labelLookupCalls, 1, "label-id lookup path must be used")
	assert.Equal(t, 0, probe.streamCalls, "full streaming scan must be skipped")
}
