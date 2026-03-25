package multidb

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sizeTrackingStreamingInner struct {
	storage.Engine
	nodes             []*storage.Node
	streamNodesCalls  int
	streamPrefixCalls int
	lastPrefix        string
}

func (e *sizeTrackingStreamingInner) StreamNodes(_ context.Context, fn func(node *storage.Node) error) error {
	e.streamNodesCalls++
	for _, node := range e.nodes {
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *sizeTrackingStreamingInner) StreamEdges(_ context.Context, _ func(edge *storage.Edge) error) error {
	return nil
}

func (e *sizeTrackingStreamingInner) StreamNodeChunks(_ context.Context, _ int, fn func(nodes []*storage.Node) error) error {
	return fn(e.nodes)
}

func (e *sizeTrackingStreamingInner) StreamNodesByPrefix(_ context.Context, prefix string, fn func(node *storage.Node) error) error {
	e.streamPrefixCalls++
	e.lastPrefix = prefix
	for _, node := range e.nodes {
		if !strings.HasPrefix(string(node.ID), prefix) {
			continue
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func TestSizeTrackingEngine_StreamNodesByPrefix_Delegates(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	inner := &sizeTrackingStreamingInner{
		Engine: base,
		nodes: []*storage.Node{
			{ID: "tenant_a:n1"},
			{ID: "tenant_b:n2"},
			{ID: "tenant_a:n3"},
		},
	}

	wrappedEngine := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant_a")
	prefixStreamer, ok := wrappedEngine.(storage.PrefixStreamingEngine)
	require.True(t, ok, "size tracking wrapper must preserve PrefixStreamingEngine")

	var got []storage.NodeID
	err := prefixStreamer.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *storage.Node) error {
		got = append(got, node.ID)
		if len(got) == 1 {
			return storage.ErrIterationStopped
		}
		return nil
	})
	// Delegated prefix stream returns ErrIterationStopped as-is; caller handles it.
	require.ErrorIs(t, err, storage.ErrIterationStopped)
	assert.Equal(t, 1, inner.streamPrefixCalls)
	assert.Equal(t, 0, inner.streamNodesCalls)
	assert.Equal(t, "tenant_a:", inner.lastPrefix)
	assert.Equal(t, []storage.NodeID{"tenant_a:n1"}, got)
}

func TestSizeTrackingEngine_ForEachNodeIDByLabel_Delegates(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	for i := 0; i < 5; i++ {
		_, err := base.CreateNode(&storage.Node{
			ID:     storage.NodeID("tenant_a:n-" + string(rune('0'+i))),
			Labels: []string{"Person"},
		})
		require.NoError(t, err)
	}

	wrapped := newSizeTrackingEngine(base, &DatabaseManager{}, "tenant_a")
	lookup, ok := wrapped.(storage.LabelNodeIDLookupEngine)
	require.True(t, ok, "size tracking wrapper must preserve LabelNodeIDLookupEngine")

	var count int
	err := lookup.ForEachNodeIDByLabel("Person", func(id storage.NodeID) bool {
		count++
		return count < 2
	})
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}
