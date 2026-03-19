package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type relationshipScanGuardEngine struct {
	*storage.MemoryEngine
	forbidLabelScan bool
	outgoingCalls   int
}

func (e *relationshipScanGuardEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e.forbidLabelScan {
		return nil, fmt.Errorf("GetNodesByLabel should not be called for indexed relationship start-node pruning")
	}
	return e.MemoryEngine.GetNodesByLabel(label)
}

func (e *relationshipScanGuardEngine) AllNodes() ([]*storage.Node, error) {
	if e.forbidLabelScan {
		return nil, fmt.Errorf("AllNodes should not be called for indexed relationship start-node pruning")
	}
	return e.MemoryEngine.AllNodes()
}

func (e *relationshipScanGuardEngine) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	e.outgoingCalls++
	return e.MemoryEngine.GetOutgoingEdges(nodeID)
}

func seedRelationshipLimitData(t *testing.T, eng *relationshipScanGuardEngine, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		origID := storage.NodeID(fmt.Sprintf("nornic:orig-%03d", i))
		trID := storage.NodeID(fmt.Sprintf("nornic:tr-%03d", i))
		_, err := eng.CreateNode(&storage.Node{
			ID:     origID,
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"originalText": fmt.Sprintf("src-%03d", i),
			},
		})
		require.NoError(t, err)
		_, err = eng.CreateNode(&storage.Node{
			ID:     trID,
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"language":       "es",
				"translatedText": fmt.Sprintf("dst-%03d", i),
			},
		})
		require.NoError(t, err)
		require.NoError(t, eng.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("nornic:edge-%03d", i)),
			Type:      "TRANSLATES_TO",
			StartNode: origID,
			EndNode:   trID,
		}))
	}
}

func TestRelationshipMatchLimitShortCircuitsTraversal(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &relationshipScanGuardEngine{MemoryEngine: base}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	seedRelationshipLimitData(t, eng, 50)

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
RETURN o.originalText AS originalText
LIMIT 1
`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.LessOrEqual(t, eng.outgoingCalls, 2, "LIMIT 1 should stop traversal after the first start-node match")
}

func TestRelationshipMatchUsesIndexForIsNotNullStartNodeFilter(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &relationshipScanGuardEngine{MemoryEngine: base}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	seedRelationshipLimitData(t, eng, 8)
	_, err := exec.Execute(ctx, "CREATE INDEX idx_original_text FOR (o:OriginalText) ON (o.originalText)", nil)
	require.NoError(t, err)
	eqWhere, err := exec.Execute(ctx, "MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) WHERE o.originalText = 'src-000' RETURN o.originalText AS originalText LIMIT 1", nil)
	require.NoError(t, err)
	require.Len(t, eqWhere.Rows, 1)

	warmup, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE o.originalText IS NOT NULL
RETURN
  o.originalText AS originalText,
  t.language AS language,
  t.translatedText AS translatedText
ORDER BY t.language
LIMIT 1
`, nil)
	require.NoError(t, err)
	require.Len(t, warmup.Rows, 1)

	eng.forbidLabelScan = true

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE o.originalText IS NOT NULL
RETURN
  o.originalText AS originalText,
  t.language AS language,
  t.translatedText AS translatedText
ORDER BY t.language
LIMIT 1
`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 3)
}
