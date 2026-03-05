package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupApocPathExecutor(t *testing.T) (*StorageExecutor, *storage.NamespacedEngine) {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { base.Close() })
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	return exec, eng
}

func TestApocPathExtra_ParseApocPathExpandParams(t *testing.T) {
	exec, eng := setupApocPathExecutor(t)

	_, err := eng.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"id": "n1"}})
	require.NoError(t, err)

	cypher := "MATCH (n:Person {id: 'n1'}) CALL apoc.path.expand(n, '>KNOWS', '+Person|-Blocked', 1, 3) YIELD path RETURN path"
	params := exec.parseApocPathExpandParams(cypher)

	require.NotNil(t, params.startNode)
	assert.Equal(t, storage.NodeID("n1"), params.startNode.ID)
	assert.Equal(t, []string{"KNOWS"}, params.relationshipTypes)
	assert.Equal(t, "outgoing", params.direction)
	assert.Equal(t, []string{"Person"}, params.includeLabels)
	assert.Equal(t, []string{"Blocked"}, params.excludeLabels)
	assert.Equal(t, 1, params.minLevel)
	assert.Equal(t, 3, params.maxLevel)
}

func TestApocPathExtra_ParseApocPathExpandParams_Defaults(t *testing.T) {
	exec, _ := setupApocPathExecutor(t)
	params := exec.parseApocPathExpandParams("RETURN 1")
	assert.Nil(t, params.startNode)
	assert.Equal(t, 1, params.minLevel)
	assert.Equal(t, 1, params.maxLevel)
	assert.Equal(t, "both", params.direction)
}

func TestApocPathExtra_FindNodeByVariableInMatch(t *testing.T) {
	exec, eng := setupApocPathExecutor(t)
	_, err := eng.CreateNode(&storage.Node{ID: "v1", Labels: []string{"Person"}, Properties: map[string]interface{}{"id": "v1", "name": "alice"}})
	require.NoError(t, err)

	n := exec.findNodeByVariableInMatch("MATCH (p:Person {id: 'v1'}) RETURN p", "p")
	require.NotNil(t, n)
	assert.Equal(t, storage.NodeID("v1"), n.ID)

	assert.Nil(t, exec.findNodeByVariableInMatch("MATCH (x:Other {id:'x1'}) RETURN x", "p"))
}

func TestApocPathExtra_BFSPathTraversal(t *testing.T) {
	exec, eng := setupApocPathExecutor(t)

	_, err := eng.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "c", Labels: []string{"Person"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS", Properties: map[string]interface{}{}}))
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "KNOWS", Properties: map[string]interface{}{}}))

	start, err := eng.GetNode("a")
	require.NoError(t, err)
	require.NotNil(t, start)

	cfg := apocPathConfig{minLevel: 1, maxLevel: 2, direction: "outgoing", relationshipTypes: []string{"KNOWS"}}
	paths := exec.bfsPathTraversal(start, cfg)
	assert.NotEmpty(t, paths)

	// includeLabels filter should drop non-matching end labels
	cfg2 := apocPathConfig{minLevel: 1, maxLevel: 2, direction: "outgoing", includeLabels: []string{"DoesNotExist"}}
	paths2 := exec.bfsPathTraversal(start, cfg2)
	assert.Empty(t, paths2)
}
