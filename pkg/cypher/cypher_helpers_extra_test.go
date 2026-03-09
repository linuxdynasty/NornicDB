package cypher

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type typedResultFixture struct {
	Name      string    `cypher:"name"`
	Age       int       `json:"age"`
	CreatedAt time.Time `json:"created_at"`
	Score     float64
}

func TestCypherHelpers_DecodeMapAndAssignValue(t *testing.T) {
	m := map[string]interface{}{
		"name":       "alice",
		"age":        float64(32),
		"created_at": "2024-01-02T03:04:05Z",
		"score":      int64(7),
	}
	var out typedResultFixture
	err := decodeMap(m, reflect.ValueOf(&out).Elem())
	require.NoError(t, err)
	assert.Equal(t, "alice", out.Name)
	assert.Equal(t, 32, out.Age)
	assert.WithinDuration(t, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), out.CreatedAt, time.Second)
	assert.Equal(t, 7.0, out.Score)

	// assignValue error branch (unsupported conversion)
	var i int
	err = assignValue(reflect.ValueOf(&i).Elem(), "not-a-number")
	assert.Error(t, err)
}

func TestCypherHelpers_ExtractorsAndEnsureLabel(t *testing.T) {
	assert.Equal(t, "myGraph", extractGraphNameFromReturn("RETURN gds.graph.project('myGraph', ['A'], ['R'])"))
	assert.Equal(t, "", extractGraphNameFromReturn("RETURN 1"))
	assert.Equal(t, 0.75, extractFloatArg("{dampingFactor: 0.75, iterations: 20}", "dampingFactor"))
	assert.Equal(t, 0.0, extractFloatArg("{iterations: 20}", "dampingFactor"))

	labels := ensureLabel([]string{"A"}, "B")
	assert.ElementsMatch(t, []string{"A", "B"}, labels)
	labels2 := ensureLabel([]string{"A", "B"}, "B")
	assert.ElementsMatch(t, []string{"A", "B"}, labels2)
}

func TestCypherHelpers_ProcedureCatalogNodeID(t *testing.T) {
	id := procedureCatalogNodeID("  Db.Labels  ")
	assert.Equal(t, storage.NodeID(procedureCatalogPrefix+"db.labels"), id)
}

func TestCypherHelpers_NodeMapAndEmbeddingSummary(t *testing.T) {
	exec := &StorageExecutor{}

	nodePending := &storage.Node{
		ID:         "n-pending",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"name": "pending"},
	}
	pending := exec.nodeToMap(nodePending)
	require.Equal(t, "n-pending", pending["_nodeId"])
	require.Equal(t, "n-pending", pending["id"]) // fallback to storage ID when user id absent
	pEmb, ok := pending["embedding"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "pending", pEmb["status"])

	nodeReady := &storage.Node{
		ID:              "n-ready",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 2, 3, 4}},
		EmbedMeta: map[string]interface{}{
			"embedding_model": "bge-m3",
		},
		Properties: map[string]interface{}{"id": "user-id", "name": "ready"},
	}
	ready := exec.nodeToMap(nodeReady)
	require.Equal(t, "user-id", ready["id"]) // preserve user-provided id
	rEmb, ok := ready["embedding"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "ready", rEmb["status"])
	assert.Equal(t, 4, rEmb["dimensions"])
	assert.Equal(t, "bge-m3", rEmb["model"])
}

func TestCypherHelpers_ProcedureRegistryAndPatternNames(t *testing.T) {
	reg := NewProcedureRegistry()
	err := reg.RegisterBuiltIn(ProcedureSpec{Name: "db.labels", MinArgs: 0, MaxArgs: 0}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	})
	require.NoError(t, err)
	err = reg.RegisterUser(ProcedureSpec{Name: "custom.proc", MinArgs: 0, MaxArgs: 1}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	})
	require.NoError(t, err)

	builtins := reg.ListBuiltIns()
	require.Len(t, builtins, 1)
	assert.Equal(t, "db.labels", builtins[0].Name)

	assert.Equal(t, "Generic", PatternGeneric.String())
	assert.Equal(t, "MutualRelationship", PatternMutualRelationship.String())
	assert.Equal(t, "IncomingCountAgg", PatternIncomingCountAgg.String())
	assert.Equal(t, "OutgoingCountAgg", PatternOutgoingCountAgg.String())
	assert.Equal(t, "EdgePropertyAgg", PatternEdgePropertyAgg.String())
	assert.Equal(t, "LargeResultSet", PatternLargeResultSet.String())
}

func TestCypherHelpers_LooksNumericAndSkipString(t *testing.T) {
	assert.True(t, looksNumeric("42"))
	assert.True(t, looksNumeric("3.1415"))
	assert.False(t, looksNumeric("x42"))
	assert.Equal(t, "17", ExtractSkipString("MATCH (n) RETURN n SKIP 17 LIMIT 5"))
	assert.Equal(t, "", ExtractSkipString("MATCH (n) RETURN n"))
}
