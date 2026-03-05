package cypher

import (
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
