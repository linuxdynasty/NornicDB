package cypher

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApocLoadExportHelpers_ExtractLoadArg(t *testing.T) {
	e := &StorageExecutor{}
	assert.Equal(t, "data.json", e.extractApocLoadArg("CALL apoc.load.json('data.json') YIELD value", "JSON"))
	assert.Equal(t, "https://example.com/a.csv", e.extractApocLoadArg("CALL apoc.load.csv(https://example.com/a.csv) YIELD map", "CSV"))
	assert.Equal(t, "", e.extractApocLoadArg("CALL somethingElse()", "JSON"))
}

func TestApocLoadExportHelpers_ExtractExportArgAndQuery(t *testing.T) {
	e := &StorageExecutor{}
	assert.Equal(t, "out.json", e.extractApocExportArg("CALL apoc.export.json.all('out.json', {})", "JSON"))
	assert.Equal(t, "out.csv", e.extractApocExportArg("CALL apoc.export.csv.query('MATCH (n) RETURN n', 'out.csv', {})", "CSV"))
	assert.Equal(t, "MATCH (n) RETURN n", e.extractApocExportQuery("CALL apoc.export.csv.query('MATCH (n) RETURN n', 'out.csv', {})"))
	assert.Equal(t, "", e.extractApocExportQuery("CALL apoc.export.csv.all('x.csv',{})"))
}

func TestApocLoadExportHelpers_ExportFormatting(t *testing.T) {
	e := &StorageExecutor{}
	nodes := []*storage.Node{{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}}}
	edges := []*storage.Edge{{ID: "e1", Type: "KNOWS", StartNode: "n1", EndNode: "n2", Properties: map[string]interface{}{"since": int64(2020)}}}

	nf := e.nodesToExportFormat(nodes)
	ef := e.edgesToExportFormat(edges)
	assert.Len(t, nf, 1)
	assert.Len(t, ef, 1)
	assert.Equal(t, "n1", nf[0]["id"])
	assert.Equal(t, "KNOWS", ef[0]["type"])
	assert.EqualValues(t, int64(2020), ef[0]["properties"].(map[string]interface{})["since"])
}

func TestApocLoadExportHelpers_CountProperties(t *testing.T) {
	e := &StorageExecutor{}
	nodes := []*storage.Node{{Properties: map[string]interface{}{"a": 1, "b": 2}}, {Properties: map[string]interface{}{}}}
	edges := []*storage.Edge{{Properties: map[string]interface{}{"c": 3}}}
	assert.Equal(t, 3, e.countProperties(nodes, edges))
	assert.Equal(t, 0, e.countProperties(nil, nil))
}

func TestApocLoadExportHelpers_CallApocLoadCsvParams_Delegates(t *testing.T) {
	e := &StorageExecutor{}
	// invalid invocation should return same validation error path as callApocLoadCsv
	_, err := e.callApocLoadCsvParams(nil, "CALL apoc.load.csvParams()")
	assert.Error(t, err)
}

func TestApocLoadExportHelpers_CallApocLoadJsonArray_FromFile(t *testing.T) {
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	e := NewStorageExecutor(eng)

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "arr.json")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`[1,{"a":2},3]`), 0o644))

	q := "CALL apoc.load.jsonArray('" + jsonPath + "') YIELD value RETURN value"
	res, err := e.callApocLoadJsonArray(context.Background(), q)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, []string{"value"}, res.Columns)
	assert.Len(t, res.Rows, 3)
}

func TestApocLoadExportHelpers_CallApocImportJson_FromFile(t *testing.T) {
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	e := NewStorageExecutor(eng)

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "graph.json")
	graphJSON := `{
		"nodes": [
			{"id":"n1","labels":["Person"],"properties":{"name":"alice"}},
			{"id":"n2","labels":["Person"],"properties":{"name":"bob"}}
		],
		"relationships": [
			{"id":"e1","type":"KNOWS","startNode":"n1","endNode":"n2","properties":{"since":2020}}
		]
	}`
	require.NoError(t, os.WriteFile(jsonPath, []byte(graphJSON), 0o644))

	q := "CALL apoc.import.json('" + jsonPath + "') YIELD source, nodes, relationships"
	res, err := e.callApocImportJson(context.Background(), q)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, []string{"source", "nodes", "relationships"}, res.Columns)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, jsonPath, res.Rows[0][0])
	assert.EqualValues(t, 2, res.Rows[0][1])
	assert.EqualValues(t, 1, res.Rows[0][2])
}
