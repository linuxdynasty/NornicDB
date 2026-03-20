package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateParsesBacktickedPropertyKeys(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (n:MongoRecord {`+"`_mongo_collection`"+`: 'nornic_chat_prompts', `+"`_mongo_id`"+`: 'm1'})
`, nil)
	if err != nil {
		t.Fatalf("CREATE with backticked property keys failed: %v", err)
	}

	result, err := exec.Execute(ctx, `
MATCH (n:MongoRecord {_mongo_id: 'm1'})
RETURN n._mongo_collection
`, nil)
	if err != nil {
		t.Fatalf("MATCH by parsed property key failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if got := result.Rows[0][0]; got != "nornic_chat_prompts" {
		t.Fatalf("expected _mongo_collection=nornic_chat_prompts, got %#v", got)
	}
}

func TestSetWholeMapLiteralParsesBacktickedKeys(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (n:MongoRecord {_mongo_id: 'm2'})`, nil)
	if err != nil {
		t.Fatalf("seed CREATE failed: %v", err)
	}

	_, err = exec.Execute(ctx, `
MATCH (n:MongoRecord {_mongo_id: 'm2'})
SET n = {`+"`_mongo_collection`"+`: 'nornic_language_list', `+"`_mongo_database`"+`: 'nornic-translation', `+"`_mongo_id`"+`: 'm2'}
`, nil)
	if err != nil {
		t.Fatalf("SET whole-map with backticked keys failed: %v", err)
	}

	result, err := exec.Execute(ctx, `
MATCH (n:MongoRecord {_mongo_id: 'm2'})
RETURN n._mongo_collection, n._mongo_database
`, nil)
	if err != nil {
		t.Fatalf("MATCH after SET n = map failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if got := result.Rows[0][0]; got != "nornic_language_list" {
		t.Fatalf("expected _mongo_collection=nornic_language_list, got %#v", got)
	}
	if got := result.Rows[0][1]; got != "nornic-translation" {
		t.Fatalf("expected _mongo_database=nornic-translation, got %#v", got)
	}
}

func TestUnwindCreateSetWholeMapFromParameter(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
CREATE (n:MongoRecord)
SET n = row.properties
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"properties": map[string]interface{}{
					"_mongo_database":   "nornic-translation",
					"_mongo_collection": "nornic_translation",
					"_mongo_id":         "m3",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("UNWIND CREATE SET whole-map failed: %v", err)
	}

	result, err := exec.Execute(ctx, `
MATCH (n:MongoRecord {_mongo_id: 'm3'})
RETURN n._mongo_collection, n._mongo_database, n._mongo_id
`, nil)
	if err != nil {
		t.Fatalf("MATCH for UNWIND-created node failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if got := result.Rows[0][0]; got != "nornic_translation" {
		t.Fatalf("expected _mongo_collection=nornic_translation, got %#v", got)
	}
}

func TestUnwindCreateSetMergeFromParameterMap(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
CREATE (n:MongoRecord)
SET n += row
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"mongo_id":         "merge-1",
				"mongo_collection": "nornic_translation",
				"page":             "https://example.org/path?a=1",
				"active":           true,
			},
			{
				"mongo_id":         "merge-2",
				"mongo_collection": "nornic_translation_text",
				"active":           false,
			},
		},
	})
	if err != nil {
		t.Fatalf("UNWIND CREATE SET += row failed: %v", err)
	}

	result, err := exec.Execute(ctx, `
MATCH (n:MongoRecord)
WHERE n.mongo_id IN ['merge-1', 'merge-2']
RETURN n.mongo_id, n.mongo_collection, n.active, n.page
ORDER BY n.mongo_id
`, nil)
	if err != nil {
		t.Fatalf("verification MATCH failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestUnwindReturnPropertyAccessFromParameterMap(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
UNWIND $rows AS row
RETURN row.path AS path, row.name AS name, row.relative_path AS relative_path, row.is_dependency AS is_dependency
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"path":          "/tmp/a.py",
				"name":          "a.py",
				"relative_path": "a.py",
				"is_dependency": false,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, []string{"path", "name", "relative_path", "is_dependency"}, result.Columns)
	require.Equal(t, "/tmp/a.py", result.Rows[0][0])
	require.Equal(t, "a.py", result.Rows[0][1])
	require.Equal(t, "a.py", result.Rows[0][2])
	require.Equal(t, false, result.Rows[0][3])
}

func TestUnwindTopLevelMergeWithPropertyAccessFromParameterMap(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (f:File {path: row.path})
SET f.name = row.name, f.relative_path = row.relative_path, f.is_dependency = row.is_dependency
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"path":          "/tmp/b.py",
				"name":          "b.py",
				"relative_path": "pkg/b.py",
				"is_dependency": true,
			},
		},
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
MATCH (f:File {path: '/tmp/b.py'})
RETURN f.name, f.relative_path, f.is_dependency
`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "b.py", result.Rows[0][0])
	require.Equal(t, "pkg/b.py", result.Rows[0][1])
	require.Equal(t, true, result.Rows[0][2])
}

func TestUnwindTopLevelMergeSetMergeUsesNestedMapPropertyAccess(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (m:Module {name: row.name})
ON CREATE SET m.lang = row.lang
ON MATCH SET m.lang = COALESCE(m.lang, row.lang)
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"name": "pkg.dep", "lang": "python"},
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (m:Module {name: row.name})
SET m += row.props
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"name": "pkg.dep",
				"props": map[string]interface{}{
					"full_import_name": "pkg.dep.shared",
				},
			},
		},
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
MATCH (m:Module {name: 'pkg.dep'})
RETURN m.lang, m.full_import_name
`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "python", result.Rows[0][0])
	require.Equal(t, "pkg.dep.shared", result.Rows[0][1])
}

func TestUnwindCreateSetMergeFromParameterMap_LargeComplexStrings(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	const total = 1500
	rows := make([]map[string]interface{}, 0, total)
	for i := 0; i < total; i++ {
		rows = append(rows, map[string]interface{}{
			"mongoId":      fmt.Sprintf("complex-%d", i),
			"sourceId":     fmt.Sprintf("complex-%d", i),
			"originalText": fmt.Sprintf("message %d with json-like payload: {\"a\":1,\"b\":[1,2,3],\"c\":\"x,y,z\"}", i),
			"page":         "https://example.org/path?x=1,y=2",
			"meta":         "{\"nested\":{\"k\":\"v,with,commas\"},\"arr\":[{\"x\":1},{\"x\":2}]}",
		})
	}

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
CREATE (n:MongoDocument)
SET n += row
`, map[string]interface{}{"rows": rows})
	if err != nil {
		t.Fatalf("UNWIND CREATE SET += row with complex strings failed: %v", err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:MongoDocument) RETURN count(n)", nil)
	if err != nil {
		t.Fatalf("count after UNWIND failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(result.Rows))
	}
	got, ok := result.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64 count, got %T (%#v)", result.Rows[0][0], result.Rows[0][0])
	}
	if got != total {
		t.Fatalf("expected %d nodes after UNWIND, got %d", total, got)
	}
}

func TestUnwindCreateSetMergeFromParameterMap_ValueContainsAS(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
CREATE (n:MongoDocument)
SET n += row
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"mongoId":      "as-1",
				"sourceId":     "as-1",
				"originalText": "this value contains as with spaces",
			},
			{
				"mongoId":      "as-2",
				"sourceId":     "as-2",
				"originalText": "normal text",
			},
		},
	})
	if err != nil {
		t.Fatalf("UNWIND CREATE SET += row failed when value contained ' as ': %v", err)
	}

	result, err := exec.Execute(ctx, `
MATCH (n:MongoDocument)
WHERE n.mongoId IN ['as-1', 'as-2']
RETURN count(n)
`, nil)
	if err != nil {
		t.Fatalf("count verification failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected one count row, got %d", len(result.Rows))
	}
	got, ok := result.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64 count, got %T (%#v)", result.Rows[0][0], result.Rows[0][0])
	}
	if got != 2 {
		t.Fatalf("expected 2 created nodes, got %d", got)
	}
}

func TestUnwindCreateSetWholeMapFromParameter_LargeBatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	const total = 6000
	rows := make([]map[string]interface{}, 0, total)
	for i := 0; i < total; i++ {
		rows = append(rows, map[string]interface{}{
			"mongo_id":    fmt.Sprintf("bulk-%d", i),
			"source":      "nornic_translation",
			"code":        i,
			"description": fmt.Sprintf("entry-%d", i),
		})
	}

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
CREATE (n:MongoRecord)
SET n = row
`, map[string]interface{}{"rows": rows})
	if err != nil {
		t.Fatalf("UNWIND large-batch CREATE/SET with row failed: %v", err)
	}

	result, err := exec.Execute(ctx, `MATCH (n:MongoRecord) RETURN count(n)`, nil)
	if err != nil {
		t.Fatalf("MATCH count after large UNWIND failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected one count row, got %d", len(result.Rows))
	}
	got, ok := result.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64 count, got %T (%#v)", result.Rows[0][0], result.Rows[0][0])
	}
	if got != total {
		t.Fatalf("expected %d nodes after large UNWIND, got %d", total, got)
	}
}

func TestUnwindCreateSetWholeMapFromParameter_LargeBatch_RowPropertiesWorks(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	const total = 6000
	rows := make([]map[string]interface{}, 0, total)
	for i := 0; i < total; i++ {
		rows = append(rows, map[string]interface{}{
			"properties": map[string]interface{}{
				"mongo_id":    fmt.Sprintf("bulk-%d", i),
				"source":      "nornic_translation",
				"code":        i,
				"description": fmt.Sprintf("entry-%d", i),
			},
		})
	}

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
CREATE (n:MongoRecord)
SET n = row.properties
`, map[string]interface{}{"rows": rows})
	if err != nil {
		t.Fatalf("UNWIND large-batch CREATE/SET with row.properties failed: %v", err)
	}

	result, err := exec.Execute(ctx, `MATCH (n:MongoRecord) RETURN count(n)`, nil)
	if err != nil {
		t.Fatalf("MATCH count after large UNWIND failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected one count row, got %d", len(result.Rows))
	}
	got, ok := result.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64 count, got %T (%#v)", result.Rows[0][0], result.Rows[0][0])
	}
	if got != total {
		t.Fatalf("expected %d nodes after large UNWIND, got %d", total, got)
	}
}

func TestParseValue_MapLiterals(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	val := exec.parseValue("{_mongo_id: 'm4', _mongo_collection: 'nornic_translation'}")
	if _, ok := val.(map[string]interface{}); !ok {
		t.Fatalf("expected plain map literal to parse as map, got %T", val)
	}

	val = exec.parseValue("{`_mongo_id`: 'm5', `_mongo_collection`: 'nornic_translation'}")
	if _, ok := val.(map[string]interface{}); !ok {
		t.Fatalf("expected backticked map literal to parse as map, got %T", val)
	}
}

func TestCreateSetWholeMapLiteral(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (n:MongoRecord)
SET n = {_mongo_id: 'm6', _mongo_collection: 'nornic_translation', _mongo_database: 'nornic-translation'}
`, nil)
	if err != nil {
		t.Fatalf("CREATE...SET whole-map failed: %v", err)
	}

	result, err := exec.Execute(ctx, `
MATCH (n:MongoRecord {_mongo_id: 'm6'})
RETURN n._mongo_collection
`, nil)
	if err != nil {
		t.Fatalf("MATCH after CREATE...SET whole-map failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
}

func TestReplaceVariableInQuery_ForNestedMapPropertyAccess(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	query := "CREATE (n:MongoRecord) SET n = row.properties"
	out := exec.replaceVariableInQuery(query, "row", map[string]interface{}{
		"properties": map[string]interface{}{
			"_mongo_id":         "m7",
			"_mongo_collection": "nornic_translation",
		},
	})

	if strings.Contains(out, "row.properties") {
		t.Fatalf("expected row.properties to be substituted, got: %s", out)
	}
	if !strings.Contains(out, "SET n = {") {
		t.Fatalf("expected map literal substitution in query, got: %s", out)
	}
}

func TestReplaceVariableInQuery_ReplacesScalarAcrossNewlinesAndPunctuation(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	query := "MATCH (o:OriginalText {__tmpJoinKey: k})\nMATCH (t:TranslatedText {__tmpJoinKey: k})\nRETURN count(*) AS c"
	out := exec.replaceVariableInQuery(query, "k", "k1")
	require.Contains(t, out, "__tmpJoinKey: 'k1'")
	require.NotContains(t, out, "__tmpJoinKey: k")
}

func TestReplaceVariableInQuery_DoesNotReplaceMapKeysOrPropertyNames(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	query := "CREATE (n:TestNode {name: name}) RETURN n.name AS nodeName"
	out := exec.replaceVariableInQuery(query, "name", "A")
	require.Contains(t, out, "{name: 'A'}")
	require.Contains(t, out, "RETURN n.name AS nodeName")
	require.NotContains(t, out, "{'A':")
}
