package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestE2E_UnwindMergeChainBatch_QVersionsFamily_Deterministic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE CONSTRAINT commit_hash_unique IF NOT EXISTS FOR (c:Commit) REQUIRE c.hash IS UNIQUE`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (ck:CodeKey {entity_id: row.entity_id, relation_type: row.relation_type})
MERGE (cs:CodeState {state_id: row.state_id})
SET cs.code_key = row.code_key,
    cs.tx_id = row.tx_id,
    cs.commit_hash = row.commit_hash,
    cs.valid_from_iso = row.valid_from_iso,
    cs.value_json = row.value_json,
    cs.asserted_by = row.asserted_by,
    cs.semantic_type = row.semantic_type
MERGE (ck)-[:HAS_STATE]->(cs)
MERGE (c:Commit {hash: row.commit_hash})
ON CREATE SET c.tx_id = row.tx_id, c.actor = row.asserted_by
MERGE (c)-[:CHANGED]->(cs)
MERGE (c)-[:TOUCHED]->(ck)
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id":      "entity-1",
				"relation_type":  "calls",
				"state_id":       "state-1",
				"code_key":       "repo_fact|calls|a",
				"tx_id":          "tx-1",
				"commit_hash":    "commit-shared",
				"valid_from_iso": "2026-03-20T20:22:20Z",
				"value_json":     `{"repo":"git-to-graph","source":"a","target":"b"}`,
				"asserted_by":    "TJ Sweet",
				"semantic_type":  "CallEdgeVersion",
			},
			{
				"entity_id":      "entity-2",
				"relation_type":  "contains",
				"state_id":       "state-2",
				"code_key":       "repo_fact|contains|x",
				"tx_id":          "tx-1",
				"commit_hash":    "commit-shared",
				"valid_from_iso": "2026-03-20T20:22:20Z",
				"value_json":     `{"repo":"git-to-graph","parent":"x","child":"y"}`,
				"asserted_by":    "TJ Sweet",
				"semantic_type":  "ContainsEdgeVersion",
			},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch)

	statesRes, err := exec.Execute(ctx, `
MATCH (ck:CodeKey)-[:HAS_STATE]->(cs:CodeState)
RETURN ck.entity_id, ck.relation_type, cs.state_id, cs.code_key, cs.tx_id, cs.commit_hash, cs.asserted_by, cs.semantic_type
ORDER BY cs.state_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"entity-1", "calls", "state-1", "repo_fact|calls|a", "tx-1", "commit-shared", "TJ Sweet", "CallEdgeVersion"},
		{"entity-2", "contains", "state-2", "repo_fact|contains|x", "tx-1", "commit-shared", "TJ Sweet", "ContainsEdgeVersion"},
	}, statesRes.Rows)

	commitRes, err := exec.Execute(ctx, `
MATCH (c:Commit)
RETURN c.hash, c.tx_id, c.actor
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"commit-shared", "tx-1", "TJ Sweet"}}, commitRes.Rows)

	changedRes, err := exec.Execute(ctx, `
MATCH (c:Commit)-[:CHANGED]->(cs:CodeState)
RETURN c.hash, cs.state_id
ORDER BY cs.state_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"commit-shared", "state-1"}, {"commit-shared", "state-2"}}, changedRes.Rows)

	touchedRes, err := exec.Execute(ctx, `
MATCH (c:Commit)-[:TOUCHED]->(ck:CodeKey)
RETURN c.hash, ck.entity_id, ck.relation_type
ORDER BY ck.entity_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"commit-shared", "entity-1", "calls"}, {"commit-shared", "entity-2", "contains"}}, touchedRes.Rows)
}

func TestE2E_QVersionsFallbackRowShape_Deterministic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE CONSTRAINT commit_hash_unique IF NOT EXISTS FOR (c:Commit) REQUIRE c.hash IS UNIQUE`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(`
MERGE (ck:CodeKey {entity_id: $entity_id, relation_type: $relation_type})
MERGE (cs:CodeState {state_id: $state_id})
SET cs.code_key = $code_key,
    cs.tx_id = $tx_id,
    cs.commit_hash = $commit_hash,
    cs.valid_from_iso = $valid_from_iso,
    cs.valid_from = datetime($valid_from_iso),
    cs.value_json = $value_json,
    cs.valid_to = CASE WHEN $valid_to_iso IS NULL THEN null ELSE datetime($valid_to_iso) END,
    cs.asserted_at = datetime($asserted_at_iso),
    cs.asserted_by = $asserted_by,
    cs.semantic_type = $semantic_type
MERGE (c:Commit {hash: $commit_hash})
ON CREATE SET c.timestamp = datetime($asserted_at_iso), c.tx_id = $tx_id, c.actor = $asserted_by
WITH $entity_id AS entity_id, $relation_type AS relation_type, $state_id AS state_id, $commit_hash AS commit_hash
MATCH (ck:CodeKey {entity_id: entity_id, relation_type: relation_type})
MATCH (cs:CodeState {state_id: state_id})
MATCH (c:Commit {hash: commit_hash})
MERGE (ck)-[:HAS_STATE]->(cs)
MERGE (c)-[:CHANGED]->(cs)
MERGE (c)-[:TOUCHED]->(ck)
`), map[string]interface{}{
		"entity_id":       "entity-single",
		"relation_type":   "calls",
		"state_id":        "state-single",
		"code_key":        "repo_fact|calls|single",
		"tx_id":           "tx-single",
		"commit_hash":     "commit-single-row",
		"valid_from_iso":  "2026-03-20T20:22:20Z",
		"valid_to_iso":    nil,
		"asserted_at_iso": "2026-03-20T20:22:20Z",
		"value_json":      `{"repo":"git-to-graph","source":"single-a","target":"single-b"}`,
		"asserted_by":     "TJ Sweet",
		"semantic_type":   "CallEdgeVersion",
	})
	require.NoError(t, err)

	statesRes, err := exec.Execute(ctx, `
MATCH (ck:CodeKey)-[:HAS_STATE]->(cs:CodeState)
RETURN ck.entity_id, ck.relation_type, cs.state_id, cs.code_key, cs.tx_id, cs.commit_hash, cs.asserted_by, cs.semantic_type, cs.valid_from_iso
ORDER BY cs.state_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{
		"entity-single",
		"calls",
		"state-single",
		"repo_fact|calls|single",
		"tx-single",
		"commit-single-row",
		"TJ Sweet",
		"CallEdgeVersion",
		"2026-03-20T20:22:20Z",
	}}, statesRes.Rows)

	commitRes, err := exec.Execute(ctx, `
MATCH (c:Commit)
RETURN c.hash, c.tx_id, c.actor, c.timestamp
ORDER BY c.hash
`, nil)
	require.NoError(t, err)
	require.Len(t, commitRes.Rows, 1)
	require.Equal(t, "commit-single-row", commitRes.Rows[0][0])
	require.Equal(t, "tx-single", commitRes.Rows[0][1])
	require.Equal(t, "TJ Sweet", commitRes.Rows[0][2])
	require.NotNil(t, commitRes.Rows[0][3])

	changedRes, err := exec.Execute(ctx, `
MATCH (c:Commit)-[:CHANGED]->(cs:CodeState)
RETURN c.hash, cs.state_id
ORDER BY cs.state_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"commit-single-row", "state-single"}}, changedRes.Rows)

	touchedRes, err := exec.Execute(ctx, `
MATCH (c:Commit)-[:TOUCHED]->(ck:CodeKey)
RETURN c.hash, ck.entity_id, ck.relation_type
ORDER BY ck.entity_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"commit-single-row", "entity-single", "calls"}}, touchedRes.Rows)

	nodeCounts, err := exec.Execute(ctx, `
MATCH (ck:CodeKey) RETURN count(ck)
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, nodeCounts.Rows)

	stateCounts, err := exec.Execute(ctx, `
MATCH (cs:CodeState) RETURN count(cs)
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, stateCounts.Rows)
}

func TestE2E_UnwindMergeChainBatch_QEventsOptionalLookup_Deterministic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedQueries := []string{
		`CREATE (:CodeState {state_id: 'state-by-id-1', code_key: 'ck-a', tx_id: 'tx-a'})`,
		`CREATE (:CodeState {state_id: 'state-by-key-1', code_key: 'ck-fallback-1', tx_id: 'tx-fb-1'})`,
		`CREATE (:CodeState {state_id: 'state-by-key-2', code_key: 'ck-fallback-2', tx_id: 'tx-fb-2'})`,
	}
	for _, q := range seedQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (cc:CodeChange {change_id: row.change_id})
SET cc.tx_id = row.tx_id,
    cc.actor = row.actor,
    cc.op_type = row.op_type,
    cc.commit_hash = row.commit_hash
MERGE (c:Commit {hash: row.commit_hash})
ON CREATE SET c.tx_id = row.tx_id, c.actor = row.actor
MERGE (c)-[:EMITTED]->(cc)
WITH cc, row
OPTIONAL MATCH (csByID:CodeState {state_id: row.affected_state_id})
WITH cc, row, csByID
OPTIONAL MATCH (csByKey:CodeState {code_key: row.affected_code_key, tx_id: row.tx_id})
WITH cc, coalesce(csByID, csByKey) AS cs
WHERE cs IS NOT NULL
MERGE (cc)-[:IMPACTS]->(cs)
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"change_id":         "chg-row-id",
				"tx_id":             "tx-a",
				"actor":             "alice",
				"op_type":           "update",
				"commit_hash":       "commit-a",
				"affected_state_id": "state-by-id-1",
				"affected_code_key": "unused",
			},
			{
				"change_id":         "chg-row-fallback-1",
				"tx_id":             "tx-fb-1",
				"actor":             "bob",
				"op_type":           "create",
				"commit_hash":       "commit-b",
				"affected_state_id": "missing-id",
				"affected_code_key": "ck-fallback-1",
			},
			{
				"change_id":         "chg-row-fallback-2",
				"tx_id":             "tx-fb-2",
				"actor":             "carol",
				"op_type":           "delete",
				"commit_hash":       "commit-c",
				"affected_state_id": "",
				"affected_code_key": "ck-fallback-2",
			},
			{
				"change_id":         "chg-row-miss",
				"tx_id":             "tx-none",
				"actor":             "dave",
				"op_type":           "update",
				"commit_hash":       "commit-d",
				"affected_state_id": "missing",
				"affected_code_key": "missing-key",
			},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch)

	changesRes, err := exec.Execute(ctx, `
MATCH (cc:CodeChange)
RETURN cc.change_id, cc.tx_id, cc.actor, cc.op_type, cc.commit_hash
ORDER BY cc.change_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"chg-row-fallback-1", "tx-fb-1", "bob", "create", "commit-b"},
		{"chg-row-fallback-2", "tx-fb-2", "carol", "delete", "commit-c"},
		{"chg-row-id", "tx-a", "alice", "update", "commit-a"},
		{"chg-row-miss", "tx-none", "dave", "update", "commit-d"},
	}, changesRes.Rows)

	emittedRes, err := exec.Execute(ctx, `
MATCH (c:Commit)-[:EMITTED]->(cc:CodeChange)
RETURN c.hash, c.tx_id, c.actor, cc.change_id
ORDER BY cc.change_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"commit-b", "tx-fb-1", "bob", "chg-row-fallback-1"},
		{"commit-c", "tx-fb-2", "carol", "chg-row-fallback-2"},
		{"commit-a", "tx-a", "alice", "chg-row-id"},
		{"commit-d", "tx-none", "dave", "chg-row-miss"},
	}, emittedRes.Rows)

	impactsRes, err := exec.Execute(ctx, `
MATCH (cc:CodeChange)-[:IMPACTS]->(cs:CodeState)
RETURN cc.change_id, cs.state_id
ORDER BY cc.change_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"chg-row-fallback-1", "state-by-key-1"},
		{"chg-row-fallback-2", "state-by-key-2"},
		{"chg-row-id", "state-by-id-1"},
	}, impactsRes.Rows)

	missRes, err := exec.Execute(ctx, `MATCH (:CodeChange {change_id: 'chg-row-miss'})-[:IMPACTS]->(:CodeState) RETURN count(*)`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, missRes.Rows)
}

func TestE2E_UnwindMergeChainBatch_QEventsOptionalLookup_ConsecutiveOptionalMatches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedQueries := []string{
		`CREATE (:CodeState {state_id: 'state-by-id-1', code_key: 'ck-a', tx_id: 'tx-a'})`,
		`CREATE (:CodeState {state_id: 'state-by-key-1', code_key: 'ck-fallback-1', tx_id: 'tx-fb-1'})`,
	}
	for _, q := range seedQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (cc:CodeChange {change_id: row.change_id})
SET cc.tx_id = row.tx_id,
    cc.actor = row.actor,
    cc.timestamp = datetime(row.timestamp_iso),
    cc.op_type = row.op_type,
    cc.commit_hash = row.commit_hash
MERGE (c:Commit {hash: row.commit_hash})
ON CREATE SET c.timestamp = datetime(row.timestamp_iso), c.tx_id = row.tx_id, c.actor = row.actor
MERGE (c)-[:EMITTED]->(cc)
WITH cc, row
OPTIONAL MATCH (csByID:CodeState {state_id: row.affected_state_id})
OPTIONAL MATCH (csByKey:CodeState {code_key: row.affected_code_key, tx_id: row.tx_id})
WITH cc, coalesce(csByID, csByKey) AS cs
WHERE cs IS NOT NULL
MERGE (cc)-[:IMPACTS]->(cs)
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"change_id":         "chg-row-id",
				"tx_id":             "tx-a",
				"actor":             "alice",
				"timestamp_iso":     "2026-03-20T12:00:00Z",
				"op_type":           "update",
				"commit_hash":       "commit-a",
				"affected_state_id": "state-by-id-1",
				"affected_code_key": "unused",
			},
			{
				"change_id":         "chg-row-fallback",
				"tx_id":             "tx-fb-1",
				"actor":             "bob",
				"timestamp_iso":     "2026-03-20T12:01:00Z",
				"op_type":           "create",
				"commit_hash":       "commit-b",
				"affected_state_id": "missing-id",
				"affected_code_key": "ck-fallback-1",
			},
			{
				"change_id":         "chg-row-miss",
				"tx_id":             "tx-none",
				"actor":             "carol",
				"timestamp_iso":     "2026-03-20T12:02:00Z",
				"op_type":           "delete",
				"commit_hash":       "commit-c",
				"affected_state_id": "missing",
				"affected_code_key": "missing-key",
			},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch)

	impactsRes, err := exec.Execute(ctx, `
MATCH (cc:CodeChange)-[:IMPACTS]->(cs:CodeState)
RETURN cc.change_id, cs.state_id
ORDER BY cc.change_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"chg-row-fallback", "state-by-key-1"},
		{"chg-row-id", "state-by-id-1"},
	}, impactsRes.Rows)
}

func TestE2E_UnwindCompoundBatch_QVersionsMonolithicTwoPass_Deterministic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE CONSTRAINT commit_hash_unique IF NOT EXISTS FOR (c:Commit) REQUIRE c.hash IS UNIQUE`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (ck:CodeKey {entity_id: row.entity_id, relation_type: row.relation_type})
MERGE (cs:CodeState {state_id: row.state_id})
SET cs.code_key = row.code_key,
    cs.tx_id = row.tx_id,
    cs.commit_hash = row.commit_hash,
    cs.valid_from_iso = row.valid_from_iso,
    cs.valid_from = datetime(row.valid_from_iso),
    cs.value_json = row.value_json,
    cs.valid_to = CASE WHEN row.valid_to_iso IS NULL THEN null ELSE datetime(row.valid_to_iso) END,
    cs.asserted_at = datetime(row.asserted_at_iso),
    cs.asserted_by = row.asserted_by,
    cs.semantic_type = row.semantic_type
MERGE (c:Commit {hash: row.commit_hash})
ON CREATE SET c.timestamp = datetime(row.asserted_at_iso), c.tx_id = row.tx_id, c.actor = row.asserted_by
WITH $rows AS rows
UNWIND rows AS row
MATCH (ck:CodeKey {entity_id: row.entity_id, relation_type: row.relation_type})
MATCH (cs:CodeState {state_id: row.state_id})
MATCH (c:Commit {hash: row.commit_hash})
MERGE (ck)-[:HAS_STATE]->(cs)
MERGE (c)-[:CHANGED]->(cs)
MERGE (c)-[:TOUCHED]->(ck)
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id":       "entity-1",
				"relation_type":   "calls",
				"state_id":        "state-1",
				"code_key":        "repo_fact|calls|a",
				"tx_id":           "tx-1",
				"commit_hash":     "commit-shared",
				"valid_from_iso":  "2026-03-20T20:22:20Z",
				"valid_to_iso":    nil,
				"asserted_at_iso": "2026-03-20T20:22:20Z",
				"value_json":      `{"repo":"git-to-graph","source":"a","target":"b"}`,
				"asserted_by":     "TJ Sweet",
				"semantic_type":   "CallEdgeVersion",
			},
			{
				"entity_id":       "entity-2",
				"relation_type":   "contains",
				"state_id":        "state-2",
				"code_key":        "repo_fact|contains|x",
				"tx_id":           "tx-1",
				"commit_hash":     "commit-shared",
				"valid_from_iso":  "2026-03-20T20:22:20Z",
				"valid_to_iso":    nil,
				"asserted_at_iso": "2026-03-20T20:22:20Z",
				"value_json":      `{"repo":"git-to-graph","parent":"x","child":"y"}`,
				"asserted_by":     "TJ Sweet",
				"semantic_type":   "ContainsEdgeVersion",
			},
		},
	})
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()
	require.True(t, trace.CompoundQueryFastPath)
	require.True(t, trace.UnwindMergeChainBatch)

	statesRes, err := exec.Execute(ctx, `
MATCH (ck:CodeKey)-[:HAS_STATE]->(cs:CodeState)
RETURN ck.entity_id, ck.relation_type, cs.state_id, cs.code_key, cs.tx_id, cs.commit_hash, cs.asserted_by, cs.semantic_type
ORDER BY cs.state_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"entity-1", "calls", "state-1", "repo_fact|calls|a", "tx-1", "commit-shared", "TJ Sweet", "CallEdgeVersion"},
		{"entity-2", "contains", "state-2", "repo_fact|contains|x", "tx-1", "commit-shared", "TJ Sweet", "ContainsEdgeVersion"},
	}, statesRes.Rows)

	commitRes, err := exec.Execute(ctx, `MATCH (c:Commit) RETURN c.hash, c.tx_id, c.actor ORDER BY c.hash`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"commit-shared", "tx-1", "TJ Sweet"}}, commitRes.Rows)

	changedRes, err := exec.Execute(ctx, `
MATCH (c:Commit)-[:CHANGED]->(cs:CodeState)
RETURN c.hash, cs.state_id
ORDER BY cs.state_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"commit-shared", "state-1"}, {"commit-shared", "state-2"}}, changedRes.Rows)

	touchedRes, err := exec.Execute(ctx, `
MATCH (c:Commit)-[:TOUCHED]->(ck:CodeKey)
RETURN c.hash, ck.entity_id, ck.relation_type
ORDER BY ck.entity_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"commit-shared", "entity-1", "calls"}, {"commit-shared", "entity-2", "contains"}}, touchedRes.Rows)
}

func TestE2E_UnwindCompoundBatch_QEventsMonolithicTwoPassOptionalLookup_Deterministic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seedQueries := []string{
		`CREATE (:CodeState {state_id: 'state-by-id-1', code_key: 'ck-a', tx_id: 'tx-a'})`,
		`CREATE (:CodeState {state_id: 'state-by-key-1', code_key: 'ck-fallback-1', tx_id: 'tx-fb-1'})`,
		`CREATE (:CodeState {state_id: 'state-by-key-2', code_key: 'ck-fallback-2', tx_id: 'tx-fb-2'})`,
	}
	for _, q := range seedQueries {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (cc:CodeChange {change_id: row.change_id})
SET cc.tx_id = row.tx_id,
    cc.actor = row.actor,
    cc.timestamp = datetime(row.timestamp_iso),
    cc.op_type = row.op_type,
    cc.commit_hash = row.commit_hash
MERGE (c:Commit {hash: row.commit_hash})
ON CREATE SET c.timestamp = datetime(row.timestamp_iso), c.tx_id = row.tx_id, c.actor = row.actor
WITH $rows AS rows
UNWIND rows AS row
MATCH (cc:CodeChange {change_id: row.change_id})
MATCH (c:Commit {hash: row.commit_hash})
MERGE (c)-[:EMITTED]->(cc)
WITH cc, row
OPTIONAL MATCH (csByID:CodeState {state_id: row.affected_state_id})
OPTIONAL MATCH (csByKey:CodeState {code_key: row.affected_code_key, tx_id: row.tx_id})
WITH cc, coalesce(csByID, csByKey) AS cs
WHERE cs IS NOT NULL
MERGE (cc)-[:IMPACTS]->(cs)
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"change_id":         "chg-row-id",
				"tx_id":             "tx-a",
				"actor":             "alice",
				"timestamp_iso":     "2026-03-20T12:00:00Z",
				"op_type":           "update",
				"commit_hash":       "commit-a",
				"affected_state_id": "state-by-id-1",
				"affected_code_key": "unused",
			},
			{
				"change_id":         "chg-row-fallback-1",
				"tx_id":             "tx-fb-1",
				"actor":             "bob",
				"timestamp_iso":     "2026-03-20T12:01:00Z",
				"op_type":           "create",
				"commit_hash":       "commit-b",
				"affected_state_id": "missing-id",
				"affected_code_key": "ck-fallback-1",
			},
			{
				"change_id":         "chg-row-fallback-2",
				"tx_id":             "tx-fb-2",
				"actor":             "carol",
				"timestamp_iso":     "2026-03-20T12:02:00Z",
				"op_type":           "delete",
				"commit_hash":       "commit-c",
				"affected_state_id": "",
				"affected_code_key": "ck-fallback-2",
			},
			{
				"change_id":         "chg-row-miss",
				"tx_id":             "tx-none",
				"actor":             "dave",
				"timestamp_iso":     "2026-03-20T12:03:00Z",
				"op_type":           "update",
				"commit_hash":       "commit-d",
				"affected_state_id": "missing",
				"affected_code_key": "missing-key",
			},
		},
	})
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()
	require.True(t, trace.CompoundQueryFastPath)
	require.True(t, trace.UnwindMergeChainBatch)

	changesRes, err := exec.Execute(ctx, `
MATCH (cc:CodeChange)
RETURN cc.change_id, cc.tx_id, cc.actor, cc.op_type, cc.commit_hash
ORDER BY cc.change_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"chg-row-fallback-1", "tx-fb-1", "bob", "create", "commit-b"},
		{"chg-row-fallback-2", "tx-fb-2", "carol", "delete", "commit-c"},
		{"chg-row-id", "tx-a", "alice", "update", "commit-a"},
		{"chg-row-miss", "tx-none", "dave", "update", "commit-d"},
	}, changesRes.Rows)

	emittedRes, err := exec.Execute(ctx, `
MATCH (c:Commit)-[:EMITTED]->(cc:CodeChange)
RETURN c.hash, c.tx_id, c.actor, cc.change_id
ORDER BY cc.change_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"commit-b", "tx-fb-1", "bob", "chg-row-fallback-1"},
		{"commit-c", "tx-fb-2", "carol", "chg-row-fallback-2"},
		{"commit-a", "tx-a", "alice", "chg-row-id"},
		{"commit-d", "tx-none", "dave", "chg-row-miss"},
	}, emittedRes.Rows)

	impactsRes, err := exec.Execute(ctx, `
MATCH (cc:CodeChange)-[:IMPACTS]->(cs:CodeState)
RETURN cc.change_id, cs.state_id
ORDER BY cc.change_id
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"chg-row-fallback-1", "state-by-key-1"},
		{"chg-row-fallback-2", "state-by-key-2"},
		{"chg-row-id", "state-by-id-1"},
	}, impactsRes.Rows)
}

func TestE2E_UnwindCompoundBatch_NAryThreeStage_Deterministic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, strings.TrimSpace(`
UNWIND $rows AS row
MERGE (a:StageA {id: row.a_id})
SET a.kind = row.kind
MERGE (b:StageB {id: row.b_id})
SET b.kind = row.kind
MERGE (c:StageC {id: row.c_id})
SET c.kind = row.kind
WITH $rows AS rows
UNWIND rows AS row
MATCH (a:StageA {id: row.a_id})
MATCH (b:StageB {id: row.b_id})
MERGE (a)-[:AB]->(b)
WITH $rows AS rows
UNWIND rows AS row
MATCH (b:StageB {id: row.b_id})
MATCH (c:StageC {id: row.c_id})
MERGE (b)-[:BC]->(c)
`), map[string]interface{}{
		"rows": []map[string]interface{}{
			{"a_id": "a-1", "b_id": "b-1", "c_id": "c-1", "kind": "alpha"},
			{"a_id": "a-2", "b_id": "b-2", "c_id": "c-2", "kind": "beta"},
		},
	})
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()
	require.True(t, trace.CompoundQueryFastPath)
	require.True(t, trace.UnwindMergeChainBatch)

	aRes, err := exec.Execute(ctx, `MATCH (a:StageA) RETURN a.id, a.kind ORDER BY a.id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a-1", "alpha"}, {"a-2", "beta"}}, aRes.Rows)

	bRes, err := exec.Execute(ctx, `MATCH (b:StageB) RETURN b.id, b.kind ORDER BY b.id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"b-1", "alpha"}, {"b-2", "beta"}}, bRes.Rows)

	cRes, err := exec.Execute(ctx, `MATCH (c:StageC) RETURN c.id, c.kind ORDER BY c.id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"c-1", "alpha"}, {"c-2", "beta"}}, cRes.Rows)

	abRes, err := exec.Execute(ctx, `MATCH (a:StageA)-[:AB]->(b:StageB) RETURN a.id, b.id ORDER BY a.id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a-1", "b-1"}, {"a-2", "b-2"}}, abRes.Rows)

	bcRes, err := exec.Execute(ctx, `MATCH (b:StageB)-[:BC]->(c:StageC) RETURN b.id, c.id ORDER BY b.id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"b-1", "c-1"}, {"b-2", "c-2"}}, bcRes.Rows)
}
