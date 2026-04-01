//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func TestVectorTraversalShapeMatrix_BoltVsHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping traversal matrix e2e benchmark in -short")
	}
	reportf := func(format string, args ...any) {
		t.Helper()
		msg := fmt.Sprintf(format, args...)
		t.Log(msg)
		fmt.Fprintln(os.Stdout, msg)
	}

	repoRoot := mustRepoRoot(t)
	dataDir := t.TempDir()
	binPath := buildNornicBinary(t, repoRoot)
	httpPort := pickPort(t)
	boltPort := pickPort(t)
	grpcPort := pickPort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proc := startNornicDB(t, ctx, binPath, dataDir, httpPort, boltPort, grpcPort)
	defer proc.stop(t)

	httpAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)
	boltAddr := fmt.Sprintf("127.0.0.1:%d", boltPort)
	waitTCP(t, httpAddr, 30*time.Second)
	waitTCP(t, boltAddr, 30*time.Second)

	httpClient := &http.Client{Timeout: 15 * time.Second}
	dbName := discoverDefaultDatabase(t, httpClient, httpAddr)
	driver := newBoltDriver(t, boltAddr)
	defer func() { _ = driver.Close(context.Background()) }()

	seedVectorTraversalFixtureE2E(t, driver)
	rootIDs := fetchTraversalRootIDsE2E(t, driver)

	fanouts := parseFanoutsEnv("NORNICDB_TRAVERSAL_FANOUTS", []int{1, 2, 3})
	iterations := envInt("NORNICDB_TRAVERSAL_MATRIX_ITERS", 3)
	pathCap := envInt("NORNICDB_TRAVERSAL_PATH_CAP", 5)

	type tableRow struct {
		shape    string
		fanout   string
		depth    int
		protocol string
		meanMS   float64
		p50MS    float64
		p95MS    float64
	}
	rows := make([]tableRow, 0, 128)

	shapeSpecs := []struct {
		name      string
		fanoutSet []int
		queryFor  func(depth, fanout, pathCap int) (string, map[string]any, string)
		assertRow func(t *testing.T, row []any, depth, fanout, pathCap int, expectedNodeID string)
	}{
		{
			name:      "chain",
			fanoutSet: []int{1},
			queryFor: func(depth, _fanout, _pathCap int) (string, map[string]any, string) {
				return fmt.Sprintf(`
CALL db.index.vector.queryNodes('idx_original_text', $vectorTopK, $query)
YIELD node, score
MATCH p = (node)-[:BENCH_HOP*1..%d]->(:BenchmarkHop)
WITH node, score, max(length(p)) AS maxDepth
RETURN elementId(node) AS nodeID, score, maxDepth
LIMIT $topK
`, depth), map[string]any{"vectorTopK": int64(1), "topK": int64(1), "query": []any{0.95, 0.05, 0.0}}, rootIDs["chain-root"]
			},
			assertRow: func(t *testing.T, row []any, depth, _fanout, _pathCap int, expectedNodeID string) {
				require.Len(t, row, 3)
				require.Equal(t, normalizeElementIDE2E(expectedNodeID), normalizeElementIDE2E(fmt.Sprintf("%v", row[0])))
				require.Equal(t, int64(depth), rowAsInt64(t, row[2]))
			},
		},
		{
			name:      "branching",
			fanoutSet: fanouts,
			queryFor: func(depth, fanout, pathCap int) (string, map[string]any, string) {
				return fmt.Sprintf(`
CALL db.index.vector.queryNodes('idx_original_text', $vectorTopK, $query)
YIELD node, score
MATCH p = (node)-[:BENCH_HOP|REL_A|REL_B*1..%d]->(x)
WHERE ALL(n IN nodes(p) WHERE size(labels(n)) > 0)
WITH node, score, p, length(p) AS d
ORDER BY d ASC
WITH node, score, collect(p)[0..$pathCap] AS paths
RETURN elementId(node) AS nodeID, score, size(paths) AS pathCount
LIMIT $topK
`, depth), map[string]any{"vectorTopK": int64(1), "topK": int64(1), "pathCap": int64(pathCap), "query": branchingVectorForFanoutE2E(fanout)}, rootIDs[fmt.Sprintf("branch-f%d", fanout)]
			},
			assertRow: func(t *testing.T, row []any, depth, fanout, pathCap int, expectedNodeID string) {
				require.Len(t, row, 3)
				require.Equal(t, normalizeElementIDE2E(expectedNodeID), normalizeElementIDE2E(fmt.Sprintf("%v", row[0])))
				require.Equal(t, int64(minIntE2E(pathCap, geometricPathCountE2E(fanout, depth))), rowAsInt64(t, row[2]))
			},
		},
		{
			name:      "frontier",
			fanoutSet: fanouts,
			queryFor: func(depth, fanout, _pathCap int) (string, map[string]any, string) {
				return fmt.Sprintf(`
CALL db.index.vector.queryNodes('idx_original_text', $vectorTopK, $query)
YIELD node, score
MATCH (node)-[:REL*1..%d]->(x)
WITH node, score, length(shortestPath((node)-[:REL*1..%d]->(x))) AS d
WITH node, score, min(d) AS nearest, count(*) AS reachable
RETURN elementId(node) AS nodeID, score, nearest, reachable
LIMIT $topK
`, depth, depth), map[string]any{"vectorTopK": int64(1), "topK": int64(1), "query": frontierVectorForFanoutE2E(fanout)}, rootIDs[fmt.Sprintf("frontier-f%d", fanout)]
			},
			assertRow: func(t *testing.T, row []any, depth, fanout, _pathCap int, expectedNodeID string) {
				require.Len(t, row, 4)
				require.Equal(t, normalizeElementIDE2E(expectedNodeID), normalizeElementIDE2E(fmt.Sprintf("%v", row[0])))
				require.Equal(t, int64(1), rowAsInt64(t, row[2]))
				require.Equal(t, int64(geometricPathCountE2E(fanout, depth)), rowAsInt64(t, row[3]))
			},
		},
		{
			name:      "constrained",
			fanoutSet: []int{2},
			queryFor: func(depth, _fanout, _pathCap int) (string, map[string]any, string) {
				return fmt.Sprintf(`
CALL db.index.vector.queryNodes('idx_original_text', $vectorTopK, $query)
YIELD node, score
MATCH p = (node)-[:REL*1..%d]->(x)
WHERE any(r IN relationships(p) WHERE r.weight >= $minWeight)
  AND any(n IN nodes(p) WHERE n.category IN $cats)
RETURN elementId(node) AS nodeID, score, max(length(p)) AS maxDepth
LIMIT $topK
`, depth), map[string]any{"vectorTopK": int64(2), "topK": int64(2), "query": []any{0.12, 0.84, 0.04}, "minWeight": 2.5, "cats": []string{"allowed"}}, rootIDs["constrained-strong"]
			},
			assertRow: func(t *testing.T, row []any, depth, _fanout, _pathCap int, expectedNodeID string) {
				require.Len(t, row, 3)
				require.Equal(t, normalizeElementIDE2E(expectedNodeID), normalizeElementIDE2E(fmt.Sprintf("%v", row[0])))
				require.Equal(t, int64(depth), rowAsInt64(t, row[2]))
			},
		},
	}

	for _, spec := range shapeSpecs {
		for _, fanout := range spec.fanoutSet {
			for depth := 1; depth <= 6; depth++ {
				query, params, expectedNodeID := spec.queryFor(depth, fanout, pathCap)
				boltSummary := runSerialBench(iterations, func(ctx context.Context) error {
					row, err := runBoltSingleRow(ctx, driver, query, params)
					if err != nil {
						return err
					}
					spec.assertRow(t, row, depth, fanout, pathCap, expectedNodeID)
					return nil
				})
				rows = append(rows, tableRow{shape: spec.name, fanout: fanoutLabel(spec.name, fanout), depth: depth, protocol: "bolt", meanMS: durationToMS(meanDuration(boltSummary.lat)), p50MS: durationToMS(percentile(boltSummary.lat, 0.50)), p95MS: durationToMS(percentile(boltSummary.lat, 0.95))})

				httpSummary := runSerialBench(iterations, func(ctx context.Context) error {
					row, err := neo4jHTTPCommitSingleRow(ctx, httpClient, httpAddr, dbName, query, params)
					if err != nil {
						return err
					}
					spec.assertRow(t, row, depth, fanout, pathCap, expectedNodeID)
					return nil
				})
				rows = append(rows, tableRow{shape: spec.name, fanout: fanoutLabel(spec.name, fanout), depth: depth, protocol: "http", meanMS: durationToMS(meanDuration(httpSummary.lat)), p50MS: durationToMS(percentile(httpSummary.lat, 0.50)), p95MS: durationToMS(percentile(httpSummary.lat, 0.95))})
			}
		}
	}

	reportf("| shape | fanout | depth | protocol | mean_ms | p50_ms | p95_ms |")
	reportf("| --- | ---: | ---: | --- | ---: | ---: | ---: |")
	for _, row := range rows {
		reportf("| %s | %s | %d | %s | %.3f | %.3f | %.3f |", row.shape, row.fanout, row.depth, row.protocol, row.meanMS, row.p50MS, row.p95MS)
	}
}

func seedVectorTraversalFixtureE2E(t *testing.T, driver neo4j.DriverWithContext) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sess := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer func() { _ = sess.Close(ctx) }()

	runWrite := func(query string, params map[string]any) {
		t.Helper()
		_, err := sess.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			_, err := tx.Run(ctx, query, params)
			return nil, err
		})
		require.NoError(t, err)
	}

	runWrite("CALL db.index.vector.createNodeIndex('idx_original_text', 'OriginalText', 'embedding', 3, 'cosine')", nil)
	runWrite(`
UNWIND $rows AS row
CREATE (o:OriginalText {textKey: row.textKey, originalText: row.originalText, embedding: row.embedding, nodeKey: row.textKey})
`, map[string]any{"rows": traversalRootRowsE2E()})
	runWrite(`
UNWIND $rows AS row
CREATE (h:BenchmarkHop {nodeKey: row.nodeKey, hopDepth: row.hopDepth, rootKey: row.rootKey})
`, map[string]any{"rows": chainNodeRowsE2E()})
	runWrite(`
UNWIND $rows AS row
CREATE (h:BranchHop {nodeKey: row.nodeKey, category: row.category, hopDepth: row.hopDepth, rootKey: row.rootKey, branchSlot: row.branchSlot})
`, map[string]any{"rows": branchingNodeRowsE2E()})
	runWrite(`
UNWIND $rows AS row
CREATE (h:FrontierHop {nodeKey: row.nodeKey, category: row.category, hopDepth: row.hopDepth, rootKey: row.rootKey, branchSlot: row.branchSlot})
`, map[string]any{"rows": frontierNodeRowsE2E()})
	runWrite(`
UNWIND $rows AS row
CREATE (h:ConstrainedHop {nodeKey: row.nodeKey, category: row.category, hopDepth: row.hopDepth, rootKey: row.rootKey, branchSlot: row.branchSlot})
`, map[string]any{"rows": constrainedNodeRowsE2E()})
	for edgeType, rows := range traversalEdgeRowsE2E() {
		query := fmt.Sprintf(`
UNWIND $rows AS row
MATCH (a {nodeKey: row.from}), (b {nodeKey: row.to})
CREATE (a)-[:%s {weight: row.weight, depth: row.depth}]->(b)
`, edgeType)
		runWrite(query, map[string]any{"rows": rows})
	}
}

func runBoltSingleRow(ctx context.Context, driver neo4j.DriverWithContext, query string, params map[string]any) ([]any, error) {
	sess := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer func() { _ = sess.Close(ctx) }()
	out, err := sess.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, err
		}
		if !res.Next(ctx) {
			return nil, res.Err()
		}
		return append([]any{}, res.Record().Values...), res.Err()
	})
	if err != nil {
		return nil, err
	}
	row, _ := out.([]any)
	return row, nil
}

func neo4jHTTPCommitSingleRow(ctx context.Context, c *http.Client, httpAddr, db, statement string, params map[string]any) ([]any, error) {
	reqBody := map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}}
	raw, _ := json.Marshal(reqBody)
	url := fmt.Sprintf("http://%s/db/%s/tx/commit", httpAddr, db)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("neo4j http status=%d body=%s", resp.StatusCode, string(body))
	}
	var parsed struct {
		Results []struct {
			Data []struct {
				Row []any `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	if len(parsed.Errors) > 0 || len(parsed.Results) == 0 || len(parsed.Results[0].Data) == 0 {
		return nil, fmt.Errorf("unexpected neo4j http response: %s", string(body))
	}
	return parsed.Results[0].Data[0].Row, nil
}

func runSerialBench(iterations int, fn func(context.Context) error) benchSummary {
	if iterations <= 0 {
		iterations = 1
	}
	lat := make([]time.Duration, 0, iterations)
	start := time.Now()
	for i := 0; i < iterations; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		begin := time.Now()
		err := fn(ctx)
		lat = append(lat, time.Since(begin))
		cancel()
		if err != nil {
			break
		}
	}
	sortDurations(lat)
	return benchSummary{ops: len(lat), dur: time.Since(start), lat: lat}
}

func meanDuration(lat []time.Duration) time.Duration {
	if len(lat) == 0 {
		return 0
	}
	var total time.Duration
	for _, item := range lat {
		total += item
	}
	return time.Duration(int64(total) / int64(len(lat)))
}

func durationToMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func rowAsInt64(t *testing.T, value any) int64 {
	t.Helper()
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		v, err := typed.Int64()
		require.NoError(t, err)
		return v
	default:
		t.Fatalf("unexpected numeric type %T (%v)", value, value)
		return 0
	}
}

func fanoutLabel(shape string, fanout int) string {
	if shape == "chain" {
		return "-"
	}
	return strconv.Itoa(fanout)
}

func normalizeElementIDE2E(value string) string {
	parts := strings.Split(value, ":")
	if len(parts) >= 3 {
		return parts[len(parts)-1]
	}
	return value
}

func parseFanoutsEnv(key string, fallback []int) []int {
	raw := os.Getenv(key)
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		if n, err := strconv.Atoi(strings.TrimSpace(part)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func traversalRootRowsE2E() []map[string]any {
	return []map[string]any{
		{"textKey": "chain-root", "originalText": "chain baseline", "embedding": []any{0.95, 0.05, 0.0}},
		{"textKey": "branch-f1", "originalText": "branch fanout one", "embedding": branchingVectorForFanoutE2E(1)},
		{"textKey": "branch-f2", "originalText": "branch fanout two", "embedding": branchingVectorForFanoutE2E(2)},
		{"textKey": "branch-f3", "originalText": "branch fanout three", "embedding": branchingVectorForFanoutE2E(3)},
		{"textKey": "frontier-f1", "originalText": "frontier fanout one", "embedding": frontierVectorForFanoutE2E(1)},
		{"textKey": "frontier-f2", "originalText": "frontier fanout two", "embedding": frontierVectorForFanoutE2E(2)},
		{"textKey": "frontier-f3", "originalText": "frontier fanout three", "embedding": frontierVectorForFanoutE2E(3)},
		{"textKey": "constrained-strong", "originalText": "weighted allowed root", "embedding": []any{0.12, 0.84, 0.04}},
		{"textKey": "constrained-weak", "originalText": "filtered weak root", "embedding": []any{0.11, 0.79, 0.10}},
	}
}

func chainNodeRowsE2E() []map[string]any {
	rows := make([]map[string]any, 0, 6)
	for depth := 1; depth <= 6; depth++ {
		rows = append(rows, map[string]any{"nodeKey": fmt.Sprintf("chain-root:hop:%d", depth), "hopDepth": int64(depth), "rootKey": "chain-root"})
	}
	return rows
}

func branchingNodeRowsE2E() []map[string]any {
	rows := []map[string]any{}
	for _, fanout := range []int{1, 2, 3} {
		rows = append(rows, treeNodeRowsE2E(fmt.Sprintf("branch-f%d", fanout), fanout, 6, "allowed")...)
	}
	return rows
}

func frontierNodeRowsE2E() []map[string]any {
	rows := []map[string]any{}
	for _, fanout := range []int{1, 2, 3} {
		rows = append(rows, treeNodeRowsE2E(fmt.Sprintf("frontier-f%d", fanout), fanout, 6, "allowed")...)
	}
	return rows
}

func constrainedNodeRowsE2E() []map[string]any {
	rows := treeNodeRowsE2E("constrained-strong", 2, 6, "allowed")
	rows = append(rows, treeNodeRowsE2E("constrained-weak", 2, 6, "other")...)
	return rows
}

func treeNodeRowsE2E(rootKey string, fanout, maxDepth int, category string) []map[string]any {
	rows := []map[string]any{}
	nextOrdinal := 0
	levelCount := 1
	for depth := 1; depth <= maxDepth; depth++ {
		for parent := 0; parent < levelCount; parent++ {
			for childIdx := 0; childIdx < fanout; childIdx++ {
				nextOrdinal++
				rows = append(rows, map[string]any{
					"nodeKey":    fmt.Sprintf("%s:node:%02d:%04d", rootKey, depth, nextOrdinal),
					"category":   category,
					"hopDepth":   int64(depth),
					"rootKey":    rootKey,
					"branchSlot": int64(childIdx),
				})
			}
		}
		levelCount *= fanout
	}
	return rows
}

func traversalEdgeRowsE2E() map[string][]map[string]any {
	out := map[string][]map[string]any{
		"BENCH_HOP": {},
		"REL_A":     {},
		"REL_B":     {},
		"REL":       {},
	}
	for depth := 1; depth <= 6; depth++ {
		from := "chain-root"
		if depth > 1 {
			from = fmt.Sprintf("chain-root:hop:%d", depth-1)
		}
		to := fmt.Sprintf("chain-root:hop:%d", depth)
		out["BENCH_HOP"] = append(out["BENCH_HOP"], map[string]any{"from": from, "to": to, "weight": 1.0, "depth": int64(depth)})
	}
	for _, fanout := range []int{1, 2, 3} {
		appendTreeEdgesE2E(out, fmt.Sprintf("branch-f%d", fanout), fanout, 6, func(depth, childIdx int) string {
			if depth == 1 {
				return "BENCH_HOP"
			}
			if childIdx%2 == 0 {
				return "REL_A"
			}
			return "REL_B"
		}, 1.0)
		appendTreeEdgesE2E(out, fmt.Sprintf("frontier-f%d", fanout), fanout, 6, func(_depth, _childIdx int) string { return "REL" }, 1.0)
	}
	appendTreeEdgesE2E(out, "constrained-strong", 2, 6, func(_depth, _childIdx int) string { return "REL" }, 5.0)
	appendTreeEdgesE2E(out, "constrained-weak", 2, 6, func(_depth, _childIdx int) string { return "REL" }, 0.2)
	return out
}

func appendTreeEdgesE2E(dest map[string][]map[string]any, rootKey string, fanout, maxDepth int, edgeTypeFn func(depth, childIdx int) string, weight float64) {
	currentLevel := []string{rootKey}
	nextOrdinal := 0
	for depth := 1; depth <= maxDepth; depth++ {
		nextLevel := make([]string, 0, len(currentLevel)*fanout)
		for _, parentKey := range currentLevel {
			for childIdx := 0; childIdx < fanout; childIdx++ {
				nextOrdinal++
				childKey := fmt.Sprintf("%s:node:%02d:%04d", rootKey, depth, nextOrdinal)
				edgeType := edgeTypeFn(depth, childIdx)
				dest[edgeType] = append(dest[edgeType], map[string]any{"from": parentKey, "to": childKey, "weight": weight, "depth": int64(depth)})
				nextLevel = append(nextLevel, childKey)
			}
		}
		currentLevel = nextLevel
	}
}

func branchingVectorForFanoutE2E(fanout int) []any {
	switch fanout {
	case 1:
		return []any{1.0, 0.0, 0.0}
	case 2:
		return []any{0.0, 1.0, 0.0}
	default:
		return []any{0.0, 0.0, 1.0}
	}
}

func frontierVectorForFanoutE2E(fanout int) []any {
	switch fanout {
	case 1:
		return []any{0.75, 0.25, 0.0}
	case 2:
		return []any{0.25, 0.75, 0.0}
	default:
		return []any{0.25, 0.25, 0.5}
	}
}

func geometricPathCountE2E(fanout, depth int) int {
	if fanout == 1 {
		return depth
	}
	return int((math.Pow(float64(fanout), float64(depth+1)) - float64(fanout)) / float64(fanout-1))
}

func minIntE2E(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fetchTraversalRootIDsE2E(t *testing.T, driver neo4j.DriverWithContext) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sess := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer func() { _ = sess.Close(ctx) }()
	out, err := sess.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, "MATCH (o:OriginalText) RETURN o.textKey AS textKey, elementId(o) AS nodeID", nil)
		if err != nil {
			return nil, err
		}
		ids := map[string]string{}
		for res.Next(ctx) {
			textKey, _ := res.Record().Get("textKey")
			nodeID, _ := res.Record().Get("nodeID")
			ids[fmt.Sprintf("%v", textKey)] = fmt.Sprintf("%v", nodeID)
		}
		return ids, res.Err()
	})
	require.NoError(t, err)
	ids, _ := out.(map[string]string)
	return ids
}
