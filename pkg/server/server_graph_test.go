package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type graphOutgoingErrorEngine struct {
	storage.Engine
}

func (e *graphOutgoingErrorEngine) GetOutgoingEdges(storage.NodeID) ([]*storage.Edge, error) {
	return nil, fmt.Errorf("boom")
}

type graphNoMVCCEngine struct {
	storage.Engine
}

func decodeGraphPayload(t *testing.T, recorderBody interface{ Bytes() []byte }) graphPayload {
	t.Helper()
	var payload graphPayload
	require.NoError(t, json.Unmarshal(recorderBody.Bytes(), &payload))
	return payload
}

func getDefaultStorage(t *testing.T, server *Server) storage.Engine {
	t.Helper()
	engine, err := server.dbManager.GetStorage(server.dbManager.DefaultDatabaseName())
	require.NoError(t, err)
	return engine
}

func defaultGraphPath(server *Server, operation string) string {
	return "/nornicdb/graph/" + server.dbManager.DefaultDatabaseName() + "/" + operation
}

func TestGraphNeighborhoodEndpoint(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "c", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "d", Labels: []string{"Topic"}, Properties: map[string]interface{}{"name": "Databases"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "KNOWS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "cd", StartNode: "c", EndNode: "d", Type: "LIKES"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids":           []string{"a"},
		"depth":              2,
		"relationship_types": []string{"KNOWS"},
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 3, payload.Meta.NodeCount)
	require.Equal(t, 2, payload.Meta.EdgeCount)
	require.Equal(t, "node", payload.Meta.GeneratedFrom)
	require.Equal(t, 2, payload.Meta.Depth)
	require.Equal(t, []string{"a", "b", "c"}, []string{payload.Nodes[0].ID, payload.Nodes[1].ID, payload.Nodes[2].ID})
}

func TestGraphNeighborhoodEndpoint_RejectsWhitespaceOnlyNodeIDs(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{" ", "\t", "\n"},
		"depth":    1,
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
}

func TestGraphNeighborhoodEndpoint_LimitPreservesEdgesBetweenIncludedNodes(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS"}))

	// limit=2 means only the two seed nodes are allowed in the node set.
	// The connecting edge between those already-included nodes must still be returned.
	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"a", "b"},
		"depth":    1,
		"limit":    2,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 2, payload.Meta.NodeCount)
	require.Equal(t, 1, payload.Meta.EdgeCount)
	require.Equal(t, "ab", payload.Edges[0].ID)
}

func TestGraphNeighborhoodEndpoint_LimitTruncatesSeedNodes(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	for _, id := range []string{"a", "b", "c"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Person"}})
		require.NoError(t, err)
	}

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"a", "b", "c"},
		"depth":    1,
		"limit":    2,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 2, payload.Meta.NodeCount)
	require.True(t, payload.Meta.Truncated)
}

func TestGraphPathEndpoint(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	for _, id := range []string{"a", "b", "c", "z"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Node"}, Properties: map[string]interface{}{"name": id}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINKS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "LINKS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "az", StartNode: "a", EndNode: "z", Type: "OTHER"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "path"), map[string]interface{}{
		"source_node_id":     "a",
		"target_node_id":     "c",
		"relationship_types": []string{"LINKS"},
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 3, payload.Meta.NodeCount)
	require.Equal(t, 2, payload.Meta.EdgeCount)
	require.Equal(t, "query", payload.Meta.GeneratedFrom)
	require.Equal(t, "ab", payload.Edges[0].ID)
	require.Equal(t, "bc", payload.Edges[1].ID)
}

func TestGraphPathEndpoint_LimitExceededDoesNotReturnNotFound(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	for _, id := range []string{"a", "b", "c"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Node"}, Properties: map[string]interface{}{"name": id}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINKS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "LINKS"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "path"), map[string]interface{}{
		"source_node_id":     "a",
		"target_node_id":     "c",
		"relationship_types": []string{"LINKS"},
		"limit":              2,
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
	require.Contains(t, resp.Body.String(), "path search limit exceeded")
}

func TestGraphTemporalEndpoint(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice v1"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS", Properties: map[string]interface{}{"weight": 1}}))
	asOf := time.Now().UTC().Format(time.RFC3339Nano)

	time.Sleep(2 * time.Millisecond)
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice v2"}}))
	require.NoError(t, engine.UpdateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LIKES", Properties: map[string]interface{}{"weight": 2}}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "temporal"), map[string]interface{}{
		"node_ids": []string{"a", "b"},
		"as_of":    asOf,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 2, payload.Meta.NodeCount)
	require.Equal(t, 1, payload.Meta.EdgeCount)
	require.Equal(t, "Alice v1", payload.Nodes[0].Properties["name"])
	require.Equal(t, "KNOWS", payload.Edges[0].Type)
	require.Equal(t, asOf, payload.Meta.AsOf)
}

func TestGraphTemporalEndpoint_RejectsWhitespaceOnlyNodeIDs(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "temporal"), map[string]interface{}{
		"node_ids": []string{" ", "\t", "\n"},
		"as_of":    time.Now().UTC().Format(time.RFC3339Nano),
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
}

func TestGraphDiffEndpoint(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice v1"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS"}))
	asOf := time.Now().UTC().Format(time.RFC3339Nano)

	time.Sleep(2 * time.Millisecond)
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice v2"}}))
	require.NoError(t, engine.DeleteEdge("ab"))
	require.NoError(t, engine.DeleteNode("b"))
	_, err = engine.CreateNode(&storage.Node{ID: "c", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ac", StartNode: "a", EndNode: "c", Type: "KNOWS"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids": []string{"a", "b", "c"},
		"as_of":    asOf,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	nodesByID := make(map[string]graphNodePayload, len(payload.Nodes))
	for _, node := range payload.Nodes {
		nodesByID[node.ID] = node
	}
	edgesByID := make(map[string]graphEdgePayload, len(payload.Edges))
	for _, edge := range payload.Edges {
		edgesByID[edge.ID] = edge
	}
	require.Equal(t, "changed", nodesByID["a"].Status)
	require.Equal(t, "added", nodesByID["b"].Status)
	require.Equal(t, "removed", nodesByID["c"].Status)
	require.Equal(t, "added", edgesByID["ab"].Status)
	require.Equal(t, "removed", edgesByID["ac"].Status)
	require.Equal(t, asOf, payload.Meta.AsOf)
	require.Equal(t, "current", payload.Meta.CompareTo)
}

func TestGraphNeighborhoodEndpoint_RespectsResolvedDatabaseReadAccess(t *testing.T) {
	server, authenticator := setupTestServer(t)
	readerToken := getAuthToken(t, authenticator, "reader")
	require.NotNil(t, server.allowlistStore)
	require.NotNil(t, server.privilegesStore)

	dbName := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{dbName}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", dbName, false, false))

	engine := getDefaultStorage(t, server)
	_, err := engine.CreateNode(&storage.Node{ID: "rbac-a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"rbac-a"},
	}, "Bearer "+readerToken)
	require.Equal(t, 403, resp.Code)

	var payload struct {
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	require.NotEmpty(t, payload.Errors)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", payload.Errors[0].Code)
	require.Contains(t, payload.Errors[0].Message, "not allowed")
}

func TestGraphEdgesForNode_RespectsContextCancellation(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := graphEdgesForNode(ctx, engine, storage.NodeID("any"))
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}

func TestGraphEdgesForNode_DedupesOutgoingAndIncoming(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "self", Labels: []string{"Node"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "self-loop", StartNode: "self", EndNode: "self", Type: "LOOPS"}))

	edges, err := graphEdgesForNode(context.Background(), engine, "self")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, storage.EdgeID("self-loop"), edges[0].ID)
}

func TestGraphExpandEndpoint_DelegatesToNeighborhood(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "KNOWS"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "expand"), map[string]interface{}{
		"node_ids": []string{"a"},
		"depth":    1,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)
	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, "node", payload.Meta.GeneratedFrom)
	require.Equal(t, 2, payload.Meta.NodeCount)
}

func TestGraphPathEndpoint_NotFoundReturns404(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}})
	require.NoError(t, err)

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "path"), map[string]interface{}{
		"source_node_id": "a",
		"target_node_id": "b",
	}, "Bearer "+token)
	require.Equal(t, 404, resp.Code)
}

func TestGraphPathEndpoint_RejectsAsOfWithDatabaseRouteHint(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "path"), map[string]interface{}{
		"source_node_id": "a",
		"target_node_id": "b",
		"as_of":          time.Now().UTC().Format(time.RFC3339Nano),
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
	require.Contains(t, resp.Body.String(), "/nornicdb/graph/{database}/path")
	require.Contains(t, resp.Body.String(), "/nornicdb/graph/{database}/temporal")
}

func TestGraphDiffEndpoint_WithCompareToUsesBaselineTimestamp(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{
		ID:         "a",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice v1"},
	})
	require.NoError(t, err)
	baseline := time.Now().UTC().Format(time.RFC3339Nano)

	time.Sleep(2 * time.Millisecond)
	require.NoError(t, engine.UpdateNode(&storage.Node{
		ID:         "a",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice v2"},
	}))
	target := time.Now().UTC().Format(time.RFC3339Nano)

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids":   []string{"a"},
		"as_of":      target,
		"compare_to": baseline,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)
	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, target, payload.Meta.AsOf)
	require.Equal(t, baseline, payload.Meta.CompareTo)
}

func TestWriteGraphResolveError_MapsSentinels(t *testing.T) {
	server, _ := setupTestServer(t)

	recNotFound := httptest.NewRecorder()
	server.writeGraphResolveError(recNotFound, multidb.ErrDatabaseNotFound)
	require.Equal(t, 404, recNotFound.Code)

	recOffline := httptest.NewRecorder()
	server.writeGraphResolveError(recOffline, multidb.ErrDatabaseOffline)
	require.Equal(t, 503, recOffline.Code)

	recBadRequest := httptest.NewRecorder()
	server.writeGraphResolveError(recBadRequest, ErrBadRequest)
	require.Equal(t, 400, recBadRequest.Code)
}

func TestGraphHelperParsersAndNormalizers(t *testing.T) {
	ids := normalizeGraphNodeIDs([]string{" a ", "", "a", "\t", "b", "b"})
	require.Equal(t, []string{"a", "b"}, ids)

	normalized := normalizeNodeIDs([]string{"b", " a ", "b", "", "a"})
	require.Equal(t, []string{"a", "b"}, normalized)

	unixVer, err := parseGraphVersion("1710000000")
	require.NoError(t, err)
	require.Equal(t, int64(1710000000), unixVer.CommitTimestamp.Unix())

	rfcVer, err := parseGraphVersion("2026-03-31T13:29:57Z")
	require.NoError(t, err)
	require.Equal(t, "2026-03-31T13:29:57Z", rfcVer.CommitTimestamp.Format(time.RFC3339))

	_, err = parseGraphVersion("not-a-date")
	require.Error(t, err)
}

func TestGraphHelperComparators(t *testing.T) {
	left := graphEdgePayload{
		ID:         "ab",
		Source:     "a",
		Target:     "b",
		Type:       "KNOWS",
		Semantic:   true,
		Properties: map[string]interface{}{"weight": 1},
	}
	right := graphEdgePayload{
		ID:         "ab",
		Source:     "a",
		Target:     "b",
		Type:       "KNOWS",
		Semantic:   true,
		Properties: map[string]interface{}{"weight": 1},
	}
	require.True(t, sameEdgePayload(left, right))

	right.Properties["weight"] = 2
	require.False(t, sameEdgePayload(left, right))
}

func TestGraphHandlers_MethodNotAllowed(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	for _, op := range []string{"neighborhood", "expand", "path", "temporal", "diff"} {
		resp := makeRequest(t, server, "GET", defaultGraphPath(server, op), nil, "Bearer "+token)
		require.Equal(t, 405, resp.Code)
	}
}

func TestGraphNeighborhoodEndpoint_RejectsAsOf(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"a"},
		"as_of":    time.Now().UTC().Format(time.RFC3339Nano),
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
	require.Contains(t, resp.Body.String(), "/nornicdb/graph/{database}/temporal")
	require.Contains(t, resp.Body.String(), "/nornicdb/graph/{database}/diff")
}

func TestGraphPathEndpoint_RequiresSourceAndTarget(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "path"), map[string]interface{}{
		"source_node_id": "a",
	}, "Bearer "+token)
	require.Equal(t, 400, resp.Code)
	require.Contains(t, resp.Body.String(), "source_node_id and target_node_id are required")
}

func TestGraphTemporalAndDiff_RequireAsOf(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	temporal := makeRequest(t, server, "POST", defaultGraphPath(server, "temporal"), map[string]interface{}{
		"node_ids": []string{"a"},
		"as_of":    "invalid",
	}, "Bearer "+token)
	require.Equal(t, 400, temporal.Code)
	require.Contains(t, temporal.Body.String(), "as_of must be a valid datetime")

	diff := makeRequest(t, server, "POST", defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids": []string{"a"},
	}, "Bearer "+token)
	require.Equal(t, 400, diff.Code)
	require.Contains(t, diff.Body.String(), "as_of is required")

	diffInvalidCompareTo := makeRequest(t, server, "POST", defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids":   []string{"a"},
		"as_of":      time.Now().UTC().Format(time.RFC3339Nano),
		"compare_to": "not-a-date",
	}, "Bearer "+token)
	require.Equal(t, 400, diffInvalidCompareTo.Code)
	require.Contains(t, diffInvalidCompareTo.Body.String(), "compare_to must be a valid datetime")
}

func TestGraphTemporalAndDiff_RejectExcessiveNodeIDs(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	nodeIDs := make([]string, 0, maxGraphTemporalDiffNodeIDs+1)
	for i := 0; i < maxGraphTemporalDiffNodeIDs+1; i++ {
		nodeIDs = append(nodeIDs, fmt.Sprintf("n-%d", i))
	}

	temporal := makeRequest(t, server, "POST", defaultGraphPath(server, "temporal"), map[string]interface{}{
		"node_ids": nodeIDs,
		"as_of":    time.Now().UTC().Format(time.RFC3339Nano),
	}, "Bearer "+token)
	require.Equal(t, 400, temporal.Code)
	require.Contains(t, temporal.Body.String(), "node_ids exceeds maximum")
	require.Contains(t, temporal.Body.String(), "temporal graph requests")

	diff := makeRequest(t, server, "POST", defaultGraphPath(server, "diff"), map[string]interface{}{
		"node_ids": nodeIDs,
		"as_of":    time.Now().UTC().Format(time.RFC3339Nano),
	}, "Bearer "+token)
	require.Equal(t, 400, diff.Code)
	require.Contains(t, diff.Body.String(), "node_ids exceeds maximum")
	require.Contains(t, diff.Body.String(), "diff graph requests")
}

func TestCollectLatestNeighborhood_CanceledContext(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)
	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = server.collectLatestNeighborhood(ctx, engine, []string{"a"}, 1, 10, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, context.Canceled)
}

func TestCollectLatestPath_SourceEqualsTargetBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)

	_, err := server.collectLatestPath(context.Background(), engine, "missing", "missing", 0, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, storage.ErrNotFound)

	_, err = engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}})
	require.NoError(t, err)
	collection, err := server.collectLatestPath(context.Background(), engine, "a", "a", 0, newGraphFilterSet(nil, nil))
	require.NoError(t, err)
	require.Len(t, collection.nodes, 1)
}

func TestCollectLatestInducedSubgraph_PropagatesOutgoingError(t *testing.T) {
	server, _ := setupTestServer(t)
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()
	_, err := base.CreateNode(&storage.Node{ID: "nornic:a", Labels: []string{"Node"}})
	require.NoError(t, err)

	engine := &graphOutgoingErrorEngine{Engine: base}
	_, err = server.collectLatestInducedSubgraph(engine, []string{"nornic:a"}, newGraphFilterSet(nil, nil))
	require.EqualError(t, err, "boom")
}

func TestCollectSnapshotInducedSubgraph_RequiresMVCCInterfaces(t *testing.T) {
	server, _ := setupTestServer(t)
	base := storage.NewMemoryEngine()
	defer func() { _ = base.Close() }()

	engine := &graphNoMVCCEngine{Engine: base}
	_, err := server.collectSnapshotInducedSubgraph(engine, []string{"a"}, storage.MVCCVersion{}, newGraphFilterSet(nil, nil))
	require.ErrorIs(t, err, storage.ErrNotImplemented)
}

func TestGraphFilterSetAndCollectionHelpers(t *testing.T) {
	filtered := newGraphFilterSet([]string{" Person ", ""}, []string{" KNOWS ", ""})
	require.False(t, filtered.allowNode(nil))
	require.False(t, filtered.allowNode(&storage.Node{ID: "x", Labels: []string{"Topic"}}))
	require.True(t, filtered.allowNode(&storage.Node{ID: "p", Labels: []string{"Person"}}))

	require.False(t, filtered.allowEdge(nil))
	require.False(t, filtered.allowEdge(&storage.Edge{ID: "e1", Type: "LIKES"}))
	require.True(t, filtered.allowEdge(&storage.Edge{ID: "e2", Type: "KNOWS"}))

	unfiltered := newGraphFilterSet(nil, nil)
	require.True(t, unfiltered.allowNode(&storage.Node{ID: "any"}))
	require.True(t, unfiltered.allowEdge(&storage.Edge{ID: "any"}))

	collection := newGraphCollection()
	collection.addNode(nil, "added")
	collection.addEdge(nil, "added")

	collection.addNode(&storage.Node{ID: "n1", Labels: []string{"L"}, Properties: map[string]interface{}{"k": "v1"}}, "")
	collection.addNode(&storage.Node{ID: "n1", Labels: []string{"L2"}, Properties: map[string]interface{}{"k": "v2"}}, "added")
	collection.addNode(&storage.Node{ID: "n1", Labels: []string{"L3"}, Properties: map[string]interface{}{"k": "v3"}}, "removed")
	require.Equal(t, "added", collection.nodes["n1"].Status)
	require.Equal(t, "v3", collection.nodes["n1"].Properties["k"])

	collection.addEdge(&storage.Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]interface{}{"k": "v1"}}, "")
	collection.addEdge(&storage.Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]interface{}{"k": "v2"}}, "added")
	collection.addEdge(&storage.Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]interface{}{"k": "v3"}}, "removed")
	require.Equal(t, "added", collection.edges["e1"].Status)
	require.Equal(t, "v3", collection.edges["e1"].Properties["k"])

	collection.truncated = true
	payload := collection.payload(graphMetaPayload{Database: "nornic", GeneratedFrom: "node"})
	require.Equal(t, 1, payload.Meta.NodeCount)
	require.Equal(t, 1, payload.Meta.EdgeCount)
	require.True(t, payload.Meta.Truncated)
}

func TestDiffGraphCollections_OmitsUnchangedEntries(t *testing.T) {
	baseline := newGraphCollection()
	target := newGraphCollection()

	baseline.nodes["same-node"] = graphNodePayload{
		ID:         "same-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	target.nodes["same-node"] = graphNodePayload{
		ID:         "same-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	baseline.nodes["changed-node"] = graphNodePayload{
		ID:         "changed-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Before"},
	}
	target.nodes["changed-node"] = graphNodePayload{
		ID:         "changed-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "After"},
	}

	baseline.edges["same-edge"] = graphEdgePayload{
		ID:         "same-edge",
		Source:     "a",
		Target:     "b",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"w": 1},
	}
	target.edges["same-edge"] = graphEdgePayload{
		ID:         "same-edge",
		Source:     "a",
		Target:     "b",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"w": 1},
	}
	baseline.edges["changed-edge"] = graphEdgePayload{
		ID:         "changed-edge",
		Source:     "a",
		Target:     "c",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"w": 1},
	}
	target.edges["changed-edge"] = graphEdgePayload{
		ID:         "changed-edge",
		Source:     "a",
		Target:     "c",
		Type:       "LIKES",
		Properties: map[string]interface{}{"w": 2},
	}

	diff := diffGraphCollections(baseline, target)

	_, hasSameNode := diff.nodes["same-node"]
	_, hasSameEdge := diff.edges["same-edge"]
	require.False(t, hasSameNode)
	require.False(t, hasSameEdge)

	require.Equal(t, "changed", diff.nodes["changed-node"].Status)
	require.Equal(t, "changed", diff.edges["changed-edge"].Status)
}
