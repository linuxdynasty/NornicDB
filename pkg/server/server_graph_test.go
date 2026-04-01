package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, "removed", nodesByID["b"].Status)
	require.Equal(t, "added", nodesByID["c"].Status)
	require.Equal(t, "removed", edgesByID["ab"].Status)
	require.Equal(t, "added", edgesByID["ac"].Status)
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
