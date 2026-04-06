package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// DELETE Query Routing Tests
// =============================================================================
// These tests ensure DELETE queries are routed to executeDelete, not executeMatch.
// Bug history: DELETE was incorrectly going to executeMatch because:
// 1. Old code checked for " DELETE " (with trailing space) but DELETE is often
//    followed by variable name (e.g., "DELETE n" not "DELETE ")
// 2. Relationship deletion wasn't being detected properly in executeDelete
// =============================================================================

// TestDeleteRouting_SimpleNode tests basic node deletion routing
func TestDeleteRouting_SimpleNode(t *testing.T) {
	query := "MATCH (n:Person) DELETE n"

	hasDelete := findKeywordIndex(query, "DELETE") > 0
	if !hasDelete {
		t.Errorf("DELETE routing failed for query: %s", query)
	}
}

// TestDeleteRouting_WithProperty tests deletion with property filter containing "Delete"
func TestDeleteRouting_WithProperty(t *testing.T) {
	// The word 'ToDelete' is inside a string literal and should be ignored
	query := "MATCH (n:Temp {name: 'ToDelete'}) DELETE n"

	deleteIdx := findKeywordIndex(query, "DELETE")

	// DELETE should be found OUTSIDE the string literal (after position 30)
	if deleteIdx <= 30 {
		t.Errorf("DELETE keyword found inside string literal at index %d, should be > 30", deleteIdx)
	}

	// Verify routing check passes
	hasDelete := deleteIdx > 0
	if !hasDelete {
		t.Errorf("DELETE routing failed for query: %s", query)
	}
}

// TestDeleteRouting_Relationship tests relationship deletion routing
func TestDeleteRouting_Relationship(t *testing.T) {
	query := "MATCH ()-[r:KNOWS]->() DELETE r"

	hasDelete := findKeywordIndex(query, "DELETE") > 0
	if !hasDelete {
		t.Errorf("DELETE routing failed for relationship query: %s", query)
	}
}

// TestDeleteRouting_DetachDelete tests DETACH DELETE routing
func TestDeleteRouting_DetachDelete(t *testing.T) {
	query := "MATCH (n:Person) DETACH DELETE n"

	hasDetachDelete := containsKeywordOutsideStrings(query, "DETACH DELETE")
	if !hasDetachDelete {
		t.Errorf("DETACH DELETE routing failed for query: %s", query)
	}
}

// TestDeleteRouting_MultipleVariables tests deletion of multiple variables
func TestDeleteRouting_MultipleVariables(t *testing.T) {
	query := "MATCH (a)-[r]->(b) DELETE a, r, b"

	hasDelete := findKeywordIndex(query, "DELETE") > 0
	if !hasDelete {
		t.Errorf("DELETE routing failed for multi-variable query: %s", query)
	}
}

// TestDeleteRouting_WithWhere tests DELETE with WHERE clause
func TestDeleteRouting_WithWhere(t *testing.T) {
	query := "MATCH (n:Person) WHERE n.age > 100 DELETE n"

	hasDelete := findKeywordIndex(query, "DELETE") > 0
	if !hasDelete {
		t.Errorf("DELETE routing failed for WHERE clause query: %s", query)
	}
}

// TestDeleteRouting_NotConfusedByStringContent ensures DELETE inside strings is ignored
func TestDeleteRouting_NotConfusedByStringContent(t *testing.T) {
	testCases := []struct {
		name  string
		query string
	}{
		{"action property", "MATCH (n {action: 'DELETE'}) DELETE n"},
		{"delete in name", "MATCH (n {name: 'DeleteMe'}) DELETE n"},
		{"delete substring", "MATCH (n:ToDelete) DELETE n"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// The DELETE keyword for the actual delete operation should be found
			// OUTSIDE any string literals
			deleteIdx := findKeywordIndex(tc.query, "DELETE")
			if deleteIdx <= 0 {
				t.Errorf("DELETE keyword not found in: %s", tc.query)
			}

			// Verify it's the correct DELETE (the operation, not string content)
			// by checking it appears after the closing paren
			closeParenIdx := strings.LastIndex(tc.query, ")")
			if deleteIdx < closeParenIdx {
				t.Errorf("DELETE found too early (idx=%d, closeParen=%d) - may be inside string: %s",
					deleteIdx, closeParenIdx, tc.query)
			}
		})
	}
}

// TestExecuteWithoutTransaction_DeleteRouting verifies the exact routing logic
func TestExecuteWithoutTransaction_DeleteRouting(t *testing.T) {
	testCases := []struct {
		name         string
		query        string
		shouldDelete bool
	}{
		{"simple node delete", "MATCH (n) DELETE n", true},
		{"node with label", "MATCH (n:Person) DELETE n", true},
		{"relationship delete", "MATCH ()-[r]->() DELETE r", true},
		{"detach delete", "MATCH (n) DETACH DELETE n", true},
		{"with where", "MATCH (n) WHERE n.x = 1 DELETE n", true},
		{"string with delete", "MATCH (n {x: 'DELETE'}) DELETE n", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hasDelete := findKeywordIndex(tc.query, "DELETE") > 0
			hasDetachDelete := containsKeywordOutsideStrings(tc.query, "DETACH DELETE")

			wouldRouteToDelete := hasDelete || hasDetachDelete

			if wouldRouteToDelete != tc.shouldDelete {
				t.Errorf("Routing mismatch for %q: got wouldRouteToDelete=%v, want %v",
					tc.query, wouldRouteToDelete, tc.shouldDelete)
			}
		})
	}
}

func TestContainsOutsideStrings_Branches(t *testing.T) {
	assert.False(t, containsOutsideStrings("MATCH (n)", ""))
	assert.True(t, containsOutsideStrings("MATCH (a)-[:R]->(b)", "->"))

	// Inside single quotes should be ignored.
	assert.False(t, containsOutsideStrings("MATCH (n {txt:'a->b'})", "->"))
	// Escaped quotes branch.
	assert.False(t, containsOutsideStrings("MATCH (n {txt:'a\\'->\\'b'})", "->"))
	// Doubled single-quote escape branch.
	assert.False(t, containsOutsideStrings("MATCH (n {txt:'a''->''b'})", "->"))

	// Inside double quotes should be ignored.
	assert.False(t, containsOutsideStrings("MATCH (n {txt:\"a->b\"})", "->"))
	// Backtick identifier branch.
	assert.False(t, containsOutsideStrings("MATCH (`a->b`)-[:R]->(n)", "a->b"))

	// Line comment branch.
	assert.False(t, containsOutsideStrings("MATCH (n) // -> in comment\nRETURN n", "->"))
	// Block comment branch.
	assert.False(t, containsOutsideStrings("MATCH (n) /* -> in comment */ RETURN n", "->"))
}

func TestRouting_ExactTranslationQuery_ExecutesViaMatchPath(t *testing.T) {
	const cypher = "MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) WHERE t.language = 'fr' RETURN o, t, t.createdAt ORDER BY t.createdAt DESC LIMIT 10"

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 12; i++ {
		originalID := storage.NodeID(fmt.Sprintf("test:o%d", i))
		translatedID := storage.NodeID(fmt.Sprintf("test:tfr%d", i))
		createdAt := fmt.Sprintf("2026-04-%02dT12:00:00Z", i+1)

		_, err := store.CreateNode(&storage.Node{
			ID:     originalID,
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"textKey": fmt.Sprintf("k%d", i),
			},
		})
		require.NoError(t, err)

		_, err = store.CreateNode(&storage.Node{
			ID:     translatedID,
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"language":  "fr",
				"createdAt": createdAt,
			},
		})
		require.NoError(t, err)

		err = store.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("test:e%d", i)),
			Type:      "TRANSLATES_TO",
			StartNode: originalID,
			EndNode:   translatedID,
		})
		require.NoError(t, err)
	}

	_, err := store.CreateNode(&storage.Node{
		ID:     "test:es-original",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey": "es",
		},
	})
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     "test:es-translation",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"language":  "es",
			"createdAt": "2026-05-01T12:00:00Z",
		},
	})
	require.NoError(t, err)

	err = store.CreateEdge(&storage.Edge{
		ID:        "test:es-edge",
		Type:      "TRANSLATES_TO",
		StartNode: "test:es-original",
		EndNode:   "test:es-translation",
	})
	require.NoError(t, err)

	result, err := exec.executeWithoutTransaction(ctx, cypher, strings.ToUpper(cypher))
	require.NoError(t, err)
	require.Len(t, result.Rows, 10)
	require.Equal(t, []string{"o", "t", "t.createdAt"}, result.Columns)

	for i, row := range result.Rows {
		require.Len(t, row, 3)

		original, ok := row[0].(*storage.Node)
		require.True(t, ok)
		require.Contains(t, original.Labels, "OriginalText")

		translated, ok := row[1].(*storage.Node)
		require.True(t, ok)
		require.Contains(t, translated.Labels, "TranslatedText")
		require.Equal(t, "fr", translated.Properties["language"])

		createdAt, ok := row[2].(string)
		require.True(t, ok)
		require.Equal(t, translated.Properties["createdAt"], createdAt)

		if i > 0 {
			prev := result.Rows[i-1][2].(string)
			require.GreaterOrEqual(t, prev, createdAt)
		}
	}

	require.Equal(t, "2026-04-12T12:00:00Z", result.Rows[0][2])
	require.Equal(t, "2026-04-03T12:00:00Z", result.Rows[9][2])
}
