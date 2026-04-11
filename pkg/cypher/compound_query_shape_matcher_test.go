package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchCompoundQueryShape_CreateDeleteRel(t *testing.T) {
	match, ok := matchCompoundQueryShape("MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r")
	require.True(t, ok)
	require.Equal(t, shapeKindCompoundCreateDeleteRel, match.Kind)
	require.True(t, match.Probe.Matched)
	require.Equal(t, "Actor", match.Captures.String("label1"))
	require.Equal(t, "Movie", match.Captures.String("label2"))
	require.Equal(t, "r", match.Captures.String("rel_var"))
	require.Equal(t, "TEMP_REL", match.Captures.String("rel_type"))
	require.Equal(t, "r", match.Captures.String("delete_var"))
	require.Equal(t, 1, match.Captures.Int("limit"))
}

func TestMatchCompoundQueryShape_PropCreateDeleteRel(t *testing.T) {
	match, ok := matchCompoundQueryShape("MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r")
	require.True(t, ok)
	require.Equal(t, shapeKindCompoundPropCreateDeleteRel, match.Kind)
	require.True(t, match.Probe.Matched)
	require.Equal(t, "Person", match.Captures.String("label1"))
	require.Equal(t, "id", match.Captures.String("prop1"))
	require.Equal(t, "1", match.Captures.String("value1"))
	require.Equal(t, "2", match.Captures.String("value2"))
	require.Equal(t, "TEMP_KNOWS", match.Captures.String("rel_type"))
}

func TestMatchCompoundQueryShape_PropCreateDeleteReturnCountRel(t *testing.T) {
	match, ok := matchCompoundQueryShape("MATCH (s:Supplier {supplierID: 1}), (p:Product {productID: 2}) CREATE (s)-[r:TEST_REL]->(p) WITH r DELETE r RETURN count(r)")
	require.True(t, ok)
	require.Equal(t, shapeKindCompoundPropCreateDeleteReturnCountRel, match.Kind)
	require.True(t, match.Probe.Matched)
	require.Equal(t, "r", match.Captures.String("rel_var"))
	require.Equal(t, "r", match.Captures.String("with_var"))
	require.Equal(t, "r", match.Captures.String("delete_var"))
	require.Equal(t, "r", match.Captures.String("count_var"))
}

func TestMatchCompoundQueryShape_RejectsBadDeleteVar(t *testing.T) {
	match, ok := matchCompoundPropCreateDeleteReturnCountRelShape("MATCH (s:Supplier {supplierID: 1}), (p:Product {productID: 2}) CREATE (s)-[r:TEST_REL]->(p) WITH r DELETE x RETURN count(r)")
	require.False(t, ok)
	require.Equal(t, shapeKindUnknown, match.Kind)
	require.False(t, match.Probe.Matched)
	require.Contains(t, match.Probe.RejectReason, "variable mismatch")
}
