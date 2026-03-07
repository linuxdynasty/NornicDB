package dbconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ============================================================================
// ParseDuration
// ============================================================================

func TestParseDuration_Valid(t *testing.T) {
	assert.Equal(t, 5*time.Minute, ParseDuration("5m"))
	assert.Equal(t, 30*time.Second, ParseDuration("30s"))
	assert.Equal(t, 2*time.Hour, ParseDuration("2h"))
}

func TestParseDuration_Invalid(t *testing.T) {
	assert.Equal(t, time.Duration(0), ParseDuration("notaduration"))
	assert.Equal(t, time.Duration(0), ParseDuration(""))
}

func TestParseDuration_WithSpaces(t *testing.T) {
	assert.Equal(t, 10*time.Minute, ParseDuration("  10m  "))
}

// ============================================================================
// normalizeBM25Engine
// ============================================================================

func TestNormalizeBM25Engine_V1(t *testing.T) {
	assert.Equal(t, "v1", normalizeBM25Engine("v1"))
	assert.Equal(t, "v1", normalizeBM25Engine("V1"))
	assert.Equal(t, "v1", normalizeBM25Engine("  v1  "))
}

func TestNormalizeBM25Engine_Default(t *testing.T) {
	assert.Equal(t, "v2", normalizeBM25Engine("v2"))
	assert.Equal(t, "v2", normalizeBM25Engine("anything"))
	assert.Equal(t, "v2", normalizeBM25Engine(""))
}

// ============================================================================
// dbNameFromNodeID
// ============================================================================

func TestDbNameFromNodeID_WithPrefix(t *testing.T) {
	result := dbNameFromNodeID(dbConfigPrefix + "mydb")
	assert.Equal(t, "mydb", result)
}

func TestDbNameFromNodeID_WithoutPrefix(t *testing.T) {
	result := dbNameFromNodeID("nornic:node-xyz")
	assert.Equal(t, "", result)
}

func TestDbNameFromNodeID_Empty(t *testing.T) {
	result := dbNameFromNodeID("")
	assert.Equal(t, "", result)
}

// ============================================================================
// IsAllowedKey
// ============================================================================

func TestIsAllowedKey_Allowed(t *testing.T) {
	// NORNICDB_EMBEDDING_DIMENSIONS and NORNICDB_SEARCH_MIN_SIMILARITY are known allowed keys
	assert.True(t, IsAllowedKey("NORNICDB_EMBEDDING_DIMENSIONS"))
	assert.True(t, IsAllowedKey("NORNICDB_SEARCH_MIN_SIMILARITY"))
}

func TestIsAllowedKey_NotAllowed(t *testing.T) {
	assert.False(t, IsAllowedKey("RANDOM_UNKNOWN_KEY"))
	assert.False(t, IsAllowedKey(""))
}

// ============================================================================
// overridesFromProperties
// ============================================================================

func TestOverridesFromProperties_Nil(t *testing.T) {
	result := overridesFromProperties(nil)
	assert.Nil(t, result)
}

func TestOverridesFromProperties_NoOverridesKey(t *testing.T) {
	result := overridesFromProperties(map[string]any{"other": "val"})
	assert.Nil(t, result)
}

func TestOverridesFromProperties_WrongType(t *testing.T) {
	result := overridesFromProperties(map[string]any{"overrides": 42})
	assert.Nil(t, result)
}

func TestOverridesFromProperties_InvalidJSON(t *testing.T) {
	result := overridesFromProperties(map[string]any{"overrides": "{not-json"})
	assert.Nil(t, result)
}

func TestIsAllowedKey_Excluded(t *testing.T) {
	KeysExcludedFromPerDB["NORNICDB_EMBEDDING_MODEL"] = true
	t.Cleanup(func() {
		delete(KeysExcludedFromPerDB, "NORNICDB_EMBEDDING_MODEL")
	})

	assert.False(t, IsAllowedKey("NORNICDB_EMBEDDING_MODEL"))
}
