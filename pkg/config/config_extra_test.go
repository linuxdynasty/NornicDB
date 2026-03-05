package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// FeatureFlagsConfig Heimdall getters
// ============================================================================

func TestFeatureFlagsConfig_HeimdallGetters(t *testing.T) {
	f := &FeatureFlagsConfig{
		HeimdallEnabled:  true,
		HeimdallModel:    "llama3",
		HeimdallProvider: "ollama",
		HeimdallAPIURL:   "http://localhost:11434",
		HeimdallAPIKey:   "my-secret-key",
	}
	assert.True(t, f.GetHeimdallEnabled())
	assert.Equal(t, "llama3", f.GetHeimdallModel())
	assert.Equal(t, "ollama", f.GetHeimdallProvider())
	assert.Equal(t, "http://localhost:11434", f.GetHeimdallAPIURL())
	assert.Equal(t, "my-secret-key", f.GetHeimdallAPIKey())
}

func TestFeatureFlagsConfig_HeimdallGetters_Defaults(t *testing.T) {
	f := &FeatureFlagsConfig{}
	assert.False(t, f.GetHeimdallEnabled())
	assert.Equal(t, "", f.GetHeimdallModel())
	assert.Equal(t, "", f.GetHeimdallProvider())
	assert.Equal(t, "", f.GetHeimdallAPIURL())
	assert.Equal(t, "", f.GetHeimdallAPIKey())
}

// ============================================================================
// FindConfigFile – env override path
// ============================================================================

func TestFindConfigFile_EnvOverride(t *testing.T) {
	// Create a temp file to act as a real config
	tmp, err := os.CreateTemp("", "nornicdb-config-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmp.Name())
	tmp.Close()

	t.Setenv("NORNICDB_CONFIG", tmp.Name())
	result := FindConfigFile()
	assert.Equal(t, tmp.Name(), result)
}

func TestFindConfigFile_NoEnv_ReturnsString(t *testing.T) {
	// Without a real file, FindConfigFile returns "" or a candidate path
	os.Unsetenv("NORNICDB_CONFIG")
	result := FindConfigFile()
	// Just ensure it doesn't panic and returns a string
	assert.IsType(t, "", result)
}

// ============================================================================
// ApplyEnvVars – basic smoke test
// ============================================================================

func TestApplyEnvVars_NoEnvVars(t *testing.T) {
	cfg := LoadDefaults()
	err := ApplyEnvVars(cfg)
	assert.NoError(t, err)
}

func TestApplyEnvVars_WithSomeEnvVars(t *testing.T) {
	cfg := LoadDefaults()
	t.Setenv("NORNICDB_PORT", "7688")
	t.Setenv("NORNICDB_HOST", "testhost")
	err := ApplyEnvVars(cfg)
	assert.NoError(t, err)
}

// ============================================================================
// GetParserType – feature_flags.go
// ============================================================================

func TestGetParserType_Default(t *testing.T) {
	result := GetParserType()
	assert.IsType(t, "", result)
}
