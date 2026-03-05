package decay

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_GetConfig(t *testing.T) {
	config := &Config{
		RecalculateInterval:           0,
		ArchiveThreshold:              0.07,
		RecencyWeight:                 0.5,
		FrequencyWeight:               0.3,
		ImportanceWeight:              0.2,
		PromotionEnabled:              true,
		EpisodicToSemanticThreshold:   8,
		SemanticToProceduralThreshold: 40,
	}
	manager := New(config)
	require.NotNil(t, manager)

	got := manager.GetConfig()
	require.NotNil(t, got)

	// Should return a copy with the same values
	assert.Equal(t, 0.07, got.ArchiveThreshold)
	assert.True(t, got.PromotionEnabled)
	assert.Equal(t, int64(8), got.EpisodicToSemanticThreshold)
	assert.Equal(t, int64(40), got.SemanticToProceduralThreshold)

	// Modifying the copy should not affect the original
	got.ArchiveThreshold = 0.99
	assert.NotEqual(t, 0.99, manager.config.ArchiveThreshold)
}

func TestManager_GetConfig_DefaultConfig(t *testing.T) {
	manager := New(nil)
	got := manager.GetConfig()
	require.NotNil(t, got)
	assert.Equal(t, DefaultConfig().ArchiveThreshold, got.ArchiveThreshold)
	assert.Equal(t, DefaultConfig().RecencyWeight, got.RecencyWeight)
}
