package nornicdb

import (
	"testing"
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestClusteringTimer_StopDoesNotPanic(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Memory.KmeansClusterInterval = 5 * time.Millisecond
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	db.startClusteringTimer(5 * time.Millisecond)

	time.Sleep(10 * time.Millisecond)
	db.stopClusteringTimer()
	db.stopClusteringTimer() // idempotent

	// Give the goroutine a moment to observe stop.
	time.Sleep(10 * time.Millisecond)
}
