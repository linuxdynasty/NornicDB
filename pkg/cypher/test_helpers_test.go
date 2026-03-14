package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// newTestMemoryEngine creates an in-memory storage engine that is automatically
// closed when the test completes. This prevents Badger background goroutines
// (compaction, GC) from racing with subsequent tests.
func newTestMemoryEngine(t testing.TB) *storage.MemoryEngine {
	t.Helper()
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}
