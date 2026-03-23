package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkCallSubqueryUnion_FixedCost_CacheMiss profiles the fixed execution
// overhead of CALL { ... UNION ALL ... } with a cache-busted query per iteration.
func BenchmarkCallSubqueryUnion_FixedCost_CacheMiss(b *testing.B) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "sp-1",
		Labels: []string{"SystemPrompt"},
		Properties: map[string]interface{}{
			"promptId": "prompt-id",
			"text":     "system text",
		},
	})
	if err != nil {
		b.Fatalf("seed create failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Cache-bust to force execution path profiling instead of query-cache hits.
		query := fmt.Sprintf(`
MATCH (p:SystemPrompt {promptId: "prompt-id"})
WITH p
CALL {
  WITH p
  RETURN 0 AS sortOrder, 'SYSTEM_PROMPT' AS rowType, p.text AS systemPrompt, null AS originalText, null AS score, null AS language, null AS translatedText
  UNION ALL
  WITH p
  RETURN 1 AS sortOrder, 'CANDIDATE' AS rowType, null AS systemPrompt, 'x-%d' AS originalText, 1.0 AS score, 'en' AS language, 'x' AS translatedText
}
RETURN rowType, systemPrompt, originalText, score, language, translatedText
ORDER BY sortOrder, score DESC, language
LIMIT 6`, i)
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			b.Fatalf("execute failed: %v", err)
		}
	}
}
