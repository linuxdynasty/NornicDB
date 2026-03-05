package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func countFromResult(t *testing.T, result *ExecuteResult) int64 {
	t.Helper()
	if result == nil || len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		t.Fatalf("missing count result: %+v", result)
	}
	switch v := result.Rows[0][0].(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		t.Fatalf("unexpected count type %T (%v)", v, v)
		return 0
	}
}

func TestExplicitTransaction_NamespacedCreateCommit(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	if _, err := exec.Execute(ctx, "CREATE (n:TxNs {name: 'commit'})", nil); err != nil {
		t.Fatalf("CREATE in tx failed: %v", err)
	}
	if _, err := exec.Execute(ctx, "COMMIT", nil); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:TxNs {name: 'commit'}) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatalf("verification query failed: %v", err)
	}
	if got := countFromResult(t, result); got != 1 {
		t.Fatalf("expected 1 committed node, got %d", got)
	}
}

func TestExplicitTransaction_NamespacedCreateRollback(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	if _, err := exec.Execute(ctx, "CREATE (n:TxNs {name: 'rollback'})", nil); err != nil {
		t.Fatalf("CREATE in tx failed: %v", err)
	}
	if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:TxNs {name: 'rollback'}) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatalf("verification query failed: %v", err)
	}
	if got := countFromResult(t, result); got != 0 {
		t.Fatalf("expected 0 rolled-back nodes, got %d", got)
	}
}
