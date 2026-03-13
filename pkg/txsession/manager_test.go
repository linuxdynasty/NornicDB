package txsession

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func newExecutorFactory(t *testing.T) ExecutorFactory {
	t.Helper()
	return func(_ string) (*cypher.StorageExecutor, error) {
		store := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = store.Close() })
		return cypher.NewStorageExecutor(store), nil
	}
}

func TestManagerOpenErrors(t *testing.T) {
	mgr := NewManager(time.Second, nil)
	if _, err := mgr.Open(context.Background(), "neo4j"); err == nil {
		t.Fatalf("expected error when factory is nil")
	}

	mgr = NewManager(time.Second, func(_ string) (*cypher.StorageExecutor, error) {
		return nil, fmt.Errorf("factory failed")
	})
	if _, err := mgr.Open(context.Background(), "neo4j"); err == nil {
		t.Fatalf("expected factory error")
	}
}

func TestManagerLifecycle_ExecuteCommitAndDelete(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))
	baseTime := time.Unix(1700000000, 0)
	mgr.idFunc = func() string { return "tx-1" }
	mgr.nowFunc = func() time.Time { return baseTime }

	session, err := mgr.Open(context.Background(), "neo4j")
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if session.ID != "tx-1" {
		t.Fatalf("unexpected session id: %s", session.ID)
	}
	if !session.Expires.Equal(baseTime.Add(time.Second)) {
		t.Fatalf("unexpected expiry: %v", session.Expires)
	}

	if _, ok := mgr.Get("tx-1"); !ok {
		t.Fatalf("expected session to be retrievable")
	}

	result, err := mgr.ExecuteInSession(context.Background(), session, "RETURN 1 AS one", nil)
	if err != nil {
		t.Fatalf("execute in session failed: %v", err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(1) {
		t.Fatalf("unexpected execute result in session: %#v", result.Rows)
	}

	mgr.nowFunc = func() time.Time { return baseTime.Add(5 * time.Second) }
	mgr.Touch(session)
	if !session.Expires.Equal(baseTime.Add(6 * time.Second)) {
		t.Fatalf("touch should refresh expiration, got %v", session.Expires)
	}

	if _, err := mgr.CommitAndDelete(context.Background(), session); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if _, ok := mgr.Get("tx-1"); ok {
		t.Fatalf("expected session deleted after commit")
	}

	mgr.Delete("tx-1") // no-op path
}

func TestManagerLifecycle_RollbackAndErrorGuards(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))
	session, err := mgr.Open(context.Background(), "neo4j")
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	if _, err := mgr.ExecuteInSession(context.Background(), nil, "RETURN 1", nil); err == nil {
		t.Fatalf("expected nil session execute to fail")
	}
	if _, err := mgr.CommitAndDelete(context.Background(), nil); err == nil {
		t.Fatalf("expected nil session commit to fail")
	}
	if err := mgr.RollbackAndDelete(context.Background(), nil); err == nil {
		t.Fatalf("expected nil session rollback to fail")
	}

	if _, err := mgr.ExecuteInSession(context.Background(), session, "RETURN 2 AS two", nil); err != nil {
		t.Fatalf("execute in session failed: %v", err)
	}

	if err := mgr.RollbackAndDelete(context.Background(), session); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if _, ok := mgr.Get(session.ID); ok {
		t.Fatalf("expected session deleted after rollback")
	}
}

func TestManagerOpenWithExecutor(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))

	store := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = store.Close() })
	exec := cypher.NewStorageExecutor(store)

	session, err := mgr.OpenWithExecutor(context.Background(), "neo4j", exec)
	if err != nil {
		t.Fatalf("open with executor failed: %v", err)
	}
	if session == nil || session.Executor == nil {
		t.Fatalf("expected non-nil session and executor")
	}
	if session.Database != "neo4j" {
		t.Fatalf("unexpected session database: %s", session.Database)
	}
	if _, ok := mgr.Get(session.ID); !ok {
		t.Fatalf("expected session to be tracked")
	}

	if err := mgr.RollbackAndDelete(context.Background(), session); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if _, ok := mgr.Get(session.ID); ok {
		t.Fatalf("expected session deleted after rollback")
	}
}

func TestManagerOpenWithExecutorErrors(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))

	if _, err := mgr.OpenWithExecutor(context.Background(), "neo4j", nil); err == nil {
		t.Fatalf("expected nil executor error")
	}
}

func TestManagerNewManagerDefaultTTLAndTouchNil(t *testing.T) {
	mgr := NewManager(0, newExecutorFactory(t))
	if mgr.ttl != 30*time.Second {
		t.Fatalf("expected default ttl 30s, got %v", mgr.ttl)
	}
	// No panic path.
	mgr.Touch(nil)
}

func TestManagerOpenWithExecutorBeginFailure(t *testing.T) {
	mgr := NewManager(time.Second, newExecutorFactory(t))

	// CompositeEngine does not support explicit transactions (BEGIN).
	c := storage.NewCompositeEngine(
		map[string]storage.Engine{"a": storage.NewMemoryEngine()},
		map[string]string{"a": "a"},
		map[string]string{"a": "read_write"},
	)
	exec := cypher.NewStorageExecutor(c)

	if _, err := mgr.OpenWithExecutor(context.Background(), "neo4j", exec); err == nil {
		t.Fatalf("expected begin failure for non-transactional engine")
	}
}
