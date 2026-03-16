package fabric

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestCacheKeyIncludesAuthContext(t *testing.T) {
	loc := &LocationRemote{URI: "http://remote.example", DBName: "tenant", AuthMode: "oidc_forwarding"}

	k1 := cacheKey(loc, "Bearer token-a")
	k2 := cacheKey(loc, "Bearer token-b")
	if k1 == k2 {
		t.Fatalf("expected distinct cache keys for different forwarded auth tokens")
	}

	userPassLoc := &LocationRemote{URI: "http://remote.example", DBName: "tenant", AuthMode: "user_password", User: "alice", Password: "secret"}
	k3 := cacheKey(userPassLoc, "Bearer ignored")
	k4 := cacheKey(userPassLoc, "Bearer also-ignored")
	if k3 != k4 {
		t.Fatalf("expected user/password auth cache key to ignore forwarded token")
	}
}

func TestRemoteFragmentExecutorCacheIsConcurrentAndAuthIsolated(t *testing.T) {
	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()

	loc := &LocationRemote{URI: "http://remote.example", DBName: "tenant", AuthMode: "oidc_forwarding"}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if _, err := re.getOrCreateEngine(loc, "Bearer same-token"); err != nil {
				t.Errorf("getOrCreateEngine failed: %v", err)
			}
		}()
	}
	wg.Wait()

	re.mu.RLock()
	if got := len(re.engineCache); got != 1 {
		re.mu.RUnlock()
		t.Fatalf("expected single cached engine for same auth context, got %d", got)
	}
	re.mu.RUnlock()

	if _, err := re.getOrCreateEngine(loc, "Bearer other-token"); err != nil {
		t.Fatalf("getOrCreateEngine failed for second auth context: %v", err)
	}

	re.mu.RLock()
	if got := len(re.engineCache); got != 2 {
		re.mu.RUnlock()
		t.Fatalf("expected auth-isolated cache entries, got %d", got)
	}
	re.mu.RUnlock()
}

func TestRemoteFragmentExecutor_ExplicitTxHandle_CommitLifecycle(t *testing.T) {
	var (
		openCount     int
		execCount     int
		commitCount   int
		rollbackCount int
	)
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant/tx"):
			openCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{},
				"errors":  []any{},
				"commit":  srv.URL + "/db/tenant/tx/1/commit",
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant/tx/1"):
			execCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{
					map[string]any{
						"columns": []string{"n"},
						"data":    []any{map[string]any{"row": []any{1}}},
					},
				},
				"errors": []any{},
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant/tx/1/commit"):
			commitCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/db/tenant/tx/1"):
			rollbackCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()

	loc := &LocationRemote{URI: srv.URL, DBName: "tenant", AuthMode: "oidc_forwarding"}
	tx := NewFabricTransaction("tx-remote-commit")
	participant := participantKeyFromLocation(loc)
	sub, err := tx.GetOrOpen(participant, true)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}
	ctx := WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)

	_, err = re.Execute(ctx, loc, "RETURN 1 AS n", nil, "Bearer tok")
	if err != nil {
		t.Fatalf("remote execute failed: %v", err)
	}
	if err := tx.Commit(nil, nil); err != nil {
		t.Fatalf("fabric commit failed: %v", err)
	}

	if openCount != 1 || execCount != 1 || commitCount != 1 || rollbackCount != 0 {
		t.Fatalf("unexpected lifecycle counts open=%d exec=%d commit=%d rollback=%d", openCount, execCount, commitCount, rollbackCount)
	}
}

func TestRemoteFragmentExecutor_UserPasswordAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []any{
				map[string]any{
					"columns": []string{"x"},
					"data":    []any{map[string]any{"row": []any{1}}},
				},
			},
			"errors": []any{},
		})
	}))
	defer srv.Close()

	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()

	loc := &LocationRemote{URI: srv.URL, DBName: "tenant", AuthMode: "user_password", User: "alice", Password: "secret"}
	res, err := re.Execute(context.Background(), loc, "RETURN 1 AS x", nil, "ignored-token")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
}

func TestRemoteFragmentExecutor_ExecuteError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []any{},
			"errors":  []any{map[string]any{"message": "query failed"}},
		})
	}))
	defer srv.Close()

	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()

	loc := &LocationRemote{URI: srv.URL, DBName: "tenant", AuthMode: "oidc_forwarding"}
	_, err := re.Execute(context.Background(), loc, "INVALID", nil, "tok")
	if err == nil {
		t.Fatal("expected error from failed remote query")
	}
	if !strings.Contains(err.Error(), "remote execution") {
		t.Fatalf("expected 'remote execution' in error, got: %v", err)
	}
}

func TestRemoteFragmentExecutor_CloseWithTxHandles(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant/tx"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{},
				"errors":  []any{},
				"commit":  srv.URL + "/db/tenant/tx/99/commit",
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant/tx/99"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{
					map[string]any{
						"columns": []string{"x"},
						"data":    []any{map[string]any{"row": []any{1}}},
					},
				},
				"errors": []any{},
			})
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/db/tenant/tx/99"):
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant/tx/99/commit"):
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	re := NewRemoteFragmentExecutor()
	loc := &LocationRemote{URI: srv.URL, DBName: "tenant", AuthMode: "oidc_forwarding"}
	tx := NewFabricTransaction("tx-close-test")
	participant := participantKeyFromLocation(loc)
	sub, _ := tx.GetOrOpen(participant, true)
	ctx := WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)

	_, err := re.Execute(ctx, loc, "RETURN 1 AS x", nil, "tok")
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	// Close should rollback open tx handles and close engines.
	err = re.Close()
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestRemoteFragmentExecutor_GetOrCreateTxHandle_MissingFabricTx(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant/tx"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{},
				"errors":  []any{},
				"commit":  srv.URL + "/db/tenant/tx/5/commit",
			})
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/db/tenant/tx/5"):
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()

	loc := &LocationRemote{URI: srv.URL, DBName: "tenant", AuthMode: "oidc_forwarding"}
	sub := &SubTransaction{ShardName: "test-shard", State: "open"}
	// Context with SubTransaction but WITHOUT FabricTransaction → should error.
	ctx := WithSubTransaction(context.Background(), sub)

	_, err := re.Execute(ctx, loc, "RETURN 1 AS x", nil, "tok")
	if err == nil {
		t.Fatal("expected error for missing fabric transaction context")
	}
	if !strings.Contains(err.Error(), "fabric transaction context is missing") {
		t.Fatalf("expected missing fabric tx context error, got: %v", err)
	}
}

func TestRemoteFragmentExecutor_ExplicitTxHandle_RollbackLifecycle(t *testing.T) {
	var (
		openCount     int
		execCount     int
		commitCount   int
		rollbackCount int
	)
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant/tx"):
			openCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{},
				"errors":  []any{},
				"commit":  srv.URL + "/db/tenant/tx/2/commit",
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant/tx/2"):
			execCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{
					map[string]any{
						"columns": []string{"n"},
						"data":    []any{map[string]any{"row": []any{1}}},
					},
				},
				"errors": []any{},
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant/tx/2/commit"):
			commitCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/db/tenant/tx/2"):
			rollbackCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()

	loc := &LocationRemote{URI: srv.URL, DBName: "tenant", AuthMode: "oidc_forwarding"}
	tx := NewFabricTransaction("tx-remote-rollback")
	participant := participantKeyFromLocation(loc)
	sub, err := tx.GetOrOpen(participant, true)
	if err != nil {
		t.Fatalf("GetOrOpen failed: %v", err)
	}
	ctx := WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)

	_, err = re.Execute(ctx, loc, "RETURN 1 AS n", nil, "Bearer tok")
	if err != nil {
		t.Fatalf("remote execute failed: %v", err)
	}
	if err := tx.Rollback(nil); err != nil {
		t.Fatalf("fabric rollback failed: %v", err)
	}

	if openCount != 1 || execCount != 1 || commitCount != 0 || rollbackCount != 1 {
		t.Fatalf("unexpected lifecycle counts open=%d exec=%d commit=%d rollback=%d", openCount, execCount, commitCount, rollbackCount)
	}
}
