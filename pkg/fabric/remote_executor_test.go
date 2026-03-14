package fabric

import (
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
