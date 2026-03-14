package fabric

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// RemoteFragmentExecutor executes fragments against a remote NornicDB instance
// via the RemoteEngine transport (Bolt or HTTP, auto-detected from URI scheme).
type RemoteFragmentExecutor struct {
	// engineCache caches RemoteEngine instances by URI+database key to avoid
	// reconnecting on every fragment execution.
	engineCache map[string]*storage.RemoteEngine
}

// NewRemoteFragmentExecutor creates a remote executor.
func NewRemoteFragmentExecutor() *RemoteFragmentExecutor {
	return &RemoteFragmentExecutor{
		engineCache: make(map[string]*storage.RemoteEngine),
	}
}

// Execute runs a Cypher query against a remote NornicDB instance.
//
// Parameters:
//   - ctx: context for cancellation/deadline propagation
//   - loc: the remote location (URI, database, auth config)
//   - query: the Cypher query to execute
//   - params: query parameters
//   - authToken: the caller's auth token for OIDC forwarding
func (r *RemoteFragmentExecutor) Execute(ctx context.Context, loc *LocationRemote, query string, params map[string]interface{}, authToken string) (*ResultStream, error) {
	engine, err := r.getOrCreateEngine(loc, authToken)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to remote '%s' at %s: %w", loc.DBName, loc.URI, err)
	}

	// Use the remote engine's transport to execute the query.
	columns, rows, err := executeRemoteQuery(ctx, engine, query, params)
	if err != nil {
		return nil, fmt.Errorf("remote execution on '%s' (%s) failed: %w", loc.DBName, loc.URI, err)
	}

	return &ResultStream{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// Close closes all cached remote engines.
func (r *RemoteFragmentExecutor) Close() error {
	var lastErr error
	for key, engine := range r.engineCache {
		if err := engine.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close remote engine '%s': %w", key, err)
		}
		delete(r.engineCache, key)
	}
	return lastErr
}

// cacheKey produces a unique key for engine caching.
func cacheKey(loc *LocationRemote) string {
	return loc.URI + "|" + loc.DBName + "|" + loc.AuthMode
}

// getOrCreateEngine returns a cached engine or creates a new one.
func (r *RemoteFragmentExecutor) getOrCreateEngine(loc *LocationRemote, authToken string) (*storage.RemoteEngine, error) {
	key := cacheKey(loc)
	if engine, exists := r.engineCache[key]; exists {
		return engine, nil
	}

	cfg := storage.RemoteEngineConfig{
		URI:      loc.URI,
		Database: loc.DBName,
	}

	switch loc.AuthMode {
	case "user_password":
		cfg.User = loc.User
		cfg.Password = loc.Password
	default:
		// oidc_forwarding: forward the caller's auth token.
		cfg.AuthToken = authToken
	}

	engine, err := storage.NewRemoteEngine(cfg)
	if err != nil {
		return nil, err
	}

	r.engineCache[key] = engine
	return engine, nil
}

// executeRemoteQuery runs a Cypher query via a RemoteEngine and returns columns + rows.
// This uses the engine's GetNodesByLabel as a transport mechanism for arbitrary Cypher.
// The RemoteEngine.transport.query method handles the actual Bolt/HTTP execution.
func executeRemoteQuery(ctx context.Context, engine *storage.RemoteEngine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	// Access the transport directly via the QueryCypher method.
	columns, rows, err := engine.QueryCypher(ctx, query, params)
	if err != nil {
		return nil, nil, err
	}
	return columns, rows, nil
}
