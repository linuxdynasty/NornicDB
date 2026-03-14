package fabric

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// CypherExecutor is the interface for executing Cypher queries against a storage engine.
// It decouples the fabric package from the concrete cypher.StorageExecutor to avoid
// circular imports.
type CypherExecutor interface {
	// ExecuteQuery runs a Cypher query against a storage engine and returns columns + rows.
	ExecuteQuery(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error)
}

// LocalFragmentExecutor executes fragments against a local storage engine
// via the existing Cypher executor infrastructure.
type LocalFragmentExecutor struct {
	cypherExec CypherExecutor
	getEngine  func(dbName string) (storage.Engine, error)
}

// NewLocalFragmentExecutor creates a local executor.
//
// Parameters:
//   - cypherExec: the Cypher query executor
//   - getEngine: function to resolve a database name to a storage.Engine
func NewLocalFragmentExecutor(cypherExec CypherExecutor, getEngine func(string) (storage.Engine, error)) *LocalFragmentExecutor {
	return &LocalFragmentExecutor{
		cypherExec: cypherExec,
		getEngine:  getEngine,
	}
}

// Execute runs a Cypher query against a local database.
func (l *LocalFragmentExecutor) Execute(ctx context.Context, loc *LocationLocal, query string, params map[string]interface{}) (*ResultStream, error) {
	engine, err := l.getEngine(loc.DBName)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage for database '%s': %w", loc.DBName, err)
	}

	columns, rows, err := l.cypherExec.ExecuteQuery(ctx, loc.DBName, engine, query, params)
	if err != nil {
		return nil, fmt.Errorf("local execution on '%s' failed: %w", loc.DBName, err)
	}

	return &ResultStream{
		Columns: columns,
		Rows:    rows,
	}, nil
}
