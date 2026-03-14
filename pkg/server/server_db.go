package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
)

// =============================================================================
// Neo4j-Compatible Database Endpoint Handler
// =============================================================================

// handleDatabaseEndpoint routes /db/{databaseName}/... requests
// Implements Neo4j HTTP API transaction model:
//
//	POST /db/{dbName}/tx/commit - implicit transaction (query and commit)
//	POST /db/{dbName}/tx - open explicit transaction
//	POST /db/{dbName}/tx/{txId} - execute in open transaction
//	POST /db/{dbName}/tx/{txId}/commit - commit transaction
//	DELETE /db/{dbName}/tx/{txId} - rollback transaction
func (s *Server) handleDatabaseEndpoint(w http.ResponseWriter, r *http.Request) {
	// Parse path: /db/{databaseName}/...
	path := strings.TrimPrefix(r.URL.Path, "/db/")
	parts := strings.Split(path, "/")

	if len(parts) < 1 || parts[0] == "" {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.Invalid", "database name required")
		return
	}

	dbName := parts[0]
	remaining := parts[1:]

	// Route based on remaining path
	switch {
	case len(remaining) == 0:
		// /db/{dbName} - database info
		s.handleDatabaseInfo(w, r, dbName)

	case remaining[0] == "tx":
		// Transaction endpoints
		s.handleTransactionEndpoint(w, r, dbName, remaining[1:])

	case remaining[0] == "cluster":
		// /db/{dbName}/cluster - cluster status
		s.handleClusterStatus(w, r, dbName)

	default:
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "unknown endpoint")
	}
}

// getExecutorForDatabase returns a Cypher executor scoped to the specified database.
//
// This method provides database isolation by creating a Cypher executor that operates
// on a namespaced storage engine. All queries executed through the returned executor
// will only see data in the specified database.
//
// Executors are cached per database and reused across requests for efficiency.
// This dramatically reduces memory allocations (from 14% to near-zero).
//
// Parameters:
//   - dbName: The name of the database to get an executor for
//
// Returns:
//   - *cypher.StorageExecutor: A Cypher executor scoped to the database
//   - error: Returns an error if the database doesn't exist or cannot be accessed
//
// Example:
//
//	executor, err := server.getExecutorForDatabase("tenant_a")
//	if err != nil {
//		return err // Database doesn't exist
//	}
//	result, err := executor.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
//
// Thread Safety:
//   - Safe for concurrent use
//   - Executors are cached and reused (thread-safe per StorageExecutor design)
//   - Multiple requests can use the same executor concurrently
//
// Performance:
//   - Executors are created once per database and cached
//   - Subsequent requests reuse the cached executor (zero allocation overhead)
//   - Storage engines are cached by DatabaseManager for efficiency
func (s *Server) getExecutorForDatabase(dbName string) (*cypher.StorageExecutor, error) {
	// Check cache first (read lock for fast path)
	s.executorsMu.RLock()
	if executor, ok := s.executors[dbName]; ok {
		s.executorsMu.RUnlock()
		// Executors can be created before the embedding model finishes loading.
		// Ensure cached executors pick up the latest query embedder lazily.
		if baseExec := s.db.GetCypherExecutor(); baseExec != nil {
			if emb := baseExec.GetEmbedder(); emb != nil && executor.GetEmbedder() == nil {
				executor.SetEmbedder(emb)
			}
		}
		return executor, nil
	}
	s.executorsMu.RUnlock()

	// Get namespaced storage for this database
	executor, err := s.newExecutorForDatabase(dbName)
	if err != nil {
		return nil, err
	}

	// Cache the executor (write lock for cache update)
	s.executorsMu.Lock()
	// Double-check in case another goroutine created it while we were waiting
	if existing, ok := s.executors[dbName]; ok {
		s.executorsMu.Unlock()
		return existing, nil
	}
	s.executors[dbName] = executor
	s.executorsMu.Unlock()

	return executor, nil
}

// getExecutorForDatabaseWithAuth returns an executor for dbName and forwards authToken
// to remote constituent resolution when a composite database contains remote constituents.
func (s *Server) getExecutorForDatabaseWithAuth(dbName string, authToken string) (*cypher.StorageExecutor, error) {
	if authToken == "" || !s.databaseHasRemoteConstituent(dbName) {
		return s.getExecutorForDatabase(dbName)
	}

	storageEngine, err := s.dbManager.GetStorageWithAuth(dbName, authToken)
	if err != nil {
		return nil, err
	}

	executor := cypher.NewStorageExecutor(storageEngine)
	executor.SetDatabaseManager(&databaseManagerAdapter{manager: s.dbManager, db: s.db, server: s})

	if searchSvc, err := s.db.GetOrCreateSearchService(dbName, storageEngine); err == nil {
		executor.SetSearchService(searchSvc)
	}

	if baseExec := s.db.GetCypherExecutor(); baseExec != nil {
		if emb := baseExec.GetEmbedder(); emb != nil {
			executor.SetEmbedder(emb)
		}
		if inferMgr := baseExec.GetInferenceManager(); inferMgr != nil {
			executor.SetInferenceManager(inferMgr)
		}
	}

	if q := s.db.GetEmbedQueue(); q != nil {
		executor.SetNodeMutatedCallback(func(nodeID string) { q.Enqueue(nodeID) })
	}

	return executor, nil
}

func (s *Server) databaseHasRemoteConstituent(dbName string) bool {
	info, err := s.dbManager.GetDatabase(dbName)
	if err != nil || info == nil || info.Type != "composite" {
		return false
	}
	for _, ref := range info.Constituents {
		if ref.Type == "remote" {
			return true
		}
	}
	return false
}

// newExecutorForDatabase creates a fresh executor scoped to a single database.
// Unlike getExecutorForDatabase, this does not cache the executor and is intended
// for per-transaction session state (explicit HTTP transactions).
func (s *Server) newExecutorForDatabase(dbName string) (*cypher.StorageExecutor, error) {
	// This returns a NamespacedEngine that automatically prefixes all keys
	// with the database name, ensuring complete data isolation.
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		return nil, err
	}

	executor := cypher.NewStorageExecutor(storageEngine)
	executor.SetDatabaseManager(&databaseManagerAdapter{manager: s.dbManager, db: s.db, server: s})

	// Reuse DB's cached search service instead of creating a new one.
	if searchSvc, err := s.db.GetOrCreateSearchService(dbName, storageEngine); err == nil {
		executor.SetSearchService(searchSvc)
	}

	// Copy query embedder from the base DB executor so string-input vector procedures work.
	if baseExec := s.db.GetCypherExecutor(); baseExec != nil {
		if emb := baseExec.GetEmbedder(); emb != nil {
			executor.SetEmbedder(emb)
		}
		if inferMgr := baseExec.GetInferenceManager(); inferMgr != nil {
			executor.SetInferenceManager(inferMgr)
		}
	}

	// Wire embed queue callback for per-database executor mutations.
	if q := s.db.GetEmbedQueue(); q != nil {
		executor.SetNodeMutatedCallback(func(nodeID string) {
			q.Enqueue(nodeID)
		})
	}

	return executor, nil
}

// invalidateExecutor removes a cached executor for a dropped database.
func (s *Server) invalidateExecutor(dbName string) {
	s.executorsMu.Lock()
	defer s.executorsMu.Unlock()
	delete(s.executors, dbName)
}

// invalidateAllExecutors clears all cached executors to force fresh database manager references.
// This is used when database metadata changes (e.g., dropping databases) to ensure
// all executors see the updated state.
func (s *Server) invalidateAllExecutors() {
	s.executorsMu.Lock()
	defer s.executorsMu.Unlock()
	// Clear all executors - they will be recreated with fresh database manager references
	s.executors = make(map[string]*cypher.StorageExecutor)
}

// databaseManagerAdapter wraps multidb.DatabaseManager to implement
// cypher.DatabaseManagerInterface, avoiding import cycles.
type databaseManagerAdapter struct {
	manager *multidb.DatabaseManager
	db      *nornicdb.DB
	server  *Server // Reference to server for cache invalidation
}

func (a *databaseManagerAdapter) CreateDatabase(name string) error {
	return a.manager.CreateDatabase(name)
}

func (a *databaseManagerAdapter) DropDatabase(name string) error {
	if err := a.manager.DropDatabase(name); err != nil {
		return err
	}
	if a.db != nil {
		a.db.ResetSearchService(name)
		a.db.ResetInferenceService(name)
	}
	// Invalidate cached executor for dropped database
	if a.server != nil {
		a.server.invalidateExecutor(name)
		// Also invalidate all executors to ensure fresh database manager references
		// This ensures queries from other databases see the updated database list
		a.server.invalidateAllExecutors()
	}
	return nil
}

func (a *databaseManagerAdapter) ListDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListDatabases()
	result := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		result[i] = &databaseInfoAdapter{info: db}
	}
	return result
}

func (a *databaseManagerAdapter) Exists(name string) bool {
	return a.manager.Exists(name)
}

func (a *databaseManagerAdapter) CreateAlias(alias, databaseName string) error {
	return a.manager.CreateAlias(alias, databaseName)
}

func (a *databaseManagerAdapter) DropAlias(alias string) error {
	return a.manager.DropAlias(alias)
}

func (a *databaseManagerAdapter) ListAliases(databaseName string) map[string]string {
	return a.manager.ListAliases(databaseName)
}

func (a *databaseManagerAdapter) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.manager.ResolveDatabase(nameOrAlias)
}

func (a *databaseManagerAdapter) SetDatabaseLimits(databaseName string, limits interface{}) error {
	// Convert interface{} to *multidb.Limits
	limitsPtr, ok := limits.(*multidb.Limits)
	if !ok {
		return fmt.Errorf("invalid limits type")
	}
	return a.manager.SetDatabaseLimits(databaseName, limitsPtr)
}

func (a *databaseManagerAdapter) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.manager.GetDatabaseLimits(databaseName)
}

func (a *databaseManagerAdapter) CreateCompositeDatabase(name string, constituents []interface{}) error {
	// Convert []interface{} to []multidb.ConstituentRef
	refs := make([]multidb.ConstituentRef, len(constituents))
	for i, c := range constituents {
		ref, ok := c.(multidb.ConstituentRef)
		if !ok {
			// Try to convert from map
			if m, ok := c.(map[string]interface{}); ok {
				ref = multidb.ConstituentRef{
					Alias:        getString(m, "alias"),
					DatabaseName: getString(m, "database_name"),
					Type:         getString(m, "type"),
					AccessMode:   getString(m, "access_mode"),
					URI:          getString(m, "uri"),
					SecretRef:    getString(m, "secret_ref"),
					AuthMode:     getString(m, "auth_mode"),
					User:         getString(m, "user"),
					Password:     getString(m, "password"),
				}
			} else {
				return fmt.Errorf("invalid constituent type at index %d", i)
			}
		}
		refs[i] = ref
	}
	return a.manager.CreateCompositeDatabase(name, refs)
}

func (a *databaseManagerAdapter) DropCompositeDatabase(name string) error {
	if err := a.manager.DropCompositeDatabase(name); err != nil {
		return err
	}
	// Invalidate any cached executors that might reference this composite database
	// Note: Composite databases don't have their own executors cached, but we should
	// invalidate the executor for the database we're querying from (usually "nornic")
	// to ensure subsequent queries see the updated state
	if a.server != nil {
		// Invalidate executor cache to force fresh database manager reference
		// This ensures all executors see the updated database list
		a.server.invalidateAllExecutors()
	}
	return nil
}

func (a *databaseManagerAdapter) AddConstituent(compositeName string, constituent interface{}) error {
	var ref multidb.ConstituentRef
	if m, ok := constituent.(map[string]interface{}); ok {
		ref = multidb.ConstituentRef{
			Alias:        getString(m, "alias"),
			DatabaseName: getString(m, "database_name"),
			Type:         getString(m, "type"),
			AccessMode:   getString(m, "access_mode"),
			URI:          getString(m, "uri"),
			SecretRef:    getString(m, "secret_ref"),
			AuthMode:     getString(m, "auth_mode"),
			User:         getString(m, "user"),
			Password:     getString(m, "password"),
		}
	} else if r, ok := constituent.(multidb.ConstituentRef); ok {
		ref = r
	} else {
		return fmt.Errorf("invalid constituent type")
	}
	return a.manager.AddConstituent(compositeName, ref)
}

func (a *databaseManagerAdapter) RemoveConstituent(compositeName string, alias string) error {
	return a.manager.RemoveConstituent(compositeName, alias)
}

func (a *databaseManagerAdapter) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	constituents, err := a.manager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(constituents))
	for i, c := range constituents {
		result[i] = c
	}
	return result, nil
}

func (a *databaseManagerAdapter) ListCompositeDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListCompositeDatabases()
	result := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		result[i] = &databaseInfoAdapter{info: db}
	}
	return result
}

func (a *databaseManagerAdapter) IsCompositeDatabase(name string) bool {
	return a.manager.IsCompositeDatabase(name)
}

func (a *databaseManagerAdapter) GetStorageForUse(name string, authToken string) (interface{}, error) {
	return a.manager.GetStorageWithAuth(name, authToken)
}

// Helper function to get string from map
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// databaseInfoAdapter wraps multidb.DatabaseInfo to implement
// cypher.DatabaseInfoInterface.
type databaseInfoAdapter struct {
	info *multidb.DatabaseInfo
}

func (a *databaseInfoAdapter) Name() string {
	return a.info.Name
}

func (a *databaseInfoAdapter) Type() string {
	return a.info.Type
}

func (a *databaseInfoAdapter) Status() string {
	return a.info.Status
}

func (a *databaseInfoAdapter) IsDefault() bool {
	return a.info.IsDefault
}

func (a *databaseInfoAdapter) CreatedAt() time.Time {
	return a.info.CreatedAt
}

// handleDatabaseInfo returns database information for the specified database.
//
// This endpoint provides metadata about a database including its name, status,
// whether it's the default database, and current statistics (node and edge counts).
//
// Endpoint: GET /db/{dbName}
//
// Parameters:
//   - dbName: The name of the database to get information about
//
// Response (200 OK):
//
//	{
//	  "name": "tenant_a",
//	  "status": "online",
//	  "default": false,
//	  "nodeCount": 1234,
//	  "edgeCount": 5678,
//	  "nodeStorageBytes": 123456,
//	  "managedEmbeddingBytes": 4194304
//	}
//
// Errors:
//   - 404 Not Found: Database doesn't exist (Neo.ClientError.Database.DatabaseNotFound)
//   - 500 Internal Server Error: Failed to access database (Neo.ClientError.Database.General)
//
// Example:
//
//	GET /db/tenant_a
//	Response: {
//	  "name": "tenant_a",
//	  "status": "online",
//	  "default": false,
//	  "nodeCount": 100,
//	  "edgeCount": 50,
//	  "nodeStorageBytes": 20480,
//	  "managedEmbeddingBytes": 12288
//	}
//
// Thread Safety:
//   - Safe for concurrent use
//   - Statistics are read from namespaced storage (thread-safe)
//
// Performance:
//   - Node and edge counts are computed on-demand
//   - For large databases, this may take a few milliseconds
//   - Consider caching if this endpoint is called frequently
func (s *Server) handleDatabaseInfo(w http.ResponseWriter, r *http.Request, dbName string) {
	// Check if database exists
	// This is a fast lookup in the DatabaseManager's metadata
	if !s.dbManager.Exists(dbName) {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Database.DatabaseNotFound",
			fmt.Sprintf("Database '%s' not found", dbName))
		return
	}

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.getDatabaseAccessMode(getClaims(r)).CanAccessDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
		return
	}

	// Get storage for this database to get stats
	// This returns a NamespacedEngine that provides isolated access
	storage, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.Database.General",
			fmt.Sprintf("Failed to access database: %v", err))
		return
	}

	// Get stats from the namespaced storage
	// These counts reflect only data in this database (due to namespacing)
	nodeCount, err := storage.NodeCount()
	if err != nil {
		// Log error but continue with 0 count
		nodeCount = 0
	}
	edgeCount, err := storage.EdgeCount()
	if err != nil {
		// Log error but continue with 0 count
		edgeCount = 0
	}
	_, nodeStorageBytes, _ := s.dbManager.GetStorageSize(dbName)
	_, _, managedEmbeddingBytes := s.db.GetDatabaseManagedEmbeddingStats(dbName)

	// Check if this is the default database
	// The default database is configured at startup and cannot be dropped
	defaultDB := s.dbManager.DefaultDatabaseName()
	searchStatus := s.db.GetDatabaseSearchStatus(dbName)

	response := map[string]interface{}{
		"name":                  dbName,
		"status":                "online",
		"default":               dbName == defaultDB,
		"nodeCount":             nodeCount,
		"edgeCount":             edgeCount,
		"nodeStorageBytes":      nodeStorageBytes,
		"managedEmbeddingBytes": managedEmbeddingBytes,
		"searchReady":           searchStatus.Ready,
		"searchBuilding":        searchStatus.Building,
		"searchInitialized":     searchStatus.Initialized,
		"searchPhase":           searchStatus.Phase,
		"searchProcessed":       searchStatus.ProcessedNodes,
		"searchTotal":           searchStatus.TotalNodes,
		"searchRate":            searchStatus.RateNodesPerSec,
		"searchEtaSeconds":      searchStatus.ETASeconds,
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleClusterStatus returns cluster status (standalone mode)
func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request, dbName string) {
	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.getDatabaseAccessMode(getClaims(r)).CanAccessDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
		return
	}
	response := map[string]interface{}{
		"mode":     "standalone",
		"database": dbName,
		"status":   "online",
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleTransactionEndpoint routes transaction-related requests
func (s *Server) handleTransactionEndpoint(w http.ResponseWriter, r *http.Request, dbName string, remaining []string) {
	switch {
	case len(remaining) == 0:
		// POST /db/{dbName}/tx - open new transaction
		if r.Method != http.MethodPost {
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
			return
		}
		s.handleOpenTransaction(w, r, dbName)

	case remaining[0] == "commit" && len(remaining) == 1:
		// POST /db/{dbName}/tx/commit - implicit transaction
		if r.Method != http.MethodPost {
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
			return
		}
		s.handleImplicitTransaction(w, r, dbName)

	case len(remaining) == 1:
		// POST/DELETE /db/{dbName}/tx/{txId}
		txID := remaining[0]
		switch r.Method {
		case http.MethodPost:
			s.handleExecuteInTransaction(w, r, dbName, txID)
		case http.MethodDelete:
			s.handleRollbackTransaction(w, r, dbName, txID)
		default:
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST or DELETE required")
		}

	case len(remaining) == 2 && remaining[1] == "commit":
		// POST /db/{dbName}/tx/{txId}/commit
		if r.Method != http.MethodPost {
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
			return
		}
		txID := remaining[0]
		s.handleCommitTransaction(w, r, dbName, txID)

	default:
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "unknown transaction endpoint")
	}
}

// TransactionRequest follows Neo4j HTTP API format exactly.
type TransactionRequest struct {
	Statements []StatementRequest `json:"statements"`
}

// StatementRequest is a single Cypher statement.
type StatementRequest struct {
	Statement          string                 `json:"statement"`
	Parameters         map[string]interface{} `json:"parameters,omitempty"`
	ResultDataContents []string               `json:"resultDataContents,omitempty"` // ["row", "graph"]
	IncludeStats       bool                   `json:"includeStats,omitempty"`
}

// TransactionResponse follows Neo4j HTTP API format exactly.
type TransactionResponse struct {
	Results       []QueryResult        `json:"results"`
	Errors        []QueryError         `json:"errors"`
	Commit        string               `json:"commit,omitempty"`        // URL to commit (for open transactions)
	Transaction   *TransactionInfo     `json:"transaction,omitempty"`   // Transaction state
	LastBookmarks []string             `json:"lastBookmarks,omitempty"` // Bookmark for causal consistency
	Notifications []ServerNotification `json:"notifications,omitempty"` // Server notifications
	Receipt       interface{}          `json:"receipt,omitempty"`       // Mutation receipt (tx_id, wal_seq_start, wal_seq_end, hash)
}

// TransactionInfo holds transaction state.
type TransactionInfo struct {
	Expires string `json:"expires"` // RFC1123 format
}

// QueryResult is a single query result.
type QueryResult struct {
	Columns []string    `json:"columns"`
	Data    []ResultRow `json:"data"`
	Stats   *QueryStats `json:"stats,omitempty"`
}

// ResultRow is a row of results with metadata.
type ResultRow struct {
	Row   []interface{} `json:"row"`
	Meta  []interface{} `json:"meta,omitempty"`
	Graph *GraphResult  `json:"graph,omitempty"`
}

// GraphResult holds graph-format results.
type GraphResult struct {
	Nodes         []GraphNode         `json:"nodes"`
	Relationships []GraphRelationship `json:"relationships"`
}

// GraphNode is a node in graph format.
type GraphNode struct {
	ID         string                 `json:"id"`
	ElementID  string                 `json:"elementId"`
	Labels     []string               `json:"labels"`
	Properties map[string]interface{} `json:"properties"`
}

// GraphRelationship is a relationship in graph format.
type GraphRelationship struct {
	ID         string                 `json:"id"`
	ElementID  string                 `json:"elementId"`
	Type       string                 `json:"type"`
	StartNode  string                 `json:"startNodeElementId"`
	EndNode    string                 `json:"endNodeElementId"`
	Properties map[string]interface{} `json:"properties"`
}

// QueryStats holds query execution statistics.
type QueryStats struct {
	NodesCreated         int  `json:"nodes_created,omitempty"`
	NodesDeleted         int  `json:"nodes_deleted,omitempty"`
	RelationshipsCreated int  `json:"relationships_created,omitempty"`
	RelationshipsDeleted int  `json:"relationships_deleted,omitempty"`
	PropertiesSet        int  `json:"properties_set,omitempty"`
	LabelsAdded          int  `json:"labels_added,omitempty"`
	LabelsRemoved        int  `json:"labels_removed,omitempty"`
	IndexesAdded         int  `json:"indexes_added,omitempty"`
	IndexesRemoved       int  `json:"indexes_removed,omitempty"`
	ConstraintsAdded     int  `json:"constraints_added,omitempty"`
	ConstraintsRemoved   int  `json:"constraints_removed,omitempty"`
	ContainsUpdates      bool `json:"contains_updates,omitempty"`
}

// QueryError is an error from a query (Neo4j format).
type QueryError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ServerNotification is a warning/info from the server.
type ServerNotification struct {
	Code        string           `json:"code"`
	Severity    string           `json:"severity"`
	Title       string           `json:"title"`
	Description string           `json:"description"`
	Position    *NotificationPos `json:"position,omitempty"`
}

// NotificationPos is the position of a notification in the query.
type NotificationPos struct {
	Offset int `json:"offset"`
	Line   int `json:"line"`
	Column int `json:"column"`
}

// stripCypherComments removes Cypher comments from a query string.
// Supports both single-line comments (//) and multi-line comments (/* */).
// This follows the Cypher specification for comments.
//
// Examples:
//
//	"MATCH (n) RETURN n // comment" -> "MATCH (n) RETURN n "
//	"MATCH (n) /* comment */ RETURN n" -> "MATCH (n)  RETURN n"
//	"MATCH (n)\n// line comment\nRETURN n" -> "MATCH (n)\n\nRETURN n"
func stripCypherComments(query string) string {
	if query == "" {
		return query
	}

	var result strings.Builder
	result.Grow(len(query))

	lines := strings.Split(query, "\n")
	inMultiLineComment := false
	outputLines := []string{}

	for _, line := range lines {
		processed := line

		// Handle multi-line comments that span lines
		if inMultiLineComment {
			// Check if this line closes the multi-line comment
			if idx := strings.Index(processed, "*/"); idx >= 0 {
				// Multi-line comment ends on this line
				processed = processed[idx+2:]
				inMultiLineComment = false
				// Continue processing the rest of this line
			} else {
				// Still inside multi-line comment, skip entire line
				// Don't add anything for skipped comment-only lines
				continue
			}
		}

		// Process remaining line for comments
		var lineResult strings.Builder
		i := 0
		lineStartedMultiLineComment := false
		for i < len(processed) {
			// Check for start of multi-line comment
			if i+1 < len(processed) && processed[i:i+2] == "/*" {
				// Find end of multi-line comment
				endIdx := strings.Index(processed[i+2:], "*/")
				if endIdx >= 0 {
					// Multi-line comment ends on same line
					i = i + 2 + endIdx + 2
					continue
				} else {
					// Multi-line comment spans to next line
					inMultiLineComment = true
					lineStartedMultiLineComment = true
					break
				}
			}

			// Check for single-line comment
			if i+1 < len(processed) && processed[i:i+2] == "//" {
				// Rest of line is comment, stop processing
				break
			}

			// Regular character, keep it
			lineResult.WriteByte(processed[i])
			i++
		}

		// Add processed line to output
		// Don't add empty lines that started a multi-line comment (they're entirely comment)
		lineStr := lineResult.String()
		// Only trim if entire line is whitespace (preserve trailing spaces after comments)
		trimmed := strings.TrimSpace(lineStr)
		if trimmed == "" && lineStr != "" {
			// Entire line is whitespace, use empty string
			lineStr = ""
		}
		if !lineStartedMultiLineComment || lineStr != "" {
			outputLines = append(outputLines, lineStr)
		}
	}

	// Join lines, preserving original line structure
	resultStr := strings.Join(outputLines, "\n")

	// Preserve trailing newline if original had one
	if strings.HasSuffix(query, "\n") {
		resultStr += "\n"
	}

	return resultStr
}

// handleImplicitTransaction executes statements in an implicit transaction.
// This is the main query endpoint: POST /db/{dbName}/tx/commit
func (s *Server) handleImplicitTransaction(w http.ResponseWriter, r *http.Request, dbName string) {
	var req TransactionRequest
	if err := s.readJSON(r, &req); err != nil {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "invalid request body")
		return
	}

	response := TransactionResponse{
		Results:       make([]QueryResult, 0, len(req.Statements)),
		Errors:        make([]QueryError, 0),
		LastBookmarks: []string{s.generateBookmark()},
	}

	claims := getClaims(r)
	hasError := false

	// Default to database from URL path
	// Each statement can override this with its own :USE command
	defaultDbName := dbName

	for _, stmt := range req.Statements {
		if hasError {
			// Skip remaining statements after error (rollback semantics)
			break
		}

		// Extract :USE command from this statement if present
		// Each statement can have its own :USE command to switch databases
		effectiveDbName := defaultDbName
		queryStatement := stmt.Statement

		// Check if statement starts with :USE
		trimmedStmt := strings.TrimSpace(stmt.Statement)
		if strings.HasPrefix(trimmedStmt, ":USE") || strings.HasPrefix(trimmedStmt, ":use") {
			lines := strings.Split(stmt.Statement, "\n")
			var remainingLines []string
			foundUse := false
			for _, line := range lines {
				trimmed := strings.TrimSpace(line)
				if !foundUse && (strings.HasPrefix(trimmed, ":USE") || strings.HasPrefix(trimmed, ":use")) {
					// Extract database name from :USE command
					parts := strings.Fields(trimmed)
					if len(parts) >= 2 {
						effectiveDbName = parts[1]
					}
					foundUse = true
					// Check if there's more content on this line after :USE command
					// Format: ":USE dbname rest of query"
					useIndex := strings.Index(trimmed, ":USE")
					if useIndex == -1 {
						useIndex = strings.Index(strings.ToUpper(trimmed), ":USE")
					}
					if useIndex >= 0 {
						// Find where :USE command ends (after database name)
						afterUse := trimmed[useIndex+4:] // Skip ":USE"
						afterUse = strings.TrimSpace(afterUse)
						// Extract database name and remaining query
						fields := strings.Fields(afterUse)
						if len(fields) > 0 {
							// Database name is first field, rest is the query
							if len(fields) > 1 {
								remainingQuery := strings.Join(fields[1:], " ")
								if remainingQuery != "" {
									remainingLines = append(remainingLines, remainingQuery)
								}
							}
						}
					}
					// Skip the :USE line itself (already processed)
					continue
				}
				remainingLines = append(remainingLines, line)
			}
			if foundUse {
				queryStatement = strings.Join(remainingLines, "\n")
				queryStatement = strings.TrimSpace(queryStatement)
			}
		}

		// Per-database access: deny if principal may not access this database (Neo4j-aligned).
		if !s.getDatabaseAccessMode(claims).CanAccessDatabase(effectiveDbName) {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Security.Forbidden",
				Message: fmt.Sprintf("Access to database '%s' is not allowed.", effectiveDbName),
			})
			hasError = true
			continue
		}

		// Per-database write: for mutations, require ResolvedAccess.Write for this (principal, db).
		if isMutationQuery(stmt.Statement) {
			if claims == nil {
				response.Errors = append(response.Errors, QueryError{
					Code:    "Neo.ClientError.Security.Forbidden",
					Message: "Write permission required",
				})
				hasError = true
				continue
			}
			if !s.getResolvedAccess(claims, effectiveDbName).Write {
				response.Errors = append(response.Errors, QueryError{
					Code:    "Neo.ClientError.Security.Forbidden",
					Message: fmt.Sprintf("Write on database '%s' is not allowed.", effectiveDbName),
				})
				hasError = true
				continue
			}
		}

		// Check if database exists before attempting to get executor
		if !s.dbManager.Exists(effectiveDbName) {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Database.DatabaseNotFound",
				Message: fmt.Sprintf("Database '%s' not found", effectiveDbName),
			})
			hasError = true
			continue
		}

		// Strip Cypher comments from query before execution
		// Comments are part of Cypher spec but should be removed before parsing
		queryStatement = stripCypherComments(queryStatement)
		queryStatement = strings.TrimSpace(queryStatement)
		// Remove UTF-8 BOM if present (some clients send it; breaks executor routing e.g. CREATE DATABASE)
		if strings.HasPrefix(queryStatement, "\xef\xbb\xbf") {
			queryStatement = strings.TrimPrefix(queryStatement, "\xef\xbb\xbf")
			queryStatement = strings.TrimSpace(queryStatement)
		}

		// Skip empty statements (after comment removal)
		if queryStatement == "" {
			// Empty statement after comment removal - return empty result
			response.Results = append(response.Results, QueryResult{
				Columns: []string{},
				Data:    []ResultRow{},
			})
			continue
		}

		// Get executor for the specified database (or the one from :USE command).
		// For composite databases with remote constituents, preserve caller identity by
		// forwarding the request auth token into remote constituent engine construction.
		executor, err := s.getExecutorForDatabaseWithAuth(effectiveDbName, r.Header.Get("Authorization"))
		if err != nil {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Database.General",
				Message: fmt.Sprintf("Failed to access database '%s': %v", effectiveDbName, err),
			})
			hasError = true
			continue
		}

		// Track query execution time for slow query logging
		queryStart := time.Now()
		execCtx := cypher.WithAuthToken(r.Context(), r.Header.Get("Authorization"))
		result, err := executor.Execute(execCtx, queryStatement, stmt.Parameters)
		queryDuration := time.Since(queryStart)

		// Log slow queries
		s.logSlowQuery(stmt.Statement, stmt.Parameters, queryDuration, err)

		if err != nil {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Statement.SyntaxError",
				Message: err.Error(),
			})
			hasError = true
			continue
		}

		// Auto-grant access when a new database is created: admins and the creating principal get full access.
		if isCreateDatabaseStatement(queryStatement) {
			if createdName, ok := parseCreatedDatabaseName(queryStatement); ok && createdName != "" {
				s.grantAccessToNewDatabase(r.Context(), createdName, claims)
				// Ensure CREATE DATABASE always returns a proper result (name + row).
				// Defensive: executor should return this, but if it ever returns empty we still send a valid response.
				if len(result.Columns) == 0 && len(result.Rows) == 0 {
					log.Printf("[nornicdb:CREATE_DATABASE] server defensive fix applied: executor returned empty result for CREATE DATABASE, filled with name=%q", createdName)
					result.Columns = []string{"name"}
					result.Rows = [][]interface{}{{createdName}}
				}
			}
		}

		// Per-database RBAC: filter SHOW DATABASES results by CanSeeDatabase so principals only see DBs they may access
		if isShowDatabasesQuery(queryStatement) && result.Rows != nil {
			mode := s.getDatabaseAccessMode(claims)
			filtered := make([][]interface{}, 0, len(result.Rows))
			for _, row := range result.Rows {
				if len(row) > 0 {
					if name, ok := row[0].(string); ok && mode.CanSeeDatabase(name) {
						filtered = append(filtered, row)
					}
				}
			}
			result.Rows = filtered
		}

		// Extract receipt from result metadata if present (for mutations)
		if result.Metadata != nil {
			if receipt, ok := result.Metadata["receipt"]; ok && receipt != nil {
				response.Receipt = receipt
			}
		}

		// Convert result to Neo4j format with metadata
		qr := QueryResult{
			Columns: result.Columns,
			Data:    make([]ResultRow, len(result.Rows)),
		}

		for i, row := range result.Rows {
			convertedRow := s.convertRowToNeo4jFormat(row)
			qr.Data[i] = ResultRow{
				Row:  convertedRow,
				Meta: s.generateRowMeta(convertedRow),
			}
		}

		if stmt.IncludeStats {
			qr.Stats = &QueryStats{ContainsUpdates: isMutationQuery(stmt.Statement)}
		}

		response.Results = append(response.Results, qr)
	}

	// Determine appropriate HTTP status code
	// Neo4j behavior: Query errors return 200 OK with errors in response body
	// Only infrastructure errors (database not found) return 4xx status codes
	status := http.StatusOK

	// Check for infrastructure errors (these return 4xx status codes)
	if len(response.Errors) > 0 {
		for _, err := range response.Errors {
			// Database not found is an infrastructure error - return 404
			if err.Code == "Neo.ClientError.Database.DatabaseNotFound" {
				status = http.StatusNotFound
				break
			}
			// Database access errors are infrastructure errors - return 500
			if err.Code == "Neo.ClientError.Database.General" {
				status = http.StatusInternalServerError
				break
			}
			// Query syntax errors, security errors, etc. return 200 OK
			// with errors in the response body (Neo4j standard behavior)
		}
	} else if s.db.IsAsyncWritesEnabled() {
		// For eventual consistency (async writes), mutations return 202 Accepted
		for _, stmt := range req.Statements {
			if isMutationQuery(stmt.Statement) {
				status = http.StatusAccepted
				w.Header().Set("X-NornicDB-Consistency", "eventual")
				break
			}
		}
	}

	s.writeJSON(w, status, response)
}

// grantAccessToNewDatabase grants the admin role and the creating principal's roles full access
// (see, access, read, write) to a newly created database. Called after successful CREATE DATABASE
// or CREATE COMPOSITE DATABASE. No-op when RBAC stores are not loaded.
func (s *Server) grantAccessToNewDatabase(ctx context.Context, dbName string, claims *auth.JWTClaims) {
	if s.allowlistStore == nil || s.privilegesStore == nil {
		return
	}
	allowlist := s.allowlistStore.Allowlist()
	normalizeRole := func(r string) string {
		r = strings.TrimSpace(r)
		r = strings.ToLower(r)
		r = strings.TrimPrefix(r, "role_")
		return r
	}

	// Ensure admin role has full access to the new database.
	adminRole := string(auth.RoleAdmin)
	if dbs, ok := allowlist[adminRole]; ok && len(dbs) > 0 {
		// Explicit allowlist: add new DB if not present.
		seen := false
		for _, d := range dbs {
			if d == dbName {
				seen = true
				break
			}
		}
		if !seen {
			_ = s.allowlistStore.SaveRoleDatabases(ctx, adminRole, append(append([]string(nil), dbs...), dbName))
		}
	}
	_ = s.privilegesStore.SavePrivilege(ctx, adminRole, dbName, true, true)

	// Grant the creating principal's roles full access.
	if claims != nil && len(claims.Roles) > 0 {
		seenRoles := map[string]struct{}{adminRole: {}}
		for _, r := range claims.Roles {
			role := normalizeRole(r)
			if role == "" {
				continue
			}
			if _, done := seenRoles[role]; done {
				continue
			}
			seenRoles[role] = struct{}{}
			if dbs, ok := allowlist[role]; ok && len(dbs) > 0 {
				seen := false
				for _, d := range dbs {
					if d == dbName {
						seen = true
						break
					}
				}
				if !seen {
					_ = s.allowlistStore.SaveRoleDatabases(ctx, role, append(append([]string(nil), dbs...), dbName))
				}
			}
			_ = s.privilegesStore.SavePrivilege(ctx, role, dbName, true, true)
		}
	}
}

// convertRowToNeo4jFormat converts each value in a row to Neo4j-compatible format.
// This ensures nodes and edges use elementId and have filtered properties.
func (s *Server) convertRowToNeo4jFormat(row []interface{}) []interface{} {
	converted := make([]interface{}, len(row))
	for i, val := range row {
		converted[i] = s.convertValueToNeo4jFormat(val)
	}
	return converted
}

// convertValueToNeo4jFormat converts a single value to Neo4j HTTP format.
// Handles storage.Node, storage.Edge, maps, and slices recursively.
func (s *Server) convertValueToNeo4jFormat(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case *storage.Node:
		return s.nodeToNeo4jHTTPFormat(v)
	case *storage.Edge:
		return s.edgeToNeo4jHTTPFormat(v)
	case map[string]interface{}:
		// Check if this is already a converted node (has elementId)
		if _, hasElementId := v["elementId"]; hasElementId {
			return v
		}
		// Check if this looks like a node map (has _nodeId or id + labels)
		if nodeId, hasNodeId := v["_nodeId"]; hasNodeId {
			return s.mapNodeToNeo4jHTTPFormat(nodeId, v)
		}
		if nodeId, hasId := v["id"]; hasId {
			if _, hasLabels := v["labels"]; hasLabels {
				return s.mapNodeToNeo4jHTTPFormat(nodeId, v)
			}
		}
		// Regular map - convert nested values
		result := make(map[string]interface{}, len(v))
		for k, vv := range v {
			if k == "_pathResult" {
				continue
			}
			result[k] = s.convertValueToNeo4jFormat(vv)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, vv := range v {
			result[i] = s.convertValueToNeo4jFormat(vv)
		}
		return result
	default:
		return val
	}
}

// nodeToNeo4jHTTPFormat converts a storage.Node to Neo4j HTTP API format.
// Neo4j format: {"elementId": "4:db:id", "labels": [...], "properties": {...}}
func (s *Server) nodeToNeo4jHTTPFormat(node *storage.Node) map[string]interface{} {
	if node == nil {
		return nil
	}

	elementId := fmt.Sprintf("4:nornicdb:%s", node.ID)

	// Preserve user properties exactly as stored for Neo4j compatibility.
	props := node.Properties

	return map[string]interface{}{
		"elementId":  elementId,
		"labels":     node.Labels,
		"properties": props,
	}
}

// mapNodeToNeo4jHTTPFormat converts a map representation to Neo4j HTTP format.
func (s *Server) mapNodeToNeo4jHTTPFormat(nodeId interface{}, m map[string]interface{}) map[string]interface{} {
	elementId := fmt.Sprintf("4:nornicdb:%v", nodeId)

	// Extract labels
	var labels []string
	if l, ok := m["labels"].([]string); ok {
		labels = l
	} else if l, ok := m["labels"].([]interface{}); ok {
		labels = make([]string, len(l))
		for i, v := range l {
			if s, ok := v.(string); ok {
				labels[i] = s
			}
		}
	}

	// Extract properties without key-based filtering.
	var props map[string]interface{}
	if p, ok := m["properties"].(map[string]interface{}); ok {
		props = p
	} else {
		// Properties might be at top level - collect them
		props = make(map[string]interface{})
		for k, v := range m {
			// Skip metadata fields
			if k == "id" || k == "_nodeId" || k == "labels" || k == "properties" || k == "elementId" || k == "embedding" {
				continue
			}
			props[k] = v
		}
	}

	return map[string]interface{}{
		"elementId":  elementId,
		"labels":     labels,
		"properties": props,
	}
}

// edgeToNeo4jHTTPFormat converts a storage.Edge to Neo4j HTTP API format.
func (s *Server) edgeToNeo4jHTTPFormat(edge *storage.Edge) map[string]interface{} {
	if edge == nil {
		return nil
	}

	elementId := fmt.Sprintf("5:nornicdb:%s", edge.ID)
	startElementId := fmt.Sprintf("4:nornicdb:%s", edge.StartNode)
	endElementId := fmt.Sprintf("4:nornicdb:%s", edge.EndNode)

	return map[string]interface{}{
		"elementId":          elementId,
		"type":               edge.Type,
		"startNodeElementId": startElementId,
		"endNodeElementId":   endElementId,
		"properties":         edge.Properties,
	}
}

// generateRowMeta generates Neo4j-compatible metadata for each value in a row.
// Neo4j meta format: {"id": 123, "type": "node", "deleted": false, "elementId": "4:db:id"}
func (s *Server) generateRowMeta(row []interface{}) []interface{} {
	meta := make([]interface{}, len(row))
	for i, val := range row {
		switch v := val.(type) {
		case map[string]interface{}:
			// Check for elementId (Neo4j format node/edge)
			if elementId, ok := v["elementId"].(string); ok {
				// Determine if it's a node or relationship based on elementId prefix
				entityType := "node"
				if strings.HasPrefix(elementId, "5:") {
					entityType = "relationship"
				}
				// Extract numeric ID from elementId (4:nornicdb:uuid -> hash to int)
				idPart := strings.TrimPrefix(elementId, "4:nornicdb:")
				idPart = strings.TrimPrefix(idPart, "5:nornicdb:")
				numericId := s.hashStringToInt64(idPart)

				meta[i] = map[string]interface{}{
					"id":        numericId,
					"type":      entityType,
					"deleted":   false,
					"elementId": elementId,
				}
			} else {
				meta[i] = nil
			}
		default:
			meta[i] = nil
		}
	}
	return meta
}

// hashStringToInt64 converts a string ID to an int64 for Neo4j compatibility.
// Neo4j drivers expect numeric IDs in metadata.
func (s *Server) hashStringToInt64(id string) int64 {
	var hash int64
	for _, c := range id {
		hash = hash*31 + int64(c)
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}

// generateBookmark generates a bookmark for causal consistency
func (s *Server) generateBookmark() string {
	return fmt.Sprintf("FB:nornicdb:%d", time.Now().UnixNano())
}

// Transaction management (explicit transactions)
//
// Explicit HTTP transactions are bound to a per-tx executor instance:
//   1) open transaction => BEGIN on dedicated executor
//   2) execute in tx    => run statements on the same executor/tx context
//   3) commit           => optional final statements, then COMMIT
//   4) rollback         => ROLLBACK and discard tx session
//
// This ensures rollback semantics are real (writes are not persisted on rollback)
// and keeps implicit transaction behavior unchanged.

func (s *Server) transactionCommitURL(dbName, txID string) string {
	host := s.config.Address
	if host == "0.0.0.0" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s:%d/db/%s/tx/%s/commit", host, s.config.Port, dbName, txID)
}

func (s *Server) appendStatementResult(response *TransactionResponse, result *cypher.ExecuteResult) {
	qr := QueryResult{
		Columns: result.Columns,
		Data:    make([]ResultRow, len(result.Rows)),
	}
	for i, row := range result.Rows {
		convertedRow := s.convertRowToNeo4jFormat(row)
		qr.Data[i] = ResultRow{Row: convertedRow, Meta: s.generateRowMeta(convertedRow)}
	}
	response.Results = append(response.Results, qr)
	if result.Metadata != nil {
		if receipt, ok := result.Metadata["receipt"]; ok && receipt != nil {
			response.Receipt = receipt
		}
	}
}

func (s *Server) executeTxStatements(
	ctx context.Context,
	authToken string,
	claims *auth.JWTClaims,
	dbName string,
	session *txsession.Session,
	statements []StatementRequest,
	response *TransactionResponse,
) {
	ctx = cypher.WithAuthToken(ctx, authToken)
	for _, stmt := range statements {
		if isMutationQuery(stmt.Statement) {
			if claims == nil {
				response.Errors = append(response.Errors, QueryError{
					Code:    "Neo.ClientError.Security.Forbidden",
					Message: "Write permission required",
				})
				continue
			}
			if !s.getResolvedAccess(claims, dbName).Write {
				response.Errors = append(response.Errors, QueryError{
					Code:    "Neo.ClientError.Security.Forbidden",
					Message: fmt.Sprintf("Write on database '%s' is not allowed.", dbName),
				})
				continue
			}
		}

		result, err := s.txSessions.ExecuteInSession(ctx, session, stmt.Statement, stmt.Parameters)
		if err != nil {
			code, message := mapSessionExecError(err)
			response.Errors = append(response.Errors, QueryError{
				Code:    code,
				Message: message,
			})
			continue
		}
		s.appendStatementResult(response, result)
	}
}

func mapSessionExecError(err error) (code, message string) {
	if err == nil {
		return "Neo.ClientError.Statement.SyntaxError", ""
	}
	msg := err.Error()
	// If the engine already returned a Neo4j-style code prefix
	// (for example Neo.ClientError.Transaction.ForbiddenDueToTransactionType: ...),
	// preserve it for protocol compatibility.
	if strings.HasPrefix(msg, "Neo.") {
		if idx := strings.Index(msg, ":"); idx > 0 {
			return strings.TrimSpace(msg[:idx]), strings.TrimSpace(msg[idx+1:])
		}
		return msg, msg
	}
	// Wrapped errors may prefix extra context before the Neo4j error code.
	// Example:
	// "apply input failed: ... Neo.ClientError.Transaction.ForbiddenDueToTransactionType: ..."
	if start := strings.Index(msg, "Neo."); start >= 0 {
		rest := msg[start:]
		if idx := strings.Index(rest, ":"); idx > 0 {
			return strings.TrimSpace(rest[:idx]), strings.TrimSpace(rest[idx+1:])
		}
	}
	return "Neo.ClientError.Statement.SyntaxError", msg
}

func (s *Server) handleOpenTransaction(w http.ResponseWriter, r *http.Request, dbName string) {
	claims := getClaims(r)

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.getDatabaseAccessMode(claims).CanAccessDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
		return
	}

	var req TransactionRequest
	_ = s.readJSON(r, &req) // Optional body

	var txSession *txsession.Session
	var err error
	authToken := r.Header.Get("Authorization")
	if authToken != "" && s.databaseHasRemoteConstituent(dbName) {
		executor, execErr := s.getExecutorForDatabaseWithAuth(dbName, authToken)
		if execErr != nil {
			response := TransactionResponse{
				Results: make([]QueryResult, 0),
				Errors: []QueryError{{
					Code:    "Neo.ClientError.Transaction.TransactionStartFailed",
					Message: execErr.Error(),
				}},
			}
			s.writeJSON(w, http.StatusInternalServerError, response)
			return
		}
		txSession, err = s.txSessions.OpenWithExecutor(r.Context(), dbName, executor)
	} else {
		txSession, err = s.txSessions.Open(r.Context(), dbName)
	}
	if err != nil {
		if errors.Is(err, multidb.ErrDatabaseNotFound) {
			response := TransactionResponse{
				Results: make([]QueryResult, 0),
				Errors: []QueryError{{
					Code:    "Neo.ClientError.Database.DatabaseNotFound",
					Message: fmt.Sprintf("Database '%s' not found", dbName),
				}},
			}
			s.writeJSON(w, http.StatusNotFound, response)
			return
		}
		response := TransactionResponse{
			Results: make([]QueryResult, 0),
			Errors: []QueryError{{
				Code:    "Neo.ClientError.Transaction.TransactionStartFailed",
				Message: err.Error(),
			}},
		}
		s.writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
		Commit:  s.transactionCommitURL(dbName, txSession.ID),
		Transaction: &TransactionInfo{
			Expires: txSession.Expires.Format(time.RFC1123),
		},
	}

	if len(req.Statements) > 0 {
		s.executeTxStatements(r.Context(), r.Header.Get("Authorization"), claims, dbName, txSession, req.Statements, &response)
		response.Transaction.Expires = txSession.Expires.Format(time.RFC1123)
	}

	s.writeJSON(w, http.StatusCreated, response)
}

func (s *Server) handleExecuteInTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	claims := getClaims(r)
	if !s.getDatabaseAccessMode(claims).CanAccessDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
		return
	}

	tx, ok := s.txSessions.Get(txID)
	if !ok || tx == nil || tx.Database != dbName {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "transaction not found")
		return
	}

	var req TransactionRequest
	_ = s.readJSON(r, &req)

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
		Commit:  s.transactionCommitURL(dbName, txID),
		Transaction: &TransactionInfo{
			Expires: tx.Expires.Format(time.RFC1123),
		},
	}

	s.executeTxStatements(r.Context(), r.Header.Get("Authorization"), claims, dbName, tx, req.Statements, &response)
	response.Transaction.Expires = tx.Expires.Format(time.RFC1123)

	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCommitTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	claims := getClaims(r)

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.getDatabaseAccessMode(claims).CanAccessDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
		return
	}

	var req TransactionRequest
	_ = s.readJSON(r, &req) // Optional final statements

	response := TransactionResponse{
		Results:       make([]QueryResult, 0),
		Errors:        make([]QueryError, 0),
		LastBookmarks: []string{s.generateBookmark()},
	}

	tx, ok := s.txSessions.Get(txID)
	if !ok || tx == nil || tx.Database != dbName {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "transaction not found")
		return
	}

	// Execute optional final statements in transaction context first.
	s.executeTxStatements(r.Context(), r.Header.Get("Authorization"), claims, dbName, tx, req.Statements, &response)
	if len(response.Errors) > 0 {
		_ = s.txSessions.RollbackAndDelete(r.Context(), tx)
		s.writeJSON(w, http.StatusOK, response)
		return
	}

	commitResult, err := s.txSessions.CommitAndDelete(r.Context(), tx)
	if err != nil {
		response.Errors = append(response.Errors, QueryError{
			Code:    "Neo.ClientError.Transaction.TransactionCommitFailed",
			Message: err.Error(),
		})
		s.writeJSON(w, http.StatusOK, response)
		return
	}
	if commitResult != nil {
		if commitResult.Metadata != nil {
			if receipt, ok := commitResult.Metadata["receipt"]; ok && receipt != nil {
				response.Receipt = receipt
			}
		}
	}

	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleRollbackTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.getDatabaseAccessMode(getClaims(r)).CanAccessDatabase(dbName) {
		s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
		return
	}

	tx, ok := s.txSessions.Get(txID)
	if !ok || tx == nil || tx.Database != dbName {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "transaction not found")
		return
	}

	_ = s.txSessions.RollbackAndDelete(r.Context(), tx)

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
	}
	s.writeJSON(w, http.StatusOK, response)
}
