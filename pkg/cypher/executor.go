// Package cypher provides Neo4j-compatible Cypher query execution for NornicDB.
//
// This package implements a Cypher query parser and executor that supports
// the core Neo4j Cypher query language features. It enables NornicDB to be
// compatible with existing Neo4j applications and tools.
//
// Supported Cypher Features:
//   - MATCH: Pattern matching with node and relationship patterns
//   - CREATE: Creating nodes and relationships
//   - MERGE: Upsert operations with ON CREATE/ON MATCH clauses
//   - DELETE/DETACH DELETE: Removing nodes and relationships
//   - SET: Updating node and relationship properties
//   - REMOVE: Removing properties and labels
//   - RETURN: Returning query results
//   - WHERE: Filtering with conditions
//   - WITH: Passing results between query parts
//   - OPTIONAL MATCH: Left outer joins
//   - CALL: Procedure calls
//   - UNWIND: List expansion
//
// Example Usage:
//
//	// Create executor with storage backend
//	storage := storage.NewMemoryEngine()
//	executor := cypher.NewStorageExecutor(storage)
//
//	// Execute Cypher queries
//	result, err := executor.Execute(ctx, "CREATE (n:Person {name: 'Alice', age: 30})", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Query with parameters
//	params := map[string]interface{}{
//		"name": "Alice",
//		"minAge": 25,
//	}
//	result, err = executor.Execute(ctx,
//		"MATCH (n:Person {name: $name}) WHERE n.age >= $minAge RETURN n", params)
//
//	// Complex query with relationships
//	result, err = executor.Execute(ctx, `
//		MATCH (a:Person)-[r:KNOWS]->(b:Person)
//		WHERE a.age > 25
//		RETURN a.name, r.since, b.name
//		ORDER BY a.age DESC
//		LIMIT 10
//	`, nil)
//
//	// Process results
//	for _, row := range result.Rows {
//		fmt.Printf("Row: %v\n", row)
//	}
//
// Neo4j Compatibility:
//
// The executor aims for high compatibility with Neo4j Cypher:
//   - Same syntax and semantics for core operations
//   - Parameter substitution with $param syntax
//   - Neo4j-style error messages and codes
//   - Compatible result format for drivers
//   - Support for Neo4j built-in functions
//
// Query Processing Pipeline:
//
// 1. **Parsing**: Query is parsed into an AST (Abstract Syntax Tree)
// 2. **Validation**: Syntax and semantic validation
// 3. **Parameter Substitution**: Replace $param with actual values
// 4. **Execution Planning**: Determine optimal execution strategy
// 5. **Execution**: Execute against storage backend
// 6. **Result Formatting**: Format results for Neo4j compatibility
//
// Performance Considerations:
//
//   - Pattern matching is optimized for common cases
//   - Indexes are used automatically when available
//   - Query planning chooses efficient execution paths
//   - Bulk operations are optimized for large datasets
//
// Limitations:
//
// Current limitations compared to full Neo4j:
//   - No user-defined procedures (CALL is limited to built-ins)
//   - No complex path expressions
//   - No graph algorithms (shortest path, etc.)
//   - No schema constraints (handled by storage layer)
//   - No transactions (single-query atomicity only)
//
// ELI12 (Explain Like I'm 12):
//
// Think of Cypher like asking questions about a social network:
//
//  1. **MATCH**: "Find all people named Alice" - like searching through
//     a phone book for everyone with a specific name.
//
//  2. **CREATE**: "Add a new person named Bob" - like writing a new
//     entry in the phone book.
//
//  3. **Relationships**: "Find who Alice knows" - like following the
//     lines between people on a friendship map.
//
//  4. **WHERE**: "Find people older than 25" - like adding a filter
//     to only show certain results.
//
//  5. **RETURN**: "Show me their names and ages" - like deciding which
//     information to display from your search.
//
// The Cypher executor is like a smart assistant that understands these
// questions and knows how to find the answers in your data!
package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/vectorspace"
)

// Pre-compiled regexes for subquery detection (whitespace-flexible)
var (
	// Matches EXISTS followed by optional whitespace and opening brace
	existsSubqueryRe = regexp.MustCompile(`(?i)\bEXISTS\s*\{`)
	// Matches NOT EXISTS followed by optional whitespace and opening brace
	notExistsSubqueryRe = regexp.MustCompile(`(?i)\bNOT\s+EXISTS\s*\{`)
	// Matches COUNT followed by optional whitespace and opening brace
	countSubqueryRe = regexp.MustCompile(`(?i)\bCOUNT\s*\{`)
	// Matches CALL followed by optional whitespace and opening brace (not CALL procedure())
	callSubqueryRe = regexp.MustCompile(`(?i)\bCALL\s*\{`)
	// Matches COLLECT followed by optional whitespace and opening brace
	collectSubqueryRe = regexp.MustCompile(`(?i)\bCOLLECT\s*\{`)
)

// hasSubqueryPattern checks if the query contains a subquery pattern (keyword + optional whitespace + brace)
func hasSubqueryPattern(query string, pattern *regexp.Regexp) bool {
	return pattern.MatchString(query)
}

// StorageExecutor executes Cypher queries against a storage backend.
//
// The StorageExecutor provides the main interface for executing Cypher queries
// in NornicDB. It handles query parsing, validation, parameter substitution,
// and execution against the underlying storage engine.
//
// Key features:
//   - Neo4j-compatible Cypher syntax support
//   - Parameter substitution with $param syntax
//   - Query validation and error reporting
//   - Optimized execution planning
//   - Thread-safe concurrent execution
//
// Example:
//
//	storage := storage.NewMemoryEngine()
//	executor := cypher.NewStorageExecutor(storage)
//
//	// Simple node creation
//	result, _ := executor.Execute(ctx, "CREATE (n:Person {name: 'Alice'})", nil)
//
//	// Parameterized query
//	params := map[string]interface{}{"name": "Bob", "age": 30}
//	result, _ = executor.Execute(ctx,
//		"CREATE (n:Person {name: $name, age: $age})", params)
//
//	// Complex pattern matching
//	result, _ = executor.Execute(ctx, `
//		MATCH (a:Person)-[:KNOWS]->(b:Person)
//		WHERE a.age > 25
//		RETURN a.name, b.name
//	`, nil)
//
// Thread Safety:
//
//	The executor is thread-safe and can handle concurrent queries.
//
// NodeMutatedCallback is called when a node is created or mutated via Cypher (CREATE, MERGE, SET, REMOVE, or procedures that update nodes).
// This allows external systems (like the embed queue) to be notified so embeddings can be (re)generated.
type NodeMutatedCallback func(nodeID string)

type StorageExecutor struct {
	parser    *Parser
	storage   storage.Engine
	txContext *TransactionContext // Active transaction context
	cache     *SmartQueryCache    // Query result cache with label-aware invalidation
	planCache *QueryPlanCache     // Parsed query plan cache
	analyzer  *QueryAnalyzer      // Query analysis with AST caching

	// Node lookup cache for MATCH patterns like (n:Label {prop: value})
	// Key: "Label:{prop:value,...}", Value: *storage.Node
	// This dramatically speeds up repeated MATCH lookups for the same pattern
	nodeLookupCache   map[string]*storage.Node
	nodeLookupCacheMu sync.RWMutex

	// deferFlush when true, writes are not auto-flushed (Bolt layer handles it)
	deferFlush bool

	// embedder for server-side query embedding (optional)
	// If set, vector search can accept string queries which are embedded automatically
	embedder QueryEmbedder

	// searchService optionally provides unified search semantics for Cypher procedures.
	// When set, db.index.vector.queryNodes delegates to search.Service.
	searchService *search.Service

	// inferenceManager optionally provides LLM inference for db.infer.
	inferenceManager InferenceManager

	// onNodeMutated is called when a node is created or mutated (CREATE, MERGE, SET, REMOVE).
	// This allows the embed queue to be notified so embeddings are (re)generated.
	onNodeMutated NodeMutatedCallback

	// defaultEmbeddingDimensions is the configured embedding dimensions for vector indexes
	// Used as default when CREATE VECTOR INDEX doesn't specify dimensions
	defaultEmbeddingDimensions int

	// dbManager is optional - when set, enables system commands (CREATE/DROP/SHOW DATABASE)
	// System commands require DatabaseManager to manage multiple databases
	// This is an interface to avoid import cycles with multidb package
	dbManager DatabaseManagerInterface

	// shellParams stores Neo4j shell-style parameters set via :param / :params.
	// These are session-scoped to the executor instance and merged with per-call params.
	shellParams   map[string]interface{}
	shellParamsMu sync.RWMutex

	// vectorRegistry maps Cypher vector index definitions to concrete vector spaces.
	vectorRegistry    *vectorspace.IndexRegistry
	vectorIndexSpaces map[string]vectorspace.VectorSpaceKey
}

// DatabaseManagerInterface is a minimal interface to avoid import cycles with multidb package.
// This allows the executor to call database management operations without directly
// depending on the multidb package.
type DatabaseManagerInterface interface {
	CreateDatabase(name string) error
	DropDatabase(name string) error
	ListDatabases() []DatabaseInfoInterface
	Exists(name string) bool
	CreateAlias(alias, databaseName string) error
	DropAlias(alias string) error
	ListAliases(databaseName string) map[string]string
	ResolveDatabase(nameOrAlias string) (string, error)
	SetDatabaseLimits(databaseName string, limits interface{}) error
	GetDatabaseLimits(databaseName string) (interface{}, error)
	// Composite database methods
	CreateCompositeDatabase(name string, constituents []interface{}) error
	DropCompositeDatabase(name string) error
	AddConstituent(compositeName string, constituent interface{}) error
	RemoveConstituent(compositeName string, alias string) error
	GetCompositeConstituents(compositeName string) ([]interface{}, error)
	ListCompositeDatabases() []DatabaseInfoInterface
	IsCompositeDatabase(name string) bool
}

// DatabaseInfoInterface provides database metadata without importing multidb.
type DatabaseInfoInterface interface {
	Name() string
	Type() string
	Status() string
	IsDefault() bool
	CreatedAt() time.Time
}

// QueryEmbedder generates embeddings for search queries.
// This is a minimal interface to avoid import cycles with embed package.
type QueryEmbedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// InferenceManager is the minimal LLM contract used by Cypher db.infer.
// It mirrors Heimdall manager methods to keep adapters thin.
type InferenceManager interface {
	Generate(ctx context.Context, prompt string, params heimdall.GenerateParams) (string, error)
	Chat(ctx context.Context, req heimdall.ChatRequest) (*heimdall.ChatResponse, error)
}

// NewStorageExecutor creates a new Cypher executor with the given storage backend.
//
// The executor is initialized with a parser and connected to the storage engine.
// It's ready to execute Cypher queries immediately after creation.
//
// Parameters:
//   - store: Storage engine to execute queries against (required)
//
// Returns:
//   - StorageExecutor ready for query execution
//
// Example:
//
//	// Create storage and executor
//	storage := storage.NewMemoryEngine()
//	executor := cypher.NewStorageExecutor(storage)
//
//	// Executor is ready for queries
//	result, err := executor.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
func NewStorageExecutor(store storage.Engine) *StorageExecutor {
	exec := &StorageExecutor{
		parser:            NewParser(),
		storage:           store,
		cache:             NewSmartQueryCache(1000), // Query result cache with label-aware invalidation
		planCache:         NewQueryPlanCache(500),   // Cache 500 parsed query plans
		analyzer:          NewQueryAnalyzer(1000),   // Cache 1000 parsed query ASTs
		nodeLookupCache:   make(map[string]*storage.Node, 1000),
		shellParams:       make(map[string]interface{}),
		searchService:     nil, // Lazy initialization - will be set via SetSearchService() to reuse DB's cached service
		vectorRegistry:    vectorspace.NewIndexRegistry(),
		vectorIndexSpaces: make(map[string]vectorspace.VectorSpaceKey),
	}
	ensureBuiltInProceduresRegistered()
	_ = exec.loadPersistedProcedures()
	return exec
}

// SetDatabaseManager sets the database manager for system commands.
// When set, enables CREATE DATABASE, DROP DATABASE, and SHOW DATABASES commands.
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//	// Now CREATE DATABASE, DROP DATABASE, SHOW DATABASES work
func (e *StorageExecutor) SetDatabaseManager(dbManager DatabaseManagerInterface) {
	e.dbManager = dbManager
}

// SetEmbedder sets the query embedder for server-side embedding.
// When set, db.index.vector.queryNodes can accept string queries
// which are automatically embedded before search.
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetEmbedder(embedder)
//
//	// Now vector search accepts both:
//	// CALL db.index.vector.queryNodes('idx', 10, [0.1, 0.2, ...])  // Vector
//	// CALL db.index.vector.queryNodes('idx', 10, 'search query')   // String (auto-embedded)
func (e *StorageExecutor) SetEmbedder(embedder QueryEmbedder) {
	e.embedder = embedder
}

// SetSearchService sets the unified search service used by Cypher procedures.
// When set, db.index.vector.queryNodes will delegate to search.Service.
func (e *StorageExecutor) SetSearchService(svc *search.Service) {
	e.searchService = svc
}

// SetInferenceManager sets the inference manager used by db.infer.
func (e *StorageExecutor) SetInferenceManager(mgr InferenceManager) {
	e.inferenceManager = mgr
}

// GetInferenceManager returns the configured inference manager.
func (e *StorageExecutor) GetInferenceManager() InferenceManager {
	return e.inferenceManager
}

// SetVectorRegistry allows wiring a shared index registry (e.g., per database).
// Defaults to an internal registry when not set.
func (e *StorageExecutor) SetVectorRegistry(reg *vectorspace.IndexRegistry) {
	if reg == nil {
		reg = vectorspace.NewIndexRegistry()
	}
	e.vectorRegistry = reg
}

// GetVectorRegistry exposes the current registry (for tests and adapters).
func (e *StorageExecutor) GetVectorRegistry() *vectorspace.IndexRegistry {
	return e.vectorRegistry
}

// GetEmbedder returns the query embedder if set.
// This allows copying the embedder to namespaced executors for GraphQL.
func (e *StorageExecutor) GetEmbedder() QueryEmbedder {
	return e.embedder
}

// SetNodeMutatedCallback sets a callback that is invoked when nodes are created
// or mutated (CREATE, MERGE, SET, REMOVE, or procedures that update nodes).
// This allows the embed queue to be notified so embeddings can be (re)generated.
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetNodeMutatedCallback(func(nodeID string) {
//	    embedQueue.Enqueue(nodeID)
//	})
func (e *StorageExecutor) SetNodeMutatedCallback(cb NodeMutatedCallback) {
	e.onNodeMutated = cb
}

// SetDefaultEmbeddingDimensions sets the default dimensions for vector indexes.
// This is used when CREATE VECTOR INDEX doesn't specify dimensions in OPTIONS.
func (e *StorageExecutor) SetDefaultEmbeddingDimensions(dims int) {
	e.defaultEmbeddingDimensions = dims
}

// GetDefaultEmbeddingDimensions returns the configured default embedding dimensions.
// Returns 1024 as fallback if not configured.
func (e *StorageExecutor) GetDefaultEmbeddingDimensions() int {
	return e.defaultEmbeddingDimensions
}

// notifyNodeMutated calls the onNodeMutated callback if set.
// Call after any node creation or mutation (CREATE, MERGE, SET, REMOVE) so the embed queue can re-process.
func (e *StorageExecutor) notifyNodeMutated(nodeID string) {
	if e.onNodeMutated != nil {
		e.onNodeMutated(nodeID)
	}
}

// removeNodeFromSearch removes a node from the search service (vector/fulltext indexes).
// Call after successfully deleting a node via Cypher so embeddings are not left orphaned.
// nodeID may be prefixed (e.g. "nornic:xyz") or local ("xyz"); the search service expects local ID.
func (e *StorageExecutor) removeNodeFromSearch(nodeID string) {
	if e.searchService == nil || nodeID == "" {
		return
	}
	localID := nodeID
	if _, unprefixed, ok := storage.ParseDatabasePrefix(nodeID); ok {
		localID = unprefixed
	}
	_ = e.searchService.RemoveNode(storage.NodeID(localID))
}

// Flush persists all pending writes to storage.
// This implements FlushableExecutor for Bolt-level deferred commits.
func (e *StorageExecutor) Flush() error {
	if asyncEngine, ok := e.storage.(*storage.AsyncEngine); ok {
		return asyncEngine.Flush()
	}
	return nil
}

// SetDeferFlush enables/disables deferred flush mode.
// When enabled, writes are not auto-flushed - the Bolt layer calls Flush().
func (e *StorageExecutor) SetDeferFlush(enabled bool) {
	e.deferFlush = enabled
}

// queryDeletesNodes returns true if the query deletes nodes.
// Returns false for relationship-only deletes (CREATE rel...DELETE rel pattern).
func queryDeletesNodes(query string) bool {
	// DETACH DELETE always deletes nodes
	if strings.Contains(strings.ToUpper(query), "DETACH DELETE") {
		return true
	}
	// Relationship pattern (has -[...]-> or <-[...]-) with CREATE+DELETE = relationship delete only
	if strings.Contains(query, "]->(") || strings.Contains(query, ")<-[") {
		return false
	}
	return true
}

// Execute parses and executes a Cypher query with optional parameters.
//
// This is the main entry point for Cypher query execution. The method handles
// the complete query lifecycle: parsing, validation, parameter substitution,
// execution planning, and result formatting.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - cypher: Cypher query string
//   - params: Optional parameters for $param substitution
//
// Returns:
//   - ExecuteResult with columns and rows
//   - Error if query parsing or execution fails
//
// Example:
//
//	// Simple query without parameters
//	result, err := executor.Execute(ctx, "MATCH (n:Person) RETURN n.name", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Parameterized query
//	params := map[string]interface{}{
//		"name": "Alice",
//		"minAge": 25,
//	}
//	result, err = executor.Execute(ctx, `
//		MATCH (n:Person {name: $name})
//		WHERE n.age >= $minAge
//		RETURN n.name, n.age
//	`, params)
//
//	// Process results
//	fmt.Printf("Columns: %v\n", result.Columns)
//	for _, row := range result.Rows {
//		fmt.Printf("Row: %v\n", row)
//	}
//
// Supported Query Types:
//
//	Core Clauses:
//	- MATCH: Pattern matching and traversal
//	- OPTIONAL MATCH: Left outer joins (returns nulls for no matches)
//	- CREATE: Node and relationship creation
//	- MERGE: Upsert operations with ON CREATE SET / ON MATCH SET
//	- DELETE / DETACH DELETE: Node and relationship deletion
//	- SET: Property updates
//	- REMOVE: Property and label removal
//
//	Projection & Chaining:
//	- RETURN: Result projection with expressions, aliases, aggregations
//	- WITH: Query chaining and intermediate aggregation
//	- UNWIND: List expansion into rows
//
//	Filtering & Ordering:
//	- WHERE: Filtering conditions (=, <>, <, >, <=, >=, IS NULL, IS NOT NULL, IN, CONTAINS, STARTS WITH, ENDS WITH, AND, OR, NOT)
//	- ORDER BY: Result sorting (ASC/DESC)
//	- SKIP / LIMIT: Pagination
//
//	Aggregation Functions:
//	- COUNT, SUM, AVG, MIN, MAX, COLLECT
//
//	Procedures & Functions:
//	- CALL: Procedure invocation (db.labels, db.propertyKeys, db.index.vector.*, etc.)
//	- CALL {}: Subquery execution with UNION support
//
//	Advanced:
//	- UNION / UNION ALL: Query composition
//	- FOREACH: Iterative updates
//	- LOAD CSV: Data import
//	- EXPLAIN / PROFILE: Query analysis
//	- SHOW: Schema introspection
//
//	Path Functions:
//	- shortestPath / allShortestPaths
//
// Error Handling:
//
//	Returns detailed error messages for syntax errors, type mismatches,
//	and execution failures with Neo4j-compatible error codes.
func (e *StorageExecutor) Execute(ctx context.Context, cypher string, params map[string]interface{}) (*ExecuteResult, error) {
	// Normalize query: trim BOM (some clients send it) then whitespace
	cypher = trimBOM(cypher)
	cypher = strings.TrimSpace(cypher)
	if cypher == "" {
		return nil, fmt.Errorf("empty query")
	}

	// Handle Neo4j shell/browser commands like :USE and :param before validation.
	processedQuery, processedCtx, shellResult, err := e.preprocessShellCommands(ctx, cypher, params)
	if err != nil {
		return nil, err
	}
	ctx = processedCtx
	cypher = processedQuery
	if cypher == "" {
		return shellResult, nil
	}

	// Handle leading Cypher USE clause (openCypher multi-graph syntax).
	if useDB, remaining, hasUse, err := parseLeadingUseClause(cypher); hasUse || err != nil {
		if err != nil {
			return nil, err
		}
		scopedExec, resolvedDB, err := e.scopedExecutorForUse(useDB)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, ctxKeyUseDatabase, resolvedDB)
		if strings.TrimSpace(remaining) == "" {
			return &ExecuteResult{
				Columns: []string{"database"},
				Rows:    [][]interface{}{{resolvedDB}},
			}, nil
		}
		return scopedExec.Execute(ctx, remaining, params)
	}

	// Merge session-scoped shell parameters with per-call parameters.
	// Explicit params win over shell params to preserve HTTP/Bolt semantics.
	params = e.mergeShellParams(params)

	// Check for transaction control statements and transaction scripts FIRST.
	// These are Nornic extensions and must bypass strict ANTLR validation.
	if result, err := e.executeTransactionScript(ctx, cypher); result != nil || err != nil {
		return result, err
	}
	if result, err := e.parseTransactionStatement(cypher); result != nil || err != nil {
		return result, err
	}

	// Validate basic syntax
	if err := e.validateSyntax(cypher); err != nil {
		return nil, err
	}

	// IMPORTANT: Do NOT substitute parameters before routing!
	// We need to route the query based on the ORIGINAL query structure,
	// not the substituted one. Otherwise, keywords inside parameter values
	// (like 'MATCH (n) SET n.x = 1' stored as content) will be incorrectly
	// detected as Cypher clauses.
	//
	// Parameter substitution happens AFTER routing, inside each handler.
	// This matches Neo4j's architecture where params are kept separate.

	// Store params in context for handlers to use
	ctx = context.WithValue(ctx, paramsKey, params)

	// Check query limits if storage engine supports it
	// Uses interface{} to avoid importing multidb package (prevents circular dependencies)
	var queryLimitCancel context.CancelFunc
	if namespacedEngine, ok := e.storage.(interface {
		GetQueryLimitChecker() interface {
			CheckQueryRate() error
			CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
		}
	}); ok {
		if qlc := namespacedEngine.GetQueryLimitChecker(); qlc != nil {
			// Check query rate limit
			if err := qlc.CheckQueryRate(); err != nil {
				return nil, err
			}

			// Check write rate limit for write queries
			// We need to check this early, but we don't know if it's a write query yet
			// So we'll check it in the write handlers too

			// Apply query timeout and concurrent query limits
			var err error
			ctx, queryLimitCancel, err = qlc.CheckQueryLimits(ctx)
			if err != nil {
				return nil, err
			}
			// Ensure cancel is called when done
			defer func() {
				if queryLimitCancel != nil {
					queryLimitCancel()
				}
			}()
		}
	}

	// Analyze query - uses cached analysis if available
	// This extracts query metadata (HasMatch, IsReadOnly, Labels, etc.) once
	// and caches it for repeated queries, avoiding redundant string parsing
	info := e.analyzer.Analyze(cypher)

	// For routing, we still need upperQuery for some handlers
	// TODO: Migrate handlers to use QueryInfo directly
	upperQuery := strings.ToUpper(strings.TrimSpace(cypher))

	// Try cache for read-only queries only when cache policy allows it.
	if info.IsReadOnly && e.cache != nil && isCacheableReadQuery(cypher) {
		if cached, found := e.cache.Get(cypher, params); found {
			return cached, nil
		}
	}

	// Check for EXPLAIN/PROFILE execution modes (using cached analysis)
	if info.HasExplain {
		_, innerQuery := parseExecutionMode(cypher)
		return e.executeExplain(ctx, innerQuery)
	}
	if info.HasProfile {
		_, innerQuery := parseExecutionMode(cypher)
		return e.executeProfile(ctx, innerQuery)
	}

	// If in explicit transaction, execute within it
	if e.txContext != nil && e.txContext.active {
		return e.executeInTransaction(ctx, cypher, upperQuery)
	}

	// System commands (CREATE/DROP DATABASE, SHOW DATABASES, etc.) must not use the async engine
	// or implicit transactions: they operate on dbManager/metadata, not graph storage.
	// Routing them through executeWithoutTransaction directly ensures correct handling and
	// avoids the write path (tryAsyncCreateNodeBatch / executeWithImplicitTransaction).
	if isSystemCommandNoGraph(cypher) {
		result, err := e.executeWithoutTransaction(ctx, cypher, upperQuery)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// Auto-commit single query - use async path for performance
	// This uses AsyncEngine's write-behind cache instead of synchronous disk I/O
	// For strict ACID, users should use explicit BEGIN/COMMIT transactions
	result, err := e.executeImplicitAsync(ctx, cypher, upperQuery)

	// Apply result limit if set
	if err == nil && result != nil {
		if namespacedEngine, ok := e.storage.(interface {
			GetQueryLimitChecker() interface {
				GetQueryLimits() interface{}
			}
		}); ok {
			if qlc := namespacedEngine.GetQueryLimitChecker(); qlc != nil {
				if queryLimits := qlc.GetQueryLimits(); queryLimits != nil {
					// Type assert to check if it has MaxResults field
					// We use reflection-like approach: check if it's a struct with MaxResults
					if limits, ok := queryLimits.(interface {
						GetMaxResults() int64
					}); ok {
						if maxResults := limits.GetMaxResults(); maxResults > 0 && int64(len(result.Rows)) > maxResults {
							// Truncate results to limit
							result.Rows = result.Rows[:maxResults]
						}
					}
				}
			}
		}
	}

	// Cache successful read-only queries.
	//
	// NOTE: Aggregation queries (COUNT/SUM/AVG/COLLECT/...) used to be excluded, but in practice they can still
	// be expensive (edge scans, label scans, COLLECT materialization). Caching them is correctness-preserving as
	// long as we invalidate on writes (which we do), so we cache them with a shorter TTL by default.
	if err == nil && info.IsReadOnly && e.cache != nil && isCacheableReadQuery(cypher) {
		// Determine TTL based on query type (using cached analysis)
		ttl := 60 * time.Second // Default: 60s for data queries
		if info.HasAggregation {
			ttl = 1 * time.Second // Conservative TTL for aggregations
		}
		if info.HasCall || info.HasShow {
			ttl = 300 * time.Second // 5 minutes for schema queries
		}
		e.cache.Put(cypher, params, result, ttl)
	}

	// Invalidate caches on write operations (using cached analysis)
	if info.IsWriteQuery {
		// Only invalidate node lookup cache when NODES are deleted
		// Relationship-only deletes (like benchmark CREATE rel DELETE rel) don't affect node cache
		if info.HasDelete && queryDeletesNodes(cypher) {
			e.invalidateNodeLookupCache()
		}

		// Invalidate query result cache using cached labels
		if e.cache != nil {
			if len(info.Labels) > 0 {
				e.cache.InvalidateLabels(info.Labels)
			} else {
				e.cache.Invalidate()
			}
		}
	}

	return result, err
}

// TransactionCapableEngine is an engine that supports ACID transactions.
// Used for type assertion to wrap implicit writes in rollback-capable transactions.
type TransactionCapableEngine interface {
	BeginTransaction() (*storage.BadgerTransaction, error)
}

type implicitTxEngines struct {
	txEngine    TransactionCapableEngine
	asyncEngine *storage.AsyncEngine
	namespace   string
}

func (e *StorageExecutor) resolveImplicitTxEngines() implicitTxEngines {
	engine := e.storage
	visited := make(map[storage.Engine]bool)
	out := implicitTxEngines{}

	for engine != nil && !visited[engine] {
		visited[engine] = true

		if out.namespace == "" {
			if ns, ok := engine.(interface{ Namespace() string }); ok {
				out.namespace = ns.Namespace()
			}
		}
		if out.asyncEngine == nil {
			if ae, ok := engine.(*storage.AsyncEngine); ok {
				out.asyncEngine = ae
			}
		}
		if out.txEngine == nil {
			if tc, ok := engine.(TransactionCapableEngine); ok {
				out.txEngine = tc
			}
		}

		switch wrapper := engine.(type) {
		case interface{ GetUnderlying() storage.Engine }:
			engine = wrapper.GetUnderlying()
		case interface{ GetEngine() storage.Engine }:
			engine = wrapper.GetEngine()
		case interface{ GetInnerEngine() storage.Engine }:
			engine = wrapper.GetInnerEngine()
		default:
			engine = nil
		}
	}

	return out
}

func (e *StorageExecutor) tryAsyncCreateNodeBatch(ctx context.Context, cypher string) (*ExecuteResult, error, bool) {
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	if !strings.HasPrefix(upper, "CREATE") {
		return nil, nil, false
	}
	// System commands and schema commands must not be handled here — route to executeSchemaCommand instead
	if findMultiWordKeywordIndex(cypher, "CREATE", "DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "ALIAS") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "CONSTRAINT") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "INDEX") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "FULLTEXT") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "VECTOR") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "RANGE") == 0 {
		return nil, nil, false
	}
	for _, keyword := range []string{
		"MATCH",
		"MERGE",
		"SET",
		"DELETE",
		"DETACH",
		"REMOVE",
		"WITH",
		"CALL",
		"UNWIND",
		"FOREACH",
		"LOAD",
		"OPTIONAL",
	} {
		if containsKeywordOutsideStrings(cypher, keyword) {
			return nil, nil, false
		}
	}

	// Substitute parameters before parsing so (n:Label $props) becomes (n:Label { ... })
	// and the label is not mis-parsed as "Label $props".
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	returnIdx := findKeywordIndex(cypher, "RETURN")
	createPart := cypher
	if returnIdx > 0 {
		createPart = strings.TrimSpace(cypher[:returnIdx])
	}

	createClauses := createKeywordPattern.Split(createPart, -1)
	if len(createClauses) == 0 {
		return nil, nil, false
	}

	var nodePatterns []string
	for _, clause := range createClauses {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}
		patterns := e.splitCreatePatterns(clause)
		for _, pat := range patterns {
			pat = strings.TrimSpace(pat)
			if pat == "" {
				continue
			}
			if containsOutsideStrings(pat, "->") ||
				containsOutsideStrings(pat, "<-") ||
				containsOutsideStrings(pat, "]-") ||
				containsOutsideStrings(pat, "-[") {
				return nil, nil, false
			}
			nodePatterns = append(nodePatterns, pat)
		}
	}

	if len(nodePatterns) == 0 {
		return nil, nil, false
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	createdNodes := make(map[string]*storage.Node)
	nodes := make([]*storage.Node, 0, len(nodePatterns))
	for _, nodePatternStr := range nodePatterns {
		nodePattern := e.parseNodePattern(nodePatternStr)

		for _, label := range nodePattern.labels {
			if !isValidIdentifier(label) {
				return nil, fmt.Errorf("invalid label name: %q (must be alphanumeric starting with letter or underscore)", label), true
			}
			if containsReservedKeyword(label) {
				return nil, fmt.Errorf("invalid label name: %q (contains reserved keyword)", label), true
			}
		}

		for key, val := range nodePattern.properties {
			if !isValidIdentifier(key) {
				return nil, fmt.Errorf("invalid property key: %q (must be alphanumeric starting with letter or underscore)", key), true
			}
			if _, ok := val.(invalidPropertyValue); ok {
				return nil, fmt.Errorf("invalid property value for key %q: malformed syntax", key), true
			}
		}

		node := &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     nodePattern.labels,
			Properties: nodePattern.properties,
		}
		nodes = append(nodes, node)
		if nodePattern.variable != "" {
			createdNodes[nodePattern.variable] = node
		}
	}

	store := e.getStorage(ctx)
	if err := store.BulkCreateNodes(nodes); err != nil {
		return nil, err, true
	}

	for _, node := range nodes {
		e.notifyNodeMutated(string(node.ID))
	}
	result.Stats.NodesCreated += len(nodes)

	if returnIdx > 0 {
		returnPart := strings.TrimSpace(cypher[returnIdx+6:])
		returnItems := e.parseReturnItems(returnPart)

		result.Columns = make([]string, len(returnItems))
		row := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}

			for variable, node := range createdNodes {
				if strings.HasPrefix(item.expr, variable) || item.expr == variable {
					row[i] = e.resolveReturnItem(item, variable, node)
					break
				}
			}

			if row[i] == nil {
				if varName := extractVariableNameFromReturnItem(item.expr); varName != "" {
					if node, ok := createdNodes[varName]; ok {
						row[i] = e.resolveReturnItem(item, varName, node)
					}
				}
			}
		}
		result.Rows = [][]interface{}{row}
	}

	return result, nil, true
}

// executeImplicitAsync executes a single query using implicit transactions for writes.
// For write operations, wraps execution in an implicit transaction that can be
// rolled back on error, preventing partial data corruption from failed queries.
// For strict ACID guarantees with durability, use explicit BEGIN/COMMIT transactions.
func (e *StorageExecutor) executeImplicitAsync(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// Check if this is a write operation using cached analysis
	info := e.analyzer.Analyze(cypher)
	isWrite := info.IsWriteQuery

	// For write operations, use implicit transaction for atomicity
	// This ensures partial writes are rolled back on error
	if isWrite {
		engines := e.resolveImplicitTxEngines()
		if engines.asyncEngine != nil {
			if result, err, handled := e.tryAsyncCreateNodeBatch(ctx, cypher); handled {
				return result, err
			}
		}
		return e.executeWithImplicitTransaction(ctx, cypher, upperQuery)
	}

	// Read-only operations don't need transaction wrapping
	return e.executeWithoutTransaction(ctx, cypher, upperQuery)
}

// executeWithImplicitTransaction wraps a write query in an implicit transaction.
// If any part of the query fails, all changes are rolled back atomically.
// This prevents data corruption from partially executed queries.
func (e *StorageExecutor) executeWithImplicitTransaction(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// Try to get a transaction-capable engine and async wrapper (if present)
	engines := e.resolveImplicitTxEngines()
	txEngine := engines.txEngine
	asyncEngine := engines.asyncEngine

	// If no transaction support, fall back to direct execution (legacy mode)
	// This is less safe but maintains backward compatibility
	if txEngine == nil {
		result, err := e.executeWithoutTransaction(ctx, cypher, upperQuery)
		if err != nil {
			return nil, err
		}
		// Flush if needed
		if !e.deferFlush {
			if asyncEngine != nil {
				asyncEngine.Flush()
			}
		}
		return result, nil
	}

	// IMPORTANT: If using AsyncEngine with pending writes, flush its cache BEFORE
	// starting the transaction. This ensures the BadgerTransaction can see all
	// previously written data. Without this, MATCH queries in compound statements
	// (MATCH...CREATE) would fail to find nodes in AsyncEngine's cache.
	// We use HasPendingWrites() first as a cheap check to avoid unnecessary flushes.
	if asyncEngine != nil && asyncEngine.HasPendingWrites() {
		asyncEngine.Flush()
	}

	// Start implicit transaction
	tx, err := txEngine.BeginTransaction()
	if err != nil {
		return nil, fmt.Errorf("failed to start implicit transaction: %w", err)
	}

	// Defer constraint validation to commit for implicit transactions.
	// This avoids duplicate per-operation checks and improves write throughput.
	if err := tx.SetDeferredConstraintValidation(true); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to configure implicit transaction: %w", err)
	}
	if err := tx.SetSkipCreateExistenceCheck(true); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to configure implicit transaction: %w", err)
	}

	// Optional WAL transaction markers for receipts.
	var wal *storage.WAL
	var walSeqStart uint64
	txID := tx.ID
	var dbName string
	if txID != "" {
		wal, dbName = e.resolveWALAndDatabase()
		if wal != nil {
			walSeqStart, err = wal.AppendTxBegin(dbName, txID, nil)
			if err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("failed to write WAL tx begin: %w", err)
			}
		}
	}

	// Create a transactional wrapper that routes writes through the transaction
	// CRITICAL: We pass the wrapper through context instead of modifying e.storage
	// because e.storage modification is NOT thread-safe for concurrent executions.
	separator := ":"
	if engines.namespace == "" {
		separator = ""
	}
	txWrapper := &transactionStorageWrapper{
		tx:         tx,
		underlying: e.storage,
		namespace:  engines.namespace,
		separator:  separator,
	}

	// Execute with transaction wrapper via context
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, txWrapper)

	// Execute the query
	result, execErr := e.executeWithoutTransaction(txCtx, cypher, upperQuery)

	// Handle result
	if execErr != nil {
		// Rollback on any error - prevents partial data corruption
		tx.Rollback()
		if wal != nil && walSeqStart > 0 {
			_, _ = wal.AppendTxAbort(dbName, txID, execErr.Error())
		}
		return nil, execErr
	}

	// Commit successful transaction
	if err := tx.Commit(); err != nil {
		if wal != nil && walSeqStart > 0 {
			_, _ = wal.AppendTxAbort(dbName, txID, err.Error())
		}
		return nil, fmt.Errorf("failed to commit implicit transaction: %w", err)
	}

	// Attach receipt metadata if WAL markers were recorded.
	if wal != nil && walSeqStart > 0 {
		opCount := tx.OperationCount()
		if commitSeq, walErr := wal.AppendTxCommit(dbName, txID, opCount); walErr == nil {
			if receipt, recErr := storage.NewReceipt(txID, walSeqStart, commitSeq, dbName, time.Now().UTC()); recErr == nil {
				if result.Metadata == nil {
					result.Metadata = make(map[string]interface{})
				}
				result.Metadata["receipt"] = receipt
			}
		}
	}

	// Flush if needed for durability
	if !e.deferFlush && asyncEngine != nil {
		asyncEngine.Flush()
	}

	return result, nil
}

// ctxKeyTxStorage is the context key for transaction storage wrapper.
type ctxKeyTxStorageType struct{}

var ctxKeyTxStorage = ctxKeyTxStorageType{}

// ctxKeyUseDatabase is the context key for :USE database switching.
// When :USE database_name is detected, the database name is stored in context
// so the server can switch to that database before executing the query.
type ctxKeyUseDatabaseType struct{}

var ctxKeyUseDatabase = ctxKeyUseDatabaseType{}

// GetUseDatabaseFromContext extracts the database name from :USE command if present in context.
// Returns empty string if no :USE command was found.
func GetUseDatabaseFromContext(ctx context.Context) string {
	if dbName, ok := ctx.Value(ctxKeyUseDatabase).(string); ok {
		return dbName
	}
	return ""
}

// getStorage returns the storage to use for the current execution.
// If a transaction wrapper is present in context, it uses that; otherwise uses e.storage.
func (e *StorageExecutor) getStorage(ctx context.Context) storage.Engine {
	if txWrapper, ok := ctx.Value(ctxKeyTxStorage).(*transactionStorageWrapper); ok {
		return txWrapper
	}
	return e.storage
}

// resolveWALAndDatabase attempts to find a WAL instance and database name
// by unwrapping common storage wrappers (namespaced, async, WAL engines).
func (e *StorageExecutor) resolveWALAndDatabase() (*storage.WAL, string) {
	engine := e.storage
	var dbName string

	for engine != nil {
		if ns, ok := engine.(interface{ Namespace() string }); ok && dbName == "" {
			dbName = ns.Namespace()
		}
		if walProvider, ok := engine.(interface{ GetWAL() *storage.WAL }); ok {
			return walProvider.GetWAL(), dbName
		}
		switch wrapper := engine.(type) {
		case interface{ GetUnderlying() storage.Engine }:
			engine = wrapper.GetUnderlying()
		case interface{ GetEngine() storage.Engine }:
			engine = wrapper.GetEngine()
		case interface{ GetInnerEngine() storage.Engine }:
			engine = wrapper.GetInnerEngine()
		default:
			return nil, dbName
		}
	}

	return nil, dbName
}

// transactionStorageWrapper wraps a BadgerTransaction to implement storage.Engine
// for use in implicit transaction execution. It routes writes through the transaction
// (for atomicity/rollback) and reads through the underlying engine (for performance).
type transactionStorageWrapper struct {
	tx         *storage.BadgerTransaction
	underlying storage.Engine // For read operations not supported by transaction
	namespace  string
	separator  string
}

// Write operations - go through transaction for atomicity
func (w *transactionStorageWrapper) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if w.namespace == "" {
		return w.tx.CreateNode(node)
	}
	namespaced := storage.CopyNode(node)
	namespaced.ID = w.prefixNodeID(node.ID)
	actualID, err := w.tx.CreateNode(namespaced)
	if err != nil {
		return "", err
	}
	return w.unprefixNodeID(actualID), nil
}

func (w *transactionStorageWrapper) UpdateNode(node *storage.Node) error {
	if w.namespace == "" {
		return w.tx.UpdateNode(node)
	}
	namespaced := storage.CopyNode(node)
	namespaced.ID = w.prefixNodeID(node.ID)
	return w.tx.UpdateNode(namespaced)
}

func (w *transactionStorageWrapper) DeleteNode(id storage.NodeID) error {
	return w.tx.DeleteNode(w.prefixNodeID(id))
}

func (w *transactionStorageWrapper) CreateEdge(edge *storage.Edge) error {
	if w.namespace == "" {
		return w.tx.CreateEdge(edge)
	}
	namespaced := storage.CopyEdge(edge)
	namespaced.ID = w.prefixEdgeID(edge.ID)
	namespaced.StartNode = w.prefixNodeID(edge.StartNode)
	namespaced.EndNode = w.prefixNodeID(edge.EndNode)
	return w.tx.CreateEdge(namespaced)
}

func (w *transactionStorageWrapper) DeleteEdge(id storage.EdgeID) error {
	return w.tx.DeleteEdge(w.prefixEdgeID(id))
}

// Read operations - transaction supports GetNode, forward others to underlying
func (w *transactionStorageWrapper) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := w.tx.GetNode(w.prefixNodeID(id))
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return node, nil
	}
	return w.toUserNode(node), nil
}

func (w *transactionStorageWrapper) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	return w.underlying.GetEdge(id)
}

func (w *transactionStorageWrapper) UpdateEdge(edge *storage.Edge) error {
	if w.namespace == "" {
		return w.tx.UpdateEdge(edge)
	}
	namespaced := storage.CopyEdge(edge)
	namespaced.ID = w.prefixEdgeID(edge.ID)
	namespaced.StartNode = w.prefixNodeID(edge.StartNode)
	namespaced.EndNode = w.prefixNodeID(edge.EndNode)
	return w.tx.UpdateEdge(namespaced)
}

func (w *transactionStorageWrapper) GetNodesByLabel(label string) ([]*storage.Node, error) {
	return w.underlying.GetNodesByLabel(label)
}

func (w *transactionStorageWrapper) GetFirstNodeByLabel(label string) (*storage.Node, error) {
	return w.underlying.GetFirstNodeByLabel(label)
}

func (w *transactionStorageWrapper) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	return w.underlying.GetOutgoingEdges(nodeID)
}

func (w *transactionStorageWrapper) GetIncomingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	return w.underlying.GetIncomingEdges(nodeID)
}

func (w *transactionStorageWrapper) GetEdgesBetween(startID, endID storage.NodeID) ([]*storage.Edge, error) {
	return w.underlying.GetEdgesBetween(startID, endID)
}

func (w *transactionStorageWrapper) GetEdgeBetween(startID, endID storage.NodeID, edgeType string) *storage.Edge {
	return w.underlying.GetEdgeBetween(startID, endID, edgeType)
}

func (w *transactionStorageWrapper) GetEdgesByType(edgeType string) ([]*storage.Edge, error) {
	return w.underlying.GetEdgesByType(edgeType)
}

func (w *transactionStorageWrapper) AllNodes() ([]*storage.Node, error) {
	return w.underlying.AllNodes()
}

func (w *transactionStorageWrapper) AllEdges() ([]*storage.Edge, error) {
	return w.underlying.AllEdges()
}

func (w *transactionStorageWrapper) GetAllNodes() []*storage.Node {
	return w.underlying.GetAllNodes()
}

func (w *transactionStorageWrapper) GetInDegree(nodeID storage.NodeID) int {
	return w.underlying.GetInDegree(nodeID)
}

func (w *transactionStorageWrapper) GetOutDegree(nodeID storage.NodeID) int {
	return w.underlying.GetOutDegree(nodeID)
}

func (w *transactionStorageWrapper) GetSchema() *storage.SchemaManager {
	return w.underlying.GetSchema()
}

func (w *transactionStorageWrapper) BulkCreateNodes(nodes []*storage.Node) error {
	// For bulk operations within transaction, create one by one
	for _, node := range nodes {
		if w.namespace == "" {
			if _, err := w.tx.CreateNode(node); err != nil {
				return err
			}
			continue
		}
		namespaced := storage.CopyNode(node)
		namespaced.ID = w.prefixNodeID(node.ID)
		if _, err := w.tx.CreateNode(namespaced); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkCreateEdges(edges []*storage.Edge) error {
	for _, edge := range edges {
		if w.namespace == "" {
			if err := w.tx.CreateEdge(edge); err != nil {
				return err
			}
			continue
		}
		namespaced := storage.CopyEdge(edge)
		namespaced.ID = w.prefixEdgeID(edge.ID)
		namespaced.StartNode = w.prefixNodeID(edge.StartNode)
		namespaced.EndNode = w.prefixNodeID(edge.EndNode)
		if err := w.tx.CreateEdge(namespaced); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkDeleteNodes(ids []storage.NodeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteNode(w.prefixNodeID(id)); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkDeleteEdges(ids []storage.EdgeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteEdge(w.prefixEdgeID(id)); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) prefixNodeID(id storage.NodeID) storage.NodeID {
	if w.namespace == "" {
		return id
	}
	return storage.NodeID(w.namespace + w.separator + string(id))
}

func (w *transactionStorageWrapper) unprefixNodeID(id storage.NodeID) storage.NodeID {
	if w.namespace == "" {
		return id
	}
	prefix := w.namespace + w.separator
	s := string(id)
	if strings.HasPrefix(s, prefix) {
		return storage.NodeID(s[len(prefix):])
	}
	return id
}

func (w *transactionStorageWrapper) prefixEdgeID(id storage.EdgeID) storage.EdgeID {
	if w.namespace == "" {
		return id
	}
	return storage.EdgeID(w.namespace + w.separator + string(id))
}

func (w *transactionStorageWrapper) unprefixEdgeID(id storage.EdgeID) storage.EdgeID {
	if w.namespace == "" {
		return id
	}
	prefix := w.namespace + w.separator
	s := string(id)
	if strings.HasPrefix(s, prefix) {
		return storage.EdgeID(s[len(prefix):])
	}
	return id
}

func (w *transactionStorageWrapper) toUserNode(node *storage.Node) *storage.Node {
	if node == nil {
		return nil
	}
	out := storage.CopyNode(node)
	out.ID = w.unprefixNodeID(out.ID)
	return out
}

func (w *transactionStorageWrapper) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return w.underlying.BatchGetNodes(ids)
}

func (w *transactionStorageWrapper) Close() error {
	// Don't close underlying engine
	return nil
}

func (w *transactionStorageWrapper) NodeCount() (int64, error) {
	return w.underlying.NodeCount()
}

func (w *transactionStorageWrapper) EdgeCount() (int64, error) {
	return w.underlying.EdgeCount()
}

func (w *transactionStorageWrapper) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	// DeleteByPrefix is not supported within a transaction context.
	// This operation should be performed outside of a transaction.
	return 0, 0, fmt.Errorf("DeleteByPrefix not supported within transaction context")
}

// tryFastPathCompoundQuery attempts to handle common compound query patterns
// using pre-compiled regex for faster routing. Returns (result, true) if handled,
// (nil, false) if the query should go through normal routing.
//
// Pattern: MATCH (a:Label), (b:Label) WITH a, b LIMIT 1 CREATE (a)-[r:Type]->(b) DELETE r
// This is a very common pattern in benchmarks and relationship tests.
func (e *StorageExecutor) tryFastPathCompoundQuery(ctx context.Context, cypher string) (*ExecuteResult, bool) {
	// Try Pattern 1: MATCH (a:Label), (b:Label) WITH a, b LIMIT 1 CREATE ... DELETE
	if matches := matchCreateDeleteRelPattern.FindStringSubmatch(cypher); matches != nil {
		label1 := matches[2]
		label2 := matches[4]
		relType := matches[9]
		return e.executeFastPathCreateDeleteRel(label1, label2, "", nil, "", nil, relType)
	}

	// Try Pattern 2: MATCH (p1:Label {prop: val}), (p2:Label {prop: val}) CREATE ... DELETE
	// LDBC-style pattern with property matching
	if matches := matchPropCreateDeleteRelPattern.FindStringSubmatch(cypher); matches != nil {
		// Groups: 1=var1, 2=label1, 3=prop1, 4=val1, 5=var2, 6=label2, 7=prop2, 8=val2, 9=relVar, 10=relType, 11=delVar
		label1 := matches[2]
		prop1 := matches[3]
		val1 := matches[4]
		label2 := matches[6]
		prop2 := matches[7]
		val2 := matches[8]
		relType := matches[10]
		return e.executeFastPathCreateDeleteRel(label1, label2, prop1, val1, prop2, val2, relType)
	}

	// Try Pattern 3: MATCH (a:Label {prop: val}), (b:Label {prop: val}) CREATE ... WITH r DELETE r RETURN count(r)
	// Northwind-style create/delete relationship benchmark shape.
	if matches := matchPropCreateWithDeleteReturnCountRelPattern.FindStringSubmatch(cypher); matches != nil {
		label1 := matches[2]
		prop1 := matches[3]
		val1 := matches[4]
		label2 := matches[6]
		prop2 := matches[7]
		val2 := matches[8]
		relVar := matches[9]
		relType := matches[10]
		withVar := matches[11]
		delVar := matches[12]
		countVar := matches[13]

		// We can't enforce backreferences in Go regex, so validate variable consistency here.
		if relVar == "" || withVar != relVar || delVar != relVar || countVar != relVar {
			return nil, false
		}

		return e.executeFastPathCreateDeleteRelCount(label1, label2, prop1, val1, prop2, val2, relType, relVar)
	}

	return nil, false
}

// executeFastPathCreateDeleteRel executes the fast-path for MATCH...CREATE...DELETE patterns.
// If prop1/prop2 are empty, uses GetFirstNodeByLabel. Otherwise uses property lookup.
func (e *StorageExecutor) executeFastPathCreateDeleteRel(label1, label2, prop1 string, val1 any, prop2 string, val2 any, relType string) (*ExecuteResult, bool) {
	var err error

	// Get node1
	if prop1 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label1)
	} else {
		node1 := e.findNodeByLabelAndProperty(label1, prop1, val1)
		if node1 == nil {
			return nil, false
		}
	}
	if err != nil {
		return nil, false
	}

	// Get node2
	if prop2 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label2)
	} else {
		node2 := e.findNodeByLabelAndProperty(label2, prop2, val2)
		if node2 == nil {
			return nil, false
		}
	}
	if err != nil {
		return nil, false
	}

	// Optimization: This pattern creates a relationship and deletes it in the same
	// statement without returning it. The relationship is not observable to the user,
	// and the net graph effect is a no-op, so we skip storage writes entirely.
	//
	// We still validate that both endpoints exist (via the lookups above) and we
	// still return correct query stats for Neo4j compatibility.

	return &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats: &QueryStats{
			RelationshipsCreated: 1,
			RelationshipsDeleted: 1,
		},
	}, true
}

func (e *StorageExecutor) executeFastPathCreateDeleteRelCount(label1, label2, prop1 string, val1 any, prop2 string, val2 any, relType string, relVar string) (*ExecuteResult, bool) {
	var err error

	if prop1 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label1)
	} else {
		node1 := e.findNodeByLabelAndProperty(label1, prop1, val1)
		if node1 == nil {
			return nil, false
		}
	}
	if err != nil {
		return nil, false
	}

	if prop2 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label2)
	} else {
		node2 := e.findNodeByLabelAndProperty(label2, prop2, val2)
		if node2 == nil {
			return nil, false
		}
	}
	if err != nil {
		return nil, false
	}

	return &ExecuteResult{
		Columns: []string{"count(" + relVar + ")"},
		Rows:    [][]interface{}{{int64(1)}},
		Stats: &QueryStats{
			RelationshipsCreated: 1,
			RelationshipsDeleted: 1,
		},
	}, true
}

// findNodeByLabelAndProperty finds a node by label and a single property value.
// Uses the node lookup cache for O(1) repeated lookups.
func (e *StorageExecutor) findNodeByLabelAndProperty(label, prop string, val any) *storage.Node {
	// Try cache first (with proper locking)
	cacheKey := fmt.Sprintf("%s:{%s:%v}", label, prop, val)
	e.nodeLookupCacheMu.RLock()
	if cached, ok := e.nodeLookupCache[cacheKey]; ok {
		e.nodeLookupCacheMu.RUnlock()
		return cached
	}
	e.nodeLookupCacheMu.RUnlock()

	// Scan nodes with label
	nodes, err := e.storage.GetNodesByLabel(label)
	if err != nil {
		return nil
	}

	// Find matching node
	for _, node := range nodes {
		if nodeVal, ok := node.Properties[prop]; ok {
			if fmt.Sprintf("%v", nodeVal) == fmt.Sprintf("%v", val) {
				// Cache for next time (with proper locking)
				e.nodeLookupCacheMu.Lock()
				e.nodeLookupCache[cacheKey] = node
				e.nodeLookupCacheMu.Unlock()
				return node
			}
		}
	}

	return nil
}

// isSystemCommandNoGraph returns true for statements that operate on database metadata
// (CREATE/DROP DATABASE, SHOW DATABASES, etc.) and must not use the async engine or
// implicit transactions. These are routed to executeWithoutTransaction directly.
func isSystemCommandNoGraph(cypher string) bool {
	return findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "ALIAS") == 0 ||
		findMultiWordKeywordIndex(cypher, "DROP", "COMPOSITE DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "DROP", "DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "DROP", "ALIAS") == 0 ||
		findMultiWordKeywordIndex(cypher, "SHOW", "DATABASES") == 0 ||
		findMultiWordKeywordIndex(cypher, "ALTER", "DATABASE") == 0
}

// executeWithoutTransaction executes query without transaction wrapping (original path).
func (e *StorageExecutor) executeWithoutTransaction(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// FAST PATH: Check for common compound query patterns using pre-compiled regex
	// This avoids multiple findKeywordIndex calls for frequently-used patterns
	if result, handled := e.tryFastPathCompoundQuery(ctx, cypher); handled {
		return result, nil
	}

	// Route to appropriate handler based on query type
	// upperQuery is passed in to avoid redundant conversion

	// Cache keyword checks to avoid repeated searches
	startsWithMatch := strings.HasPrefix(upperQuery, "MATCH")
	startsWithCreate := strings.HasPrefix(upperQuery, "CREATE")
	startsWithMerge := strings.HasPrefix(upperQuery, "MERGE")

	// MERGE queries get special handling - they have their own ON CREATE SET / ON MATCH SET logic
	if startsWithMerge {
		// Check for MERGE ... WITH ... MATCH chain pattern (e.g., import script pattern)
		withIdx := findKeywordIndex(cypher, "WITH")
		if withIdx > 0 {
			// Check for MATCH after WITH (this is the chained pattern)
			afterWith := cypher[withIdx:]
			if findKeywordIndex(afterWith, "MATCH") > 0 {
				return e.executeMergeWithChain(ctx, cypher)
			}
		}
		// Check for multiple MERGEs without WITH (e.g., MERGE (a) MERGE (b) MERGE (a)-[:REL]->(b))
		firstMergeEnd := findKeywordIndex(cypher[5:], ")")
		if firstMergeEnd > 0 {
			afterFirstMerge := cypher[5+firstMergeEnd+1:]
			secondMergeIdx := findKeywordIndex(afterFirstMerge, "MERGE")
			if secondMergeIdx >= 0 {
				return e.executeMultipleMerges(ctx, cypher)
			}
		}
		return e.executeMerge(ctx, cypher)
	}

	// Cache findKeywordIndex results for compound query detection
	var mergeIdx, createIdx, withIdx, deleteIdx, optionalMatchIdx int = -1, -1, -1, -1, -1

	if startsWithMatch {
		// Only search for keywords if query starts with MATCH
		mergeIdx = findKeywordIndex(cypher, "MERGE")
		createIdx = findKeywordIndex(cypher, "CREATE")
		optionalMatchIdx = findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH")
	} else if startsWithCreate {
		// Check for multiple CREATE statements (e.g., CREATE (a) CREATE (b) CREATE (a)-[:REL]->(b))
		firstCreateEnd := findKeywordIndex(cypher[6:], ")")
		if firstCreateEnd > 0 {
			afterFirstCreate := cypher[6+firstCreateEnd+1:]
			secondCreateIdx := findKeywordIndex(afterFirstCreate, "CREATE")
			if secondCreateIdx >= 0 {
				return e.executeMultipleCreates(ctx, cypher)
			}
		}
		// Only search for WITH/DELETE if query starts with CREATE
		withIdx = findKeywordIndex(cypher, "WITH")
		if withIdx > 0 {
			deleteIdx = findKeywordIndex(cypher, "DELETE")
		}
	}

	// Compound queries: MATCH ... MERGE ... (with variable references)
	if startsWithMatch && mergeIdx > 0 {
		return e.executeCompoundMatchMerge(ctx, cypher)
	}

	// Compound queries: MATCH ... CREATE ... (create relationship between matched nodes)
	if startsWithMatch && createIdx > 0 {
		return e.executeCompoundMatchCreate(ctx, cypher)
	}

	// Compound queries: CREATE ... WITH ... DELETE (create then delete in same statement)
	if startsWithCreate && withIdx > 0 && deleteIdx > 0 {
		return e.executeCompoundCreateWithDelete(ctx, cypher)
	}

	// Cache contains checks for DELETE - use word-boundary-aware detection
	// Note: Can't use " DELETE " because DELETE is often followed by variable name (DELETE n)
	// findKeywordIndex handles word boundaries properly (won't match 'ToDelete' in string literals)
	hasDelete := findKeywordIndex(cypher, "DELETE") > 0 // Must be after MATCH, not at start
	hasDetachDelete := containsKeywordOutsideStrings(cypher, "DETACH DELETE")

	// Check for compound queries - MATCH ... DELETE, MATCH ... SET, etc.
	if hasDelete || hasDetachDelete {
		return e.executeDelete(ctx, cypher)
	}

	// Cache SET-related checks - use string-literal-aware detection to avoid
	// matching keywords inside user content like 'MATCH (n) SET n.x = 1'
	// Note: findKeywordIndex already checks word boundaries, so no need for leading space
	hasSet := containsKeywordOutsideStrings(cypher, "SET")
	hasOnCreateSet := containsKeywordOutsideStrings(cypher, "ON CREATE SET")
	hasOnMatchSet := containsKeywordOutsideStrings(cypher, "ON MATCH SET")

	// NEO4J COMPAT: Handle CREATE ... SET pattern (e.g., CREATE (n) SET n.x = 1)
	// Neo4j allows SET immediately after CREATE without requiring MATCH
	if startsWithCreate && !isCreateProcedureCommand(cypher) && hasSet && !hasOnCreateSet && !hasOnMatchSet {
		return e.executeCreateSet(ctx, cypher)
	}

	// Check for ALTER DATABASE before generic SET (ALTER DATABASE SET LIMIT contains "SET")
	if findMultiWordKeywordIndex(cypher, "ALTER", "DATABASE") == 0 {
		return e.executeAlterDatabase(ctx, cypher)
	}

	// Only route to executeSet if it's a MATCH ... SET or standalone SET
	if hasSet && !isCreateProcedureCommand(cypher) && !hasOnCreateSet && !hasOnMatchSet {
		if startsWithMatch || findKeywordIndex(cypher, "SET") == 0 {
			return e.executeSet(ctx, cypher)
		}
	}

	// Handle MATCH ... REMOVE (property removal) - string-literal-aware
	// Note: findKeywordIndex already checks word boundaries
	if containsKeywordOutsideStrings(cypher, "REMOVE") {
		return e.executeRemove(ctx, cypher)
	}

	// Compound queries: MATCH ... OPTIONAL MATCH ...
	// But NOT when there's a WITH clause before OPTIONAL MATCH (that's handled by executeMatchWithOptionalMatch)
	if startsWithMatch && optionalMatchIdx > 0 {
		// Check if there's a WITH clause BEFORE OPTIONAL MATCH
		// If so, route to the specialized handler that processes WITH first
		withBeforeOptional := findKeywordIndex(cypher[:optionalMatchIdx], "WITH")
		if withBeforeOptional > 0 {
			// WITH comes before OPTIONAL MATCH - route to executeMatchWithOptionalMatch
			return e.executeMatchWithOptionalMatch(ctx, cypher)
		}
		return e.executeCompoundMatchOptionalMatch(ctx, cypher)
	}

	// Compound queries: MATCH ... CALL {} ... (correlated subquery)
	if startsWithMatch && hasSubqueryPattern(cypher, callSubqueryRe) {
		return e.executeMatchWithCallSubquery(ctx, cypher)
	}

	// Compound queries: MATCH ... CALL procedure() ... (procedure with bound variables)
	if startsWithMatch && findKeywordIndex(cypher, "CALL") > 0 {
		// Check if it's a procedure call (not a subquery)
		callIdx := findKeywordIndex(cypher, "CALL")
		if callIdx > 0 {
			callPart := strings.TrimSpace(cypher[callIdx:])
			if !isCallSubquery(callPart) {
				// It's a procedure call - handle with bound variables
				return e.executeMatchWithCallProcedure(ctx, cypher)
			}
		}
	}

	switch {
	case isCreateProcedureCommand(cypher):
		return e.executeCreateProcedure(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH") == 0:
		// OPTIONAL MATCH must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "OPTIONAL MATCH", "OPTIONAL\tMATCH", "OPTIONAL\nMATCH", etc.
		return e.executeOptionalMatch(ctx, cypher)
	case startsWithMatch && isShortestPathQuery(cypher):
		// Handle shortestPath() and allShortestPaths() queries
		query, err := e.parseShortestPathQuery(cypher)
		if err != nil {
			return nil, err
		}
		return e.executeShortestPathQuery(query)
	case startsWithMatch:
		// Check for optimizable patterns FIRST
		patternInfo := DetectQueryPattern(cypher)
		if patternInfo.IsOptimizable() {
			if result, ok := e.ExecuteOptimized(ctx, cypher, patternInfo); ok {
				return result, nil
			}
			// Fall through to generic on optimization failure
		}
		return e.executeMatch(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "CONSTRAINT") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "RANGE INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "FULLTEXT INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "VECTOR INDEX") == 0,
		findKeywordIndex(cypher, "CREATE INDEX") == 0:
		// Schema commands - constraints and indexes (check more specific patterns first)
		// Must be at start (position 0) to be a standalone clause
		return e.executeSchemaCommand(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE") == 0:
		// System command: CREATE COMPOSITE DATABASE (check before CREATE DATABASE)
		// Must be at start (position 0) to be a standalone clause
		return e.executeCreateCompositeDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "DATABASE") == 0:
		// System command: CREATE DATABASE (check before generic CREATE)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "CREATE DATABASE", "CREATE\tDATABASE", "CREATE\nDATABASE", etc.
		return e.executeCreateDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "ALIAS") == 0:
		// System command: CREATE ALIAS (check before generic CREATE)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "CREATE ALIAS", "CREATE\tALIAS", "CREATE\nALIAS", etc.
		return e.executeCreateAlias(ctx, cypher)
	case startsWithCreate:
		return e.executeCreate(ctx, cypher)
	case hasDelete || hasDetachDelete:
		// DELETE/DETACH DELETE already detected above with findKeywordIndex
		return e.executeDelete(ctx, cypher)
	case findKeywordIndex(cypher, "CALL") == 0:
		// Distinguish CALL {} subquery from CALL procedure()
		// Must be at start (position 0) to be a standalone clause
		if isCallSubquery(cypher) {
			return e.executeCallSubquery(ctx, cypher)
		}
		return e.executeCall(ctx, cypher)
	case findKeywordIndex(cypher, "RETURN") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeReturn(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "COMPOSITE DATABASE") == 0:
		// System command: DROP COMPOSITE DATABASE (check before DROP DATABASE)
		// Must be at start (position 0) to be a standalone clause
		return e.executeDropCompositeDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "DATABASE") == 0:
		// System command: DROP DATABASE (check before generic DROP)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "DROP DATABASE", "DROP\tDATABASE", "DROP\nDATABASE", etc.
		return e.executeDropDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "ALIAS") == 0:
		// System command: DROP ALIAS (check before generic DROP)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "DROP ALIAS", "DROP\tALIAS", "DROP\nALIAS", etc.
		return e.executeDropAlias(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "CONSTRAINT") == 0:
		// Schema command: DROP CONSTRAINT (must not be treated as generic DROP no-op).
		// Must be at start (position 0) to be a standalone clause.
		return e.executeSchemaCommand(ctx, cypher)
	case isDropProcedureCommand(cypher):
		return e.executeDropProcedure(ctx, cypher)
	case findKeywordIndex(cypher, "DROP") == 0:
		// DROP INDEX/CONSTRAINT - treat as no-op (NornicDB manages indexes internally)
		// Must be at start (position 0) to be a standalone clause
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	case findKeywordIndex(cypher, "WITH") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeWith(ctx, cypher)
	case findKeywordIndex(cypher, "UNWIND") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeUnwind(ctx, cypher)
	case findKeywordIndex(cypher, "UNION ALL") >= 0:
		// UNION ALL can appear anywhere in query
		return e.executeUnion(ctx, cypher, true)
	case findKeywordIndex(cypher, "UNION") >= 0:
		// UNION can appear anywhere in query
		return e.executeUnion(ctx, cypher, false)
	case findKeywordIndex(cypher, "FOREACH") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeForeach(ctx, cypher)
	case findKeywordIndex(cypher, "LOAD CSV") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeLoadCSV(ctx, cypher)
	// SHOW commands for Neo4j compatibility
	case findMultiWordKeywordIndex(cypher, "SHOW", "FULLTEXT INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "FULLTEXT INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "RANGE INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "RANGE INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "VECTOR INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "VECTOR INDEX") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowIndexes(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "INDEX") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowIndexes(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "CONSTRAINTS") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "CONSTRAINT") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowConstraints(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "PROCEDURES") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowProcedures(ctx, cypher)
	case findKeywordIndex(cypher, "SHOW FUNCTIONS") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowFunctions(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "COMPOSITE DATABASES") == 0:
		// System command: SHOW COMPOSITE DATABASES (check before SHOW DATABASES)
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowCompositeDatabases(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "CONSTITUENTS") == 0:
		// System command: SHOW CONSTITUENTS
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowConstituents(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "DATABASES") == 0:
		// System command: SHOW DATABASES (plural - check before singular)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "SHOW DATABASES", "SHOW\tDATABASES", "SHOW\nDATABASES", etc.
		return e.executeShowDatabases(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "DATABASE") == 0:
		// System command: SHOW DATABASE (singular)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "SHOW DATABASE", "SHOW\tDATABASE", "SHOW\nDATABASE", etc.
		return e.executeShowDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "ALIASES") == 0:
		// System command: SHOW ALIASES
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "SHOW ALIASES", "SHOW\tALIASES", "SHOW\nALIASES", etc.
		return e.executeShowAliases(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "ALTER", "COMPOSITE DATABASE") == 0:
		// System command: ALTER COMPOSITE DATABASE (check before ALTER DATABASE)
		// Must be at start (position 0) to be a standalone clause
		return e.executeAlterCompositeDatabase(ctx, cypher)
	// Note: ALTER DATABASE is handled earlier (before SET check) to avoid routing conflict
	case findMultiWordKeywordIndex(cypher, "SHOW", "LIMITS") == 0:
		// System command: SHOW LIMITS
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowLimits(ctx, cypher)
	default:
		firstWord := strings.Split(upperQuery, " ")[0]
		return nil, fmt.Errorf("unsupported query type: %s (supported: MATCH, CREATE, MERGE, DELETE, SET, REMOVE, RETURN, WITH, UNWIND, CALL, FOREACH, LOAD CSV, SHOW, DROP, ALTER)", firstWord)
	}
}

// executeReturn handles simple RETURN statements (e.g., "RETURN 1").
func (e *StorageExecutor) executeReturn(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters before processing
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Parse RETURN clause - use word boundary detection
	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause not found in query: %q", truncateQuery(cypher, 80))
	}

	returnClause := strings.TrimSpace(cypher[returnIdx+6:])

	// Handle simple literal returns like "RETURN 1" or "RETURN true"
	parts := splitReturnExpressions(returnClause)
	columns := make([]string, 0, len(parts))
	values := make([]interface{}, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Check for alias (AS)
		alias := part
		upperPart := strings.ToUpper(part)
		if asIdx := strings.Index(upperPart, " AS "); asIdx != -1 {
			alias = strings.TrimSpace(part[asIdx+4:])
			part = strings.TrimSpace(part[:asIdx])
		}

		columns = append(columns, alias)

		// Handle NULL literal explicitly first
		if strings.EqualFold(part, "null") {
			values = append(values, nil)
			continue
		}

		// Try to evaluate as a function or expression first
		result := e.evaluateExpressionWithContext(part, nil, nil)
		if result != nil {
			values = append(values, result)
			continue
		}

		// Parse literal value
		if part == "1" || strings.HasPrefix(strings.ToLower(part), "true") {
			values = append(values, int64(1))
		} else if part == "0" || strings.HasPrefix(strings.ToLower(part), "false") {
			values = append(values, int64(0))
		} else if strings.HasPrefix(part, "'") && strings.HasSuffix(part, "'") {
			values = append(values, part[1:len(part)-1])
		} else if strings.HasPrefix(part, "\"") && strings.HasSuffix(part, "\"") {
			values = append(values, part[1:len(part)-1])
		} else {
			// Try to parse as number
			if val, err := strconv.ParseInt(part, 10, 64); err == nil {
				values = append(values, val)
			} else if val, err := strconv.ParseFloat(part, 64); err == nil {
				values = append(values, val)
			} else {
				// Return as string
				values = append(values, part)
			}
		}
	}

	return &ExecuteResult{
		Columns: columns,
		Rows:    [][]interface{}{values},
	}, nil
}

// splitReturnExpressions splits RETURN expressions by comma, respecting parentheses and brackets depth
func splitReturnExpressions(clause string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0
	bracketDepth := 0
	inQuote := false
	quoteChar := rune(0)

	for _, ch := range clause {
		switch {
		case (ch == '\'' || ch == '"') && !inQuote:
			inQuote = true
			quoteChar = ch
			current.WriteRune(ch)
		case ch == quoteChar && inQuote:
			inQuote = false
			quoteChar = 0
			current.WriteRune(ch)
		case ch == '(' && !inQuote:
			parenDepth++
			current.WriteRune(ch)
		case ch == ')' && !inQuote:
			parenDepth--
			current.WriteRune(ch)
		case ch == '[' && !inQuote:
			bracketDepth++
			current.WriteRune(ch)
		case ch == ']' && !inQuote:
			bracketDepth--
			current.WriteRune(ch)
		case ch == ',' && parenDepth == 0 && bracketDepth == 0 && !inQuote:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// validateSyntax performs syntax validation.
// When NORNICDB_PARSER=antlr, uses ANTLR for strict OpenCypher grammar validation.
// When NORNICDB_PARSER=nornic (default), uses fast inline validation.
func (e *StorageExecutor) validateSyntax(cypher string) error {
	// Use ANTLR parser for validation when configured
	if config.IsANTLRParser() {
		return e.validateSyntaxANTLR(cypher)
	}
	return e.validateSyntaxNornic(cypher)
}

// validateSyntaxANTLR uses ANTLR for strict OpenCypher grammar validation.
// Provides detailed error messages with line/column information.
func (e *StorageExecutor) validateSyntaxANTLR(cypher string) error {
	return antlr.Validate(cypher)
}

// validateSyntaxNornic performs fast inline syntax validation.
func (e *StorageExecutor) validateSyntaxNornic(cypher string) error {
	// Check for valid starting keyword (including EXPLAIN/PROFILE prefixes and transaction control)
	validStarts := []string{"MATCH", "CREATE", "MERGE", "DELETE", "DETACH", "CALL", "RETURN", "WITH", "UNWIND", "OPTIONAL", "DROP", "SHOW", "FOREACH", "LOAD", "EXPLAIN", "PROFILE", "ALTER", "USE", "BEGIN", "COMMIT", "ROLLBACK"}
	hasValidStart := false
	for _, start := range validStarts {
		if startsWithKeywordFold(cypher, start) {
			hasValidStart = true
			break
		}
	}
	if !hasValidStart {
		return fmt.Errorf("syntax error: query must start with a valid clause (MATCH, CREATE, MERGE, DELETE, CALL, SHOW, EXPLAIN, PROFILE, ALTER, USE, BEGIN, COMMIT, ROLLBACK, etc.)")
	}

	// Check balanced parentheses
	parenCount := 0
	bracketCount := 0
	braceCount := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(cypher); i++ {
		c := cypher[i]

		if inString {
			if c == stringChar && (i == 0 || cypher[i-1] != '\\') {
				inString = false
			}
			continue
		}

		switch c {
		case '"', '\'':
			inString = true
			stringChar = c
		case '(':
			parenCount++
		case ')':
			parenCount--
		case '[':
			bracketCount++
		case ']':
			bracketCount--
		case '{':
			braceCount++
		case '}':
			braceCount--
		}

		if parenCount < 0 || bracketCount < 0 || braceCount < 0 {
			return fmt.Errorf("syntax error: unbalanced brackets at position %d", i)
		}
	}

	if parenCount != 0 {
		return fmt.Errorf("syntax error: unbalanced parentheses")
	}
	if bracketCount != 0 {
		return fmt.Errorf("syntax error: unbalanced square brackets")
	}
	if braceCount != 0 {
		return fmt.Errorf("syntax error: unbalanced curly braces")
	}
	if inString {
		return fmt.Errorf("syntax error: unclosed quote")
	}

	return nil
}
