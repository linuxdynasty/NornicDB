package nornicdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// =============================================================================
// HTTP Server Interface Methods
// =============================================================================

// Stats returns database statistics.
type DBStats struct {
	NodeCount int64 `json:"node_count"`
	EdgeCount int64 `json:"edge_count"`
	// Removed TransactionCount - was never incremented (always 0)
	// NornicDB uses thread-safe maps with RWMutex, not ACID transactions
}

// Stats returns current database statistics.
func (db *DB) Stats() DBStats {
	stats := DBStats{}
	db.mu.RLock()
	st := db.storage
	db.mu.RUnlock()

	if st != nil {
		nodeCount, _ := st.NodeCount()
		edgeCount, _ := st.EdgeCount()
		stats.NodeCount = nodeCount
		stats.EdgeCount = edgeCount
	}
	return stats
}

// SetGPUManager sets the GPU manager for vector search acceleration.
// Uses interface{} to avoid circular import with gpu package.
// If clustering is already enabled (via feature flag), this upgrades it to use GPU.
func (db *DB) SetGPUManager(manager interface{}) {
	db.gpuManagerMu.Lock()
	db.gpuManager = manager
	db.gpuManagerMu.Unlock()

	gpuMgr, _ := manager.(*gpu.Manager)

	// Upgrade all cached per-database search services.
	db.searchServicesMu.RLock()
	for _, entry := range db.searchServices {
		if entry == nil || entry.svc == nil {
			continue
		}
		entry.svc.SetGPUManager(gpuMgr)

		if gpuMgr != nil && nornicConfig.IsGPUClusteringEnabled() && entry.svc.IsClusteringEnabled() {
			entry.svc.EnableClustering(gpuMgr, db.kmeansNumClusters())
		}
	}
	db.searchServicesMu.RUnlock()

	if gpuMgr != nil {
		fmt.Println("🚀 GPU acceleration enabled for search services (all databases)")
	}
}

// GetGPUManager returns the GPU manager if set.
// Returns interface{} - caller must type assert to *gpu.Manager.
func (db *DB) GetGPUManager() interface{} {
	db.gpuManagerMu.RLock()
	defer db.gpuManagerMu.RUnlock()
	return db.gpuManager
}

// TriggerSearchClustering runs k-means clustering on search embeddings.
// Call this after bulk data loading to enable cluster-accelerated search.
// Returns nil if clustering is not enabled or there are too few embeddings.
func (db *DB) TriggerSearchClustering() error {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return ErrClosed
	}

	if !nornicConfig.IsGPUClusteringEnabled() {
		return nil
	}

	ctx := db.buildCtx
	if ctx == nil {
		ctx = context.Background()
	}
	db.runClusteringOnceAllDatabases(ctx)
	return nil
}

// startClusteringTimer starts a background timer that runs k-means clustering
// at a regular interval. This is preferred over trigger-based clustering which
// can cause performance issues when embeddings are created frequently.
// Runs immediately on startup, then every interval thereafter (skipping if no changes).
func (db *DB) startClusteringTimer(interval time.Duration) {
	db.mu.Lock()
	if db.clusterTicker != nil || db.closed {
		db.mu.Unlock()
		return
	}
	ticker := time.NewTicker(interval)
	stopCh := make(chan struct{})
	db.clusterTicker = ticker
	db.clusterTickerStop = stopCh
	db.bgWg.Add(1)
	db.mu.Unlock()

	go func(t *time.Ticker, stop <-chan struct{}) {
		defer db.bgWg.Done()
		log.Printf("🔬 K-means clustering timer started (interval: %v)", interval)

		// Use buildCtx so clustering stops when DB is closed (e.g. SIGINT/SIGTERM).
		ctx := db.buildCtx
		if ctx == nil {
			ctx = context.Background()
		}

		// Run immediately on startup
		db.runClusteringOnceAllDatabases(ctx)

		// Then run on timer
		for {
			select {
			case <-stop:
				log.Printf("🔬 K-means clustering timer stopped")
				return
			case <-t.C:
				db.runClusteringOnceAllDatabases(ctx)
			}
		}
	}(ticker, stopCh)
}

// stopClusteringTimer stops the k-means clustering timer if running.
func (db *DB) stopClusteringTimer() {
	db.mu.Lock()
	ticker := db.clusterTicker
	stopCh := db.clusterTickerStop
	if ticker == nil {
		db.mu.Unlock()
		return
	}
	db.clusterTicker = nil
	db.clusterTickerStop = nil
	db.mu.Unlock()

	ticker.Stop()
	if stopCh != nil {
		close(stopCh)
	}
}

// SearchStats contains search service statistics.
type SearchStats struct {
	EmbeddingCount    int     `json:"embedding_count"`
	ClusteringEnabled bool    `json:"clustering_enabled"`
	IsClustered       bool    `json:"is_clustered"`
	NumClusters       int     `json:"num_clusters,omitempty"`
	AvgClusterSize    float64 `json:"avg_cluster_size,omitempty"`
	ClusterIterations int     `json:"cluster_iterations,omitempty"`
}

// GetSearchStats returns search service statistics including clustering info.
func (db *DB) GetSearchStats() *SearchStats {
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	if err != nil || svc == nil {
		return nil
	}

	stats := &SearchStats{
		EmbeddingCount:    svc.EmbeddingCount(),
		ClusteringEnabled: svc.IsClusteringEnabled(),
	}

	if clusterStats := svc.ClusterStats(); clusterStats != nil {
		stats.IsClustered = clusterStats.Clustered
		stats.NumClusters = clusterStats.NumClusters
		stats.AvgClusterSize = clusterStats.AvgClusterSize
		stats.ClusterIterations = clusterStats.Iterations
	}

	return stats
}

// IsAsyncWritesEnabled returns true if async writes (eventual consistency) is enabled.
// When enabled, write operations return immediately and are flushed in the background.
// HTTP handlers should return 202 Accepted instead of 201 Created for writes.
func (db *DB) IsAsyncWritesEnabled() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.config.Database.AsyncWritesEnabled
}

// CypherResult holds results from a Cypher query.
type CypherResult struct {
	Columns  []string               `json:"columns"`
	Rows     [][]interface{}        `json:"rows"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// ExecuteCypher runs a Cypher query and returns structured results.
func (db *DB) ExecuteCypher(ctx context.Context, query string, params map[string]interface{}) (*CypherResult, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Execute query through Cypher executor
	result, err := db.cypherExecutor.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}

	return &CypherResult{
		Columns:  result.Columns,
		Rows:     result.Rows,
		Metadata: result.Metadata,
	}, nil
}

// TypedCypherResult holds typed query results.
type TypedCypherResult[T any] struct {
	Columns []string `json:"columns"`
	Rows    []T      `json:"rows"`
}

// ExecuteCypherTyped runs a Cypher query and decodes results into typed structs.
// Usage:
//
//	type Task struct {
//	    ID     string `cypher:"id"`
//	    Title  string `cypher:"title"`
//	    Status string `cypher:"status"`
//	}
//	result, err := db.ExecuteCypherTyped[Task](ctx, "MATCH (t:Task) RETURN t.id, t.title, t.status", nil)
func ExecuteCypherTyped[T any](db *DB, ctx context.Context, query string, params map[string]interface{}) (*TypedCypherResult[T], error) {
	raw, err := db.ExecuteCypher(ctx, query, params)
	if err != nil {
		return nil, err
	}

	rows := make([]T, 0, len(raw.Rows))
	for _, row := range raw.Rows {
		var decoded T
		if err := decodeRow(raw.Columns, row, &decoded); err != nil {
			return nil, fmt.Errorf("failed to decode row: %w", err)
		}
		rows = append(rows, decoded)
	}

	return &TypedCypherResult[T]{
		Columns: raw.Columns,
		Rows:    rows,
	}, nil
}

// First returns the first row or zero value if empty.
func (r *TypedCypherResult[T]) First() (T, bool) {
	if len(r.Rows) == 0 {
		var zero T
		return zero, false
	}
	return r.Rows[0], true
}

// decodeRow decodes a row into a typed struct using reflection.
func decodeRow(columns []string, values []interface{}, dest interface{}) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer")
	}

	destElem := destVal.Elem()
	destType := destElem.Type()

	// Handle map return (node as map)
	if len(values) == 1 {
		if m, ok := values[0].(map[string]interface{}); ok {
			// Check for nested properties
			if props, ok := m["properties"].(map[string]interface{}); ok {
				return decodeMapToStruct(props, destElem, destType)
			}
			return decodeMapToStruct(m, destElem, destType)
		}
	}

	// Build field mapping from struct tags
	fieldMap := make(map[string]int)
	for i := 0; i < destType.NumField(); i++ {
		field := destType.Field(i)
		name := field.Tag.Get("cypher")
		if name == "" {
			name = field.Tag.Get("json")
			if idx := strings.Index(name, ","); idx != -1 {
				name = name[:idx]
			}
		}
		if name == "" || name == "-" {
			name = strings.ToLower(field.Name)
		}
		fieldMap[name] = i
	}

	// Map columns to fields
	for i, col := range columns {
		if i >= len(values) {
			break
		}

		// Normalize column name (handle n.property notation)
		colName := col
		if idx := strings.LastIndex(col, "."); idx != -1 {
			colName = col[idx+1:]
		}
		colName = strings.ToLower(colName)

		fieldIdx, ok := fieldMap[colName]
		if !ok {
			continue
		}

		field := destElem.Field(fieldIdx)
		if !field.CanSet() {
			continue
		}

		if err := assignValue(field, values[i]); err != nil {
			return fmt.Errorf("field %s: %w", col, err)
		}
	}

	return nil
}

// decodeMapToStruct decodes a map into a struct
func decodeMapToStruct(m map[string]interface{}, destElem reflect.Value, destType reflect.Type) error {
	for i := 0; i < destType.NumField(); i++ {
		field := destType.Field(i)
		fieldVal := destElem.Field(i)

		if !fieldVal.CanSet() {
			continue
		}

		name := field.Tag.Get("cypher")
		if name == "" {
			name = field.Tag.Get("json")
			if idx := strings.Index(name, ","); idx != -1 {
				name = name[:idx]
			}
		}
		if name == "" || name == "-" {
			name = strings.ToLower(field.Name)
		}

		val, ok := m[name]
		if !ok {
			val, ok = m[strings.ToLower(name)]
		}
		if !ok {
			val, ok = m[field.Name]
		}
		if !ok {
			continue
		}

		if err := assignValue(fieldVal, val); err != nil {
			return fmt.Errorf("field %s: %w", name, err)
		}
	}
	return nil
}

// assignValue assigns a value to a reflect.Value with type conversion
func assignValue(field reflect.Value, val interface{}) error {
	if val == nil {
		return nil
	}

	valReflect := reflect.ValueOf(val)

	// Direct assignment if types match
	if valReflect.Type().AssignableTo(field.Type()) {
		field.Set(valReflect)
		return nil
	}

	// Type conversion
	if valReflect.Type().ConvertibleTo(field.Type()) {
		field.Set(valReflect.Convert(field.Type()))
		return nil
	}

	switch field.Kind() {
	case reflect.String:
		field.SetString(fmt.Sprintf("%v", val))
		return nil
	case reflect.Bool:
		if b, ok := val.(bool); ok {
			field.SetBool(b)
			return nil
		}
	}

	return fmt.Errorf("cannot assign %T to %v", val, field.Type())
}

// Node represents a graph node for HTTP API.
type Node struct {
	ID         string                 `json:"id"`
	Labels     []string               `json:"labels"`
	Properties map[string]interface{} `json:"properties"`
	CreatedAt  time.Time              `json:"created_at"`
}

// ListNodes returns nodes with optional label filter.
// Uses streaming iteration to avoid loading all nodes into memory.
func (db *DB) ListNodes(ctx context.Context, label string, limit, offset int) ([]*Node, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	var nodes []*Node
	count := 0

	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		// Filter by label if specified
		if label != "" {
			hasLabel := false
			for _, l := range n.Labels {
				if l == label {
					hasLabel = true
					break
				}
			}
			if !hasLabel {
				return nil // Skip, continue iteration
			}
		}

		// Handle offset
		if count < offset {
			count++
			return nil
		}

		// Handle limit - stop early when we have enough
		if len(nodes) >= limit {
			return storage.ErrIterationStopped
		}

		// Decrypt sensitive fields before returning
		decryptedProps := db.decryptProperties(n.Properties)

		nodes = append(nodes, &Node{
			ID:         string(n.ID),
			Labels:     n.Labels,
			Properties: decryptedProps,
			CreatedAt:  n.CreatedAt,
		})
		count++
		return nil
	})

	if err != nil && err != storage.ErrIterationStopped {
		return nil, err
	}

	return nodes, nil
}

// GetNode retrieves a node by ID.
func (db *DB) GetNode(ctx context.Context, id string) (*Node, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	n, err := db.storage.GetNode(storage.NodeID(id))
	if err != nil {
		return nil, ErrNotFound
	}

	// Decrypt sensitive fields after retrieval
	decryptedProps := db.decryptProperties(n.Properties)

	return &Node{
		ID:         string(n.ID),
		Labels:     n.Labels,
		Properties: decryptedProps,
		CreatedAt:  n.CreatedAt,
	}, nil
}

// CreateNode creates a new node.
func (db *DB) CreateNode(ctx context.Context, labels []string, properties map[string]interface{}) (*Node, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	id := generateID("node")
	now := time.Now()

	// Encrypt sensitive fields before storage (PHI/PII protection)
	encryptedProps := db.encryptProperties(properties)

	node := &storage.Node{
		ID:         storage.NodeID(id),
		Labels:     labels,
		Properties: encryptedProps,
		CreatedAt:  now,
	}

	actualID, err := db.storage.CreateNode(node)
	if err != nil {
		return nil, err
	}

	// Always queue for async embedding generation (non-blocking)
	if db.embedQueue != nil {
		db.embedQueue.Enqueue(string(actualID))
	}

	// Update search indexes (live indexing for seamless Mimir compatibility)
	if svc, _ := db.getOrCreateSearchService(db.defaultDatabaseName(), db.storage); svc != nil {
		_ = svc.IndexNode(node) // Best effort - search may lag behind writes
	}

	return &Node{
		ID:         id,
		Labels:     labels,
		Properties: properties,
		CreatedAt:  now,
	}, nil
}

// UpdateNode updates a node's properties.
func (db *DB) UpdateNode(ctx context.Context, id string, properties map[string]interface{}) (*Node, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	n, err := db.storage.GetNode(storage.NodeID(id))
	if err != nil {
		return nil, ErrNotFound
	}

	// If this update changes any embeddable content, invalidate managed embeddings so they
	// can be regenerated by the embed worker. We intentionally do NOT touch NamedEmbeddings
	// here (those are user-provided, e.g., Qdrant points).
	//
	// Embedding text is derived from labels + all non-metadata properties (see embed queue),
	// so any change to a non-metadata property should invalidate.
	invalidateManagedEmbeddings := false
	for k := range properties {
		if !isEmbeddingMetadataPropertyKey(k) {
			invalidateManagedEmbeddings = true
			break
		}
	}

	// Encrypt sensitive fields before merging
	encryptedProps := db.encryptProperties(properties)

	// Merge properties (encrypted values replace existing)
	for k, v := range encryptedProps {
		n.Properties[k] = v
	}

	if invalidateManagedEmbeddings {
		invalidateNodeManagedEmbeddings(n)
	}

	if err := db.storage.UpdateNode(n); err != nil {
		return nil, err
	}

	// Kick the embed worker so regeneration starts quickly (it will pull from the pending index).
	if invalidateManagedEmbeddings && db.embedQueue != nil {
		db.embedQueue.Trigger()
	}

	// Decrypt for return
	decryptedProps := db.decryptProperties(n.Properties)

	return &Node{
		ID:         string(n.ID),
		Labels:     n.Labels,
		Properties: decryptedProps,
		CreatedAt:  n.CreatedAt,
	}, nil
}

func isEmbeddingMetadataPropertyKey(key string) bool {
	switch key {
	// Internal embedding fields / markers
	case "embedding",
		"has_embedding",
		"embedding_skipped",
		"embedding_model",
		"embedding_dimensions",
		"embedded_at",
		"has_chunks",
		"chunk_count",
		// Common identity/timestamps that should not affect embedding text
		"createdAt",
		"updatedAt",
		"id":
		return true
	default:
		return false
	}
}

func invalidateNodeManagedEmbeddings(node *storage.Node) {
	if node == nil {
		return
	}

	// Managed embeddings live in ChunkEmbeddings (embed worker output).
	node.ChunkEmbeddings = nil

	// Clear embedding metadata from EmbedMeta
	node.EmbedMeta = nil
}

// DeleteNode deletes a node.
func (db *DB) DeleteNode(ctx context.Context, id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	// Remove from search indexes first (before storage deletion)
	if svc, _ := db.getOrCreateSearchService(db.defaultDatabaseName(), db.storage); svc != nil {
		_ = svc.RemoveNode(storage.NodeID(id))
	}

	return db.storage.DeleteNode(storage.NodeID(id))
}

// GraphEdge represents an edge for HTTP API.
type GraphEdge struct {
	ID         string                 `json:"id"`
	Source     string                 `json:"source"`
	Target     string                 `json:"target"`
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
}

// ListEdges returns edges with optional type filter.
func (db *DB) ListEdges(ctx context.Context, relType string, limit, offset int) ([]*GraphEdge, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	allEdges, err := db.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	var edges []*GraphEdge
	count := 0
	for _, e := range allEdges {
		// Filter by type if specified
		if relType != "" && e.Type != relType {
			continue
		}

		// Handle offset
		if count < offset {
			count++
			continue
		}

		// Handle limit
		if len(edges) >= limit {
			break
		}

		edges = append(edges, &GraphEdge{
			ID:         string(e.ID),
			Source:     string(e.StartNode),
			Target:     string(e.EndNode),
			Type:       e.Type,
			Properties: e.Properties,
			CreatedAt:  e.CreatedAt,
		})
		count++
	}

	return edges, nil
}

// GetEdge retrieves an edge by ID.
func (db *DB) GetEdge(ctx context.Context, id string) (*GraphEdge, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	e, err := db.storage.GetEdge(storage.EdgeID(id))
	if err != nil {
		return nil, ErrNotFound
	}

	return &GraphEdge{
		ID:         string(e.ID),
		Source:     string(e.StartNode),
		Target:     string(e.EndNode),
		Type:       e.Type,
		Properties: e.Properties,
		CreatedAt:  e.CreatedAt,
	}, nil
}

// GetEdgesForNode returns all edges (both incoming and outgoing) for a given node.
// This is useful for graph traversal and relationship inspection.
func (db *DB) GetEdgesForNode(ctx context.Context, nodeID string) ([]*GraphEdge, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	if nodeID == "" {
		return nil, ErrInvalidID
	}

	var edges []*GraphEdge
	storageNodeID := storage.NodeID(nodeID)

	// Get outgoing edges
	outgoing, err := db.storage.GetOutgoingEdges(storageNodeID)
	if err == nil {
		for _, e := range outgoing {
			edges = append(edges, &GraphEdge{
				ID:         string(e.ID),
				Source:     string(e.StartNode),
				Target:     string(e.EndNode),
				Type:       e.Type,
				Properties: e.Properties,
				CreatedAt:  e.CreatedAt,
			})
		}
	}

	// Get incoming edges
	incoming, err := db.storage.GetIncomingEdges(storageNodeID)
	if err == nil {
		for _, e := range incoming {
			edges = append(edges, &GraphEdge{
				ID:         string(e.ID),
				Source:     string(e.StartNode),
				Target:     string(e.EndNode),
				Type:       e.Type,
				Properties: e.Properties,
				CreatedAt:  e.CreatedAt,
			})
		}
	}

	return edges, nil
}

// CreateEdge creates a new edge.
func (db *DB) CreateEdge(ctx context.Context, source, target, edgeType string, properties map[string]interface{}) (*GraphEdge, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Verify nodes exist
	if _, err := db.storage.GetNode(storage.NodeID(source)); err != nil {
		return nil, fmt.Errorf("source node not found")
	}
	if _, err := db.storage.GetNode(storage.NodeID(target)); err != nil {
		return nil, fmt.Errorf("target node not found")
	}

	id := generateID("edge")
	now := time.Now()

	edge := &storage.Edge{
		ID:         storage.EdgeID(id),
		StartNode:  storage.NodeID(source),
		EndNode:    storage.NodeID(target),
		Type:       edgeType,
		Properties: properties,
		CreatedAt:  now,
	}

	if err := db.storage.CreateEdge(edge); err != nil {
		return nil, err
	}

	return &GraphEdge{
		ID:         id,
		Source:     source,
		Target:     target,
		Type:       edgeType,
		Properties: properties,
		CreatedAt:  now,
	}, nil
}

// DeleteEdge deletes an edge.
func (db *DB) DeleteEdge(ctx context.Context, id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	return db.storage.DeleteEdge(storage.EdgeID(id))
}

// SearchResult holds a search result with score.
type SearchResult struct {
	Node  *Node   `json:"node"`
	Score float64 `json:"score"`

	// RRF metadata (vector_rank/bm25_rank always emitted so clients see original
	// ranks even when Stage-2 reranking is applied; 0 = not in that result set)
	RRFScore   float64 `json:"rrf_score,omitempty"`
	VectorRank int     `json:"vector_rank"`
	BM25Rank   int     `json:"bm25_rank"`
}

// MapSearchResponse converts search service responses into DB API result shapes.
func MapSearchResponse(response *search.SearchResponse) []*SearchResult {
	if response == nil || len(response.Results) == 0 {
		return []*SearchResult{}
	}

	out := make([]*SearchResult, len(response.Results))
	for i := range response.Results {
		out[i] = mapSingleSearchResult(response.Results[i])
	}
	return out
}

func mapSingleSearchResult(r search.SearchResult) *SearchResult {
	return &SearchResult{
		Node: &Node{
			ID:         r.ID,
			Labels:     r.Labels,
			Properties: r.Properties,
		},
		Score:      r.Score,
		RRFScore:   r.RRFScore,
		VectorRank: r.VectorRank,
		BM25Rank:   r.BM25Rank,
	}
}

// Search performs full-text BM25 search.
// For hybrid vector+text search, use HybridSearch with pre-computed query embedding.
func (db *DB) Search(ctx context.Context, query string, labels []string, limit int) ([]*SearchResult, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	if err != nil || svc == nil {
		return nil, fmt.Errorf("search service not initialized")
	}

	// Get adaptive search options based on query
	opts := search.GetAdaptiveRRFConfig(query)
	opts.Limit = limit
	if len(labels) > 0 {
		opts.Types = labels
	}

	// Full-text search only (no embedding generation)
	// For hybrid search, Mimir should call VectorSearch with pre-computed embedding
	response, err := svc.Search(ctx, query, nil, opts)
	if err != nil {
		// DB API is expected to be usable right after Open() in tests and local usage.
		// If background BuildIndexes is still running, wait once for it and retry.
		if errors.Is(err, search.ErrSearchIndexBuilding) {
			if waitErr := db.ensureSearchIndexesBuilt(ctx, db.defaultDatabaseName()); waitErr == nil {
				response, err = svc.Search(ctx, query, nil, opts)
			}
		}
	}
	if err != nil {
		return nil, err
	}

	return MapSearchResponse(response), nil
}

// HybridSearch performs RRF hybrid search combining vector similarity and BM25 full-text.
// The queryEmbedding should be pre-computed by Mimir using its embedding service.
// This is the primary search method for semantic search with ranking fusion.
func (db *DB) HybridSearch(ctx context.Context, query string, queryEmbedding []float32, labels []string, limit int) ([]*SearchResult, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	if err != nil || svc == nil {
		return nil, fmt.Errorf("search service not initialized")
	}

	// Get adaptive search options based on query
	opts := search.GetAdaptiveRRFConfig(query)
	opts.Limit = limit
	if len(labels) > 0 {
		opts.Types = labels
	}

	// Execute RRF hybrid search with Mimir's pre-computed embedding
	response, err := svc.Search(ctx, query, queryEmbedding, opts)
	if err != nil {
		// If startup background indexing is still running, wait once and retry.
		if errors.Is(err, search.ErrSearchIndexBuilding) {
			if waitErr := db.ensureSearchIndexesBuilt(ctx, db.defaultDatabaseName()); waitErr == nil {
				response, err = svc.Search(ctx, query, queryEmbedding, opts)
			}
		}
	}
	if err != nil {
		return nil, err
	}

	return MapSearchResponse(response), nil
}

// FindSimilar finds nodes similar to a given node by embedding.
func (db *DB) FindSimilar(ctx context.Context, nodeID string, limit int) ([]*SearchResult, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0")
	}

	// Get target node
	target, err := db.storage.GetNode(storage.NodeID(nodeID))
	if err != nil {
		return nil, ErrNotFound
	}

	if len(target.ChunkEmbeddings) == 0 || len(target.ChunkEmbeddings[0]) == 0 {
		return nil, fmt.Errorf("node has no embedding")
	}

	// Find similar by embedding using streaming iteration
	type scored struct {
		node  *storage.Node
		score float64
	}
	var results []scored

	err = storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		// Skip self and nodes without embeddings
		if string(n.ID) == nodeID || len(n.ChunkEmbeddings) == 0 || len(n.ChunkEmbeddings[0]) == 0 {
			return nil
		}

		// Use first chunk embedding for similarity (both nodes should have at least one chunk)
		sim := vector.CosineSimilarity(target.ChunkEmbeddings[0], n.ChunkEmbeddings[0])

		// Maintain top-k results
		if len(results) < limit {
			results = append(results, scored{node: n, score: sim})
			if len(results) == limit {
				sort.Slice(results, func(i, j int) bool {
					return results[i].score > results[j].score
				})
			}
		} else if sim > results[limit-1].score {
			results[limit-1] = scored{node: n, score: sim}
			sort.Slice(results, func(i, j int) bool {
				return results[i].score > results[j].score
			})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Final sort
	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})

	searchResults := make([]*SearchResult, len(results))
	for i, r := range results {
		searchResults[i] = &SearchResult{
			Node: &Node{
				ID:         string(r.node.ID),
				Labels:     r.node.Labels,
				Properties: r.node.Properties,
				CreatedAt:  r.node.CreatedAt,
			},
			Score: r.score,
		}
	}

	return searchResults, nil
}

// GetLabels returns all distinct node labels.
// Uses streaming iteration to avoid loading all nodes into memory.
func (db *DB) GetLabels(ctx context.Context) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Use streaming helper for memory efficiency
	labels, err := storage.CollectLabels(ctx, db.storage)
	if err != nil {
		return nil, err
	}

	sort.Strings(labels)
	return labels, nil
}

// GetRelationshipTypes returns all distinct edge types.
// Uses streaming iteration to avoid loading all edges into memory.
func (db *DB) GetRelationshipTypes(ctx context.Context) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Use streaming helper for memory efficiency
	types, err := storage.CollectEdgeTypes(ctx, db.storage)
	if err != nil {
		return nil, err
	}

	sort.Strings(types)

	return types, nil
}

// IndexInfo holds index metadata.
type IndexInfo struct {
	Name     string `json:"name"`
	Label    string `json:"label"`
	Property string `json:"property"`
	Type     string `json:"type"` // btree, fulltext, vector
}

// toStringValue safely converts interface{} to string
func toStringValue(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// GetIndexes returns all indexes from the storage schema manager.
func (db *DB) GetIndexes(ctx context.Context) ([]*IndexInfo, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	schema := db.storage.GetSchema()
	if schema == nil {
		return []*IndexInfo{}, nil
	}

	// Get all indexes from schema manager
	rawIndexes := schema.GetIndexes()
	result := make([]*IndexInfo, 0, len(rawIndexes))

	for _, idx := range rawIndexes {
		if m, ok := idx.(map[string]interface{}); ok {
			info := &IndexInfo{
				Name:  toStringValue(m["name"]),
				Label: toStringValue(m["label"]),
				Type:  strings.ToLower(toStringValue(m["type"])),
			}
			// Handle single property or first property from array
			if prop, ok := m["property"].(string); ok {
				info.Property = prop
			} else if props, ok := m["properties"].([]string); ok && len(props) > 0 {
				info.Property = props[0]
			}
			result = append(result, info)
		}
	}

	return result, nil
}

// CreateIndex creates a new index on a label/property combination.
// Supported types: "property", "fulltext", "vector", "range"
func (db *DB) CreateIndex(ctx context.Context, label, property, indexType string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	schema := db.storage.GetSchema()
	if schema == nil {
		return fmt.Errorf("schema manager not initialized")
	}

	indexName := fmt.Sprintf("idx_%s_%s", strings.ToLower(label), strings.ToLower(property))

	switch strings.ToLower(indexType) {
	case "property", "btree":
		return schema.AddPropertyIndex(indexName, label, []string{property})
	case "fulltext":
		return schema.AddFulltextIndex(indexName, []string{label}, []string{property})
	case "vector":
		// Use configured embedding dimensions instead of hardcoded value
		dims := db.config.Memory.EmbeddingDimensions
		return schema.AddVectorIndex(indexName, label, property, dims, "cosine")
	case "range":
		return schema.AddRangeIndex(indexName, label, property)
	default:
		return fmt.Errorf("unsupported index type: %s (use: property, fulltext, vector, range)", indexType)
	}
}

// BootstrapCanonicalSchema creates standard constraints for the canonical Memory model.
// This is idempotent and can be safely called on startup.
func (db *DB) BootstrapCanonicalSchema(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	schema := db.storage.GetSchema()
	if schema == nil {
		return fmt.Errorf("schema manager not initialized")
	}

	required := []string{"id", "content", "tier", "decay_score", "last_accessed", "access_count"}
	for _, prop := range required {
		constraint := storage.Constraint{
			Name:       fmt.Sprintf("canonical_memory_%s_required", prop),
			Type:       storage.ConstraintExists,
			Label:      "Memory",
			Properties: []string{prop},
		}
		if err := storage.ValidateConstraintOnCreationForEngine(db.storage, constraint); err != nil {
			return err
		}
		if err := schema.AddConstraint(constraint); err != nil {
			return err
		}
	}

	nodeKey := storage.Constraint{
		Name:       "canonical_memory_id_key",
		Type:       storage.ConstraintNodeKey,
		Label:      "Memory",
		Properties: []string{"id"},
	}
	if err := storage.ValidateConstraintOnCreationForEngine(db.storage, nodeKey); err != nil {
		return err
	}
	if err := schema.AddConstraint(nodeKey); err != nil {
		return err
	}

	typeConstraints := map[string]storage.PropertyType{
		"id":            storage.PropertyTypeString,
		"content":       storage.PropertyTypeString,
		"tier":          storage.PropertyTypeString,
		"decay_score":   storage.PropertyTypeFloat,
		"last_accessed": storage.PropertyTypeString,
		"access_count":  storage.PropertyTypeInteger,
	}
	for prop, expectedType := range typeConstraints {
		name := fmt.Sprintf("canonical_memory_%s_type", prop)
		ptc := storage.PropertyTypeConstraint{
			Name:         name,
			Label:        "Memory",
			Property:     prop,
			ExpectedType: expectedType,
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(db.storage, ptc); err != nil {
			return err
		}
		if err := schema.AddPropertyTypeConstraint(name, "Memory", prop, expectedType); err != nil {
			return err
		}
	}

	return nil
}

// BackupableEngine is an interface for engines that support backup.
type BackupableEngine interface {
	Backup(path string) error
}

func (db *DB) rebuildTemporalIndexesNoLock(ctx context.Context) error {
	if maint, ok := db.baseStorage.(storage.TemporalMaintenanceEngine); ok {
		return maint.RebuildTemporalIndexes(ctx)
	}
	return nil
}

// RebuildTemporalIndexes rebuilds temporal history/current indexes from stored nodes.
func (db *DB) RebuildTemporalIndexes(ctx context.Context) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrClosed
	}
	return db.rebuildTemporalIndexesNoLock(ctx)
}

// PruneTemporalHistory prunes older closed temporal versions according to opts.
func (db *DB) PruneTemporalHistory(ctx context.Context, opts storage.TemporalPruneOptions) (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return 0, ErrClosed
	}
	if maint, ok := db.baseStorage.(storage.TemporalMaintenanceEngine); ok {
		return maint.PruneTemporalHistory(ctx, opts)
	}
	return 0, nil
}

// Backup creates a database backup to the specified path.
// For BadgerDB, this creates a streaming backup that is consistent and portable.
// For in-memory databases, it exports all data as JSON.
func (db *DB) Backup(ctx context.Context, path string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrClosed
	}

	// Check if storage engine supports backup
	if backupable, ok := db.storage.(BackupableEngine); ok {
		return backupable.Backup(path)
	}

	// Fallback: Export as JSON for non-backupable engines (memory)
	nodes, err := db.storage.AllNodes()
	if err != nil {
		return fmt.Errorf("failed to get nodes: %w", err)
	}

	edges, err := db.storage.AllEdges()
	if err != nil {
		return fmt.Errorf("failed to get edges: %w", err)
	}

	backup := map[string]interface{}{
		"version":    "1.0",
		"created_at": time.Now().Format(time.RFC3339),
		"nodes":      nodes,
		"edges":      edges,
	}

	data, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal backup: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write backup: %w", err)
	}

	return nil
}

// Restore restores the database from a JSON backup file.
// This is primarily for in-memory databases or cross-engine migration.
// For BadgerDB production use, use the storage-level backup/restore.
//
// Example:
//
//	err := db.Restore(ctx, "backup-20241201.json")
func (db *DB) Restore(ctx context.Context, path string) error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	db.mu.RUnlock()

	// Read backup file
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read backup: %w", err)
	}

	// Parse backup
	var backup struct {
		Version   string          `json:"version"`
		CreatedAt string          `json:"created_at"`
		Nodes     []*storage.Node `json:"nodes"`
		Edges     []*storage.Edge `json:"edges"`
	}

	if err := json.Unmarshal(data, &backup); err != nil {
		return fmt.Errorf("failed to parse backup: %w", err)
	}

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}

	// Restore nodes
	for _, node := range backup.Nodes {
		_, err := db.storage.CreateNode(node)
		if err != nil {
			// Try update if node exists
			if updateErr := db.storage.UpdateNode(node); updateErr != nil {
				db.mu.Unlock()
				return fmt.Errorf("failed to restore node %s: %w", node.ID, err)
			}
		}
	}

	// Restore edges
	for _, edge := range backup.Edges {
		if err := db.storage.CreateEdge(edge); err != nil {
			// Try update if edge exists
			if updateErr := db.storage.UpdateEdge(edge); updateErr != nil {
				db.mu.Unlock()
				return fmt.Errorf("failed to restore edge %s: %w", edge.ID, err)
			}
		}
	}

	if err := db.rebuildTemporalIndexesNoLock(ctx); err != nil {
		db.mu.Unlock()
		return fmt.Errorf("failed to rebuild temporal indexes after restore: %w", err)
	}
	db.mu.Unlock()

	// Restart search indexing after releasing the DB write lock; starting the
	// background search build while Restore still holds db.mu deadlocks via
	// startBackgroundTask's read lock.
	dbName := db.defaultDatabaseName()
	db.ResetSearchService(dbName)
	if svc, err := db.getOrCreateSearchService(dbName, db.storage); err == nil && svc != nil {
		db.searchServicesMu.RLock()
		entry := db.searchServices[dbName]
		db.searchServicesMu.RUnlock()
		if entry != nil {
			db.startSearchIndexBuild(entry, ctx)
		}
	} else if err != nil {
		log.Printf("⚠️  Warning: failed to restart search indexing after restore: %v", err)
	}

	return nil
}

// ExportUserData exports all data for a user (GDPR compliance).
// Uses streaming iteration to avoid loading all nodes into memory.
// Supports "json" (default) and "csv" formats.
func (db *DB) ExportUserData(ctx context.Context, userID, format string) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Collect user data using streaming
	var userData []map[string]interface{}
	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		if owner, ok := n.Properties["owner_id"].(string); ok && owner == userID {
			userData = append(userData, map[string]interface{}{
				"id":         string(n.ID),
				"labels":     n.Labels,
				"properties": n.Properties,
				"created_at": n.CreatedAt,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Format output
	if format == "csv" {
		return db.exportUserDataCSV(userData)
	}

	// Default to JSON
	return json.Marshal(map[string]interface{}{
		"user_id":     userID,
		"data":        userData,
		"exported_at": time.Now(),
	})
}

// exportUserDataCSV converts user data to CSV format.
func (db *DB) exportUserDataCSV(userData []map[string]interface{}) ([]byte, error) {
	var buf bytes.Buffer

	// Collect all unique property keys across all nodes
	propertyKeys := make(map[string]bool)
	for _, data := range userData {
		if props, ok := data["properties"].(map[string]interface{}); ok {
			for k := range props {
				propertyKeys[k] = true
			}
		}
	}

	// Sort property keys for consistent output
	sortedKeys := make([]string, 0, len(propertyKeys))
	for k := range propertyKeys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Write CSV header
	headers := []string{"id", "labels", "created_at"}
	headers = append(headers, sortedKeys...)
	buf.WriteString(strings.Join(headers, ",") + "\n")

	// Write data rows
	for _, data := range userData {
		row := make([]string, len(headers))

		// ID
		row[0] = escapeCSV(toStringValue(data["id"]))

		// Labels
		if labels, ok := data["labels"].([]string); ok {
			row[1] = escapeCSV(strings.Join(labels, ";"))
		} else {
			row[1] = ""
		}

		// Created at
		if createdAt, ok := data["created_at"].(time.Time); ok {
			row[2] = escapeCSV(createdAt.Format(time.RFC3339))
		} else {
			row[2] = ""
		}

		// Properties
		if props, ok := data["properties"].(map[string]interface{}); ok {
			for i, key := range sortedKeys {
				if val, exists := props[key]; exists {
					row[3+i] = escapeCSV(toStringValue(val))
				} else {
					row[3+i] = ""
				}
			}
		}

		buf.WriteString(strings.Join(row, ",") + "\n")
	}

	return buf.Bytes(), nil
}

// escapeCSV escapes a string for CSV output
func escapeCSV(s string) string {
	needsQuote := strings.ContainsAny(s, ",\"\n\r")
	if !needsQuote {
		return s
	}
	// Escape double quotes by doubling them
	escaped := strings.ReplaceAll(s, "\"", "\"\"")
	return "\"" + escaped + "\""
}

// DeleteUserData deletes all data for a user (GDPR compliance).
// Uses streaming iteration to avoid loading all nodes into memory.
func (db *DB) DeleteUserData(ctx context.Context, userID string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	// Collect IDs to delete first (can't delete while iterating)
	var toDelete []storage.NodeID
	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		if owner, ok := n.Properties["owner_id"].(string); ok && owner == userID {
			toDelete = append(toDelete, n.ID)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Now delete the collected nodes
	for _, id := range toDelete {
		// Remove from search indexes first (before storage deletion)
		if svc, _ := db.getOrCreateSearchService(db.defaultDatabaseName(), db.storage); svc != nil {
			_ = svc.RemoveNode(id)
		}
		if err := db.storage.DeleteNode(id); err != nil {
			return err
		}
	}

	return nil
}

// AnonymizeUserData anonymizes all data for a user (GDPR compliance).
// Uses streaming iteration to avoid loading all nodes into memory.
func (db *DB) AnonymizeUserData(ctx context.Context, userID string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	anonymousID := generateID("")

	// Collect nodes to update (can't update while streaming in some engines)
	// We must make copies since other goroutines might be iterating over these nodes
	var toUpdate []*storage.Node
	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
		if owner, ok := n.Properties["owner_id"].(string); ok && owner == userID {
			// Make a deep copy of the node to avoid concurrent modification
			nodeCopy := &storage.Node{
				ID:           n.ID,
				Labels:       append([]string(nil), n.Labels...),
				Properties:   make(map[string]any, len(n.Properties)),
				CreatedAt:    n.CreatedAt,
				UpdatedAt:    n.UpdatedAt,
				DecayScore:   n.DecayScore,
				LastAccessed: n.LastAccessed,
				AccessCount:  n.AccessCount,
				ChunkEmbeddings: func() [][]float32 {
					chunks := make([][]float32, len(n.ChunkEmbeddings))
					for i, emb := range n.ChunkEmbeddings {
						chunks[i] = append([]float32(nil), emb...)
					}
					return chunks
				}(),
			}
			for k, v := range n.Properties {
				nodeCopy.Properties[k] = v
			}

			// Replace identifying info in the copy
			nodeCopy.Properties["owner_id"] = anonymousID
			delete(nodeCopy.Properties, "email")
			delete(nodeCopy.Properties, "name")
			delete(nodeCopy.Properties, "username")
			delete(nodeCopy.Properties, "ip_address")
			toUpdate = append(toUpdate, nodeCopy)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Now update the collected nodes
	for _, n := range toUpdate {
		if err := db.storage.UpdateNode(n); err != nil {
			return err
		}
	}

	return nil
}
