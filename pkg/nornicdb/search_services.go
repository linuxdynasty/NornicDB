package nornicdb

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type dbSearchService struct {
	dbName string
	engine storage.Engine
	svc    *search.Service

	buildOnce  sync.Once
	buildDone  chan struct{}
	buildErr   error
	buildErrMu sync.RWMutex

	clusterMu               sync.Mutex
	lastClusteredEmbedCount int

	pendingMu    sync.Mutex
	pendingOps   map[string]pendingSearchMutation
	pendingFlush sync.Once
}

type pendingSearchMutation struct {
	node   *storage.Node
	remove bool
}

func (e *dbSearchService) queueIndex(node *storage.Node) {
	if e == nil || node == nil {
		return
	}
	e.pendingMu.Lock()
	if e.pendingOps == nil {
		e.pendingOps = make(map[string]pendingSearchMutation)
	}
	e.pendingOps[string(node.ID)] = pendingSearchMutation{node: storage.CopyNode(node), remove: false}
	e.pendingMu.Unlock()
}

func (e *dbSearchService) queueRemove(localID string) {
	if e == nil || localID == "" {
		return
	}
	e.pendingMu.Lock()
	if e.pendingOps == nil {
		e.pendingOps = make(map[string]pendingSearchMutation)
	}
	e.pendingOps[localID] = pendingSearchMutation{remove: true}
	e.pendingMu.Unlock()
}

func (e *dbSearchService) drainPending() map[string]pendingSearchMutation {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	if len(e.pendingOps) == 0 {
		return nil
	}
	out := e.pendingOps
	e.pendingOps = make(map[string]pendingSearchMutation)
	return out
}

// DatabaseSearchStatus exposes per-database search service readiness/progress.
type DatabaseSearchStatus struct {
	Ready           bool    `json:"ready"`
	Building        bool    `json:"building"`
	Initialized     bool    `json:"initialized"`
	Phase           string  `json:"phase,omitempty"`
	ProcessedNodes  int64   `json:"processed_nodes,omitempty"`
	TotalNodes      int64   `json:"total_nodes,omitempty"`
	RateNodesPerSec float64 `json:"rate_nodes_per_sec,omitempty"`
	ETASeconds      int64   `json:"eta_seconds,omitempty"`
}

func splitQualifiedID(id string) (dbName string, local string, ok bool) {
	dbName, local, ok = strings.Cut(id, ":")
	if !ok || dbName == "" || local == "" {
		return "", "", false
	}
	return dbName, local, true
}

func (db *DB) defaultDatabaseName() string {
	if namespaced, ok := db.storage.(*storage.NamespacedEngine); ok {
		return namespaced.Namespace()
	}
	// DB storage must always be namespaced; anything else is a programmer error.
	panic("nornicdb: DB storage is not namespaced")
}

// kmeansNumClusters returns the configured number of k-means clusters (0 = auto from dataset size).
// Used when enabling clustering so EnableClustering receives the configured value.
func (db *DB) kmeansNumClusters() int {
	if db.config != nil {
		return db.config.Memory.KmeansNumClusters
	}
	return 0
}

func (db *DB) getOrCreateSearchService(dbName string, storageEngine storage.Engine) (*search.Service, error) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	if dbName == "system" {
		return nil, fmt.Errorf("search service not available for system database")
	}

	dims := db.embeddingDims
	minSim := db.searchMinSimilarity
	bm25Engine := search.DefaultBM25Engine()
	db.dbConfigResolverMu.RLock()
	resolver := db.dbConfigResolver
	db.dbConfigResolverMu.RUnlock()
	if resolver != nil {
		rd, rs, re := resolver(dbName)
		if rd > 0 {
			dims = rd
		}
		minSim = rs
		if re != "" {
			bm25Engine = re
		}
	}

	var gpuMgr *gpu.Manager
	db.gpuManagerMu.RLock()
	if m, ok := db.gpuManager.(*gpu.Manager); ok {
		gpuMgr = m
	}
	db.gpuManagerMu.RUnlock()

	db.searchServicesMu.RLock()
	if entry, ok := db.searchServices[dbName]; ok {
		svc := entry.svc
		reranker := db.searchReranker
		rr := db.rerankerResolver
		db.searchServicesMu.RUnlock()
		if rr != nil {
			reranker = rr(dbName)
		}

		// If clustering is enabled globally, ensure cached services have clustering enabled too.
		// Services may be created before the feature flag is turned on (e.g., early HTTP calls),
		// in which case they need to be upgraded in place. Use configured cluster count.
		if svc != nil && featureflags.IsGPUClusteringEnabled() && !svc.IsClusteringEnabled() {
			var mgr *gpu.Manager
			if gpuMgr != nil && gpuMgr.IsEnabled() {
				mgr = gpuMgr
			}
			svc.EnableClustering(mgr, db.kmeansNumClusters())
		}
		// If a Stage-2 reranker is configured (e.g. Heimdall LLM), ensure it is applied.
		// This keeps behavior consistent even if the reranker is configured after the
		// service was created (e.g. Heimdall initializes later in server startup).
		if svc != nil {
			svc.SetReranker(reranker)
		}
		return svc, nil
	}
	db.searchServicesMu.RUnlock()

	// Serialize creation so only one goroutine is in the create+insert path at a time,
	// avoiding RWMutex deadlock when background build, flush indexNodeFromEvent, and CreateNode contend.
	db.searchServiceCreationMu.Lock()
	defer db.searchServiceCreationMu.Unlock()

	// Re-check after acquiring creation lock; another goroutine may have created it.
	db.searchServicesMu.RLock()
	if entry, ok := db.searchServices[dbName]; ok {
		svc := entry.svc
		db.searchServicesMu.RUnlock()
		return svc, nil
	}
	db.searchServicesMu.RUnlock()

	if storageEngine == nil {
		if db.baseStorage == nil {
			return nil, fmt.Errorf("search service unavailable: base storage is nil")
		}
		storageEngine = storage.NewNamespacedEngine(db.baseStorage, dbName)
	}

	if dims <= 0 {
		dims = 1024
	}
	svc := search.NewServiceWithDimensionsAndBM25Engine(storageEngine, dims, bm25Engine)
	svc.SetDefaultMinSimilarity(minSim)
	persistSearchIndexesEnabled := db.config != nil && db.config.Database.DataDir != "" && db.config.Database.PersistSearchIndexes
	svc.SetPersistenceEnabled(persistSearchIndexesEnabled)

	// EXPERIMENTAL: when PersistSearchIndexes is true, set paths so BuildIndexes saves indexes after a
	// build and loads them on startup (skipping the full iteration when both are present).
	// HNSW is also persisted so the approximate nearest-neighbor index does not need rebuilding.
	// If a rebuild is required (e.g., missing/incompatible artifacts), IVF-HNSW rebuild at startup
	// can be long on large datasets (~30 minutes for ~1M embeddings on observed hardware).
	if persistSearchIndexesEnabled {
		base := filepath.Join(db.config.Database.DataDir, "search", dbName)
		fulltextFilename := "bm25"
		if strings.EqualFold(strings.TrimSpace(bm25Engine), search.BM25EngineV2) {
			fulltextFilename = "bm25.v2"
		}
		svc.SetFulltextIndexPath(filepath.Join(base, fulltextFilename))
		svc.SetVectorIndexPath(filepath.Join(base, "vectors"))
		svc.SetHNSWIndexPath(filepath.Join(base, "hnsw"))
	}

	// Enable GPU brute-force search if a GPU manager is configured.
	if gpuMgr != nil {
		svc.SetGPUManager(gpuMgr)
	}

	// Enable per-database clustering if the feature flag is enabled.
	// Each Service maintains its own cluster index and must cluster independently.
	// Cluster count comes from db.config.Memory.KmeansNumClusters (0 = auto from dataset size).
	if featureflags.IsGPUClusteringEnabled() {
		var mgr *gpu.Manager
		if gpuMgr != nil && gpuMgr.IsEnabled() {
			mgr = gpuMgr
		}
		numClusters := db.kmeansNumClusters()
		svc.EnableClustering(mgr, numClusters)
	}

	// Apply configured Stage-2 reranker (if any). Use per-DB resolver when set.
	db.searchServicesMu.RLock()
	reranker := db.searchReranker
	rr := db.rerankerResolver
	db.searchServicesMu.RUnlock()
	if rr != nil {
		reranker = rr(dbName)
	}
	svc.SetReranker(reranker)

	entry := &dbSearchService{
		dbName:    dbName,
		engine:    storageEngine,
		svc:       svc,
		buildDone: make(chan struct{}),
	}

	db.searchServicesMu.Lock()
	// Double-check in case another goroutine created it while we were building.
	if existing, ok := db.searchServices[dbName]; ok {
		db.searchServicesMu.Unlock()
		return existing.svc, nil
	}
	db.searchServices[dbName] = entry
	db.searchServicesMu.Unlock()

	return svc, nil
}

// SetSearchReranker configures the Stage-2 reranker for all per-database search services.
//
// This is typically set by the server when Heimdall is enabled and the
// vector rerank feature flag is turned on.
func (db *DB) SetSearchReranker(r search.Reranker) {
	db.searchServicesMu.Lock()
	db.searchReranker = r
	entries := make([]*dbSearchService, 0, len(db.searchServices))
	for _, entry := range db.searchServices {
		entries = append(entries, entry)
	}
	db.searchServicesMu.Unlock()

	for _, entry := range entries {
		if entry == nil || entry.svc == nil {
			continue
		}
		entry.svc.SetReranker(r)
	}
}

// SetRerankerResolver sets an optional function that returns the reranker for a given database.
// When set, getOrCreateSearchService uses it instead of the single global searchReranker (enables per-DB rerankers).
func (db *DB) SetRerankerResolver(fn func(dbName string) search.Reranker) {
	db.searchServicesMu.Lock()
	db.rerankerResolver = fn
	db.searchServicesMu.Unlock()
}

// GetOrCreateSearchService returns the per-database search service for dbName.
//
// storageEngine should be a *storage.NamespacedEngine for dbName (typically
// obtained via multidb.DatabaseManager). If nil, db.baseStorage is wrapped with
// a NamespacedEngine for dbName.
func (db *DB) GetOrCreateSearchService(dbName string, storageEngine storage.Engine) (*search.Service, error) {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	return db.getOrCreateSearchService(dbName, storageEngine)
}

// ResetSearchService drops the cached search service for a database.
// The next call to GetOrCreateSearchService will create a fresh, empty service.
func (db *DB) ResetSearchService(dbName string) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	db.searchServicesMu.Lock()
	delete(db.searchServices, dbName)
	db.searchServicesMu.Unlock()
}

// GetDatabaseSearchStatus returns readiness and progress for the database search service.
// It does not create a new service; it reports status for existing cached services only.
func (db *DB) GetDatabaseSearchStatus(dbName string) DatabaseSearchStatus {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	db.searchServicesMu.RLock()
	entry, ok := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if !ok || entry == nil || entry.svc == nil {
		return DatabaseSearchStatus{Ready: false, Building: false, Initialized: false, Phase: "not_initialized", ETASeconds: -1}
	}
	p := entry.svc.GetBuildProgress()
	return DatabaseSearchStatus{
		Ready:           p.Ready,
		Building:        p.Building,
		Initialized:     true,
		Phase:           p.Phase,
		ProcessedNodes:  p.ProcessedNodes,
		TotalNodes:      p.TotalNodes,
		RateNodesPerSec: p.RateNodesPerSec,
		ETASeconds:      p.ETASeconds,
	}
}

func (db *DB) startSearchIndexBuild(entry *dbSearchService, ctx context.Context) {
	if entry == nil || entry.svc == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	entry.buildOnce.Do(func() {
		db.bgWg.Add(1)
		go func() {
			defer db.bgWg.Done()
			err := entry.svc.BuildIndexes(ctx)
			entry.buildErrMu.Lock()
			entry.buildErr = err
			if err == nil {
				// Mark clustering as current so runClusteringOnceAllDatabases skips this db
				// (we either restored IVF-HNSW from disk or ran k-means in warmup).
				entry.clusterMu.Lock()
				entry.lastClusteredEmbedCount = entry.svc.EmbeddingCount()
				entry.clusterMu.Unlock()
			}
			entry.buildErrMu.Unlock()
			close(entry.buildDone)
		}()
	})
}

func (db *DB) ensurePendingFlush(entry *dbSearchService) {
	if entry == nil || entry.svc == nil {
		return
	}
	entry.pendingFlush.Do(func() {
		db.bgWg.Add(1)
		go func() {
			defer db.bgWg.Done()
			<-entry.buildDone

			for {
				ops := entry.drainPending()
				if len(ops) == 0 {
					return
				}
				ids := make([]string, 0, len(ops))
				for id := range ops {
					ids = append(ids, id)
				}
				sort.Strings(ids)
				for _, id := range ids {
					op := ops[id]
					if op.remove {
						if err := entry.svc.RemoveNode(storage.NodeID(id)); err != nil {
							log.Printf("⚠️ Failed to remove node %s from deferred search mutation in db %s: %v", id, entry.dbName, err)
						}
						continue
					}
					if op.node == nil {
						continue
					}
					if err := entry.svc.IndexNode(op.node); err != nil {
						log.Printf("⚠️ Failed to index node %s from deferred search mutation in db %s: %v", id, entry.dbName, err)
					}
				}
			}
		}()
	})
}

func (db *DB) ensureSearchIndexesBuilt(ctx context.Context, dbName string) error {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}

	db.searchServicesMu.RLock()
	entry, ok := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if !ok || entry == nil {
		return fmt.Errorf("search service not initialized for database %q", dbName)
	}
	db.startSearchIndexBuild(entry, ctx)
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-entry.buildDone:
		entry.buildErrMu.RLock()
		err := entry.buildErr
		entry.buildErrMu.RUnlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// EnsureSearchIndexesBuilt ensures the per-database search indexes are built exactly once.
// If the service doesn’t exist yet, it is created (using storageEngine if provided).
func (db *DB) EnsureSearchIndexesBuilt(ctx context.Context, dbName string, storageEngine storage.Engine) (*search.Service, error) {
	svc, err := db.getOrCreateSearchService(dbName, storageEngine)
	if err != nil {
		return nil, err
	}
	if err := db.ensureSearchIndexesBuilt(ctx, dbName); err != nil {
		return svc, err
	}
	return svc, nil
}

// EnsureSearchIndexesBuildStarted starts per-database search indexing if not already started
// and returns immediately without waiting for completion.
func (db *DB) EnsureSearchIndexesBuildStarted(dbName string, storageEngine storage.Engine) (*search.Service, error) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	svc, err := db.getOrCreateSearchService(dbName, storageEngine)
	if err != nil {
		return nil, err
	}
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if entry == nil {
		return svc, nil
	}
	ctx := db.buildCtx
	if ctx == nil {
		ctx = context.Background()
	}
	db.startSearchIndexBuild(entry, ctx)
	return svc, nil
}

func (db *DB) indexNodeFromEvent(node *storage.Node) {
	if node == nil {
		return
	}

	dbName, local, ok := splitQualifiedID(string(node.ID))
	if !ok {
		// Unprefixed IDs are not supported. This indicates a bug in the storage event pipeline.
		log.Printf("⚠️ storage event had unprefixed node ID: %q", node.ID)
		return
	}
	// Qdrant gRPC points are stored under a reserved sub-namespace and are indexed
	// by the Qdrant vector index cache, not the hybrid search service.
	if strings.HasPrefix(local, "qdrant:") {
		return
	}

	svc, err := db.getOrCreateSearchService(dbName, nil)
	if err != nil || svc == nil {
		return
	}

	userNode := storage.CopyNode(node)
	userNode.ID = storage.NodeID(local)

	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if entry != nil {
		progress := svc.GetBuildProgress()
		if progress.Building || !progress.Ready {
			entry.queueIndex(userNode)
			ctx := db.buildCtx
			if ctx == nil {
				ctx = context.Background()
			}
			db.startSearchIndexBuild(entry, ctx)
			db.ensurePendingFlush(entry)
			return
		}
	}

	if err := svc.IndexNode(userNode); err != nil {
		log.Printf("⚠️ Failed to index node %s in db %s: %v", node.ID, dbName, err)
	}
}

func (db *DB) removeNodeFromEvent(nodeID storage.NodeID) {
	dbName, local, ok := splitQualifiedID(string(nodeID))
	if !ok {
		// Unprefixed ID (e.g. single-db or callback from engine that doesn't prefix).
		// Use default database and the ID as-is so embeddings are still removed.
		dbName = db.defaultDatabaseName()
		local = string(nodeID)
	}

	db.searchServicesMu.RLock()
	entry, ok := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if !ok || entry == nil {
		// Service not in cache yet; nothing to remove.
		return
	}

	progress := entry.svc.GetBuildProgress()
	if progress.Building || !progress.Ready {
		entry.queueRemove(local)
		ctx := db.buildCtx
		if ctx == nil {
			ctx = context.Background()
		}
		db.startSearchIndexBuild(entry, ctx)
		db.ensurePendingFlush(entry)
		return
	}

	if err := entry.svc.RemoveNode(storage.NodeID(local)); err != nil {
		log.Printf("⚠️ Failed to remove node %s from search indexes in db %s: %v", nodeID, dbName, err)
	}
}

// runClusteringOnceAllDatabases runs k-means for each database. Stops when ctx is cancelled (e.g. shutdown).
func (db *DB) runClusteringOnceAllDatabases(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Ensure the default database service exists so an immediate clustering run
	// produces deterministic behavior even before the first search request.
	if _, err := db.getOrCreateSearchService(db.defaultDatabaseName(), db.storage); err != nil {
		log.Printf("⚠️  K-means clustering: failed to initialize default search service: %v", err)
	}

	// If storage can enumerate namespaces, initialize per-database services so
	// clustering can run across all known databases (excluding system).
	if lister, ok := db.baseStorage.(storage.NamespaceLister); ok {
		for _, ns := range lister.ListNamespaces() {
			if ns == "" || ns == "system" {
				continue
			}
			if _, err := db.getOrCreateSearchService(ns, nil); err != nil {
				log.Printf("⚠️  K-means clustering: failed to initialize search service for db %s: %v", ns, err)
			}
		}
	}

	db.searchServicesMu.RLock()
	entries := make([]*dbSearchService, 0, len(db.searchServices))
	for _, entry := range db.searchServices {
		entries = append(entries, entry)
	}
	db.searchServicesMu.RUnlock()

	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if entry == nil || entry.dbName == "system" {
			continue
		}
		if entry.svc == nil || !entry.svc.IsClusteringEnabled() {
			continue
		}
		progress := entry.svc.GetBuildProgress()
		// Do not run timer/manual clustering while initial search build is still in progress.
		// BuildIndexes warmup already runs BM25-seeded k-means + IVF-HNSW when enabled.
		if progress.Building {
			log.Printf("🔬 K-means clustering deferred for db %s: search build in progress (phase=%s)", entry.dbName, progress.Phase)
			continue
		}
		// Also skip until initial build reaches ready; otherwise we'd cluster partial indexes.
		if !progress.Ready {
			log.Printf("🔬 K-means clustering deferred for db %s: search not ready yet (phase=%s)", entry.dbName, progress.Phase)
			continue
		}
		if entry.svc.ClusteringInProgress() {
			continue
		}

		// Serialize clustering per database to avoid duplicate work when multiple
		// triggers fire concurrently (startup hooks, manual triggers, timer ticks).
		// Hold clusterMu only for the check and the count update; do not hold it across
		// TriggerClustering (k-means + rebuildClusterHNSW can take a long time and would
		// block other code that needs clusterMu or that triggers IndexNode while rebuild
		// holds vector index read lock).
		entry.clusterMu.Lock()
		currentCount := entry.svc.EmbeddingCount()
		shouldRun := currentCount != entry.lastClusteredEmbedCount || entry.lastClusteredEmbedCount == 0
		entry.clusterMu.Unlock()
		if !shouldRun {
			continue
		}

		if err := entry.svc.TriggerClustering(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  K-means clustering skipped for db %s: %v", entry.dbName, err)
			continue
		}

		entry.clusterMu.Lock()
		entry.lastClusteredEmbedCount = entry.svc.EmbeddingCount()
		doneCount := entry.lastClusteredEmbedCount
		entry.clusterMu.Unlock()
		log.Printf("🔬 K-means clustering completed for db %s (%d embeddings)", entry.dbName, doneCount)
	}
}
