package dbconfig

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// ResolvedDbConfig holds effective per-DB config for search and embedding.
// Used by getOrCreateSearchService and by the admin API "effective" response.
type ResolvedDbConfig struct {
	// EmbeddingDimensions is the vector size (e.g. 1024).
	EmbeddingDimensions int
	// SearchMinSimilarity is the min cosine similarity for vector search (0.0–1.0).
	SearchMinSimilarity float64
	// BM25Engine selects fulltext engine implementation ("v1" or "v2").
	BM25Engine string
	// Effective is the full effective value for every allowed key (string form for API).
	Effective map[string]string
}

// Resolve merges global config with per-DB overrides and returns the resolved config.
// Overrides are applied on top of global; omitted keys use global default.
func Resolve(global *config.Config, overrides map[string]string) *ResolvedDbConfig {
	r := &ResolvedDbConfig{
		EmbeddingDimensions: global.Memory.EmbeddingDimensions,
		SearchMinSimilarity: global.Memory.SearchMinSimilarity,
		BM25Engine:          normalizeBM25Engine(os.Getenv("NORNICDB_SEARCH_BM25_ENGINE")),
		Effective:           make(map[string]string),
	}
	if r.EmbeddingDimensions <= 0 {
		r.EmbeddingDimensions = 1024
	}
	// Build effective map from global (we'll overlay overrides below).
	effectiveFromGlobal(global, r.Effective)
	for k, v := range overrides {
		if !IsAllowedKey(k) {
			continue
		}
		applyOverride(r, k, v)
		r.Effective[k] = v
	}
	return r
}

func effectiveFromGlobal(c *config.Config, m map[string]string) {
	if c == nil {
		return
	}
	// Embeddings
	m["NORNICDB_EMBEDDING_ENABLED"] = boolStr(c.Memory.EmbeddingEnabled)
	m["NORNICDB_EMBEDDING_PROVIDER"] = c.Memory.EmbeddingProvider
	m["NORNICDB_EMBEDDING_MODEL"] = c.Memory.EmbeddingModel
	m["NORNICDB_EMBEDDING_API_URL"] = c.Memory.EmbeddingAPIURL
	m["NORNICDB_EMBEDDING_API_KEY"] = c.Memory.EmbeddingAPIKey
	m["NORNICDB_EMBEDDING_DIMENSIONS"] = strconv.Itoa(c.Memory.EmbeddingDimensions)
	if c.Memory.EmbeddingDimensions <= 0 {
		m["NORNICDB_EMBEDDING_DIMENSIONS"] = "1024"
	}
	m["NORNICDB_EMBEDDING_CACHE_SIZE"] = strconv.Itoa(c.Memory.EmbeddingCacheSize)
	m["NORNICDB_EMBEDDING_PROPERTIES_INCLUDE"] = strings.Join(c.EmbeddingWorker.PropertiesInclude, ",")
	m["NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE"] = strings.Join(c.EmbeddingWorker.PropertiesExclude, ",")
	m["NORNICDB_EMBEDDING_INCLUDE_LABELS"] = boolStr(c.EmbeddingWorker.IncludeLabels)
	m["NORNICDB_EMBEDDING_GPU_LAYERS"] = strconv.Itoa(c.Memory.EmbeddingGPULayers)
	m["NORNICDB_EMBEDDING_WARMUP_INTERVAL"] = c.Memory.EmbeddingWarmupInterval.String()
	// Search
	m["NORNICDB_SEARCH_MIN_SIMILARITY"] = strconv.FormatFloat(c.Memory.SearchMinSimilarity, 'f', -1, 64)
	m["NORNICDB_SEARCH_BM25_ENGINE"] = normalizeBM25Engine(os.Getenv("NORNICDB_SEARCH_BM25_ENGINE"))
	m["NORNICDB_SEARCH_RERANK_ENABLED"] = boolStr(c.Features.SearchRerankEnabled)
	m["NORNICDB_SEARCH_RERANK_PROVIDER"] = c.Features.SearchRerankProvider
	m["NORNICDB_SEARCH_RERANK_MODEL"] = c.Features.SearchRerankModel
	m["NORNICDB_SEARCH_RERANK_API_URL"] = c.Features.SearchRerankAPIURL
	m["NORNICDB_SEARCH_RERANK_API_KEY"] = c.Features.SearchRerankAPIKey
	// K-means (from Memory)
	m["NORNICDB_KMEANS_MIN_EMBEDDINGS"] = strconv.Itoa(c.Memory.KmeansMinEmbeddings)
	m["NORNICDB_KMEANS_CLUSTER_INTERVAL"] = c.Memory.KmeansClusterInterval.String()
	m["NORNICDB_KMEANS_NUM_CLUSTERS"] = strconv.Itoa(c.Memory.KmeansNumClusters)
	// Auto-links
	m["NORNICDB_AUTO_LINKS_ENABLED"] = boolStr(c.Memory.AutoLinksEnabled)
	m["NORNICDB_AUTO_LINKS_THRESHOLD"] = strconv.FormatFloat(c.Memory.AutoLinksSimilarityThreshold, 'f', -1, 64)
	// Embed worker
	m["NORNICDB_EMBED_WORKER_NUM_WORKERS"] = strconv.Itoa(c.EmbeddingWorker.NumWorkers)
	m["NORNICDB_EMBED_SCAN_INTERVAL"] = c.EmbeddingWorker.ScanInterval.String()
	m["NORNICDB_EMBED_BATCH_DELAY"] = c.EmbeddingWorker.BatchDelay.String()
	m["NORNICDB_EMBED_TRIGGER_DEBOUNCE"] = c.EmbeddingWorker.TriggerDebounceDelay.String()
	m["NORNICDB_EMBED_MAX_RETRIES"] = strconv.Itoa(c.EmbeddingWorker.MaxRetries)
	m["NORNICDB_EMBED_CHUNK_SIZE"] = strconv.Itoa(c.EmbeddingWorker.ChunkSize)
	m["NORNICDB_EMBED_CHUNK_OVERLAP"] = strconv.Itoa(c.EmbeddingWorker.ChunkOverlap)
	m["NORNICDB_MVCC_LIFECYCLE_INTERVAL"] = c.Database.MVCCLifecycleCycleInterval.String()
	// Feature flags for Auto-TLP (from Features; K-means clustering is env-only in feature_flags)
	m["NORNICDB_AUTO_TLP_ENABLED"] = boolStr(c.Features.TopologyAutoIntegrationEnabled)
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func applyOverride(r *ResolvedDbConfig, key, value string) {
	value = strings.TrimSpace(value)
	meta, ok := AllowedKeysSet()[key]
	if !ok {
		return
	}
	switch meta.Type {
	case "number":
		if i, err := strconv.Atoi(value); err == nil {
			switch key {
			case "NORNICDB_EMBEDDING_DIMENSIONS":
				r.EmbeddingDimensions = i
				if r.EmbeddingDimensions <= 0 {
					r.EmbeddingDimensions = 1024
				}
			case "NORNICDB_SEARCH_MIN_SIMILARITY":
				// stored as string for generic keys; we also set float
				if f, err := strconv.ParseFloat(value, 64); err == nil {
					r.SearchMinSimilarity = f
				}
			}
		} else if f, err := strconv.ParseFloat(value, 64); err == nil {
			if key == "NORNICDB_SEARCH_MIN_SIMILARITY" || key == "NORNICDB_AUTO_LINKS_THRESHOLD" {
				r.SearchMinSimilarity = f
			}
		}
	case "boolean":
		b := value == "true" || value == "1"
		_ = b
		// Only EmbeddingDimensions and SearchMinSimilarity are used in ResolvedDbConfig for now
	case "string", "duration":
		if key == "NORNICDB_SEARCH_MIN_SIMILARITY" {
			if f, err := strconv.ParseFloat(value, 64); err == nil {
				r.SearchMinSimilarity = f
			}
		}
	}
	// Always update EmbeddingDimensions and SearchMinSimilarity when present in overrides
	if key == "NORNICDB_EMBEDDING_DIMENSIONS" {
		if i, err := strconv.Atoi(value); err == nil && i > 0 {
			r.EmbeddingDimensions = i
		}
	}
	if key == "NORNICDB_SEARCH_MIN_SIMILARITY" {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			r.SearchMinSimilarity = f
		}
	}
	if key == "NORNICDB_SEARCH_BM25_ENGINE" {
		r.BM25Engine = normalizeBM25Engine(value)
	}
}

func normalizeBM25Engine(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "v1":
		return "v1"
	default:
		return "v2"
	}
}

// ParseDuration parses a duration string (e.g. 5m, 30s). Returns 0 on error.
func ParseDuration(s string) time.Duration {
	d, _ := time.ParseDuration(strings.TrimSpace(s))
	return d
}
