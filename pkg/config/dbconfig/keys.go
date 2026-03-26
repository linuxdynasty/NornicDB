// Package dbconfig: allowed per-DB config keys for validation and UI.

package dbconfig

// KeyMeta describes one allowed per-database config key.
type KeyMeta struct {
	Key      string `json:"key"`
	Type     string `json:"type"`     // "string", "number", "boolean", "duration"
	Category string `json:"category"` // "Embeddings", "Search", "HNSW", etc.
}

// AllowedKeys returns the list of allowed per-DB config keys and their metadata.
// Used by API validation and by GET /admin/databases/config/keys.
func AllowedKeys() []KeyMeta {
	return []KeyMeta{
		// Embeddings
		{"NORNICDB_EMBEDDING_ENABLED", "boolean", "Embeddings"},
		{"NORNICDB_EMBEDDING_PROVIDER", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_MODEL", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_API_URL", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_API_KEY", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_DIMENSIONS", "number", "Embeddings"},
		{"NORNICDB_EMBEDDING_CACHE_SIZE", "number", "Embeddings"},
		{"NORNICDB_EMBEDDING_PROPERTIES_INCLUDE", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE", "string", "Embeddings"},
		{"NORNICDB_EMBEDDING_INCLUDE_LABELS", "boolean", "Embeddings"},
		{"NORNICDB_EMBEDDING_GPU_LAYERS", "number", "Embeddings"},
		{"NORNICDB_EMBEDDING_WARMUP_INTERVAL", "duration", "Embeddings"},
		// Search
		{"NORNICDB_SEARCH_MIN_SIMILARITY", "number", "Search"},
		{"NORNICDB_SEARCH_BM25_ENGINE", "string", "Search"},
		{"NORNICDB_SEARCH_RERANK_ENABLED", "boolean", "Search"},
		{"NORNICDB_SEARCH_RERANK_PROVIDER", "string", "Search"},
		{"NORNICDB_SEARCH_RERANK_MODEL", "string", "Search"},
		{"NORNICDB_SEARCH_RERANK_API_URL", "string", "Search"},
		{"NORNICDB_SEARCH_RERANK_API_KEY", "string", "Search"},
		{"NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC", "number", "Search"},
		// HNSW
		{"NORNICDB_VECTOR_ANN_QUALITY", "string", "HNSW"},
		{"NORNICDB_VECTOR_HNSW_M", "number", "HNSW"},
		{"NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION", "number", "HNSW"},
		{"NORNICDB_VECTOR_HNSW_EF_SEARCH", "number", "HNSW"},
		{"NORNICDB_VECTOR_HNSW_METAL_MIN_CANDIDATES", "number", "HNSW"},
		// IVF-HNSW
		{"NORNICDB_VECTOR_IVF_HNSW_ENABLED", "boolean", "IVF-HNSW"},
		{"NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", "number", "IVF-HNSW"},
		{"NORNICDB_VECTOR_IVF_HNSW_MAX_CLUSTERS", "number", "IVF-HNSW"},
		// Vector GPU
		{"NORNICDB_VECTOR_GPU_BRUTE_MIN_N", "number", "Vector"},
		{"NORNICDB_VECTOR_GPU_BRUTE_MAX_N", "number", "Vector"},
		// K-means
		{"NORNICDB_KMEANS_CLUSTERING_ENABLED", "boolean", "K-means"},
		{"NORNICDB_KMEANS_MIN_EMBEDDINGS", "number", "K-means"},
		{"NORNICDB_KMEANS_CLUSTER_INTERVAL", "duration", "K-means"},
		{"NORNICDB_KMEANS_NUM_CLUSTERS", "number", "K-means"},
		{"NORNICDB_KMEANS_MAX_ITERATIONS", "number", "K-means"},
		// Auto-links
		{"NORNICDB_AUTO_LINKS_ENABLED", "boolean", "Auto-links"},
		{"NORNICDB_AUTO_LINKS_THRESHOLD", "number", "Auto-links"},
		// Auto-TLP
		{"NORNICDB_AUTO_TLP_ENABLED", "boolean", "Auto-TLP"},
		{"NORNICDB_AUTO_TLP_LLM_QC_ENABLED", "boolean", "Auto-TLP"},
		{"NORNICDB_AUTO_TLP_LLM_AUGMENT_ENABLED", "boolean", "Auto-TLP"},
		// Embed worker
		{"NORNICDB_EMBED_WORKER_NUM_WORKERS", "number", "Embed worker"},
		{"NORNICDB_EMBED_SCAN_INTERVAL", "duration", "Embed worker"},
		{"NORNICDB_EMBED_BATCH_DELAY", "duration", "Embed worker"},
		{"NORNICDB_EMBED_TRIGGER_DEBOUNCE", "duration", "Embed worker"},
		{"NORNICDB_EMBED_MAX_RETRIES", "number", "Embed worker"},
		{"NORNICDB_EMBED_CHUNK_SIZE", "number", "Embed worker"},
		{"NORNICDB_EMBED_CHUNK_OVERLAP", "number", "Embed worker"},
		{"NORNICDB_MVCC_LIFECYCLE_INTERVAL", "duration", "MVCC lifecycle"},
	}
}

// AllowedKeysSet returns a set of allowed key names for validation.
func AllowedKeysSet() map[string]KeyMeta {
	set := make(map[string]KeyMeta)
	for _, m := range AllowedKeys() {
		set[m.Key] = m
	}
	return set
}

// KeysExcludedFromPerDB are not allowed as per-DB overrides (reserved for future use).
var KeysExcludedFromPerDB = map[string]bool{}

// IsAllowedKey returns true if the key can be set as a per-DB override.
func IsAllowedKey(key string) bool {
	if KeysExcludedFromPerDB[key] {
		return false
	}
	_, ok := AllowedKeysSet()[key]
	return ok
}
