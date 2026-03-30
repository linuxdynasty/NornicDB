# NornicDB Search Implementation

## Overview

The search package (`pkg/search/`) implements a **hybrid search system** combining:

1. **Vector Search** - Cosine similarity on 1024-dimensional embeddings
2. **BM25 Full-Text Search** - Keyword search with TF-IDF scoring
3. **RRF (Reciprocal Rank Fusion)** - Industry-standard algorithm to merge rankings

By default, **both sides see all node properties**:

- **Embedding text / vector search**: the embedding worker builds text from **all node properties plus node labels** by default, excluding built-in metadata fields like `embedding`, `has_embedding`, timestamps, and similar internal keys. This is configurable through embedding include/exclude settings if you want to restrict which properties contribute to embeddings.
- **BM25 full-text search**: BM25 also indexes **all node properties**. A small set of common text fields such as `content`, `text`, `title`, and `name` are added first for better ranking, but searchability is not limited to those fields.

This is the same approach used by:

- Azure AI Search
- Elasticsearch
- Weaviate
- Google Cloud Search

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Search Service                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  Vector Index   │  │  Fulltext Index │  │   Storage   │ │
│  │  (Cosine Sim)   │  │    (BM25)       │  │   Engine    │ │
│  └────────┬────────┘  └────────┬────────┘  └──────┬──────┘ │
│           │                    │                   │        │
│           └────────────┬───────┘                   │        │
│                        │                           │        │
│              ┌─────────▼─────────┐                 │        │
│              │   RRF Fusion      │─────────────────┘        │
│              │ score = Σ(w/(k+r))│                          │
│              └─────────┬─────────┘                          │
│                        │                                    │
│              ┌─────────▼─────────┐                          │
│              │   Ranked Results  │                          │
│              └───────────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

## Full-Text Search Properties

BM25 indexes **all node properties**, but these properties are treated as **priority fields** for ranking and are added first (matching Mimir's Neo4j `node_search` index):

| Property       | Description                |
| -------------- | -------------------------- |
| `content`      | Main content field         |
| `text`         | Text content (file chunks) |
| `title`        | Node titles                |
| `name`         | Node names                 |
| `description`  | Node descriptions          |
| `path`         | File paths                 |
| `workerRole`   | Agent worker roles         |
| `requirements` | Task requirements          |

After those priority fields, **all remaining properties are also indexed**. Property names are included alongside values, so searches can match both general text and field-oriented content.

Example: a search for `docker configuration` can match a node through `content`, `title`, or any other indexed property that contains those terms.

## Embedding Text Inputs

The vector side of hybrid search depends on whatever text was embedded for the node.

By default, managed embeddings are generated from:

- node labels
- all node properties
- excluding built-in metadata/internal fields

That means hybrid search is not limited to a single `content` field unless you configure it that way.

If you set embedding include/exclude options, the vector side follows those rules, while the BM25 side still indexes all properties.

## RRF Algorithm

**Formula**: `RRF_score(doc) = Σ (weight_i / (k + rank_i))`

Where:

- `k` = constant (default: 60)
- `rank_i` = rank of document in result set i (1-indexed)
- `weight_i` = importance weight for result set i

### Adaptive Weights

The system automatically adjusts weights based on query characteristics:

| Query Type | Words | Vector Weight | BM25 Weight | Rationale              |
| ---------- | ----- | ------------- | ----------- | ---------------------- |
| Short      | 1-2   | 0.5           | 1.5         | Exact keyword matching |
| Medium     | 3-5   | 1.0           | 1.0         | Balanced               |
| Long       | 6+    | 1.5           | 0.5         | Semantic understanding |

## Usage

```go
import "github.com/orneryd/nornicdb/pkg/search"

// Create search service
engine := storage.NewMemoryEngine()
svc := search.NewService(engine)

// Build indexes from storage
ctx := context.Background()
svc.BuildIndexes(ctx)

// Hybrid RRF search
opts := search.DefaultSearchOptions()
opts.Limit = 10
opts.MinSimilarity = 0.5

response, err := svc.Search(ctx, "authentication security", embedding, opts)

// Results include RRF metadata
for _, r := range response.Results {
    fmt.Printf("%s: rrf=%.4f vectorRank=%d bm25Rank=%d\n",
        r.ID, r.RRFScore, r.VectorRank, r.BM25Rank)
}
```

### Search Options

```go
type SearchOptions struct {
    Limit         int       // Max results (default: 50)
    MinSimilarity float64   // Vector threshold (default: 0.5)
    Types         []string  // Filter by node labels

    // RRF configuration
    RRFK         float64   // RRF constant (default: 60)
    VectorWeight float64   // Vector weight (default: 1.0)
    BM25Weight   float64   // BM25 weight (default: 1.0)
    MinRRFScore  float64   // Min RRF score (default: 0.01)
}
```

## Fallback Chain

The search automatically falls back when needed:

1. **RRF Hybrid** (if embedding provided)
2. **Vector Only** (if BM25 returns no results)
3. **Full-Text Only** (if no embedding or vector search fails)

## Caching

Search results are cached by the unified search service so that **repeated identical requests** (same query and options) return immediately from memory. The cache is shared by all entry points (HTTP `/nornicdb/search`, Cypher vector procedures, MCP, etc.).

- **Key:** Query text + options (limit, types/labels, rerank, MMR settings). Same inputs ⇒ cache hit.
- **Size:** Up to 1000 entries (LRU); entries expire after 5 minutes (TTL).
- **Invalidation:** The cache is cleared whenever the index changes (`IndexNode` or `RemoveNode`), so results stay correct after updates.

Use the same query and options for repeated calls to benefit from the cache (e.g. same search box query and limit).

## Performance (Apple M3 Max)

| Operation     | Scale          | Time   |
| ------------- | -------------- | ------ |
| Vector Search | 10K vectors    | ~8.5ms |
| BM25 Search   | 10K documents  | ~255µs |
| RRF Fusion    | 100 candidates | ~27µs  |
| Index Build   | 38K nodes      | ~5.4s  |

## Test Coverage

```bash
# Run all search tests
go test -v ./pkg/search/...

# Run with real Neo4j data
go test -v ./pkg/search/... -run RealData

# Run benchmarks
go test -bench=. ./pkg/search/...
```

## Files

- `search.go` - Main service with RRF fusion
- `vector_index.go` - Cosine similarity search
- `fulltext_index.go` - BM25 inverted index
- `search_test.go` - Comprehensive tests

## Future Improvements

1. **HNSW Index** - O(log n) vector search vs current O(n)
2. **GPU Acceleration** - Metal/CUDA for vector operations
3. **Stemming** - Better text normalization
4. **Field Boosting** - Weight title matches higher
5. **Phrase Search** - Exact phrase matching
