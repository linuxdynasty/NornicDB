---
name: hybrid-search-3phase
overview: Deliver a 3-phase roadmap to improve hybrid retrieval quality, ranking stability, and ANN scale economics in NornicDB with measurable ops/sec, latency, recall, and memory gains, without feature-flag gating for universally positive changes.
todos:
  - id: phase1-hybrid-order
    content: Refactor hybrid execution to filter->BM25->vector and add candidate-domain plumbing through search + vector pipeline.
    status: pending
  - id: phase1-adaptive-switch
    content: Implement adaptive exact-vs-ANN strategy selection based on candidate set size, dims, and live latency counters.
    status: pending
  - id: phase1-metrics-bench
    content: Expand telemetry and benchmark suites to report recall@k, p50/p95, ops/sec, allocations, and memory.
    status: pending
  - id: phase2-global-bm25
    content: Implement global BM25 stats snapshots with atomic query-time snapshot binding and persistence metadata.
    status: pending
  - id: phase2-rank-stability
    content: Add deterministic rank-stability regression tests across reload/compaction/rebuild paths.
    status: pending
  - id: phase3-compressed-ann
    content: Introduce compressed ANN profile(s) integrated with IVF-HNSW routing and explicit profile config semantics.
    status: pending
  - id: phase3-public-bench
    content: Publish reproducible large-scale performance/memory/recall benchmark reports for all ANN profiles.
    status: pending
---

# Hybrid Retrieval 3-Phase Execution Plan

## Goal and Guardrails

- Implement all three phases in production paths (no feature-flag gating for changes that are strictly quality/perf improvements).
- Keep Neo4j-compatible semantics where applicable (especially BM25 and type/ranking behavior).
- Require per-phase benchmark and regression evidence before merge.

## Current Baseline (What we are changing)

- Current RRF hybrid flow runs vector search first, BM25 second, then type filtering and RRF fusion in [pkg/search/search.go](../../pkg/search/search.go).
- Vector strategy already has static N-based switching and candidate generators in [pkg/search/vector_pipeline.go](../../pkg/search/vector_pipeline.go) and [pkg/search/search.go](../../pkg/search/search.go).
- BM25 v2 is local-index scoped with mutable IDF recomputation in [pkg/search/fulltext_index_v2.go](../../pkg/search/fulltext_index_v2.go).
- HNSW quality profiles exist today (`fast|balanced|accurate`) in [pkg/search/hnsw_config.go](../../pkg/search/hnsw_config.go).

## Phase 1: Fastest ROI (Execution Order, Adaptive Switch, Telemetry)

### 1) Hybrid planner ordering: filter -> keyword -> vector

- Refactor `rrfHybridSearch` in [pkg/search/search.go](../../pkg/search/search.go) to execute:
- type/metadata filter preselection (cheap filter set)
- BM25 candidate generation on filtered domain
- vector search restricted/reranked against candidate pool (or cluster subset)
- final RRF fusion
- Add a candidate-domain abstraction (e.g., `AllowedIDs


Great read. Here’s a distilled extraction of the article and a direct NornicDB application map.

Source: [How ByteDance Solved Billion-Scale Vector Search Problem with Apache Doris 4.0](https://www.velodb.io/blog/bytedance-solved-billion-scale-vector-search-problem-with-apache-doris-4-0)

## Key points extracted + NornicDB application

- **Problem 1: Pure vector search misses exact-intent constraints**
  - **Article point:** Semantic similarity alone confuses exact values (city, numbers, legal identifiers).
  - **Apply to NornicDB:** Make hybrid retrieval the default for enterprise queries:
    - structured filters first (`WHERE` exact predicates)
    - lexical/BM25 next (must-match terms)
    - vector similarity last
  - **Concrete NornicDB change:** Planner rule that reorders candidate reduction in this sequence before vector scoring.

- **Problem 2: Ranking instability from segment-local stats**
  - **Article point:** BM25 computed per segment causes ranking drift after merges/maintenance.
  - **Apply to NornicDB:** Compute and cache **global corpus stats** (DF/N) per index/database namespace, not local shard/segment stats.
  - **Concrete change:** Add a stats layer for full-text indexes with refresh policy + versioning so same query yields stable rankings over time.

- **Problem 3: HNSW memory cost explodes at billion scale**
  - **Article point:** Graph ANN index can be memory-heavy; IVFPQ drastically cuts footprint with acceptable recall tradeoff.
  - **Apply to NornicDB:** Add optional compressed ANN backend (IVF/PQ-style) for very large datasets.
  - **Concrete change:** Introduce pluggable vector index profiles:
    - `hnsw_high_recall`
    - `ivf_pq_balanced`
    - `disk_optimized`
    and expose selection in index config.

- **Technique: Progressive filtering**
  - **Article point:** Cheapest ops first shrink candidate set before expensive vector compute.
  - **Apply to NornicDB:** Push down filters and keyword constraints aggressively in Cypher execution.
  - **Concrete change:** Add optimizer pass for filter pushdown and candidate-size-aware execution path.

- **Technique: Global BM25 scoring**
  - **Article point:** Stable relevance from global term stats.
  - **Apply to NornicDB:** Add `bm25_mode = global|local` with global as default for production.
  - **Concrete change:** Build background-maintained global DF tables and include stats snapshot ID in query profile for explainability.

- **Technique: Compression tradeoff (accuracy vs memory)**
  - **Article point:** Slight recall drop can be worth 20x memory win.
  - **Apply to NornicDB:** Make recall/latency/memory tradeoffs explicit in config and docs.
  - **Concrete change:** Benchmark harness that reports `recall@k`, `p95 latency`, memory footprint per index type.

- **Optimization: Brute force beats index on small candidate sets**
  - **Article point:** After heavy filtering, sequential exact distance is faster than index traversal.
  - **Apply to NornicDB:** Add adaptive switch:
    - if candidate count < threshold, run exact vector scan
    - else ANN search
  - **Concrete change:** dynamic threshold tuning by dimension, hardware profile, and observed p95 latency.

- **Operational lesson: Trust/stability matters as much as raw speed**
  - **Article point:** Ranking churn kills user trust.
  - **Apply to NornicDB:** Add rank-stability regression tests and “query replay consistency” CI checks.
  - **Concrete change:** test suite asserting rank variance bounds across compaction/merge cycles.

## Recommended NornicDB rollout (practical order)

- **Phase 1 (fastest ROI):**
  - Hybrid planner ordering (filter → keyword → vector)
  - Candidate-size adaptive exact/ANN switch
  - Bench + telemetry (`recall@k`, p50/p95, memory)

- **Phase 2 (quality/trust):**
  - Global BM25 stats and stable scoring snapshots
  - Rank-stability regression tests

- **Phase 3 (scale economics):**
  - Compressed ANN mode (IVF/PQ-like) with explicit profile configs
  - Large-scale memory/recall benchmark publication

### Phase 3 Detail: What This Looks Like in Practice

Goal: keep usable recall while cutting RAM enough to run 10M-1B vectors on materially smaller instances.

- **Core architecture:**
  - Keep a small coarse routing structure in memory (IVF-style centroid lists).
  - Store compressed vector codes (PQ-style sub-quantized codes) instead of full float32 vectors for ANN candidate search.
  - Re-rank top candidates using original vectors when needed (exact or higher-precision pass).
  - Preserve current high-recall HNSW path as a profile option; do not force one index mode for every workload.

- **Proposed profile set (explicit and opinionated):**
  - Activation switch: `NORNICDB_VECTOR_ANN_QUALITY=compressed`
    - `fast|balanced|accurate` keep current HNSW/IVF-HNSW behavior.
    - `compressed` enables the Phase 3 compressed ANN path.
  - `hnsw_high_recall`
    - Best recall, highest memory cost.
    - Use for latency-critical and precision-critical applications.
  - `ivf_pq_balanced`
    - Default for large datasets.
    - Large memory reduction with modest recall tradeoff.
  - `ivf_pq_memory_saver`
    - Aggressive compression for constrained hardware.
    - Lower memory and cost, higher recall/latency tradeoff.
  - `disk_optimized`
    - Minimal resident memory, higher tail latency.
    - For very large corpora with infrequent query bursts.

- **Representative target envelope (to validate, not promise):**
  - `hnsw_high_recall`: memory baseline 1.0x, recall@10 near-max.
  - `ivf_pq_balanced`: ~3x-8x lower memory, recall@10 drop <= 2-5 points.
  - `ivf_pq_memory_saver`: ~8x-20x lower memory, recall@10 drop <= 5-12 points.
  - `disk_optimized`: RAM floor mode, recall depends on rerank budget.

- **Example config shape (conceptual):**
  - Existing ANN/HNSW/IVF knobs remain first-class and are directly reused where semantically compatible.
  - New compressed-only knobs are introduced only where there is no equivalent existing control.
  - Environment example:
    - `NORNICDB_VECTOR_ANN_QUALITY=compressed`
    - `NORNICDB_KMEANS_NUM_CLUSTERS=4096` (reused for IVF list defaults/routing)
    - `NORNICDB_KMEANS_SEED_MAX_TERMS=256` (reused for seeded training candidate pool)
    - `NORNICDB_KMEANS_SEED_DOCS_PER_TERM=1` (reused for seeded training candidate cap)
    - `NORNICDB_VECTOR_PQ_SEGMENTS=16` (new, compressed-specific)
    - `NORNICDB_VECTOR_PQ_BITS=8` (new, compressed-specific)
    - `NORNICDB_VECTOR_IVFPQ_NPROBE=16` (new, compressed-specific)
    - `NORNICDB_VECTOR_IVFPQ_RERANK_TOPK=200` (new, compressed-specific)

- **Execution flow for lower hardware cost:**
  - Apply metadata/type filters first.
  - Route into small IVF candidate partitions.
  - Score compressed codes (fast and memory-light).
  - Re-rank only a bounded candidate set with full vectors.
  - If filtered candidate pool is small, skip ANN and do exact scan directly.

- **Config contract (important):**
  - Existing HNSW/IVF controls are not treated as legacy fallback controls.
  - In `compressed` mode, shared knobs are consumed directly by Phase 3.
  - Compressed-specific flags only cover new capabilities (PQ/code/rerank/probe).
  - This keeps one coherent configuration model across ANN modes.

- **Acceptance criteria before defaulting to compressed mode:**
  - No major recall regression on reference workloads:
    - `recall@10` and `recall@50` within agreed SLO by workload class.
  - Significant memory savings:
    - At least 3x reduction on balanced profile in large-corpus tests.
  - Stable latency envelope:
    - p95 and p99 within target for interactive workloads.
  - Deterministic reproducibility:
    - Published benchmark harness, dataset characteristics, and config manifests.

- **Why this helps "less hardware":**
  - ANN memory footprint is the primary scaling limiter at high vector counts.
  - Compression reduces bytes/vector so more corpus fits in RAM.
  - Better fit in memory reduces node count and instance size requirements.
  - Bounded rerank keeps quality predictable without restoring full-memory cost.

If you want, I can turn this into a concrete NornicDB engineering RFC with package-level changes (`pkg/search`, `pkg/cypher` planner, config knobs, test matrix, and benchmark acceptance criteria).

### Phase 3 Implementation Status (Current)

- Compressed ANN (`NORNICDB_VECTOR_ANN_QUALITY=compressed`) is implemented end-to-end in the search pipeline (profile resolution, build/query runtime, persistence, warmup/load-or-rebuild, and strategy fingerprint compatibility).
- Benchmark matrix (`BenchmarkANNQueryPipelineChunked`) is in place and exercised across full/half/quarter/eighth corpus slices.
- Latest `count=3` query snapshot (Apple M3 Max, `benchtime=2s`):
  - `full_n=12000`: HNSW ~`5611 ns/op`, IVFPQ ~`66767 ns/op`
  - `half_n=6000`: HNSW ~`5608 ns/op`, IVFPQ ~`69922 ns/op`
  - `quarter_n=3000`: HNSW ~`5647 ns/op`, IVFPQ ~`49235 ns/op`
  - `eighth_n=1500`: HNSW ~`5673 ns/op`, IVFPQ ~`34773 ns/op`
- Relative to early Phase 3 baseline at `full_n=12000`, compressed IVFPQ query latency improved from ~`127826 ns/op` to ~`66767 ns/op` (~`1.91x` faster) while maintaining bounded query memory pressure (~`5.23-5.29 MiB` heap delta in current runs).