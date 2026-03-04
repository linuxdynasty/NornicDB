# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Latest Changes]

- See `docs/latest-untagged.md` for the untagged `latest` image changelog.

## [1.0.13] - 2026-03-04

### Added

- **Cypher Graph-RAG procedures**: Added Cypher RAG primitives:
  - `CALL db.retrieve({...})`
  - `CALL db.rretrieve({...})`
  - `CALL db.rerank({...})` (candidate-based rerank API)
  - `CALL db.infer({...})`
  - `CALL db.index.vector.embed(...)`
- **Cypher cache policy extension**: Query planner/cache policy now supports the new primitives with explicit `db.infer({cache: true})` opt-in behavior.

### Changed

- **Search strategy stability**: Re-enabled auto strategy stitching/transitions with improved locking and safer coordination.
- **Core maintainability refactor**: Rebalanced architecture by extracting shared Cypher parsing/coercion and splitting oversized core files into cohesive modules while preserving behavior.
- **Syntax cleanup**: Minor syntax option cleanup to keep parser behavior aligned after refactors.

### Fixed

- **Async CREATE schema safety**: Excluded schema commands from async create-node batch handling to preserve expected execution semantics.
- **Schema regression protection**: Added regression coverage for schema commands with `AsyncEngine` to prevent drift.

### Technical Details

- **Statistics**: 7 commits.
- **Key areas touched**: `pkg/cypher/*`, `pkg/storage/*`, `pkg/nornicdb/*`, query cache policy.

## [1.0.12-hotfix] - 2026-02-28

### Added

- **Runtime search strategy transition framework**: Added safe brute-force ↔ HNSW transition plumbing with cutover safeguards and transition tests.

### Changed

- **Hotfix safety posture**: Disabled automatic strategy switching by default in hotfix flow to prioritize runtime stability.
- **Documentation refresh**: Updated README/docs for hotfix behavior and operational guidance.

### Fixed

- **Docker / llama.cpp crash**: Fixed header overwrite issue that could cause SIGSEGV in Docker builds.
- **Windows build reliability**: Fixed Windows build path issues for the hotfix release.

## [1.0.12-preview] - 2026-02-20

### Added

- **BM25 v2 Fulltext Engine**: Complete rewrite of the fulltext index (`FulltextIndexV2`) with compact uint32/uint16 postings (5–8× memory reduction), top-k score-bound pruning, per-query plan caching, batch index mutations, and O(log n) prefix expansion via sorted lexicon. Now the default engine; opt out with `NORNICDB_SEARCH_BM25_ENGINE=v1`.
- **BM25-Seeded HNSW Construction**: `LexicalSeedDocIDs` identifies the 2,048 most lexically discriminative documents (256 high-IDF terms × 8 top docs) and inserts them first, establishing a well-connected graph backbone before the remaining corpus. Reduces HNSW build time for 1M embeddings from ~27 min to ~10 min (2.7× speedup) with no change to recall or graph quality. Tunable via `NORNICDB_HNSW_LEXICAL_SEED_MAX_TERMS` and `NORNICDB_HNSW_LEXICAL_SEED_PER_TERM`. The same seed set initialises k-means centroids via `bm25+kmeans++` mode, reducing k-means convergence iterations by ~40%.
- **Hybrid Cluster Routing**: New `HybridClusterRouter` blends semantic and lexical cluster scores (`w_sem × semantic + w_lex × lexical`) for IVF cluster probe selection, ensuring queries route to the correct cluster even when the query embedding is between two equidistant centroids. Tunable via `NORNICDB_VECTOR_ROUTING_MODE=hybrid`, `_HYBRID_ROUTING_W_SEM`, `_HYBRID_ROUTING_W_LEX`.
- **Search Result Cache**: LRU cache (default 1,000 entries, 5-minute TTL) for search responses. Cache key incorporates query string, limit, result types, and reranker settings. Automatically invalidated on index mutations.
- **Stage-2 Reranking**: Two new reranking backends available after initial RRF retrieval:
  - `LocalReranker`: loads a GGUF cross-encoder model locally (e.g. `bge-reranker-v2-m3`) for accurate query-document relevance scoring.
  - `LLMReranker`: fail-open LLM-based reranker via Heimdall or any external provider; falls back to original RRF order on error.
  - Configured via `NORNICDB_SEARCH_RERANK_ENABLED`, `_PROVIDER`, `_MODEL`, `_TOP_N`.
- **Configurable Embedding Properties**: Control which node properties contribute to embedding text via `NORNICDB_EMBEDDING_PROPERTIES_INCLUDE`, `_EXCLUDE`, and `NORNICDB_EMBEDDING_INCLUDE_LABELS`. Allows domain-specific embedding tuning without code changes.
- **Search Readiness API**: `IsReady()` and `GetBuildProgress()` endpoints expose index build status and ETA so clients and the UI can show warm-up progress instead of partial results.
- **Per-Database Config Overrides**: New `/admin/databases/{db}/config` API allows setting embedding and search parameters (model, dimensions, include/exclude properties) per database without server restart.
- **Per-Database RBAC / Access Control**: New auth primitives — `Role`, `Entitlement`, `Privilege` — with a per-database allowlist stored in the system graph. `DatabaseAccessMode` interface and `RequestRBACContext` propagate identity through the request path. New Web UI page for database access management.
- **Heimdall Remote LLM Providers**: `GeneratorOpenAI` and `GeneratorOllama` bring OpenAI-compatible and Ollama APIs as first-class Heimdall backends, supplementing local GGUF. Configurable via `NORNICDB_HEIMDALL_PROVIDER`, `_OPENAI_API_KEY`, `_OLLAMA_BASE_URL`.
- **Heimdall Streaming Agentic Loop**: `GenerateWithTools` implements a native tool-call streaming loop with real-time SSE progress events. The loop autonomously executes MCP memory operations (store/recall/discover) and emits per-step notifications to the client.
- **Multi-DB Heimdall Router**: Heimdall can now access all configured databases, not just the default, enabling cross-database memory and retrieval operations.
- **Write Forwarding**: Follower nodes in replication deployments automatically forward write requests to the leader, removing the need for clients to route writes explicitly.
- **macOS File Browser View**: New file browser tab in the macOS menu bar app for browsing and tagging indexed files directly from the UI.
- **macOS Folder-Level Tag Inheritance**: Tags applied to a folder node propagate automatically as labels to all descendant file nodes.
- **BM25 Lexical Index Persistence** *(experimental, gated by `NORNICDB_PERSIST_SEARCH_INDEXES`)*: BM25 index is saved to disk alongside the HNSW and vector indexes to eliminate rebuild time on restart.
- **VectorFileStore** *(experimental)*: File-backed append-only vector storage keeps only an id→offset map in RAM, reducing in-process memory for large embedding corpora. Only the id→offset map (~80 bytes/vector) lives in RAM; all 4 KB vectors are paged from disk on demand.
- **HNSW & IVF-HNSW Index Persistence** *(experimental)*: Debounced, non-blocking background saves of both HNSW and IVF-HNSW indexes. Configurable via `NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC`.
- **ANTLR Parser: REDUCE and FOREACH**: ANTLR parser now supports `REDUCE()` accumulator expressions and `FOREACH` iteration clauses.
- **Orphan Detection Improvement**: Stale vector index entries for deleted nodes are identified and removed during index rebuild, preventing phantom candidates in search results.
- **`scripts/seed_and_search.py`**: End-to-end benchmark script for seeding a corpus and measuring search latency at scale.
- **`scripts/convert_search_index_to_msgpack`**: Utility for migrating existing BM25 index files to msgpack format.
- **`CONTRIBUTORS.md`**: New contributors file.

### Changed

- **Cypher: Transactional Semantics**: `BEGIN` / `COMMIT` now work as a proper multi-statement transaction boundary; queries inside a `BEGIN` block share state and commit atomically.
- **Cypher: SET Expression Evaluation**: `SET n.x = reduce(acc, x IN list | ...)`, list concatenation, and other runtime expressions are now evaluated inside `SET` rather than stored literally.
- **Cypher: SET Map Parameter Substitution**: `SET n += $props` correctly expands parameter maps at runtime.
- **Cypher: WHERE Relationship Patterns**: Complex `WHERE` clauses containing relationship patterns with inline properties are now evaluated correctly.
- **Cypher: OPTIONAL MATCH Compound Patterns**: Multi-hop `OPTIONAL MATCH` patterns now correctly propagate null bindings for unmatched paths.
- **Cypher: `toLower()` / `toUpper()` with Parameters**: Function calls where the argument is a parameter reference (e.g. `toLower($name)`) now resolve the parameter before applying the function.
- **Storage: EmbedMeta Field**: Embedding metadata (`_embeddings_stored_separately`, `_embedding_chunk_count`) moved from raw node properties to a dedicated `EmbedMeta` struct, avoiding property namespace pollution.
- **Storage: Bounded Embedding Transactions**: Large embedding chunks are written in separate, size-bounded transactions to prevent BadgerDB transaction size limit errors.
- **Storage: AsyncEngine Count Lock Order**: Fixed lock acquisition order in count queries to eliminate a latent deadlock under concurrent read/write load.
- **WAL: Snapshot Retention Policies**: Configurable retention period and maximum snapshot count prevent unbounded WAL growth.
- **WAL: Compact JSON Snapshots**: Snapshot encoding switched to compact JSON, reducing snapshot file size.
- **WAL: Buffered Atomic Writes**: `writeAtomicRecordV2Bufio` wraps WAL writes in a `bufio.Writer`, reducing syscall count and improving write throughput on spinning or NVMe storage.
- **WAL: Synchronous Writes for System Commands**: `CREATE DATABASE` and `DROP DATABASE` use synchronous WAL writes to guarantee durability of administrative operations.
- **Heimdall: Plugin Refactor**: Built-in actions extracted into external plugin packages, reducing Heimdall core size and enabling independent plugin versioning.
- **Heimdall: RAG Pipeline**: Query embedding is skipped when the query already has an embedding; exponential backoff added for embedding failures; context window handling improved.
- **macOS: Tag-Safe Labels**: File-indexer tagging updated to store tags as node labels only, with strict label validation across all SET/CREATE mutation paths.

### Fixed

- AsyncEngine lock-order deadlock in concurrent count queries.
- `SET n += $props` not substituting parameter map values.
- `WHERE (a)-[r:TYPE {prop: val}]->(b)` patterns incorrectly rejected or returning empty results.
- `toLower($param)` and `toUpper($param)` returning the literal parameter name.
- `OPTIONAL MATCH` with multi-hop patterns not propagating null bindings correctly.
- Pending embedding queue entries not cleaned up on node deletion, causing ghost queue entries.
- Mimir legacy loader removed — no longer attempts compatibility shim on startup.
- macOS menu bar: connection reliability, dark mode dimension control visibility, focus handling, folder-trash DB deletion.
- Windows build target for cross-compilation.

### Performance

- **Sub-10ms graph-RAG retrieval**: full round-trip (embed query → RRF hybrid search → hydrate → HTTP serialize) measured at 7–8 ms in production on million-node datasets.
- **BM25 indexing**: 3–5× throughput improvement from batch mutations; 5–8× memory reduction from compact postings.
- **HNSW construction**: 2.7× faster for 1M-embedding corpus with BM25-seeded insertion order; seeded build runs within 13% of the theoretical construction minimum.
- **Buffered WAL writes**: reduced syscall overhead on write-heavy workloads.

### Web UI (Bifrost)

- Database selector in the search panel — search targets the active database, not just the default.
- RRF score and per-source rank columns in search results for explainability.
- Search readiness ETA banner shown while indexes are warming up.
- Database access management page for configuring per-database RBAC roles.
- Dynamic role list in the user admin page reflects server-side role changes in real time.

### Technical Details

- **Statistics**: 69 commits, 332 files changed, +34,077 / −13,473 lines.
- **New packages / files**: `pkg/search/fulltext_index_v2.go`, `pkg/search/vector_file_store.go`, `pkg/search/hybrid_cluster_routing.go`, `pkg/search/local_rerank.go`, `pkg/search/llm_rerank.go`, `pkg/auth/allowlist.go`, `pkg/auth/database_access.go`, `pkg/auth/privileges.go`, `pkg/auth/roles.go`, `pkg/heimdall/generator_openai.go`, `pkg/heimdall/generator_ollama.go`, `pkg/config/dbconfig/`.
- **Removed**: `pkg/storage/mimir_loader.go` (legacy compatibility shim).

---

## [1.0.11] - 2026-01-27

### Added

- **Qdrant gRPC Compatibility Layer**: Full implementation of Qdrant gRPC API (v1.16.x) enabling existing Qdrant SDKs (Python, Go, Rust, JavaScript) to connect without modification
  - Collections service: Create, Get, List, Delete, Update, CollectionExists
  - Points service: Upsert, Get, Delete, Count, Search, Query, Scroll, payload operations, vector operations
  - Snapshots service: Create, List, Delete, CreateFull
  - Text queries via `Points.Query` with `VectorInput.Document` support
  - RBAC enforcement with per-RPC permissions configuration
  - Feature flag: `NORNICDB_QDRANT_GRPC_ENABLED=true` (disabled by default)
  - Files: `pkg/qdrantgrpc/*`, `pkg/server/server_qdrantgrpc.go`, `docs/user-guides/qdrant-grpc.md`

- **Canonical Graph Ledger**: Temporal DDL, transaction log queries, and receipts for audit-grade mutation tracking
  - Temporal constraints with validity windows (no-overlap enforcement)
  - Transaction log queries via `CALL db.txlog.*` procedures
  - Receipts with tx_id, wal_seq_start/end, and hash for auditability
  - WAL retention configuration for ledger-grade retention
  - Files: `pkg/cypher/call_temporal.go`, `pkg/cypher/call_txlog.go`, `pkg/storage/receipt.go`, `pkg/storage/temporal_constraint.go`, `docs/user-guides/canonical-graph-ledger.md`

- **Temporal Procedures**: New Cypher procedures for temporal data management
  - `CALL db.temporal.createConstraint()`, `CALL db.temporal.dropConstraint()`
  - `CALL db.temporal.listConstraints()`, `CALL db.temporal.validate()`
  - `CALL db.txlog.scan()`, `CALL db.txlog.getReceipt()`
  - Files: `pkg/cypher/call_temporal.go`, `pkg/cypher/temporal_procedures_test.go`

- **Vector Search Improvements**: Major refactoring and performance enhancements
  - HNSW configuration system with integration tests
  - Candidate generation strategies: GPU K-means, IVF-HNSW hybrid, vector ID collapse
  - Vector pipeline with query specification and strategy benchmarking
  - Vector registry for named vector management
  - ARM64 Metal acceleration for HNSW operations
  - Files: `pkg/search/hnsw_config.go`, `pkg/search/vector_pipeline.go`, `pkg/search/vector_query_spec.go`, `pkg/search/gpu_kmeans_candidate_gen.go`, `pkg/search/hnsw_metal.go`, `pkg/vectorspace/registry.go`

- **Storage Serialization**: New msgpack serializer option and serializer migration tools
  - Msgpack serializer for improved performance and smaller storage footprint
  - Serializer detection and migration utilities
  - Serialization benchmarks and comprehensive tests
  - Files: `pkg/storage/badger_serialization.go`, `pkg/storage/serializer_migration.go`, `pkg/storage/serialization_bench_test.go`, `docs/operations/storage-serialization.md`

- **Property Validation**: Type validation and constraint enforcement for node properties
  - Property type validation with comprehensive test coverage
  - Integration with schema persistence system
  - Files: `pkg/storage/property_validation.go`, `pkg/storage/property_validation_test.go`, `docs/user-guides/property-data-types.md`

- **Schema Persistence**: Persistent schema storage with constraint validation
  - Schema persistence across restarts
  - Constraint validation layer with namespace support
  - Files: `pkg/storage/schema_persistence.go`, `pkg/storage/badger_schema.go`, `pkg/storage/badger_constraint_validation.go`

- **Storage Recovery**: Enhanced recovery mechanisms for WAL corruption and data integrity
  - WAL repair utilities with corruption detection
  - WAL segments management with retention policies
  - Storage recovery tests and validation
  - Files: `pkg/storage/wal_repair.go`, `pkg/storage/wal_segments.go`, `pkg/nornicdb/storage_recovery.go`, `docs/operations/wal-compaction.md`

- **Auto-Embedding Inference**: Automatic embedding generation with intelligent chunking
  - Auto-embedding inference system with query chunking
  - Embedding invalidation and regeneration support
  - Files: `pkg/nornicdb/auto_embed_inference.go`, `pkg/cypher/query_embed_chunk.go`, `pkg/util/text_chunk.go`

- **ARM64 SIMD Optimizations**: NEON SIMD implementation for ARM64 platforms
  - Native NEON SIMD operations for vector math
  - C++ bridge for optimal performance
  - Files: `pkg/simd/neon_simd.go`, `pkg/simd/neon_simd_arm64.cpp`, `docs/architecture/neon-simd-implementation.md`

- **Query Autocomplete Plugin**: Intelligent query autocomplete for Cypher queries
  - Plugin system for query autocomplete
  - Integration with Heimdall AI assistant
  - Files: `pkg/heimdall/plugin.go` (enhanced), `docs/features/autocomplete-plugin.md`, `ui/src/components/browser/QueryAutocomplete.tsx`

- **Auth Caching**: Performance improvements for authentication
  - Auth cache with TTL and invalidation
  - Reduced authentication overhead for high-throughput scenarios
  - Files: `pkg/auth/auth_cache.go`, `pkg/auth/auth_cache_test.go`

- **Replication Enhancements**: TLS support and shared-secret authentication
  - TLS transport security for replication
  - Shared-secret authentication
  - Codec improvements and transport roundtrip tests
  - Replicated engine with comprehensive test coverage
  - Files: `pkg/replication/transport_security.go`, `pkg/replication/codec.go`, `pkg/replication/replicated_engine.go`, `pkg/replication/handlers.go`

- **NornicDB gRPC Search Service**: Native gRPC search service
  - Protobuf definitions and generated code
  - Search service implementation with tests
  - Files: `pkg/nornicgrpc/*`, `docs/architecture/embedding-search-architecture.md`

- **Performance Documentation**: Comprehensive performance guides
  - HTTP/2 implementation documentation
  - HTTP optimization options guide
  - Max concurrent streams comparison
  - Single request benchmark analysis
  - pprof quick guide
  - Files: `docs/performance/http2-implementation.md`, `docs/performance/http-optimization-options.md`, `docs/performance/maxconcurrentstreams-comparison.md`, `docs/performance/single-request-benchmark.md`, `docs/performance/pprof-quick-guide.md`

- **UI Improvements**: Refactored browser UI with new components
  - Query autocomplete component
  - Node details panel with editing capabilities
  - Search panel enhancements
  - Query results table improvements
  - Files: `ui/src/components/browser/*`, `ui/src/pages/Browser.tsx`

- **Neural Training**: Cypher query training infrastructure
  - Training scripts for Cypher query generation
  - Model export and conversion utilities
  - Files: `neural/train_nornicdb_cypher.py`, `neural/scripts/*`, `neural/NEXT_STEPS.md`

### Changed

- **Bolt Protocol**: Enhanced PackStream implementation with hash support and improved chunking
  - PackStream hash calculation for message integrity
  - Improved chunked message handling
  - Integer comparison fixes for ID handling
  - Files: `pkg/bolt/packstream.go`, `pkg/bolt/packstream_hash_test.go`, `docs/api-reference/bolt-protocol.md`

- **Cypher Executor**: Major improvements to query execution
  - Enhanced CREATE with return path and relationship support
  - Improved DELETE handling within queries
  - Long query resilience improvements
  - Identifier unquoting fixes
  - Files: `pkg/cypher/executor.go`, `pkg/cypher/create.go`, `pkg/cypher/delete_in_query_test.go`

- **Storage Engine**: Significant improvements to Badger storage
  - Fast path for nodes by label and ID
  - Enhanced transaction handling for edges
  - Improved async engine with callback deadlock prevention
  - Namespace prefix handling improvements
  - Label-to-nodeID lookup optimization
  - Files: `pkg/storage/badger*.go`, `pkg/storage/async_engine.go`, `pkg/storage/label_nodeid_lookup.go`

- **Search Service**: Major refactoring for performance and maintainability
  - Vector pipeline architecture
  - Query specification system
  - Strategy benchmarking framework
  - Files: `pkg/search/search.go`, `pkg/search/vector_pipeline.go`, `pkg/search/vector_query_spec.go`

- **WAL System**: Enhanced durability and operability
  - WAL segments with retention policies
  - Corruption detection and repair
  - Improved compaction and truncation
  - Files: `pkg/storage/wal.go`, `pkg/storage/wal_segments.go`, `pkg/storage/wal_repair.go`, `docs/operations/wal-compaction.md`

- **GPU Operations**: Improved CUDA and Metal support
  - CUDA sync pointer checks
  - Metal build fixes
  - GPU K-means candidate generation
  - Files: `pkg/gpu/cuda/cuda_bridge.go`, `pkg/gpu/gpu.go`, `pkg/search/hnsw_metal.go`

- **Replication**: Enhanced multi-region and HA standby support
  - Improved storage adapter with benchmarks
  - Enhanced transport layer
  - Standby startup improvements
  - Files: `pkg/replication/storage_adapter.go`, `pkg/replication/ha_standby.go`, `pkg/replication/multi_region.go`

- **Configuration**: Extended configuration options
  - Qdrant gRPC configuration
  - Temporal constraint configuration
  - WAL retention configuration
  - Files: `pkg/config/config.go`, `nornicdb.example.yaml`

- **Heimdall**: Enhanced AI assistant capabilities
  - Improved plugin system
  - Metrics and discovery chunk testing
  - Scheduler improvements
  - Files: `pkg/heimdall/*`

### Fixed

- **Edge Index Tracking**: Fixed edge index tracking on CREATE operations
- **Auto-TLP Trigger**: Fixed Auto-TLP trigger after async embeddings persist
- **Orphaned Vectors**: Prevented orphaned vectors on node reindex/delete
- **Badger Transactions**: Fixed transaction handling for edges in executor
- **WAL Corruption Detection**: Improved corruption detection on startup
- **SET with Map Variables**: Added support for `SET n += mapVar` syntax
- **Create Return ID**: Fixed ID return in CREATE RETURN statements
- **Integer Comparison**: Fixed integer comparison to ID in Bolt protocol
- **Auth Cache**: Fixed auth cache for all users
- **Create Relationship**: Fixed issue #14 with create relationship
- **IN id() Evaluation**: Fixed IN id() evaluation in queries
- **Deletion Mechanism**: Fixed deletion mechanism edge cases
- **Server Shutdown**: Fixed server shutdown and deadlock issues
- **Replication Connections**: Fixed replication connection settings
- **Path Return on Create**: Fixed path return on CREATE operations
- **Flaky Tests**: Fixed flaky test issues
- **Default Account Lockout**: Prevented default account lockout
- **Memory Allocations**: Reduced memory allocations by reusing and caching services per DB

### Performance

- **Write Performance**: Enhanced write performance for Badger with optimizations
- **Drop Database**: Improved drop database performance using Badger prefix drop
- **Memory Allocations**: Reduced allocations by reusing and caching services per database
- **Fast Path Queries**: Added fast path for nodes by label and ID
- **HTTP/2**: HTTP/2 implementation with max concurrent streams optimization
- **Vector Search**: Major performance improvements through pipeline refactoring
- **GPU Acceleration**: ARM64 Metal acceleration for HNSW operations
- **SIMD Optimizations**: ARM64 NEON SIMD for vector operations

### Documentation

- **Qdrant gRPC Guide**: Comprehensive guide for Qdrant gRPC compatibility (`docs/user-guides/qdrant-grpc.md`)
- **Canonical Graph Ledger**: Complete guide for canonical graph implementation (`docs/user-guides/canonical-graph-ledger.md`)
- **Property Data Types**: Complete reference for all supported property types (`docs/user-guides/property-data-types.md`)
- **Embedding Search Architecture**: Detailed architecture documentation (`docs/architecture/embedding-search-architecture.md`)
- **HTTP/2 Implementation**: Performance guide for HTTP/2 (`docs/performance/http2-implementation.md`)
- **Storage Serialization**: Guide for storage serialization options (`docs/operations/storage-serialization.md`)
- **WAL Compaction**: WAL compaction and retention guide (`docs/operations/wal-compaction.md`)
- **Performance Guides**: Multiple performance analysis documents

### Technical Details

- **Statistics**: 369 files changed, 48,599 insertions(+), 4,857 deletions(-)
- **New Packages**: `pkg/qdrantgrpc`, `pkg/nornicgrpc`, `pkg/vectorspace`, `pkg/util` (hash, text_chunk)
- **Major Refactors**: Search service, storage serialization, WAL system, replication transport
- **Test Coverage**: Extensive new test files across all major features
- **Benchmarks**: New benchmark tests for vector search, storage operations, and replication

### Notable Files Changed

- Added: `pkg/qdrantgrpc/*`, `pkg/nornicgrpc/*`, `pkg/vectorspace/*`, `pkg/cypher/call_temporal.go`, `pkg/cypher/call_txlog.go`, `pkg/storage/receipt.go`, `pkg/storage/temporal_constraint.go`, `pkg/storage/schema_persistence.go`, `pkg/storage/wal_repair.go`, `pkg/storage/wal_segments.go`, `pkg/search/vector_pipeline.go`, `pkg/search/hnsw_config.go`, `pkg/simd/neon_simd*.go`, `docs/user-guides/qdrant-grpc.md`, `docs/user-guides/canonical-graph-ledger.md`, `docs/user-guides/property-data-types.md`
- Modified: `pkg/cypher/*`, `pkg/storage/*`, `pkg/search/*`, `pkg/replication/*`, `pkg/auth/*`, `pkg/bolt/*`, `pkg/server/*`, `pkg/nornicdb/*`, `ui/*`

## [1.0.10] - 2025-12-21

### Added

- **Vulkan GPU backend**: Added a native Vulkan GPU backend (pure-Go bindings) with compute shaders and tests. Files: `pkg/gpu/vulkan/*` (README, BUILD, bridge, shaders, tests).
- **Multi-database / Composite DB support**: Added composite/multi-database support and enforcement layer enabling multi-tenant routing and aliases. Files: `pkg/multidb/*`, `pkg/server/multi_database_*.go`.
- **GraphQL improvements**: Expanded GraphQL models, resolvers, subscriptions and tests. Files: `pkg/graphql/*` (models, resolvers, schema).
- **OAuth provider & Swagger UI CLI**: New convenience commands for running an OAuth provider and a bundled Swagger UI. Files: `cmd/oauth-provider/*`, `cmd/swagger-ui/*`.
- **Composite Storage Engine**: Added `pkg/storage/composite_*` for routing and dedup across multiple storage backends.
- **Server basepath and multi-db tests**: Added server basepath handling and multi-database E2E tests. Files: `pkg/server/basepath.go`, `pkg/server/multi_database_*.go`.

### Changed

- **Build scripts**: Windows build scripts updated and hardened (`build.ps1`, `build.bat`), improved help text and added `vulkan` native variant. Safer quoting and ASCII-only help to avoid PowerShell parse issues.
- **Cypher & executor**: Continued cypher fixes and performance work — many parser, aggregation and transaction improvements across `pkg/cypher/*`.
- **Embedding & search**: Embeddings improvements (local GGUF support) and search namespace-awareness changes (`pkg/embed/*`, `pkg/search/*`).
- **UI updates**: Swagger + admin UI changes and Vite/build adjustments.

### Fixed

- **PowerShell build parsing**: Fixed PowerShell parsing issues in `build.ps1` (unbalanced quoting and non-ASCII banner artifacts) so native builds parse and run reliably.
- **Various query parsing and multi-db routing bugs**: Multiple fixes for query parsing, id() support, WITH/YIELD/RETURN interactions, and multi-database routing.

### Tests

- Extensive new tests and test harnesses for multi-db, composite storage, Vulkan bridge, GraphQL resolvers and many cypher scenarios. New and updated tests under `pkg/*` and `cmd/*`.

### Docs

- Large documentation additions: API OpenAPI, multi-database guides, GraphQL guide, operations/CLI docs, and architecture notes. Several new files in `docs/` and `docs/architecture`.

### Notable files changed or added

- Added: `pkg/gpu/vulkan/*`, `pkg/multidb/*`, `cmd/oauth-provider/*`, `cmd/swagger-ui/*`, `docs/api-reference/openapi.yaml`, `docs/development/MULTI_DB_E2E_TEST.md`, `scripts/run_multi_db_e2e_test.sh`.
- Modified: `build.ps1`, `build.bat`, `Makefile`, `pkg/cypher/*`, `pkg/storage/*`, `pkg/graphql/*`, `pkg/server/server.go`, `pkg/nornicdb/*`, `ui/*`, many docs.

### Technical details

- Vulkan backend includes compute shader (`topk.comp`) and bridge code for platform integration. Local GGUF embedding support and embedding queue improvements were merged.
- Composite storage provides namespaced routing and deduplication for multi-DB setups (`pkg/storage/namespaced.go`).
- Build and CI: Windows native build target for `vulkan` added; PowerShell and batch scripts hardened to avoid parse failures.

### Credits

- Contributors: commits between tags include work by `orneryd` and `TJ Sweet` (see commit log for full details).

### Added

- **Configurable Async Write Settings**: New configuration options for async write-behind cache
  - `NORNICDB_ASYNC_WRITES_ENABLED` (default: true) - Enable/disable async writes
  - `NORNICDB_ASYNC_FLUSH_INTERVAL` (default: 50ms) - Control flush frequency
  - `NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE` (default: 50000) - Limit memory usage for node cache
  - `NORNICDB_ASYNC_MAX_EDGE_CACHE_SIZE` (default: 100000) - Limit memory usage for edge cache
- **Configurable Minimum Similarity**: Added `SetDefaultMinSimilarity()` and `GetDefaultMinSimilarity()` methods to search service
  - Apple Intelligence compatibility with lower similarity thresholds
  - Per-search override via `SearchOptions.MinSimilarity`
- **Enhanced Async Engine**: Improved cache configuration with size limits and memory management
- **Comprehensive Testing**: Added `async_engine_cache_config_test.go` with 362 lines of test coverage

### Changed

- **Search Service**: Refactored similarity resolution with configurable defaults and explicit fallback
- **Async Engine**: Enhanced with memory limits and configurable cache sizes to prevent unbounded growth
- **Configuration**: Extended with async write settings for better performance tuning

### Performance

- **Memory Management**: Prevents unbounded memory growth during bulk operations via cache size limits
- **Write Throughput**: Configurable async flush intervals for balancing consistency vs throughput

### Technical Details

- New methods: `Service.SetDefaultMinSimilarity()`, `Service.GetDefaultMinSimilarity()`
- Enhanced `AsyncEngine` with configurable cache bounds
- Extended `Config` struct with async write settings
- 172 lines of new configuration tests
- Improved WAL test reliability (increased sleep timing)

## [1.0.9] - 2025-12-16 - GraphQL API & Neo4j Compatibility Improvements

Features:

- Add full GraphQL API with gqlgen (schema, resolvers, introspection)
- GraphQL endpoints: /graphql, /graphql/playground
- Complete CRUD mutations (createNode, updateNode, deleteNode, merge, bulk ops)
- GraphQL queries: search, similar, cypher, stats, schema, labels
- Admin mutations: triggerEmbedding, rebuildSearchIndex, runDecay, clearAll

Fixes:

- Fix Cypher map projection returns without explicit AS aliases
- Fix aggregation query alias handling in RETURN clauses
- Fix embed queue pending embeddings index tracking
- Improve Neo4j driver compatibility in UI Browser responses

Tests:

- Add GraphQL resolver and handler tests
- Add storage pending embeddings index tests
- Add node needs embedding tests
- Add Cypher return alias tests

Docs:

- Add GraphQL README with usage examples
- Add QUERIES.md with sample GraphQL operations

## [1.0.8] - 2025-12-15

### Added

- **ANTLR Cypher Parser Integration**: Added optional ANTLR-based OpenCypher parser as alternative to the fast inline Nornic parser
  - Switch via `NORNICDB_PARSER=antlr` environment variable (default: `nornic`)
  - Full OpenCypher grammar support with detailed error messages (line/column info)
  - Programmatic switching via `config.SetParserType()` and `config.WithANTLRParser()`
  - New make targets: `make antlr-test`, `make antlr-generate`, `make test-parsers`
- **SIMD Vector Math Package** (`pkg/simd`): Internal SIMD-accelerated vector operations for faster embedding similarity
  - ARM64 NEON support via `vek` package
  - AMD64 AVX2/SSE support
  - Metal GPU acceleration on macOS (Apple Silicon)
  - Integrated into vector similarity pipeline for cosine/euclidean/dot-product operations
- **WAL Hardening**: Added trailer canary and 8-byte alignment for improved durability
  - New tests and documentation in `docs/operations/durability.md`
- **Search Optimizations Documentation**: Comprehensive guide at `docs/performance/searching.md`
- **Cypher Parser Modes Documentation**: Architecture doc at `docs/architecture/cypher-parser-modes.md`

### Changed

- **SIMD Backend**: Switched from inline assembly to `vek` package for cross-platform SIMD support
- **Makefile**: Added auto-detection for GPU backends and Vulkan build instructions
- Tests updated to be parser-agnostic (work with both Nornic and ANTLR parsers)

### Performance

- ANTLR parser benchmarks (Northwind database):
  - Nornic: 3,000-4,200 ops/sec (recommended for production)
  - ANTLR: 0.8-2,100 ops/sec (50-5000x slower, use for development/debugging)
- SIMD vector operations: Up to 4x faster similarity calculations on supported hardware

### Technical Details

- New packages: `pkg/simd`, `pkg/cypher/antlr`
- New config: `pkg/config/feature_flags.go` for parser type management
- ANTLR grammar files: `pkg/cypher/antlr/CypherLexer.g4`, `pkg/cypher/antlr/CypherParser.g4`
- 27,000+ lines of ANTLR-generated parser code
- Metal shader implementation for GPU-accelerated SIMD on macOS

## [1.0.7] - 2025-12-14

### Added

- Documentation: added `DIY.md` with Vulkan/Makefile build instructions and targets.

### Changed

- Makefile and docs: added and documented AMD64 Vulkan build targets and related instructions.

### Fixed

- Neo4j compatibility: return `*storage.Node` and `*storage.Edge` directly from Cypher query results with correct Bolt protocol serialization and property access handling. (commit bc38eb3)
- Neo4j compatibility: improved integer handling in Cypher/driver interoperability. (commit ef8f07e)

### Technical Details

- Commits included: `bc38eb3` (node/edge return serialization), `ef8f07e` (integer compatibility), `f24825a`/`436ce36` (docs/Makefile Vulkan targets and `DIY.md`).

## [1.0.6] - 2025-12-12

### Added

- Timer-based K-means clustering scheduler: runs immediately on startup and then periodically (configurable interval).
- New configuration: `NORNICDB_KMEANS_CLUSTER_INTERVAL` (duration) to control the clustering interval. Default: `15m`.

### Changed

- Switched K-means clustering from an embed-queue trigger to a timer-based scheduler that skips runs when the embedding count has not changed since the last clustering.
- DB lifecycle now starts the clustering ticker on open and stops it cleanly on close.

### Fixed

- Prevent excessive K-means executions (previously fired after each embedding) that caused UI slowness and frequent re-clustering.
- Cypher `YIELD`/`RETURN` compatibility: allow `RETURN` after `YIELD` for property access (for example `CALL ... YIELD node RETURN node.id`) and ensure `ORDER BY`, `SKIP`, and `LIMIT` are correctly parsed and applied even when `RETURN` is absent.
- Improved `YIELD` parsing and `applyYieldFilter` semantics (whitespace normalization, `YIELD *` support, return projection correctness).

### Tests

- Added `pkg/cypher/yield_return_test.go` with comprehensive tests for `YIELD`/`RETURN` combinations, `ORDER BY`, `SKIP`, `LIMIT`, and `YIELD *` behaviour.
- Updated `pkg/cypher/neo4j_compat_test.go` for Neo4j compatibility cases.

### Technical Details

- Key files modified: `pkg/nornicdb/db.go`, `pkg/config/config.go`, `pkg/cypher/call.go`, `pkg/cypher/yield_return_test.go`.
- The scheduler uses `searchService.EmbeddingCount()` and a `lastClusteredEmbedCount` guard to avoid unnecessary clustering runs.

### Test Coverage

- All package tests pass; example test output excerpts:

## [1.0.5] - 2025-12-10

### Fixed

- **Critical: Node/Edge Count Returns 0 After Delete+Recreate Cycles** - `MATCH (n) RETURN count(n)` returned 0 even when nodes existed in the database

  - Atomic counters (`nodeCount`, `edgeCount`) in `BadgerEngine` became out of sync during delete+recreate cycles
  - Root cause: Nodes created via implicit transactions (MERGE, CREATE) bypass `CreateNode()` and use `UpdateNode()`
  - `UpdateNode()` checks if key exists to determine `wasInsert=true/false`, only incrementing counter when `wasInsert=true`
  - During delete+recreate, keys could still exist in BadgerDB from previous sessions, causing `wasInsert=false` for genuinely new nodes
  - The counter would increment for only 1 node, leaving `nodeCount=1` even with 234 nodes in the database
  - **Production symptom**: After deleting all nodes and re-importing 234 nodes via MERGE, `/metrics` showed `nornicdb_nodes_total 0` while `MATCH (n:Label)` returned all 234 nodes correctly
  - **Solution**: Changed `BadgerEngine.NodeCount()` and `EdgeCount()` to scan actual keys with prefix iteration instead of trusting atomic counter
  - Key-only iteration (no value loading) provides fast O(n) counting with guaranteed correctness
  - Atomic counter is updated after each scan to keep it synchronized for future calls
  - **Impact**: Count queries now always reflect reality. Embeddings worked because they scan actual data; node counts failed because they used broken counter.

- **Critical: Aggregation Query Caching Caused Stale Counts** - `MATCH (n) RETURN count(n)` was being cached, returning stale results after node creation/deletion
  - `SmartQueryCache` was caching aggregation queries (COUNT, SUM, AVG, etc.) which must always be fresh
  - Modified `pkg/cypher/executor.go` to detect aggregation via `HasAggregation` flag and skip caching entirely
  - Modified `pkg/cypher/query_info.go` to analyze queries and set `HasAggregation=true` for COUNT/SUM/AVG/MIN/MAX/COLLECT
  - Modified `pkg/cypher/cache.go` to invalidate queries with no labels when any node is created/deleted (affects `MATCH (n)` count queries)
  - **Impact**: Count queries always return fresh data, no manual cache clear needed

### Changed

- **Performance Trade-off: NodeCount() and EdgeCount() Now O(n)** - Changed from O(1) atomic counter to O(n) key scan for correctness
  - Key-only iteration is very fast (no value decoding, just prefix scan)
  - BadgerDB iterators are highly optimized for this access pattern
  - Correctness > speed for core count operations
  - Future optimization: Maintain accurate counter through all write paths

### Technical Details

- Modified `pkg/storage/badger.go`:
  - `NodeCount()` now uses `BadgerDB.View()` with key-only iterator (`PrefetchValues=false`)
  - `EdgeCount()` uses same pattern for edge prefix scan
  - Both methods sync atomic counter after scan to reduce overhead on subsequent calls
- Modified `pkg/storage/async_engine.go`:
  - Fixed `NodeCount()` calculation to include `inFlightCreates` to prevent race condition during flush
  - Prevented double-counting during flush window when nodes transition from cache to engine
- Modified `pkg/cypher/executor.go`:
  - Added aggregation detection to skip caching for COUNT/SUM/AVG/MIN/MAX queries
- Modified `pkg/cypher/query_info.go`:
  - Added `HasAggregation` field to `QueryInfo` struct
  - Updated `analyzeQuery()` to detect aggregation functions
- Modified `pkg/cypher/cache.go`:
  - Fixed `InvalidateLabels()` to also invalidate queries with no labels (e.g., `MATCH (n)`)

### Test Coverage

- All existing tests pass with updated expectations
- Modified `pkg/storage/realtime_count_test.go` to account for transient over-counting before flush
- Modified `pkg/cypher/executor_cache_test.go` to use non-aggregation queries (since aggregations aren't cached)
- Added comprehensive logging and debugging to trace count calculation flow
- Production issue validated as fixed: 234 nodes now counted correctly after delete+reimport

## [1.0.4] - 2025-12-10

### Fixed

- **Critical: Node/Edge Count Tracking During DETACH DELETE** - Edge counts became incorrect (negative, double-counted, or stale) during `DETACH DELETE` operations

  - `deleteEdgesWithPrefix()` was deleting edges but not returning count of edges actually deleted
  - `deleteNodeInTxn()` wasn't tracking edges deleted along with the node
  - `BulkDeleteNodes()` only decremented node count, not edge count for cascade-deleted edges
  - Unit tests showed counts going negative or remaining high after deletes, resetting to zero only on restart
  - Fixed by updating `deleteEdgesWithPrefix()` signature to return `(int64, []EdgeID, error)`
  - Fixed `deleteNodeInTxn()` to aggregate and return edges deleted with node
  - Fixed `BulkDeleteNodes()` to correctly decrement `edgeCount` and notify `edgeDeleted` callbacks
  - Added comprehensive tests in `pkg/storage/async_engine_delete_stats_test.go`
  - **Impact**: `/admin/stats` and Cypher `count()` queries now remain accurate during bulk delete operations

- **Critical: ORDER BY Ignored for Relationship Patterns** - `ORDER BY`, `SKIP`, and `LIMIT` clauses were completely ignored for queries with relationship patterns

  - Queries like `MATCH (p:Person)-[:WORKS_IN]->(a:Area) RETURN p.name ORDER BY p.name` returned unordered results
  - `executeMatchWithRelationships()` was returning immediately without applying post-processing clauses
  - Fixed by capturing result, applying ORDER BY/SKIP/LIMIT, then returning
  - Affects all queries with relationship traversal: `(a)-[:TYPE]->(b)`, `(a)<-[:TYPE]-(b)`, chained patterns
  - **Impact**: Fixes data integrity issues where clients relied on sorted results

- **Critical: Cartesian Product MATCH Returns Zero Rows** - Comma-separated node patterns returned empty results instead of cartesian product

  - `MATCH (p:Person), (a:Area) RETURN p.name, a.code` returned 0 rows (should return N×M combinations)
  - `executeMatch()` only parsed first pattern, ignoring subsequent comma-separated patterns
  - Fixed by detecting multiple patterns via `splitNodePatterns()` and routing to new `executeCartesianProductMatch()`
  - Now correctly generates all combinations of matched nodes
  - Supports WHERE filtering, aggregation, ORDER BY, SKIP, LIMIT on cartesian results
  - **Impact**: Critical for Northwind-style bulk insert patterns like `MATCH (s), (c) CREATE (p)-[:REL]->(c)`

- **Critical: Cartesian Product CREATE Only Creates One Relationship** - `MATCH` with multiple patterns followed by `CREATE` only created relationships for first match

  - `MATCH (p:Person), (a:Area) CREATE (p)-[:WORKS_IN]->(a)` created 1 relationship (should create 3 for 3 persons × 1 area)
  - `executeMatchCreateBlock()` was collecting only first matching node per pattern variable
  - Fixed by collecting ALL matching nodes and iterating through cartesian product combinations
  - Each CREATE now executes once per combination in the cartesian product
  - **Impact**: Fixes bulk relationship creation patterns used in data import workflows

- **UNWIND CREATE with RETURN Returns Variable Name Instead of Values** - Return clause after `UNWIND...CREATE` returned literal variable names
  - `UNWIND ['A','B','C'] AS name CREATE (n {name: name}) RETURN n.name` returned `["name","name","name"]` (should be `["A","B","C"]`)
  - `replaceVariableInQuery()` failed to handle variables inside curly braces like `{name: name}`
  - String splitting on spaces left `name}` which didn't match variable `name`
  - Fixed by properly trimming braces `{}[]()` and preserving surrounding punctuation during replacement
  - **Impact**: Fixes all UNWIND+CREATE+RETURN workflows, critical for bulk data ingestion with result tracking

### Changed

- **Cartesian Product Performance** - New `executeCartesianProductMatch()` efficiently handles multi-pattern queries
  - Builds combinations incrementally to avoid memory explosion on large datasets
  - Supports early filtering with WHERE clause before building full product
  - Properly integrates with query optimizer (ORDER BY, SKIP, LIMIT applied after filtering)

### Technical Details

- Modified `pkg/storage/badger.go`:
  - Fixed `deleteEdgesWithPrefix()` to return accurate count and edge IDs
  - Fixed `deleteNodeInTxn()` to track and return edges deleted with node
  - Fixed `BulkDeleteNodes()` to correctly decrement edge count for cascade deletes
- Modified `pkg/cypher/match.go`:
  - Added `executeCartesianProductMatch()` for comma-separated pattern handling
  - Added `executeCartesianAggregation()` for aggregation over cartesian results
  - Added `evaluateWhereForContext()` for WHERE clause evaluation on node contexts
  - Fixed `executeMatch()` to detect and route multiple patterns correctly
  - Fixed relationship pattern path to apply ORDER BY/SKIP/LIMIT before returning
- Modified `pkg/cypher/create.go`:
  - Updated `executeMatchCreateBlock()` to collect all pattern matches (not just first)
  - Added cartesian product iteration for CREATE execution
  - Now creates relationships for every combination in MATCH cartesian product
- Modified `pkg/cypher/clauses.go`:
  - Fixed `replaceVariableInQuery()` to handle variables in property maps `{key: value}`
  - Improved punctuation preservation during variable substitution

### Test Coverage

- All existing tests pass (100% backwards compatibility)
- Added `pkg/storage/async_engine_delete_stats_test.go` with comprehensive count tracking tests
- Fixed `TestWorksInRelationshipTypeAlternation` - ORDER BY now works correctly
- Fixed `TestUnwindWithCreate/UNWIND_CREATE_with_RETURN` - Returns actual values, not variable names
- Cartesian product patterns now pass all Northwind benchmark compatibility tests

## [1.0.3] - 2025-12-09

### Fixed

- **Critical: Double .gguf Extension Bug** - Model names with `.gguf` extension (e.g., `bge-m3.gguf`) were having `.gguf` appended again, resulting in `bge-m3.gguf.gguf` and "model not found" errors
  - Fixed in `pkg/heimdall/scheduler.go` - Now checks `strings.HasSuffix()` before adding extension
  - Fixed in `pkg/embed/local_gguf.go` - Same fix for embedding model resolution
  - This prevented both Heimdall AI assistant and auto-embeddings from working on macOS
- **Missing LaunchAgent Environment Variables** - macOS menu bar app's LaunchAgent plist was missing critical env vars
  - Added `NORNICDB_MODELS_DIR=/usr/local/var/nornicdb/models`
  - Added `NORNICDB_HEIMDALL_MODEL` to pass model name to Heimdall
  - Added `NORNICDB_EMBEDDING_MODEL` to pass model name to embeddings
  - Updated both plist generators in `macos/MenuBarApp/NornicDBMenuBar.swift`
- **macOS Models Path Resolution** - Added `/usr/local/var/nornicdb/models` as first candidate in Heimdall's model path resolution (was only checking Docker paths)
- **Swift YAML Config Indentation** - Fixed multi-line string indentation errors in plist generation

### Changed

- **Non-blocking Regenerate Embeddings** - `POST /nornicdb/embed/trigger?regenerate=true` now returns `202 Accepted` immediately
  - Clearing and regeneration happens asynchronously in background goroutine
  - Prevents UI from blocking for minutes during large regenerations
  - Added detailed logging for background operations
- **UI Confirmation Dialog** - Added confirmation modal before regenerating all embeddings
  - Shows warning about destructive operation
  - Displays current embedding count
  - Red warning styling to indicate danger

### Added

- **Swift YAML Parser Unit Test** - Created `macos/MenuBarApp/ConfigParserTest.swift` to verify config loading works correctly
  - Tests section extraction, boolean parsing, string parsing
  - Validates against actual `~/.nornicdb/config.yaml` file

## [1.0.2] - 2025-01-27

### Added

- **macOS Code Intelligence / File Indexer**: New file indexing system in the macOS menu bar app that provides semantic code search capabilities.
  - Automatically indexes source files with intelligent chunking (functions, classes, methods extracted separately)
  - **Apple Vision Integration**: PNG/image files are processed with Apple's Vision framework for:
    - OCR text extraction (reads text from screenshots, diagrams, etc.)
    - Image classification (identifies objects, scenes, activities in images)
  - Creates searchable `File` and `FileChunk` nodes linked via `HAS_CHUNK` relationships
  - Real-time file watching with automatic re-indexing on changes
  - Supports code files, markdown, images, and more
- **NornicDB Icons**: Added proper application icons for macOS app

### Changed

- **Keychain-based API Token Storage**: API tokens (Ollama, OpenAI, etc.) are now stored securely in macOS Keychain instead of plain YAML config files
- Improved default provider value handling

### Fixed

- **In-flight Node Deletion Race Condition**: Fixed a critical bug in `AsyncEngine` where nodes being flushed to disk could survive `DETACH DELETE` operations.
  - When a node was in the middle of being written (in `inFlightNodes`), delete operations would only remove it from cache
  - The flush would then complete, writing the "deleted" node back to BadgerDB
  - Now properly marks in-flight nodes for deletion so they're removed after flush completes
- **Node/Edge Count Consistency**: `NodeCount()` and `EdgeCount()` now validate that nodes can be decoded before counting, ensuring counts match what `AllNodes()` and `AllEdges()` actually return
- CUDA Dockerfile fixes for improved GPU support
- Documentation link fixes

## [1.0.1] - 2025-12-08

### Added

- macOS installer improvements: wizard-first startup, menu bar start/health wait, security tab, auto-generated JWT/encryption secrets, scrollable wizard, starting status indicator.
- Menu bar app: ensures `~/.nornicdb/config.yaml` path, shows restart progress, auto-generates secrets if empty, saves auth/encryption correctly.
- Docker ARM64 (Metal) image now builds and copies Heimdall plugin and sets `NORNICDB_HEIMDALL_PLUGINS_DIR`.
- Legacy env compatibility for Neo4j env vars (auth, transaction timeout, data dir, default db, read-only, bolt/http ports).

### Changed

- Encryption: full-database Badger encryption, salt stored at `db.salt`, rejects missing password, clearer errors on wrong password; stats report AES-256 (BadgerDB).
- Auth/JWT: server uses configured JWT secret (no hardcoded dev secret); cookie SameSite=Lax, 7d Max-Age.
- Config defaults: password `password`, embedding provider `local`; strict durability forces WAL sync immediate/interval 0.
- Tests updated and all passing (`go test ./...`).

### Fixed

- Prevent server autostart before wizard (plist created/loaded only after wizard save/start).
- Heimdall env override test; flexible boolean parsing for read-only; duration parsing for legacy env names.

## [1.0.0] - 2024-12-06

### Changed

- **BREAKING**: Repository split from `github.com/orneryd/Mimir/nornicdb` to `github.com/orneryd/NornicDB`
- **BREAKING**: Module path changed from `github.com/orneryd/mimir/nornicdb` to `github.com/orneryd/nornicdb`
- Preserved full commit history from Mimir repository
- Updated all documentation to reflect standalone repository
- Cleaned up repository structure (removed Mimir-specific files)

### Migration

See [MIGRATION.md](MIGRATION.md) for detailed migration instructions.

---

## Historical Changes (from Mimir Project)

The following changes occurred while NornicDB was part of the Mimir project. Full commit history has been preserved in this repository.

### Features Implemented (Pre-Split)

- Neo4j Bolt protocol compatibility
- Cypher query language support (MATCH, CREATE, MERGE, DELETE, WHERE, WITH, RETURN, etc.)
- BadgerDB storage backend
- In-memory storage engine for testing
- GPU-accelerated embeddings (Metal, CUDA)
- Vector search with semantic similarity
- Full-text search
- Query result caching
- Connection pooling
- Heimdall LLM integration
- Web UI (Bifrost)
- Docker images for multiple platforms
- Comprehensive test suite (90%+ coverage)
- Extensive documentation

### Performance Achievements (Pre-Split)

- 3-52x faster than Neo4j across benchmarks
- 100-500 MB memory footprint vs 1-4 GB for Neo4j
- Sub-second cold start vs 10-30s for Neo4j
- GPU-accelerated embedding generation

### Bug Fixes (Pre-Split)

- Fixed WHERE IS NOT NULL with aggregation
- Fixed relationship direction in MATCH patterns
- Fixed MERGE with ON CREATE/ON MATCH
- Fixed concurrent access issues
- Fixed memory leaks in query execution
- Fixed Bolt protocol edge cases

---

## Version History

### Release Tags

- `v1.0.0` - First standalone release (December 6, 2024)
- `v1.0.6` - 2025-12-12

### Pre-Split Versions

Prior to v1.0.0, NornicDB was versioned as part of the Mimir project. The commit history includes all previous development work.

---

## Migration Notes

### For Users Migrating from Mimir

If you were using NornicDB from the Mimir repository, please see [MIGRATION.md](MIGRATION.md) for detailed instructions on:

- Updating import paths
- Updating git remotes
- Updating Docker images
- Updating CI/CD pipelines

### Compatibility

- **Neo4j Compatibility**: Maintained 100%
- **API Stability**: No breaking changes to public APIs (except import paths)
- **Docker Images**: Same naming convention, new build source
- **Data Format**: Fully compatible with existing data

---

## Contributing

See [CONTRIBUTING.md](docs/CONTRIBUTING.md) and [AGENTS.md](AGENTS.md) for contribution guidelines.

---

[1.0.13]: https://github.com/orneryd/NornicDB/compare/v1.0.12-hotfix...v1.0.13
[1.0.12-hotfix]: https://github.com/orneryd/NornicDB/compare/v1.0.12...v1.0.12-hotfix
[1.0.12]: https://github.com/orneryd/NornicDB/compare/v1.0.12-preview...v1.0.12
[1.0.12-preview]: https://github.com/orneryd/NornicDB/compare/v1.0.11...v1.0.12-preview
[1.0.11]: https://github.com/orneryd/NornicDB/compare/v1.0.10...v1.0.11
[1.0.10]: https://github.com/orneryd/NornicDB/compare/v1.0.9...v1.0.10
[1.0.9]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.9
[1.0.6]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.6
[1.0.1]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.1
[1.0.0]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.0
