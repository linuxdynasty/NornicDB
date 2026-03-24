# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Latest Changes]

- See `docs/latest-untagged.md` for the untagged `latest` image changelog.

## [v1.0.29] - 2026-03-23

### Added

- **Optimistic mutation metadata for async CREATE paths**:
  - added Cypher-side optimistic metadata tracking for created node/relationship IDs
  - async CREATE fast-path now records created IDs up-front for client response usage.

### Fixed

- **Cypher mutation and grouped-query compatibility for multi-entity update flows**:
  - fixed chained `MATCH ... WHERE ... MATCH ... SET ... RETURN` handling for update queries that join source/target entity sets
  - fixed multi-MATCH WHERE extraction to use the correct terminal WHERE before RETURN, preventing false `expected multiple MATCH clauses` errors
  - fixed `SET ... RETURN count(...)` aggregation semantics so update-count projections return deterministic values (`count(t)` now behaves correctly in mutation returns)
  - fixed chained MATCH normalization so queries containing `OPTIONAL MATCH` are not rewritten into incompatible required-MATCH forms
  - fixed joined-row aggregation handling for `COLLECT(...)` with non-aggregate return columns, preserving grouped-per-key result semantics
  - improved MATCH WHERE extraction boundaries in mixed clause pipelines so optional-tail clauses do not leak into WHERE parsing.
- **Correlated CALL/UNION correctness and performance**:
  - restored correct UNION subquery result behavior for correlated execution paths
  - reduced fixed overhead in correlated query routing/execution to improve hot-path latency.
- **Correlated CALL subquery write semantics (`WITH ... CREATE/MERGE/...`)**:
  - fixed correlated `CALL { WITH ... }` execution so imported node variables bind correctly across `CREATE`/`MERGE` write branches
  - fixed write-tail fallback rewriting to avoid dropping side effects for valid `MATCH ... CALL { WITH p MERGE ... }` shapes
  - fixed `WITH`-import boundary parsing so write clauses after `WITH` are preserved and executed.
- **Bolt parity for text-based vector queries**:
  - fixed Bolt database-scoped executor wiring so `db.index.vector.queryNodes(..., $text)` works when an embedder is configured
  - aligned Bolt behavior with HTTP/GraphQL execution paths for string query input.
- **Async transaction response metadata shape**:
  - surfaced optimistic metadata in transaction responses alongside receipt metadata when available.
- **Mutation stats and deduplication correctness**:
  - fixed DELETE/DETACH DELETE mutation stats under repeated OPTIONAL MATCH row expansion by deduplicating per-entity deletes
  - fixed branch regression where some SET/DELETE/CALL-IN-TRANSACTIONS paths returned nil projection values instead of expected results.
- **Indexed OR-IN lookup path for key-list reads**:
  - added index-backed planning for predicates shaped like `propA IN $keys OR propB IN $keys` across alternate key fields
  - avoids full label scans for large key-list lookups and cleanup/read query patterns.

### Tests

- Added/updated regression and benchmark coverage for correlated UNION/call-subquery behavior and real-data execution profiles.
- Added regression coverage for:
  - Bolt DB-scoped executor embedder inheritance and string vector query execution
  - async CREATE fast-path optimistic ID metadata propagation.
  - correlated subquery create-or-update shape (`OPTIONAL MATCH + CALL { WITH ... UNION ... }`)
  - CALL subquery write-path regression cases (`WITH ... CREATE`, `WITH ... MERGE`)
  - delete deduplication under OPTIONAL MATCH row multiplication
  - parser/import handling for `WITH ... WHERE ...` correlated subquery clauses
  - exact `UNWIND + OPTIONAL MATCH + collect(CASE...)` key-lookup shape (per-key grouped rows and null-arm behavior)
  - `DETACH DELETE` with `WHERE elementId(...)` + `OPTIONAL MATCH` cleanup shape
  - OR-combined indexed `IN` predicate planning without scan fallback
  - exact mutation query shapes:
    - `MATCH ... WHERE ... OR (...) CREATE ... CREATE ... RETURN ...`
    - `MATCH ... WHERE ... OR (...) MATCH ... SET ... RETURN ...`
    - `MATCH ... WHERE elementId(...) SET ... RETURN count(...) AS updated`
  - fan-out and null-arm guard tests for OR-based creation filters.

### Technical Details

- **Range covered**: `v1.0.28..HEAD`
- **Commits in range**: 1 (non-merge)
- **Files changed in range**: 8
- **Primary focus areas**: correlated UNION subquery correctness and hot-path performance.
- **Additional staged delta (not including changelog edits)**: 9 files, +208 / -11
- **Additional staged delta (not including changelog edits)**: 17 files, +952 / -56

## [v1.0.28] - 2026-03-23

### Added

- **Vector query embedding cache for Cypher procedures**:
  - added executor-level embedding-result caching for `db.index.vector.queryNodes` / compatibility vector query paths when the query input is text
  - added in-flight de-duplication for concurrent identical embed requests so the same query text is embedded once and shared across waiters.
  - what this means: repeated semantic/vector query calls spend less time in embedding and create fewer duplicate embedding workloads under concurrent traffic.

### Changed

- **Correlated subquery execution optimizations**:
  - restored safe UNION fast paths in correlated `MATCH ... CALL { ... UNION ... } ...` execution with strict guards for write-safety and variable-dependency correctness
  - improved correlated seed extraction and batched lookup handling in subquery execution hot paths.
  - what this means: lower fixed-cost overhead for common correlated subquery/UNION shapes while preserving Neo4j-compatible semantics.
- **Query cache key normalization performance**:
  - replaced allocation-heavy whitespace normalization (`strings.Fields` join) with a single-pass compaction strategy.
  - what this means: fewer cache-key allocations and reduced GC pressure on read-heavy workloads.
- **Traversal optimization safety hardening**:
  - strengthened fallback start-node pruning behavior to fail open and preserve deterministic traversal semantics for chained/complex patterns.
  - what this means: traversal optimizations remain active without sacrificing correctness on multi-segment graph patterns.

### Fixed

- **UNION/subquery fixed-cost overhead in hot Cypher paths**:
  - reduced allocation-heavy row dedupe and subquery processing overhead for CALL/UNION shapes.
- **Correlated CALL + UNION semantics in mixed query shapes**:
  - fixed guarded fast-path routing to keep duplicate-row and chained-traversal result behavior consistent while optimized execution is enabled.

### Tests

- Added/expanded benchmark and regression coverage for:
  - correlated subquery + UNION fixed-cost cache-miss profiling
  - real-data Cypher/fabric-style e2e benchmark harnesses
  - traversal optional/WHERE pruning safety behavior
  - vector procedure caching behavior and compatibility paths.

### Technical Details

- **Range covered**: `v1.0.27..HEAD`
- **Commits in range**: 1 (non-merge)
- **Repository delta**: 13 files changed, +1,754 / -86 lines
- **Non-test surface changed**: 8 files
- **Primary focus areas**: Cypher hot-path latency, allocation/GC reduction, correlated subquery/UNION execution, and vector query embed caching.

## [v1.0.27] - 2026-03-22

### Added

- **Indexed temporal `AS OF` lookups and current-version tracking**:
  - added storage-backed temporal indexes keyed by namespace, label, temporal key, and validity window so point-in-time lookups no longer depend on full label scans
  - added current-pointer tracking for open/current temporal intervals and wired rebuild/prune maintenance through DB admin flows.
  - what this means: temporal queries scale with ordered index lookups instead of broad scans, and restore/startup flows can rebuild temporal state deterministically.
- **MVCC historical reads and retention controls**:
  - added committed node/edge version records, persisted MVCC head metadata, snapshot-visible reads, and wrapper delegation for namespaced, WAL, and async engines
  - added retention policy controls, pruning, retained-floor anchoring, and historical-read maintenance APIs.
  - what this means: NornicDB now supports explicit historical graph reads with predictable retention behavior instead of only current-state inspection.
- **Closure-based transaction helper API**:
  - added `DB.Begin`, `DB.Update`, and `DB.View` wrappers for closure-scoped transaction execution
  - exported `Transaction` as the public closure-facing transaction type.
  - what this means: callers can use transaction-scoped closures without manually juggling rollback/commit boilerplate.

### Changed

- **Storage transaction isolation model**:
  - transactions now anchor reads to a begin-time MVCC snapshot and keep point reads, label scans, and graph visibility checks pinned to that snapshot
  - commit-time validation now checks node, edge, endpoint, and adjacency races against the transaction snapshot.
  - what this means: storage transactions now provide standard Snapshot Isolation semantics rather than best-effort read-your-writes only behavior.
- **Cypher and MCP write-path stability**:
  - compound `MATCH ... CREATE` execution now reuses the standard single-clause MATCH binding path for safe query shapes, while preserving special post-filter handling for `NOT (a)-[:TYPE]->(b)` modifiers
  - MCP relationship/task mutations now retry bounded snapshot-conflict failures instead of surfacing transient storage conflicts directly.
  - what this means: common ID-targeted relationship creation queries are more reliable under load without regressing migration-style anti-relationship filters.
- **Temporal/search interaction**:
  - search indexing and rebuild flows now treat historical temporal versions as non-searchable and keep indexes current-only by default
  - temporal overlap validation now uses indexed predecessor/successor checks where supported.
  - what this means: historical state no longer pollutes current search results, and temporal writes avoid increasingly expensive validation scans.
- **Operations and configuration surface**:
  - added MVCC retention knobs to config, environment variables, and sample YAML
  - macOS installer defaults and first-run wizard presets now keep search reranking disabled unless the user explicitly chooses the advanced AI setup
  - clarified async-write consistency wording and documented retained-floor/MRS behavior.
  - what this means: operators have explicit controls and clearer expectations for history depth, pruning, and eventual-consistency modes.

### Fixed

- **Historical lookup performance cliffs**:
  - fixed sparse post-prune historical lookups by persisting a retained-floor anchor in MVCC head metadata.
- **Conflict normalization and retryability**:
  - fixed lower-level Badger conflict leakage by normalizing commit conflicts to `ErrConflict` with clearer concurrent-modification messages.
- **Compound MATCH...CREATE query-shape regressions**:
  - fixed comma-separated `MATCH (a), (b) WHERE elementId(...) ... CREATE ...` relationship creation so compound CREATE blocks reuse correct MATCH bindings
  - fixed the related regression where migration-style `AND NOT (o)-[:TRANSLATES_TO]->(t)` filters were bypassed by the single-clause fast path.
- **Graph-consistent concurrent delete behavior**:
  - fixed transaction validation so node deletes and adjacent edge changes cannot commit into a dangling-edge state across concurrent snapshots.
- **MVCC endpoint validation fallback behavior**:
  - fixed transaction edge creation/commit validation to accept readable endpoint nodes even when MVCC head metadata is temporarily missing, instead of incorrectly rejecting valid edges as dangling
  - fixed the associated transaction read fallback so missing-head recovery uses the transaction's anchored Badger snapshot rather than the live engine state, preserving Snapshot Isolation.
- **Startup/restore maintenance reliability**:
  - fixed temporal rebuild/search maintenance ordering and added explicit MVCC head rebuild/bootstrap flows for current stores.
- **Namespacing and shutdown hardening**:
  - fixed duplicate namespace prefixing in transaction and namespaced storage wrappers by making node/edge prefix helpers idempotent
  - fixed Badger `DB Closed` panics to return `ErrStorageClosed` and suppressed benign shutdown-time search indexing errors after database close/cancel.
- **HNSW runtime transition tombstone leakage**:
  - fixed HNSW result assembly to exclude tombstoned candidates that can survive in the search candidate heap after a delete during runtime strategy transition replay.
- **Plugin test isolation**:
  - fixed Heimdall plugin loader tests by resetting the global subsystem manager between test cases so plugin registrations do not leak across subtests.

### Tests

- Added/expanded regression and benchmark coverage for:
  - indexed temporal `AS OF` lookups, temporal overlap validation, rebuilds, and pruning
  - MVCC visibility, head rebuilds, pruning, retained-floor behavior, and search invariance smoke tests
  - snapshot isolation semantics including read-your-writes, repeatable label scans, write-write conflicts, edge/node delete races, snapshot-consistent edge traversal, write skew, and contention aborts
  - closure-based transaction retries and concurrent counter increments through `DB.Update()`
  - compound `MATCH ... CREATE` elementId relationship creation, migration `NOT` relationship filters, missing-MVCC-head edge creation fallback, snapshot-safe missing-head reads, shutdown hardening, namespaced prefix idempotence, plugin loader isolation, and deleted-entrypoint HNSW search filtering.

### Documentation

- Added/updated documentation for:
  - historical reads, MVCC retention, pruning guarantees, startup/recovery behavior, and retained-floor semantics
  - storage transaction isolation guarantees and feature parity language
  - temporal query usage, serialization expectations, and operational configuration examples.

### Technical Details

- **Range covered**: `v1.0.26..HEAD`
- **Commits in range**: 4 (non-merge)
- **Repository delta**: 47 files changed, +6,819 / -362 lines
- **Non-test surface changed**: 35 files
- **Primary focus areas**: indexed temporal lookups, MVCC historical storage, Snapshot Isolation correctness, and retry-friendly transaction ergonomics.

## [v1.0.26] - 2026-03-20

### Changed

- **Cypher mutation pipeline compatibility**:
  - generalized execution handling for complex mutation chains combining `UNWIND`, `MERGE`, `SET`, `OPTIONAL MATCH`, `WITH`, and `WHERE`
  - improved clause sequencing reliability for multi-stage write statements with intermediate projections.
- **Proxy/base-path runtime behavior**:
  - restored and hardened proxied UI/base-path asset loading behavior for containerized deployments
  - improved path normalization to reduce route/asset resolution drift behind reverse proxies.
- **Managed vector/query execution safeguards**:
  - tightened vector/search-related persistence and serialization guardrails for large payload handling
  - improved runtime safety defaults for high-volume decode/read paths.

### Fixed

- **Unique-key conflict handling in batched MERGE writes**:
  - fixed batched `UNWIND ... MERGE` paths to correctly reuse matching nodes during a statement, preventing false duplicate-create violations under unique constraints.
- **Mutation context propagation across chained clauses**:
  - fixed variable binding continuity so downstream relationship merges resolve correctly after intermediate `WITH` projections and optional matches.
- **Aggregate alias continuity across chained MATCH stages**:
  - fixed alias preservation in chained query stages to prevent dropped/incorrect projected values.
- **UI/path security hardening**:
  - fixed reflected path handling in UI routing flows to mitigate injection/open-redirect risk surfaces.
- **Storage path-safety hardening**:
  - fixed segment/file access validation in WAL-related paths to reduce traversal-style file access risk.
- **Dependency security updates**:
  - upgraded vulnerable transport/runtime dependencies to patched versions in security-sensitive protocol paths.

### Tests

- Added/expanded regression coverage for:
  - complex Cypher mutation shape permutations (`UNWIND` + chained `MERGE`/`OPTIONAL MATCH`/`WITH`/`WHERE`)
  - unique-constraint behavior under batched merge writes
  - UNWIND property substitution and merge-chain execution edge cases
  - proxy/base-path UI routing behavior
  - storage path validation and bounded message-pack decode limits.

### Documentation

- Added/updated policy and project documentation for patent/licensing posture clarity.

### Technical Details

- **Range covered**: `v1.0.25..HEAD`
- **Commits in range**: 14 (non-merge)
- **Repository delta**: 35 files changed, +1,760 / -91 lines
- **Non-test surface changed**: 25 files
- **Primary focus areas**: Cypher write-path correctness, constraint-safe batched merges, security hardening, and proxy deployment reliability.

## [v1.0.25] - 2026-03-20

### Changed

- **Correlated subquery execution reliability**:
  - generalized correlated `CALL { ... }` execution handling for broader valid clause combinations, including mixed `WITH`, `UNION`, and procedure-yield pipelines
  - improved execution consistency for multi-stage query pipelines that combine procedural and graph pattern clauses.
- **ID-based query execution path optimization**:
  - added direct ID-seek planning for simple `MATCH ... WHERE id(...) = ...` and `elementId(...) = ...` query shapes
  - reduced unnecessary scan behavior for high-frequency point-lookup patterns.
- **Container build base-image sourcing**:
  - updated Docker build variants to use mirrored/public base-image registries for common runtime/build dependencies
  - reduced susceptibility to upstream rate-limit failures during CI/CD image resolution.
- **Keyword-aware clause parsing consistency**:
  - migrated remaining clause-routing keyword detection paths away from raw string index checks to shared keyword helpers
  - improved robustness for mixed whitespace/newline formatting and reduced false keyword matches inside expression bodies.

### Fixed

- **`WITH` identifier substitution robustness**:
  - fixed identifier replacement behavior to avoid accidental token corruption in downstream clauses.
- **Empty-seed correlated subquery result shape**:
  - fixed no-seed correlated `CALL` paths to preserve projected column schemas rather than returning fallback/internal columns.
- **Node import projection in correlated subqueries**:
  - fixed node-variable import binding so property projections from imported variables resolve correctly in subquery bodies.
- **Canonical ID comparison normalization**:
  - fixed `id(...)` / `elementId(...)` comparison handling by normalizing canonical element-ID inputs to internal IDs before evaluation.
- **MERGE resolution under stale lookup conditions**:
  - fixed merge-path behavior to recover correctly when fast lookup candidates are stale or conflict with already-existing rows.
- **Cypher compatibility hardening**:
  - fixed edge-case compatibility issues in optional pattern matching and UNWIND map-property access paths.
- **Nested UNWIND parsing correctness**:
  - fixed double-UNWIND `AS` parsing offsets so nested list expansion queries return expected rows under complex clause chains.
- **COLLECT subquery WHERE rewrite stability**:
  - fixed COLLECT-subquery rewriting when a `WHERE` clause is already present, preventing malformed query text and empty result regressions.

### Tests

- Added regression coverage for:
  - correlated subquery execution variants (including `UNION` + procedure-yield + projection pipelines)
  - empty-seed correlated subquery schema behavior
  - node-import property projection correctness in subqueries
  - direct ID/elements-ID point-lookup planning paths
  - merge conflict/stale-lookup recovery behavior.
- Restored full `pkg/cypher` regression pass after parser/executor clause-detection updates, including complex nested UNWIND/list-expression cases and COLLECT-subquery branch coverage.

### Technical Details

- **Range covered**: `v1.0.24..HEAD`
- **Commits in range**: 10 (non-merge)
- **Repository delta**: 23 files changed, +1,768 / -291 lines
- **Non-test surface changed**: 17 files
- **Primary focus areas**: correlated subquery correctness, point-lookup performance, Cypher compatibility hardening, merge-path resiliency, and CI/CD container base-image reliability.

## [v1.0.24] - 2026-03-19

### Changed

- **CALL/YIELD pipeline execution**:
  - generalized post-`YIELD` clause handling so `MATCH`, `WITH`, `RETURN`, `ORDER BY`, `SKIP`, and `LIMIT` pipelines execute consistently after procedure calls
  - removed brittle fixed-shape assumptions and aligned handling with broader valid clause combinations.
- **Server test fixture reuse**:
  - refactored high-frequency server test paths to share fixtures through grouped subtests where isolation was not required
  - reduced repeated full server/bootstrap setup in hot test paths.
- **Test startup behavior**:
  - disabled external embedding initialization in shared server test setup to avoid unnecessary async retry work in generic tests.

### Fixed

- **CALL clause boundary parsing**:
  - fixed `YIELD` parsing to respect CALL-clause boundaries instead of matching by raw string position
  - resolved cases where downstream clauses were incorrectly parsed or skipped in multi-clause statements.
- **N-column YIELD projection correctness**:
  - fixed projection behavior so `YIELD` supports variable-width output columns without shape-specific fallbacks
  - resolved incorrect/empty result sets in valid procedure-followed query pipelines.
- **Concurrent metadata snapshot race in multi-database management**:
  - made database metadata snapshots lock-safe when listing and fetching database info
  - prevents race conditions between storage-size cache initialization and metadata reads under concurrent access.

### Tests

- Added regression coverage for:
  - procedure-call pipelines with trailing clause permutations
  - boundary-aware `YIELD` parsing in multi-clause statements
  - multi-column `YIELD` projection with downstream `MATCH`/`WITH`/`RETURN` flows.
- Added a dedicated race regression test for concurrent storage-size initialization vs database metadata listing.
- Consolidated selected multi-database and server branch tests into shared-fixture suites while preserving isolated setup for lifecycle-sensitive cases.

### Technical Details

- **Range covered**: `v1.0.23..HEAD`
- **Commits in range**: 3 (non-merge)
- **Repository delta**: 5 files changed, +806 / -108 lines
- **Primary focus areas**: procedure pipeline correctness, parser robustness, and generalized clause compatibility for Cypher execution.

## [Earlier Releases Summary] - up to v1.0.23

This condensed section summarizes user-facing progress from all releases prior to the latest five entries.

### Highlights

- **Composite/Fabric capability matured significantly**:
  - introduced and hardened multi-database/composite execution across HTTP and Bolt
  - improved `USE`/subquery planning and execution behavior
  - strengthened remote constituent connectivity and auth-aware routing.
- **Cypher compatibility and execution quality improved**:
  - expanded support for complex `CALL`/`YIELD`/`WITH`/`UNION`/`UNWIND` query pipelines
  - improved deterministic result-shape handling, aggregation behavior, and clause-boundary parsing
  - hardened DDL/index/constraint handling and compatibility edge cases.
- **Hot-path and index-driven performance improved**:
  - added index-first routing for common query shapes (`IN`, `IS NOT NULL`, top-K/ordered limits)
  - improved correlated apply/join paths and reduced allocation-heavy execution branches
  - expanded plan/query cache usage where safe.
- **Search and vector behavior became more deterministic**:
  - improved BM25/index consistency and startup/rebuild behavior
  - hardened dropped-database artifact cleanup
  - improved rerank configuration application at per-database scope.
- **Operations and UX advanced**:
  - added Browser multi-statement execution UX
  - improved database metadata visibility in UI
  - added/expanded environment-variable and topology/operator documentation
  - added stdout/stderr lifecycle controls and other operational hardening.
- **Reliability and coverage expanded across the stack**:
  - large increase in deterministic regression/integration/performance tests
  - broad hardening across storage, server, cypher, and fabric execution paths.

### Documentation

- Expanded the multi-database guide and added Fabric gap-analysis, delivery-plan, and performance-audit notes to document the new composite execution model and remaining follow-up items.
- Refreshed README badges/community links during the release range.

### Technical Details

- **Range covered**: `v1.0.16..main`
- **Commits in range**: 21 (non-merge)
- **Repository delta**: 230 files changed, +25,221 / -5,323 lines
- **Non-test surface changed**: 67 files
- **Primary focus areas**: Fabric/composite execution, remote constituent routing, transaction/protocol parity, multidatabase stats/UI, planner/executor performance.

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

[v1.0.13]: https://github.com/orneryd/NornicDB/compare/v1.0.12-hotfix...v1.0.13
[v1.0.14]: https://github.com/orneryd/NornicDB/compare/v1.0.13...v1.0.14
[v1.0.12-hotfix]: https://github.com/orneryd/NornicDB/compare/v1.0.12...v1.0.12-hotfix
[v1.0.12]: https://github.com/orneryd/NornicDB/compare/v1.0.12-preview...v1.0.12
[v1.0.12-preview]: https://github.com/orneryd/NornicDB/compare/v1.0.11...v1.0.12-preview
[v1.0.11]: https://github.com/orneryd/NornicDB/compare/v1.0.10...v1.0.11
[v1.0.10]: https://github.com/orneryd/NornicDB/compare/v1.0.9...v1.0.10
[v1.0.9]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.9
[v1.0.6]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.6
[v1.0.1]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.1
[v1.0.0]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.0
