# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Latest Changes]

- See `docs/latest-untagged.md` for the untagged `latest` image changelog.

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
  - fixed transaction edge creation/commit validation to accept readable endpoint nodes even when MVCC head metadata is temporarily missing, instead of incorrectly rejecting valid edges as dangling.
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
  - compound `MATCH ... CREATE` elementId relationship creation, migration `NOT` relationship filters, missing-MVCC-head edge creation fallback, shutdown hardening, namespaced prefix idempotence, plugin loader isolation, and deleted-entrypoint HNSW search filtering.

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

## [v1.0.23] - 2026-03-19

### Changed

- **Docker image defaults (BGE variants)**:
  - reranking is now disabled by default in BGE-enabled image definitions to reduce unexpected startup/runtime overhead
- **Relationship MATCH execution path**:
  - added safer early `LIMIT` short-circuiting for eligible relationship traversal shapes
  - improved start-node pruning via property indexes for common predicate patterns (including `IS NOT NULL`).

### Fixed

- **Index reliability after restart**:
  - fixed persisted schema cache rebuild so property/composite index entries are restored on startup, not just schema metadata
  - addresses cases where indexes appeared present (`SHOW INDEXES`) but behaved inconsistently until recreated.
- **Cypher DDL compatibility and robustness**:
  - fixed parsing/normalization for broader valid DDL forms, including backtick identifiers, trailing `OPTIONS`, and `DROP CONSTRAINT ... IF EXISTS` variants
  - improved cache invalidation behavior after schema drops to prevent stale execution decisions.

### Tests

- Added regression tests for:
  - relationship traversal early-limit short-circuit behavior
  - indexed relationship start-node pruning paths
  - DDL parsing compatibility variants
  - index usability persistence across engine restart.

### Technical Details

- **Range covered**: `v1.0.22..HEAD`
- **Commits in range**: 3 (non-merge)
- **Repository delta**: 15 files changed, +481 / -88 lines
- **Primary focus areas**: index persistence correctness, DDL compatibility hardening, relationship-match execution efficiency, and conservative search default behavior in BGE images.

## [v1.0.22] - 2026-03-19

### Added

- **Expanded Cypher regression coverage**:
  - added targeted tests for correlated `UNWIND` + multi-`MATCH` execution routing
  - added stability tests for `MATCH ... CREATE ... RETURN count(*)` cardinality and idempotent behavior.

### Changed

- **MATCH...CREATE join execution hot path**:
  - added join-aware combination building for N-variable equality-join shapes to avoid unnecessary cartesian expansion
  - improved selective pushdown behavior for `IN` + equality predicates before combination building.
- **UNWIND correlated rewrite routing**:
  - preserved correlation semantics in rewrite transforms so value-bucket joins do not degrade into cross-key cartesian behavior.

### Fixed

- **Aggregation row-shape correctness**: Fixed edge cases where aggregation-only `RETURN count(*)` could produce an empty rowset instead of a single deterministic row.
- **MATCH/WHERE context handling**: Fixed multi-variable `WHERE` handling in `MATCH...CREATE` paths so per-node filters are only applied when semantically valid and cross-variable predicates are evaluated in the correct phase.
- **Relationship existence filter handling**: Fixed `NOT (a)-[:TYPE]->(b)` handling in post-filter execution so write guards remain correct under batched correlated joins.

### Tests

- Added deterministic tests for:
  - dual-side `IN` join-filtered `MATCH...CREATE` write paths
  - join-aware combination builder correctness for multi-variable equality joins
  - correlated UNWIND + multi-MATCH execution routing and count semantics.
- Expanded Fabric/Cypher targeted regression validation around direct context matching and correlated execution behavior.

### Technical Details

- **Range covered**: `v1.0.20..HEAD`
- **Commits in range**: 3 (non-merge)
- **Repository delta**: 15 files changed, +2,799 / -116 lines
- **Primary focus areas**: Cypher write-path reliability, correlated `UNWIND` routing correctness, `MATCH...CREATE` join-path performance, and aggregation result-shape determinism.

## [v1.0.20] - 2026-03-18

### Added

- **Fabric plan cache**: Added `pkg/fabric/plan_cache.go` and wired prepared-plan reuse for repeated multi-graph query shapes to reduce repeated planner work on cache misses.
- **Fabric/Cypher hot-path tracing**: Added explicit hot-path trace plumbing (`pkg/fabric/tracing.go`, `pkg/cypher/executor_hotpath_trace.go`) to verify/lock optimization-path usage in integration tests.
- **Targeted index-seek regression coverage**:
  - added explicit tests for `MATCH ... WHERE <prop> IS NOT NULL ORDER BY <prop> LIMIT K` index top-K behavior
  - added tests for indexed `IN $param` seek paths used by correlated Fabric APPLY batching
  - added constant-conjunct top-K matching coverage.
- **Planning docs expansion**:
  - Added `docs/plans/gpu-hnsw-construction-plan.md`
  - Rewrote/expanded `docs/plans/ui-enhancements.md` to reflect current priorities.

### Changed

- **Correlated join execution path hardening**:
  - restored and stabilized correlated Fabric APPLY batched lookup hot paths across cloned executor contexts
  - improved trace propagation across `USE`-scoped executor clones so hot-path verification reflects real runtime routing.
- **Cypher/Fabric performance optimization pass**:
  - reduced allocation-heavy branches in correlated APPLY processing
  - improved index-first routing for high-frequency MATCH shapes
  - refined indexed top-K and index-seek behavior for large-cardinality query paths.
- **Safe query cache behavior for remote targets**:
  - tightened result cache gating so remote-constituent execution paths avoid unsafe/undesired local result caching.
- **UI base-path normalization**:
  - normalized Vite/UI base-path handling to avoid trailing-slash concatenation issues and route/API join breakage.

### Fixed

- **UNWIND map-row create/merge regression**: Fixed Cypher UNWIND parameter handling where map-row create/merge flows could fail or route incorrectly under Fabric/composite execution.
- **BM25 deterministic lexical-seed stability**: Fixed nondeterministic lexical seed selection in BM25 v2 paths and stabilized index drop/recreate guards.
- **Dropped-database search artifact cleanup**: Fixed stale per-database persisted search artifacts surviving `DROP DATABASE` and being reloaded unexpectedly on recreate.
- **`IS NOT NULL` top-K matcher robustness**: Fixed hot-path matcher to keep index top-K active when additional constant top-level `AND` predicates are present.

### Tests

- Added deterministic end-to-end/integration coverage for:
  - correlated Fabric APPLY joins (including 100k-scale profiling benchmark paths)
  - hot-path trace assertions (`OuterIndexTopK`, batched APPLY row path, scan-fallback guardrails)
  - indexed `IN` and indexed top-K MATCH execution branches
  - UNWIND property map regression scenarios.
- Expanded storage schema/index tests around top-K ordering and cache invalidation behavior.

### Documentation

- Updated roadmap entries in `README.md` to reflect current planning state.
- Refreshed UI enhancement planning doc with concrete structure and testability focus.
- Added GPU-assisted HNSW construction plan doc with persistence-compatibility constraints.

### Technical Details

- **Range covered**: `v1.0.19..HEAD`
- **Commits in range**: 15 (non-merge)
- **Repository delta**: 53 files changed, +5,884 / -779 lines
- **Non-test surface changed**: 36 files
- **Primary focus areas**: Fabric/Cypher correlated-join performance, index-driven hot paths, cache safety for remote constituents, deterministic search behavior, and roadmap/planning documentation updates.

## [v1.0.19] - 2026-03-17

### Added

- **Cypher multi-statement browser execution UX**: Added semicolon-delimited script execution in the Browser query pane with Cypher-aware splitting (strings/comments/backticks safe), stacked per-statement result panels, per-statement status/timing, and a **continue-on-error** toggle.
- **Infinigraph implementation guidance**: Rewrote and expanded the Infinigraph topology user guide to target managed-service parity scenarios (not generic Fabric-only usage), including concrete architecture and implementation diagrams.

### Changed

- **Per-database rerank override application**: Search request paths now consistently use per-database reranker resolution across HTTP and Qdrant gRPC execution paths instead of relying on global-only rerank flags.
- **Reranker resolution model**: Server startup now resolves effective per-DB rerank provider/model/API settings from DB config overrides with global fallback and cached external reranker instances.
- **Database UI metadata**: `/databases` and related API/UI surfaces now expose and display search strategy details more consistently for operational visibility.

### Fixed

- **UNWIND create/execution hardening**: Fixed execution-path edge handling in Cypher/Fabric integration where UNWIND-based create flows could diverge under composite/fabric routing branches.
- **Composite-only Fabric routing guardrails**: Tightened routing so Fabric-specific execution paths are applied only where composite semantics require them, reducing misrouting and ambiguous behavior.

### Tests

- **Storage package coverage push**: Expanded real-assertion coverage across storage engines and helper paths to sustain 90%+ package coverage and lock regression-prone branches.
- **Cypher/Fabric regression expansion**: Added and hardened deterministic tests around composite routing, UNWIND execution, and protocol-level parity paths.

### Documentation

- Replaced generic topology guidance with a concrete Infinigraph parity implementation guide, including:
  - control-plane/data-plane architecture diagram
  - property contract diagram
  - bounded cross-constituent query flow diagram
  - shard split/rebalance sequence diagram

### Technical Details

- **Range covered**: `v1.0.18..main`
- **Commits in range**: 8 (non-merge)
- **Repository delta**: 42 files changed, +3,946 / -393 lines
- **Non-test surface changed**: 22 files
- **Primary focus areas**: per-DB rerank correctness, Browser multi-statement UX, Cypher/Fabric hardening, storage regression coverage, and Infinigraph implementation documentation.

## [v1.0.18] - 2026-03-16

### Added

- **Canonical environment variable reference**: Added and expanded the operations environment-variable inventory with runtime-referenced `NORNICDB_*` keys, defaults, and usage notes in `docs/operations/environment-variables.md`.
- **Operations/user documentation additions**: Added Infinigraph topology user guide and linked operations docs/navigation updates.

### Changed

- **STDIO log growth controls**: Added automatic stdout/stderr compaction limits and scheduling controls in `cmd/nornicdb` with defaults tuned for hourly compaction and KB-based sizing.
- **Config/docs alignment**: Updated operations configuration docs and resolver coverage to better align documented behavior with runtime config resolution.
- **README/community metadata refresh**: Updated release badges and repository metadata links.

### Fixed

- **UTF-8 truncation correctness**: Fixed OpenAI Heimdall truncation behavior to avoid returning invalid UTF-8 when truncating near rune boundaries.
- **Repository path compatibility**: Fixed checked-in path issues affecting cross-platform repository handling (including Windows-unfriendly path forms).
- **Auto-TLP + memory decay settings persistence**: Fixed settings-save behavior for these configuration paths.

### Tests

- Expanded deterministic coverage in Fabric/Cypher/Server execution paths, including planner/executor/transaction and `USE`/composite branches.
- Added regression coverage for OpenAI truncation handling and fulltext index behavior.
- Added CLI log-compaction tests and additional resolver/config test coverage.

### Documentation

- Updated operations docs index and configuration references.
- Added/updated planning docs for UI enhancements and topology guidance.

### Technical Details

- **Range covered**: `v1.0.17..main`
- **Commits in range**: 12 (non-merge)
- **Repository delta**: 41 files changed, +9,255 / -392 lines
- **Non-test surface changed**: 9 files
- **Primary focus areas**: env-var documentation, log lifecycle controls, Fabric/Cypher deterministic test expansion, truncation correctness, and release/readme metadata refresh.

## [v1.0.17] - 2026-03-14

### Added

- **Fabric composite execution framework**: Added the initial `pkg/fabric` planner, executor, transaction, catalog, and gateway stack for Neo4j-style composite database execution across local and remote constituents.
- **Remote constituent connectivity**: Added remote engine and credential plumbing for multidatabase/composite graphs so routed queries can execute against remote databases.
- **Cypher transaction-script and shell preprocessing support**: Added transaction keywords, shell command preprocessing, and `USE`-aware subquery execution to improve scripted Cypher workflows.
- **Per-database storage size reporting**: Added cached node and embedding byte statistics surfaced through `/databases` and the Web UI.

### Changed

- **Composite transaction routing parity**: Completed Fabric-aware database-manager routing across HTTP and Bolt so composite transactions preserve Neo4j-compatible error semantics and database targeting behavior.
- **Nested `CALL { USE ... }` planning**: Replaced flat/regex-style handling with recursive subquery decomposition that validates `USE` scope boundaries and preserves import bindings through correlated execution paths.
- **Composite Neo4j semantics hardening**: Expanded parity behavior for composite schema/existence flows, auth-aware routing, remote constituent execution, and empty-result handling across server and storage adapters.
- **Fabric hot-path performance**: Removed multiple allocation hotspots in planner/executor paths and added simple equality index-seek support in Cypher execution. Benchmarks in the range show notable wins including `deduplicateRows` improving from `1.91ms` to `1.22ms` and `combineRows` from `246ns` to `74ns`.

### Fixed

- **Correlated `WITH` + `USE` execution bugs**: Fixed correlated subquery planning/execution cases that previously produced incorrect APPLY shaping or unstable record propagation.
- **Bolt vs HTTP routing drift**: Fixed Bolt database-manager routing so it matches HTTP behavior for composite/Fabric execution and correlated import handling.
- **Empty Fabric result UI crash**: Fixed empty-result and null-column handling that could surface unstable response shapes and crash query result rendering in the UI.
- **Composite schema and remote edge cases**: Fixed remaining parity gaps around composite schema commands, remote existence checks, auth forwarding, and transaction manager behavior.

### Tests

- Added broad regression and integration coverage across Fabric planner/executor/gateway/transaction paths, composite schema and transaction flows, Bolt db-manager routing, remote engine behavior, and auth forwarding.
- Added performance audit artifacts and benchmark coverage for Fabric hot paths plus targeted regression tests for Cypher index-seek execution.

### Documentation

- Expanded the multi-database guide and added Fabric gap-analysis, delivery-plan, and performance-audit notes to document the new composite execution model and remaining follow-up items.
- Refreshed README badges/community links during the release range.

### Technical Details

- **Range covered**: `v1.0.16..main`
- **Commits in range**: 21 (non-merge)
- **Repository delta**: 230 files changed, +25,221 / -5,323 lines
- **Non-test surface changed**: 67 files
- **Primary focus areas**: Fabric/composite execution, remote constituent routing, transaction/protocol parity, multidatabase stats/UI, planner/executor performance.

## [v1.0.16] - 2026-03-12

### Changed

- **Cypher openCypher strictness hardening**:
  - Tightened `SET +=` inline map parsing to reject malformed map literals.
  - Tightened `CREATE ... RETURN` and related parser/validation paths to reject permissive invalid forms.
  - Ensured `UNWIND ... SET n = row` map-literal handling follows expected Cypher semantics.
- **Cypher execution correctness**:
  - Fixed `SET ... WITH ... RETURN` row semantics so property updates remain visible in trailing pipeline clauses.
  - Added protection against non-progressing `CALL ... IN TRANSACTIONS` iterative loops.
  - Added strict malformed relationship-pattern rejection in traversal/match paths.
- **Cypher schema/index compatibility**:
  - Restored Neo4j-compatible `CREATE INDEX` parsing for parenthesized node patterns.
  - Added compatibility support for additional `CREATE INDEX` / `CREATE FULLTEXT INDEX` syntax variants and persistence paths.
  - Added qualified `SHOW ... INDEXES` routing/handling (`FULLTEXT`, `RANGE`, `VECTOR`) with deterministic type filtering.
- **Web UI routing behavior**: Updated UI router behavior to preserve/enforce trailing slash paths for proxy compatibility.
- **Heimdall model source update**: Updated Docker model-download source for Qwen GGUF in Heimdall images.

### Fixed

- **Index parsing bug**: Fixed `CREATE INDEX FOR (n:Label) ON (n.prop)` label parsing edge case where `)` could be captured as part of label token.
- **Silent schema no-op bug**: Fixed successful-but-non-persisted index creation paths for compatibility syntaxes.
- **SHOW INDEX compatibility gap**: Fixed `SHOW FULLTEXT INDEXES` command handling and added qualified `SHOW RANGE/VECTOR INDEXES` support instead of unsupported-command errors.
- **Pre-commit conflict handling**: Hardened `.githooks/pre-commit` workflow behavior to avoid merge-conflict-inducing side effects.

### Tests

- Expanded Cypher deterministic branch coverage across parser, schema/index, dispatch, transaction, mutation, traversal, APOC/call wrappers, and compatibility paths.
- Added targeted regression tests for schema routing with async storage stacks and Neo4j-compatible index command variants.

### Technical Details

- **Range covered**: `v1.0.15..HEAD`
- **Commits in range**: 19 (non-merge)
- **Repository delta**: 57 files changed, +5,185 / -165 lines
- **Non-test surface changed**: 20 files
- **Primary focus areas**: Cypher correctness/compatibility, index semantics, deterministic coverage expansion, UI routing compatibility, Heimdall model-source resiliency.

## [v1.0.15] - 2026-03-10

### Added

- **Coverage/report generation tooling in-repo**: Added `scripts/generate-coverage.sh` and expanded `scripts/filter-generated-coverage.sh` so CI coverage reporting is reproducible locally and can consistently exclude generated and hardware-specific paths.
- **Developer hook automation**: Added `.githooks/pre-commit` plus `scripts/install-git-hooks.sh` to enforce formatting checks in local workflows.
- **Llama CPU libs build path**: Added dedicated CPU llama-libs image build assets (`docker/Dockerfile.llama-cpu`, `scripts/hydrate-llama-cpu-libs.sh`) and corresponding workflow wiring.

### Changed

- **CI/CD workflow restructuring**: Updated `.github/workflows/ci.yml`, `cd.yml`, `cd-llama-cpu.yml`, and `cd-llama-cuda.yml` to separate llama library publishing, use prebuilt runtime assets in CI, and centralize coverage/report logic.
- **UI asset embedding compatibility**:
  - `ui/embed.go` now embeds `all:dist` to avoid empty-subdirectory embed failures.
  - `pkg/server/ui.go` switched UI assets from `embed.FS` to `fs.FS` and added explicit nil-asset validation.
- **Search service event routing while index builds run** (`pkg/nornicdb/search_services.go`): Introduced deferred mutation queues for index/remove events during build windows and deterministic post-build flush behavior.
- **DB background-task lifecycle hardening** (`pkg/nornicdb/db.go`): Added tracked background-task startup helper, safer close sequencing, and cleaner goroutine ownership boundaries.
- **GraphQL edge/node ID normalization** (`pkg/graphql/resolvers/helpers_namespaced.go`): Consolidated repeated source/target ID coercion paths into a shared helper for namespaced result handling.
- **Ops/runtime guardrails**:
  - `pkg/config/config.go`: perf gates moved to warn-level behavior.
  - `pkg/inference/cooldown.go`: added default fallback lookup path for empty-label cooldown entries.
  - `pkg/storage/namespaced.go`: bulk node/edge creation now rejects nil inputs with `ErrInvalidData`.
  - `pkg/nornicdb/db_admin.go`: `FindSimilar` now validates `limit > 0`.

### Fixed

- **Cypher parser strictness and AST correctness**:
  - Rejects invalid bare `OPTIONAL` forms and unterminated string literals (`pkg/cypher/parser.go`).
  - Parses map literals into structured AST map expressions instead of permissive fallback handling (`pkg/cypher/ast_builder.go`).
- **Cypher fulltext compatibility** (`pkg/cypher/call_fulltext.go`): fulltext parameter extraction now supports `db.index.fulltext.queryRelationships(...)` alongside node queries.
- **Cypher traversal/query correctness**:
  - Relationship pattern matching now fails fast on malformed bracket/paren patterns (`pkg/cypher/merge.go`).
  - `SUM(...)` evaluation aligned with openCypher semantics, including arithmetic combinations and null-handling behavior (`pkg/cypher/clauses.go`).
  - `CALL ... IN TRANSACTIONS` now guards against non-progressing iterative batching for non-batchable write subqueries (`pkg/cypher/executor_subqueries.go`).
  - `MATCH ... CREATE` zero-row behavior now short-circuits CREATE as expected when upstream MATCH yields no rows (`pkg/cypher/create.go`).
- **Embedder creation race** (`pkg/nornicdb/db.go`): added a second registry check inside single-flight creation lock to prevent duplicate concurrent embedder initialization.

### Technical Details

- **Range covered**: `v1.0.14..HEAD`
- **Commits in range**: 95 (non-merge)
- **Non-test surface changed**: 42 files, +1,257 / -490 lines
- **Primary focus areas**: CI/CD + coverage pipeline hardening, Cypher compatibility/correctness fixes, search/build concurrency stabilization, UI embedding reliability.

## [v1.0.14] - 2026-03-07

### Changed

- **Neo4j procedure compatibility completion**: Finished the stored-procedure parity tranche with startup-compiled procedure DDL, msgpack-backed procedure catalog persistence, registry preloading on startup, and transaction-script handling for `BEGIN TRANSACTION` / `BEGIN` shorthand, `COMMIT`, and rollback-oriented flows.
- **CI/CD simplification and release automation**: Replaced the older workflow sprawl with a tighter CI path, added Docker CD workflows for release tags and manual dispatch, and split `llama-cuda-libs` publishing into its own workflow so CUDA base images are only rebuilt when their pinned dependency version changes.
- **Docker image build resilience**: Updated the release Dockerfiles to download required embedding / reranker / Heimdall models during `docker build` instead of assuming a pre-populated local `models/` directory.
- **Configuration hardening**: Standardized memory-limit parsing to a single integer-in-megabytes model with fail-fast validation for invalid configuration values.
- **Repository hygiene**: Renamed `LICENCE` to `LICENSE.md`, refreshed coverage/reporting badges and workflow names, and aligned release automation with the current repository layout.

### Fixed

- **MCP server build regression**: Fixed a helper-path regression where `pkg/mcp/server.go` referenced `containsLabel()` from production code even though the helper only existed in tests, breaking non-test builds.
- **Snapshot collision risk in storage recovery**: Fixed the snapshot overwrite / timestamp-collision path that could make recovery select the wrong snapshot in CI or fast-running test environments.
- **Coverage-report drift and generated-code filtering**: Corrected package scoping and generated-folder exclusions so CI coverage reporting reflects handwritten code instead of noisy generated or hardware-specific paths.
- **Storage helper regressions exposed by tests**: Fixed helper-path issues uncovered during the storage coverage push, including schema fallback assumptions, streaming helper safety, and other small branch-specific regressions across async, namespaced, composite, WAL, and transaction helpers.

### Tests

- **Major handwritten coverage expansion**: Added a large maintenance pass of regression tests across `pkg/storage`, `pkg/mcp`, `pkg/nornicdb`, `pkg/auth`, `pkg/config`, and handwritten ANTLR / Cypher helpers.
- **Parser and storage hardening**: Extended parser, procedure, APOC adapter, transaction, WAL, async-engine, namespaced-engine, and composite-engine coverage to exercise previously untested compatibility and recovery paths.
- **Flake reduction**: Added targeted regression coverage around snapshot recovery and helper behavior so CI catches the same edge cases that previously slipped through.

### Documentation

- **Procedure compatibility docs**: Added and refined stored procedure parity and usage documentation, including procedure DDL and transaction-script examples.
- **Operational docs refresh**: Updated README / docs links, coverage-related docs, and release-supporting notes to match the current CI/CD and packaging flow.

### Technical Details

- **Statistics**: 30 commits, 341 files changed, +23,539 / -4,441 lines.
- **Primary focus areas**: Cypher procedure compatibility, CI/CD and Docker release automation, storage/MCP hardening, and large regression-coverage expansion.

## [v1.0.13] - 2026-03-04

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

## [v1.0.12-hotfix] - 2026-02-28

### Added

- **Runtime search strategy transition framework**: Added safe brute-force ↔ HNSW transition plumbing with cutover safeguards and transition tests.

### Changed

- **Hotfix safety posture**: Disabled automatic strategy switching by default in hotfix flow to prioritize runtime stability.
- **Documentation refresh**: Updated README/docs for hotfix behavior and operational guidance.

### Fixed

- **Docker / llama.cpp crash**: Fixed header overwrite issue that could cause SIGSEGV in Docker builds.
- **Windows build reliability**: Fixed Windows build path issues for the hotfix release.

## [v1.0.12-preview] - 2026-02-20

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
- **BM25 Lexical Index Persistence** _(experimental, gated by `NORNICDB_PERSIST_SEARCH_INDEXES`)_: BM25 index is saved to disk alongside the HNSW and vector indexes to eliminate rebuild time on restart.
- **VectorFileStore** _(experimental)_: File-backed append-only vector storage keeps only an id→offset map in RAM, reducing in-process memory for large embedding corpora. Only the id→offset map (~80 bytes/vector) lives in RAM; all 4 KB vectors are paged from disk on demand.
- **HNSW & IVF-HNSW Index Persistence** _(experimental)_: Debounced, non-blocking background saves of both HNSW and IVF-HNSW indexes. Configurable via `NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC`.
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

## [v1.0.11] - 2026-01-27

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

## [v1.0.10] - 2025-12-21

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

## [v1.0.9] - 2025-12-16 - GraphQL API & Neo4j Compatibility Improvements

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

## [v1.0.8] - 2025-12-15

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

## [v1.0.7] - 2025-12-14

### Added

- Documentation: added `DIY.md` with Vulkan/Makefile build instructions and targets.

### Changed

- Makefile and docs: added and documented AMD64 Vulkan build targets and related instructions.

### Fixed

- Neo4j compatibility: return `*storage.Node` and `*storage.Edge` directly from Cypher query results with correct Bolt protocol serialization and property access handling. (commit bc38eb3)
- Neo4j compatibility: improved integer handling in Cypher/driver interoperability. (commit ef8f07e)

### Technical Details

- Commits included: `bc38eb3` (node/edge return serialization), `ef8f07e` (integer compatibility), `f24825a`/`436ce36` (docs/Makefile Vulkan targets and `DIY.md`).

## [v1.0.6] - 2025-12-12

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

## [v1.0.5] - 2025-12-10

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

## [v1.0.4] - 2025-12-10

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

## [v1.0.3] - 2025-12-09

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

## [v1.0.2] - 2025-01-27

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

## [v1.0.1] - 2025-12-08

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

## [v1.0.0] - 2024-12-06

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
