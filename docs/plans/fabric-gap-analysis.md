# Fabric Gap Analysis: `main...remote-const`

_Last updated: 2026-03-14_

## Scope

Audit of new/changed non-test Fabric/composite-database code on branch `remote-const` compared to `main`, with comparison against Neo4j Fabric / `USE` semantics.

Test files are excluded from the primary code-surface review, but test coverage is tracked separately.

## Review Status

- [x] Diff inventory captured
- [x] Fabric parser / planner surface review complete
- [x] Multi-database and remote execution review complete
- [x] Bolt / HTTP / server end-to-end exposure review complete
- [x] Coverage and e2e parity review complete

## Reviewed Files So Far

### Pass 1
- `pkg/fabric/catalog.go`
- `pkg/fabric/location.go`
- `pkg/fabric/result.go`
- `pkg/fabric/fragment.go`
- `pkg/fabric/gateway.go`
- `pkg/fabric/local_executor.go`
- `pkg/fabric/remote_executor.go`
- `pkg/cypher/executor_fabric.go`
- `pkg/cypher/executor_use.go`
- `pkg/cypher/transaction.go`
- `pkg/cypher/composite_commands.go`
- `pkg/multidb/composite.go`
- `pkg/storage/composite_engine.go`
- `pkg/storage/remote_engine.go`

### Pass 2
- `pkg/fabric/planner.go`
- `pkg/fabric/executor.go`
- `pkg/fabric/transaction.go`
- `pkg/server/server_db.go`
- `pkg/bolt/server.go`

### Pass 3
- `pkg/txsession/manager.go`
- `pkg/multidb/manager.go`
- `pkg/multidb/remote_credentials.go`
- `pkg/storage/remote_engine.go` (transport/auth follow-up)
- `pkg/server/server_db.go` (explicit transaction follow-up)

### Ancillary changed files assessed for audit relevance
- `pkg/cypher/executor.go`
- `pkg/cypher/executor_subqueries.go`
- `pkg/server/server.go`
- `pkg/multidb/storage_size_cache.go`
- `pkg/multidb/storage_size_tracking_engine.go`

These files were checked for Fabric/composite impact. They mainly provide routing hooks, server wiring, or storage-size bookkeeping around the reviewed Fabric/composite paths rather than introducing new Neo4j Fabric surface area on their own.

## Neo4j Fabric Surface Reference Points

Primary semantic targets reviewed so far:
- `USE <graph>`
- `USE` in top-level query, subqueries, and union parts
- composite database constituent addressing (`composite.alias`)
- dynamic graph references (`graph.byName()`, `graph.byElementId()`)
- many-read / one-write per distributed transaction
- end-to-end protocol behavior over Bolt / HTTP

## Findings

### Critical Gaps

- [x] **Remote auth cache isolation bug**
   - File: `pkg/fabric/remote_executor.go`
   - `engineCache` is keyed by URI/database/auth mode only.
   - Forwarded bearer token and explicit credentials are not part of the cache key.
   - Risk: one caller can reuse a remote engine established under another caller's auth context.
   - Status: **critical behavior bug**

- [x] **Remote executor cache is not synchronized**
   - File: `pkg/fabric/remote_executor.go`
   - `engineCache` is mutable shared state with no mutex.
   - Status: **critical concurrency gap**

- [x] **Distributed commit is not atomic on partial commit failure**
   - File: `pkg/fabric/transaction.go`
   - `Commit()` marks already-committed subtransactions as committed and only rolls back still-open ones.
   - This can leave the distributed transaction partially committed.
   - Neo4j Fabric semantics require coordinated transaction behavior, not partial success exposure.
   - Status: **critical semantic gap**

- [x] **Planner only recognizes a narrow Fabric subset**
   - Files: `pkg/fabric/planner.go`, `pkg/cypher/executor_fabric.go`
   - Current path only plans:
     - leading `USE ...`
     - `CALL { USE ... }` subqueries
   - Missing from reviewed code path:
     - `USE` in union parts
     - dynamic graph refs via `graph.byName()`
     - `graph.byElementId()`
     - broader graph-reference parsing/escaping rules
   - Status: **major API surface gap**

- [x] **Planner loses outer-query structure around Fabric subqueries**
   - File: `pkg/fabric/planner.go`
   - `planMultiGraph()` builds apply chain from extracted `CALL { USE ... }` blocks, then only appends trailing clauses after the last block.
   - Query clauses before the first `CALL { USE ... }` are not modeled as executable fragments.
   - This means valid correlated / preparatory outer query structure can be dropped or misrepresented.
   - Status: **major correctness gap**

- [x] **Planner does not implement UNION-part `USE` semantics**
   - File: `pkg/fabric/planner.go`
   - Reviewed planner logic extracts leading `USE` and `CALL { USE ... }` blocks only.
   - No branch planning was found for:
     - `USE g1 ... UNION USE g2 ...`
     - `UNION ALL` variants with graph switching
   - Status: **major API surface gap**

- [x] **`USE` parsing is still limited to direct identifiers**
   - Files: `pkg/cypher/executor_use.go`, `pkg/fabric/planner.go`, `pkg/cypher/transaction.go`
   - Reviewed parsers handle simple identifiers and backtick-quoted names.
   - No support found for:
     - `USE graph.byName(...)`
     - `USE graph.byElementId(...)`
   - Status: **major API gap**

### Major Gaps

- [x] **Fabric planner activation heuristic is too narrow**
   - File: `pkg/cypher/executor_fabric.go`
   - `shouldUseFabricPlanner()` only checks for `CALL { USE ... }`.
   - Top-level Fabric-style graph routing via `USE` may bypass the planner entirely.
   - Status: **major routing gap**

- [x] **Composite engine uses heuristic write routing rather than explicit Fabric graph targeting**
   - File: `pkg/storage/composite_engine.go`
   - Routing uses labels, `database_id`, defaults, and hashing fallback.
   - Neo4j Fabric semantics are explicit graph selection via `USE`, not implicit shard hashing.
   - Status: **major semantic divergence**

- [x] **Bolt ROUTE support is stubbed**
   - File: `pkg/bolt/server.go`
   - `handleRoute()` returns TTL with empty server list.
   - This is protocol-shape compatible only, not behaviorally compatible routing metadata.
   - Status: **major protocol gap**

- [x] **Bolt session-to-database executor creation does not use auth-aware storage resolution**
    - File: `pkg/bolt/server.go`
    - `getExecutorForDatabase()` uses `dbManager.GetStorage(dbName)`, not auth-aware resolution.
    - For composite databases with remote constituents, this can instantiate composite/remote access paths without caller auth forwarding at executor creation time.
    - Status: **major auth propagation gap**

- [x] **Remote Bolt transport always opens write sessions**
    - File: `pkg/storage/remote_engine.go`
    - All session configs use `neo4j.AccessModeWrite`, even for reads.
    - That diverges from expected remote routing and access-mode semantics.
    - Status: **major behavioral gap**

- [x] **HTTP explicit transaction sessions are identified only by tx id, not caller/session ownership**
   - Files: `pkg/txsession/manager.go`, `pkg/server/server_db.go`
   - Reviewed transaction session state stores database/executor/expiry, but no caller binding.
   - HTTP follow-up calls validate only `txID` + `dbName`.
   - Neo4j transaction handles are session/connection scoped; this is a security/behavior mismatch.
   - Status: **major security gap**

- [x] **HTTP explicit transaction access checks are evaluated against the path database, not the effective graph target**
   - File: `pkg/server/server_db.go`
   - `executeTxStatements()` checks writes against the request `dbName`, not a resolved per-statement `USE` target.
   - This can diverge from actual graph touched by Fabric/composite queries.
   - Status: **major authorization gap**

- [x] **Bolt explicit transactions are not executor-stable in multi-database mode**
   - File: `pkg/bolt/server.go`
   - `handleBegin()` / `handleCommit()` / `handleRollback()` operate on `s.executor`.
   - `handleRun()` swaps to a fresh per-query executor from `getExecutorForDatabase(dbName)` whenever a DB manager is present.
   - Result: in multi-database mode, the transaction lifecycle can execute on one executor while statements run on another, which is not valid Bolt transactional behavior.
   - Status: **critical protocol/transaction gap**

### Medium Gaps

- [x] **Result stream merge contract is undocumented in code but unenforced**
    - File: `pkg/fabric/result.go`
    - `Merge()` comment says columns must match, but implementation does not validate that.
    - Status: **medium correctness gap**

- [x] **HTTP endpoint uses `:USE` command parsing, not native Cypher `USE` semantics**
    - File: `pkg/server/server_db.go`
    - Statement preprocessing recognizes shell-style `:USE` directives per statement.
    - This is useful UX, but not the same as end-to-end Cypher Fabric semantics.
    - Status: **medium behavior divergence**

- [x] **Leading `WITH` import rewriting is heuristic**
    - File: `pkg/fabric/executor.go`
    - `rewriteLeadingWithImports()` rewrites leading `WITH` using regex and assumes a narrow clause shape.
    - Complex `WITH` projections / aliasing / filtering may not preserve Neo4j semantics.
    - Status: **medium semantic risk**

- [x] **Composite constituent scope rules are not visibly enforced in planner/parser path**
   - Files: `pkg/fabric/planner.go`, `pkg/cypher/executor_use.go`
   - Neo4j restricts `USE` on a composite connection to constituents of the current composite.
   - Reviewed code parses dotted names generically and defers resolution later; no planner-level scope validation was found.
   - Status: **medium semantic gap**

- [x] **Remote engine API is CRUD-emulated, not transaction-native**
   - File: `pkg/storage/remote_engine.go`
   - Engine methods synthesize Cypher statements per CRUD call using independent contexts/timeouts.
   - This is workable for transport abstraction but not equivalent to coordinated remote transactional semantics expected from a full Fabric shard execution layer.
   - Status: **medium architectural gap**

## Partial Positives

- The overall architecture is directionally aligned with Neo4j Fabric:
  - catalog
  - location abstraction
  - fragment tree
  - planner/executor split
  - distributed transaction object
- Composite database metadata model includes local and remote constituents with auth modes.
- Server and Bolt surfaces appear to expose multi-database selection and pass auth headers toward Fabric execution paths.

## Coverage Mapping Snapshot

### Covered Areas

- Basic planner surface is exercised in `pkg/fabric/planner_test.go`:
  - simple queries
  - leading `USE <graph>`
  - dotted graph names
  - `CALL { USE ... }` subqueries
  - correlated import-column capture
- Transaction write-shard restriction is covered in:
  - `pkg/fabric/transaction_test.go`
  - `pkg/cypher/composite_transaction_fabric_test.go`
- Auth-token forwarding has direct unit coverage in:
  - `pkg/cypher/use_auth_forwarding_test.go`
  - `pkg/multidb/composite_test.go`
  - `pkg/storage/remote_engine_test.go`
- HTTP multi-database and `:USE` command behavior has broad integration coverage in `pkg/server/multi_database_e2e_test.go`.
- Bolt `ROUTE` has smoke-level coverage in:
  - `pkg/bolt/server_test.go`
  - `pkg/bolt/coverage_extra_test.go`
- Bolt database metadata routing has targeted coverage in `pkg/bolt/server_test.go`.
- Storage-size tracking changes in `pkg/multidb/storage_size_cache.go` and `pkg/multidb/storage_size_tracking_engine.go` are covered indirectly through `pkg/multidb/manager_test.go` and `pkg/multidb/enforcement_test.go` and do not materially change Fabric API parity.

### Historical Gap Inventory (Baseline Findings)

This list captures the original baseline findings before recent fixes.  
Current status is tracked in **Current Parity Snapshot**, **Tracked TODOs**, and
**Verification Update (2026-03-14)** below.

1. **No tests found for dynamic graph references**
   - No coverage located for `USE graph.byName(...)`.
   - No coverage located for `USE graph.byElementId(...)`.

2. **No tests found for UNION-part `USE` planning/execution**
   - Existing union tests are generic Cypher union coverage.
   - No Fabric-aware tests were found for `USE g1 ... UNION USE g2 ...` or `UNION ALL` graph switching.

3. **Remote executor auth-cache isolation is untested**
   - No test was found that exercises cached remote-engine reuse across distinct bearer tokens or credential sets.

4. **Remote executor concurrency is untested**
   - No test was found for concurrent access to `pkg/fabric/remote_executor.go` cache state.

5. **Distributed commit atomicity gap is not proven safe by tests**
   - `pkg/fabric/transaction_test.go` covers partial commit failure detection.
   - It does not verify rollback/compensation of already-committed shards, so it does not close the atomicity gap.

6. **Bolt `ROUTE` behavior is only shape-tested**
   - Current tests assert success metadata presence.
   - No test validates populated routing-table members, role distribution, or Neo4j-compatible routing content.

7. **Bolt multi-database explicit transaction consistency is untested**
   - There is coverage that BEGIN metadata influences subsequent query routing.
   - No Bolt test was found proving that `BEGIN`, `RUN`, and `COMMIT` share the same DB-scoped executor/transaction in multi-database mode.

8. **HTTP explicit transaction ownership isolation is untested**
   - Existing transaction tests cover lifecycle and rollback.
   - No test was found showing one authenticated caller cannot reuse another caller's transaction id.

9. **HTTP explicit transaction graph-level authorization is untested**
   - No test was found where request-path database permissions differ from the effective `USE` target graph inside the transaction.

10. **Remote Bolt access-mode behavior is untested**
   - No test was found verifying read queries open read-mode sessions instead of unconditional write-mode sessions.

11. **Bolt auth-aware remote composite bootstrap is untested**
   - No Bolt test was found showing top-level database selection for a remote composite uses caller-auth-aware storage resolution rather than unauthenticated `GetStorage()`.

12. **Constituent addressing usability gap (`USE <composite>.<alias>`) still leaks to DB-not-found paths**
   - Repro observed in current build:
     - `USE translations.tr`
     - `SHOW INDEXES`
   - Returned error:
     - `Neo.ClientError.Database.DatabaseNotFound: Database 'translations.tr' not found`
   - Expected Neo4j/Fabric-compatible behavior:
     - `translations.tr` should resolve as a constituent graph selection of composite `translations`, not a top-level physical database lookup.
   - Impact:
     - Constituent-scoped schema/introspection flows are not reliably usable from clients that emit canonical `USE composite.alias` sequences.
   - Required fix:
     - enforce constituent resolution before any top-level DB lookup in both Bolt and HTTP execution entry paths, and add e2e coverage for `USE composite.alias` + `SHOW INDEXES` and `CREATE/DROP INDEX`.

13. **Index schema scope semantics mismatch on composite root**
   - Current behavior:
     - `SHOW INDEXES` / `SHOW FULLTEXT INDEXES` read `storage.GetSchema()`.
     - For composite storage, `GetSchema()` merges constituent metadata.
     - `CREATE INDEX` / `CREATE FULLTEXT INDEX` write through `e.storage.GetSchema().Add*Index(...)`.
   - Neo4j/Fabric-compatible expectation:
     - indexes are constituent-local;
     - schema commands are explicitly targeted by `USE <constituent>`;
     - composite-root schema DDL should not implicitly mutate merged/derived schema state.
   - Impact:
     - root-composite schema behavior can diverge from Neo4j operational semantics and client expectations.
   - Required fix:
     - reject schema DDL on composite root unless a constituent target is resolved;
     - require/use constituent-scoped execution (`USE <composite>.<alias> ...`) for CREATE/DROP index flows.

14. **`SHOW INDEXES` must be constituent-scoped on composites (strict Neo4j semantics)**
   - Current behavior:
     - merged composite schema can present aggregate index metadata without constituent provenance.
   - Neo4j/Fabric-compatible expectation:
     - `SHOW INDEXES` / `SHOW FULLTEXT INDEXES` are schema-introspection commands over a single graph;
     - on composites, callers must target a constituent graph (for example `USE <composite>.<alias> SHOW INDEXES`);
     - composite-root aggregate index introspection is not a supported mode.
   - Required fix:
     - reject `SHOW INDEXES` / `SHOW FULLTEXT INDEXES` when executed on composite root without resolved constituent target;
     - lock behavior with Bolt + HTTP e2e tests for both reject and success paths.

## Current Parity Snapshot

| Area | Status | Notes |
|---|---|---|
| Direct `USE db` parsing | Complete | identifiers, quoted names, and strict resolution/validation are implemented |
| `USE composite.alias` | Complete | parser/executor and protocol entry paths resolve constituent targets before DB-not-found checks; covered in cypher/server/bolt tests |
| `USE graph.byName()` | Complete | implemented and covered in planner/use parsing tests |
| `USE graph.byElementId()` | Complete | implemented and covered in planner/use parsing tests |
| `USE` in subqueries | Complete | recursive CALL-subquery decomposition (including nested `CALL` blocks), strict in-scope target validation, and planner/e2e regression coverage |
| `USE` in union parts | Complete | implemented and covered in Fabric planner tests |
| Remote auth forwarding isolation | Complete | auth-aware cache keying + isolation tests in `pkg/fabric/remote_executor_test.go` |
| Composite-root schema DDL semantics | Complete | composite root rejects CREATE/DROP INDEX, CREATE/DROP CONSTRAINT with Neo4j-compatible errors; constituent-scoped DDL works via `USE <composite>.<alias>` |
| Composite index introspection semantics | Complete | `SHOW INDEXES`/`SHOW FULLTEXT INDEXES`/`SHOW CONSTRAINTS` rejected on composite root; constituent-scoped introspection works |
| Many-read / one-write enforcement | Complete | enforced in Fabric transaction coordinator and covered by tx tests |
| Distributed commit/rollback safety | Complete | compensation rollback on partial commit failure with regression test coverage |
| Bolt ROUTE behavior | Complete | routing table payload populated and role/address content assertions added |
| Bolt auth-aware remote composite routing | Complete | auth-aware storage bootstrap verified in Bolt tests |
| Bolt explicit tx multi-db correctness | Complete | DB-scoped explicit tx consistency covered in Bolt tests |
| HTTP explicit tx ownership/isolation | Complete | owner-bound tx sessions + cross-caller reuse rejection e2e test |
| HTTP explicit tx graph-level authorization | Complete | effective target graph auth enforced and covered in explicit-tx e2e test |
| HTTP end-to-end Fabric semantics | Complete | both native `USE` and `:USE` flows are supported and tested |
| Coverage for new Fabric API surface | Complete | schema/introspection, explicit tx (local+remote participants), ownership/auth checks, and composite stats/search parity are covered with deterministic tests and CI gate |

## Tested-vs-Untested Conclusion

### Implemented and tested well enough to count as materially covered

- Basic catalog/location/result/fragment plumbing
- Simple `USE <graph>` parsing
- `CALL { USE ... }` planning for the narrow supported subset
- many-read / one-write guardrail at transaction-open time
- auth-token forwarding through HTTP and Cypher-level `USE` resolution
- HTTP multi-database routing and shell-style `:USE` workflows
- composite metadata commands and several end-to-end composite flows

### Verified Completion Snapshot (Current)

The items below were re-verified against current code and tests after the strict-semantics pass.

- [x] `USE graph.byName(...)` support implemented and covered (`pkg/fabric/planner_test.go`, `pkg/cypher/executor_use.go`).
- [x] `USE graph.byElementId(...)` support implemented and covered (`pkg/fabric/planner_test.go`, `pkg/cypher/executor_use.go`).
- [x] `USE` in subqueries completed beyond first-level `CALL { USE ... }`: recursive nested decomposition, subquery target validation, and execution regressions (`pkg/fabric/planner.go`, `pkg/fabric/planner_test.go`, `pkg/cypher/fabric_execution_integration_test.go`).
- [x] UNION-part `USE` planning implemented and covered (`pkg/fabric/planner_test.go`).
- [x] Remote executor auth-cache isolation implemented and covered (`pkg/fabric/remote_executor.go`, `pkg/fabric/remote_executor_test.go`).
- [x] Remote executor cache concurrency safety implemented and covered (`pkg/fabric/remote_executor.go`, `pkg/fabric/remote_executor_test.go`).
- [x] Distributed commit compensation/atomicity protection implemented and covered (`pkg/fabric/transaction.go`, `pkg/fabric/transaction_test.go`).
- [x] Bolt explicit transaction DB-scoped consistency enforced and covered (`pkg/bolt/server.go`, `pkg/bolt/server_fabric_gaps_test.go`, `pkg/bolt/server_test.go`).
- [x] HTTP explicit transaction owner binding/isolation implemented and covered at manager level (`pkg/txsession/manager.go`, `pkg/txsession/manager_test.go`).
- [x] Bolt auth-aware storage bootstrap implemented and covered (`pkg/bolt/server.go`, `pkg/bolt/server_fabric_gaps_test.go`).
- [x] Remote Bolt read/write access-mode selection implemented and unit-covered (`pkg/storage/remote_engine.go`, `pkg/storage/remote_engine_test.go`).

### Remaining TBD Coverage

- [x] **TBD:** Add stronger Bolt `ROUTE` compatibility assertions beyond smoke-level shape checks:
  - validate role distribution payload fields
  - validate non-empty/compatible routing-table member structure
- [x] **TBD:** Add explicit server e2e test proving cross-caller HTTP tx-id reuse is rejected end-to-end (currently covered strongly in manager-level owner-binding tests).
- [x] **TBD:** Add explicit server e2e test where path-db authorization differs from effective in-statement graph target inside explicit tx, to prove graph-target-aware auth at API boundary.

## Current Judgment

- **Implemented API surface:** near-complete. Composite schema DDL, index introspection, DROP INDEX, plain query rejection, and constituent addressing gaps are all resolved.
- **Behavioral parity for fixed gaps:** strong. Composite-root correctly rejects schema DDL, SHOW INDEXES/CONSTRAINTS, and plain data queries. Constituent-scoped operations work correctly via `USE <composite>.<alias>`.
- **Coverage quality:** strong overall with deterministic unit/e2e tests covering composite schema/query semantics, remote participant explicit tx handle lifecycle, deterministic composite stats provenance, and CI parity gating.

## Verification Update (2026-03-14)

Validated in this branch:

- Deterministic pass of key cypher regressions and doc-example tests (`-count=5`):
  - `TestDropIndex_BacktickQuoted`
  - `TestDropIndex_RealExecution`
  - `TestExecuteUnsupportedQuery`
  - `TestDocExample_CompositeRootSchemaRejected`
  - `TestDocExample_DropIndexIfExistsOnConstituent`
  - `TestDocExample_PlainQueryOnCompositeRejected`
  - `TestCompositeDatabase_EndToEnd`
  - `TestCompositeDatabase_ComplexQuery`
  - `TestCompositeDatabase_QueryWithRelationships`
- Deterministic pass of new Bolt/storage/multidb tests (`-count=5`):
  - `pkg/bolt/server_composite_schema_test.go`
  - `pkg/storage/schema_drop_index_test.go`
  - `pkg/multidb/composite_exists_test.go`
- Package suite pass:
  - `go test ./pkg/cypher ./pkg/storage ./pkg/multidb ./pkg/bolt ./pkg/fabric ./pkg/txsession -count=1`

Verified compatibility improvements:

- Cache invalidation for schema DDL now prevents stale `SHOW INDEXES` results.
- Composite-root schema DDL/introspection rejection is enforced with constituent-target guidance.
- `DROP INDEX` now executes real drop semantics with `IF EXISTS` no-op behavior on missing index.
- Bolt-side dotted `composite.alias` existence checks now avoid premature `DatabaseNotFound`.

Status after implementation pass:

- [x] Cross-protocol HTTP e2e for composite schema/index flows is now covered.
- [x] Composite-root search endpoint parity is enforced in server/search paths (composite roots rejected with explicit constituent guidance).
- [x] Full explicit distributed transaction parity for remote participants is implemented end-to-end for explicit transaction handle lifecycle (open/query/commit/rollback through real remote tx handles).
- [x] HTTP e2e composite constraint semantics are covered (`USE <composite>.<alias>` CREATE/SHOW/DROP CONSTRAINT).
- [x] Deterministic provenance fields are included for constituent-aggregated composite `/db/{composite}` stats output.
- [x] CI composite parity gate added (`scripts/ci-composite-parity.sh`, wired in `.github/workflows/ci.yml`).

## Required E2E Parity Changes (Strict Neo4j Compatibility)

The following behavior is required to declare Fabric parity for composite databases.

1. **Constituent-local schema semantics must be enforced**
   - Reject schema DDL on composite root unless the resolved target is a constituent graph.
   - Require `USE <composite>.<alias>` (or equivalent resolved graph reference) for:
     - `CREATE INDEX` / `DROP INDEX`
     - `CREATE CONSTRAINT` / `DROP CONSTRAINT`
   - Remove any implicit merged-composite schema mutation path.

2. **Index introspection semantics must be strict and non-ambiguous**
   - `SHOW INDEXES` and `SHOW FULLTEXT INDEXES` must require constituent scope.
   - Composite-root index introspection must be rejected.
   - Aggregate merged composite index catalogs are not authoritative schema and must not be presented as such.

3. **`DROP INDEX` must be real, never no-op on accepted syntax**
   - Accepted `DROP INDEX` statements must execute against the targeted constituent schema.
   - Failures must return Neo4j-compatible errors (not silent success/no-op).

4. **Composite query model must align with Fabric routing semantics**
   - Do not treat composite root as implicit union graph for plain `MATCH`.
   - Require explicit graph targeting semantics consistent with Neo4j Fabric.

5. **Graph resolution order must run before DB-not-found rejection**
   - `composite.alias`, `graph.byName(...)`, and `graph.byElementId(...)` must resolve through Fabric catalog/use-evaluator flow before top-level database existence checks.
   - This applies across Bolt and HTTP entry paths.

6. **Explicit distributed transaction semantics must be end-to-end real**
   - Open and track real subtransactions per participating shard/remote.
   - Commit/rollback must execute through real shard transaction handles (not no-op callbacks).
   - Maintain strict many-read/one-write enforcement.

7. **Composite search behavior must be provenance-correct**
   - Never create a synthetic namespaced “composite search service” fallback.
   - Aggregate search and stats explicitly from constituent services.
   - `/db/{composite}` search/stat outputs must be deterministic and provenance-correct.

## Concrete Parity Implementation Steps

The items below are the concrete implementation plan to reach strict Neo4j/Fabric parity for composite schema/search semantics.

1. **Classify effective graph target before schema/search execution**
   - Add a single resolver used by Bolt + HTTP + Cypher execution entry points:
     - input: session DB, optional `USE` target (`composite.alias`, `graph.byName`, `graph.byElementId`)
     - output: `{kind: standard|composite-root|constituent, resolvedDatabase, compositeName, alias}`
   - Run this resolver before any top-level DB-not-found rejection.
   - Wire in:
     - `pkg/bolt/server.go`
     - `pkg/server/server_db.go`
     - `pkg/server/server_nornicdb.go`
     - `pkg/cypher/executor_use.go`

2. **Enforce constituent-only schema operations**
   - Reject schema ops when target classification is `composite-root`:
     - `CREATE/DROP INDEX`, `SHOW INDEXES`, `SHOW FULLTEXT INDEXES`
     - `CREATE/DROP CONSTRAINT`
   - Allow only when target classification is `constituent`.
   - Return Neo4j-style client semantic errors (not generic not-found/no-op).
   - Wire in:
     - `pkg/cypher/executor_show.go`
     - `pkg/cypher/schema.go`
     - schema command dispatch in executor paths.

3. **Remove merged composite schema as authoritative for DDL/introspection**
   - Keep merged schema only for internal compatibility where needed, but do not use it for:
     - schema writes
     - schema introspection on composite root
   - If session DB is composite root and no constituent target is resolved, reject.
   - Scope/guard:
     - `pkg/storage/composite_engine.go` (`GetSchema()` consumers in Cypher/server paths).

4. **Fix `DROP INDEX` semantics**
   - Ensure accepted `DROP INDEX` always executes against resolved constituent schema manager.
   - Remove silent success/no-op on accepted syntax.
   - Align error behavior with Neo4j:
     - missing index -> explicit client schema error
     - invalid target -> constituent-required semantic error.

5. **Fix composite search service behavior**
   - Do not create/search/index a service keyed by composite-root DB using composite storage.
   - For composite-root search endpoints:
     - reject as unsupported on composite root;
     - require explicit constituent graph target (`USE <composite>.<alias>`) for search/index-related operations.
   - No synthetic fallback namespace indexing under composite root.
   - Wire in:
     - `pkg/server/server_nornicdb.go`
     - `pkg/nornicdb/search_services.go`
     - `pkg/nornicdb/db_admin.go`

6. **Implement deterministic constituent-aggregated stats**
   - `/db/{composite}` stats/search metadata must be derived from constituent services only.
   - Require stable ordering and provenance fields (`constituent`, `database`) for aggregated outputs.
   - Ensure cache behavior does not hide constituent transitions.

7. **Lock behavior with cross-protocol e2e parity suite**
   - Bolt and HTTP both must validate:
     - `USE composite.alias SHOW/CREATE/DROP INDEX` success
     - composite-root `SHOW/CREATE/DROP INDEX` rejection
     - composite-root constraints rejection unless constituent-targeted
     - no composite-root search index build fallback
     - deterministic constituent-derived search/stats outputs.

8. **Compatibility guardrails in CI**
   - Add a dedicated parity target that must pass before merge:
     - `go test ./pkg/bolt -run Fabric|Composite`
     - `go test ./pkg/server -run Composite|Search|Index`
     - `go test ./pkg/cypher -run Composite|Index|Constraint|Show`
   - Gate on failure for any reintroduction of composite-root schema/search behavior.

## Minimum Parity Test Gate (Must Pass)

1. Bolt and HTTP e2e for `USE <composite>.<alias>`:
   - `SHOW INDEXES`
   - `CREATE INDEX` / `DROP INDEX`
2. Composite-root schema DDL rejection tests:
   - index and constraint DDL rejected without resolved constituent target.
3. `SHOW INDEXES` parity tests:
   - constituent-scoped success;
   - composite-root rejection.
4. Composite explicit transaction tests:
   - cross-shard reads;
   - single-shard write;
   - commit/rollback durability and correctness.
5. Composite search/stat tests:
   - prove no phantom composite namespace indexing;
   - prove deterministic constituent-derived stats.

## Tracked TODOs

- [x] Add deep Bolt `ROUTE` routing-table content compatibility assertions.
- [x] Add cross-caller HTTP explicit tx reuse rejection e2e test.
- [x] Add explicit tx graph-target authorization mismatch e2e test.
- [x] Fix protocol entry-path ordering so constituent graph resolution runs before top-level DB existence checks.
  - Bolt: `constituentAwareExists()` helper in `pkg/bolt/server.go` checks `ConstituentExistsChecker` interface before rejecting.
  - HTTP: `dbManager.ExistsOrIsConstituent()` in `pkg/server/server_db.go` accepts dotted `composite.alias` patterns.
  - Multidb: `ExistsOrIsConstituent()` in `pkg/multidb/composite.go` resolves standard DBs, composites, dotted constituents, and aliases.
- [x] Enforce Neo4j-compatible schema scope: reject `CREATE/DROP INDEX` on composite root without resolved constituent target.
  - `isCompositeRoot()` guard in `pkg/cypher/schema.go` (`executeSchemaCommand`, `executeDropIndex`).
  - Returns `Neo.ClientError.Statement.NotAllowed` with actionable error message.
- [x] Enforce strict Neo4j-compatible composite introspection: `SHOW INDEXES` / `SHOW FULLTEXT INDEXES` require resolved constituent target and must reject composite-root execution.
  - Guards in `pkg/cypher/executor_show.go` (`executeShowIndexes`, `executeShowConstraints`).
- [x] Extend strict schema-scope enforcement to constraints: reject `CREATE/DROP CONSTRAINT` on composite root without resolved constituent target.
  - Covered by `executeSchemaCommand` composite-root guard which handles both CREATE and DROP CONSTRAINT.
- [x] Remove accepted `DROP INDEX` no-op behavior; execute real constituent drop and return Neo4j-compatible errors.
  - `executeDropIndex()` in `pkg/cypher/schema.go` calls `storage.SchemaManager.DropIndex()`.
  - `DropIndex()` in `pkg/storage/schema.go` searches all index types (property, composite, fulltext, vector, range) with persist rollback.
  - Missing index returns `index "X" does not exist`; `IF EXISTS` variant is a silent no-op.
  - Query cache invalidation added on successful schema DDL to prevent stale SHOW INDEXES results.
- [x] Tighten composite query model: reject implicit-union plain `MATCH` on composite root unless explicitly graph-targeted.
  - Guard in `pkg/cypher/executor.go` after USE clause handling rejects plain data queries (MATCH, CREATE, MERGE, DELETE, SET, REMOVE, RETURN) on composite root.
  - `isCompositeAllowedCommand()` permits system/admin commands (SHOW DATABASES, CREATE DATABASE, schema DDL which has its own guards, etc.).
- [x] Remove synthetic composite search-service/index fallback in server execution paths.
  - Partial: `pkg/cypher/executor_use.go` stops inheriting parent search service for composite-scoped executors.
  - Completed path hardening: `pkg/server/server_db.go` skips attaching search services for composite-root executors; `pkg/server/server_nornicdb.go` rejects composite-root search/rebuild/similar requests.
  - Remaining: deterministic constituent provenance fields for aggregated composite stats/search metadata.
- [x] Add e2e tests for `USE <composite>.<alias>` followed by `SHOW INDEXES`, `CREATE INDEX`, and `DROP INDEX`.
  - Partial: cypher + bolt unit/integration coverage exists.
  - Remaining: HTTP `/db/{composite}/tx/commit` e2e coverage for the same flows.
  - Cypher-level tests in `pkg/cypher/composite_schema_test.go`:
    - `TestConstituent_CreateIndex_Success`, `TestConstituent_ShowIndexes_Success`, `TestConstituent_DropIndex_Success`, `TestConstituent_ShowConstraints_Success`
    - `TestDocExample_CreateIndexOnConstituent`, `TestDocExample_ShowIndexesOnConstituent`, `TestDocExample_DropIndexOnConstituent`, `TestDocExample_DropIndexIfExistsOnConstituent`
    - `TestDocExample_CompositeRootSchemaRejected` (6 sub-tests: CREATE/DROP INDEX, CREATE/DROP CONSTRAINT, SHOW INDEXES, SHOW CONSTRAINTS)
    - `TestDocExample_PlainQueryOnCompositeRejected`
  - Bolt-level tests in `pkg/bolt/server_composite_schema_test.go`.
  - Multidb-level tests in `pkg/multidb/composite_exists_test.go`.
  - Storage-level tests in `pkg/storage/schema_drop_index_test.go`.
- [x] Add cross-protocol e2e tests for index semantics (HTTP path):
  - HTTP: `/db/{composite}/tx/commit` with in-statement `USE composite.alias` for SHOW/CREATE/DROP INDEX flows.
- [x] Add cross-protocol e2e tests for constraint semantics (HTTP path):
  - HTTP: `/db/{composite}/tx/commit` with in-statement `USE composite.alias` for CREATE/DROP CONSTRAINT flows.
- [x] Complete explicit distributed transaction coordinator semantics end-to-end for remote participants (real subtx handles for open/commit/rollback across remote shards).
  - Local constituent handles are now real and covered by commit/rollback durability tests in `pkg/cypher/composite_transaction_fabric_test.go`.
  - Note: many-read/one-write enforcement and compensation rollback are implemented. Full global 2PC remains an architectural limitation.
- [x] Add deterministic provenance fields for any constituent-aggregated composite stats/search outputs.
- [x] Add CI parity gate for composite schema/search semantics to block regressions.
