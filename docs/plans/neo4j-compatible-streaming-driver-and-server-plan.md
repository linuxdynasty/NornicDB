# Neo4j-Compatible Streaming Driver + End-to-End Streaming Execution Plan

## Objective

Implement a NornicDB client/ORM surface that is Neo4j-driver compatible while enabling true incremental query streaming:

- Start consuming query input immediately.
- Start emitting result rows as soon as first row is available.
- Preserve Neo4j/Bolt transactional semantics and protocol expectations.
- Avoid full-result materialization in hot query paths.

This plan covers both client-facing API and server execution architecture changes.

## Scope

In scope:

- Neo4j-compatible client wrapper package (driver/session/tx/result-style API).
- Bolt server streaming path (`RUN`/`PULL` incremental delivery).
- HTTP streaming endpoint for incremental results (SSE/chunked JSON).
- Internal executor/fabric row-stream interface (replace full `[][]interface{}` in streaming paths).
- Backpressure, cancellation, timeout, and memory bounds.

Out of scope:

- Changing Cypher language semantics.
- Relaxing transaction guarantees.
- Introducing non-deterministic partial write visibility before commit.

## Compatibility Contract (Must-Have)

1. Neo4j API parity at driver-facing surface:
- `Driver`, `Session`, `Transaction`, `Result` workflow.
- Incremental row iteration identical in behavior for success/error/cancel.

2. Transaction behavior:
- Reads stream immediately.
- Writes are not considered durable until commit acknowledgement.
- On tx failure, no success status is emitted for pending rows beyond protocol guarantees.

3. Fabric/composite:
- Maintain many-read/one-write enforcement.
- Preserve strict `USE`/composite scoping rules already implemented.

## Current Gaps

1. Executor/fabric still relies on materialized `Rows [][]interface{}` in multiple paths.
2. Fabric APPLY/UNION flows do in-memory row aggregation.
3. HTTP `/db/{db}/tx/commit` returns only completed result payload.
4. No unified internal `RowStream` abstraction with backpressure.

## Architecture Target

### A. Internal Streaming Primitive

Add a streaming contract in Cypher/Fabric layers:

- `type Row struct { Values []interface{} }`
- `type RowStream interface { Next(ctx) (Row, error); Columns() []string; Close() error }`
- Errors are terminal and surfaced once.
- `io.EOF` indicates stream completion.

### B. Execution Model

- Planner stays unchanged.
- Executors return `RowStream` where possible.
- Materialized adapters remain temporarily for compatibility but are not used in optimized streaming path.

### C. Protocol Mapping

1. Bolt:
- `RUN` creates execution stream handle.
- `PULL n` drains up to `n` rows.
- Respect fetch-size and client pull pacing.

2. HTTP:
- Add `/db/{db}/tx/stream` endpoint:
  - chunked JSON lines or SSE events (`columns`, `row`, `summary`, `error`).
  - compatible fallback remains `/tx/commit`.

## Implementation Phases

## Phase 0: Baseline & Safety Gates

Files:
- `pkg/bolt/*`
- `pkg/server/*`
- `pkg/cypher/*`
- `pkg/fabric/*`

Tasks:
- Add benchmarks for:
  - first-row latency
  - full-query latency
  - peak memory for large result sets
- Add pprof captures for representative query classes.
- Add regression suites for transaction correctness under cancellation.

Acceptance:
- Baseline numbers captured in `docs/performance/`.
- CI benchmarks runnable and stable.

## Phase 1: Internal RowStream Core

Files:
- `pkg/cypher/types.go`
- `pkg/cypher/executor*.go`
- `pkg/fabric/result.go`

Tasks:
- Introduce `RowStream` and adapters:
  - `MaterializedToStreamAdapter`
  - `StreamToMaterializedAdapter` (temporary bridge)
- Ensure existing callers compile unchanged initially.
- Add context cancellation propagation through stream operations.

Acceptance:
- Unit tests validate `Columns/Next/Close` contracts.
- No semantic changes to existing non-stream paths.

## Phase 2: Cypher Executor Streaming Paths

Files:
- `pkg/cypher/executor.go`
- `pkg/cypher/match*.go`
- `pkg/cypher/clauses*.go`
- `pkg/cypher/executor_subqueries.go`

Tasks:
- Implement streaming for read-heavy clauses first:
  - `MATCH ... RETURN`
  - `WITH/UNWIND/ORDER/LIMIT` where possible with bounded buffering
- Keep deterministic ordering behavior identical.
- For unsupported shapes, fallback to materialized adapter explicitly.

Acceptance:
- First-row latency improved for large scans.
- Memory usage reduced for streaming-eligible queries.
- Existing semantic tests pass unchanged.

## Phase 3: Fabric Streaming Execution

Files:
- `pkg/fabric/executor.go`
- `pkg/fabric/local_executor.go`
- `pkg/fabric/remote_executor.go`
- `pkg/cypher/executor_fabric.go`

Tasks:
- Replace `ResultStream{Rows [][]interface{}}` dependency in APPLY/UNION pipelines with row iterators.
- Implement bounded join buffering for correlated APPLY:
  - per-batch key extraction
  - streaming grouped join emission
- Preserve many-read/one-write tx guardrails.
- Ensure no full constituent scan materialization for row-delivery path.

Acceptance:
- Fabric queries with large intermediates stream rows progressively.
- No OOM behavior on large composite joins in integration tests.

## Phase 4: Bolt Incremental Delivery

Files:
- `pkg/bolt/server.go`
- `pkg/bolt/session*.go`

Tasks:
- Bind query execution handle to session statement state.
- Implement `RUN` + incremental `PULL` backed by `RowStream`.
- Honor pull-size, cancellation, and tx close semantics.
- Ensure consistent summary/metadata after stream completion.

Acceptance:
- Neo4j driver clients consume rows incrementally.
- Fetch-size tuning affects memory and latency as expected.

## Phase 5: HTTP Streaming Endpoint

Files:
- `pkg/server/server_db.go`
- `pkg/server/server_router.go`
- `pkg/server/*_test.go`

Tasks:
- Add `/db/{db}/tx/stream` endpoint with chunked output.
- Event model:
  - `columns`
  - `row`
  - `summary`
  - `error`
- Integrate auth, timeout, and request cancellation.

Acceptance:
- Large result queries stream to HTTP clients without full buffering.
- Existing `/tx/commit` behavior remains unchanged.

## Phase 6: Neo4j-Compatible Wrapper Driver/ORM

Files:
- `pkg/client/neo4jcompat/*` (new)
- `pkg/client/neo4jcompat_test/*` (new)

Tasks:
- Expose API surface mirroring common Neo4j driver usage:
  - `NewDriver`
  - `NewSession`
  - `Run`, `ReadTransaction`, `WriteTransaction`
  - `Result.Next()/Record()/Err()/Consume()`
- Internally use Bolt incremental stream path.
- Add ergonomic ORM helper layer on top (optional query mapping helpers), without changing core driver semantics.

Acceptance:
- Drop-in sample app compatibility for core workflows.
- Streaming verified from first row with large result sets.

## Phase 7: Hardening, Docs, CI Gates

Files:
- `docs/user-guides/*`
- `docs/api-reference/*`
- `.github/workflows/*`

Tasks:
- Add user docs:
  - streaming over Bolt
  - streaming over HTTP
  - transaction semantics for streamed writes
  - cancellation and timeout behavior
- Add CI parity gate:
  - semantic compatibility suite
  - streaming latency/memory guardrails

Acceptance:
- Documentation reflects actual behavior and limitations.
- CI fails on semantic regressions or major streaming performance regressions.

## Testing Strategy (Required)

1. Unit tests:
- stream contract (`Next`, `Close`, error propagation)
- backpressure and cancellation
- semicolon and parser edge cases unaffected

2. Integration tests:
- large payload inserts and large result scans
- composite/fabric streaming joins
- explicit tx commit/rollback with streamed reads/writes

3. Protocol tests:
- Bolt incremental pull behavior
- HTTP stream framing correctness

4. Determinism tests:
- stable ordering (when query specifies it)
- no duplicate/missing row under concurrent pulls

## Performance Targets

Initial targets for streaming-eligible queries:

- First-row latency: >= 50% improvement vs materialized baseline.
- Peak memory: >= 40% reduction on large scans.
- Throughput: no regression > 10% for full-result delivery.

## Risks and Mitigations

1. Risk: semantic drift in complex query shapes.
- Mitigation: fallback to materialized adapter until stream path proven for that shape.

2. Risk: tx state inconsistencies under cancellation.
- Mitigation: strict session state machine tests and forced cancel chaos tests.

3. Risk: Fabric APPLY join complexity.
- Mitigation: bounded batch join pipeline + deterministic integration corpus.

## Deliverables

1. Streaming-capable internal execution path with bounded memory.
2. Bolt incremental row delivery compatible with Neo4j client expectations.
3. HTTP streaming endpoint for non-Bolt consumers.
4. Neo4j-compatible wrapper driver/ORM package.
5. Comprehensive docs + CI performance/compatibility gates.

## Definition of Done

All phases complete, and:

- No known semantic parity gaps for supported streaming query classes.
- No full-result mandatory buffering in primary Bolt streaming path.
- Composite/fabric streaming joins validated under large datasets.
- Documentation and CI gates in place and green.
