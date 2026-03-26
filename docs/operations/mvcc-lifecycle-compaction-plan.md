# MVCC Lifecycle and Compaction Control Plan

**Status:** Partially Implemented and Verified  
**Owner:** Storage/Engine  
**Date:** 2026-03-25

## Verification Legend

- [x] Implemented and covered by current tests
- [~] Implemented in part, or implemented but not fully covered/finished
- [ ] Not implemented yet

## Current Verification Summary

- [x] Core lifecycle manager exists and is wired through storage wrappers, DB admin methods, Heimdall metrics, and server admin endpoints
- [x] Operator admin UI exists under Security and drives lifecycle inspection and control end to end
- [x] Core manager package tests are currently passing
- [x] Server lifecycle admin route tests are currently passing
- [x] Real-engine MVCC churn and tombstone compaction tests exist in the storage package
- [~] Advanced operator semantics from the original plan are only partially implemented
- [ ] Performance non-regression gate from the original plan has not been completed

## 1. Purpose

This document defines a single cohesive architecture to control MVCC history growth, prevent compaction starvation, and preserve snapshot correctness under sustained read/write load.

## 2. Goals

1. Bound storage growth while preserving current temporal semantics.
2. Make retention pressure actionable, not just observable.
3. Avoid global compaction stalls caused by long-running readers.
4. Provide predictable operator behavior under normal and emergency pressure.
5. Ensure fairness across tenants/workloads.

## 3. Non-Goals

1. No breaking changes to existing snapshot-read semantics.
2. No removal of retained-floor behavior.
3. No mandatory tiered-history rollout in this phase.

## 4. Architecture

Verification:

- [x] Introduce one subsystem: `MVCCLifecycleManager`
- [x] Centralize reader tracking, watermark computation, prune planning, fenced apply, metrics, and pressure policy in the manager/runtime package
- [x] Keep existing APIs (`PruneMVCCVersions`, `RebuildMVCCHeads`) as delegating wrappers

1. Introduce one subsystem: `MVCCLifecycleManager`.
2. Centralize in manager:

- reader tracking
- watermark computation
- prune planning
- apply with version fences
- metrics and pressure policy

3. Keep existing APIs (`PruneMVCCVersions`, `RebuildMVCCHeads`) as delegating wrappers.

## 5. Core Data and Safety Model

Verification:

- [x] `safe_floor` is computed via minimum retained bounds and monotonic floor advancement helpers
- [x] Floor can only advance, never regress
- [x] Pruning is constrained by the retained floor, and explicit chain-hard-cap fallback behavior is implemented
- [x] Snapshot reads below retained floor still return not found

1. `safe_floor` per logical key is computed as:

```text
safe_floor = min(
  oldest_reader_version,
  ttl_bound_version,
  max_versions_bound_version
)
new_floor = monotonic_max(previous_floor, safe_floor)
```

2. Floor can only advance, never regress.
3. Pruning and chain-cap actions are only allowed above `safe_floor`.
4. If snapshot version is below floor, return not found (current contract).

## 6. Reader Watermark Model

Verification:

- [x] Global active-reader boolean has been replaced by an active reader registry in lifecycle manager code
- [x] Reader records include reader ID, snapshot version, start time, and namespace
- [x] Oldest reader version and oldest reader age are computed and exposed
- [x] Watermark remains runtime state rather than persisted state
- [x] Correctness still relies on persisted floor/head invariants, not persisted watermark state

1. Replace global active-reader boolean with active reader registry.
2. Track per reader:

- reader ID
- snapshot version
- start timestamp
- tenant/namespace

3. Compute:

- `oldest_reader_version`
- `oldest_reader_ts`
- `oldest_reader_age_seconds`

4. Watermark is runtime state, not persisted.
5. Correctness relies on persisted floor/head invariants, not persisted watermark.

## 7. Planner and Apply Execution

Verification:

- [x] Planner reads persisted MVCC heads and version keyspace iteration to build immutable prune plans
- [~] Optional narrowing indexes are not present in the current implementation
- [x] Apply phase validates head-version fences before mutating storage
- [x] Fence mismatch handling skips the key, increments mismatch accounting, and backs off retries
- [~] Iterator-boundary fence checks and explicit snapshot-consistent iterator guarantees are not separately implemented beyond current head/version scan behavior

1. Planner source of truth:

- persisted MVCC head metadata
- MVCC version keyspace iterator
- optional narrowing indexes

2. Planner creates immutable run plan with per-key fences.
3. Apply phase checks version fence before mutation.
4. If fence mismatch:

- skip key
- increment stale-plan metric
- requeue for replan with backoff

5. Planner iteration guarantee:

- use snapshot-consistent iterator when available; otherwise enforce iterator-boundary fence checks.

### 7.1 Fence Retry and Invalid-Plan Rules

Verification:

- [x] Initial requeue delay and exponential backoff with jitter up to a capped maximum are implemented
- [x] Per-key retry limit per run is implemented
- [x] Cross-run retry budget within a rolling ten-minute window is implemented
- [x] Hot-contention cooldown is implemented after repeated fence mismatch
- [~] Infinite-loop prevention exists through backoff and cooldown, but the exact "second mismatch same-cycle" rule is not implemented as written

1. Requeue policy:

- initial requeue delay: 100ms
- exponential backoff with jitter up to 5s cap

2. Retry limits:

- per-key retries per run: 3
- per-key retries across runs: configurable default 20 within 10 minutes

3. Long-term invalidation:

- if retry budget exceeded, mark key as `hot-contention` for cooldown window (default 60s)
- continue servicing other keys (fairness preserved)

4. Infinite-loop prevention:

- no immediate same-cycle replan for same key after second mismatch
- all retries must pass through backoff queue

## 8. Work Prioritization and Fairness

Verification:

- [x] Priority scoring exists and includes debt, tombstone depth, and age signals
- [x] Cost model uses iterator seeks, value-log reads, and bytes rewritten/deleted proxies
- [x] Scheduler orders work by score-over-cost
- [x] Anti-starvation behavior exists through skip-count boosting and reserved slice handling
- [x] Namespace budgets and reserved work slices provide basic multi-tenant fairness controls

1. Priority score:

```text
priority = f(debt_bytes, tombstone_depth, key_hotness, key_age)
```

2. Cost model uses concrete proxies:

- iterator seeks
- value-log reads
- bytes rewritten/deleted

3. Scheduler maximizes debt-reduction-per-cost.
4. Anti-starvation:

- priority aging
- max skip count per key
- reserved slice for oldest unserved high-debt keys

5. Multi-tenant isolation:

- optional per-namespace lifecycle budget caps
- minimum guaranteed maintenance slice per namespace

## 9. Pressure Policy and Backpressure

Verification:

- [x] Pressure bands `normal`, `high`, and `critical` exist
- [x] Hysteresis windows for band transitions are implemented and tested
- [x] Long-snapshot admission tightening is implemented and client warning headers/fields are emitted
- [x] Pinned-bytes metric is implemented and feeds pressure evaluation
- [x] Pressure controller enforcement can reject long snapshots under pressure
- [~] Graceful cancel and hard-kill behavior for already-running snapshots is implemented for active transaction readers, but not yet for every broader shared session/query scope

1. Bands:

- normal
- high
- critical

2. Hysteresis for band transitions to avoid flapping.
3. Reactions:

- high: rate-limit new long snapshots, emit client warnings
- critical: reject new long snapshots, keep short snapshots

4. Pinned-bytes threshold policy is mandatory.
5. Metric with enforcement:

- `mvcc_bytes_pinned_by_oldest_reader`

6. Snapshot lifetime policy:

- configurable max snapshot lifetime
- graceful cancel first
- hard kill only under sustained critical pressure

### 9.1 Baseline Threshold Guidance

Verification:

- [x] Baseline thresholds exist in default lifecycle config
- [x] Enter/exit debounce windows exist in default lifecycle config

Default baseline (operator-tunable):

1. `high_enter`: `max(5 GiB, 0.15 * data_dir_free_space)`
2. `high_exit`: `0.8 * high_enter`
3. `critical_enter`: `max(20 GiB, 0.35 * data_dir_free_space)`
4. `critical_exit`: `0.8 * critical_enter`

Default debounce windows:

1. enter window: 30s sustained breach
2. exit window: 120s sustained below exit threshold

## 10. Snapshot Kill Semantics

Verification:

- [~] Transaction-scoped forced expiration is implemented, but broader query/session-scoped expiration is not
- [~] Safe cancel points now exist at transaction operation and commit boundaries, but not across every shared snapshot consumer
- [x] Deterministic forced-expiration error family is implemented for graceful cancel and hard expiration
- [x] Structured audit events for forced expiration are implemented

1. Scope kills per query/session/client only.
2. Cancel points occur at safe boundaries (no torn row/page semantics).
3. Return deterministic transient/resource-pressure error code.
4. Emit structured audit event for each forced expiration.

### 10.1 Session and Dependent Transaction Effects

Verification:

- [~] Shared-snapshot graceful-cancel semantics are partially implemented through transaction-scoped reader cancellation
- [~] Snapshot-scope hard-expiration behavior is partially implemented through transaction-scoped reader expiration
- [ ] Multiplexed-query forced-failure behavior is not implemented

1. If multiple queries share one snapshot/session:

- graceful cancel applies to that query first
- hard expiration applies to that snapshot/session scope only

2. Dependent transactions on other snapshots are unaffected.
3. If a client multiplexes many queries on one long-lived snapshot, all queries on that snapshot fail consistently after hard expiration with same error code family.

## 11. Extreme Churn Guardrails

Verification:

- [x] `max_chain_hard_cap` fallback is enforced in prune planning
- [x] Hard-cap enforcement remains bounded by `safe_floor` invariants
- [x] Emergency mode can activate from debt-growth slope under critical pressure
- [x] Emergency mode increases compaction budget, tightens snapshot lifetime, and adds separate emergency prioritization logic
- [x] Emergency adjustments honor configured resource ceilings

1. Add `max_chain_hard_cap` fallback.
2. Hard cap is bounded by `safe_floor` invariants.
3. Emergency mode trigger on debt-growth slope.
4. Emergency mode behavior:

- increase compaction budget
- tighten long-snapshot admission
- prioritize highest debt-yield keys

5. Emergency mode must honor global resource ceilings.

## 12. Resource Ceilings

Verification:

- [~] Runtime and IO ceilings are enforced inside each cycle, and CPU share is throttled on a best-effort basis, but full hard CPU enforcement is still not complete
- [x] Limit fields exist for max CPU share, IO budget, and runtime per cycle
- [x] Emergency mode budget adjustments are capped by those limits

1. Lifecycle manager must enforce hard max resource share.
2. Define limits:

- max CPU share
- max IO budget per interval
- max runtime per cycle

3. Emergency mode cannot exceed these limits.

## 13. Metrics

Verification:

- [x] Required pressure metrics are exposed in lifecycle status
- [~] Several lifecycle metrics are defined, exposed, and actively populated from live prune plans, but not all planned metrics are fully populated yet
- [x] Global aggregate metrics are exposed
- [~] Per-namespace metrics exist for aggregated debt and prunable/pruned bytes, but not every planned metric is tracked per namespace

1. Required pressure metrics:

- `mvcc_bytes_pinned_by_oldest_reader`
- `mvcc_compaction_debt_bytes`
- `mvcc_compaction_debt_keys`

2. Required lifecycle metrics:

- `mvcc_active_snapshot_readers`
- `mvcc_oldest_reader_age_seconds`
- `mvcc_prunable_bytes_total`
- `mvcc_pruned_bytes_total`
- `mvcc_tombstone_chain_max_depth`
- `mvcc_floor_lag_versions`
- `mvcc_prune_run_duration_seconds`
- `mvcc_prune_run_keys_scanned_total`
- `mvcc_prune_stale_plan_skips_total`

3. Expose all metrics per namespace and global aggregate.

### 13.1 Metrics Cadence and Overhead Controls

Verification:

- [x] Debt sampling fraction and periodic full-scan controls are implemented in the planner
- [x] Separate 10s/60s rollup intervals are implemented in lifecycle status output
- [x] Per-key debug export with capped cardinality is implemented through admin debt inspection

1. Per-key debt sampling:

- default sample fraction: 5%
- full scan every N cycles (default 20)

2. Aggregation interval:

- 10s rollup for hot counters
- 60s rollup for debt histograms

3. Export model:

- per-namespace aggregates always on
- per-key detail behind debug flag and capped cardinality

## 14. API and Operator Surface

Verification:

- [x] Lifecycle status endpoint exists and returns pressure, debt, reader, and last-run data
- [x] Manual operations exist for prune-now and pause/resume
- [x] Runtime lifecycle schedule control exists through `POST /admin/databases/{db}/mvcc/schedule`
- [x] Inspect top N debt keys is implemented through `GET /admin/databases/{db}/mvcc/debt?limit=N`
- [x] Client warning headers/fields for pressure-induced degradations are implemented
- [x] Admin UI exists for database selection, runtime controls, debt inspection, reader inspection, and confirmation-gated lifecycle actions

1. Add lifecycle status endpoint:

- pressure band
- oldest reader
- pinned bytes
- debt bytes/keys
- last run summary
- skipped due to fence mismatch

2. Add manual operations:

- trigger prune now
- pause/resume lifecycle manager
- inspect top N debt keys

3. Add client warning headers/fields for pressure-induced degradations.

## 15. Replication and Coordination Scope

Verification:

- [x] Current lifecycle behavior is node-local
- [x] Reader watermark state is node-local runtime state
- [~] Replication-safe compaction behavior is the working assumption, but there is not dedicated verification for coordinated replicated lifecycle behavior in this phase
- [ ] Control-plane coordinated pressure hints are not implemented

1. Default model: lifecycle decisions are local to each node.
2. Watermark is node-local because active readers are node-local runtime state.
3. In replicated deployments:

- compaction actions must remain log/order-safe for local store invariants
- no cross-node watermark coordination required in this phase

4. Optional future mode:

- control-plane coordinated pressure hints only (not shared watermark correctness).

## 16. Implementation Sequence

Verification:

- [x] Manager scaffolding and config exist
- [x] Reader registry and watermark computation exist
- [x] Planner/apply with version fences exist
- [x] Prioritization, fairness, and cost model exist in initial form
- [x] Pressure bands and admission enforcement exist
- [x] Metrics and status endpoint exist
- [x] Emergency mode and ceiling-aware budget adjustment exist in initial form
- [x] Legacy maintenance methods delegate into lifecycle support
- [ ] Feature-flagged rollout path was not implemented; current behavior is config-driven instead

1. Create manager scaffolding and config.
2. Add reader registry and watermark computation.
3. Implement planner/apply with version fences.
4. Add prioritization, fairness, and cost model.
5. Add pressure bands and enforcement actions.
6. Add metrics and status endpoint.
7. Add emergency mode and hard ceilings.
8. Wire legacy maintenance methods to manager.
9. Enable by feature flag, then default-on.

## 17. Testing Plan

Verification:

- [x] Unit tests exist for floor monotonicity, fence correctness, scheduler behavior, hysteresis transitions, and policy triggers
- [~] Integration coverage exists for active-reader tombstone compaction, high-churn prune bounding, and operator debt inspection, but not all planned contention scenarios are covered
- [~] Reliability coverage exists in part, but restart-with-history and watermark reset scenarios are not fully covered as described here
- [ ] Performance tests for lifecycle debt-reduction rate and write-latency impact are not complete

1. Unit tests:

- floor monotonicity
- fence correctness
- scoring and fairness behavior
- hysteresis transitions
- policy action triggers

2. Integration tests:

- one long reader + high churn
- many staggered medium readers with slow watermark movement
- mixed-tenant contention with quotas
- stale-plan race under concurrent writes

3. Reliability tests:

- restart with active lifecycle history
- watermark reset behavior
- emergency mode enter/exit under sustained pressure

4. Performance tests:

- debt reduction rate
- bounded latency impact on writes
- no runaway growth under configured policy

## 18. Acceptance Criteria

Verification:

- [x] Storage growth is bounded under the tested churn scenarios currently covered by storage tests
- [x] No known snapshot correctness regression was introduced in the covered lifecycle changes
- [x] Compaction can continue making progress with active-reader-aware behavior in tested cases
- [~] Operators can inspect pressure and debt, but some planned explanatory metrics are still unpopulated
- [~] One-tenant starvation mitigation exists through namespace budgets, but broad acceptance-level proof is incomplete
- [~] Emergency mode exists, but full stabilization proof against ceilings has not been completed

1. Storage growth is bounded by retention policy under sustained churn.
2. No snapshot correctness regressions against current behavior.
3. Compaction continues making progress with active readers.
4. Operators can explain pressure via pinned-bytes and debt metrics.
5. One tenant cannot starve all others under lifecycle load.
6. Emergency mode stabilizes debt without exceeding resource ceilings.

## 19. Future Tiered-History Compatibility

Verification:

- [~] The current architecture is compatible in principle, but no tiered-history implementation work has been done yet

This design intentionally prepares tiered temporal storage by making floor advancement, debt accounting, and compaction decisions explicit and policy-driven.

## 20. Performance Non-Regression Gate (Mandatory)

Verification:

- [ ] No before/after lifecycle benchmark suite with confidence intervals has been completed
- [ ] No validated p50/p95/p99 regression report has been produced for reads or writes
- [ ] No published storage-growth-slope benchmark report exists yet
- [ ] This release gate remains open

All lifecycle changes must preserve serving performance.

Release gate:

1. No statistically significant regression in p50/p95/p99 read latency on representative workloads.
2. No statistically significant regression in p50/p95/p99 write latency on representative workloads.
3. Throughput regression budget:

- reads: <= 3%
- writes: <= 5%

4. Lifecycle CPU/IO overhead must remain within configured ceilings in normal mode.
5. Under pressure/emergency mode, latency may degrade, but system must remain within SLO error budget and recover to baseline after pressure subsides.
6. Any regression above budgets requires:

- explicit waiver
- documented tradeoff
- rollback plan

Validation protocol:

1. Run before/after benchmarks with same dataset, config, and hardware.
2. Include cache-warm and cache-busted runs.
3. Report p50/p95/p99, throughput, allocs/op, and storage growth slope.
4. Publish results with confidence intervals.

Adaptation path:

1. `debt_bytes` splits by tier (`hot`, `warm`, `cold`).
2. `safe_floor` advancement drives tier demotion eligibility.
3. planner can optimize for cross-tier debt reduction while preserving current snapshot guarantees.
