# Latest (Untagged) Changelog

This document tracks the currently untagged changes on `main`. These changes are the release-candidate notes for the planned `v1.0.17-preview` release.

---

## Planned `v1.0.17-preview`

### Release Summary

`v1.0.17-preview` is a composite-database preview release focused on the new Fabric execution stack, remote constituent routing, cross-protocol transaction parity, and multidatabase observability improvements.

### Changed

- **Fabric composite execution stack**:
  - Added the initial planner, executor, transaction, catalog, and gateway layers under `pkg/fabric` for Neo4j-style composite database execution.
  - Added remote engine and remote credential plumbing so composite graphs can route queries to remote constituents.
  - Added auth-aware Fabric routing across HTTP/Bolt database-manager paths.
- **Cypher composite execution support**:
  - Added transaction keywords and shell command preprocessing for scripted Cypher workflows.
  - Added `USE`-aware subquery execution plus recursive `CALL { USE ... }` decomposition for nested composite subqueries.
  - Added simple equality index-seek support in Cypher execution to improve some routed query paths.
- **Multi-database observability**:
  - Added cached per-database node and embedding byte statistics in `/databases`.
  - Surfaced the new size metrics in the Web UI databases page.
- **Performance hardening**:
  - Removed multiple Fabric planner/executor allocation hotspots.
  - Benchmarks in the range show `deduplicateRows` improving from `1.91ms` to `1.22ms` and `combineRows` from `246ns` to `74ns`.

### Fixed

- **Correlated `WITH` + `USE` execution**: Fixed APPLY shaping and record propagation bugs in correlated composite subqueries.
- **Bolt/HTTP routing parity**: Fixed Bolt database-manager routing so it matches HTTP behavior for composite execution and correlated import handling.
- **Empty Fabric result handling**: Fixed empty-result and null-column response shapes that could destabilize server responses and crash UI query rendering.
- **Composite parity edge cases**: Fixed remaining gaps around composite schema commands, remote existence checks, auth forwarding, and transaction manager behavior.

### Test and Hardening Work

- **Broad Fabric regression coverage** across planner, executor, transaction, gateway, remote executor, and correlated subquery execution paths.
- **Composite parity integration tests** for schema commands, transaction routing, remote constituents, auth forwarding, and Bolt/HTTP db-manager behavior.
- **Performance audit artifacts** and targeted Cypher regression tests for Fabric hot paths and index-seek execution.

### Documentation

- Expanded the multi-database guide and added Fabric gap-analysis, delivery-plan, and performance-audit notes.
- Refreshed `CHANGELOG.md` with a dedicated `v1.0.17-preview` release section and release-range metadata.

### Technical Details

- **Range**: `v1.0.16..main`
- **Statistics**: 21 commits (non-merge), 230 files changed, +25,221 / -5,323 lines
- **Non-test surface**: 67 files
- **Primary focus areas**: Fabric/composite execution, remote constituent routing, transaction/protocol parity, multidatabase stats/UI, planner/executor performance.
