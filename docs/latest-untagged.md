# Latest (Untagged) Changelog

This document tracks the currently untagged changes on `main`. These changes are the release-candidate notes for the planned `v1.0.16` maintenance release.

---

## Planned `v1.0.16`

### Release Summary

`v1.0.16` is a Cypher correctness and compatibility maintenance release focused on strict openCypher semantics, schema/index compatibility with Neo4j tooling, and deterministic regression coverage expansion.

### Changed

- **Cypher strictness hardening**:
  - Tightened `SET +=` map-literal parsing and invalid syntax rejection.
  - Tightened permissive `CREATE/RETURN` and mutation parser paths.
  - Corrected `UNWIND ... SET n = row` map-literal handling.
- **Cypher execution correctness**:
  - Preserved property update semantics across `SET ... WITH ... RETURN` pipelines.
  - Added non-progress loop protection for `CALL ... IN TRANSACTIONS`.
  - Hardened malformed relationship pattern handling in traversal/match paths.
- **Schema/index compatibility work**:
  - Restored Neo4j-compatible index parsing for parenthesized node patterns.
  - Added compatibility support for additional `CREATE INDEX` and `CREATE FULLTEXT INDEX` command variants.
  - Added qualified `SHOW FULLTEXT/RANGE/VECTOR INDEXES` routing with filtered results.
- **UI routing compatibility**: Browser path handling now preserves/enforces trailing slash for reverse-proxy compatibility.
- **Heimdall image robustness**: Updated Docker Qwen model source path for more reliable model hydration.

### Fixed

- **Index parser label-token bug**: Fixed `CREATE INDEX FOR (n:Label) ON (n.prop)` edge case that could capture `)` in label parsing.
- **Index persistence no-op bug**: Fixed compatibility syntax paths that returned success without persisting schema indexes.
- **SHOW command compatibility gap**: Fixed `SHOW FULLTEXT INDEXES` unsupported-command behavior and added qualified SHOW index variants.
- **Pre-commit workflow stability**: Hardened local pre-commit behavior to reduce merge-conflict side effects during staged updates.

### Test and Hardening Work

- **Large Cypher coverage expansion** across parser, schema, transaction, dispatch, subquery, mutation, traversal, and APOC compatibility branches.
- **Deterministic regression tests** added for async schema routing, Neo4j index syntax variants, and qualified SHOW index command families.
- **Assertion hardening** to reduce permissive/non-deterministic branches in existing tests.

### Documentation

- Updated `CHANGELOG.md` with a dedicated `v1.0.16` release section and release-range metadata.

### Technical Details

- **Range**: `v1.0.15..main`
- **Statistics**: 19 commits (non-merge), 57 files changed, +5,185 / -165 lines
- **Non-test surface**: 20 files
- **Primary focus areas**: Cypher semantics/compatibility, schema/index correctness, deterministic test hardening, UI trailing-slash routing compatibility.
