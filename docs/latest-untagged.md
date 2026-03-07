# Latest (Untagged) Changelog

This document tracks the currently untagged changes on `main`. These changes are the release-candidate notes for the planned `v1.0.14` maintenance release.

---

## Planned `v1.0.14`

### Release Summary

`v1.0.14` is a maintenance-focused release centered on compatibility completion, operational hardening, CI/CD cleanup, and broader regression coverage. The overall theme is stability rather than headline product expansion.

### Changed

- **Stored procedure compatibility work completed**: Finished the procedure-parity tranche with startup-compiled procedure DDL, msgpack-backed procedure catalog persistence, startup registry preloading, and transaction-script support for `BEGIN TRANSACTION`, `BEGIN`, `COMMIT`, and rollback-oriented flows.
- **CI/CD pipeline simplified**: Consolidated the repo onto a cleaner CI path, updated coverage reporting behavior, and added Docker CD workflows for release tags and manual dispatch.
- **CUDA base-image publishing isolated**: Split `llama-cuda-libs` into its own workflow so CUDA / llama.cpp base-image publishing only happens when that layer actually changes.
- **Dockerfiles made self-sufficient**: Release image builds now download their own required models during `docker build` instead of depending on a preloaded host `models/` directory.
- **Memory-limit parsing standardized**: Configuration parsing now uses a single integer-megabytes model with fail-fast validation for invalid values.

### Fixed

- **MCP production build regression**: Fixed the missing `containsLabel()` helper path so production builds no longer depend on a test-only symbol.
- **Storage snapshot overwrite edge case**: Fixed a recovery-path collision risk where fast consecutive snapshots could overwrite or confuse later recovery selection.
- **Coverage reporting noise**: Corrected coverage scoping and exclusion rules so CI reports reflect handwritten package code rather than generated or hardware-specific paths.
- **Test-exposed helper regressions**: Addressed small but real regressions surfaced during the coverage push across storage, namespacing, transaction, WAL, and helper branches.

### Test and Hardening Work

- **Large handwritten coverage expansion** across `pkg/storage`, `pkg/cypher`, handwritten `pkg/cypher/antlr`, `pkg/mcp`, `pkg/nornicdb`, `pkg/auth`, and `pkg/config`.
- **Procedure, parser, and storage regression tests** were extended to cover more compatibility, recovery, and edge-case branches.
- **Flaky-path coverage** was added around snapshot recovery and helper behavior to make CI failures more reproducible and actionable.

### Documentation

- Added and refined stored-procedure parity documentation and examples.
- Updated supporting docs and release/coverage references to match the current automation and packaging flow.

### Technical Details

- **Range**: `v1.0.13..main`
- **Statistics**: 30 commits, 341 files changed, +23,539 / -4,441 lines
- **Primary focus areas**: Cypher procedure compatibility, CI/CD and Docker release automation, storage/MCP hardening, and regression coverage expansion
