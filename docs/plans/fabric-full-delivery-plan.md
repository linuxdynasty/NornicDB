---
name: nornicdb-fabric-remote-constituents-full-delivery-plan
overview: Full implementation plan for Neo4j Fabric-compatible remote constituents in NornicDB with complete Cypher/API parity, complete transaction behavior, complete auth propagation, and no placeholder or deferred behavior.
status: approved-target
last_updated: 2026-03-13
owners:
  - pkg/cypher
  - pkg/fabric
  - pkg/multidb
  - pkg/storage
  - pkg/server
  - pkg/bolt
references:
  - ~/src/neo4j/community/fabric
  - docs/plans/sharding-base-plan.md
  - docs/plans/sharding.md
---

# NornicDB Fabric + Remote Constituents Full Delivery Plan

## 1. Non-Negotiable Delivery Contract

This plan is the implementation contract for Fabric-style remote constituents (sharding) in NornicDB.

Hard constraints:

- No fake paths.
- No compatibility notes that hide missing behavior.
- No "for now" semantics.
- No silent fallbacks that change Cypher meaning.
- All user-facing behavior must be deterministic and Neo4j-compatible where the syntax/feature is claimed.

Definition of done for this project:

- Claimed Cypher syntax executes with production behavior (not parser-only acceptance).
- Remote constituents are first-class in Bolt, HTTP tx API, and GraphQL execution paths through a shared execution core.
- Distributed transaction semantics are fully implemented for the supported model (many-read/one-write-shard).
- Auth is preserved end-to-end across shard fan-out.
- Documentation reflects exact runtime behavior, with no speculative statements.
- New/changed handwritten code has 100% branch coverage in touched packages.

## 2. Required Compatibility Surface

## 2.1 Cypher DDL / Management

Must support and execute correctly:

- `CREATE COMPOSITE DATABASE <name>`
- `CREATE COMPOSITE DATABASE <name> IF NOT EXISTS`
- `CREATE COMPOSITE DATABASE <name> ALIAS <alias> FOR DATABASE <db>`
- `CREATE COMPOSITE DATABASE <name> ALIAS <alias> FOR DATABASE <db> AT '<uri>'`
- `... USER <user> PASSWORD '<password>'`
- `... OIDC CREDENTIAL FORWARDING`
- `ALTER COMPOSITE DATABASE <name> ADD ALIAS ...`
- `ALTER COMPOSITE DATABASE <name> DROP ALIAS ...`
- `SHOW CONSTITUENTS FOR COMPOSITE DATABASE <name>`
- `SHOW DATABASES` and `SHOW COMPOSITE DATABASES` must include accurate topology state.

## 2.2 Cypher Query Routing

Must support and execute correctly:

- top-level `USE <database>`
- top-level `USE <composite>`
- `CALL { USE <composite.alias> ... }`
- chained and correlated subqueries with `WITH` import/export semantics
- mixed local and remote constituent plans in one query tree

Example that must execute correctly:

```cypher
USE nornic
CALL {
  USE nornic.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId, t.textKey AS textKey, t.textKey128 AS textKey128
}
CALL {
  USE nornic.txt
  WITH translationId
  MATCH (tt:TranslationText)
  WHERE tt.translationId = translationId
  RETURN collect(tt) AS texts
}
RETURN translationId, textKey, textKey128, texts
```

## 2.3 Transaction Semantics

Supported model (must be fully implemented):

- One distributed transaction can read from multiple shards.
- One distributed transaction can write to exactly one write shard.
- Attempting writes to a second write shard is rejected deterministically with stable code/message.
- Explicit tx API behavior (`BEGIN`/`COMMIT`/`ROLLBACK`) must work for composite/remote through coordinator-backed transaction sessions.
- Implicit tx (`/tx/commit`) and Bolt auto-commit must share the same write constraints and failure semantics.

## 2.4 Auth and Identity Propagation

Must support both auth models:

- explicit remote credentials (`USER/PASSWORD`)
- OIDC credential forwarding (`OIDC CREDENTIAL FORWARDING`)

Rules:

- when explicit credentials are present, remote calls use those credentials
- when OIDC forwarding is selected or implicit, caller credential/token is forwarded unchanged
- forwarded identity must remain attributable in audit logs and error traces
- no raw credentials in API responses or logs

## 2.5 Failure Semantics

Distributed reads and subqueries:

- if required shard execution fails/timeouts, entire query fails deterministically
- no partial-result success for required shards
- retries are explicit and bounded; never silent semantic degradation

Distributed writes:

- commit outcome is atomic for supported many-read/one-write model
- deterministic rollback on coordinator failure paths

## 3. Target Architecture (Implementation, Not Placeholder)

## 3.1 Shared Execution Entry

Introduce `QueryGateway` as the single semantic entrypoint for:

- Bolt
- HTTP tx API
- GraphQL `executeCypher`
- gRPC execution adapters

All protocols call the same planner/executor stack.

## 3.2 Fabric Core Package

Implement `pkg/fabric/` fully:

- `fragment.go` (`Init`, `Leaf`, `Exec`, `Apply`, `Union`)
- `catalog.go` + `location.go`
- `planner.go` (USE-aware decomposition)
- `executor.go` (fragment execution orchestrator)
- `transaction.go` (coordinator)
- `local_executor.go`
- `remote_executor.go` (Bolt-first transport with parity fallback path only when URI scheme is HTTP)

## 3.3 Remote Engine

`pkg/storage/remote_engine.go` responsibilities:

- complete `storage.Engine` behavior over remote transport
- deterministic errors mapped to storage/cypher layer contracts
- batch operations use true batched execution (no N+1 loops)
- context deadlines/cancellation propagated end-to-end
- compile-time interface conformance assertions

## 3.4 Catalog + Multidb

`pkg/multidb` responsibilities:

- canonical constituent metadata model (`alias`, `db`, `type`, `uri`, `auth mode`)
- secure credential-at-rest handling for explicit remote credentials
- runtime credential resolution and decryption in execution path
- zero password leakage in `SHOW` outputs, management APIs, and logs

## 3.5 Transaction Coordinator

Coordinator requirements:

- track sub-transaction per participating shard
- enforce one-write-shard invariant
- support explicit tx lifecycle APIs for composite dbs
- rollback consistency on participant failures
- expose stable transaction metadata for diagnostics

## 3.6 Management Surface (Neo4j-Compatible)

Management operations must be delivered through Neo4j-compatible Cypher and protocol surfaces:

- management via Cypher DDL in `system` database (`CREATE/ALTER/DROP/SHOW COMPOSITE DATABASE ...`)
- callable through Bolt and HTTP transaction APIs using the same semantics
- GraphQL execution path must forward to the same engine semantics when executing management Cypher

No custom Nornic-only cluster admin API is required for feature completeness unless Neo4j has an equivalent management surface.

UI:

- composite and constituent management built on top of the same Cypher management commands
- auth mode display (redacted)
- topology visualization derived from `SHOW` results

## 4. Security Requirements

Credential handling:

- explicit remote credentials are encrypted at rest
- encryption key precedence:
  1. `NORNICDB_REMOTE_CREDENTIALS_KEY`
  2. database encryption secret
  3. JWT signing key fallback (allowed, warned)
- startup logs must warn when using fallback key source
- APIs must never return raw credentials

OIDC forwarding:

- forwarded token must preserve principal/claims for downstream auth
- remote execution must fail closed on invalid/expired token

## 5. Concrete Work Breakdown (All Required)

## Workstream A: Cypher + Planner Correctness

Deliverables:

- full parsing + semantic handling for `USE` at top-level and subqueries
- correlated subquery variable scope compatibility
- deterministic erroring on invalid cross-scope usage

Acceptance tests:

- comprehensive query matrix for `USE + CALL + WITH + RETURN`
- equivalence tests against expected Neo4j behavior for supported patterns

## Workstream B: Remote Constituent DDL + Metadata

Deliverables:

- full DDL execution for create/alter/drop constituent paths
- strict validation of auth mode combinations
- secure metadata persistence and retrieval

Acceptance tests:

- DDL syntax permutations
- metadata persistence/reload across restart
- auth redaction tests

## Workstream C: Remote Execution Runtime

Deliverables:

- remote executor over Bolt path
- HTTP path only when URI scheme is HTTP(S), with matching semantics
- batched operations and deterministic mapping of remote errors

Acceptance tests:

- transport parity tests (Bolt vs HTTP URI)
- cancellation/timeout propagation tests
- batch correctness/performance threshold tests

## Workstream D: Distributed Transaction Layer

Deliverables:

- explicit tx support for composites via coordinator
- subtransaction open/commit/rollback orchestration
- one-write-shard enforcement and deterministic error contract

Acceptance tests:

- explicit tx lifecycle tests across multi-shard reads and writes
- second-write-shard rejection tests
- rollback on remote participant failure tests

## Workstream E: Protocol Parity

Deliverables:

- identical semantic outcomes across Bolt/HTTP/GraphQL/gRPC adapters

Acceptance tests:

- protocol conformance suite: same query, same outcome (rows/errors/codes)

## Workstream F: Admin/API/UI Operations

Deliverables:

- Neo4j-compatible management commands fully functional via `system` database Cypher
- protocol parity for management operations across Bolt/HTTP tx/GraphQL execution paths
- UI workflows for management actions built on those compatible commands only

Acceptance tests:

- management command conformance tests (syntax, behavior, error contract) against Neo4j-compatible expectations
- protocol parity tests for management commands (same statement, same outcome)
- UI e2e tests for create/update/remove constituent flows through Cypher-backed operations

## Workstream G: Documentation

Deliverables:

- user guide for composite + remote constituents
- exact Cypher command cookbook with working examples
- operational runbook (auth, keying, diagnostics, failure handling)

Acceptance tests:

- docs examples are runnable in CI smoke harness

## 6. Test and Coverage Gates

Mandatory gates before merge:

- all touched package tests pass with `-race`
- all newly added handwritten code paths are branch-covered at 100%
- no flaky tests (must pass repeat runs)
- compatibility suite green for declared features

Minimum required automated suites:

- unit: parser/planner/executor/transaction/security helpers
- integration: multi-node remote constituent topologies
- protocol parity: Bolt vs HTTP vs GraphQL vs gRPC
- resilience: timeout, cancellation, network split, shard unavailable
- security: auth forwarding, credential redaction, encrypted metadata

## 7. Performance Gates

Required evidence:

- benchmark before/after for distributed query execution and batching
- no uncontrolled N+1 patterns in remote paths
- bounded overhead for distributed planner/executor vs single-db path

Failure to meet performance gates blocks release.

## 8. Operational Readiness Gates

Required:

- metrics for per-shard latency/errors/tx outcomes
- tracing correlation across coordinator and remote shards
- audit log continuity for forwarded principal
- alertable health endpoints for shard availability

## 9. Release Criteria

Release is allowed only when all are true:

- all Workstreams A-G complete
- all test/coverage/performance/ops gates pass
- no known semantic gaps for claimed Neo4j-compatible fabric surface
- documentation and examples match actual runtime behavior

If any required capability is incomplete, release is blocked.
