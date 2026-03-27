# NornicDB Issue Index

Use this page when you know a symptom, but not the right document yet.

## Quick Triage

- Is the server up? `curl http://localhost:7474/health`
- Can you authenticate? `POST /auth/token`
- Is the issue API-specific? Check `docs/api-reference/README.md`
- Is the issue operational? Check `docs/operations/README.md`

## By Symptom

### Startup & Connectivity

- Server won't start / exits immediately
  - `docs/operations/troubleshooting.md#server-wont-start-until-data-is-deleted-corruption-after-crash`
  - `docs/operations/deployment.md`
- Cannot connect to HTTP/Bolt
  - `docs/operations/troubleshooting.md#cannot-connect-to-server`
  - `docs/getting-started/quick-start.md`

### Auth & Access

- `401 Unauthorized`
  - `docs/operations/troubleshooting.md#401-unauthorized`
  - `docs/security/README.md`
- `403 Forbidden` / role mismatch
  - `docs/operations/troubleshooting.md#403-forbidden`
  - `docs/security/per-database-rbac.md`
  - `docs/security/entitlements.md`

### Query & API Errors

- Cypher behavior/function confusion
  - `docs/user-guides/cypher-queries.md`
  - `docs/api-reference/cypher-functions/README.md`
- HTTP endpoint mismatch
  - `docs/api-reference/OPENAPI.md`
  - `docs/api-reference/openapi.yaml`
- Neo4j compatibility question
  - `docs/neo4j-migration/feature-parity.md`
  - `docs/neo4j-migration/cypher-compatibility.md`

### Performance & Capacity

- Slow queries
  - `docs/operations/troubleshooting.md#slow-queries`
  - `docs/performance/hot-path-query-cookbook.md`
  - `docs/performance/http-optimization-options.md`
- High memory usage / OOM
  - `docs/operations/troubleshooting.md#high-memory-usage`
  - `docs/operations/low-memory-mode.md`
- High CPU usage
  - `docs/operations/troubleshooting.md#high-cpu-usage`
  - `docs/performance/README.md`

### Data Integrity & Recovery

- Data not persisting
  - `docs/operations/troubleshooting.md#data-not-persisting`
  - `docs/operations/backup-restore.md`
- Corruption / recovery after crash
  - `docs/operations/troubleshooting.md#server-wont-start-until-data-is-deleted-corruption-after-crash`
  - `docs/operations/wal-compaction.md`

### Search, Embeddings, and AI

- Embeddings not generating
  - `docs/operations/troubleshooting.md#embeddings-not-generating`
  - `docs/features/vector-embeddings.md`
- Vector/hybrid search quality issues
  - `docs/user-guides/vector-search.md`
  - `docs/user-guides/hybrid-search.md`
  - `docs/performance/searching.md`
- Agent/MCP integration issues
  - `docs/ai-agents/README.md`
  - `docs/features/mcp-integration.md`
  - `docs/user-guides/heimdall-mcp-tools.md`

### Security & Compliance

- HTTP hardening / SSRF / CSRF / XSS
  - `docs/security/http-security.md`
- Encryption/audit/RBAC requirements
  - `docs/compliance/README.md`
  - `docs/compliance/encryption.md`
  - `docs/compliance/audit-logging.md`
  - `docs/compliance/rbac.md`

## If You Still Can't Find It

- Browse by role/task in `docs/README.md`
- Use section indexes:
  - `docs/user-guides/README.md`
  - `docs/operations/README.md`
  - `docs/api-reference/README.md`
  - `docs/security/README.md`
