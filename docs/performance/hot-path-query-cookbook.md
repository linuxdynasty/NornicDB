# Hot Path Query Cookbook

A practical, domain-neutral guide for fast, predictable graph query latency in production systems.

Use this cookbook as a menu of proven query shapes. Pick the shape that matches your endpoint behavior, adapt labels/properties, and measure p50/p95/p99.

## Generic Model Used In Examples

- Node labels: `EntityA`, `EntityB`, `Tenant`, `Event`
- Relationship types: `KEY_REL`, `BELONGS_TO`, `LINKS_TO`
- Properties: `primaryKey`, `alternateKey`, `tenantId`, `category`, `status`, `createdAt`, `updatedAt`, `sessionId`

Replace names with your schema names.

## Baseline Index And Constraint Patterns

```cypher
CREATE CONSTRAINT entitya_primarykey_uniq IF NOT EXISTS FOR (n:EntityA) REQUIRE n.primaryKey IS UNIQUE;
CREATE INDEX entitya_alternatekey_idx IF NOT EXISTS FOR (n:EntityA) ON (n.alternateKey);
CREATE INDEX entitya_tenantid_idx IF NOT EXISTS FOR (n:EntityA) ON (n.tenantId);
CREATE INDEX entityb_category_status_createdat_idx IF NOT EXISTS FOR (n:EntityB) ON (n.category, n.status, n.createdAt);
CREATE INDEX entityb_tenantid_idx IF NOT EXISTS FOR (n:EntityB) ON (n.tenantId);
CREATE INDEX event_createdat_idx IF NOT EXISTS FOR (e:Event) ON (e.createdAt);
```

Test-only cleanup support:

```cypher
CREATE INDEX entitya_sessionid_idx IF NOT EXISTS FOR (n:EntityA) ON (n.sessionId);
CREATE INDEX entityb_sessionid_idx IF NOT EXISTS FOR (n:EntityB) ON (n.sessionId);
```

## Area 1: Point Lookup And Existence

### 1.1 Upsert And Read By Stable Key

```cypher
MERGE (n:EntityA {primaryKey: $primaryKey})
SET n.payload = $payload, n.updatedAt = $now;

MATCH (n:EntityA {primaryKey: $primaryKey})
RETURN n
LIMIT 1;
```

### 1.2 Lookup By Either Of Two Keys

```cypher
CALL {
  WITH $lookupKey AS k
  MATCH (n:EntityA {primaryKey: k}) RETURN n
  UNION
  WITH $lookupKey AS k
  MATCH (n:EntityA {alternateKey: k}) RETURN n
}
RETURN n
LIMIT 1;
```

### 1.3 Fast Existence Check

```cypher
MATCH (n:EntityA {primaryKey: $primaryKey})
RETURN 1 AS found
LIMIT 1;
```

Use when you only need yes/no, not full payload.

### 1.4 Direct ID Seek Shape

```cypher
MATCH (n:EntityA)
WHERE elementId(n) = $id
RETURN n
LIMIT 1;
```

Keep this as a dedicated template so planners can reliably choose direct ID seek.

## Area 2: Batch Retrieval

### 2.1 Bulk Lookup For Many Keys

```cypher
UNWIND $keys AS k
CALL {
  WITH k
  MATCH (n:EntityA {primaryKey: k}) RETURN n
  UNION
  WITH k
  MATCH (n:EntityA {alternateKey: k}) RETURN n
}
RETURN k AS lookupKey, collect(n) AS results;
```

### 2.2 Chunked IN-List Lookup

```cypher
MATCH (n:EntityA)
WHERE n.primaryKey IN $keys
RETURN n;
```

Use with bounded chunk size (for example 100-1000 keys per call).

### 2.3 OR-To-UNION Dual-Key Lookup

```cypher
CALL {
  WITH $lookupKey AS k
  MATCH (n:EntityA {primaryKey: k}) RETURN n
  UNION
  WITH $lookupKey AS k
  MATCH (n:EntityA {alternateKey: k}) RETURN n
}
WITH DISTINCT n
RETURN n;
```

Prefer this over a single `OR` predicate across different properties.

## Area 3: Queue, Feed, And Pagination

### 3.1 Queue/List View (Newest First)

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

### 3.2 Optional Filter As Separate Template

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
  AND a.scope STARTS WITH $scopePrefix
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

### 3.3 Keyset (Seek) Pagination

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
  AND b.createdAt < $cursorCreatedAt
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT $pageSize;
```

Preferred for deep pagination.

### 3.4 Offset Pagination (Small Pages Only)

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
SKIP $offset
LIMIT $limit;
```

Use only for shallow pages.

### 3.5 Top-N With Composite Index-Friendly Shape

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

Keep the sort key and filtered fields aligned with a composite index when possible.

### 3.6 Optional Predicate Split (Two Templates)

Template A (no prefix filter):

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

Template B (with prefix filter):

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
  AND a.scope STARTS WITH $scopePrefix
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

Prefer two templates over `($scopePrefix = '' OR a.scope STARTS WITH $scopePrefix)`.

## Area 4: Search Patterns

### 4.1 Vector Similarity Search

```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
MATCH (node:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
RETURN node, b, score
ORDER BY score DESC
LIMIT $topK;
```

### 4.2 Full-Text Then Graph Expand

```cypher
CALL db.index.fulltext.queryNodes('idx_entitya_text', $query)
YIELD node, score
MATCH (node)-[:KEY_REL]->(b:EntityB)
RETURN node, b, score
ORDER BY score DESC
LIMIT $topK;
```

### 4.3 Hybrid Candidate Merge (Application Side)

Run vector and full-text candidate queries separately, merge and rerank in application code for stable control.

## Area 5: Aggregates And Dashboard Reads

### 5.1 Grouped Count By Status

```cypher
MATCH (:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
RETURN b.status AS status, count(*) AS total
ORDER BY total DESC;
```

### 5.2 Rolling Window Counts

```cypher
MATCH (e:Event)
WHERE e.createdAt >= $windowStart
RETURN count(*) AS eventsInWindow;
```

### 5.3 Tenant-Scoped Aggregate

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE a.tenantId = $tenantId
RETURN b.status, count(*) AS total;
```

### 5.4 Null-Normalized Predicate Shape

```cypher
MATCH (b:EntityB)
WHERE b.status = 'open'
  AND (b.isReviewed = false OR b.isReviewed IS NULL)
RETURN b
LIMIT 100;
```

Prefer explicit null-aware predicates instead of `coalesce(...)` wrappers on filtered columns.

## Area 6: Relationship And Traversal

### 6.1 Idempotent Relationship Upsert

```cypher
MATCH (a:EntityA {primaryKey: $fromKey})
MATCH (b:EntityB {primaryKey: $toKey})
MERGE (a)-[r:LINKS_TO]->(b)
SET r.updatedAt = $now;
```

### 6.4 Relationship Attach By ID

```cypher
MATCH (a:EntityA) WHERE elementId(a) = $fromId
MATCH (b:EntityB) WHERE elementId(b) = $toId
CREATE (a)-[:LINKS_TO {createdAt: $now}]->(b);
```

Use stable ID lookups before relationship creation for predictable attach latency.

### 6.2 Bounded Traversal

```cypher
MATCH p = (start:EntityA {primaryKey: $startKey})-[:LINKS_TO*1..3]->(n)
RETURN p
LIMIT 100;
```

Always bound traversal depth and result count.

### 6.3 Traversal With Early Filters

```cypher
MATCH p = (start:EntityA {primaryKey: $startKey})-[:LINKS_TO*1..3]->(n:EntityB)
WHERE n.status = $status
RETURN p
LIMIT 100;
```

## Area 7: Write Hot Paths

### 7.1 Targeted Bulk Update

```cypher
CALL {
  WITH $lookupKey AS k
  MATCH (n:EntityA {primaryKey: k}) RETURN n
  UNION
  WITH $lookupKey AS k
  MATCH (n:EntityA {alternateKey: k}) RETURN n
}
MATCH (n)-[:KEY_REL]->(b:EntityB {category: $category})
WHERE b.status = 'pending'
SET b.status = 'queued', b.updatedAt = $now;
```

### 7.2 Read-After-Write In One Transaction

```cypher
MERGE (n:EntityA {primaryKey: $primaryKey})
SET n.updatedAt = $now
WITH n
MATCH (n)-[:KEY_REL]->(b:EntityB)
RETURN n, b
LIMIT 1;
```

Use when the API needs immediate confirmed view of updated data.

### 7.3 Bulk Ingestion With UNWIND

```cypher
UNWIND $rows AS row
MERGE (n:EntityA {primaryKey: row.primaryKey})
SET n += row.props, n.updatedAt = $now;
```

Use bounded batch sizes to avoid oversized transactions.

### 7.4 Single-Statement Autocommit Shape

```cypher
MERGE (n:EntityA {primaryKey: $primaryKey})
ON CREATE SET n.createdAt = $now
SET n.updatedAt = $now, n.payload = $payload
RETURN n;
```

Favor single-statement request shapes on hot write/read paths.

## Area 8: Cleanup, TTL, And Archival

### 8.1 Safe Batch Cleanup For Test Data

```cypher
MATCH (n:EntityB)
WHERE n.sessionId = $sessionId
WITH n LIMIT 1000
DETACH DELETE n;
```

Repeat until zero rows remain.

### 8.2 TTL-Like Time-Bucket Cleanup

```cypher
MATCH (e:Event)
WHERE e.createdAt < $cutoff
WITH e LIMIT 5000
DETACH DELETE e;
```

Run on a schedule.

### 8.3 Archive Then Delete Pattern

```cypher
MATCH (n:EntityB)
WHERE n.createdAt < $cutoff
WITH n LIMIT 1000
CREATE (:ArchiveRecord {sourceId: elementId(n), payload: n, archivedAt: $now})
DETACH DELETE n;
```

Use when retention policy requires recoverable archives.

### 8.4 Streaming Batch Delete Loop

```cypher
MATCH (n:EntityB)
WHERE n.sessionId = $sessionId
WITH n LIMIT 500
DETACH DELETE n;
```

Repeat in application/job scheduler until zero rows are affected.

## Area 9: Multi-Tenant Query Isolation

### 9.1 Tenant-First Point Lookup

```cypher
MATCH (n:EntityA)
WHERE n.tenantId = $tenantId AND n.primaryKey = $primaryKey
RETURN n
LIMIT 1;
```

### 9.2 Tenant-Scoped List

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE a.tenantId = $tenantId
  AND b.category = $category
  AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

Keep tenant predicates explicit in all hot-path templates.

## Area 10: Optional-Match Heavy Workloads

### 10.1 Optional Data Expansion (Light)

```cypher
MATCH (a:EntityA {primaryKey: $primaryKey})
OPTIONAL MATCH (a)-[:KEY_REL]->(b:EntityB)
RETURN a, collect(b) AS related;
```

### 10.2 Split Heavy Optional Shapes

If optional branches become large and sparse, split into multiple focused queries and compose in the application layer.

## Area 11: Plan Reuse And Diagnostics

### 11.1 Stable Parameterized Template Reuse

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category AND b.status = $status
RETURN b
ORDER BY b.createdAt DESC
LIMIT $limit;
```

Keep the query text shape stable and pass only parameter changes across calls.

### 11.2 PROFILE Index Acceptance/Rejection Checks

```cypher
PROFILE
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category AND b.status = $status
RETURN b
ORDER BY b.createdAt DESC
LIMIT 30;
```

Use profiling to confirm index-backed seeks/sorts and to identify rejection causes such as function wrapping or predicate shape.

## Common Anti-Patterns To Avoid

1. Cross-property `OR` in one predicate.
2. Function-wrapped filter columns in `WHERE` (`coalesce`, `toLower`, etc.).
3. Optional branch predicates inside one template (`$x = '' OR ...`).
4. Broad `CONTAINS` substring filters on large datasets.
5. Unbounded traversals.
6. Large single-transaction delete/ingest operations.
7. Deep `SKIP/LIMIT` pagination for user-facing feeds.

## Operational Checklist

1. Separate cold-run and warm-run measurements.
2. Track p50, p95, and p99 under realistic concurrency.
3. Reuse stable query templates.
4. Keep related operations in one transaction when practical.
5. Validate indexes and constraints regularly:

```cypher
SHOW INDEXES;
SHOW CONSTRAINTS;
```

If latency spikes:
1. Verify required indexes are `ONLINE`.
2. Re-run identical shape 3-5 times to compare first-hit vs steady state.
3. Split optional-filter and optional-match paths into separate templates.
