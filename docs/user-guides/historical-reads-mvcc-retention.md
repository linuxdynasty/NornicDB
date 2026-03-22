# Historical Reads and MVCC Retention

NornicDB keeps multi-version history for nodes and edges so you can ask historical questions without disturbing current reads. This is backed by MVCC (multi-version concurrency control): each logical record can have a chain of committed versions plus a persisted current head.

This guide covers:

- what MVCC history gives you
- how to query historical state
- how retention and pruning work
- which knobs control history size and age
- how MVCC interacts with search and serialization

## What This Feature Does

MVCC history enables:

- snapshot-consistent reads of nodes, edges, labels, and relationship topology
- temporal procedures that can answer "what was true at system time T?"
- pruning of older versions without deleting the current head
- tombstone-chain compaction when it is safe to do so

Important behavior:

- the current head is always preserved
- search indexes stay current-only; pruning historical versions does not change current search results
- retention settings define the default pruning policy, but they do not start a background pruning job by themselves

## Querying Historical State

### Cypher Temporal Procedure

The main Cypher entry point is `db.temporal.asOf`.

```cypher
CALL db.temporal.asOf(
  'Account',
  'accountId',
  'acct-42',
  'valid_from',
  'valid_to',
  datetime('2026-03-01T00:00:00Z')
) YIELD node
RETURN node
```

That answers the business-time question: which row was valid at the requested `asOf` instant.

You can also pin the query to a historical MVCC snapshot by supplying `systemTime` and, optionally, `systemSequence`:

```cypher
CALL db.temporal.asOf(
  'Account',
  'accountId',
  'acct-42',
  'valid_from',
  'valid_to',
  datetime('2026-03-01T00:00:00Z'),
  datetime('2026-03-15T12:30:00Z'),
  1842
) YIELD node
RETURN node
```

Use the optional snapshot arguments when you need to answer both:

- business time: which version was valid for the domain event
- system time: which version was committed and visible at a historical database snapshot

### Embedded Go APIs

If you embed NornicDB, the storage and DB layers expose explicit MVCC visibility methods:

```go
head, err := engine.GetNodeCurrentHead(nodeID)
if err != nil {
    return err
}

node, err := engine.GetNodeVisibleAt(nodeID, head.Version)
if err != nil {
    return err
}
```

For label and relationship scans against a snapshot, use:

- `GetNodesByLabelVisibleAt`
- `GetEdgesByTypeVisibleAt`
- `GetEdgesBetweenVisibleAt`

## Retention Policy

NornicDB now has a formal retention policy surface for MVCC history.

Defaults:

- `MaxVersionsPerKey = 100`
- `TTL = 0` which means no age-based protection

Semantics:

- `MaxVersionsPerKey` applies to closed historical versions
- the current head is preserved separately and is never deleted by pruning
- `TTL` protects versions newer than `now - TTL` from pruning
- pruning is head-safe and tombstone-aware

### YAML Configuration

```yaml
database:
  storage_serializer: msgpack
  mvcc_retention_max_versions: 1
  mvcc_retention_ttl: "168h"
```

### Environment Variables

```bash
export NORNICDB_STORAGE_SERIALIZER=msgpack
export NORNICDB_MVCC_RETENTION_MAX_VERSIONS=100
export NORNICDB_MVCC_RETENTION_TTL=168h
```

### Choosing Values

Use a lower version cap when:

- the same keys are updated frequently
- oldest-snapshot lookup latency matters more than long audit depth
- your workload produces long tombstone/recreate chains

Use a TTL when:

- you need a guaranteed recent history window even under heavy churn
- operators need predictable lookback such as the last 24 hours or 7 days

Practical starting points:

- default workload: `100` versions, no TTL
- moderate write churn with recent debugging needs: `50` versions, `24h` TTL
- audit-heavy but bounded history: `100` versions, `168h` TTL

## Pruning History

Retention settings define the default policy, but pruning is a maintenance action.

For embedded deployments, call the DB maintenance API:

```go
deleted, err := db.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{})
if err != nil {
    return err
}

log.Printf("pruned %d historical versions", deleted)
```

Passing zero values uses the configured engine retention policy.

You can also override the configured defaults for a one-off maintenance run:

```go
deleted, err := db.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{
    MaxVersionsPerKey: 50,
    MinRetentionAge:   24 * time.Hour,
})
```

If you need to reconstruct head state from stored history, use:

```go
if err := db.RebuildMVCCHeads(ctx); err != nil {
    return err
}
```

## Search Behavior

Search indexing remains current-only.

That means:

- historical MVCC versions are not added to current search indexes
- pruning historical versions does not change current vector or BM25 search results
- HNSW and current candidate selection continue to operate on the latest visible graph state

This separation is intentional: MVCC history is for temporal correctness and historical inspection, not for duplicating search corpus entries.

## Serialization Notes

`storage_serializer` still controls the primary storage format for main records in the broader storage layer.

For MVCC specifically:

- MVCC version records use Msgpack on the hot path
- MVCC head metadata uses Msgpack
- new MVCC/internal metadata does not use gob

For new deployments, keep `storage_serializer: msgpack`.

## Operational Guidance

Watch for these signals:

- frequent updates to the same logical keys
- a growing gap between current-read latency and oldest-snapshot latency
- long delete/recreate sequences on the same IDs

When those appear:

1. lower `mvcc_retention_max_versions`
2. add an `mvcc_retention_ttl` if you need a recent lookback guarantee
3. run `PruneMVCCVersions` during maintenance windows or scheduled operator workflows

## Related Docs

- [Operations Configuration](../operations/configuration.md)
- [Environment Variables](../operations/environment-variables.md)
- [Storage Serialization](../operations/storage-serialization.md)
- [Cypher Queries](cypher-queries.md)