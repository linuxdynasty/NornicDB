# Hybrid Query Benchmarks

This benchmark focuses on the query shape that matters most for NornicDB's positioning: semantic retrieval followed by graph expansion in the same engine.

The goal is not to claim a universal leaderboard result. The goal is to show what happens when vector search and one-hop graph traversal share one execution path instead of being stitched together across multiple systems.

## Summary

- **Vector-only** queries stayed in sub-millisecond to low-millisecond territory locally, depending on transport.
- **Vector + one-hop graph traversal** added a small incremental cost locally.
- **Remote latency tracked client-to-server RTT**, which means end-to-end latency became network-bound rather than database-bound.

## Test Setup

| Item          | Value                                        |
| ------------- | -------------------------------------------- |
| Nodes         | 67,280                                       |
| Edges         | 40,921                                       |
| Embeddings    | 67,298                                       |
| Vector index  | HNSW, CPU-only                               |
| Request count | 800 per query type                           |
| Query types   | Vector top-k; Vector top-k + 1-hop traversal |

Local environment:

- Apple M3 Max
- 64 GB RAM
- Native macOS installer

Remote environment:

- GCP
- 8 vCPU
- 32 GB RAM

## Local Results

| Workload       | Transport |   Throughput |    Mean |     P50 |     P95 |     P99 |     Max | Allocs/op |
| -------------- | --------- | -----------: | ------: | ------: | ------: | ------: | ------: | --------: |
| Vector only    | HTTP      | 19,342 req/s |  511 us |  470 us |  750 us |  869 us | 1.02 ms |   138,031 |
| Vector only    | Bolt      | 22,309 req/s |  444 us |  428 us |  629 us |  814 us |  968 us |   206,710 |
| Vector + 1 hop | HTTP      | 11,523 req/s |  859 us |  699 us | 1.54 ms | 3.46 ms | 4.71 ms |   123,352 |
| Vector + 1 hop | Bolt      |  7,977 req/s | 1.24 ms | 1.10 ms | 1.97 ms | 4.91 ms | 6.14 ms |   181,790 |

## Remote Results

Client-to-server latency was about **110 ms**.

| Workload       | Environment |      P50 |
| -------------- | ----------- | -------: |
| Vector only    | Remote GCP  | 110.7 ms |
| Vector + 1 hop | Remote GCP  | 112.9 ms |

The practical result is straightforward: once local compute for hybrid retrieval is in low single-digit milliseconds, network RTT dominates the user-visible latency budget.

## Why This Matters

Most systems make this query shape a composition problem:

1. embed the query
2. call a vector store
3. move the results into a graph store or application layer
4. expand neighbors and shape the result there

NornicDB keeps that inside one execution engine. The benchmark does not prove every workload is constant-time, but it does show that shallow hybrid retrieval can stay tight enough locally that deployment topology matters more than extra database-side micro-optimizations.

## Caveats

- These are **single-node** measurements.
- The dataset is **not billion-scale**.
- Remote throughput is **latency-bound**, not compute-bound.
- These numbers are useful for query-shape comparison, not as a blanket claim for every graph or vector workload.

## Verification Queries

Vector-only:

```bash
curl -s -u "$NORNIC_USERNAME:$NORNIC_PASSWORD" "$ENDPOINT" \
  -H "Content-Type: application/json" -H "Accept: application/json" \
  -d '{
    "statements":[
      {
        "statement":"CALL db.index.vector.queryNodes('\''idx_original_text'\'', $topK, $text) YIELD node, score RETURN node.originalText AS originalText, score ORDER BY score DESC LIMIT $topK",
        "parameters":{"text":"get it delivered","topK":5},
        "resultDataContents":["row"]
      }
    ]
  }'
```

Vector + one-hop graph traversal:

```bash
curl -s -u "$NORNIC_USERNAME:$NORNIC_PASSWORD" "$ENDPOINT" \
  -H "Content-Type: application/json" -H "Accept: application/json" \
  -d '{
    "statements":[
      {
        "statement":"CALL db.index.vector.queryNodes('\''idx_original_text'\'', $topK, $text) YIELD node, score MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) WHERE t.language = $targetLang RETURN node.originalText AS originalText, score, t.language AS language, coalesce(t.auditedText, t.translatedText) AS translatedText ORDER BY score DESC, language LIMIT $topK",
        "parameters":{"text":"get it delivered","topK":5,"targetLang":"es"},
        "resultDataContents":["row"]
      }
    ]
  }'
```

## Related Reading

- [Benchmarks vs Neo4j](benchmarks-vs-neo4j.md)
- [Graph-RAG: Typical Distributed vs NornicDB In-Memory](../architecture/graph-rag-nornicdb-comparison.md)
- [Canonical Graph + Mutation Log Guide](../user-guides/canonical-graph-ledger.md)
