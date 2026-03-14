# Fabric Performance Audit Status

## Scope
- Package: `pkg/fabric`
- Date: 2026-03-14
- Goal: close the open high/medium allocation hotspots from the fabric performance review with semantic safety.

## Implemented

1. `queryIsWrite` allocation removal (planner)
- Replaced full-query uppercasing with zero-allocation keyword scanning (`hasKeywordAt`).
- Result: `0 B/op`, `0 allocs/op` in benchmarks.

2. `inferReturnColumnsFromQuery` allocation reduction (executor)
- Replaced broad string uppercasing in parsing path with case-insensitive targeted scans.
- Result: stabilized at `2 allocs/op`, reduced latency for multi-column returns.

3. `rewriteLeadingWithImports` hot-path cleanup (executor)
- Removed `fmt.Sprintf` and broad `ToUpper` usage from leading `WITH` rewrite logic.
- Result: reduced allocation count and improved average runtime.

4. `inCompositeScope` zero-allocation prefix checks (planner/catalog)
- Removed repeated graph-list materialization from validation path.
- Result: `0 B/op`, `0 allocs/op`.

5. `combineRowsByColumns` map-allocation removal (executor)
- Added precomputed index merge path (`combineRowsByIndexes`) and exercised it in apply execution loop.
- Result: `0 B/op`, `0 allocs/op` for row merge microbenchmark.

6. `deduplicateRows` hot-path rewrite (executor)
- Removed `fmt.Sprintf`-style row identity path and per-row hash object allocations.
- Implemented typed FNV-1a row hashing with deterministic map-key ordering and allocation-free marker/string writes.
- Benchmark impact:
  - Before: `~1.91ms/op`, `~1.34MB/op`, `~90034 allocs/op`
  - After:  `~1.22ms/op`, `~541KB/op`, `34 allocs/op`

7. `executeUnion` medium-path concurrency (executor)
- Added read-only parallel branch execution with deterministic output order (`LHS` rows then `RHS` rows).
- Added semantic safety guard: if either branch contains write fragments, execution stays sequential to preserve deterministic write routing semantics.

8. Dead helper cleanup
- Removed unused `mergeRowParams` helper and its isolated test-only coverage.

## Benchmarks (latest)
Command:
```bash
go test ./pkg/fabric -run ^$ -bench 'Benchmark(QueryIsWrite|InferReturnColumnsFromQuery|RewriteLeadingWithImports|InCompositeScope|DeduplicateRows|CombineRowsByIndexes)$' -benchmem -count=1
```

Observed:
- `BenchmarkDeduplicateRows`: `1216467 ns/op`, `541320 B/op`, `34 allocs/op`
- `BenchmarkCombineRowsByIndexes`: `74.45 ns/op`, `0 B/op`, `0 allocs/op`
- `BenchmarkQueryIsWrite/read`: `1737 ns/op`, `0 B/op`, `0 allocs/op`
- `BenchmarkQueryIsWrite/write`: `791.1 ns/op`, `0 B/op`, `0 allocs/op`
- `BenchmarkInferReturnColumnsFromQuery`: `673.1 ns/op`, `128 B/op`, `2 allocs/op`
- `BenchmarkRewriteLeadingWithImports`: `1113 ns/op`, `144 B/op`, `3 allocs/op`
- `BenchmarkInCompositeScope`: `49.45 ns/op`, `0 B/op`, `0 allocs/op`

## Validation
- `go test ./pkg/fabric -count=1` passes.
- `go test ./pkg/server -run TestMultiDatabase_E2E_FullSequence/Step10_QueryCompositeDatabase -count=1` passes.
- Fabric semantics preserved for distinct dedupe and write-shard guard behavior.

