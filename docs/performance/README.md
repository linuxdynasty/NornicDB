# Performance

**Benchmarks, optimization guides, and performance tuning.**

## 📚 Documentation

- **[Search Methodology](searching.md)** - Complete guide to vector, full-text, RRF, and reranking
- **[Benchmarks vs Neo4j](benchmarks-vs-neo4j.md)** - Performance comparison
- **[Test Results](test-results.md)** - Test suite results
- **[Optimization Guide](http-optimization-options.md)** - Performance tuning
- **[GPU Acceleration](../features/gpu-acceleration.md)** - GPU performance
- **[Query Optimization](searching.md)** - Query tuning

## ⚡ Performance Highlights

### Vector Search
- **10-100x faster** with GPU acceleration
- **O(log N)** HNSW index lookups
- **Sub-millisecond** queries on 1M vectors

### Query Execution
- **Parallel execution** for independent operations
- **Query caching** with LRU eviction
- **Index-backed** property lookups

### Storage
- **Badger LSM** for write-heavy workloads
- **Batch writes** for bulk imports
- **Compression** for reduced disk usage

## 📊 Benchmarks

See **[Benchmarks vs Neo4j](benchmarks-vs-neo4j.md)** for detailed comparisons.

---

**Optimize your database** → **[Optimization Guide](http-optimization-options.md)**
