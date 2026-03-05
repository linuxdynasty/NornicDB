# NornicDB Documentation

Welcome to **NornicDB** - A high-performance graph database with GPU acceleration, Neo4j compatibility, and advanced AI integration.

## 🚀 Quick Start Paths

### New to NornicDB?

👉 **[Getting Started Guide](getting-started/)** - Installation and first queries in 5 minutes

### Migrating from Neo4j?

👉 **[Neo4j Migration Guide](neo4j-migration/)** - 96% feature parity, easy migration

### Building AI Agents?

👉 **[AI Integration Guide](ai-agents/)** - Cursor, MCP, and agent patterns

### Need API Reference?

👉 **[API Documentation](api-reference/)** - Complete function reference

---

## 📖 Documentation Sections

### For Users

- **[Getting Started](getting-started/)** - Installation, quick start, first queries
- **[User Guides](user-guides/)** - Cypher queries, vector search, transactions
- **[API Reference](api-reference/)** - Complete function and endpoint documentation
- **[Features](features/)** - Memory decay, GPU acceleration, link prediction

### For Developers

- **[Architecture](architecture/)** - System design, storage engine, query execution
- **[Performance](performance/)** - Benchmarks, optimization, GPU acceleration
- **[Advanced Topics](advanced/)** - K-Means clustering, embeddings, custom functions
- **[Development](development/)** - Contributing, testing, code style

### For Operations

- **[Operations Guide](operations/)** - Deployment, monitoring, backup, scaling
- **[Security Guide](security/)** - HTTP security, CSRF/SSRF/XSS protection
- **[Clustering Guide](user-guides/clustering.md)** - Hot Standby, Raft, Multi-Region
- **[Compliance](compliance/)** - GDPR, HIPAA, SOC2, encryption, audit logging

### For AI Integration

- **[AI Agents](ai-agents/)** - Cursor integration, chat modes, MCP tools
- **[Neo4j Migration](neo4j-migration/)** - Feature parity, migration guide

## 🎯 Key Features

### 🧠 Graph-Powered Memory

- Semantic relationships between data
- Multi-hop graph traversal
- Automatic relationship inference
- Memory decay simulation

### 🚀 GPU Acceleration

- 10-100x speedup for vector search
- Multi-backend support (CUDA, OpenCL, Metal, Vulkan)
- Automatic CPU fallback
- Memory-optimized embeddings

### 🔍 Advanced Search

- Vector similarity search with cosine similarity
- Full-text search with BM25 scoring
- Hybrid search (RRF) combining both methods
- Cross-encoder reranking (Stage 2 retrieval; local GGUF or external API)
- MMR diversification for result variety
- HNSW indexing for O(log N) performance
- Eval harness for search quality validation

### 🔗 Neo4j Compatible

- Bolt protocol support
- Cypher query language
- Standard Neo4j drivers work out-of-the-box
- Easy migration from Neo4j

### 📡 GraphQL API

- Full GraphQL API with queries and mutations
- Real-time subscriptions for live updates
- Schema introspection and auto-complete
- GraphQL Playground for interactive exploration

### 📋 OpenAPI/Swagger Specification

- Complete OpenAPI 3.0 specification for all REST endpoints
- Interactive API documentation with Swagger UI
- Easy integration with API testing tools (Postman, Insomnia)
- Client library generation support

### 🔐 Enterprise-Ready

- **High Availability** - Hot Standby, Raft consensus, Multi-Region
- **Security Middleware** - CSRF/SSRF/XSS protection on all HTTP endpoints
- **WAL Auto-Compaction** - Automatic snapshots prevent unbounded disk growth
- GDPR, HIPAA, SOC2 compliance
- RBAC and audit logging
- ACID transactions

## 📊 Documentation Statistics

- **21 packages** fully documented
- **13,400+ lines** of GoDoc comments
- **350+ functions** with examples
- **40+ ELI12 explanations** for complex concepts
- **4.1:1 documentation-to-code ratio**

## 🎯 Popular Topics

- [HTTP Security Guide](security/http-security.md) ⭐ **NEW**
- [WAL Compaction Guide](operations/wal-compaction.md) ⭐ **NEW**
- [Durability Configuration](operations/durability.md) ⭐ **NEW**
- [Clustering & High Availability](user-guides/clustering.md)
- [GraphQL API Guide](user-guides/graphql.md) ⭐ **NEW**
- [Vector Search Guide](user-guides/vector-search.md)
- [Hybrid Search (RRF)](user-guides/hybrid-search.md)
- [GPU Acceleration](features/gpu-acceleration.md)
- [Memory Decay System](features/memory-decay.md)
- [Cypher Function Reference](api-reference/cypher-functions/)
- [Benchmarks vs Neo4j](performance/benchmarks-vs-neo4j.md)
- [Docker Deployment](getting-started/docker-deployment.md)
- [Feature Flags](features/feature-flags.md)

## 📋 Project Status

- **Version:** 1.0.0 🎉
- **Status:** Production Ready ✅
- **Docker:** `timothyswt/nornicdb-arm64-metal:latest`
- **[Changelog](../CHANGELOG.md)** - Version history and release notes

## 🤝 Contributing

Found an issue or want to improve documentation? Check out our [Contributing Guide](../CONTRIBUTING.md).

## 📄 License

NornicDB is MIT licensed. See [LICENSE](../LICENSE.md) for details.

---

**Last Updated:** December 4, 2025  
**Version:** 1.0.0 🎉  
**Docker:** `timothyswt/nornicdb-arm64-metal:latest`  
**Status:** Production Ready ✅
