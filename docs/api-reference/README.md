# API Reference

**Complete API documentation for NornicDB.**

## 📚 Documentation Sections

### Cypher Functions

- **[Function Index](cypher-functions/)** - Complete list of all 62 functions
- **[String Functions](cypher-functions/#string-functions-15-functions)** - Text manipulation
- **[Math Functions](cypher-functions/#mathematical-functions-7-functions)** - Calculations
- **[Aggregation Functions](cypher-functions/#aggregation-functions-2-functions)** - COUNT, SUM, AVG
- **[List Functions](cypher-functions/#list-functions-9-functions)** - Array operations
- **[Date/Time Functions](cypher-functions/#datetime-functions-4-functions)** - Date/time
- **[Node & Relationship Functions](cypher-functions/#node--relationship-functions-11-functions)** - Graph operations

### HTTP API

- **[REST Endpoints](OPENAPI.md)** - HTTP API documentation
- **[OpenAPI/Swagger Spec](openapi.yaml)** - Interactive API documentation
- **[OpenAPI Guide](OPENAPI.md)** - How to use the OpenAPI specification
- **[Transaction API](OPENAPI.md)** - ACID transactions
- **[Search Endpoints](OPENAPI.md)** - Vector and hybrid search
- **[Admin Endpoints](OPENAPI.md)** - System management

### GraphQL API

- **[GraphQL Guide](../user-guides/graphql.md)** - Complete GraphQL API guide
- **[Subscriptions](../user-guides/graphql.md#real-time-subscriptions)** - Real-time subscriptions
- **[Queries & Mutations](../user-guides/graphql.md)** - Query and mutation examples

### Protocols

- **[Bolt Protocol](bolt-protocol.md)** - Binary protocol specification
- **[Client Drivers](../neo4j-migration/feature-parity.md)** - Compatible drivers

## 🚀 Quick Start

### Using Cypher Functions

```cypher
// String functions
RETURN toLower("HELLO") AS lowercase

// Math functions
RETURN sqrt(16) AS squareRoot

// Aggregations
MATCH (p:Person)
RETURN count(p) AS total, avg(p.age) AS averageAge
```

### Using HTTP API

```bash
# Execute Cypher query
curl -X POST http://localhost:7474/db/data/tx/commit \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [{
      "statement": "MATCH (n:Person) RETURN n LIMIT 10"
    }]
  }'
```

### Using Bolt Protocol

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    result = session.run("MATCH (n:Person) RETURN n LIMIT 10")
    for record in result:
        print(record["n"])
```

## 📖 Function Categories

### String Functions (15 functions)

Transform and manipulate text data.

**Common:** `toLower()`, `toUpper()`, `trim()`, `substring()`, `replace()`

[See all string functions →](cypher-functions/#string-functions-15-functions)

### Mathematical Functions (7 functions)

Perform calculations and transformations.

**Common:** `abs()`, `round()`, `sqrt()`, `rand()`

[See all math functions →](cypher-functions/#mathematical-functions-7-functions)

### Aggregation Functions (6 functions)

Summarize data across multiple rows.

**Common:** `count()`, `sum()`, `avg()`, `min()`, `max()`, `collect()`

[See all aggregation functions →](cypher-functions/#aggregation-functions-2-functions)

### List Functions (8 functions)

Work with arrays and collections.

**Common:** `size()`, `head()`, `tail()`, `range()`

[See all list functions →](cypher-functions/#list-functions-9-functions)

### Temporal Functions (4 functions)

Handle dates, times, and durations.

**Common:** `timestamp()`, `date()`, `datetime()`, `duration()`

[See all date/time functions →](cypher-functions/#datetime-functions-4-functions)

### Node & Relationship Functions (11 functions)

Access graph structure and metadata.

**Common:** `id()`, `labels()`, `type()`, `properties()`, `nodes()`, `relationships()`

[See all node/relationship functions →](cypher-functions/#node--relationship-functions-11-functions)

## 🌐 HTTP API Endpoints

### Neo4j Compatible

```
GET  /                           - Discovery endpoint
GET  /db/{name}                  - Database info
POST /db/{name}/tx/commit       - Execute query (implicit transaction)
POST /db/{name}/tx              - Begin transaction
POST /db/{name}/tx/{id}         - Execute in transaction
POST /db/{name}/tx/{id}/commit  - Commit transaction
DELETE /db/{name}/tx/{id}       - Rollback transaction
```

### NornicDB Extensions

#### Authentication
```
POST /auth/token                - Get JWT token
POST /auth/logout               - Logout
GET  /auth/me                   - Current user info
POST /auth/api-token             - Generate API token (admin)
GET  /auth/oauth/redirect       - OAuth redirect
GET  /auth/oauth/callback        - OAuth callback
GET  /auth/users                 - List users (admin)
POST /auth/users                 - Create user (admin)
GET  /auth/users/{username}      - Get user (admin)
PUT  /auth/users/{username}      - Update user (admin)
DELETE /auth/users/{username}    - Delete user (admin)
```

#### Search & Embeddings
```
POST /nornicdb/search            - Hybrid search (vector + BM25)
POST /nornicdb/similar           - Vector similarity search
GET  /nornicdb/decay             - Memory decay statistics
POST /nornicdb/embed/trigger     - Trigger embedding generation
GET  /nornicdb/embed/stats       - Embedding statistics
POST /nornicdb/embed/clear       - Clear all embeddings (admin)
POST /nornicdb/search/rebuild    - Rebuild search indexes
```

#### Admin & System
```
GET  /admin/stats                - System statistics (admin)
GET  /admin/config               - Server configuration (admin)
POST /admin/backup               - Create backup (admin)
GET  /admin/gpu/status           - GPU status (admin)
POST /admin/gpu/enable           - Enable GPU (admin)
POST /admin/gpu/disable          - Disable GPU (admin)
POST /admin/gpu/test             - Test GPU (admin)
```

#### GDPR Compliance
```
GET  /gdpr/export                - GDPR data export
POST /gdpr/delete                - GDPR erasure request
```

#### GraphQL & AI
```
POST /graphql                    - GraphQL endpoint
GET  /graphql/playground         - GraphQL Playground
POST /mcp                        - MCP server endpoint
POST /api/bifrost/chat/completions - Heimdall AI chat
```

[See complete HTTP API documentation →](OPENAPI.md)  
[OpenAPI/Swagger Specification →](openapi.yaml)

## 🔌 Client Drivers

### Official Neo4j Drivers

NornicDB is compatible with official Neo4j drivers:

- **Python:** `neo4j` package
- **JavaScript:** `neo4j-driver` package
- **Java:** Neo4j Java Driver
- **Go:** `neo4j-go-driver`
- **.NET:** Neo4j.Driver

[See driver compatibility →](../neo4j-migration/feature-parity.md)

### Example Usage

**Python:**

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("admin", "admin")
)

with driver.session() as session:
    result = session.run(
        "MATCH (p:Person {name: $name}) RETURN p",
        name="Alice"
    )
    for record in result:
        print(record["p"])
```

**JavaScript:**

```javascript
const neo4j = require("neo4j-driver");

const driver = neo4j.driver(
  "bolt://localhost:7687",
  neo4j.auth.basic("admin", "admin")
);

const session = driver.session();
const result = await session.run("MATCH (p:Person {name: $name}) RETURN p", {
  name: "Alice",
});

result.records.forEach((record) => {
  console.log(record.get("p"));
});
```

## 📚 Related Documentation

- **[User Guides](../user-guides/)** - How to use features
- **[Features](../features/)** - Feature documentation
- **[Getting Started](../getting-started/)** - Installation and setup

## 🔍 Search This Documentation

Looking for a specific function or endpoint?

- **[Cypher Functions Index](cypher-functions/)** - Searchable function list
- **[HTTP API Reference](OPENAPI.md)** - All endpoints
- **[OpenAPI/Swagger Spec](openapi.yaml)** - Interactive API documentation
- **[Bolt Protocol Spec](bolt-protocol.md)** - Protocol details

## 📋 OpenAPI/Swagger Specification

We provide a complete OpenAPI 3.0 specification for all REST API endpoints:

- **[openapi.yaml](openapi.yaml)** - Complete OpenAPI specification

### Using the OpenAPI Spec

You can use the OpenAPI specification with tools like:

- **Swagger UI**: Interactive API documentation and testing
- **Postman**: Import the spec to test endpoints
- **Insomnia**: Import for API testing
- **Code Generation**: Generate client libraries in various languages

### Quick Start with Swagger UI

1. Download the OpenAPI spec: `docs/api-reference/openapi.yaml`
2. Visit [Swagger Editor](https://editor.swagger.io/)
3. Import the `openapi.yaml` file
4. Test endpoints directly from the browser

### Example: Testing with curl

```bash
# Get authentication token
curl -X POST http://localhost:7474/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "password123"}'

# Use token for authenticated requests
curl -X POST http://localhost:7474/nornicdb/search \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "machine learning", "limit": 10}'
```

---

**Ready to start?** → **[Cypher Functions](cypher-functions/)**  
**Need examples?** → **[User Guides](../user-guides/)**  
**Test the API?** → **[OpenAPI Spec](openapi.yaml)**
