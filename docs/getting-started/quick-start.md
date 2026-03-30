# NornicDB Quick Start Guide

Get NornicDB running locally, verify that it is healthy, and execute a first query.

## Run NornicDB Locally

### Option 1: Docker

This is the fastest way to get a working local instance.

```bash
# Apple Silicon / arm64
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-bge:latest

# x86_64 / CPU-only
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cpu-bge:latest
```

Open `http://localhost:7474` for the admin UI.

### Option 2: Run From Source

```bash
# Clone the repository
git clone https://github.com/orneryd/nornicdb.git
cd nornicdb

# Build the CLI
go build -o nornicdb ./cmd/nornicdb

# Start the server
./nornicdb serve
```

Run with a custom data directory or ports when needed:

```bash
./nornicdb serve \
  --data-dir ./mydata \
  --bolt-port 7688 \
  --http-port 7475
```

## Verify The Server

```bash
curl http://localhost:7474/health
```

Expected result:

```json
{ "status": "ok" }
```

## Default Local Ports

- Bolt: `7687`
- HTTP/UI: `7474`
- Default data directory: `./data` when running from source
- Default database: `nornic`

## Run Your First Query

### Browser UI

1. Open `http://localhost:7474`
2. Run:

```cypher
CREATE (n:Person {name: 'Alice'}) RETURN n;
```

Then verify it:

```cypher
MATCH (n:Person) RETURN n.name;
```

### Neo4j-Compatible Driver

Python example:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    result = session.run("CREATE (n:Person {name: 'Alice'}) RETURN n.name AS name")
    print(result.single()["name"])
```

Go example:

```go
import "github.com/neo4j/neo4j-go-driver/v5/neo4j"

driver, err := neo4j.NewDriver("bolt://localhost:7687", neo4j.NoAuth())
if err != nil {
    panic(err)
}
defer driver.Close(ctx)
```

## Enable Semantic Search

Embedding generation is disabled by default in current releases. If you want NornicDB to generate embeddings automatically for semantic and hybrid search, enable it explicitly.

```bash
./nornicdb serve --embedding-enabled
```

Or with Docker:

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NORNICDB_EMBEDDING_ENABLED=true \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-bge:latest
```

See [Vector Search](../user-guides/vector-search.md) and [Hybrid Search](../user-guides/hybrid-search.md) for search behavior and configuration.

## Common CLI Commands

```bash
# Version and help
./nornicdb version
./nornicdb --help

# Initialize a data directory
./nornicdb init --data-dir ./data

# Start the server
./nornicdb serve

# Interactive shell
./nornicdb shell
```

## Run Tests

NornicDB uses Go tooling for tests, not npm scripts.

```bash
# Full test suite
go test ./...

# Focused packages
go test ./pkg/cypher/...
go test ./pkg/storage/...
go test ./pkg/bolt/...

# Coverage
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out
```

## Stop The Server

- Docker: `docker stop nornicdb`
- Foreground process: `Ctrl+C`

## Troubleshooting

### Port Already In Use

```bash
lsof -i :7687
lsof -i :7474
```

Then either stop the conflicting process or run NornicDB on different ports:

```bash
./nornicdb serve --bolt-port 7688 --http-port 7475
```

### Build Errors

```bash
go clean
go build -o nornicdb ./cmd/nornicdb
```

### Connection Refused

- Ensure the server is running with `./nornicdb serve` or Docker
- Check that you are connecting to the correct Bolt and HTTP ports
- Verify that `curl http://localhost:7474/health` succeeds

## Next Steps

- [Installation](installation.md)
- [First Queries](first-queries.md)
- [Vector Search](../user-guides/vector-search.md)
- [Hybrid Search](../user-guides/hybrid-search.md)
- [Feature Parity](../neo4j-migration/feature-parity.md)
