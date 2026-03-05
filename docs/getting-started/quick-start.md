# NornicDB Quick Start Guide

## 🚀 Running the Database Locally

### Option 1: NPM Scripts (Recommended)

```bash
# Run the database directly (no build needed)
npm run db

# Or with custom port
npm run db:port 7688

# Build binary first, then run
npm run db:start
```

### Option 2: Go Commands

```bash
# Run directly
go run ./cmd/nornicdb-bolt

# With custom port and data directory
go run ./cmd/nornicdb-bolt -port 7688 -data ./mydata

# Build and run
go build -o bin/nornicdb-bolt ./cmd/nornicdb-bolt
./bin/nornicdb-bolt
```

## 📊 Default Configuration

- **Port**: 7687 (Neo4j Bolt protocol default)
- **Data Directory**: ./data
- **Default Database**: `nornic` (like Neo4j's `neo4j`)
- **Protocol**: Bolt 4.x
- **Authentication**: None (for development)
- **Multi-Database**: Enabled (create multiple isolated databases)

## 🔌 Connecting with Neo4j Drivers

### Python
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    result = session.run("CREATE (n:Person {name: 'Alice'}) RETURN n")
    print(result.single()[0])
```

### JavaScript
```javascript
const neo4j = require('neo4j-driver');

const driver = neo4j.driver(
  'bolt://localhost:7687',
  neo4j.auth.basic('', '')  // No auth for dev
);

const session = driver.session();
const result = await session.run(
  'CREATE (n:Person {name: $name}) RETURN n',
  { name: 'Alice' }
);
console.log(result.records[0].get('n'));
```

### Go
```go
import "github.com/neo4j/neo4j-go-driver/v5/neo4j"

driver, err := neo4j.NewDriver("bolt://localhost:7687", neo4j.NoAuth())
defer driver.Close(ctx)

session := driver.NewSession(ctx, neo4j.SessionConfig{})
defer session.Close(ctx)

result, err := session.Run(ctx,
    "CREATE (n:Person {name: $name}) RETURN n",
    map[string]any{"name": "Alice"})
```

## 🧪 Running Tests

```bash
# All tests
npm test

# With coverage report
npm run test:coverage

# Specific packages
npm run test:cypher
npm run test:storage
npm run test:bolt
```

## 📝 Mimir Integration

For Mimir initialization, run these commands after starting the database:

```cypher
// 1. Create unique constraint
CREATE CONSTRAINT node_id_unique IF NOT EXISTS 
FOR (n:Node) REQUIRE n.id IS UNIQUE;

// 2. Create fulltext index
CREATE FULLTEXT INDEX node_search IF NOT EXISTS
FOR (n:Node) ON EACH [n.properties];

// 3. Create type index
CREATE INDEX node_type IF NOT EXISTS
FOR (n:Node) ON (n.type);

// 4. Create vector index
CREATE VECTOR INDEX node_embedding_index IF NOT EXISTS
FOR (n:Node) ON (n.embedding)
OPTIONS {indexConfig: {`vector.dimensions`: 1024}};
```

## 🛑 Stopping the Database

Press `Ctrl+C` in the terminal where the database is running.

## 📋 Available Commands

| Command | Description |
|---------|-------------|
| `npm run db` | Run database directly (development) |
| `npm run db:build` | Build binary to ./bin/nornicdb-bolt |
| `npm run db:start` | Build and run |
| `npm run db:dev` | Run on port 7687 (explicit) |
| `npm run db:port 8000` | Run on custom port |
| `npm test` | Run all tests |
| `npm run test:coverage` | Run tests with coverage report |

## 🔍 Troubleshooting

### Port Already in Use
```bash
# Find process using port 7687
lsof -i :7687

# Kill the process
kill -9 <PID>

# Or run on different port
npm run db:port 7688
```

### Build Errors
```bash
# Clean and rebuild
go clean
go build ./cmd/nornicdb-bolt
```

### Connection Refused
- Ensure the database is running (`npm run db`)
- Check the port (default: 7687)
- Verify no firewall blocking the port

## 📚 Next Steps

- Read [Feature Parity](../neo4j-migration/feature-parity.md) for feature status
- Check [Multi-Database Guide](../user-guides/multi-database.md) for schema/database management
- Review [Cypher Compatibility](../neo4j-migration/cypher-compatibility.md) for compatibility details
