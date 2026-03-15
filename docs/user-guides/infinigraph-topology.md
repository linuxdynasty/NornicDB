# Implementing a Neo4j-Style Infinigraph Topology

Neo4j sometimes describes large distributed graph layouts as an "infinigraph": one logical graph entry point backed by multiple databases, shards, or regions.

In NornicDB, the equivalent pattern is a **composite database** with a mix of **local** and **remote constituents**.

This works today.

What you get is a **logical distributed graph topology**:

- one coordinator entry point
- multiple constituent databases
- explicit query routing with `USE <composite>.<alias>`
- support for local and remote constituents in the same topology
- forwarded caller identity or fixed service credentials for remote access

What you do **not** get is a single physically merged graph:

- no direct relationships across constituents
- no automatic repartitioning or global graph reshaping
- no distributed multi-write ACID transaction across multiple shards

If you already know Neo4j Fabric, this is the same mental model to use in NornicDB.

## When To Use This Pattern

Use this topology when one logical application graph needs to be split across boundaries that you still want to query through one coordinator:

- **regional partitioning**: `company.us`, `company.eu`, `company.apac`
- **tenant partitioning**: `tenant_a`, `tenant_b`, `tenant_c`
- **domain partitioning**: `users`, `orders`, `search`, `billing`
- **migration bridges**: keep one graph local while another moves to a remote NornicDB cluster

This is especially useful when the application already understands graph boundaries and can issue explicit `USE` targets inside subqueries.

## Topology Model

The standard layout is:

- one **coordinator** NornicDB instance that owns the composite database definition
- zero or more **local constituents** on the coordinator
- zero or more **remote constituents** hosted by other NornicDB instances

Example:

- `company.users` on the coordinator
- `company.orders` on the coordinator
- `company.search` on a remote search-focused instance
- `company.eu` on a remote Europe region instance

The coordinator does not physically merge those databases. It routes query fragments to them and merges the results according to the query plan.

## What Maps To Neo4j "Infinigraph"

If you are translating from Neo4j terminology, treat the mapping like this:

- Neo4j composite database / Fabric topology -> NornicDB composite database
- Neo4j local alias -> NornicDB local constituent alias
- Neo4j remote alias -> NornicDB remote constituent alias
- Fabric subquery routing -> `CALL { USE <composite>.<alias> ... }`
- Federated graph join pattern -> proxy IDs and correlated subqueries

The important caveat is the same in both systems: **relationships stay local to a constituent database**.

## Step 1: Design The Shard Boundaries

Before writing Cypher, choose what each constituent owns.

Good boundaries:

- a region
- a tenant
- a bounded business domain
- a workload specialization such as search or analytics

Avoid boundaries that require direct cross-database relationships for every query. A distributed topology works best when joins can be expressed through application identifiers such as `userId`, `tenantId`, `orderId`, or `documentId`.

## Step 2: Create The Composite Database

Local-only example:

```cypher
CREATE COMPOSITE DATABASE company
  ALIAS users FOR DATABASE users_db
  ALIAS orders FOR DATABASE orders_db
  ALIAS search FOR DATABASE search_db
```

Mixed local and remote example:

```cypher
CREATE COMPOSITE DATABASE company
  ALIAS users FOR DATABASE users_db
  ALIAS orders FOR DATABASE orders_db
  ALIAS eu FOR DATABASE europe_db
    AT "https://eu-db.example.internal/nornic-db"
    OIDC CREDENTIAL FORWARDING
    TYPE remote
    ACCESS read_write
  ALIAS search FOR DATABASE search_db
    AT "https://search.example.internal/nornic-db"
    USER "svc-search"
    PASSWORD "strong-password"
    TYPE remote
    ACCESS read
```

Inspect the topology:

```cypher
SHOW CONSTITUENTS FOR COMPOSITE DATABASE company
```

Use `TYPE remote` when the target database lives on another NornicDB instance. Use `ACCESS read`, `write`, or `read_write` to document the intended access mode for that constituent.

## Step 3: Choose The Remote Auth Model

NornicDB supports two remote auth modes.

### Option A: Forward The Caller Identity

Use this when the remote database should evaluate the same principal that authenticated to the coordinator.

Explicit form:

```cypher
CREATE COMPOSITE DATABASE company
  ALIAS eu FOR DATABASE europe_db
    AT "https://eu-db.example.internal/nornic-db"
    OIDC CREDENTIAL FORWARDING
    TYPE remote
    ACCESS read
```

Implicit default form:

```cypher
CREATE COMPOSITE DATABASE company
  ALIAS eu FOR DATABASE europe_db
    AT "https://eu-db.example.internal/nornic-db"
    TYPE remote
    ACCESS read
```

Behavior:

- the coordinator reads the incoming `Authorization` header
- that header is forwarded to the remote constituent
- the remote server applies auth and RBAC for that same caller context

Use this when you want end-to-end user or service-principal identity propagation.

### Option B: Use Fixed Service Credentials

Use this when the coordinator should always connect to the remote database as a specific service account.

```cypher
CREATE COMPOSITE DATABASE company
  ALIAS search FOR DATABASE search_db
    AT "https://search.example.internal/nornic-db"
    USER "svc-search"
    PASSWORD "strong-password"
    TYPE remote
    ACCESS read
```

Behavior:

- outbound remote requests use the configured username and password
- the remote server authorizes that fixed account, not the original caller
- remote passwords are encrypted before composite metadata is persisted

Use this when the remote shard should be accessed through a stable integration identity.

### Auth Rules

- `AT "<url>" OIDC CREDENTIAL FORWARDING` forwards caller auth
- `AT "<url>"` with no auth clause also defaults to forwarding caller auth
- `AT "<url>" USER "..." PASSWORD "..."` uses explicit service credentials
- `USER` and `PASSWORD` must be provided together
- `USER` / `PASSWORD` cannot be combined with `OIDC CREDENTIAL FORWARDING`

## Step 4: Query Constituents Explicitly

Composite roots are routing contexts, not implicit merged graph targets.

Query a single constituent directly:

```cypher
USE company.users
MATCH (u:User)
WHERE u.email = 'alice@example.com'
RETURN u.id, u.email, u.name
```

Query a remote constituent the same way:

```cypher
USE company.eu
MATCH (c:Customer)
WHERE c.country = 'DE'
RETURN c.id, c.name, c.tier
LIMIT 25
```

The client-side query shape is the same for local and remote constituents. The routing difference is handled by the composite catalog and Fabric executor.

## Step 5: Model Cross-Constituent Navigation With Proxy IDs

Do not model cross-constituent access as direct relationships. Model it as shared identifiers and explicit subquery routing.

Example:

- `company.users` owns `User` nodes with `u.id`
- `company.orders` owns `Order` nodes with `o.user_id`

Query pattern:

```cypher
USE company

CALL {
  USE company.users
  MATCH (u:User)
  WHERE u.region = 'eu'
  RETURN u.id AS userId, u.email AS email
}

CALL {
  USE company.orders
  WITH userId
  MATCH (o:Order {user_id: userId})
  RETURN collect({
    id: o.id,
    total: o.total,
    status: o.status
  }) AS orders
}

RETURN userId, email, orders
LIMIT 20
```

This is the core pattern for Neo4j-style federated graph design in NornicDB:

- fetch identifiers from one constituent
- pass them forward with `WITH`
- route the next subquery to another constituent
- merge results in the outer query

## Step 6: Use Nested `CALL { USE ... }` For Multi-Shard Workflows

Nested or sequential routed subqueries are the normal way to orchestrate work across constituents.

```cypher
USE company

CALL {
  USE company.users
  MATCH (u:User)
  WHERE u.active = true
  RETURN u.id AS userId, u.segment AS segment
}

CALL {
  USE company.search
  WITH userId, segment
  MATCH (d:Document)
  WHERE d.owner_id = userId AND d.segment = segment
  RETURN count(d) AS docCount
}

RETURN userId, segment, docCount
ORDER BY docCount DESC
LIMIT 10
```

This is the NornicDB form of a logical graph workflow spanning multiple databases.

## Step 7: Understand Transaction Boundaries

NornicDB supports explicit composite transactions for local and remote constituents, but the write model is intentionally constrained.

Current behavior:

- reads can span multiple constituents in one transaction
- a transaction may write to only one constituent shard
- attempting a write on a second shard returns a deterministic transaction error
- remote participants use real remote transaction handles during explicit transactions

This means the topology supports **many-read/one-write** flows, not distributed multi-write ACID.

Practical guidance:

- keep writes localized to one constituent per transaction
- perform cross-constituent enrichment as reads around that write boundary
- use idempotent application workflows when multiple shards must be updated over time

## Step 8: Operate The Topology Safely

### Recommended Coordinator Practices

- define composites on a stable coordinator instance
- choose shard aliases that reflect ownership clearly
- document which constituents are local vs remote
- standardize whether each remote uses forwarded auth or service credentials

### Recommended Data Modeling Practices

- use globally meaningful business identifiers
- keep each edge local to one constituent
- store foreign keys for cross-constituent lookup
- avoid designs that require recursive hopping between many shards in one request path

### Remote Credential Considerations

If you use `USER` / `PASSWORD`, the coordinator needs remote credential encryption configured. Prefer a dedicated `NORNICDB_REMOTE_CREDENTIALS_KEY` rather than falling back to broader encryption or JWT secrets.

## Example Topologies

### Regional Topology

- `company.us`
- `company.eu`
- `company.apac`

Use when data residency or latency drives regional partitioning.

### Tenant Topology

- `saas.tenant_a`
- `saas.tenant_b`
- `saas.tenant_c`

Use when each tenant needs strong isolation but operations still need coordinator-driven reporting workflows.

### Domain Topology

- `company.users`
- `company.orders`
- `company.search`
- `company.analytics`

Use when different workloads need different infrastructure or operational policies.

## Current Constraints

These are the constraints to design around today:

- no direct relationships across constituents
- no plain `MATCH` or `CREATE` on the composite root without explicit `USE` targeting
- no composite-root search execution; search endpoints target a constituent database instead
- no multi-write distributed ACID across multiple constituent shards in one transaction

These are design constraints, not temporary documentation omissions. Build the topology around them.

## Troubleshooting

### Remote auth works locally but fails remotely

Check which auth mode the constituent uses:

- `OIDC CREDENTIAL FORWARDING` means the remote shard authorizes the original caller
- `USER` / `PASSWORD` means the remote shard authorizes the configured service account

### Remote writes fail in a transaction after another write succeeded

This usually means the transaction has already opened a write participant on another constituent. Split the workflow so each transaction writes to only one shard.

### A query needs relationships across shards

That topology is not supported directly. Remodel the interaction around shared identifiers and routed subqueries.

## See Also

- [Multi-Database Support](multi-database.md)
- [Transactions](transactions.md)
- [Clustering](clustering.md)
