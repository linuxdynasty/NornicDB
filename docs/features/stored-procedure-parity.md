# Stored Procedure Parity Matrix

This document tracks the current `CALL` parity contract in NornicDB.

## Runtime Contract

- A single registry-backed runtime resolves both built-in and user-defined procedures.
- `SHOW PROCEDURES` and `CALL dbms.procedures()` are generated from the same registry.
- Procedure metadata includes:
  - canonical `name`
  - `signature`
  - `description`
  - `mode` (`READ`, `WRITE`, `DBMS`)
  - `worksOnSystem`
  - argument cardinality (`minArgs`, `maxArgs`)

## Compatibility Behavior

- Unknown procedure: `unknown procedure: <name> (try SHOW PROCEDURES for available procedures)`
- Argument arity mismatch:
  - minimum: `procedure <name> requires at least N arguments, got M`
  - maximum: `procedure <name> accepts at most N arguments, got M`
- Unknown YIELD column: `unknown YIELD column: <column>`

## Built-in Procedure Surface

The built-in catalog is registered in `pkg/cypher/procedure_registry_builtin.go`.
This includes:

- Core schema/info procedures (`db.labels`, `db.relationshipTypes`, `db.propertyKeys`, `db.info`, `db.ping`)
- Fulltext and vector procedures (`db.index.fulltext.*`, `db.index.vector.*`, `db.create.set*VectorProperty`)
- DBMS procedures (`dbms.components`, `dbms.info`, `dbms.listConfig`, `dbms.clientConfig`, `dbms.listConnections`, `dbms.procedures`, `dbms.functions`)
- Index/statistics/ops procedures (`db.await*`, `db.resampleIndex`, `db.stats.*`, `db.clearQueryCaches`, `tx.setMetaData`)
- NornicDB procedures (`nornicdb.version`, `nornicdb.stats`, `nornicdb.decay.info`)

## User-Defined Procedure Plugins

Procedure plugins are loaded via the existing plugin system:

- Plugin may expose `Procedures() map[string]...` in addition to functions.
- Procedure handlers are registered into the same global runtime registry.
- User procedures are visible in both `SHOW PROCEDURES` and `dbms.procedures()`.

Supported user-procedure handler shape:

- `func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error)`

## Migration Guidance

- Existing procedure callers do not need query changes.
- Prefer exact procedure names (`CALL dbms.procedures()`) over broad pattern matching.
- For plugin authors, provide `Signature`, `Mode`, `MinArgs`, and `MaxArgs` to enable strict runtime validation.
