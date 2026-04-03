# REQUIRE Block Constraint Specification

**Status:** Proposed specification  
**Audience:** Coding agents and maintainers implementing schema-contract extensions  
**Goal:** Add `REQUIRE { ... }` block support on top of the existing primitive constraint system, without restating or reintroducing constraint work that is already complete.

---

## 1. Scope

This document defines the REQUIRE block constraint implementation.

Assume all of the following already exist and must be reused rather than redesigned here:

- single-predicate `CREATE CONSTRAINT ... FOR ... REQUIRE ...` on nodes
- standalone relationship-scoped primitive constraints
- current primitive storage, enforcement, conflict handling, and `SHOW CONSTRAINTS`
- current single-constraint extensions outside block syntax

This spec is therefore limited to:

- parsing `REQUIRE { ... }` blocks
- storing block contracts as metadata
- compiling node- and relationship-local primitive block entries into existing primitives
- validating residual boolean entries at create time and on writes
- contract-specific introspection

Nested relationship mini-constraints inside blocks are excluded from this specification.

---

## 2. Summary

NornicDB should accept:

```cypher
CREATE CONSTRAINT person_contract
FOR (n:Person)
REQUIRE {
  n.id IS UNIQUE
  n.name IS NOT NULL
  n.age IS :: INTEGER
  n.status IN ['active', 'inactive']
  (n.tenant, n.externalId) IS NODE KEY
  COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1
  NOT EXISTS { (n)-[:FORBIDDEN_REL]->() }
}
```

And:

```cypher
CREATE CONSTRAINT works_at_contract
FOR ()-[r:WORKS_AT]-()
REQUIRE {
  r.id IS UNIQUE
  r.startedAt IS NOT NULL
  r.role IS :: STRING
  (r.tenant, r.externalId) IS RELATIONSHIP KEY
  startNode(r) <> endNode(r)
  startNode(r).tenant = endNode(r).tenant
  r.status IN ['active', 'inactive']
  r.hoursPerWeek > 0
}
```

The implementation must:

1. Parse `REQUIRE { ... }` blocks while preserving existing single-predicate `REQUIRE` behavior.
2. Store block constraints as first-class contract metadata.
3. Compile node-local and relationship-local primitive entries into existing primitive constraints where the semantics already match.
4. Enforce residual boolean entries at runtime.
5. Validate current data at `CREATE CONSTRAINT` time for every block entry.
6. Add contract-specific introspection without changing current `SHOW CONSTRAINTS` semantics.

---

## 3. Design Constraints

1. Keep the existing top-level DDL shape unchanged.
2. Extend only the `REQUIRE` payload.
3. Preserve current single-predicate `REQUIRE` and legacy `ASSERT` forms unchanged.
4. Reuse existing primitive constraint storage and enforcement whenever possible.
5. Do not invent a new top-level contract DSL.
6. Do not overload `SHOW CONSTRAINTS` with synthetic block rows.
7. Do not introduce nested relationship entries inside blocks.

---

## 4. Syntax

## 4.1 Top-level form

The only new syntax surface is allowing a block after `REQUIRE`:

```cypher
CREATE CONSTRAINT <name>
FOR (n:Label)
REQUIRE {
  <entry>
  <entry>
  ...
}
```

Or:

```cypher
CREATE CONSTRAINT <name>
FOR ()-[r:TYPE]-()
REQUIRE {
  <entry>
  <entry>
  ...
}
```

Example targets:

```cypher
FOR (n:Person)
FOR ()-[r:WORKS_AT]-()
```

## 4.2 Allowed block entry kinds

Each block entry must be one of:

### A. Existing primitive predicate

Examples:

```cypher
n.id IS UNIQUE
n.name IS NOT NULL
n.age IS :: INTEGER
(n.tenant, n.externalId) IS NODE KEY
(n.fact_key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP

r.id IS UNIQUE
r.startedAt IS NOT NULL
r.role IS :: STRING
(r.tenant, r.externalId) IS RELATIONSHIP KEY
```

### B. Boolean validation predicate

Examples:

```cypher
n.status IN ['active', 'inactive']
COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1
NOT EXISTS { (n)-[:FORBIDDEN_REL]->() }
ALL (rel IN relationships((n)-->) WHERE type(rel) IN ['KNOWS', 'WORKS_AT'])

startNode(r) <> endNode(r)
startNode(r).tenant = endNode(r).tenant
r.status IN ['active', 'inactive']
r.hoursPerWeek > 0
r.source IN ['hris', 'manual']
```

Block entries may not include nested `FOR ... REQUIRE ...` relationship clauses. The parser should return a deterministic error for those forms.

---

## 5. Parser Changes

The parser must:

1. Add a `requireBody` abstraction with two forms:
   - the current single-predicate form
   - the new block form
2. Parse block entries as repeated block items rather than a comma-separated list.
3. Preserve current precedence so `expression requireOperator` remains a primitive entry and everything else is treated as a boolean predicate.

Pseudo-grammar:

```antlr
requireBody
  : singleRequirePredicate
  | requireBlock
  ;

requireBlock
  : LBRACE requireEntry+ RBRACE
  ;

requireEntry
  : blockPrimitivePredicate
  | blockBooleanPredicate
  ;
```

---

## 6. Contract Metadata

Schema storage must add first-class contract metadata for block constraints.

Suggested shape:

```go
type ConstraintContract struct {
  Name             string
  TargetEntityType string
  TargetLabelOrType string
  Definition       string
  Entries          []ConstraintContractEntry
}

type ConstraintContractEntry struct {
  Kind string // primitive-node, primitive-relationship, boolean-node, boolean-relationship

  PrimitiveType string
  Properties    []string
  Property      string
  ExpectedType  string

  Expression string
}
```

Required behavior:

1. Preserve the raw definition text.
2. Preserve parsed entry metadata for introspection and runtime enforcement.
3. Keep contract metadata separate from primitive constraint storage.

---

## 7. Compilation Rules

Block entries should compile to existing primitives only where the semantics already match the current engine.

## 7.1 Compile directly to existing primitives

These entries should compile into the existing primitive schema system for the targeted entity type:

- `n.id IS UNIQUE`
- `n.name IS NOT NULL`
- `n.age IS :: INTEGER`
- `(n.tenant, n.externalId) IS NODE KEY`
- `(n.key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP`
- `r.id IS UNIQUE`
- `r.startedAt IS NOT NULL`
- `r.role IS :: STRING`
- `(r.tenant, r.externalId) IS RELATIONSHIP KEY`

## 7.2 Keep as runtime-only contract entries

These entries should remain contract metadata with runtime enforcement:

- boolean predicates such as `n.status IN [...]`
- cardinality predicates such as `COUNT { ... } <= 1`
- policy predicates such as `NOT EXISTS { ... }` or `ALL (...)`
- endpoint predicates such as `startNode(r) <> endNode(r)`
- endpoint-property predicates such as `startNode(r).tenant = endNode(r).tenant`
- relationship-property predicates such as `r.status IN [...]`
- relationship-property predicates such as `r.hoursPerWeek > 0`

Rationale:

- existing standalone relationship constraints already cover relationship-scoped primitive rules outside block syntax
- limiting block entries to existing primitive predicates plus boolean predicates keeps block enforcement aligned with the current engine

---

## 8. Enforcement

Existing primitive constraints continue to be enforced exactly as they are today.

Runtime enforcement applies only to residual block entries.

## 8.1 Write paths to cover

Residual contract validation must run on:

- node create
- node property updates
- node label changes when block applicability changes
- relationship create when a node-level boolean predicate depends on adjacent edges
- relationship property updates when a node-level boolean predicate depends on adjacent edges
- relationship create
- relationship property updates
- node property updates when a relationship-targeted boolean predicate depends on endpoint state
- node label changes when a relationship-targeted boolean predicate depends on endpoint labels
- deletes when a boolean predicate depends on presence or absence of edges
- transaction commit for batched writes

## 8.2 Evaluation scope

Do not re-scan the whole graph on every write.

Evaluate only affected contracts:

- node-targeted contracts whose target label is present on the mutated node
- node-targeted contracts whose boolean predicates reference the mutated node and its adjacent edges
- relationship-targeted contracts whose target type matches the mutated relationship
- relationship-targeted contracts whose boolean predicates reference the mutated relationship or its endpoints

## 8.3 Evaluation order

1. Existing primitive constraints
2. Residual boolean predicates from block contracts

Compiled primitive entries from a block must not be enforced twice.

---

## 9. Create-Time Validation

`CREATE CONSTRAINT ... REQUIRE { ... }` must fail if any current data violates any block entry.

That includes:

- compiled primitive node entries
- compiled primitive relationship entries
- boolean predicates

There is no partial-success mode:

- compile what can be compiled
- store the contract metadata only if all entries are valid and all existing data passes
- fail the whole statement if any entry is invalid or violated

---

## 10. Introspection

Introspection includes a contract-specific listing surface.

Recommended command:

```cypher
SHOW CONSTRAINT CONTRACTS
```

Suggested columns:

- `name`
- `targetEntityType`
- `targetLabelOrType`
- `entryCount`
- `compiledEntryCount`
- `runtimeEntryCount`
- `definition`

`SHOW CONSTRAINTS` should remain focused on actual primitive constraints and should continue to show compiled primitives exactly as it does today.

---

## 11. Error Behavior

Error messages for block-contract failures should identify:

- contract name
- failing entry kind
- failing expression or property

Example:

```text
constraint contract person_contract violated: predicate `n.status IN ['active', 'inactive']` evaluated to false
constraint contract works_at_contract violated: predicate `startNode(r).tenant = endNode(r).tenant` evaluated to false
constraint contract works_at_contract violated: predicate `r.status IN ['active', 'inactive']` evaluated to false
```

---

## 12. Test Plan

## 12.1 Parser tests

- valid block with only node primitive entries
- valid block with only relationship primitive entries
- valid block with boolean predicates
- malformed block rejected

## 12.2 Creation-time validation tests

- contract creation succeeds on valid current data
- contract creation fails on existing node primitive violations
- contract creation fails on existing relationship primitive violations
- contract creation fails on existing boolean-predicate violations, including endpoint-expression predicates

## 12.3 Runtime validation tests

- invalid enum/domain predicate write rejected
- cardinality predicate write rejected
- forbidden-relationship predicate write rejected
- invalid relationship primitive write rejected
- invalid endpoint predicate write rejected
- invalid relationship-property predicate write rejected

## 12.4 Introspection tests

- `SHOW CONSTRAINT CONTRACTS` returns contract metadata
- compiled and runtime entry counts are correct
- raw definition text is preserved

---

## 13. Non-Goals

The following are out of scope:

- nested relationship mini-constraints inside blocks
- legacy `ASSERT { ... }` block syntax
- inheritance or composition between contracts
- property defaults
- automatic migration from existing primitive constraints into block contracts
- a new top-level contract DSL

---

## 14. Acceptance Criteria

The task is complete when all of the following are true:

1. Existing single-predicate `REQUIRE` syntax still works unchanged.
2. NornicDB accepts `REQUIRE { ... }` blocks for node- and relationship-scoped constraints.
3. Node-local and relationship-local primitive entries in the block compile to existing primitive constraints.
4. Residual boolean entries are validated at create time and enforced on writes.
5. Contract metadata is stored and exposed through `SHOW CONSTRAINT CONTRACTS`.
6. Existing tests continue to pass.
7. New parser, validation, enforcement, and introspection tests cover the block behavior.

---

## 15. Canonical Anchor Query

This is the anchor query the implementation should support end-to-end:

```cypher
CREATE CONSTRAINT person_contract
FOR (n:Person)
REQUIRE {
  n.id IS UNIQUE
  n.name IS NOT NULL
  n.age IS :: INTEGER
  n.status IS :: STRING
  n.status IN ['active', 'inactive']
  (n.tenant, n.externalId) IS NODE KEY
  COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1
  NOT EXISTS { (n)-[:FORBIDDEN_REL]->() }
}

CREATE CONSTRAINT works_at_contract
FOR ()-[r:WORKS_AT]-()
REQUIRE {
  r.id IS UNIQUE
  r.startedAt IS NOT NULL
  r.role IS :: STRING
  (r.tenant, r.externalId) IS RELATIONSHIP KEY
  startNode(r) <> endNode(r)
  startNode(r).tenant = endNode(r).tenant
  r.status IN ['active', 'inactive']
  r.hoursPerWeek > 0
}
```

If this query parses, stores, validates current data, introspects, and enforces correctly, the implementation is complete.
