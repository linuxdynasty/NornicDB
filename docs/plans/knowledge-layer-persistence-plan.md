# Constraint-Driven Persistence and Decay Plan

**Status:** Proposed
**Date:** April 15, 2026
**Scope:** Replace hardcoded memory-tier decay behavior with a generic, constraint-driven persistence and decay system that can support existing, proposed, or future decay models.

---

## 1. Objective

Implement a flexible persistence and decay architecture in NornicDB where retention behavior is resolved from policy and constraints rather than hardcoded cognitive tiers.

The system must support:

- no-decay entities and properties
- configurable decay rates and thresholds
- node-, edge-, and property-level decay behavior
- named policy presets for operator convenience
- future decay models without requiring new engine enums or switch statements

This plan is intentionally model-agnostic. It is not tied to any one research paper or taxonomy. Although inspired by this research paper which called out NornicDB specifically. https://arxiv.org/pdf/2604.11364

---

## 2. Problem Statement

NornicDB currently has memory-decay behavior that depends on fixed tier names and fixed decay assumptions. That makes the system harder to evolve because retention logic is embedded in runtime code rather than expressed declaratively.

That creates four engineering problems:

1. Adding new retention behavior requires code changes instead of policy changes.
2. The engine assumes a closed set of decay categories.
3. Decay is primarily entity-wide instead of being expressible at property scope.
4. Operators cannot declare retention semantics through the same schema-oriented mechanisms they already use elsewhere.

The system should instead treat persistence and decay as configurable retention policy.

---

## 3. Design Principles

1. Retention behavior must be data-driven, not hardcoded into a fixed enum.
2. Persistence and decay must be resolvable at node, edge, and property scope.
3. `NO DECAY` must be directly expressible in schema.
4. Decay rate, decay function, archive threshold, and score floor must be configurable independently.
5. Policy resolution must be deterministic and explainable.
6. Runtime paths must not silently fall back to legacy tier assumptions.
7. Named presets may exist for convenience, but the engine must operate on resolved policy.
8. The architecture must be flexible enough to support any current or future decay model.

---

## 4. Target Architecture

### 4.1 Retention Policy Layer

Retention policy is the mechanism that decides whether decay applies, at what rate, and at what scope.

Required behavior:

- resolve effective policy from configuration and constraints
- support node-, edge-, and property-level targeting
- allow `NO DECAY` and rate-based decay without relying on fixed tier names
- permit named presets but not require them
- support multiple decay functions over time

Suggested fit in NornicDB:

- shared policy resolver used by recall, recalc, archive, and ranking paths
- config-defined presets for operator convenience
- schema-backed constraints as the main control surface
- diagnostics that explain why a given entity resolved to a given policy

### 4.2 Constraint Layer

The constraint layer is the authoring surface for retention behavior.

Required behavior:

- allow operators to declare retention policy in Cypher
- validate constraints at creation time where applicable
- expose constraints through introspection and admin APIs
- support deterministic precedence when multiple decay constraints overlap
- support property-targeted constraints in addition to node and relationship targets

Suggested fit in NornicDB:

- extend the existing node and relationship constraint system
- add NornicDB-specific persistence and decay constraint families
- reuse existing constraint persistence, validation, and introspection patterns
- add retention-specific resolution rules alongside existing schema rules

### 4.3 Runtime Resolution Layer

The runtime resolution layer converts configuration and constraints into an effective policy for an entity or property.

Required behavior:

- resolve policy during recall, reinforcement, recalc, archive, and ranking
- support explicit overrides and inheritance
- allow property-level state without forcing entity-wide decay
- avoid duplicated logic across CLI, DB, and API code paths

Suggested fit in NornicDB:

- one shared resolver used by DB runtime, CLI decay tools, Cypher procedures, and background maintenance
- one policy explanation format returned by diagnostics and admin endpoints

---

## 5. Data Model Plan

### 5.1 Core Entities

- `DecayPolicy`
- `DecayPolicyBinding`
- `PropertyDecayState`
- `RetentionResolution`
- `ConstraintBackedDecayRule`
- `ProvenanceRecord`

### 5.2 Minimum Fields

#### DecayPolicy

- policy id
- policy name
- half-life or decay-rate definition in seconds
- scoring function or strategy id
- archive threshold override
- minimum score floor
- promotion or transition policy reference, if used by a caller model
- scope type: node, edge, property
- enabled or disabled

#### DecayPolicyBinding

- target matcher
- matcher type: label, relationship type, property path, constraint target, explicit id
- policy id
- precedence
- no-decay flag
- source of resolution: config, constraint, inherited
- explanation payload for diagnostics

#### PropertyDecayState

- entity id
- property path
- current decay score
- last accessed
- access count
- effective decay policy id
- archived, hidden, or superseded state

#### RetentionResolution

- target id
- target scope
- resolved policy id
- resolution source chain
- applied constraint names
- effective rate
- effective threshold
- no-decay boolean

---

## 6. Query and Resolution Semantics

### 6.1 Resolution Rules

Every decay-aware read or maintenance operation should resolve policy in this order:

1. explicit no-decay constraint or binding
2. property-level policy
3. entity-level policy
4. relationship-type or label-targeted policy
5. configured default policy

If no policy matches, the engine should either:

- treat the target as non-decaying, or
- use an explicit configured default policy,

but it must not silently assume any legacy tier.

### 6.2 Property-Level Semantics

Property-level decay is required for mixed-longevity entities.

Examples:

- a `Profile` node may keep `name` and `tenantId` permanently while decaying `lastConversationSummary`
- a `Task` relationship may keep identity and timestamps permanently while decaying a transient confidence field
- a `Document` node may keep canonical content permanently while decaying ranking hints or ephemeral summaries

Property decay should support at least these outcomes:

- lower ranking weight for the property during retrieval
- archival or hiding of the property value while preserving the parent entity
- explicit supersession or replacement of the property if configured

### 6.3 Decay Function Semantics

The engine should support multiple decay function identifiers over time.

Initial supported functions can include:

- `exponential`
- `linear`
- `step`
- `none`

The engine should resolve these as policy behavior, not as special categories.

### 6.4 Explainability

For any entity or property, the system should be able to explain:

- whether decay applies
- which policy was selected
- which constraint or binding selected it
- what rate, threshold, and floor are active
- why archival or retention occurred

---

## 7. Constraint Design

### 7.1 New Constraint Families

Add new persistence and decay-aware constraint families for `CREATE CONSTRAINT`:

- `NO DECAY`
- `DECAY RATE <seconds|rate>`
- `DECAY ARCHIVE THRESHOLD <float>`
- `DECAY POLICY <name>`
- `DECAY FUNCTION <name>`
- `DECAY FLOOR <float>`
- `DECAY SCOPE NODE|EDGE|PROPERTY`

These are NornicDB extensions, not Neo4j compatibility targets.

### 7.2 Valid Targets

These constraints should be valid on:

- node labels
- relationship types
- explicit property paths on nodes
- explicit property paths on relationships

### 7.3 Constraint Semantics

If persistence or decay is globally disabled, the constraints still exist in schema but are operationally inactive until the subsystem is enabled.

Conflicting constraints must resolve deterministically according to precedence rules rather than implicit ordering.

### 7.4 Sample Constraints in Cypher

#### Node-level no-decay

```cypher
CREATE CONSTRAINT fact_no_decay
FOR (n:CanonicalFact)
REQUIRE NO DECAY
```

#### Node-level decay rate

```cypher
CREATE CONSTRAINT event_decay
FOR (n:SessionRecord)
REQUIRE DECAY RATE 604800
```

#### Node-level named policy

```cypher
CREATE CONSTRAINT durable_claim_policy
FOR (n:CanonicalFact)
REQUIRE DECAY POLICY 'durable_fact'
```

#### Node-level custom function

```cypher
CREATE CONSTRAINT review_queue_decay
FOR (n:ReviewQueueItem)
REQUIRE DECAY FUNCTION 'linear'
```

#### Node-level archive threshold

```cypher
CREATE CONSTRAINT session_archive_threshold
FOR (n:SessionRecord)
REQUIRE DECAY ARCHIVE THRESHOLD 0.10
```

#### Property-level decay rate on node property

```cypher
CREATE CONSTRAINT profile_summary_decay
FOR (n:Profile)
REQUIRE n.lastConversationSummary DECAY RATE 2592000
```

#### Property-level no-decay on node property

```cypher
CREATE CONSTRAINT profile_identity_no_decay
FOR (n:Profile)
REQUIRE n.tenantId NO DECAY
```

#### Property-level named policy on node property

```cypher
CREATE CONSTRAINT session_summary_policy
FOR (n:SessionRecord)
REQUIRE n.summary DECAY POLICY 'session_summary'
```

#### Relationship-level no-decay

```cypher
CREATE CONSTRAINT citation_rel_no_decay
FOR ()-[r:CITES]-()
REQUIRE NO DECAY
```

#### Relationship-level decay rate

```cypher
CREATE CONSTRAINT coaccess_rel_decay
FOR ()-[r:CO_ACCESSED]-()
REQUIRE DECAY RATE 1209600
```

#### Property-level decay rate on relationship property

```cypher
CREATE CONSTRAINT rel_signal_decay
FOR ()-[r:CO_ACCESSED]-()
REQUIRE r.signalScore DECAY RATE 1209600
```

#### Property-level no-decay on relationship property

```cypher
CREATE CONSTRAINT rel_identity_no_decay
FOR ()-[r:WORKED_WITH]-()
REQUIRE r.externalId NO DECAY
```

#### Score floor

```cypher
CREATE CONSTRAINT wisdom_floor
FOR (n:RetainedDirective)
REQUIRE DECAY FLOOR 0.40
```

#### Explicit scope declaration

```cypher
CREATE CONSTRAINT draft_confidence_scope
FOR (n:Draft)
REQUIRE n.confidence DECAY SCOPE PROPERTY
```

### 7.5 Policy DDL

Decay policies should be first-class database objects, not ordinary graph nodes.

Operators should be able to define policies independently and bind them via constraints.

Example policy bootstrap:

```cypher
CREATE DECAY POLICY durable_fact
OPTIONS {
  decayEnabled: false,
  archiveThreshold: 0.0,
  scope: 'NODE',
  function: 'none'
}

CREATE DECAY POLICY session_summary
OPTIONS {
  halfLifeSeconds: 1209600,
  archiveThreshold: 0.10,
  scope: 'PROPERTY',
  function: 'exponential'
}

CREATE DECAY POLICY working_memory
OPTIONS {
  halfLifeSeconds: 604800,
  archiveThreshold: 0.05,
  scope: 'NODE',
  function: 'exponential'
}
```

Then bind those policies with constraints:

```cypher
CREATE CONSTRAINT claim_retention
FOR (n:CanonicalFact)
REQUIRE DECAY POLICY 'durable_fact'

CREATE CONSTRAINT event_retention
FOR (n:SessionRecord)
REQUIRE DECAY POLICY 'working_memory'

CREATE CONSTRAINT summary_retention
FOR (n:SessionRecord)
REQUIRE n.summary DECAY POLICY 'session_summary'
```

Suggested follow-on DDL:

```cypher
SHOW DECAY POLICIES
```

```cypher
ALTER DECAY POLICY working_memory
SET OPTIONS {
  halfLifeSeconds: 432000,
  archiveThreshold: 0.08
}
```

```cypher
DROP DECAY POLICY session_summary
```

---

## 8. API and Storage Changes

### Suggested API additions

- `POST /memory/event`
- `GET /memory/query`
- `GET /memory/decay/policy/:id`
- `POST /memory/decay/policy`
- `POST /memory/decay/bind`
- `GET /memory/decay/resolve`
- `GET /memory/decay/explain/:entityId`

### Suggested storage rules

- decay eligibility and rate are resolved from policy bindings and constraints, not a baked-in tier enum
- property decay state may be stored separately from node or edge decay state where required for precision and performance
- policy resolution artifacts should be diagnosable without mutating the underlying entity
- no-decay policies should be enforced consistently across recall, archive, and maintenance paths

---

## 9. Implementation Workstreams

### Workstream A: Policy Model

Deliverables:

- define the policy schema model
- define supported decay functions and thresholds
- define explainable resolution output

### Workstream B: Constraint Extensions

Deliverables:

- extend the existing constraint system for decay-aware constraints
- support node-, relationship-, and property-targeted constraints
- validate creation-time behavior and introspection

### Workstream C: Shared Resolver

Deliverables:

- introduce a shared decay policy resolver
- support configurable decay rates and named presets
- define precedence and conflict rules for overlapping constraints
- expose an explainable resolution trace for any effective policy

### Workstream D: Runtime Integration

Deliverables:

- route recall, recalc, archive, ranking, and stats paths through the shared resolver
- remove hardcoded tier branching from runtime code
- support property-level decay behavior

### Workstream E: UI and Tooling

Deliverables:

- show effective policy in browser and API output
- let operators inspect constraints, policies, and resolution traces
- add diagnostics for why a value decayed or did not decay

---

## 10. Implementation Sequence

1. Define the decay policy schema model and resolution precedence.
2. Centralize decay resolution in a shared helper used by recall, recalc, archive, ranking, and stats paths.
3. Add configurable per-policy half-lives, decay rates, named presets, and function identifiers.
4. Define and implement schema-backed persistence and decay constraints on nodes and relationships.
5. Extend constraint support to property-level targeting.
6. Migrate runtime logic away from fixed tier assumptions.
7. Expose policy and resolution information in API and UI surfaces.
8. Add regression tests for resolution, property-level retention, and archival behavior.
9. Add benchmark and evaluation coverage for policy resolution overhead and correctness.

---

## 11. Testing Plan

### Must-have regression cases

- no-decay entities are skipped by recalc and archive paths
- effective decay rate comes from resolved policy rather than hardcoded tier
- property-level decay can age one property without decaying the parent entity
- conflicting constraints resolve deterministically
- removing or changing a decay constraint changes future resolution without corrupting stored history
- relationship-level and property-level constraints both resolve correctly
- explain output identifies the exact binding and effective policy

### Benchmark targets

- decay policy resolution overhead
- property-level decay selectivity and maintenance cost
- archive pass throughput under mixed policy workloads
- recall and ranking overhead with resolved policy checks

---

## 12. Acceptance Criteria

The plan is complete when:

- no runtime path depends on a hardcoded tier enum to decide whether something decays
- operators can define persistence and decay semantics through config and constraints
- node-, edge-, and property-level decay are all supported
- explainable policy resolution is available for diagnostics
- new decay models can be expressed as policy and constraints without new engine categories

---

## 13. Out of Scope

- replacing the existing graph engine
- changing unrelated Cypher semantics
- tying the implementation to any single research taxonomy
- implementing LLM-based consolidation inside the storage engine itself

---

## 14. Deliverables

- a constraint-driven persistence and decay specification
- schema and API updates for policy-aware decay behavior
- a shared decay policy resolver with config-backed and constraint-backed bindings
- regression tests covering node-, edge-, and property-level semantics
- user-facing documentation for persistence and decay policy authoring

---

## 15. Notes

This plan is intentionally implementation-oriented. The main architectural shift is to stop using fixed categories as permanent engine concepts and instead operate on resolved policy.

Named presets may remain in documentation for bootstrapping a memory decay model for operator convenience, but the engine should ultimately care only about effective persistence and decay policy.
