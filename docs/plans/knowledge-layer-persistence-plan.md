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

- extend the existing node and relationship block-constraint system
- add NornicDB-specific persistence and decay constraint families
- express property-level retention as inline entries inside `REQUIRE { ... }` blocks
- reuse existing constraint persistence, validation, and introspection patterns
- add retention-specific resolution rules alongside existing schema rules

### 4.3 Runtime Resolution Layer

The runtime resolution layer converts configuration and constraints into an effective policy for an entity or property.

Required behavior:

- resolve policy during recall, reinforcement, recalc, archive, and ranking
- support explicit overrides and inheritance
- allow property-level state without forcing entity-wide decay
- resolve inline property entries from the active block constraint before falling back to entity defaults
- expose decay state through native Cypher functions without changing Neo4j-compatible node or relationship result shapes
- avoid duplicated logic across CLI, DB, and API code paths

Suggested fit in NornicDB:

- one shared resolver used by DB runtime, CLI decay tools, Cypher procedures, and background maintenance
- one policy explanation format returned by diagnostics and admin endpoints

---

## 5. Logical Resolution Model

Because decay scores are derived rather than stored on fields, this section describes runtime resolution artifacts and schema objects, not a persisted score data model.

### 5.1 Persistent Schema Objects

#### DecayPolicy

Persistent database object used to define reusable decay behavior.

Minimum fields:

- policy id
- policy name
- half-life or decay-rate definition in seconds
- scoring function or strategy id
- archive threshold override
- minimum score floor
- promotion or transition policy reference, if used by a caller model
- scope type: node, edge, property
- enabled or disabled

#### ConstraintBackedDecayRule

Logical rule compiled from block constraints and used by the resolver.

Minimum fields:

- contract name
- entity target: label or relationship type
- property path, if any
- rule kind: no-decay, policy, rate, threshold, floor, function
- referenced policy name, if any
- inline block order for deterministic precedence
- original expression text for diagnostics

### 5.2 Derived Runtime Artifacts

#### RetentionResolution

Derived resolution result produced by the shared resolver for a requested node, edge, or property.

Minimum fields:

- target id
- target scope
- resolved policy id
- resolution source chain
- applied constraint names
- applied block entries
- effective rate
- effective threshold
- no-decay boolean

#### DecayResolutionMeta

Derived metadata emitted at read time for Cypher and unified search surfaces.

Minimum fields:

- entity id
- entity scope: node or edge
- entity decay score, if applicable
- per-property resolved score map
- optional per-property explanation payload

### 5.3 Design Rule

- derived scores are not persisted into node, edge, or property payloads
- the shared resolver is the source of truth for entity- and property-level scoring
- Cypher functions and unified search metadata project derived scores outward without mutating stored graph data

---

## 6. Query and Resolution Semantics

### 6.1 Resolution Rules

Every decay-aware read or maintenance operation should resolve policy in this order:

1. explicit no-decay rule
2. property-level inline rule inside the applicable block constraint
3. entity-level rule inside the applicable block constraint
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

Property-level scores should only influence retrieval when the property is directly involved in matching, ranking, reranking, filtering, projection, summarization, or archival unless an explicit roll-up policy says otherwise. A decayed property should not silently degrade the score of the entire entity by default.

Property-level score data should not be written back into the entity's stored fields. It should be derived on demand from the shared resolver and exposed only through native Cypher access or search metadata.

### 6.3 Decay Function Semantics

The engine should support multiple decay function identifiers over time.

Initial supported scoring modes can include:

- `exponential`
- `linear`
- `step`
- `none`

The engine should resolve these as runtime scoring behavior, not as special categories.

These scoring modes should be accepted both:

- from resolved policy and constraint configuration, and
- from an explicit Cypher options object on decay scoring functions.

Cypher may override the policy-resolved scoring mode for the scope of that scoring expression only. Unified retrieval should not expose that override surface and should remain policy-resolved.

### 6.4 Explainability

For any entity or property, the system should be able to explain:

- whether decay applies
- which policy was selected
- which constraint block and inline entry selected it
- what rate, threshold, and floor are active
- why archival or retention occurred

### 6.5 Native Cypher Access

The decay subsystem should expose scoring through native Cypher functions so callers can inspect resolved scores without altering Neo4j-compatible node or relationship structures.

Proposed functions:

- `decayScore(entity)` returns the effective scalar decay score for a node or relationship
- `decayScore(entity, { scoringMode: 'linear' })` returns the effective scalar decay score for a node or relationship using the requested scoring mode
- `decayScore(entity, { property: 'summary' })` returns the effective scalar decay score for a specific property on that node or relationship
- `decayScore(entity, { property: 'summary', scoringMode: 'step' })` returns the effective scalar decay score for a specific property using the requested scoring mode
- `decay(entity)` returns a structured decay object for the node or relationship
- `decay(entity, { scoringMode: 'linear' })` returns a structured decay object for the node or relationship using the requested scoring mode
- `decay(entity, { property: 'summary' })` returns a structured decay object for the requested property
- `decay(entity, { property: 'summary', scoringMode: 'step' })` returns a structured decay object for the requested property using the requested scoring mode

The options-object shape avoids ambiguous string overloads. `property` and `scoringMode` are named keys rather than positional string arguments.

The structured `decay(...)` result should always expose a Cypher-accessible `.score` field so callers can write concise expressions without needing a second helper function when they want richer metadata.

Suggested fields on `decay(...)` results:

- `score`
- `policy`
- `scope`
- `function`
- `archiveThreshold`
- `floor`
- `applies`
- `reason`

The `decay(...)` object is a derived value. It should not imply that score metadata is being persisted back onto the node, edge, or property itself.

Example usage:

```cypher
MATCH (n:SessionRecord)
RETURN n, decayScore(n) AS entityDecayScore
```

```cypher
MATCH (n:SessionRecord)
RETURN n.summary, decayScore(n, {property: 'summary'}) AS summaryDecayScore
```

```cypher
MATCH (n:SessionRecord)
RETURN n, decayScore(n, {scoringMode: 'linear'}) AS entityDecayScoreLinear
```

```cypher
MATCH (n:SessionRecord)
RETURN n.summary, decayScore(n, {property: 'summary', scoringMode: 'step'}) AS summaryDecayScoreStep
```

```cypher
MATCH (n:SessionRecord)
RETURN n, decay(n).score AS entityDecayScore, decay(n).policy AS entityDecayPolicy
```

```cypher
MATCH (n:SessionRecord)
RETURN n.summary, decay(n, {property: 'summary'}).score AS summaryDecayScore, decay(n, {property: 'summary'}).reason AS summaryDecayReason
```

```cypher
MATCH (n:SessionRecord)
RETURN n.summary, decay(n, {property: 'summary', scoringMode: 'step'}).score AS summaryDecayScore, decay(n, {property: 'summary', scoringMode: 'step'}).function AS summaryDecayFunction
```

```cypher
MATCH ()-[r:CO_ACCESSED]-()
RETURN r, decayScore(r) AS edgeDecayScore, decay(r, {property: 'signalScore'}).score AS signalDecayScore
```

```cypher
MATCH ()-[r:CO_ACCESSED]-()
RETURN r, decay(r, {property: 'signalScore', scoringMode: 'exponential'}).score AS signalDecayScoreExponential
```

Example `ORDER BY` usage:

```cypher
MATCH (n:SessionRecord)
RETURN n, decayScore(n) AS decayScoreValue
ORDER BY decayScoreValue DESC
LIMIT 25
```

```cypher
MATCH (n:SessionRecord)
RETURN n, decayScore(n, {property: 'summary'}) AS summaryDecayScore
ORDER BY summaryDecayScore DESC, n.createdAt DESC
LIMIT 25
```

```cypher
MATCH (n:SessionRecord)
RETURN n, decay(n, {property: 'summary'}).score AS summaryDecayScore
ORDER BY summaryDecayScore DESC
```

```cypher
MATCH ()-[r:CO_ACCESSED]-()
RETURN r, decayScore(r, {property: 'signalScore', scoringMode: 'linear'}) AS signalDecayScore
ORDER BY signalDecayScore DESC
LIMIT 50
```

Compatibility rule:

- `RETURN n` remains Neo4j-compatible and does not automatically inject decay metadata into the node
- callers opt in by returning `decayScore(...)` or `decay(...)` explicitly as additional columns
- property-level scores are therefore visible to Cypher without changing Bolt node or relationship structures

Implementation rule:

- Cypher scoring functions should call the same shared runtime scorer used by unified retrieval scoring
- Cypher options objects should be validated against the accepted keys `property` and `scoringMode`
- supported Cypher `scoringMode` values remain: `exponential`, `linear`, `step`, `none`
- unified retrieval should call the same scorer but should not accept a caller-supplied `scoringMode`

### 6.6 Unified Search Metadata

The unified search service should follow the same derived-on-read model as native Cypher.

It should not persist node-, edge-, or property-level decay scores into stored entity fields. Instead, when requested, it should add resolved scoring metadata into a separate response `meta` structure.

Unified retrieval scoring should use the same scorer as Cypher scoring functions, but it should remain policy-resolved and should not expose the Cypher-only `scoringMode` override.

The shape should be a keyed object rather than an array of single-entry maps.

Preferred shape:

```json
{
  "scores": {
    "node-id-12": {
      "decay": 0.82,
      "properties": {
        "property1": { "decay": 0.44 },
        "property2": { "decay": 0.91 }
      }
    },
    "edge-id-77": {
      "decay": 0.63,
      "properties": {
        "signalScore": { "decay": 0.28 }
      }
    }
  }
}
```

That is preferable to a shape like:

```json
[
  {
    "node-id-12": {
      "decay": 0.82,
      "property1": { "decay": 0.44 },
      "property2": { "decay": 0.91 }
    }
  }
]
```

because the keyed object form is easier to merge, extend, and consume deterministically.

Suggested conventions:

- top-level key by entity id
- entity-level score at `scores[id].decay`
- property-level scores nested at `scores[id].properties[propertyKey].decay`
- optional richer metadata can be added later beside `decay`, such as `policy`, `reason`, or `scope`

Suggested retrieval scoring inputs:

- options object with optional `property` when scoring needs to target a specific property
- options object may later grow additional explicit keys without breaking call-site semantics
- retrieval callers should not provide `scoringMode`; mode selection comes from resolved policy

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
- inline property paths on nodes within a block constraint
- inline property paths on relationships within a block constraint

### 7.3 Constraint Semantics

If persistence or decay is globally disabled, the constraints still exist in schema but are operationally inactive until the subsystem is enabled.

Conflicting constraints must resolve deterministically according to precedence rules rather than implicit ordering.

Property-level retention rules should be authored inline within the same `REQUIRE { ... }` block that declares the entity-level defaults for that label or relationship type. That keeps the authoring model aligned with existing block constraints and avoids introducing a second binding mechanism just for properties.

Nested `FOR ... REQUIRE` entries should remain invalid inside a block. If operators need retention rules for a different label or relationship type, they should create a separate targeted block constraint, consistent with the current schema-contract behavior.

When property-level retention rules exist, the runtime should make the resolved score available through `decayScore(entity, {property: propertyKey})` and `decay(entity, {property: propertyKey})` even if the underlying Bolt result only returns the base node or relationship structure.

The same shared scorer should also back retrieval scoring, using the scoring mode resolved from policy rather than a caller override.

### 7.4 Sample Constraints in Cypher

#### Node-level default policy with inline property rules

```cypher
CREATE CONSTRAINT session_record_retention
FOR (n:SessionRecord)
REQUIRE {
  DECAY POLICY 'working_memory'
  DECAY ARCHIVE THRESHOLD 0.10
  n.summary DECAY POLICY 'session_summary'
  n.lastConversationSummary DECAY RATE 2592000
  n.tenantId NO DECAY
}
```

#### Node-level no-decay with explicit permanent properties

```cypher
CREATE CONSTRAINT canonical_fact_retention
FOR (n:CanonicalFact)
REQUIRE {
  NO DECAY
  n.tenantId NO DECAY
  n.externalId NO DECAY
}
```

#### Node-level custom function and score floor

```cypher
CREATE CONSTRAINT review_queue_retention
FOR (n:ReviewQueueItem)
REQUIRE {
  DECAY FUNCTION 'linear'
  DECAY RATE 604800
  n.confidence DECAY FLOOR 0.40
}
```

#### Relationship-level default policy with inline property rules

```cypher
CREATE CONSTRAINT coaccess_retention
FOR ()-[r:CO_ACCESSED]-()
REQUIRE {
  DECAY RATE 1209600
  r.signalScore DECAY RATE 1209600
  r.signalScore DECAY FLOOR 0.15
  r.externalId NO DECAY
}
```

#### Explicit property-only override inside a block

```cypher
CREATE CONSTRAINT draft_retention
FOR (n:Draft)
REQUIRE {
  DECAY RATE 604800
  n.confidence DECAY RATE 86400
  n.confidence DECAY FLOOR 0.25
}
```

In this model, property-level rules are just targeted entries in the same block constraint. They should not require a separate `CREATE CONSTRAINT` statement unless the target label or relationship type itself changes.

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
CREATE CONSTRAINT event_retention
FOR (n:SessionRecord)
REQUIRE {
  DECAY POLICY 'working_memory'
  n.summary DECAY POLICY 'session_summary'
}

CREATE CONSTRAINT claim_retention
FOR (n:CanonicalFact)
REQUIRE {
  DECAY POLICY 'durable_fact'
}
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

## 8. Cypher, Search, and Storage Changes

### Suggested Cypher additions

- native scalar function: `decayScore(entity[, options])`
- native structured function: `decay(entity[, options])`
- both functions should work for nodes and relationships
- `decay(...).score` should be the canonical Cypher-visible field for downstream sorting, filtering, and projection
- both functions derive scores from the shared resolver rather than reading persisted property-level score fields
- both functions accept an explicit Cypher options object with keys such as `property` and `scoringMode`

### Suggested storage rules

- decay eligibility and rate are resolved from decay policies plus block-constraint entries, not a baked-in tier enum
- property-level decay scores are derived on demand and are not written into the entity's stored property map
- temporary caches of resolved scores are allowed as implementation detail, but they are not the source of truth
- policy resolution artifacts should be diagnosable without mutating the underlying entity
- no-decay policies should be enforced consistently across recall, archive, and maintenance paths

### Suggested search response behavior

- unified search may return node-, edge-, and property-level decay metadata additively in a separate `meta` section
- the `meta` section should mirror the same resolved scores available through `decayScore()` and `decay()`
- search hits themselves remain standard result entities plus ordinary ranking fields
- unified retrieval scoring should call the same shared scorer implementation as Cypher, but without exposing the Cypher-only `scoringMode` override

---

## 9. Implementation Workstreams

### Workstream A: Policy Model

Deliverables:

- define the policy schema model
- define supported decay functions and thresholds
- define explainable resolution output
- define the native `decayScore()` and `decay()` Cypher function contracts, including explicit `property` and `scoringMode` options
- define the derived search metadata contract for node-, edge-, and property-level scores

### Workstream B: Constraint Extensions

Deliverables:

- extend the existing block-constraint system for decay-aware entries
- support node-, relationship-, and property-targeted constraints
- validate creation-time behavior and introspection

### Workstream C: Shared Resolver

Deliverables:

- introduce a shared decay policy resolver
- support configurable decay rates and named presets
- define precedence and conflict rules for overlapping inline block entries
- expose an explainable resolution trace for any effective policy
- make resolved node-, edge-, and property-level scores available to native Cypher functions
- make the same resolved scores available to unified search metadata without persisting them into entity fields
- centralize policy-resolved scoring so Cypher and unified retrieval call the same scorer
- keep Cypher-only scoring-mode override handling at the function surface while leaving unified retrieval policy-resolved

### Workstream D: Runtime Integration

Deliverables:

- route recall, recalc, archive, ranking, and stats paths through the shared resolver
- remove hardcoded tier branching from runtime code
- support property-level decay behavior

### Workstream E: UI and Tooling

Deliverables:

- show effective policy in browser, search metadata, and Cypher-visible outputs
- let operators inspect constraints, policies, and resolution traces
- add diagnostics for why a value decayed or did not decay

---

## 10. Implementation Sequence

1. Define the decay policy schema model and resolution precedence.
2. Centralize decay resolution in a shared helper used by recall, recalc, archive, ranking, and stats paths.
3. Add configurable per-policy half-lives, decay rates, named presets, and function identifiers.
4. Define and implement schema-backed persistence and decay entries on block constraints for nodes and relationships.
5. Extend block parsing and compiled contract metadata to support property-targeted retention entries.
6. Add native Cypher functions `decayScore()` and `decay()` for nodes and relationships with an explicit options object for `property` and optional `scoringMode`.
7. Migrate runtime logic away from fixed tier assumptions.
8. Bind unified retrieval scoring to the same shared scorer and policy-resolved scoring configuration.
9. Expose policy and resolution information in Cypher, search metadata, and UI surfaces.
10. Add regression tests for resolution, property-level retention, Cypher score access, Cypher-only `scoringMode` overrides, explicit property-target options, and archival behavior.
11. Add benchmark and evaluation coverage for policy resolution overhead and correctness.

---

## 11. Testing Plan

### Must-have regression cases

- no-decay entities are skipped by recalc and archive paths
- effective decay rate comes from resolved policy rather than hardcoded tier
- property-level inline rules can age one property without decaying the parent entity
- conflicting constraints resolve deterministically
- removing or changing a decay constraint changes future resolution without corrupting stored history
- relationship-level blocks and inline property rules both resolve correctly
- explain output identifies the exact block entry and effective policy
- `decayScore(n)` and `decayScore(n, {property: 'prop'})` return the same resolved score used by runtime policy evaluation
- `decayScore()` and `decay()` accept Cypher-only `scoringMode` overrides with values `exponential`, `linear`, `step`, and `none`
- `decay(n).score` and `decay(n, {property: 'prop'}).score` are Cypher-accessible and stable for projection and ordering
- returning `n` alone does not alter Neo4j-compatible result shape
- unified search `meta` returns entity and property decay scores in a separate keyed structure without mutating the hit payload
- unified retrieval scoring returns the same score family as the equivalent Cypher call when both use the same resolved policy
- unified retrieval does not expose the Cypher-only `scoringMode` override surface

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
- operators can define property-level retention inline in existing `REQUIRE { ... }` block constraints
- node-, edge-, and property-level decay are all supported
- explainable policy resolution is available for diagnostics
- native Cypher functions expose resolved entity and property scores without mutating Neo4j-compatible node or relationship payloads
- unified search exposes the same resolved scores additively through response metadata rather than persisted fields
- Cypher scoring and unified retrieval scoring share the same scorer implementation, but only Cypher exposes an explicit `scoringMode` override
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
- schema and Cypher/search updates for policy-aware decay behavior
- a shared decay policy resolver with config-backed defaults and constraint-backed block entries
- block-constraint extensions for inline property-level retention rules
- native Cypher function support for `decayScore()` and `decay()`
- shared runtime scorer support for Cypher and unified retrieval, with Cypher-only `scoringMode` override support and policy-resolved search scoring
- unified search metadata support for additive node-, edge-, and property-level decay scores
- regression tests covering node-, edge-, and property-level semantics
- user-facing documentation for persistence and decay policy authoring

---

## 15. Notes

This plan is intentionally implementation-oriented. The main architectural shift is to stop using fixed categories as permanent engine concepts and instead operate on resolved policy.

Named presets may remain in documentation for bootstrapping a memory decay model for operator convenience, but the engine should ultimately care only about effective persistence and decay policy.
