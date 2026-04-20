# Policy-Driven Decay and Scoring Plan

**Status:** Proposed
**Date:** April 15, 2026
**Scope:** Replace hardcoded memory-tier decay behavior with a generic, policy-driven decay and scoring system that can support existing, proposed, or future decay models, while expressing promotive declarative tiers through a separate promotion-policy subsystem, supporting MVCC-aware score-from selection for both nodes and edges, and implementing efficient archival deindexing for archived nodes and edges.

---

## 1. Objective

Implement a flexible decay and scoring architecture in NornicDB where retention behavior is resolved from policies rather than hardcoded cognitive tiers.

The system must support:

- no-decay entities and properties
- configurable decay rates and thresholds
- node-, edge-, and property-level decay behavior
- named policy presets for operator convenience
- separate promotion policies that declaratively model tier-like score boosts without changing the existing Cypher scoring API
- declarative MVCC-aware score-from selection through decay policy options
- future decay models without requiring new engine enums or switch statements
- efficient archival of whole nodes and whole edges
- asynchronous removal of archived nodes and archived edges from indexing
- property-level decay effects that can exclude properties from vectorization or retrieval surfaces without archiving, moving, or deleting those properties from storage

Nodes and edges must be treated as first-class decay targets. A node or edge must be able to decay, be scored, be archived, be removed from indexing, and be promoted using the same policy-driven machinery.

Properties are not archival targets. Properties may receive decay scores and vectorization-exclusion behavior, but they remain stored in place and remain directly queryable through Cypher.

This plan is intentionally model-agnostic. It is not tied to any one research paper or taxonomy. Although inspired by this research paper which called out NornicDB specifically. [https://arxiv.org/pdf/2604.11364](https://arxiv.org/pdf/2604.11364)

---

## 2. Problem Statement

NornicDB currently has memory-decay behavior that depends on fixed tier names and fixed decay assumptions. That makes the system harder to evolve because retention logic is embedded in runtime code rather than expressed declaratively.

That creates six engineering problems:

1. Adding new retention behavior requires code changes instead of policy changes.
2. The engine assumes a closed set of decay categories.
3. Decay is primarily entity-wide instead of being expressible at node, edge, and property scope.
4. Operators cannot declare retention semantics through the same schema-oriented mechanisms they already use elsewhere.
5. Under MVCC, decay scoring needs an explicit start-time anchor unless the policy states whether score age begins at entity creation time or at the latest visible version time.
6. Archived nodes and edges must be removed from indexing efficiently, without expensive full-index scans, while property-level decay behavior must not be confused with whole-entity archival.

The system should instead treat decay behavior as configurable retention policy, promotion behavior as a separate configurable scoring policy, score start time as an explicit policy decision, and archive cleanup as a dedicated deindex workflow for nodes and edges only.

---

## 3. Design Principles

1. Retention behavior must be data-driven, not hardcoded into a fixed enum.
2. Decay and scoring must be resolvable at node, edge, and property scope.
3. `NO DECAY` must be directly expressible in policy definitions.
4. Decay rate, decay function, archive threshold, and score floor must be configurable independently.
5. Promotion tiers must be expressible declaratively through a separate promotion-policy subsystem rather than through hardcoded runtime categories.
6. Score start time must be declaratively expressible through decay policy options using either `CREATED` or `VERSION`.
7. Nodes and edges must be handled symmetrically by the policy system. Edge decay must not be a second-class or special-case feature.
8. Archive behavior applies only to whole nodes and whole edges, never to individual properties.
9. Property-level decay may influence vectorization, ranking, filtering, reranking, and summarization, but it must not move, archive, or delete stored property values.
10. Archived nodes and edges must be removed from indexing using exact-key deindexing rather than discovery by scanning secondary indexes.
11. Runtime paths must not silently fall back to legacy tier assumptions.
12. Named presets may exist for convenience, but the engine must operate on resolved policy.
13. The architecture must be flexible enough to support any current or future decay model.

---

## 4. Target Architecture

### 4.1 Retention Policy Layer

Retention policy is the mechanism that decides whether decay applies, at what rate, at what scope, and from which score start time decay age is measured.

Required behavior:

- resolve effective decay policy from configuration and policies
- support node-, edge-, and property-level targeting
- allow `NO DECAY` and rate-based decay without relying on fixed tier names
- permit named presets but not require them
- support multiple decay functions over time
- support score start-time selection through policy options
- resolve archive eligibility for whole nodes and whole edges
- resolve property-level vectorization-exclusion behavior without treating properties as archival targets

Suggested fit in NornicDB:

- shared policy resolver used by recall, recalc, archive, and ranking paths
- config-defined presets for operator convenience
- schema-backed decay policies as the main control surface
- diagnostics that explain why a given node or edge resolved to a given decay policy and score start time

### 4.2 Promotion Policy Layer

Promotion policy is the mechanism that applies declarative, tier-like scoring adjustments after decay policy resolution without changing the existing Cypher scoring API.

Required behavior:

- resolve applicable promotion policies from configuration and policy statements
- support node-, edge-, and property-level targeting
- allow promotion policies to apply score multipliers, caps, and floors
- support deterministic composition when multiple promotion policies match
- keep promotion policies separately authored, shown, and retrieved from decay policies

Suggested fit in NornicDB:

- a dedicated promotion-policy subsystem with its own catalog and DDL
- shared runtime scoring that first resolves decay policy, then applies matching promotion policies
- diagnostics that explain which promotion policies matched for a node, edge, or property and how they affected the final score

### 4.3 Policy Subsystem Layer

The policy subsystem is the authoring surface for retention behavior and promotion behavior.

Required behavior:

- allow operators to declare decay policy in Cypher
- allow operators to declare promotion policy in Cypher
- validate policy definitions at creation time where applicable
- expose policies through introspection and admin APIs
- support deterministic precedence when multiple policy rules overlap
- support property-targeted rules in addition to node and edge targets

Suggested fit in NornicDB:

- introduce a dedicated decay-policy subsystem with its own catalog and DDL
- introduce a dedicated promotion-policy subsystem with its own catalog and DDL
- borrow authoring, validation, and introspection patterns from the constraint subsystem without making decay rules or promotion rules first-class constraints
- express property-level retention and promotion as inline policy entries inside policy bodies
- add retention-specific and promotion-specific resolution rules alongside existing schema rules

### 4.4 Runtime Resolution Layer

The runtime resolution layer converts configuration and policies into effective decay policy and final score for a node, edge, or property.

Required behavior:

- resolve decay policy during recall, reinforcement, recalc, archive, and ranking
- resolve matching promotion policies during recall, reinforcement, recalc, archive, and ranking
- resolve score start time from decay policy during score evaluation
- support explicit overrides and inheritance
- allow property-level state without forcing entity-wide decay
- resolve inline property entries from the active decay policy before falling back to entity defaults
- resolve applicable promotion policies after decay policy resolution
- expose final decay score through native Cypher functions without changing Neo4j-compatible node or relationship result shapes
- avoid duplicated logic across CLI, DB, and API code paths

Suggested fit in NornicDB:

- one shared resolver used by DB runtime, CLI decay tools, Cypher procedures, and background maintenance
- one policy explanation format returned by diagnostics and admin endpoints
- one shared scorer that computes base score from decay policy and final score from promotion-policy composition
- one shared MVCC-aware score-start resolver that interprets `CREATED` and `VERSION`

### 4.5 MVCC Interaction Layer

MVCC visibility and decay scoring must remain separate concerns.

Required behavior:

- resolve the visible node, edge, or property version using the transaction snapshot
- evaluate the base decay score using the score start time resolved from decay policy
- support `CREATED`, where decay age begins at the entity's original creation timestamp
- support `VERSION`, where decay age begins at the latest visible version timestamp under MVCC
- never require new stored versions solely because a derived score changed over time

Suggested fit in NornicDB:

- visibility resolution remains owned by MVCC
- score start-time choice remains owned by decay policy
- the shared scorer consumes both the visible node or edge version and the policy-resolved score start time

### 4.6 Archival and Deindex Layer

The archival and deindex layer is the mechanism that removes archived whole nodes and whole edges from indexing in the most performant way possible.

Required behavior:

- archive only whole nodes and whole edges
- never archive, move, or delete individual properties because of decay policy
- mark archived nodes and edges in primary storage immediately
- remove archived nodes and edges from indexing asynchronously
- avoid discovering stale index entries by scanning entire secondary indexes
- support a configurable background cleanup cadence, defaulting to nightly but configurable in seconds
- ensure archived nodes and edges are skipped efficiently during retrieval
- allow property-level vectorization exclusion without storage relocation or Cypher inaccessibility

Suggested fit in NornicDB:

- maintain a per-node and per-edge index-entry catalog that stores the exact secondary-index keys written for that entity
- when a node or edge becomes archived, enqueue a deindex work item referencing that entity and its index-entry catalog
- have the background archival job drain deindex work items and perform blind batched deletes against index keys
- keep read-time archived checks cheap so archived entities are skipped even before asynchronous deindex completes
- treat physical space reclamation as separate storage maintenance rather than part of logical archive/deindex semantics

---

## 5. Logical Resolution Model

Because decay scores are derived rather than stored on fields, this section describes runtime resolution artifacts and schema objects, not a stored score data model.

### 5.1 Schema Objects

#### DecayPolicy

Database object used to define reusable decay behavior.

Minimum fields:

- policy id
- policy name
- half-life or decay-rate definition in seconds
- scoring function or strategy id
- score start time: `CREATED` or `VERSION`
- archive threshold override for node or edge archival eligibility
- minimum score floor
- scope type: node, edge, property
- enabled or disabled

#### PromotionPolicy

Database object used to define reusable promotive scoring behavior.

Minimum fields:

- policy id
- policy name
- score multiplier
- optional score floor override
- optional score cap override
- composition mode compatibility, if restricted
- scope type: node, edge, property
- enabled or disabled

#### PolicyBackedDecayRule

Logical rule compiled from decay policy definitions and used by the resolver.

Minimum fields:

- contract name
- policy name
- entity target: label or edge type
- property path, if any
- rule kind: no-decay, policy, rate, threshold, floor, function
- referenced policy name, if any
- inline rule order for deterministic precedence
- original expression text for diagnostics

#### PolicyBackedPromotionRule

Logical rule compiled from promotion policy definitions and used by the resolver.

Minimum fields:

- contract name
- policy name
- entity target: label or edge type
- property path, if any
- rule kind: promotion-policy, multiplier, floor, cap, compose
- referenced policy name, if any
- predicate expression
- inline rule order for deterministic precedence
- original expression text for diagnostics

#### IndexEntryCatalog

Persistent catalog of exact index entries created for a node or edge.

Minimum fields:

- target id
- target scope: node or edge
- index entry key list or catalog reference
- index family identifiers, if partitioned
- last indexed version, if tracked
- archived boolean or state marker, if duplicated for cleanup convenience

#### ArchiveWorkItem

Persistent background work item used to deindex an archived node or edge.

Minimum fields:

- work item id
- target id
- target scope: node or edge
- archive state
- enqueued at
- next attempt at
- retry count
- cleanup status
- index catalog reference or direct key reference

### 5.2 Derived Runtime Artifacts

#### RetentionResolution

Derived resolution result produced by the shared resolver for a requested node, edge, or property.

Minimum fields:

- target id
- target scope
- resolved decay policy id
- resolved score start time
- resolution source chain
- applied decay policy names
- applied decay policy entries
- applied promotion policy names
- applied promotion policy entries
- effective rate
- effective threshold
- effective multiplier
- base score
- final score
- no-decay boolean
- archive-eligible boolean for node or edge targets only

#### DecayResolutionMeta

Derived metadata emitted at read time for Cypher and unified search surfaces.

Minimum fields:

- entity id
- entity scope: node or edge
- entity decay score, if applicable
- score start time
- per-property resolved score map
- optional per-property explanation payload

### 5.3 Design Rule

- derived scores are not persisted into node, edge, or property payloads
- the shared resolver is the source of truth for node-, edge-, and property-level scoring
- Cypher functions and unified search metadata project derived scores outward without mutating stored graph data
- the existing Cypher scoring API remains unchanged; resolved promotion policies affect the returned score through the shared scorer rather than through new function signatures
- the score start time is resolved from decay policy and used by the shared scorer without changing the existing Cypher scoring API
- whole-node and whole-edge archival state may be persisted
- property archival state is not persisted because properties are not archival targets
- property-level decay may exclude properties from vectorization or retrieval surfaces but must not move or delete stored property values

---

## 6. Query and Resolution Semantics

### 6.1 Resolution Rules

Every decay-aware read or maintenance operation should resolve decay policy in this order:

1. explicit no-decay rule
2. property-level inline rule inside the applicable decay policy
3. entity-level rule inside the applicable decay policy
4. edge-type or label-targeted decay policy
5. configured default decay policy

Then every decay-aware scoring operation should resolve promotion policy in this order:

6. property-level promotion policy entries that match the target
7. entity-level promotion policy entries that match the target
8. edge-type or label-targeted promotion policy
9. configured default promotion policy behavior, if any

Then every score-aware read should resolve the score start time from the resolved decay policy:

10. `CREATED`, if the resolved decay policy declares `CREATED`
11. `VERSION`, if the resolved decay policy declares `VERSION`
12. configured default score start time, if no explicit policy value applies

If no decay policy matches, the engine should either treat the target as non-decaying or use an explicit configured default decay policy, but it must not silently assume any legacy tier.

If no promotion policy matches, the target should resolve with a neutral promotion effect.

If no score start time matches, the engine should use an explicit configured default. The recommended default is `VERSION`.

### 6.2 MVCC Score Start-Time Semantics

The engine should support two policy-declared score start times:

- `CREATED`
- `VERSION`

These semantics must apply equally to nodes and edges.

#### `CREATED`

`CREATED` means the decay age is measured from the entity's original creation timestamp.

Semantics:

- MVCC determines which node or edge version is visible at the transaction snapshot
- the scorer uses the original creation timestamp as the start of decay age
- later updates do not reset decay age
- `CREATED` is the durable, age-from-origin option

#### `VERSION`

`VERSION` means the decay age is measured from the latest visible version timestamp under MVCC.

Semantics:

- MVCC still determines which node or edge version is visible at the transaction snapshot
- the scorer uses the latest visible version timestamp as the start of decay age
- updates reset decay age for the visible target
- `VERSION` is the freshness-from-last-change option

#### Rule

Visibility is always snapshot-based. Only the decay-age start time changes.

The system must not create new stored versions solely because a derived score changed.

### 6.3 Property-Level and Edge-Level Semantics

Property-level decay is required for mixed-longevity entities.

Examples:

- a `Profile` node may keep `name` and `tenantId` permanently while decaying `lastConversationSummary`
- a `Task` edge may keep identity and timestamps permanently while decaying a transient confidence field
- a `Document` node may keep canonical content permanently while decaying ranking hints or ephemeral summaries
- a `CO_ACCESSED` edge may decay as a whole, even if neither endpoint node decays at the same rate

Edge-level decay should support at least these outcomes:

- lowering ranking weight for an edge during retrieval or traversal
- archival or hiding of an edge while preserving endpoint nodes
- edge-specific decay independent of the decay policy of connected nodes

Property decay should support at least these outcomes:

- lower ranking weight for the property during retrieval
- exclusion of the property from vectorization or vector-backed retrieval if policy says so
- explicit supersession or replacement behavior in retrieval logic, if configured

Property-level promotion should support at least these outcomes:

- higher ranking weight for the property during retrieval
- tier-like score boosts for reinforced or validated properties
- score floor or cap adjustments without changing the parent entity's stored fields

Property-level scores should only influence retrieval when the property is directly involved in matching, ranking, reranking, filtering, projection, summarization, vectorization, or vector-backed retrieval unless an explicit roll-up policy says otherwise. A decayed or promoted property should not silently degrade or improve the score of the entire entity by default.

Edge decay should not be inferred from node decay by default. An edge must be able to decay on its own policy terms even if both endpoint nodes are non-decaying.

Properties are not archival targets. A property with a low archival-like score for vectorization may be excluded from vectorization outputs or vector-backed retrieval, but it remains stored in place and directly queryable in Cypher.

### 6.4 Archival Semantics

Archival applies only to whole nodes and whole edges.

When a node or edge crosses archive eligibility:

- the node or edge may be marked archived in primary storage
- the node or edge should be skipped by retrieval and ranking paths as efficiently as possible
- the node or edge must be removed from secondary indexing asynchronously
- the system must not scan secondary indexes to discover which entries to remove
- the system should use the target's stored index-entry catalog to perform direct key deletion

Property-level decay must not cause property archival, property movement, or property deletion from storage.

If a node remains indexed, its properties remain indexable under ordinary indexing rules. Property-level decay affects retrieval and vectorization behavior, not whether the property exists in storage.

### 6.5 Decay Function Semantics

The engine should support multiple decay function identifiers over time.

Initial supported scoring modes can include:

- `exponential`
- `linear`
- `step`
- `none`

The engine should resolve these as runtime scoring behavior, not as special categories.

These scoring modes should be accepted both:

- from resolved decay policy and constraint configuration, and
- from an explicit Cypher options object on decay scoring functions.

Cypher may override the policy-resolved scoring mode for the scope of that scoring expression only. Unified retrieval should not expose that override surface and should remain policy-resolved.

### 6.6 Promotion Composition Semantics

The engine should support multiple promotion-policy matches over time.

Initial supported promotion composition modes can include:

- `max`
- `multiply`
- `min`
- `sum_clamped`
- `replace`

Promotion composition should be resolved as runtime scoring behavior, not as special categories.

Promotion policies should be applied after the base decay score is resolved.

If no promotion policy matches, the final score should be the base decay score.

### 6.7 Explainability

For any entity or property, the system should be able to explain:

- whether decay applies
- which decay policy was selected
- which promotion policies matched
- which score start time was selected
- which decay policy and inline rule selected it
- which promotion policy and inline rule selected it
- what rate, threshold, floor, and multiplier are active
- whether decay age was measured from `CREATED` or `VERSION`
- why a node or edge was archived or not archived
- why a node or edge was deindexed or pending deindex
- why a property was excluded from vectorization or retrieval surfaces without being archived

### 6.8 Native Cypher Access

The decay subsystem should expose scoring through native Cypher functions so callers can inspect resolved scores without altering Neo4j-compatible node or relationship structures.

Proposed functions:

- `decayScore(entity)` returns the effective scalar decay score for a node or edge
- `decayScore(entity, { scoringMode: 'linear' })` returns the effective scalar decay score for a node or edge using the requested scoring mode
- `decayScore(entity, { property: 'summary' })` returns the effective scalar decay score for a specific property on that node or edge
- `decayScore(entity, { property: 'summary', scoringMode: 'step' })` returns the effective scalar decay score for a specific property using the requested scoring mode
- `decay(entity)` returns a structured decay object for the node or edge
- `decay(entity, { scoringMode: 'linear' })` returns a structured decay object for the node or edge using the requested scoring mode
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
- `scoreFrom`

The `decay(...)` object is a derived value. It should not imply that score metadata is being persisted back onto the node, edge, or property itself.

If a caller invokes `decayScore(...)` or `decay(...)` for a target with no matching policy, the function should return the non-decaying/default result rather than failing. The default scalar should be `1.0`, and the structured form should report a neutral non-decaying result.

The existing Cypher scoring API remains unchanged. The score returned by `decayScore(...)` and `decay(...).score` is the final resolved score after applying decay policy, the policy-declared score start time, and any matching promotion policies.

Archived properties do not exist as a concept. Properties remain directly queryable in Cypher even when property-level decay excludes them from vectorization or vector-backed retrieval.

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
MATCH ()-[r:CO_ACCESSED]-()
RETURN r, decayScore(r) AS edgeDecayScore
```

```cypher
MATCH ()-[r:CO_ACCESSED]-()
RETURN r.signalScore, decayScore(r, {property: 'signalScore'}) AS signalScoreDecay
```

```cypher
MATCH (n:SessionRecord)
RETURN n.summary, n.summary AS stillDirectlyQueryableInCypher
```

Compatibility rule:

- `RETURN n` remains Neo4j-compatible and does not automatically inject decay metadata into the node
- `RETURN r` remains Neo4j-compatible and does not automatically inject decay metadata into the edge
- callers opt in by returning `decayScore(...)` or `decay(...)` explicitly as additional columns
- property-level scores are therefore visible to Cypher without changing Bolt node or relationship structures
- missing decay policy should behave like ordinary metadata lookup in Cypher: no error, neutral score

Implementation rule:

- Cypher scoring functions should call the same shared runtime scorer used by unified retrieval scoring
- Cypher options objects should be validated against the accepted keys `property` and `scoringMode`
- supported Cypher `scoringMode` values remain: `exponential`, `linear`, `step`, `none`
- unified retrieval should call the same scorer but should not accept a caller-supplied `scoringMode`

### 6.9 Unified Search Metadata

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

Suggested conventions:

- top-level key by entity id
- entity-level score at `scores[id].decay`
- property-level scores nested at `scores[id].properties[propertyKey].decay`
- optional richer metadata can be added later beside `decay`, such as `policy`, `reason`, `scope`, or `scoreFrom`
- if no policy applies, `decay` should be reported as `1.0` unless an explicit configured default policy says otherwise

Suggested retrieval scoring inputs:

- options object with optional `property` when scoring needs to target a specific property
- options object may later grow additional explicit keys without breaking call-site semantics
- retrieval callers should not provide `scoringMode`; mode selection comes from resolved policy

The existing unified search metadata shape remains unchanged. Promotion-policy effects and score-start-time effects are reflected in the resolved score value rather than through a new response field, though richer metadata may optionally expose the selected `scoreFrom`.

Archived nodes and edges should be excluded from unified retrieval as soon as possible. Property-level exclusions should affect vectorization and vector-backed retrieval only, while stored properties remain directly queryable in Cypher.

---

## 7. Policy Subsystem Design

### 7.1 Policy Statements

Decay behavior should be authored through a dedicated decay policy subsystem, not through first-class constraints.

Promotion behavior should be authored through a dedicated promotion policy subsystem, not through first-class constraints.

`NO DECAY`, `DECAY RATE`, `DECAY ARCHIVE THRESHOLD`, `DECAY POLICY`, `DECAY FUNCTION`, `DECAY FLOOR`, and `DECAY SCOPE` are decay directives inside decay policy definitions. They are not standalone constraint types.

`PROMOTION POLICY`, `PROMOTION MULTIPLIER`, `PROMOTION FLOOR`, `PROMOTION CAP`, and `PROMOTION COMPOSE` are promotion directives inside promotion policy definitions. They are not standalone constraint types.

Score start time is declared through decay policy `OPTIONS { ... }`, not through new standalone syntax.

Suggested policy DDL surface:

- `CREATE DECAY POLICY`
- `ALTER DECAY POLICY`
- `DROP DECAY POLICY`
- `SHOW DECAY POLICIES`

Suggested promotion DDL surface:

- `CREATE PROMOTION POLICY`
- `ALTER PROMOTION POLICY`
- `DROP PROMOTION POLICY`
- `SHOW PROMOTION POLICIES`

These are NornicDB extensions, not Neo4j compatibility targets.

### 7.2 Valid Targets

Policies should be valid on:

- node labels
- edge types
- inline property paths on nodes within a policy body
- inline property paths on edges within a policy body

Edges are first-class policy targets and must support the same policy lifecycle as nodes, including creation, alteration, introspection, resolution, scoring, and archival behavior.

Properties are valid scoring targets, but not archival targets.

### 7.3 Policy Semantics

If decay is globally disabled, the decay policies still exist in schema but are operationally inactive until the subsystem is enabled.

If promotion is globally disabled, the promotion policies still exist in schema but are operationally inactive until the subsystem is enabled.

Conflicting policies or policy rules must resolve deterministically according to precedence rules rather than implicit ordering.

Property-level retention rules should be authored inline within the same decay policy body that declares the entity-level defaults for that label or edge type.

Property-level promotion rules should be authored inline within the same promotion policy body that declares the entity-level promotion behavior for that label or edge type.

Nested `FOR ... APPLY` entries should remain invalid inside a policy body. If operators need retention or promotion rules for a different label or edge type, they should create a separate targeted policy.

When property-level retention or promotion rules exist, the runtime should make the resolved score available through `decayScore(entity, {property: propertyKey})` and `decay(entity, {property: propertyKey})` even if the underlying Bolt result only returns the base node or edge structure.

The same shared scorer should also back retrieval scoring, using the scoring mode resolved from decay policy, the score start time resolved from decay policy, and the promotion adjustments resolved from promotion policy rather than a caller override.

Property-level rules may exclude properties from vectorization or vector-backed retrieval, but they do not archive, move, or delete properties from storage.

### 7.4 Sample Policies in Cypher

#### Node-level default policy with inline property rules

```cypher
CREATE DECAY POLICY session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY POLICY 'working_memory'
  DECAY ARCHIVE THRESHOLD 0.10
  n.summary DECAY POLICY 'session_summary'
  n.lastConversationSummary DECAY RATE 2592000
  n.tenantId NO DECAY
}
```

#### Edge-level default policy

```cypher
CREATE DECAY POLICY coaccess_edge_retention
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  DECAY POLICY 'edge_working_memory'
  DECAY ARCHIVE THRESHOLD 0.15
  r.externalId NO DECAY
}
```

#### Edge-level custom rate and property rules

```cypher
CREATE DECAY POLICY coaccess_retention
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  DECAY RATE 1209600
  r.signalScore DECAY RATE 1209600
  r.signalScore DECAY FLOOR 0.15
  r.externalId NO DECAY
}
```

#### Edge-level no-decay

```cypher
CREATE DECAY POLICY canonical_link_retention
FOR ()-[r:CANONICAL_LINK]-()
APPLY {
  NO DECAY
  r.externalId NO DECAY
  r.sourceSystem NO DECAY
}
```

#### Property-only override inside an edge block

```cypher
CREATE DECAY POLICY review_link_retention
FOR ()-[r:REVIEWED_WITH]-()
APPLY {
  DECAY RATE 604800
  r.confidence DECAY RATE 86400
  r.confidence DECAY FLOOR 0.25
}
```

#### Node-level promotion tiers with separate promotion policies

```cypher
CREATE PROMOTION POLICY session_record_tiering
FOR (n:SessionRecord)
APPLY {
  WHEN n.reinforcementCount >= 3
    APPLY PROMOTION POLICY 'reinforced_tier'

  WHEN n.reinforcementCount >= 5 AND n.sourceAgreement >= 0.95
    APPLY PROMOTION POLICY 'canonical_tier'

  PROMOTION COMPOSE 'max'
}
```

#### Edge-level promotion tiers

```cypher
CREATE PROMOTION POLICY coaccess_signal_tiering
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  WHEN r.coAccessCount >= 10
    APPLY PROMOTION POLICY 'reinforced_edge_tier'

  WHEN r.coAccessCount >= 50 AND r.crossSessionSupport >= 3
    APPLY PROMOTION POLICY 'canonical_edge_tier'

  PROMOTION COMPOSE 'max'
}
```

#### Edge property-level promotion tiers

```cypher
CREATE PROMOTION POLICY coaccess_signal_property_tiering
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  r.signalScore WHEN r.coAccessCount >= 10
    APPLY PROMOTION POLICY 'reinforced_signal_tier'

  r.signalScore WHEN r.coAccessCount >= 50 AND r.crossSessionSupport >= 3
    APPLY PROMOTION POLICY 'canonical_signal_tier'

  PROMOTION COMPOSE 'max'
}
```

#### Property-level vectorization exclusion without archival

```cypher
CREATE DECAY POLICY session_vectorization_rules
FOR (n:SessionRecord)
APPLY {
  n.summary DECAY POLICY 'session_summary'
  n.lastConversationSummary DECAY RATE 2592000
}
```

In this model, edges can decay just like nodes can. They can also have independent promotion policies and property-level overrides. Properties can be excluded from vectorization or vector-backed retrieval by policy, but they are never archived from storage.

### 7.5 Policy DDL

Decay policies should be first-class database objects in a dedicated subsystem, not ordinary graph nodes and not first-class constraints.

Promotion policies should be first-class database objects in a dedicated subsystem, not ordinary graph nodes and not first-class constraints.

Operators should be able to define decay policies independently and bind them via target selectors in the policy statement itself.

Operators should be able to define promotion policies independently and bind them via target selectors in the policy statement itself.

Example decay policy bootstrap with declarative score start time:

```cypher
CREATE DECAY POLICY durable_fact
OPTIONS {
  decayEnabled: false,
  archiveThreshold: 0.0,
  scope: 'NODE',
  function: 'none',
  scoreFrom: 'CREATED'
}

CREATE DECAY POLICY durable_edge
OPTIONS {
  decayEnabled: false,
  archiveThreshold: 0.0,
  scope: 'EDGE',
  function: 'none',
  scoreFrom: 'CREATED'
}

CREATE DECAY POLICY session_summary
OPTIONS {
  halfLifeSeconds: 1209600,
  archiveThreshold: 0.10,
  scope: 'PROPERTY',
  function: 'exponential',
  scoreFrom: 'VERSION'
}

CREATE DECAY POLICY working_memory
OPTIONS {
  halfLifeSeconds: 604800,
  archiveThreshold: 0.05,
  scope: 'NODE',
  function: 'exponential',
  scoreFrom: 'VERSION'
}

CREATE DECAY POLICY edge_working_memory
OPTIONS {
  halfLifeSeconds: 604800,
  archiveThreshold: 0.05,
  scope: 'EDGE',
  function: 'exponential',
  scoreFrom: 'VERSION'
}
```

Example promotion policy bootstrap:

```cypher
CREATE PROMOTION POLICY reinforced_tier
OPTIONS {
  scope: 'NODE',
  multiplier: 1.20,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION POLICY reinforced_edge_tier
OPTIONS {
  scope: 'EDGE',
  multiplier: 1.20,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION POLICY canonical_tier
OPTIONS {
  scope: 'NODE',
  multiplier: 1.35,
  scoreFloor: 0.85,
  scoreCap: 1.0
}

CREATE PROMOTION POLICY canonical_edge_tier
OPTIONS {
  scope: 'EDGE',
  multiplier: 1.35,
  scoreFloor: 0.85,
  scoreCap: 1.0
}

CREATE PROMOTION POLICY reinforced_signal_tier
OPTIONS {
  scope: 'PROPERTY',
  multiplier: 1.15,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION POLICY canonical_signal_tier
OPTIONS {
  scope: 'PROPERTY',
  multiplier: 1.30,
  scoreFloor: 0.0,
  scoreCap: 1.0
}
```

Suggested follow-on DDL:

```cypher
SHOW DECAY POLICIES
```

```cypher
SHOW PROMOTION POLICIES
```

```cypher
ALTER DECAY POLICY working_memory
SET OPTIONS {
  halfLifeSeconds: 432000,
  archiveThreshold: 0.08,
  scoreFrom: 'VERSION'
}
```

```cypher
ALTER DECAY POLICY durable_fact
SET OPTIONS {
  decayEnabled: false,
  function: 'none',
  scoreFrom: 'CREATED'
}
```

```cypher
ALTER PROMOTION POLICY canonical_tier
SET OPTIONS {
  multiplier: 1.30,
  scoreFloor: 0.80
}
```

```cypher
DROP DECAY POLICY session_summary
```

```cypher
DROP PROMOTION POLICY canonical_tier
```

---

## 8. Cypher, Search, and Storage Changes

### Suggested Cypher additions

- native scalar function: `decayScore(entity[, options])`
- native structured function: `decay(entity[, options])`
- both functions should work for nodes and edges
- `decay(...).score` should be the canonical Cypher-visible field for downstream sorting, filtering, and projection
- both functions derive scores from the shared resolver rather than reading persisted property-level score fields
- both functions accept an explicit Cypher options object with keys such as `property` and `scoringMode`

### Suggested storage rules

- decay eligibility and rate are resolved from decay policies and their compiled policy entries, not a baked-in tier enum
- promotion tier effects are resolved from promotion policies and their compiled policy entries, not a baked-in tier enum
- score start time is resolved from decay policy options, not from runtime defaults alone
- node-level and edge-level decay are both first-class resolved behaviors
- property-level decay scores are derived on demand and are not written into the entity's stored property map
- archived nodes and archived edges are persisted as archived state in primary storage
- archived nodes and archived edges are removed from secondary indexing asynchronously
- each node and edge should maintain an exact index-entry catalog to support direct deindex deletes
- archival cleanup should use batched blind deletes against known index keys rather than scanning indexes to discover stale entries
- the archival cleanup job should default to nightly execution but be configurable in seconds
- properties are never archived, moved, or deleted because of decay policy
- property-level decay may exclude properties from vectorization or vector-backed retrieval, but stored properties remain directly queryable in Cypher
- temporary caches of resolved scores are allowed as implementation detail, but they are not the source of truth
- policy resolution artifacts should be diagnosable without mutating the underlying entity
- no-decay policies should be enforced consistently across recall, archive, and maintenance paths
- derived score changes must not create new stored versions solely because time advanced

### Suggested search response behavior

- unified search may return node-, edge-, and property-level decay metadata additively in a separate `meta` section
- the `meta` section should mirror the same resolved scores available through `decayScore()` and `decay()`
- search hits themselves remain standard result entities plus ordinary ranking fields
- unified retrieval scoring should call the same shared scorer implementation as Cypher, but without exposing the Cypher-only `scoringMode` override
- archived nodes and edges must be skipped efficiently during retrieval, even before background deindexing fully drains
- property-level decay exclusions should apply to vectorization and vector-backed retrieval, not to storage visibility or direct Cypher access

---

## 9. Implementation Workstreams

### Workstream A: Policy Model

Deliverables:

- define the decay policy schema model
- define the promotion policy schema model
- define supported decay functions and thresholds
- define supported promotion composition modes and multipliers
- define supported score start-time values `CREATED` and `VERSION`
- define explainable resolution output
- define whole-node and whole-edge archival semantics
- define non-archival property exclusion semantics for vectorization and retrieval
- define the native `decayScore()` and `decay()` Cypher function contracts, including explicit `property` and `scoringMode` options
- define the derived search metadata contract for node-, edge-, and property-level scores

### Workstream B: Policy Authoring and Compilation

Deliverables:

- implement decay policy compilation for decay-aware entries
- implement promotion policy compilation for promotion-aware entries
- compile `scoreFrom` from decay policy `OPTIONS { ... }`
- support node-, edge-, and property-targeted policies
- validate creation-time behavior and introspection

### Workstream C: Shared Resolver

Deliverables:

- introduce a shared decay policy resolver
- introduce a shared promotion policy resolver
- support configurable decay rates and named presets
- support configurable promotion multipliers and named presets
- support `CREATED` and `VERSION` score-start resolution from decay policy
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
- support edge-level decay behavior
- support property-level promotion behavior
- support edge-level promotion behavior
- support MVCC-aware decay-age evaluation using policy-declared `CREATED` or `VERSION`
- support whole-node and whole-edge archive marking
- support fast archived-entity skipping in read paths

### Workstream E: Archival and Deindex Infrastructure

Deliverables:

- implement per-node and per-edge exact index-entry catalogs
- implement persistent archive work items for deindex cleanup
- implement configurable archive cleanup scheduling, default nightly and configurable in seconds
- perform asynchronous batched deindex deletes for archived nodes and edges
- ensure cleanup does not scan entire indexes to discover stale entries
- keep physical storage reclamation separate from logical archival and deindex behavior

### Workstream F: UI and Tooling

Deliverables:

- show effective decay policy and matching promotion policies in browser, diagnostics, and admin outputs
- show effective score start time in diagnostics and admin outputs
- show archived vs deindexed status for nodes and edges in diagnostics and admin outputs
- let operators inspect policies and resolution traces
- add diagnostics for why a node or edge decayed, promoted, archived, deindexed, or did not
- add diagnostics for why a property was excluded from vectorization or retrieval without being archived

---

## 10. Implementation Sequence

1. Define the decay policy schema model and resolution precedence.
2. Define the promotion policy schema model and resolution precedence.
3. Define the decay-policy `scoreFrom` option with supported values `CREATED` and `VERSION`.
4. Centralize decay resolution in a shared helper used by recall, recalc, archive, ranking, and stats paths.
5. Add configurable per-policy half-lives, decay rates, named presets, and function identifiers.
6. Add configurable per-policy promotion multipliers, named presets, and composition identifiers.
7. Define and implement schema-backed decay policy entries for nodes and edges in the dedicated decay policy subsystem.
8. Define and implement schema-backed promotion policy entries for nodes and edges in the dedicated promotion policy subsystem.
9. Extend policy parsing and compiled policy metadata to support property-targeted retention entries.
10. Extend policy parsing and compiled policy metadata to support property-targeted promotion entries.
11. Add MVCC-aware decay-age start resolution using the policy-declared `scoreFrom`.
12. Implement whole-node and whole-edge archive state transitions.
13. Implement per-entity index-entry catalogs for nodes and edges.
14. Implement persistent archive work items and configurable deindex scheduling.
15. Add asynchronous batched deindex cleanup for archived nodes and edges.
16. Add native Cypher functions `decayScore()` and `decay()` for nodes and edges with an explicit options object for `property` and optional `scoringMode`.
17. Migrate runtime logic away from fixed tier assumptions.
18. Bind unified retrieval scoring to the same shared scorer and policy-resolved scoring configuration.
19. Expose policy and resolution information in Cypher, search metadata, and UI surfaces.
20. Add regression tests for node-level, edge-level, and property-level resolution, score start-time selection, archive/deindex behavior, and archival skipping.
21. Add benchmark and evaluation coverage for policy resolution overhead and correctness.

---

## 11. Testing Plan

### Must-have regression cases

- no-decay nodes are skipped by recalc and archive paths
- no-decay edges are skipped by recalc and archive paths
- effective decay rate comes from resolved decay policy rather than hardcoded tier
- edge-level decay rules can age an edge independently of its endpoint nodes
- property-level inline decay rules can age one property without decaying the parent entity
- property-level inline promotion rules can boost one property without boosting the parent entity
- property-level vectorization exclusion does not archive, move, or delete the stored property
- properties excluded from vectorization remain directly queryable in Cypher
- conflicting policies resolve deterministically
- removing or changing a decay policy changes future resolution without corrupting stored history
- removing or changing a promotion policy changes future resolution without corrupting stored history
- node-level, edge-level, and inline property rules all resolve correctly
- explain output identifies the exact decay policy entry and exact promotion policy entry
- `decayScore(n)` and `decayScore(r)` return the same resolved score used by runtime policy evaluation
- `decay(n).score` and `decay(r).score` are Cypher-accessible and stable for projection and ordering
- returning `n` or `r` alone does not alter Neo4j-compatible result shape
- `decayScore(...)` returns `1.0` rather than an error when no decay policy or configured default applies
- unified search `meta` returns node, edge, and property decay scores in a separate keyed structure without mutating the hit payload
- unified retrieval scoring returns the same score family as the equivalent Cypher call when both use the same resolved policy
- decay policies and promotion policies are shown and retrieved separately through their respective subsystems
- `scoreFrom: 'CREATED'` measures decay age from original entity creation time
- `scoreFrom: 'VERSION'` measures decay age from the latest visible version time
- changing only derived score as time advances does not create a new stored version
- archived nodes and archived edges are skipped by retrieval paths
- archived nodes and archived edges are removed from indexing by the background cleanup process
- archive cleanup uses exact-key deindexing and does not require full index scans
- deindex cleanup is idempotent and retry-safe
- properties are never archived as part of archive cleanup

### Benchmark targets

- decay policy resolution overhead
- promotion policy resolution overhead
- score start-time resolution overhead
- edge-level decay selectivity and maintenance cost
- property-level decay selectivity and maintenance cost
- property-level promotion selectivity and ranking cost
- archive pass throughput under mixed node and edge policy workloads
- deindex throughput for archived nodes and edges
- recall and ranking overhead with resolved node and edge policy checks

---

## 12. Acceptance Criteria

The plan is complete when:

- no runtime path depends on a hardcoded tier enum to decide whether something decays or promotes
- operators can define decay semantics through config and decay policies
- operators can define promotive declarative tiers through config and promotion policies
- operators can define property-level retention inline in decay policy bodies
- operators can define property-level promotion inline in promotion policy bodies
- operators can declaratively choose `CREATED` or `VERSION` score start time through decay policy options
- node-, edge-, and property-level decay are all supported
- node-, edge-, and property-level promotion are all supported
- edges can decay just like nodes can
- only whole nodes and whole edges can be archived
- archived nodes and archived edges are removed from indexing asynchronously and efficiently
- background archival cleanup defaults to nightly execution and is configurable in seconds
- properties are never archived, moved, or deleted because of decay policy
- property-level decay can exclude properties from vectorization or vector-backed retrieval while preserving storage and direct Cypher access
- explainable policy resolution is available for diagnostics
- native Cypher functions expose resolved node, edge, and property scores without mutating Neo4j-compatible payloads
- unified search exposes the same resolved scores additively through response metadata rather than persisted fields
- Cypher scoring and unified retrieval scoring share the same scorer implementation, but only Cypher exposes an explicit `scoringMode` override
- targets without matching policy resolve to a neutral score of `1.0` instead of producing Cypher errors
- decay policies and promotion policies are authored, shown, and retrieved through separate subsystems
- new decay models and new promotion tier models can be expressed as policies without new engine categories
- the existing Cypher scoring API remains unchanged
- MVCC visibility remains snapshot-based while decay-age start time is declaratively selected by decay policy

---

## 14. Deliverables

- a policy-driven decay and scoring specification
- schema and Cypher/search updates for policy-aware decay behavior
- a shared decay policy resolver with config-backed defaults and compiled policy entries
- a shared promotion policy resolver with config-backed defaults and compiled policy entries
- dedicated decay-policy subsystem support for inline property-level retention rules
- dedicated promotion-policy subsystem support for inline property-level promotion rules
- declarative decay-policy support for `scoreFrom: 'CREATED' | 'VERSION'`
- explicit node and edge targeting support throughout policy resolution and scoring
- whole-node and whole-edge archival strategy with asynchronous deindex cleanup
- exact index-entry catalog support for nodes and edges
- archive work item infrastructure for background cleanup
- native Cypher function support for `decayScore()` and `decay()`
- shared runtime scorer support for Cypher and unified retrieval, with Cypher-only `scoringMode` override support and policy-resolved search scoring
- unified search metadata support for additive node-, edge-, and property-level decay scores
- regression tests covering node-, edge-, and property-level semantics
- user-facing documentation for decay policy authoring
- user-facing documentation for promotion policy authoring
- user-facing documentation for archival and deindex behavior

---

## 15. Notes

This plan is intentionally implementation-oriented. The main architectural shift is to stop using fixed categories as permanent engine concepts and instead operate on resolved policy.

Named presets may remain in documentation for bootstrapping a memory decay model or promotive tier model for operator convenience, but the engine should ultimately care only about effective decay policy, effective promotion policy, and policy-resolved score start time.

Property-level decay and promotion may affect vectorization and retrieval behavior, but properties remain stored in place and directly queryable in Cypher. Archival is reserved for whole nodes and whole edges.

Updated summary: added a dedicated archival/deindex layer, made archival apply only to whole nodes and edges, added exact index-entry catalogs plus archive work items for async cleanup, made the cleanup job nightly by default but configurable in seconds, and clarified that properties are never archived and only get excluded from vectorization/retrieval surfaces while remaining in storage and Cypher-visible.
