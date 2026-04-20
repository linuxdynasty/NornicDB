# Policy-Driven Decay and Scoring Plan

**Status:** Proposed
**Date:** April 15, 2026
**Scope:** Replace hardcoded Ebbinghaus memory-tier decay behavior with a generic, profile-and-policy-driven decay and scoring system that can support existing, proposed, or future decay models, while expressing promotive declarative tiers through separate promotion profile and policy subsystems, supporting MVCC-aware score-from selection for both nodes and edges, implementing efficient archival deindexing for archived nodes and edges, persisting `ON ACCESS` mutation state in a separate accessMeta index so that nodes and edges remain read-only during policy evaluation, evaluating scoring before query visibility so that invisible entities are suppressed from queries unless accessed through `reveal()`, and resolving promotion policies before decay profiles.

---

## 1. Objective

Implement a flexible decay and scoring architecture in NornicDB where retention behavior is resolved from policies rather than hardcoded cognitive tiers.

The system must support:

- no-decay entities and properties
- configurable decay rates and thresholds
- node-, edge-, and property-level decay behavior
- named policy presets for operator convenience
- separate promotion policies that declaratively model tier-like score boosts by referencing promotion profiles, without changing the existing Cypher scoring API
- declarative MVCC-aware score-from selection through decay profile options
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

The system should instead treat decay behavior as configurable retention profiles, promotion behavior as separate configurable scoring profiles and policies, score start time as an explicit profile decision, and archive cleanup as a dedicated deindex workflow for nodes and edges only.

---

## 3. Design Principles

1. Retention behavior must be data-driven, not hardcoded into a fixed enum.
2. Decay and scoring must be resolvable at node, edge, and property scope.
3. `NO DECAY` must be directly expressible in policy definitions.
4. Decay rate, decay function, archive threshold, and score floor must be configurable independently.
5. Promotion tiers must be expressible declaratively through separate promotion profile and promotion policy subsystems rather than through hardcoded runtime categories.
6. Score start time must be declaratively expressible through decay profile options using `CREATED`, `VERSION`, or `CUSTOM`.
7. Nodes and edges must be handled symmetrically by the policy system. Edge decay must not be a second-class or special-case feature.
8. Archive behavior applies only to whole nodes and whole edges, never to individual properties.
9. Property-level decay may influence vectorization, ranking, filtering, reranking, and summarization, but it must not move, archive, or delete stored property values.
9a. Properties that participate in general indexes are immune to decay scoring, decay hiding, and property-level exclusion. Indexed properties must remain stable and always visible because they are relied upon for aggregation, joining, and lookup.
10. Archived nodes and edges must be removed from indexing using exact-key deindexing rather than discovery by scanning secondary indexes.
11. Runtime paths must not silently fall back to legacy tier assumptions.
12. Named presets may exist for convenience, but the engine must operate on resolved profiles and policies.
13. The architecture must be flexible enough to support any current or future decay model.

---

## 4. Target Architecture

### 4.1 Decay Profile Layer

Decay profiles are the mechanism that decides whether decay applies, at what rate, at what scope, and from which score start time decay age is measured. Decay profiles are the only decay authoring surface — there is no separate decay policy concept.

Required behavior:

- resolve effective decay profile from configuration and profile definitions
- support node-, edge-, and property-level targeting
- allow `NO DECAY` and rate-based decay without relying on fixed tier names
- permit named presets but not require them
- support multiple decay functions over time
- support score start-time selection through profile options
- resolve archive eligibility for whole nodes and whole edges
- resolve property-level vectorization-exclusion behavior without treating properties as archival targets
- reject or ignore property-level decay rules that target properties participating in general indexes; indexed properties are immune to decay scoring and hiding
- enforce at most one decay profile per unique target as a hard constraint

Suggested fit in NornicDB:

- shared profile resolver used by recall, recalc, archive, and ranking paths
- config-defined presets for operator convenience
- schema-backed decay profiles as the main control surface
- diagnostics that explain why a given node or edge resolved to a given decay profile and score start time

### 4.2 Promotion Layer

Promotion behavior is split into two object types: profiles and policies.

Promotion profiles are named parameter bundles (multiplier, score floor, score cap, scope). They contain no logic and cannot be targeted to entities directly. They are referenced by name inside promotion policy APPLY blocks.

Promotion policies contain logic — `FOR` targets, `APPLY` blocks, `WHEN` predicates, and optional `ON ACCESS` mutation blocks. Policies bind profiles to specific node labels, edge types, and property paths. Promotion policies are resolved first, before decay profile resolution. The promotion adjustments are applied to the base decay score to produce the final score without changing the existing Cypher scoring API.

Required behavior:

- resolve applicable promotion profiles through promotion policy evaluation
- support node-, edge-, and property-level targeting
- allow promotion profiles to declare score multipliers, caps, and floors
- when multiple WHEN predicates match within a policy, the profile with the highest effective multiplier wins deterministically
- keep promotion profiles separately authored, shown, and retrieved from promotion policies
- support optional `ON ACCESS` mutation blocks that execute when the target is accessed during scoring resolution; `ON ACCESS` mutations write exclusively to a separate accessMeta index keyed to the target node or edge, never to the node or edge itself
- enforce at most one promotion policy per unique target as a hard constraint

Suggested fit in NornicDB:

- a dedicated promotion subsystem with its own catalog and DDL for both profiles and policies
- a separate accessMeta index that stores `ON ACCESS` mutation state per target node or edge as `map[string]interface{}`, serialized in msgpack alongside other data files for performance
- shared runtime scoring that first resolves the promotion policy, then resolves the decay profile, then applies promotion adjustments to the base decay score
- property reads within `ON ACCESS` blocks and `WHEN` predicates resolve from accessMeta first, falling back to the node or edge's stored properties
- diagnostics that explain which promotion policy matched, which profile was selected, and how it affected the final score

### 4.3 Authoring Subsystem Layer

The authoring subsystem is the surface for declaring decay profiles and promotion profiles and policies.

Required behavior:

- allow operators to declare decay profiles in Cypher
- allow operators to declare promotion profiles and promotion policies in Cypher
- validate definitions at creation time where applicable
- expose profiles and policies through introspection and admin APIs
- enforce one decay profile and one promotion policy per unique target
- support property-targeted rules in addition to node and edge targets

Suggested fit in NornicDB:

- introduce a dedicated decay profile subsystem with its own catalog and DDL
- introduce a dedicated promotion subsystem with its own catalog and DDL for both profiles and policies
- borrow authoring, validation, and introspection patterns from the constraint subsystem without making decay or promotion rules first-class constraints
- express property-level retention and promotion as inline entries inside profile or policy bodies
- add retention-specific and promotion-specific resolution rules alongside existing schema rules

### 4.4 Runtime Resolution Layer

The runtime resolution layer converts configuration and profiles into effective decay behavior and final score for a node, edge, or property. Scoring happens before query visibility — a node or edge must be scored before it becomes visible to the query.

Required behavior:

- evaluate the matching promotion policy first during recall, reinforcement, recalc, archive, and ranking, executing any `ON ACCESS` mutations before further scoring
- resolve decay profile second during recall, reinforcement, recalc, archive, and ranking
- resolve score start time from decay profile during score evaluation
- compute the final score from promotion and decay resolution before determining query visibility
- suppress nodes, edges, and properties from query results when their final score renders them invisible, unless the caller uses `reveal()` to bypass scoring-driven visibility
- support explicit overrides and inheritance
- allow property-level state without forcing entity-wide decay
- resolve inline property entries from the active decay profile before falling back to entity defaults
- expose final decay score through native Cypher functions without changing Neo4j-compatible node or relationship result shapes
- expose raw stored entities through `reveal()` without decay-driven visibility filtering or property hiding
- avoid duplicated logic across CLI, DB, and API code paths

Suggested fit in NornicDB:

- one shared resolver used by DB runtime, CLI decay tools, Cypher procedures, and background maintenance
- one explanation format returned by diagnostics and admin endpoints
- one shared scorer that evaluates promotion first, then computes base score from decay profile, then applies promotion adjustments to produce the final score
- one shared MVCC-aware score-start resolver that interprets `CREATED`, `VERSION`, and `CUSTOM`
- one `reveal()` bypass path that returns the raw stored entity, skipping scoring-driven visibility and property hiding

### 4.5 MVCC Interaction Layer

MVCC version resolution and decay scoring are separate concerns, but scoring gates query visibility. MVCC determines which version of an entity exists at the transaction snapshot. Scoring then determines whether that version is visible to the query.

Required behavior:

- resolve the visible node, edge, or property version using the transaction snapshot
- evaluate promotion policy and decay profile on the resolved version before exposing the entity to the query
- evaluate the base decay score using the score start time resolved from decay profile
- suppress entities whose final score falls below the archive threshold from query results, search hits, and traversal paths
- support `CREATED`, where decay age begins at the entity's original creation timestamp
- support `VERSION`, where decay age begins at the latest visible version timestamp under MVCC
- allow `reveal()` to bypass scoring-driven visibility and return the MVCC-resolved version without suppression
- never require new stored versions solely because a derived score changed over time

Suggested fit in NornicDB:

- version resolution remains owned by MVCC
- score start-time choice remains owned by decay profile
- the shared scorer consumes both the visible node or edge version and the profile-resolved score start time
- query visibility is determined after scoring: MVCC resolves the version, scoring determines whether it appears

### 4.6 Archival and Deindex Layer

The archival and deindex layer is the mechanism that removes archived whole nodes and whole edges from indexing in the most performant way possible.

Required behavior:

- archive only whole nodes and whole edges
- never archive, move, or delete individual properties because of decay profile
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

#### DecayProfile

Database object used to define reusable decay parameter bundles. Profiles contain no logic — they declare configuration values only.

Minimum fields:

- profile id
- profile name
- half-life or decay-rate definition in seconds
- scoring function or strategy id
- score start time: `CREATED`, `VERSION`, or `CUSTOM`
- custom score-from property path, if `CUSTOM`
- archive threshold override for node or edge archival eligibility
- minimum score floor
- scope type: node, edge, property
- enabled or disabled

#### PromotionProfile

Database object used to define reusable promotive scoring parameter bundles. Profiles contain no logic — they declare configuration values only.

Minimum fields:

- profile id
- profile name
- score multiplier
- optional score floor override
- optional score cap override
- scope type: node, edge, property
- enabled or disabled

#### PolicyBackedDecayRule

Logical rule compiled from decay profile definitions and used by the resolver.

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
- rule kind: promotion-profile, multiplier, floor, cap
- referenced policy name, if any
- predicate expression
- inline rule order for deterministic precedence
- original expression text for diagnostics

#### AccessMeta

Persistent metadata index that stores `ON ACCESS` mutation state separately from the node or edge it describes. Each entry is a `map[string]interface{}` keyed to a target node or edge identifier. AccessMeta entries are serialized in msgpack alongside other data files for performance.

Nodes and edges are read-only during `ON ACCESS` evaluation. All writes within an `ON ACCESS` block mutate the target's accessMeta entry, never the target's stored properties. All reads within `ON ACCESS` blocks and `WHEN` predicates resolve from the target's accessMeta entry first, falling back to the target's stored properties when the key is not present in accessMeta.

Minimum fields:

- target id
- target scope: node or edge
- metadata map: `map[string]interface{}`
- last accessed at
- last mutated at
- mutation count

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
- resolved decay profile id
- resolved score start time
- resolution source chain
- applied decay profile names
- applied decay profile entries
- applied promotion policy name
- applied promotion profile name selected by the policy
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
- the score start time is resolved from decay profile and used by the shared scorer without changing the existing Cypher scoring API
- whole-node and whole-edge archival state may be persisted
- property archival state is not persisted because properties are not archival targets
- property-level decay may exclude properties from vectorization or retrieval surfaces but must not move or delete stored property values
- `ON ACCESS` mutation state is persisted in a separate accessMeta index keyed per target node or edge, not on the node or edge itself
- accessMeta entries are `map[string]interface{}` serialized in msgpack alongside other data files for performance
- nodes and edges are read-only during `ON ACCESS` evaluation; all writes target the accessMeta index
- property reads within `ON ACCESS` blocks and `WHEN` predicates resolve from accessMeta first, falling back to stored node or edge properties
- the `policy()` Cypher function projects accessMeta outward without implying that access-tracking metadata is stored on the node or edge

---

## 6. Query and Resolution Semantics

### 6.1 Resolution Rules

Scoring happens before query visibility. When a query touches a node or edge, the engine must resolve and apply promotion and decay scoring before deciding whether the entity is visible to the query. An entity whose final score falls below the archive threshold or whose decay profile renders it invisible must not appear in `MATCH` results, `WHERE` evaluation, or search hits unless the caller explicitly uses `reveal(entity)` to bypass scoring-driven visibility.

The resolution order is: promotion first, then decay, then score-start resolution, then visibility determination.

Every scoring-aware read or maintenance operation should resolve the promotion policy first, in this order:

1. property-level promotion policy entries that match the target
2. entity-level promotion policy entries that match the target
3. edge-type or label-targeted promotion policy
4. wildcard-targeted promotion policy (`FOR (n:*)` or `FOR ()-[r:*]-()`)
5. configured default promotion behavior, if any

Then every scoring-aware operation should resolve the decay profile in this order:

6. explicit no-decay rule
7. property-level inline rule inside the applicable decay profile
8. entity-level rule inside the applicable decay profile
9. edge-type or label-targeted decay profile
10. wildcard-targeted decay profile (`FOR (n:*)` or `FOR ()-[r:*]-()`)
11. configured default decay profile

Then every score-aware read should resolve the score start time from the resolved decay profile:

12. `CREATED`, if the resolved decay profile declares `CREATED`
13. `VERSION`, if the resolved decay profile declares `VERSION`
14. `CUSTOM`, if the resolved decay profile declares `CUSTOM` with a `scoreFromProperty` path; the property is resolved from accessMeta first, falling back to stored node or edge properties; if the resolved value is null or unparsable, log a warning and fall back to entity creation time
15. configured default score start time, if no explicit profile value applies

Then the engine computes the final score and determines visibility:

16. compute the base decay score from the resolved decay profile and score start time
17. apply the resolved promotion policy adjustments to produce the final score
18. if the final score falls below the archive threshold, the entity is invisible to the query unless accessed through `reveal()`
19. if property-level decay excludes a property from retrieval surfaces, that property is hidden from the query result unless accessed through `reveal()`
20. properties that participate in general indexes are never subject to steps 18 or 19 — they are immune to decay scoring, decay hiding, and property-level exclusion regardless of any matching decay profile or promotion policy

If no promotion policy matches, the target should resolve with a neutral promotion effect.

If no decay profile matches, the engine should either treat the target as non-decaying or use an explicit configured default decay profile, but it must not silently assume any legacy tier.

If no score start time matches, the engine should use an explicit configured default. The recommended default is `VERSION`.

### 6.2 MVCC Score Start-Time Semantics

The engine should support three profile-declared score start times:

- `CREATED`
- `VERSION`
- `CUSTOM`

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

#### `CUSTOM`

`CUSTOM` means the decay age is measured from a user-specified property value on the entity.

Semantics:

- MVCC still determines which node or edge version is visible at the transaction snapshot
- the scorer reads the property path declared in the decay profile's `scoreFromProperty` option using accessMeta-first resolution: the property is resolved from the target's accessMeta entry first, falling back to the target's stored node or edge properties only when the key is not present in accessMeta
- the property value must be a timestamp; if the resolved value is missing, null, or not parsable as a timestamp, the scorer should log a warning and fall back to the entity's original creation time
- `CUSTOM` is the operator-defined, domain-specific option

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
- edge-specific decay independent of the decay profile of connected nodes

Property decay should support at least these outcomes:

- lower ranking weight for the property during retrieval
- exclusion of the property from vectorization or vector-backed retrieval if policy says so
- explicit supersession or replacement behavior in retrieval logic, if configured

Properties that participate in general indexes (lookup indexes, range indexes, composite indexes, or any index used for aggregation and joining) are immune to property-level decay scoring, decay hiding, and vectorization exclusion. These properties must remain stable and always visible to queries because index-backed operations depend on their values being present and consistent. If a decay profile or promotion policy contains a property-level rule that targets an indexed property, the engine should reject the rule at creation time with a validation error.

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

If a node remains indexed, its properties remain indexable under ordinary indexing rules. Property-level decay affects retrieval and vectorization behavior, not whether the property exists in storage. Properties that participate in general indexes are entirely immune to decay scoring, decay hiding, and vectorization exclusion — they must remain stable and always visible for aggregation, joining, and lookup.

### 6.5 Decay Function Semantics

The engine should support multiple decay function identifiers over time.

Initial supported scoring modes can include:

- `exponential`
- `linear`
- `step`
- `none`

The engine should resolve these as runtime scoring behavior, not as special categories.

These scoring modes should be accepted both:

- from resolved decay profile and constraint configuration, and
- from an explicit Cypher options object on decay scoring functions.

Cypher may override the profile-resolved scoring mode for the scope of that scoring expression only. Unified retrieval should not expose that override surface and should remain profile-resolved.

### 6.6 Promotion and Decay Resolution Order

Promotion policies are evaluated first. The promotion policy for the target is resolved and its `ON ACCESS` block is executed before decay profile resolution begins. This ensures that access-tracking mutations in accessMeta are available to decay scoring and visibility determination.

After promotion resolution, the decay profile is resolved and the base decay score is computed. The promotion adjustments are then applied to the base decay score to produce the final score. The final score determines query visibility.

If no promotion policy matches, the final score should be the base decay score.

When multiple `WHEN` predicates match within the same promotion policy, the profile with the highest effective multiplier wins. This is deterministic and does not require an explicit composition directive.

### 6.7 Explainability

For any entity or property, the system should be able to explain:

- whether decay applies
- which decay profile was selected
- which promotion policy matched and which profile was selected
- which score start time was selected
- which decay profile and inline rule selected it
- which promotion policy entry and WHEN predicate selected the profile
- what rate, threshold, floor, and multiplier are active
- whether decay age was measured from `CREATED`, `VERSION`, or `CUSTOM` and which property path was used if `CUSTOM`, whether the value was resolved from accessMeta or stored properties, and whether a fallback to entity creation time occurred due to a null or unparsable value
- why a node or edge was archived or not archived
- why a node or edge was deindexed or pending deindex
- why a property was excluded from vectorization or retrieval surfaces without being archived
- whether a property is immune to decay because it participates in a general index

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

The existing Cypher scoring API remains unchanged. The score returned by `decayScore(...)` and `decay(...).score` is the final resolved score after applying the decay profile, the profile-declared score start time, and the matching promotion policy.

The promotion policy subsystem should expose accessMeta through a native Cypher function so callers can inspect access-tracking state without altering Neo4j-compatible node or relationship structures.

Proposed function:

- `policy(entity)` returns the accessMeta map for the node or edge as a structured Cypher object

There is no correlated `policyScore()` scalar function. Unlike `decay()` / `decayScore()`, the accessMeta map is a general-purpose key-value store with no single canonical scalar to extract. Callers access individual keys through standard Cypher property access on the returned map, for example `policy(n).accessCount` or `policy(r).traversalCount`.

Suggested fields on `policy(...)` results:

- all keys present in the target's accessMeta entry, projected as a Cypher map
- `_targetId`: the target node or edge identifier
- `_targetScope`: `node` or `edge`
- `_lastAccessedAt`: timestamp of the most recent node access
- `_lastMutatedAt`: timestamp of the most recent `ON ACCESS` mutation
- `_mutationCount`: total number of `ON ACCESS` mutations applied

The `policy(...)` object is a derived value read from the accessMeta index. It does not imply that access-tracking metadata is stored on the node or edge itself.

If a caller invokes `policy(...)` for a target with no accessMeta entry, the function should return an empty map with only the `_targetId` and `_targetScope` fields rather than failing.

The scoring subsystem should expose a bypass function so callers can retrieve the raw stored entity without decay-driven visibility filtering or property hiding.

Proposed function:

- `reveal(entity)` returns the raw stored node or edge as it exists in primary storage, bypassing all scoring-driven visibility suppression and property-level decay hiding

`reveal()` does not disable scoring — the entity still has a resolved score. It disables the visibility gate that would otherwise hide the entity or its properties from the query result. `reveal()` is the only mechanism to access entities that are invisible due to scoring. It does not affect `decayScore()`, `decay()`, or `policy()` — those functions still return the resolved values.

When `reveal()` is used, the returned entity includes all stored properties, including any that would normally be hidden by property-level decay exclusion. The entity appears in query results regardless of its final score.

`reveal()` works on both nodes and edges. It should be usable in `RETURN`, `WITH`, `WHERE`, and any other Cypher clause that accepts an entity expression.

If decay is not enabled or the entity is not subject to any scoring-driven visibility suppression, `reveal()` is a no-op and returns the entity unchanged.

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

```cypher
MATCH (n:SessionRecord)
RETURN n, policy(n) AS accessMeta
```

```cypher
MATCH (n:SessionRecord)
WHERE policy(n).accessCount >= 5
RETURN n, policy(n).accessCount AS accessCount, policy(n)._lastMutatedAt AS lastAccessed
```

```cypher
MATCH ()-[r:CO_ACCESSED]-()
RETURN r, policy(r).traversalCount AS traversals, decay(r) AS decayMeta
```

```cypher
// Retrieve a node that may be invisible due to scoring
MATCH (n:SessionRecord {id: $id})
RETURN reveal(n) AS rawNode, decayScore(n) AS score
```

```cypher
// Retrieve all archived or hidden nodes with their scores for diagnostics
MATCH (n:SessionRecord)
RETURN reveal(n) AS rawNode, decay(n) AS decayMeta, policy(n) AS accessMeta
```

```cypher
// Bypass property-level hiding to see all stored properties
MATCH ()-[r:CO_ACCESSED]-()
RETURN reveal(r) AS rawEdge, reveal(r).signalScore AS rawSignal
```

Compatibility rule:

- `RETURN n` remains Neo4j-compatible and does not automatically inject decay metadata into the node; however, `n` is subject to scoring-driven visibility — if the entity's score renders it invisible, it will not appear in results unless accessed through `reveal(n)`
- `RETURN r` remains Neo4j-compatible and does not automatically inject decay metadata into the edge; same visibility rules apply
- `RETURN reveal(n)` or `RETURN reveal(r)` bypasses scoring-driven visibility and property hiding, returning the raw stored entity
- callers opt in by returning `decayScore(...)`, `decay(...)`, `policy(...)`, or `reveal(...)` explicitly as additional columns
- property-level scores are therefore visible to Cypher without changing Bolt node or relationship structures
- missing decay profile should behave like ordinary metadata lookup in Cypher: no error, neutral score

Implementation rule:

- Cypher scoring functions should call the same shared runtime scorer used by unified retrieval scoring
- Cypher options objects should be validated against the accepted keys `property` and `scoringMode`
- supported Cypher `scoringMode` values remain: `exponential`, `linear`, `step`, `none`
- unified retrieval should call the same scorer but should not accept a caller-supplied `scoringMode`

### 6.9 Unified Search Metadata

The unified search service should follow the same derived-on-read model as native Cypher.

It should not persist node-, edge-, or property-level decay scores into stored entity fields. Instead, when requested, it should add resolved scoring metadata into a separate response `meta` structure.

Unified retrieval scoring should use the same scorer as Cypher scoring functions, but it should remain profile-and-policy-resolved and should not expose the Cypher-only `scoringMode` override.

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
- retrieval callers should not provide `scoringMode`; mode selection comes from the resolved decay profile

The existing unified search metadata shape remains unchanged. Promotion-policy effects and score-start-time effects are reflected in the resolved score value rather than through a new response field, though richer metadata may optionally expose the selected `scoreFrom`.

Archived nodes and edges should be excluded from unified retrieval as soon as possible. Property-level exclusions should affect vectorization and vector-backed retrieval only, while stored properties remain directly queryable in Cypher.

---

## 7. Policy Subsystem Design

### 7.1 Policy Statements

Decay behavior should be authored through a dedicated decay profile subsystem, not through first-class constraints. Decay profiles are the only decay authoring surface — there is no separate decay policy concept.

Promotion behavior should be authored through dedicated promotion profile and promotion policy subsystems, not through first-class constraints.

`NO DECAY`, `DECAY RATE`, `DECAY ARCHIVE THRESHOLD`, `DECAY PROFILE`, and `DECAY FLOOR` are decay directives inside decay profile APPLY blocks. They are not standalone constraint types. Decay function and scope are declared through `OPTIONS { ... }` on the profile definition itself, not as inline APPLY-block directives.

`APPLY PROFILE` is the promotion directive inside promotion policy APPLY blocks. It is not a standalone constraint type. Multiplier, score floor, and score cap are declared through `OPTIONS { ... }` on the profile definition itself, not as inline APPLY-block directives.

Score start time is declared through decay profile `OPTIONS { ... }`, not through new standalone syntax.

Suggested decay DDL surface:

- `CREATE DECAY PROFILE`
- `ALTER DECAY PROFILE`
- `DROP DECAY PROFILE`
- `SHOW DECAY PROFILES`

Suggested promotion DDL surface:

- `CREATE PROMOTION PROFILE`
- `ALTER PROMOTION PROFILE`
- `DROP PROMOTION PROFILE`
- `SHOW PROMOTION PROFILES`
- `CREATE PROMOTION POLICY`
- `ALTER PROMOTION POLICY`
- `DROP PROMOTION POLICY`
- `SHOW PROMOTION POLICIES`

These are NornicDB extensions, not Neo4j compatibility targets.

### 7.2 Valid Targets

Decay profiles and promotion policies should be valid on:

- node labels
- edge types
- wildcard `*` meaning all node labels or all edge types
- inline property paths on nodes within a profile or policy body
- inline property paths on edges within a profile or policy body

The wildcard `*` target applies the profile or policy to every node label or every edge type in the database. A wildcard target is specified using `FOR (n:*)` for nodes or `FOR ()-[r:*]-()` for edges. Wildcard targeting is only valid on `CREATE` statements for decay profiles and promotion policies — it cannot be used inline within APPLY blocks or on ALTER statements.

A label-specific or edge-type-specific profile or policy takes precedence over a wildcard-targeted one. The wildcard acts as the default for any target that does not have its own explicit profile or policy.

Edges are first-class targets and must support the same lifecycle as nodes, including creation, alteration, introspection, resolution, scoring, and archival behavior.

Properties are valid scoring targets, but not archival targets.

Properties that participate in general indexes (lookup indexes, range indexes, composite indexes, or any index used for aggregation and joining) are not valid decay or promotion targets. If an inline property-level rule in a decay profile or promotion policy targets an indexed property, the engine should reject the rule at creation time with a validation error. This constraint ensures that indexed properties remain stable and always visible for index-backed operations.

There can be at most one decay profile and one promotion policy per unique target. Competing or overlapping definitions for the same target are a hard constraint violation. A wildcard target does not conflict with a label-specific or edge-type-specific target — the specific target wins. Two wildcard-scoped definitions for the same scope (node or edge) do conflict. If an operator needs different decay or promotion behavior for a different label or edge type, they must create a separate targeted profile or policy.

### 7.3 Profile Semantics

Promotion profiles are named parameter bundles. They contain no logic — no `FOR` targets, no `APPLY` blocks, no `WHEN` predicates, no `ON ACCESS` mutations. A promotion profile declares configuration values: multiplier, score floor, score cap, and scope. Promotion profiles cannot be targeted to entities directly — they are only referenced by name inside promotion policy APPLY blocks via `APPLY PROFILE 'name'`.

Decay profiles serve a dual role. A decay profile may be a named parameter bundle declaring configuration values through `OPTIONS` (half-life, scoring function, score start time, archive threshold, score floor, and scope), or it may be a targeted profile that binds directly to a node label or edge type via `FOR` and contains an `APPLY` block with inline decay directives and property-level rules. There is no separate decay policy concept — decay profiles are the only decay authoring surface.

Profiles may be altered, dropped, and introspected independently. Dropping a promotion profile that is still referenced by an active promotion policy should produce a validation error.

If decay is globally disabled, decay profiles still exist in schema but are operationally inert until the subsystem is enabled.

If promotion is globally disabled, promotion profiles and promotion policies still exist in schema but are operationally inert until the subsystem is enabled.

### 7.4 Policy Semantics

Policies exist only on the promotion side. There is no separate decay policy concept.

Promotion policies contain logic. They declare a target via `FOR`, contain an `APPLY` block, and may include `WHEN` predicates, `ON ACCESS` mutation blocks, and inline property-level rules. Promotion policies bind promotion profiles to specific node labels, edge types, and property paths through `WHEN` predicates and `APPLY PROFILE` references.

Promotion policies may include an `ON ACCESS` block that executes mutations when the policy is evaluated during scoring resolution. `ON ACCESS` blocks run before `WHEN` predicates are evaluated, allowing access-tracking mutations to feed into promotion logic within the same policy.

`ON ACCESS` mutations are applied exclusively to a separate accessMeta index, never to the target node or edge itself. The target node or edge is read-only during `ON ACCESS` evaluation. The accessMeta index stores a `map[string]interface{}` per target, keyed to the target node or edge identifier, and is serialized in msgpack alongside other data files for performance.

Property resolution within `ON ACCESS` blocks and `WHEN` predicates uses accessMeta-first semantics: a property read such as `n.accessCount` resolves from the target's accessMeta entry first, and falls back to the target's stored node or edge properties only when the key is not present in accessMeta. All writes such as `SET n.accessCount = ...` mutate the accessMeta entry for the target, not the target's stored properties. This means `ON ACCESS` Cypher syntax is unchanged — `n.propertyKey` and `r.propertyKey` work as expected — but the storage destination is the accessMeta index.

There can be at most one promotion policy per unique target. Competing or overlapping promotion policies for the same target are a hard constraint violation.

Property-level promotion rules should be authored inline within the same promotion policy body that declares the entity-level promotion behavior for that label or edge type.

Property-level retention rules should be authored inline within the same decay profile body that declares the entity-level defaults for that label or edge type.

Nested `FOR ... APPLY` entries should remain invalid inside a profile or policy body. If operators need different decay or promotion behavior for a different label or edge type, they must create a separate targeted profile or policy.

When property-level retention or promotion rules exist, the runtime should make the resolved score available through `decayScore(entity, {property: propertyKey})` and `decay(entity, {property: propertyKey})` even if the underlying Bolt result only returns the base node or edge structure.

The same shared scorer should also back retrieval scoring, using the scoring mode resolved from the decay profile, the score start time resolved from the decay profile, and the promotion adjustments resolved from the promotion policy rather than a caller override.

Property-level rules may exclude properties from vectorization or vector-backed retrieval, but they do not archive, move, or delete properties from storage.

### 7.5 Sample Policies in Cypher

#### Node-level default policy with inline property rules

```cypher
CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY ARCHIVE THRESHOLD 0.10
  n.summary DECAY PROFILE 'session_summary'
  n.lastConversationSummary DECAY RATE 2592000
  n.tenantId NO DECAY
}
```

#### Edge-level default policy

```cypher
CREATE DECAY PROFILE coaccess_edge_retention
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  DECAY PROFILE 'edge_working_memory'
  DECAY ARCHIVE THRESHOLD 0.15
  r.externalId NO DECAY
}
```

#### Edge-level custom rate and property rules

```cypher
CREATE DECAY PROFILE coaccess_retention
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
CREATE DECAY PROFILE canonical_link_retention
FOR ()-[r:CANONICAL_LINK]-()
APPLY {
  NO DECAY
  r.externalId NO DECAY
  r.sourceSystem NO DECAY
}
```

#### Property-only override inside an edge block

```cypher
CREATE DECAY PROFILE review_link_retention
FOR ()-[r:REVIEWED_WITH]-()
APPLY {
  DECAY RATE 604800
  r.confidence DECAY RATE 86400
  r.confidence DECAY FLOOR 0.25
}
```

#### Node-level promotion tiers with promotion policy

```cypher
CREATE PROMOTION POLICY session_record_tiering
FOR (n:SessionRecord)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
  }

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'

  WHEN n.accessCount >= 5 AND n.sourceAgreement >= 0.95
    APPLY PROFILE 'canonical_tier'

}
```

#### Edge-level promotion tiers

```cypher
CREATE PROMOTION POLICY coaccess_signal_tiering
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  ON ACCESS {
    SET r.traversalCount = coalesce(r.traversalCount, 0) + 1
    SET r.lastTraversedAt = timestamp()
  }

  WHEN r.traversalCount >= 10
    APPLY PROFILE 'reinforced_edge_tier'

  WHEN r.traversalCount >= 50 AND r.crossSessionSupport >= 3
    APPLY PROFILE 'canonical_edge_tier'

}
```

#### Edge property-level promotion tiers

```cypher
CREATE PROMOTION POLICY coaccess_signal_property_tiering
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  ON ACCESS {
    SET r.signalAccessCount = coalesce(r.signalAccessCount, 0) + 1
  }

  r.signalScore WHEN r.signalAccessCount >= 10
    APPLY PROFILE 'reinforced_signal_tier'

  r.signalScore WHEN r.signalAccessCount >= 50 AND r.crossSessionSupport >= 3
    APPLY PROFILE 'canonical_signal_tier'

}
```

#### Property-level vectorization exclusion without archival

```cypher
CREATE DECAY PROFILE session_vectorization_rules
FOR (n:SessionRecord)
APPLY {
  n.summary DECAY PROFILE 'session_summary'
  n.lastConversationSummary DECAY RATE 2592000
}
```

#### Wildcard decay profile for all nodes

```cypher
CREATE DECAY PROFILE default_node_retention
FOR (n:*)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY ARCHIVE THRESHOLD 0.05
}
```

#### Wildcard decay profile for all edges

```cypher
CREATE DECAY PROFILE default_edge_retention
FOR ()-[r:*]-()
APPLY {
  DECAY PROFILE 'edge_working_memory'
  DECAY ARCHIVE THRESHOLD 0.10
}
```

#### Wildcard promotion policy for all nodes

```cypher
CREATE PROMOTION POLICY default_node_promotion
FOR (n:*)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
  }

  WHEN n.reinforcementCount >= 3
    APPLY PROFILE 'reinforced_tier'
}
```

Wildcard targets use `*` in place of a label or edge type to match every node label or every edge type. A label-specific or edge-type-specific profile or policy always takes precedence over a wildcard-targeted one. Wildcards are only valid on `CREATE` statements.

In the promotion policy examples above, `ON ACCESS` SET statements such as `SET n.accessCount = coalesce(n.accessCount, 0) + 1` write exclusively to the accessMeta index for the target node or edge, not to the node or edge itself. The `coalesce(n.accessCount, 0)` read resolves from accessMeta first, falling back to the node's stored properties only when the key is absent from accessMeta. This keeps nodes and edges read-only during policy evaluation while preserving familiar Cypher property syntax.

In this model, edges can decay just like nodes can. They can also have independent promotion policies and property-level overrides. Properties can be excluded from vectorization or vector-backed retrieval by profile, but they are never archived from storage.

### 7.6 Profile and Policy DDL

Decay profiles should be first-class database objects in a dedicated subsystem, not ordinary graph nodes and not first-class constraints.

Promotion profiles and promotion policies should be first-class database objects in a dedicated subsystem, not ordinary graph nodes and not first-class constraints. Profiles are parameter bundles; policies contain logic and reference profiles.

Operators should be able to define decay profiles independently and bind them to targets via `FOR` selectors in the profile statement itself.

Operators should be able to define promotion profiles independently. Promotion profiles are referenced by name inside promotion policy APPLY blocks — they are not directly targeted to entities.

Operators should be able to define promotion policies independently and bind them to targets via `FOR` selectors in the policy statement itself.

Example decay profile bootstrap with declarative score start time:

```cypher
CREATE DECAY PROFILE durable_fact
OPTIONS {
  decayEnabled: false,
  archiveThreshold: 0.0,
  scope: 'NODE',
  function: 'none',
  scoreFrom: 'CREATED'
}

CREATE DECAY PROFILE durable_edge
OPTIONS {
  decayEnabled: false,
  archiveThreshold: 0.0,
  scope: 'EDGE',
  function: 'none',
  scoreFrom: 'CREATED'
}

CREATE DECAY PROFILE session_summary
OPTIONS {
  halfLifeSeconds: 1209600,
  archiveThreshold: 0.10,
  scope: 'PROPERTY',
  function: 'exponential',
  scoreFrom: 'VERSION'
}

CREATE DECAY PROFILE working_memory
OPTIONS {
  halfLifeSeconds: 604800,
  archiveThreshold: 0.05,
  scope: 'NODE',
  function: 'exponential',
  scoreFrom: 'VERSION'
}

CREATE DECAY PROFILE edge_working_memory
OPTIONS {
  halfLifeSeconds: 604800,
  archiveThreshold: 0.05,
  scope: 'EDGE',
  function: 'exponential',
  scoreFrom: 'VERSION'
}

CREATE DECAY PROFILE event_anchored
OPTIONS {
  halfLifeSeconds: 2592000,
  archiveThreshold: 0.05,
  scope: 'NODE',
  function: 'exponential',
  scoreFrom: 'CUSTOM',
  scoreFromProperty: 'eventTimestamp'
}
```

Example promotion profile bootstrap:

```cypher
CREATE PROMOTION PROFILE reinforced_tier
OPTIONS {
  scope: 'NODE',
  multiplier: 1.20,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE reinforced_edge_tier
OPTIONS {
  scope: 'EDGE',
  multiplier: 1.20,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE canonical_tier
OPTIONS {
  scope: 'NODE',
  multiplier: 1.35,
  scoreFloor: 0.85,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE canonical_edge_tier
OPTIONS {
  scope: 'EDGE',
  multiplier: 1.35,
  scoreFloor: 0.85,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE reinforced_signal_tier
OPTIONS {
  scope: 'PROPERTY',
  multiplier: 1.15,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE canonical_signal_tier
OPTIONS {
  scope: 'PROPERTY',
  multiplier: 1.30,
  scoreFloor: 0.0,
  scoreCap: 1.0
}
```

Suggested follow-on DDL:

```cypher
SHOW DECAY PROFILES
```

```cypher
SHOW PROMOTION PROFILES
```

```cypher
SHOW PROMOTION POLICIES
```

```cypher
ALTER DECAY PROFILE working_memory
SET OPTIONS {
  halfLifeSeconds: 432000,
  archiveThreshold: 0.08,
  scoreFrom: 'VERSION'
}
```

```cypher
ALTER DECAY PROFILE durable_fact
SET OPTIONS {
  decayEnabled: false,
  function: 'none',
  scoreFrom: 'CREATED'
}
```

```cypher
ALTER PROMOTION PROFILE canonical_tier
SET OPTIONS {
  multiplier: 1.30,
  scoreFloor: 0.80
}
```

```cypher
DROP DECAY PROFILE session_summary
```

```cypher
DROP PROMOTION PROFILE canonical_tier
```

---

## 8. Cypher, Search, and Storage Changes

### Suggested Cypher additions

- native scalar function: `decayScore(entity[, options])`
- native structured function: `decay(entity[, options])`
- native structured function: `policy(entity)` — returns the accessMeta map for the target node or edge; no correlated `policyScore()` scalar function
- native bypass function: `reveal(entity)` — returns the raw stored node or edge, bypassing scoring-driven visibility suppression and property-level decay hiding
- `decayScore(...)`, `decay(...)`, and `reveal(...)` should work for nodes and edges
- `policy(...)` should work for nodes and edges
- `decay(...).score` should be the canonical Cypher-visible field for downstream sorting, filtering, and projection
- `policy(...)` keys are accessed through standard Cypher map property access, for example `policy(n).accessCount`
- `decayScore(...)` and `decay(...)` derive scores from the shared resolver rather than reading persisted property-level score fields
- `policy(...)` reads from the accessMeta index rather than from stored node or edge properties
- `decayScore(...)` and `decay(...)` accept an explicit Cypher options object with keys such as `property` and `scoringMode`

### Suggested storage rules

- decay eligibility and rate are resolved from decay profiles and their compiled policy entries, not a baked-in tier enum
- promotion tier effects are resolved from promotion policies and the profiles they reference, not a baked-in tier enum
- score start time is resolved from decay profile options, not from runtime defaults alone
- node-level and edge-level decay are both first-class resolved behaviors
- property-level decay scores are derived on demand and are not written into the entity's stored property map
- archived nodes and archived edges are persisted as archived state in primary storage
- archived nodes and archived edges are removed from secondary indexing asynchronously
- each node and edge should maintain an exact index-entry catalog to support direct deindex deletes
- archival cleanup should use batched blind deletes against known index keys rather than scanning indexes to discover stale entries
- the archival cleanup job should default to nightly execution but be configurable in seconds
- properties are never archived, moved, or deleted because of decay profile
- property-level decay may exclude properties from vectorization or vector-backed retrieval, but stored properties remain directly queryable in Cypher via `reveal()`
- properties that participate in general indexes are immune to decay scoring, decay hiding, and property-level exclusion; they remain stable and always visible for aggregation, joining, and lookup
- property-level decay or promotion rules targeting indexed properties must be rejected at creation time
- scoring is evaluated before query visibility; promotion policies are resolved first, then decay profiles, and the final score determines whether the entity appears in query results
- entities whose final score renders them invisible are suppressed from `MATCH`, `WHERE`, search hits, and traversal paths unless accessed through `reveal()`
- `reveal()` bypasses scoring-driven visibility and property hiding but does not disable scoring itself
- accessMeta entries are persisted in a separate index keyed to the target node or edge, serialized in msgpack alongside other data files for performance
- `ON ACCESS` mutations write exclusively to the accessMeta index; nodes and edges are read-only during `ON ACCESS` evaluation
- property reads within `ON ACCESS` blocks and `WHEN` predicates resolve from accessMeta first, falling back to stored node or edge properties
- temporary caches of resolved scores are allowed as implementation detail, but they are not the source of truth
- profile and policy resolution artifacts should be diagnosable without mutating the underlying entity
- no-decay profiles should be enforced consistently across recall, archive, and maintenance paths
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

### Workstream A: Profile and Policy Model

Deliverables:

- define the decay profile schema model
- define the promotion profile and promotion policy schema models
- define supported decay functions and thresholds
- define supported promotion multipliers
- define supported score start-time values `CREATED`, `VERSION`, and `CUSTOM`
- define explainable resolution output
- define whole-node and whole-edge archival semantics
- define non-archival property exclusion semantics for vectorization and retrieval
- define indexed-property immunity: properties in general indexes are immune to decay scoring, hiding, and exclusion
- define the native `decayScore()` and `decay()` Cypher function contracts, including explicit `property` and `scoringMode` options
- define the native `policy()` Cypher function contract for accessMeta retrieval
- define the native `reveal()` Cypher function contract for bypassing scoring-driven visibility and property hiding
- define scoring-before-visibility semantics: promotion first, then decay, then visibility determination
- define the accessMeta schema model: `map[string]interface{}` keyed per target node or edge, serialized in msgpack
- define accessMeta-first property resolution semantics for `ON ACCESS` and `WHEN` evaluation
- define the derived search metadata contract for node-, edge-, and property-level scores

### Workstream B: Policy Authoring and Compilation

Deliverables:

- implement decay profile compilation for decay-aware entries
- implement promotion policy compilation for promotion-aware entries
- compile `scoreFrom` from decay profile `OPTIONS { ... }`
- support node-, edge-, and property-targeted policies
- validate creation-time behavior and introspection
- reject property-level decay or promotion rules that target properties participating in general indexes at creation time

### Workstream C: Shared Resolver

Deliverables:

- introduce a shared decay profile resolver
- introduce a shared promotion policy resolver
- support configurable decay rates and named presets
- support configurable promotion multipliers and named presets
- support `CREATED`, `VERSION`, and `CUSTOM` score-start resolution from decay profile
- define precedence and conflict rules for overlapping inline block entries
- expose an explainable resolution trace for any effective policy
- make resolved node-, edge-, and property-level scores available to native Cypher functions
- make the same resolved scores available to unified search metadata without persisting them into entity fields
- centralize profile-and-policy-resolved scoring so Cypher and unified retrieval call the same scorer
- keep Cypher-only scoring-mode override handling at the function surface while leaving unified retrieval profile-and-policy-resolved
- make accessMeta entries available to the native `policy()` Cypher function
- implement accessMeta-first property resolution for `ON ACCESS` reads and `WHEN` predicate evaluation

### Workstream D: Runtime Integration

Deliverables:

- route recall, recalc, archive, ranking, and stats paths through the shared resolver
- remove hardcoded tier branching from runtime code
- support property-level decay behavior
- support edge-level decay behavior
- support property-level promotion behavior
- support edge-level promotion behavior
- support MVCC-aware decay-age evaluation using profile-declared `CREATED`, `VERSION`, or `CUSTOM`
- implement scoring-before-visibility: resolve promotion first, then decay, then determine query visibility before exposing entities to the query
- implement scoring-driven visibility suppression for nodes, edges, and properties in `MATCH`, `WHERE`, search, and traversal paths
- implement `reveal()` bypass path that returns raw stored entities without scoring-driven visibility filtering or property hiding
- support whole-node and whole-edge archive marking
- support fast archived-entity skipping in read paths
- implement accessMeta index storage with msgpack serialization alongside other data files
- route `ON ACCESS` mutation writes to the accessMeta index, keeping nodes and edges read-only during policy evaluation
- implement accessMeta-first read resolution for property access within `ON ACCESS` blocks and `WHEN` predicates

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

- show effective decay profile and matching promotion policy in browser, diagnostics, and admin outputs
- show effective score start time in diagnostics and admin outputs
- show archived vs deindexed status for nodes and edges in diagnostics and admin outputs
- let operators inspect policies and resolution traces
- add diagnostics for why a node or edge decayed, promoted, archived, deindexed, or did not
- add diagnostics for why a property was excluded from vectorization or retrieval without being archived

---

## 10. Implementation Sequence

1. Define the decay profile schema model and resolution precedence.
2. Define the promotion profile and promotion policy schema models and resolution precedence.
3. Define the decay profile `scoreFrom` option with supported values `CREATED`, `VERSION`, and `CUSTOM`.
4. Centralize decay resolution in a shared helper used by recall, recalc, archive, ranking, and stats paths.
5. Add configurable per-profile half-lives, decay rates, named presets, and function identifiers.
6. Add configurable per-profile promotion multipliers and named presets.
7. Define and implement schema-backed decay profile entries for nodes and edges in the dedicated decay profile subsystem.
8. Define and implement schema-backed promotion policy entries for nodes and edges in the dedicated promotion subsystem.
9. Extend profile parsing and compiled metadata to support property-targeted retention entries.
10. Extend policy parsing and compiled metadata to support property-targeted promotion entries.
11. Add MVCC-aware decay-age start resolution using the profile-declared `scoreFrom`.
12. Implement whole-node and whole-edge archive state transitions.
13. Implement per-entity index-entry catalogs for nodes and edges.
14. Implement persistent archive work items and configurable deindex scheduling.
15. Add asynchronous batched deindex cleanup for archived nodes and edges.
16. Implement scoring-before-visibility: promotion first, then decay, then visibility determination before exposing entities to the query.
17. Implement the accessMeta index with msgpack serialization, `ON ACCESS` mutation routing, and accessMeta-first read resolution.
18. Add native Cypher functions `decayScore()` and `decay()` for nodes and edges with an explicit options object for `property` and optional `scoringMode`.
19. Add native Cypher function `policy()` for accessMeta retrieval on nodes and edges.
20. Add native Cypher function `reveal()` for bypassing scoring-driven visibility and property hiding on nodes and edges.
21. Migrate runtime logic away from fixed tier assumptions.
22. Bind unified retrieval scoring to the same shared scorer and profile-and-policy-resolved scoring configuration.
23. Expose policy and resolution information in Cypher, search metadata, and UI surfaces.
24. Add regression tests for node-level, edge-level, and property-level resolution, score start-time selection, scoring-before-visibility, `reveal()` bypass, accessMeta mutation and resolution, archive/deindex behavior, and archival skipping.
25. Add benchmark and evaluation coverage for profile and policy resolution overhead, scoring-before-visibility overhead, accessMeta read/write throughput, and correctness.

---

## 11. Testing Plan

### Must-have regression cases

- no-decay nodes are skipped by recalc and archive paths
- no-decay edges are skipped by recalc and archive paths
- effective decay rate comes from resolved decay profile rather than hardcoded tier
- edge-level decay rules can age an edge independently of its endpoint nodes
- property-level inline decay rules can age one property without decaying the parent entity
- property-level inline promotion rules can boost one property without boosting the parent entity
- property-level vectorization exclusion does not archive, move, or delete the stored property
- properties excluded from vectorization remain directly queryable in Cypher
- property-level decay or promotion rules targeting indexed properties are rejected at creation time with a validation error
- indexed properties remain visible in query results regardless of entity-level decay score
- indexed properties are never hidden by property-level decay exclusion
- indexed properties are never excluded from vectorization by property-level decay rules
- creating a general index on a property that already has a decay or promotion rule produces a validation error or warning
- conflicting profiles or policies for the same target are rejected at creation time
- removing or changing a decay profile changes future resolution without corrupting stored history
- removing or changing a promotion policy or profile changes future resolution without corrupting stored history
- node-level, edge-level, and inline property rules all resolve correctly
- explain output identifies the exact decay profile entry and exact promotion policy entry with the selected profile
- `decayScore(n)` and `decayScore(r)` return the same resolved score used by runtime profile and policy evaluation
- `decay(n).score` and `decay(r).score` are Cypher-accessible and stable for projection and ordering
- returning `n` or `r` alone does not alter Neo4j-compatible result shape
- `decayScore(...)` returns `1.0` rather than an error when no decay profile or configured default applies
- unified search `meta` returns node, edge, and property decay scores in a separate keyed structure without mutating the hit payload
- unified retrieval scoring returns the same score family as the equivalent Cypher call when both use the same resolved profile and policy
- decay profiles, promotion profiles, and promotion policies are shown and retrieved separately through their respective subsystems
- `scoreFrom: 'CREATED'` measures decay age from original entity creation time
- `scoreFrom: 'VERSION'` measures decay age from the latest visible version time
- `scoreFrom: 'CUSTOM'` with `scoreFromProperty` measures decay age from the specified property value
- `scoreFrom: 'CUSTOM'` resolves the `scoreFromProperty` from accessMeta first, falling back to stored node or edge properties
- `scoreFrom: 'CUSTOM'` logs a warning and falls back to entity creation time when the resolved value is missing, null, or unparsable as a timestamp
- changing only derived score as time advances does not create a new stored version
- archived nodes and archived edges are skipped by retrieval paths
- archived nodes and archived edges are removed from indexing by the background cleanup process
- archive cleanup uses exact-key deindexing and does not require full index scans
- deindex cleanup is idempotent and retry-safe
- properties are never archived as part of archive cleanup
- `ON ACCESS` SET mutations write to the accessMeta index and do not mutate the target node or edge
- `ON ACCESS` and `WHEN` property reads resolve from accessMeta first, falling back to stored node or edge properties
- `policy(n)` returns the accessMeta map for a node with all keys written by `ON ACCESS` mutations
- `policy(r)` returns the accessMeta map for an edge with all keys written by `ON ACCESS` mutations
- `policy(...)` returns an empty map with only `_targetId` and `_targetScope` when no accessMeta entry exists
- accessMeta entries survive node or edge reads without being lost or corrupted
- accessMeta entries are correctly serialized and deserialized via msgpack across restarts
- scoring is evaluated before query visibility: a node whose final score falls below the archive threshold does not appear in `MATCH` results
- scoring is evaluated before query visibility: a property excluded by decay does not appear on returned nodes unless accessed through `reveal()`
- promotion policies are resolved before decay profiles during scoring evaluation
- `reveal(n)` returns the raw stored node including all properties regardless of scoring-driven visibility
- `reveal(r)` returns the raw stored edge including all properties regardless of scoring-driven visibility
- `reveal()` does not alter the values returned by `decayScore()`, `decay()`, or `policy()` for the same entity
- `reveal()` is a no-op when decay is not enabled or the entity has no scoring-driven visibility suppression
- entities invisible due to scoring are accessible only through `reveal()` and not through ordinary `MATCH`

### Benchmark targets

- decay profile resolution overhead
- promotion policy resolution overhead
- score start-time resolution overhead
- edge-level decay selectivity and maintenance cost
- property-level decay selectivity and maintenance cost
- property-level promotion selectivity and ranking cost
- archive pass throughput under mixed node and edge profile workloads
- deindex throughput for archived nodes and edges
- recall and ranking overhead with resolved node and edge profile and policy checks
- accessMeta index read and write throughput under concurrent `ON ACCESS` mutation workloads
- accessMeta-first property resolution overhead compared to direct node or edge property reads
- accessMeta msgpack serialization and deserialization throughput
- scoring-before-visibility overhead per entity during `MATCH` and search execution
- `reveal()` bypass overhead compared to ordinary scored entity access

---

## 12. Acceptance Criteria

The plan is complete when:

- no runtime path depends on a hardcoded tier enum to decide whether something decays or promotes
- operators can define decay semantics through config and decay profiles
- operators can define promotive declarative tiers through config, promotion profiles, and promotion policies
- operators can define property-level retention inline in decay profile bodies
- operators can define property-level promotion inline in promotion policy bodies
- operators can declaratively choose `CREATED`, `VERSION`, or `CUSTOM` score start time through decay profile options
- node-, edge-, and property-level decay are all supported
- node-, edge-, and property-level promotion are all supported
- edges can decay just like nodes can
- only whole nodes and whole edges can be archived
- archived nodes and archived edges are removed from indexing asynchronously and efficiently
- background archival cleanup defaults to nightly execution and is configurable in seconds
- properties are never archived, moved, or deleted because of decay profile
- property-level decay can exclude properties from vectorization or vector-backed retrieval while preserving storage and direct Cypher access
- properties that participate in general indexes are immune to decay scoring, decay hiding, and property-level exclusion
- property-level decay or promotion rules targeting indexed properties are rejected at creation time
- explainable profile and policy resolution is available for diagnostics
- native Cypher functions expose resolved node, edge, and property scores without mutating Neo4j-compatible payloads
- unified search exposes the same resolved scores additively through response metadata rather than persisted fields
- Cypher scoring and unified retrieval scoring share the same scorer implementation, but only Cypher exposes an explicit `scoringMode` override
- targets without a matching decay profile or promotion policy resolve to a neutral score of `1.0` instead of producing Cypher errors
- decay profiles, promotion profiles, and promotion policies are authored, shown, and retrieved through separate subsystems
- new decay models can be expressed as decay profiles and new promotion tier models can be expressed as promotion profiles and policies without new engine categories
- the existing Cypher scoring API remains unchanged
- MVCC visibility remains snapshot-based while decay-age start time is declaratively selected by decay profile
- `ON ACCESS` mutation handlers write exclusively to a separate accessMeta index; nodes and edges are read-only during policy evaluation
- accessMeta entries are stored as `map[string]interface{}` keyed per target node or edge, serialized in msgpack alongside other data files
- property reads within `ON ACCESS` blocks and `WHEN` predicates resolve from accessMeta first, falling back to stored node or edge properties
- the native `policy()` Cypher function exposes accessMeta for any node or edge without mutating Neo4j-compatible payloads
- there is no correlated `policyScore()` scalar function; accessMeta is a general-purpose map, not a single score
- targets without an accessMeta entry return an empty map from `policy()` rather than producing a Cypher error
- scoring is evaluated before query visibility: promotion first, then decay, then visibility determination
- entities whose final score renders them invisible are suppressed from `MATCH`, `WHERE`, search hits, and traversal paths
- `reveal()` is the only mechanism to access scoring-invisible entities and decay-hidden properties in Cypher
- `reveal()` bypasses visibility suppression and property hiding without disabling scoring itself

---

## 14. Deliverables

- a profile-and-policy-driven decay and scoring specification
- schema and Cypher/search updates for profile-aware decay behavior and policy-aware promotion behavior
- a shared decay profile resolver with config-backed defaults and compiled profile entries
- a shared promotion policy resolver with config-backed defaults and compiled policy entries
- dedicated decay profile subsystem support for inline property-level retention rules, with creation-time validation rejecting rules that target indexed properties
- dedicated promotion policy subsystem support for inline property-level promotion rules, with creation-time validation rejecting rules that target indexed properties
- declarative decay profile support for `scoreFrom: 'CREATED' | 'VERSION' | 'CUSTOM'`
- explicit node and edge targeting support throughout profile and policy resolution and scoring
- whole-node and whole-edge archival strategy with asynchronous deindex cleanup
- exact index-entry catalog support for nodes and edges
- archive work item infrastructure for background cleanup
- native Cypher function support for `decayScore()` and `decay()`
- native Cypher function support for `policy()` to expose accessMeta on nodes and edges
- native Cypher function support for `reveal()` to bypass scoring-driven visibility and property hiding on nodes and edges
- scoring-before-visibility runtime implementation: promotion first, then decay, then visibility determination before query exposure
- accessMeta index implementation with msgpack serialization, `ON ACCESS` mutation routing, and accessMeta-first property read resolution
- shared runtime scorer support for Cypher and unified retrieval, with Cypher-only `scoringMode` override support and profile-and-policy-resolved search scoring
- unified search metadata support for additive node-, edge-, and property-level decay scores
- regression tests covering node-, edge-, and property-level semantics, scoring-before-visibility, and `reveal()` bypass
- user-facing documentation for decay profile authoring
- user-facing documentation for promotion profile and promotion policy authoring
- user-facing documentation for archival and deindex behavior

---

## 15. Notes

This plan is intentionally implementation-oriented. The main architectural shift is to stop using fixed categories as permanent engine concepts and instead operate on resolved profiles and policies.

Named presets may remain in documentation for bootstrapping a memory decay model or promotive tier model for operator convenience, but the engine should ultimately care only about effective decay profile, effective promotion policy with its selected profile, and profile-resolved score start time.

Property-level decay and promotion may affect vectorization and retrieval behavior, but properties remain stored in place and directly queryable in Cypher via `reveal()`. Archival is reserved for whole nodes and whole edges.

Updated summary: added a dedicated archival/deindex layer, made archival apply only to whole nodes and edges, added exact index-entry catalogs plus archive work items for async cleanup, made the cleanup job nightly by default but configurable in seconds, and clarified that properties are never archived and only get excluded from vectorization/retrieval surfaces while remaining in storage and Cypher-visible. Added a separate accessMeta index for `ON ACCESS` mutation state, making nodes and edges read-only during policy evaluation. AccessMeta entries are `map[string]interface{}` keyed per target, serialized in msgpack alongside other data files. Property reads in `ON ACCESS` and `WHEN` blocks resolve from accessMeta first, falling back to stored node or edge properties. Added the native `policy()` Cypher function to expose accessMeta, with no correlated `policyScore()` scalar. Changed resolution order to promotion first, then decay. Scoring now happens before query visibility — entities whose final score renders them invisible are suppressed from `MATCH`, `WHERE`, search hits, and traversal paths. Added the native `reveal()` Cypher function to bypass scoring-driven visibility suppression and property-level decay hiding, returning the raw stored entity. Added indexed-property immunity: properties that participate in general indexes (lookup, range, composite, or any index used for aggregation and joining) are immune to decay scoring, decay hiding, and property-level exclusion; rules targeting indexed properties are rejected at creation time.
