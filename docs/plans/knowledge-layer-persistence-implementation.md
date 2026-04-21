# Policy-Driven Decay and Scoring — Implementation Plan

**Status:** Draft
**Date:** April 21, 2026
**Parent Design:** [knowledge-layer-persistence-plan.md](./knowledge-layer-persistence-plan.md)
**Target Version:** 1.1.0 (incompatible break with experimental memory model)

---

## Overview

This document is the concrete implementation plan for the policy-driven decay and scoring system described in the parent design document. It maps the design's six workstreams to specific Go packages, files, types, functions, Badger key prefixes, schema persistence changes, Cypher parser additions, and test suites — sequenced into phases with explicit dependencies, acceptance gates, and migration notes.

The plan is intentionally file-and-function-level. Each phase produces a shippable, testable increment. No phase depends on a later phase.

---

## Current State Audit

### What exists today (to be replaced)

| Component | Location | Description |
|-----------|----------|-------------|
| `decay.Tier` enum | `pkg/decay/decay.go:77-128` | Hardcoded `TierEpisodic`, `TierSemantic`, `TierProcedural` with fixed half-lives |
| `decay.Manager` | `pkg/decay/decay.go` | Tier-based scoring, reinforcement, archival logic |
| `Node.DecayScore` | `pkg/storage/types.go:197` | Stored float64 on the Node struct |
| `Node.LastAccessed` | `pkg/storage/types.go:198` | Stored time on the Node struct |
| `Node.AccessCount` | `pkg/storage/types.go:199` | Stored int64 on the Node struct |
| `inference.EdgeDecay` | `pkg/inference/edge_decay.go` | Separate edge decay with confidence-based model |
| Replication codec fields | `pkg/replication/codec.go` | Sends `DecayScore` in replication |
| CLI decay commands | `cmd/` (decay stats, recalculate, archive) | Tier-aware CLI |
### What exists today (to be preserved or extended)

| Component | Location | Description |
|-----------|----------|-------------|
| Cypher Kalman functions | `pkg/cypher/kalman_functions.go` | Kalman-filter scoring — internal Cypher functions, unrelated to decay policy system, retained as-is |
| `SchemaManager` | `pkg/storage/schema.go:72-91` | Constraint/index catalog — new subsystem maps go here |
| `SchemaDefinition` | `pkg/storage/schema_persistence.go:14-27` | Persisted schema — new definition sections added here |
| Badger prefix keys | `pkg/storage/badger.go:21-36` | `0x01`–`0x10` allocated — new prefixes start at `0x11` |
| msgpack serialization | `pkg/storage/badger_serialization.go` | Already supports msgpack — accessMeta uses this |
| MVCC version system | `pkg/storage/badger_mvcc.go` | Version resolution, snapshot reads — scorer receives snapshot timestamp |
| Feature flags | `pkg/config/feature_flags.go` | Global enable/disable — decay and promotion get flags here |
| `constraint_contracts.go` | `pkg/storage/constraint_contracts.go` | Pattern for keyword scanning DDL — decay/promotion DDL follows this |
| Canonical graph ledger | `docs/user-guides/canonical-graph-ledger.md` | Knowledge-layer persistence for facts — referenced by design |

---

## New Badger Key Prefixes

```go
// pkg/storage/badger.go — append to existing prefix block
prefixAccessMeta       = byte(0x11) // accessmeta:entityID -> msgpack(AccessMetaEntry)
prefixIndexEntryCatalog = byte(0x12) // idxcat:entityID -> msgpack(IndexEntryCatalog)
prefixArchiveWorkItem  = byte(0x13) // archwork:workItemID -> msgpack(ArchiveWorkItem)
prefixDecayProfile     = byte(0x14) // decayprofile:name -> msgpack(DecayProfileDef)
prefixPromotionProfile = byte(0x15) // promoprofile:name -> msgpack(PromotionProfileDef)
prefixPromotionPolicy  = byte(0x16) // promopolicy:name -> msgpack(PromotionPolicyDef)
```

---

## Phase 1: Schema Objects and Profile Model

**Goal:** Define all types, persistence, and DDL for decay profiles, promotion profiles, and promotion policies. No runtime scoring yet — just the authoring surface.

**Depends on:** Nothing. This is the foundation.

### 1.1 New package: `pkg/retention`

Create `pkg/retention/` as the home for all new decay/promotion types and the shared resolver. Keeping it separate from `pkg/decay/` avoids entangling with legacy tier code during migration.

#### Files and types

**`pkg/retention/types.go`** — Core schema objects:

```go
// DecayFunction identifies a scoring function.
type DecayFunction string

const (
    DecayFunctionExponential DecayFunction = "exponential"
    DecayFunctionLinear      DecayFunction = "linear"
    DecayFunctionStep        DecayFunction = "step"
    DecayFunctionNone        DecayFunction = "none"
)

// ScoreFromMode identifies the score start-time anchor.
type ScoreFromMode string

const (
    ScoreFromCreated ScoreFromMode = "CREATED"
    ScoreFromVersion ScoreFromMode = "VERSION"
    ScoreFromCustom  ScoreFromMode = "CUSTOM"
)

// ScopeType identifies whether a profile/policy targets nodes, edges, or properties.
type ScopeType string

const (
    ScopeNode     ScopeType = "NODE"
    ScopeEdge     ScopeType = "EDGE"
    ScopeProperty ScopeType = "PROPERTY"
)

// DecayProfileBundle is a reusable parameter bundle (no FOR clause).
type DecayProfileBundle struct {
    Name              string        `msgpack:"name"`
    HalfLifeSeconds   int64         `msgpack:"halfLifeSeconds"`
    ArchiveThreshold  float64       `msgpack:"archiveThreshold"`
    ScoreFloor        float64       `msgpack:"scoreFloor"`
    Function          DecayFunction `msgpack:"function"`
    Scope             ScopeType     `msgpack:"scope"`
    DecayEnabled      bool          `msgpack:"decayEnabled"`
    ScoreFrom         ScoreFromMode `msgpack:"scoreFrom"`
    ScoreFromProperty string        `msgpack:"scoreFromProperty,omitempty"` // for CUSTOM
    Enabled           bool          `msgpack:"enabled"`
}

// DecayProfilePropertyRule is an inline property-level rule inside a binding.
type DecayProfilePropertyRule struct {
    PropertyPath     string         `msgpack:"propertyPath"`
    NoDecay          bool           `msgpack:"noDecay,omitempty"`
    ProfileRef       string         `msgpack:"profileRef,omitempty"`    // reference to a bundle name
    HalfLifeSeconds  int64          `msgpack:"halfLifeSeconds,omitempty"`
    ScoreFloor       float64        `msgpack:"scoreFloor,omitempty"`
    Order            int            `msgpack:"order"` // deterministic precedence
}

// DecayProfileBinding is a targeted binding (has FOR clause).
type DecayProfileBinding struct {
    Name              string                     `msgpack:"name"`
    TargetLabels      []string                   `msgpack:"targetLabels,omitempty"` // sorted; empty = wildcard
    TargetEdgeType    string                     `msgpack:"targetEdgeType,omitempty"`
    IsWildcard        bool                       `msgpack:"isWildcard"`
    IsEdge            bool                       `msgpack:"isEdge"`
    ProfileRef        string                     `msgpack:"profileRef,omitempty"` // DECAY PROFILE 'name'
    NoDecay           bool                       `msgpack:"noDecay,omitempty"`
    ArchiveThreshold  *float64                   `msgpack:"archiveThreshold,omitempty"` // override
    PropertyRules     []DecayProfilePropertyRule `msgpack:"propertyRules,omitempty"`
}

// PromotionProfileDef is a reusable promotion parameter bundle.
type PromotionProfileDef struct {
    Name       string    `msgpack:"name"`
    Scope      ScopeType `msgpack:"scope"`
    Multiplier float64   `msgpack:"multiplier"`
    ScoreFloor float64   `msgpack:"scoreFloor"`
    ScoreCap   float64   `msgpack:"scoreCap"`
    Enabled    bool      `msgpack:"enabled"`
}

// PromotionPolicyWhenClause is a WHEN predicate inside a policy.
type PromotionPolicyWhenClause struct {
    PropertyPath   string `msgpack:"propertyPath,omitempty"` // empty = entity-level
    Predicate      string `msgpack:"predicate"`              // raw expression text
    ProfileRef     string `msgpack:"profileRef"`             // APPLY PROFILE 'name'
    Order          int    `msgpack:"order"`
}

// PromotionPolicyOnAccess is the ON ACCESS block definition.
type PromotionPolicyOnAccess struct {
    Mutations []string `msgpack:"mutations"` // raw SET expressions
}

// PromotionPolicyDef is a targeted promotion policy.
type PromotionPolicyDef struct {
    Name           string                      `msgpack:"name"`
    TargetLabels   []string                    `msgpack:"targetLabels,omitempty"`
    TargetEdgeType string                      `msgpack:"targetEdgeType,omitempty"`
    IsWildcard     bool                        `msgpack:"isWildcard"`
    IsEdge         bool                        `msgpack:"isEdge"`
    OnAccess       *PromotionPolicyOnAccess    `msgpack:"onAccess,omitempty"`
    WhenClauses    []PromotionPolicyWhenClause `msgpack:"whenClauses,omitempty"`
    Enabled        bool                        `msgpack:"enabled"`
}
```

**`pkg/retention/access_meta.go`** — AccessMeta types:

```go
// AccessMetaFixedFields is the fast-path fixed-layout struct.
type AccessMetaFixedFields struct {
    AccessCount    int64 `msgpack:"accessCount"`
    LastAccessedAt int64 `msgpack:"lastAccessedAt"` // UnixNano
    TraversalCount int64 `msgpack:"traversalCount"`
    LastTraversedAt int64 `msgpack:"lastTraversedAt"` // UnixNano
}

// AccessMetaEntry is the full persisted entry per target.
type AccessMetaEntry struct {
    TargetID    string                 `msgpack:"targetId"`
    TargetScope ScopeType              `msgpack:"targetScope"` // NODE or EDGE
    Fixed       AccessMetaFixedFields  `msgpack:"fixed"`
    Overflow    map[string]interface{} `msgpack:"overflow,omitempty"`
    LastMutatedAt int64                `msgpack:"lastMutatedAt"` // UnixNano
    MutationCount int64               `msgpack:"mutationCount"`
}
```

**`pkg/retention/compiled_binding.go`** — Compiled binding table:

```go
// CompiledBinding is the pre-flattened lookup entry for a label/edge-type.
type CompiledBinding struct {
    DecayProfile       *DecayProfileBundle
    DecayBinding       *DecayProfileBinding
    PromotionPolicy    *PromotionPolicyDef
    ArchiveThreshold   float64
    ScoreFrom          ScoreFromMode
    ScoreFromProperty  string
    Function           DecayFunction
    HalfLifeNanos      int64
    ThresholdAgeNanos  int64   // pre-computed: -halfLife * ln(archiveThreshold) / ln(2)
    DecayFloor         float64
    NoDecay            bool
}

// BindingTable is the compiled lookup for all labels and edge types.
type BindingTable struct {
    mu       sync.RWMutex
    nodes    map[string]*CompiledBinding // key: sorted label set (e.g., "Label1\x00Label2")
    edges    map[string]*CompiledBinding // key: edge type
    wildNode *CompiledBinding
    wildEdge *CompiledBinding
}
```

### 1.2 Schema persistence extensions

**`pkg/storage/schema_persistence.go`** — Add to `SchemaDefinition`:

```go
// Add these fields to the SchemaDefinition struct:
DecayProfileBundles  []retention.DecayProfileBundle  `json:"decay_profile_bundles,omitempty"`
DecayProfileBindings []retention.DecayProfileBinding  `json:"decay_profile_bindings,omitempty"`
PromotionProfiles    []retention.PromotionProfileDef  `json:"promotion_profiles,omitempty"`
PromotionPolicies    []retention.PromotionPolicyDef   `json:"promotion_policies,omitempty"`
```

**`pkg/storage/schema.go`** — Add to `SchemaManager`:

```go
// Add these fields to the SchemaManager struct:
decayProfileBundles  map[string]*retention.DecayProfileBundle  // key: profile name
decayProfileBindings map[string]*retention.DecayProfileBinding // key: binding name
promotionProfiles    map[string]*retention.PromotionProfileDef // key: profile name
promotionPolicies    map[string]*retention.PromotionPolicyDef  // key: policy name
bindingTable         *retention.BindingTable                   // compiled, rebuilt on DDL change
```

Add methods: `CreateDecayProfileBundle()`, `CreateDecayProfileBinding()`, `DropDecayProfile()`, `AlterDecayProfile()`, `ShowDecayProfiles()`, `CreatePromotionProfile()`, `CreatePromotionPolicy()`, `DropPromotionProfile()`, `DropPromotionPolicy()`, `AlterPromotionProfile()`, `AlterPromotionPolicy()`, `ShowPromotionProfiles()`, `ShowPromotionPolicies()`.

### 1.3 DDL parsing

**`pkg/cypher/retention_ddl.go`** — Keyword-scanning parser for decay/promotion DDL:

Parse the following statements using the `ccSkipSpaces`/`ccScanIdent`/`ccMatchKeywordAt` pattern from `pkg/storage/constraint_contracts.go`:

- `CREATE DECAY PROFILE <name> OPTIONS { ... }`
- `CREATE DECAY PROFILE <name> FOR (n:<Label>) APPLY { ... }`
- `CREATE DECAY PROFILE <name> FOR ()-[r:<Type>]-() APPLY { ... }`
- `ALTER DECAY PROFILE <name> SET OPTIONS { ... }`
- `DROP DECAY PROFILE <name>`
- `SHOW DECAY PROFILES`
- `CREATE PROMOTION PROFILE <name> OPTIONS { ... }`
- `ALTER PROMOTION PROFILE <name> SET OPTIONS { ... }`
- `DROP PROMOTION PROFILE <name>`
- `SHOW PROMOTION PROFILES`
- `CREATE PROMOTION POLICY <name> FOR ... APPLY { ... }`
- `ALTER PROMOTION POLICY <name> ...`
- `DROP PROMOTION POLICY <name>`
- `SHOW PROMOTION POLICIES`

Each parser function returns a typed command struct (e.g., `CreateDecayProfileCmd`). The executor maps these to `SchemaManager` calls.

### 1.4 Validation rules (creation-time)

Implement in `SchemaManager` methods:

1. Reject duplicate target bindings (at most one decay profile and one promotion policy per unique target).
2. Reject property-level rules targeting properties in structural indexes (lookup, range, composite). Query the existing index maps to check.
3. Reject `DROP PROMOTION PROFILE` when the profile is referenced by an active promotion policy.
4. Reject `DROP DECAY PROFILE` (bundle) when referenced by an active binding.
5. Reject multi-label conflicts (see design §6.1 multi-label node resolution).
6. Validate `OPTIONS` key names and value types.

### 1.5 Feature flags

**`pkg/config/feature_flags.go`** — Add:

```go
DecaySubsystemEnabled     bool // default: false — gates new policy-driven decay
PromotionSubsystemEnabled bool // default: false — gates promotion policies
```

**`pkg/config/config.go`** — Add corresponding YAML and env var bindings:

```yaml
features:
  decay_subsystem: true
  promotion_subsystem: true
```

### Phase 1 acceptance gate

- All decay/promotion types compile and have msgpack round-trip tests.
- DDL parsing produces correct command structs for all 14 statement types.
- Schema persistence round-trips all new definition sections.
- Validation rejects illegal targets, duplicates, and indexed-property rules.
- Feature flags gate the new subsystem; existing behavior unchanged when flags are off.
- `SHOW DECAY PROFILES`, `SHOW PROMOTION PROFILES`, `SHOW PROMOTION POLICIES` return stored definitions.

### Phase 1 test files

- `pkg/retention/types_test.go` — msgpack serialization round-trips
- `pkg/retention/compiled_binding_test.go` — binding table compilation
- `pkg/cypher/retention_ddl_test.go` — DDL parsing for all statement forms
- `pkg/storage/schema_retention_test.go` — SchemaManager CRUD, validation, persistence

---

## Phase 2: Shared Resolver and Scorer

**Goal:** Implement the resolution cascade and scoring engine. After this phase, any code path can resolve the effective decay/promotion configuration for a node or edge and compute a score — but no read paths are wired yet.

**Depends on:** Phase 1.

### 2.1 Resolver

**`pkg/retention/resolver.go`**:

```go
// Resolver resolves effective decay and promotion configuration for a target.
type Resolver struct {
    bindingTable *BindingTable
}

// Resolve returns the RetentionResolution for a node identified by its sorted labels.
func (r *Resolver) ResolveNode(labels []string) *RetentionResolution

// Resolve returns the RetentionResolution for an edge identified by its type.
func (r *Resolver) ResolveEdge(edgeType string) *RetentionResolution

// ResolveProperty returns the resolution for a specific property on a node or edge.
func (r *Resolver) ResolveProperty(entityLabelsOrType interface{}, propertyPath string) *RetentionResolution
```

**`pkg/retention/resolution.go`** — `RetentionResolution` struct (design §5.2):

Fields: `TargetID`, `TargetScope`, `ResolvedDecayProfileID`, `ResolvedScoreFrom`, `ResolutionSourceChain`, `AppliedDecayProfileNames`, `AppliedPromotionPolicyName`, `AppliedPromotionProfileName`, `EffectiveRate`, `EffectiveThreshold`, `EffectiveMultiplier`, `BaseScore`, `FinalScore`, `NoDecay`, `ArchiveEligible`, `Explanation`.

### 2.2 Scorer

**`pkg/retention/scorer.go`**:

```go
// Scorer computes decay and promotion scores.
type Scorer struct {
    resolver *Resolver
}

// ScoreNode computes the final score for a node at scoringTime.
// scoringTime is the MVCC snapshot timestamp (not time.Now()).
func (s *Scorer) ScoreNode(
    labels []string,
    createdAt int64,       // UnixNano
    versionAt int64,       // UnixNano — latest visible version timestamp
    scoringTime int64,     // UnixNano — transaction snapshot time
    accessMeta *AccessMetaEntry, // may be nil
) *RetentionResolution

// ScoreEdge computes the final score for an edge at scoringTime.
func (s *Scorer) ScoreEdge(
    edgeType string,
    createdAt int64,
    versionAt int64,
    scoringTime int64,
    accessMeta *AccessMetaEntry,
) *RetentionResolution

// ScoreProperty computes the score for a specific property.
func (s *Scorer) ScoreProperty(
    entityLabelsOrType interface{},
    propertyPath string,
    createdAt int64,
    versionAt int64,
    scoringTime int64,
    accessMeta *AccessMetaEntry,
) *RetentionResolution
```

Scoring formula (design §6.6):

```go
func computeFinalScore(baseDecayScore, multiplier, promoFloor, promoCap, decayFloor float64) float64 {
    promoted := baseDecayScore * multiplier
    floored := math.Max(promoted, promoFloor)
    capped := math.Min(floored, promoCap)
    return math.Max(capped, decayFloor)
}
```

Decay functions:

```go
func exponentialDecay(ageNanos, halfLifeNanos int64) float64 {
    return math.Exp(-float64(ageNanos) * math.Ln2 / float64(halfLifeNanos))
}

func linearDecay(ageNanos, halfLifeNanos int64) float64 {
    ratio := float64(ageNanos) / float64(halfLifeNanos * 2) // reaches 0 at 2×halfLife
    return math.Max(0, 1.0 - ratio)
}

func stepDecay(ageNanos, halfLifeNanos int64) float64 {
    if ageNanos < halfLifeNanos { return 1.0 }
    return 0.0
}
```

### 2.3 Compiled binding table builder

**`pkg/retention/binding_builder.go`**:

Called by `SchemaManager` on every DDL change. Rebuilds the `BindingTable` from all stored bundles, bindings, profiles, and policies. Pre-computes `thresholdAgeNanos` for the fast-path integer comparison (design §6.1 Tier 3).

### Phase 2 acceptance gate

- Resolver returns correct `RetentionResolution` for all label/edge-type/property combinations.
- Scorer produces correct scores for all decay functions × score-from modes.
- Multi-label resolution picks the most specific match.
- Wildcard fallback works correctly.
- No-decay targets resolve to `1.0`.
- Targets with no matching profile resolve to `1.0` (neutral).
- `thresholdAgeNanos` fast-path matches `math.Exp` path for all test cases.

### Phase 2 test files

- `pkg/retention/resolver_test.go` — resolution cascade for all target types
- `pkg/retention/scorer_test.go` — scoring correctness, all functions, all score-from modes
- `pkg/retention/binding_builder_test.go` — compiled table correctness
- `pkg/retention/scorer_bench_test.go` — benchmark: integer fast-path vs float path

---

## Phase 3: AccessMeta Index

**Goal:** Implement the accessMeta index, the sharded counter ring for hot-path accumulation, and the flush goroutine. After this phase, ON ACCESS mutations can be accumulated and persisted.

**Depends on:** Phase 1 (types). Independent of Phase 2.

### 3.1 Sharded counter ring

**`pkg/retention/access_accumulator.go`**:

```go
const shardCount = 64

type counterShard struct {
    accessCount     atomic.Int64
    traversalCount  atomic.Int64
    lastAccessedAt  atomic.Int64 // UnixNano
    lastTraversedAt atomic.Int64 // UnixNano
    // overflow for custom keys — protected by a per-shard mutex
    mu       sync.Mutex
    overflow map[string]int64
}

type AccessAccumulator struct {
    shards [shardCount]counterShard
    // entityID → shard index mapping is hash(entityID) % shardCount
}

func (a *AccessAccumulator) IncrementAccess(entityID string)
func (a *AccessAccumulator) IncrementTraversal(entityID string)
func (a *AccessAccumulator) IncrementCustom(entityID string, key string, delta int64)
func (a *AccessAccumulator) SetTimestamp(entityID string, key string, ts int64)

// ReadThrough returns persisted + buffered delta for WHEN predicate evaluation.
func (a *AccessAccumulator) ReadThrough(entityID string, key string, persisted int64) int64
```

### 3.2 Badger persistence

**`pkg/storage/badger_access_meta.go`**:

```go
func (b *BadgerEngine) GetAccessMeta(entityID string) (*retention.AccessMetaEntry, error)
func (b *BadgerEngine) PutAccessMeta(entityID string, entry *retention.AccessMetaEntry) error
func (b *BadgerEngine) DeleteAccessMeta(entityID string) error
func (b *BadgerEngine) ScanAccessMetaPrefix(prefix string) ([]*retention.AccessMetaEntry, error)
```

Key format: `[prefixAccessMeta][entityID bytes]`

Serialization: fixed fields as known-size byte slice (no reflection), overflow map via msgpack. Pre-allocated byte buffers from `sync.Pool`.

### 3.3 Flush goroutine

**`pkg/retention/access_flusher.go`**:

```go
type AccessFlusher struct {
    accumulator *AccessAccumulator
    store       AccessMetaStore // interface satisfied by BadgerEngine
    interval    time.Duration   // configurable, default 2s
}

func (f *AccessFlusher) Start(ctx context.Context)
func (f *AccessFlusher) Stop()
```

Flush loop: iterate shards, atomically swap non-zero deltas, merge into persisted entries via single batched Badger write. Timestamps: write latest value, not accumulation.

### 3.4 Entity lifecycle integration

- **Node/edge deletion:** Enqueue accessMeta key for deletion in the same transaction. Clear from accumulator immediately.
- **Node/edge archival:** Retain accessMeta (accessible via `reveal()`). Delete only on physical reclamation.
- **MVCC version pruning:** Check for orphaned accessMeta entries when all versions are pruned.

### Phase 3 acceptance gate

- Sharded counter ring correctly accumulates increments under concurrent goroutines.
- Flush goroutine persists accumulated values to Badger.
- Read-through returns `persisted + buffered delta`.
- accessMeta survives restart (msgpack round-trip).
- Deletion cascades from node/edge deletion.
- accessMeta retained on archival.

### Phase 3 test files

- `pkg/retention/access_accumulator_test.go` — concurrent increment correctness
- `pkg/retention/access_flusher_test.go` — flush persistence, batching
- `pkg/storage/badger_access_meta_test.go` — CRUD, serialization round-trip, deletion cascade
- `pkg/retention/access_accumulator_bench_test.go` — throughput under contention

---

## Phase 4: Runtime Integration — Scoring-Before-Visibility

**Goal:** Wire the scorer into all read paths. Nodes and edges are scored before they become visible to queries. This is the core behavioral change.

**Depends on:** Phase 2 (scorer), Phase 3 (accessMeta).

### 4.1 MVCC read-path integration

**`pkg/storage/badger_mvcc.go`** — Modify node/edge read paths:

After MVCC version resolution, before returning the entity to the caller:

1. Check archived bit (Tier 2 fast path — one byte check, skip if archived and no `reveal()`).
2. Look up compiled binding for the entity's labels/edge-type (Tier 1 — single map lookup).
3. If binding exists and `NoDecay` is false:
   a. Check `now - scoreFrom > thresholdAgeNanos` (Tier 3 — integer comparison, no `math.Exp`).
   b. If below threshold and no `reveal()` in query context: suppress entity (return nil/skip).
   c. If surviving visibility: attach `RetentionResolution` to entity context for lazy score computation.
4. If `reveal()` is active for this binding: always materialize, still compute score for `decayScore()`/`decay()`.

The `scoringTime` passed to the scorer is `txn.ReadTimestamp` (already available in the MVCC read path as the snapshot timestamp).

### 4.2 Query context for reveal()

**`pkg/cypher/query_context.go`** — Add:

```go
type QueryContext struct {
    // ... existing fields ...
    RevealedBindings map[string]bool // variable names marked as visibility-bypassed
}
```

The query planner detects `reveal(variable)` during plan compilation and sets the flag. The storage read path checks this flag.

### 4.3 Property-level visibility

When the scorer indicates property-level decay hiding:
- During node/edge projection (Cypher RETURN), omit hidden properties from the result map.
- If `reveal()` is active: include all properties.
- Properties in structural indexes: never hidden (immune).

### 4.4 Search path integration

**`pkg/search/`** — Unified search:

- After vector/BM25/RRF candidate retrieval, apply scorer to each candidate.
- Suppress invisible candidates.
- If LIMIT was requested and visible results < LIMIT, continue pulling chunks until LIMIT is satisfied or index is exhausted (design §6.9 chunked retrieval).
- Add `scores` metadata to response (design §6.9 preferred shape).

### 4.5 Background maintenance paths

- **Recalc:** Use scorer with `scoringTime = time.Now()` at cycle start, frozen for batch.
- **Archive pass:** Entities whose final score falls below archive threshold → mark archived in primary storage, enqueue deindex work item.
- **Stats:** Report decay distribution based on scorer resolution, not hardcoded tiers.

### Phase 4 acceptance gate

- `MATCH (n:MemoryEpisode)` does not return nodes below archive threshold.
- `MATCH (n:MemoryEpisode) RETURN reveal(n)` returns all nodes including invisible ones.
- `MATCH (n:KnowledgeFact)` always returns facts (no-decay).
- Property-level hiding works: hidden properties absent from RETURN unless `reveal()`.
- Indexed properties always visible regardless of decay.
- Vector search returns expected LIMIT count despite decay filtering.
- Same scorer invoked by Cypher and unified search.

### Phase 4 test files

- `pkg/storage/badger_mvcc_decay_test.go` — visibility suppression, reveal bypass, property hiding
- `pkg/cypher/reveal_test.go` — reveal() plan compilation, per-variable bypass
- `pkg/search/decay_filter_test.go` — chunked retrieval, LIMIT satisfaction
- `pkg/retention/integration_test.go` — end-to-end: DDL → score → visibility

---

## Phase 5: Cypher Functions

**Goal:** Implement `decayScore()`, `decay()`, `policy()`, and `reveal()` as native Cypher functions.

**Depends on:** Phase 4 (scoring wired into read paths).

### 5.1 Function registration

**`pkg/cypher/retention_functions.go`**:

Register in the existing function registry (same pattern as `kalman_functions.go`):

- `decayScore(entity)` → `float64` — calls `Scorer.ScoreNode/ScoreEdge`, returns `FinalScore`.
- `decayScore(entity, options)` → `float64` — `options.property`, `options.scoringMode`.
- `decay(entity)` → `map[string]any` — structured result with `.score`, `.policy`, `.scope`, `.function`, `.archiveThreshold`, `.floor`, `.applies`, `.reason`, `.scoreFrom`.
- `decay(entity, options)` → `map[string]any` — property/scoringMode variants.
- `policy(entity)` → `map[string]any` — reads from accessMeta index via accumulator read-through.
- `reveal(entity)` → plan-level marker, not a runtime function. Detected by planner, not function registry. Returns entity unchanged at runtime.

### 5.2 Options validation

Validate `options` map keys against accepted set: `property`, `scoringMode`. Reject unknown keys at parse time. Validate `scoringMode` values against `exponential`, `linear`, `step`, `none`.

### 5.3 Default behavior

- `decayScore()` on a target with no matching profile: return `1.0`.
- `decay()` on a target with no matching profile: return `{score: 1.0, applies: false, reason: "no decay profile", ...}`.
- `policy()` on a target with no accessMeta: return `{_targetId: id, _targetScope: "node"}`.

### Phase 5 acceptance gate

- All Cypher examples from the design document (§6.8, Appendix A) execute correctly.
- `decayScore(n)` matches the score used for visibility determination.
- `decay(n).score` is accessible and stable for ORDER BY.
- `policy(n).accessCount` reads from accessMeta.
- `reveal(n)` works in RETURN, WITH, WHERE, ORDER BY.
- Options validation rejects unknown keys.

### Phase 5 test files

- `pkg/cypher/retention_functions_test.go` — all function variants, default behavior
- `pkg/cypher/retention_functions_options_test.go` — options validation
- `pkg/cypher/retention_reveal_integration_test.go` — reveal in all clause positions

---

## Phase 6: Archival and Deindex Infrastructure

**Goal:** Implement per-entity index-entry catalogs, archive work items, and the background deindex cleanup job.

**Depends on:** Phase 4 (archive marking).

### 6.1 Index-entry catalog

**`pkg/storage/badger_index_catalog.go`**:

When a node or edge is indexed (written to any secondary index), record the exact index keys in an `IndexEntryCatalog` entry:

```go
type IndexEntryCatalog struct {
    TargetID    string   `msgpack:"targetId"`
    TargetScope string   `msgpack:"targetScope"` // "NODE" or "EDGE"
    IndexKeys   [][]byte `msgpack:"indexKeys"`   // exact Badger keys written
}
```

Key format: `[prefixIndexEntryCatalog][entityID bytes]`

This is maintained on every index write. When a node is re-indexed (properties changed), the catalog is updated with the new key set.

### 6.2 Archive work items

**`pkg/storage/badger_archive_work.go`**:

When a node or edge is marked archived:

1. Persist `ArchiveWorkItem` with the entity's index catalog reference.
2. The background cleanup job drains work items.

```go
type ArchiveWorkItem struct {
    WorkItemID   string    `msgpack:"workItemId"`
    TargetID     string    `msgpack:"targetId"`
    TargetScope  string    `msgpack:"targetScope"`
    EnqueuedAt   int64     `msgpack:"enqueuedAt"` // UnixNano
    NextAttemptAt int64    `msgpack:"nextAttemptAt"`
    RetryCount   int       `msgpack:"retryCount"`
    Status       string    `msgpack:"status"` // "pending", "completed", "failed"
}
```

### 6.3 Background deindex job

**`pkg/storage/badger_archive_cleanup.go`**:

```go
type ArchiveCleanupJob struct {
    engine    *BadgerEngine
    interval  time.Duration // default: 24h (nightly), configurable in seconds
}

func (j *ArchiveCleanupJob) Start(ctx context.Context)
func (j *ArchiveCleanupJob) RunOnce(ctx context.Context) (deleted int, err error)
```

Process: scan `prefixArchiveWorkItem` for pending items → for each item, load its `IndexEntryCatalog` → perform batched blind deletes against the exact index keys → mark work item completed → delete completed work items.

No full index scans. Idempotent. Retry-safe (exponential backoff on failures).

### 6.4 Archived-bit fast path

Add an `Archived` bool field to the node and edge serialization format. On read, check this bit before any profile resolution. Cost: one byte check. This is the Tier 2 fast path from the design.

### Phase 6 acceptance gate

- Index-entry catalog tracks correct keys for all index types (property, composite, vector, fulltext, range).
- Archive work items are enqueued when an entity is marked archived.
- Background cleanup deletes exact index keys without scanning.
- Cleanup is idempotent — running twice doesn't error.
- Archived entities are skipped in read paths even before deindex completes.
- Cleanup interval is configurable.

### Phase 6 test files

- `pkg/storage/badger_index_catalog_test.go` — catalog CRUD, correctness
- `pkg/storage/badger_archive_work_test.go` — work item lifecycle
- `pkg/storage/badger_archive_cleanup_test.go` — end-to-end cleanup, idempotency, retry
- `pkg/storage/badger_archive_cleanup_bench_test.go` — throughput

---

## Phase 7: Migration and Legacy Removal

**Goal:** Remove all hardcoded tier assumptions, migrate existing data, and cut over.

**Depends on:** All previous phases.

### 7.1 Remove legacy types

- Delete `decay.Tier` enum (`TierEpisodic`, `TierSemantic`, `TierProcedural`) from `pkg/decay/decay.go`.
- Delete `decay.Manager` — replaced by `retention.Scorer`.
- Remove `Node.DecayScore`, `Node.LastAccessed`, `Node.AccessCount` from `pkg/storage/types.go`.
- Remove `inference.EdgeDecay` from `pkg/inference/edge_decay.go`.
- Remove tier-specific fields from `pkg/replication/codec.go`.
- Update all call sites that reference removed fields.

### 7.2 Data migration

On first startup with 1.1.0:

1. Scan all nodes: read `DecayScore`, `LastAccessed`, `AccessCount`.
2. For each node with non-zero access state: create an `AccessMetaEntry` with the migrated values.
3. Re-serialize nodes without the legacy fields.
4. Write migration marker to Badger to prevent re-running.

**`pkg/storage/migration_v1_1.go`**:

```go
func (b *BadgerEngine) MigrateToRetentionV1() error
```

### 7.3 CLI updates

- `nornicdb decay stats` → uses `Scorer` and `Resolver` instead of tier-based stats.
- `nornicdb decay recalculate` → uses `Scorer` to recompute, respects profiles.
- `nornicdb decay archive` → uses archive threshold from resolved profile, not CLI flag.
- Add `nornicdb retention show` as the new canonical introspection command.

### 7.4 Replication codec

- Stop sending `DecayScore` in replication.
- Send accessMeta entries as part of the replication stream.
- Send archived-bit changes.

### Phase 7 acceptance gate

- No reference to `TierEpisodic`, `TierSemantic`, or `TierProcedural` in any Go source file.
- No `DecayScore`, `LastAccessed`, or `AccessCount` on `Node` struct.
- Migration runs successfully on a database with existing tier-based data.
- CLI commands work with new resolver.
- Replication codec sends/receives accessMeta.
- All existing tests updated to use new types. Zero test failures.

### Phase 7 test files

- `pkg/storage/migration_v1_1_test.go` — migration correctness, idempotency
- `pkg/decay/` — package can be archived or deleted after migration
- Full regression suite re-run with `go test ./...`

---

## Phase 8: UI, Diagnostics, and Documentation

**Goal:** Surface the new system in the browser UI, admin endpoints, and user documentation.

**Depends on:** All previous phases.

### 8.1 UI additions

- Show effective decay profile and promotion policy on node/edge detail views.
- Show `decayScore`, `scoreFrom`, archived status, deindex status.
- Show accessMeta (access count, last accessed, traversal count).
- Show resolution trace (explain view).

### 8.2 Admin endpoints

- `GET /admin/retention/profiles` — list all profiles and bindings.
- `GET /admin/retention/policies` — list all promotion policies.
- `GET /admin/retention/resolve?entityId=X` — explain resolution for a specific entity.
- `GET /admin/retention/archive/status` — archive cleanup job status.

### 8.3 Documentation

- `docs/user-guides/retention-policies.md` — update existing doc (currently describes old tier system).
- `docs/user-guides/decay-profiles.md` — new: authoring decay profiles with Cypher DDL.
- `docs/user-guides/promotion-policies.md` — new: authoring promotion profiles and policies.
- `docs/user-guides/archival-deindex.md` — new: archival behavior, deindex cleanup.
- `docs/features/memory-decay.md` — rewrite to reference new system.
- `docs/operations/cli-commands.md` — update decay CLI section.

### Phase 8 acceptance gate

- UI shows decay/promotion metadata for any node or edge.
- Admin endpoints return correct data.
- All documentation reflects the new system.
- No documentation references old tier names.

---

## Cross-Cutting Concerns

### Concurrency

- `BindingTable` is read-locked during queries, write-locked only during DDL (rare).
- `AccessAccumulator` uses lock-free atomics on the hot path.
- Flush goroutine is the sole Badger writer for accessMeta keys.
- Archive cleanup job runs single-threaded with batched writes.

### Performance budget

| Operation | Target | Mechanism |
|-----------|--------|-----------|
| Visibility check (archived) | <10ns | One byte check |
| Visibility check (non-archived, non-decaying) | <50ns | Map lookup + NoDecay flag |
| Visibility check (decaying, threshold) | <100ns | Integer subtraction on UnixNano |
| Full score computation (lazy) | <500ns | One `math.Exp` + multiply/floor/cap |
| AccessMeta increment (hot path) | <30ns | Atomic int64 increment |
| AccessMeta flush (cold path) | <5ms per batch | Batched Badger write |
| Binding table rebuild (DDL) | <10ms | Map construction, rare |

### Error handling

- Missing profile → neutral score `1.0`, not error.
- Missing accessMeta → empty map from `policy()`, not error.
- Unparsable CUSTOM scoreFromProperty → warn, fall back to CREATED.
- DDL validation errors → return Cypher error to client.

### Testing strategy

Total new test files: ~25. All tests are deterministic (no `time.Now()` in assertions — inject frozen timestamps). Benchmarks cover all hot-path operations.

Run order: `go test ./pkg/retention/... ./pkg/storage/... ./pkg/cypher/... ./pkg/search/...`

---

## Implementation Sequence Summary

```
Phase 1: Schema Objects and Profile Model        (foundation — no runtime changes)
  ↓
Phase 2: Shared Resolver and Scorer               (pure computation — no I/O)
  ↓                                               
Phase 3: AccessMeta Index                         (I/O layer — parallel with Phase 2)
  ↓
Phase 4: Runtime Integration                      (wires scoring into read paths)
  ↓
Phase 5: Cypher Functions                         (user-facing query surface)
  ↓
Phase 6: Archival and Deindex Infrastructure      (background cleanup)
  ↓
Phase 7: Migration and Legacy Removal             (cut over)
  ↓
Phase 8: UI, Diagnostics, and Documentation       (polish)
```

Phases 2 and 3 can be developed in parallel. All other phases are sequential.

---

## Risk Register

| Risk | Mitigation |
|------|------------|
| Serialization format change breaks existing databases | Migration script in Phase 7; explicit version marker in Badger |
| Scoring overhead on hot read path | Three-tier fast path: archived-bit → no-decay flag → integer threshold comparison |
| AccessMeta flush lag causes stale WHEN predicate evaluation | Read-through path: `persisted + buffered delta` via atomic load |
| Multi-label node resolution complexity | Compile-time expansion + most-specific-match rule; conflict = non-decaying + diagnostic warning |
| DDL parsing complexity | Keyword-scanning pattern (proven in constraint_contracts.go), not regex |
| Replication codec breaking change | Version bump to 1.1.0 signals incompatible break |
