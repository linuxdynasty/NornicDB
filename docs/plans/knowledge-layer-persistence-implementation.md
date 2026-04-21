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

| Component                | Location                                   | Description                                                                      |
| ------------------------ | ------------------------------------------ | -------------------------------------------------------------------------------- |
| `decay.Tier` enum        | `pkg/decay/decay.go:77-128`                | Hardcoded `TierEpisodic`, `TierSemantic`, `TierProcedural` with fixed half-lives |
| `decay.Manager`          | `pkg/decay/decay.go`                       | Tier-based scoring, reinforcement, archival logic                                |
| `Node.DecayScore`        | `pkg/storage/types.go:197`                 | Stored float64 on the Node struct                                                |
| `Node.LastAccessed`      | `pkg/storage/types.go:198`                 | Stored time on the Node struct                                                   |
| `Node.AccessCount`       | `pkg/storage/types.go:199`                 | Stored int64 on the Node struct                                                  |
| `inference.EdgeDecay`    | `pkg/inference/edge_decay.go`              | Separate edge decay with confidence-based model                                  |
| Replication codec fields | `pkg/replication/codec.go`                 | Sends `DecayScore` in replication                                                |
| CLI decay commands       | `cmd/` (decay stats, recalculate, archive) | Tier-aware CLI                                                                   |

### What exists today (to be preserved or extended)

| Component                 | Location                                     | Description                                                                                         |
| ------------------------- | -------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Cypher Kalman functions   | `pkg/cypher/kalman_functions.go`             | Kalman-filter scoring — internal Cypher functions, unrelated to decay policy system, retained as-is |
| `SchemaManager`           | `pkg/storage/schema.go:72-91`                | Constraint/index catalog — new subsystem maps go here                                               |
| `SchemaDefinition`        | `pkg/storage/schema_persistence.go:14-27`    | Persisted schema — new definition sections added here                                               |
| Badger prefix keys        | `pkg/storage/badger.go:21-36`                | `0x01`–`0x10` allocated — new prefixes start at `0x11`                                              |
| msgpack serialization     | `pkg/storage/badger_serialization.go`        | Already supports msgpack — accessMeta uses this                                                     |
| MVCC version system       | `pkg/storage/badger_mvcc.go`                 | Version resolution, snapshot reads — scorer receives snapshot timestamp                             |
| Feature flags             | `pkg/config/feature_flags.go`                | Global enable/disable — decay and promotion get flags here                                          |
| `constraint_contracts.go` | `pkg/storage/constraint_contracts.go`        | Pattern for keyword scanning DDL — decay/promotion DDL follows this                                 |
| Canonical graph ledger    | `docs/user-guides/canonical-graph-ledger.md` | Knowledge-layer persistence for facts — referenced by design                                        |

---

## New Badger Key Prefixes

```go
// pkg/storage/badger.go — append to existing prefix block
prefixAccessMeta        = byte(0x11) // accessmeta:entityID -> msgpack(AccessMetaEntry)
prefixIndexEntryCatalog = byte(0x12) // idxcat:entityID -> msgpack(IndexEntryCatalog)
prefixArchiveWorkItem   = byte(0x13) // archwork:workItemID -> msgpack(ArchiveWorkItem)
prefixDecayProfile      = byte(0x14) // decayprofile:name -> msgpack(DecayProfileDef)
prefixPromotionProfile  = byte(0x15) // promoprofile:name -> msgpack(PromotionProfileDef)
prefixPromotionPolicy   = byte(0x16) // promopolicy:name -> msgpack(PromotionPolicyDef)
prefixIndexTombstone    = byte(0x17) // idxtomb:<original-index-key> -> msgpack(IndexTombstoneEntry)
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

### 1.5 Feature flag gating

The entire policy-driven retention subsystem is gated behind the **existing** `config.Memory.DecayEnabled` flag (`NORNICDB_MEMORY_DECAY_ENABLED` env var, default `false`). No new feature flags are introduced for the subsystem itself.

**Rationale:** The policy-driven system is the evolution of the existing memory-decay subsystem — it replaces the same code paths, serves the same purpose, and targets the same operator audience. Introducing separate flags would create a confusing matrix where the legacy decay and the new retention system could be independently toggled into conflicting states. A single flag keeps the contract clean: when `DecayEnabled` is `true`, the retention subsystem is active; when `false`, no decay scoring, archival, or promotion logic executes.

**Gating behavior:**

| Flag state                            | Legacy decay (pre-1.1.0)      | Policy-driven retention (1.1.0+)                                                                                                                         |
| ------------------------------------- | ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DecayEnabled = true`, pre-migration  | Legacy `decay.Manager` active | Inactive (migration not run)                                                                                                                             |
| `DecayEnabled = true`, post-migration | Removed                       | `retention.Scorer` active, profiles/policies evaluated                                                                                                   |
| `DecayEnabled = false`                | Inactive                      | Inactive — DDL succeeds (profiles/policies are persisted schema), but runtime scoring, archival, `AccessAccumulator`, and flush goroutine are all no-ops |

**Gate points in code:**

1. **`pkg/storage/badger_mvcc.go`** (Phase 4) — The visibility check skips profile resolution and returns the entity unchanged when `!config.Memory.DecayEnabled`.
2. **`pkg/retention/access_flusher.go`** (Phase 3) — `AccessFlusher.Start()` exits immediately when `!config.Memory.DecayEnabled`. `AccessAccumulator.IncrementAccess()` is a no-op (check flag before atomic increment).
3. **`pkg/retention/scorer.go`** (Phase 2) — `Scorer.ScoreNode()` and `Scorer.ScoreEdge()` return a neutral `RetentionResolution{FinalScore: 1.0, NoDecay: true}` when `!config.Memory.DecayEnabled`.
4. **`pkg/storage/badger_archive_cleanup.go`** (Phase 6) — `ArchiveCleanupJob.Start()` exits immediately when `!config.Memory.DecayEnabled`.
5. **`pkg/cypher/retention_functions.go`** (Phase 5) — `decayScore()` returns `1.0`, `decay()` returns `{score: 1.0, applies: false, reason: "decay subsystem disabled"}`, `policy()` returns empty metadata. These do not error — they degrade gracefully.
6. **`pkg/cypher/retention_ddl.go`** (Phase 1) — DDL statements (`CREATE DECAY PROFILE`, etc.) always succeed regardless of flag state. Profiles and policies are schema objects and must persist across flag toggles. The flag only gates runtime evaluation.

**`pkg/config/config.go`** — Change the default and env var parsing:

The existing default is `config.Memory.DecayEnabled = true` at line 1515. Change to `false`:

```go
// DefaultConfig() — change default:
config.Memory.DecayEnabled = false
```

The existing env var parsing (line 1984) only handles the disable case. Update to handle explicit enable:

```go
// Before:
if getEnv("NORNICDB_MEMORY_DECAY_ENABLED", "") == "false" {
    config.Memory.DecayEnabled = false
}

// After:
if v := getEnv("NORNICDB_MEMORY_DECAY_ENABLED", ""); v == "true" || v == "1" {
    config.Memory.DecayEnabled = true
} else if v == "false" || v == "0" {
    config.Memory.DecayEnabled = false
}
```

The YAML key `memory.decay_enabled` continues to work unchanged — it already sets the bool directly.

**`pkg/config/feature_flags.go`** — No changes. The existing `NORNICDB_EDGE_DECAY_ENABLED` / `IsEdgeDecayEnabled()` flag continues to gate `inference.EdgeDecay` independently. During Phase 7 migration, `inference.EdgeDecay` is removed and `NORNICDB_EDGE_DECAY_ENABLED` becomes a no-op.

### Phase 1 acceptance gate

- All decay/promotion types compile and have msgpack round-trip tests.
- DDL parsing produces correct command structs for all 14 statement types.
- Schema persistence round-trips all new definition sections.
- Validation rejects illegal targets, duplicates, and indexed-property rules.
- `config.Memory.DecayEnabled = false` (default) gates all runtime behavior; DDL persists regardless. Existing behavior unchanged when flag is off.
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

#### 2.3.1 Multi-label tie-breaking with deterministic Order

**Problem:** If Node A has labels `:Recipe:Tested`, and separate decay profiles exist for `:Recipe` and `:Tested` but no combined profile for `:Recipe:Tested`, the most-specific-match rule cannot decide — both bindings have equal specificity (1 label each). The original plan falls back to "non-decaying + diagnostic warning," but this is operationally dangerous: a tie silently disables decay for the entity, and if the warning fires on every sub-millisecond traversal it will flood the log.

**Solution:** Add an `Order` field to `DecayProfileBinding` (already present on `DecayProfilePropertyRule`). When two bindings have equal label-set specificity, the binding with the **lower Order value** wins deterministically. If `Order` values are also equal (both defaulted to 0), the builder fails the rebuild with an explicit schema-level error at DDL time — not at query time.

**Binding builder changes:**

```go
// DecayProfileBinding — add Order field:
type DecayProfileBinding struct {
    // ... existing fields ...
    Order int `msgpack:"order"` // deterministic precedence for equal-specificity conflicts
}
```

**Resolution rules (updated):**

1. Most specific match wins (most labels in the target set).
2. On equal specificity: lower `Order` wins.
3. On equal specificity AND equal `Order`: `BindingBuilder.Rebuild()` returns an error. The `SchemaManager` rejects the DDL that introduced the conflict. The database state does not change.

**Compile-time conflict detection:** The builder checks for conflicts at rebuild time, not at query time. This means:
- `CREATE DECAY PROFILE foo FOR (n:Recipe)` succeeds.
- `CREATE DECAY PROFILE bar FOR (n:Tested)` succeeds only if no node in the database currently has both `:Recipe` and `:Tested`, **or** if `foo` and `bar` have different `Order` values.
- If a node gains a new label after profile creation (e.g., `SET n:Tested` on a `:Recipe` node), the conflict is detected on the next binding table rebuild (next DDL change or server restart) and logged as a diagnostic warning. The node is treated using the lower-`Order` binding until the operator resolves the conflict.

**Log behavior:** Runtime label conflicts (detected at query time when no DDL rebuild has happened) log a warning on every occurrence. No rate limiting — conflicts are errors in the schema configuration and must be visible to the operator immediately. If the warning volume is high, that is a signal to fix the conflicting profiles, not to suppress the warnings.

### Phase 2 acceptance gate

- Resolver returns correct `RetentionResolution` for all label/edge-type/property combinations.
- Scorer produces correct scores for all decay functions × score-from modes.
- Multi-label resolution picks the most specific match.
- Multi-label tie at equal specificity is broken by `Order` field (lower wins).
- Multi-label tie with equal specificity AND equal `Order` is rejected at DDL time with a clear error message.
- Runtime label conflict (label added after profile creation) logs a warning on every occurrence, resolves to lower-`Order` binding.
- Wildcard fallback works correctly.
- No-decay targets resolve to `1.0`.
- Targets with no matching profile resolve to `1.0` (neutral).
- `thresholdAgeNanos` fast-path matches `math.Exp` path for all test cases.
- Scorer returns neutral `RetentionResolution{FinalScore: 1.0, NoDecay: true}` when `!config.Memory.DecayEnabled`.

### Phase 2 test files

- `pkg/retention/resolver_test.go` — resolution cascade for all target types
- `pkg/retention/scorer_test.go` — scoring correctness, all functions, all score-from modes
- `pkg/retention/binding_builder_test.go` — compiled table correctness, multi-label conflict detection, Order-based tie-breaking
- `pkg/retention/binding_builder_conflict_test.go` — equal-specificity with equal Order rejected at DDL time; equal-specificity with different Order resolves deterministically; runtime label conflict logs warning on every occurrence
- `pkg/retention/scorer_bench_test.go` — benchmark: integer fast-path vs float path

---

## Phase 3: AccessMeta Index

**Goal:** Implement the accessMeta index, the sharded counter ring for hot-path accumulation, and the flush goroutine. After this phase, ON ACCESS mutations can be accumulated and persisted.

**Depends on:** Phase 1 (types). Independent of Phase 2.

### 3.1 Per-P sharded counter ring

**`pkg/retention/access_accumulator.go`**:

**Problem with entity-sharded design:** In graph datasets, power-law distributions are the norm. A super-node (e.g., a canonical root concept that everything connects to) hashes to a single shard. Hundreds of concurrent goroutines contending on the same `atomic.Int64` causes CPU cache-line bouncing and degrades the hot path from <30ns to >200ns under contention.

**Solution:** Shard by processor, not by entity. Each logical P (as in `runtime.GOMAXPROCS`) gets its own accumulator shard. Goroutines write to the shard of the P they are currently scheduled on, guaranteeing zero cross-core contention regardless of graph topology. The flush goroutine aggregates across all P-local shards before writing to Badger.

```go
type entityDelta struct {
    accessCount     int64
    traversalCount  int64
    lastAccessedAt  int64 // UnixNano — max-wins
    lastTraversedAt int64 // UnixNano — max-wins
    overflow        map[string]int64
}

type pLocalShard struct {
    mu     sync.Mutex
    deltas map[string]*entityDelta // key: entityID
    _pad   [128 - 64]byte          // cache-line padding to prevent false sharing
}

type AccessAccumulator struct {
    shards []pLocalShard // length = runtime.GOMAXPROCS(0), resized on GOMAXPROCS change
}

// currentShard returns the shard for the current P using sync.Pool's P-local affinity.
//
// Mechanism: A sync.Pool is initialized with a New func that returns the next shard index
// (atomic counter). Pool.Get() returns the shard index pinned to the current P. The goroutine
// does its mutation, then Pool.Put() returns the index. Because sync.Pool internally uses
// per-P local storage, repeated Get/Put from the same P returns the same shard index —
// giving us P-local affinity without go:linkname into runtime internals.
//
// Cost: ~15ns for the Get/Put pair, well within the <30ns budget. This is stable across
// Go versions — sync.Pool's P-local behavior is a documented performance property, not
// an implementation detail.
func (a *AccessAccumulator) currentShard() *pLocalShard

func (a *AccessAccumulator) IncrementAccess(entityID string)
func (a *AccessAccumulator) IncrementTraversal(entityID string)
func (a *AccessAccumulator) IncrementCustom(entityID string, key string, delta int64)
func (a *AccessAccumulator) SetTimestamp(entityID string, key string, ts int64)

// ReadThrough returns persisted + buffered delta for WHEN predicate evaluation.
// Scans ALL P-local shards to sum deltas for the requested entityID.
// Cost: O(GOMAXPROCS) mutex acquisitions — acceptable because WHEN predicate evaluation
// is not on the sub-millisecond visibility-check hot path; it runs only during
// full score computation (lazy scoring) after the entity has survived visibility.
func (a *AccessAccumulator) ReadThrough(entityID string, key string, persisted int64) int64
```

**Why `sync.Pool`, not `hash(goroutineID)`:** Goroutine IDs are not exposed by the Go runtime without unsafe hacks, and goroutine-local storage does not exist in Go. `sync.Pool` already solves P-local affinity internally — its `Get`/`Put` methods use per-P local storage under the hood. We exploit this: the pool's `New` func assigns shard indices via an atomic counter. A goroutine calls `pool.Get()` to retrieve the shard index for its current P, does the mutation, then `pool.Put()` returns it. Because `sync.Pool` reuses per-P local slots, repeated `Get`/`Put` from the same P returns the same shard index — giving us P-local affinity through a stable, public API. No `go:linkname`, no dependency on runtime internals, no breakage across Go versions.

**Flush aggregation:** The flush goroutine iterates all P-local shards, locks each briefly, swaps out the `deltas` map with a fresh empty map, and merges all per-entity deltas into a single aggregated map before writing to Badger. Counts are summed; timestamps take the maximum value.

#### 3.1.1 Snapshot isolation and ReadThrough — eventually-consistent by design

**Problem:** If `ReadThrough` returns the absolute latest buffered delta (which includes increments from transactions that committed after the current query's snapshot), then a `WHEN` predicate in a promotion policy evaluated at snapshot T_50 would observe access counts from T_60. This breaks MVCC snapshot isolation — the same query run twice at T_50 could see different access counts if a flush occurs between executions.

**Design decision:** Access counts in the accumulator are **explicitly eventually-consistent** and are **not bound by MVCC snapshot isolation**. This is the correct choice for the following reasons:

1. **Access counts are not graph state.** They are operational metadata — telemetry about how the graph has been used. They are analogous to query statistics or cache hit counters. Binding them to snapshot isolation would require versioning every atomic increment, which defeats the purpose of the lock-free accumulator.

2. **WHEN predicates are policy thresholds, not ACID reads.** A predicate like `WHEN n.accessCount > 10` is asking "has this node been accessed enough to warrant promotion?" The answer is operationally useful whether the count is 11 or 12. It is not a transactional invariant that must be repeatable.

3. **The alternative is prohibitively expensive.** Snapshot-consistent access counts would require either: (a) versioning every counter increment as an MVCC record (destroying the <30ns hot path), or (b) snapshotting the entire accumulator state at each transaction start (O(n) memory per concurrent reader).

**Documented contract:**

```go
// ReadThrough returns the best-effort current value: persisted + sum(P-local buffered deltas).
// This value is eventually consistent with a bounded staleness of one flush interval.
// It is NOT bound by MVCC snapshot isolation. WHEN predicates that evaluate access
// counts see a real-time-ish view, not a snapshot-consistent view.
//
// This is intentional: access counts are operational metadata, not graph state.
// Promotion policy evaluation is eventually-consistent by design.
```

**Test requirement:** Add a test that demonstrates and asserts this behavior — a WHEN predicate sees an access count that was incremented *after* the query's snapshot timestamp. This is the expected behavior, not a bug.

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

- P-local sharded accumulator correctly accumulates increments under concurrent goroutines — including concurrent access to the **same** entityID from different goroutines (super-node scenario).
- Super-node benchmark: 128 goroutines incrementing the same entityID concurrently shows zero cache-line contention (throughput scales linearly with GOMAXPROCS, not inversely).
- Flush goroutine aggregates all P-local shards and persists accumulated values to Badger.
- Read-through scans all P-local shards and returns `persisted + sum(buffered deltas)`.
- accessMeta survives restart (msgpack round-trip).
- Deletion cascades from node/edge deletion.
- accessMeta retained on archival.
- All accumulator operations are no-ops when `!config.Memory.DecayEnabled`.

### Phase 3 test files

- `pkg/retention/access_accumulator_test.go` — concurrent increment correctness, P-local shard isolation
- `pkg/retention/access_accumulator_supernode_test.go` — super-node contention: 128 goroutines × 1 entityID, assert no throughput degradation vs. 128 goroutines × 128 entityIDs
- `pkg/retention/access_flusher_test.go` — flush persistence, batching, cross-shard aggregation
- `pkg/storage/badger_access_meta_test.go` — CRUD, serialization round-trip, deletion cascade
- `pkg/retention/access_accumulator_bench_test.go` — throughput under contention, uniform vs. power-law distributions

---

## Phase 4: Runtime Integration — Scoring-Before-Visibility

**Goal:** Wire the scorer into all read paths. Nodes and edges are scored before they become visible to queries. This is the core behavioral change.

**Depends on:** Phase 2 (scorer), Phase 3 (accessMeta).

### 4.1 MVCC read-path integration

**`pkg/storage/badger_mvcc.go`** — Modify node/edge read paths:

After MVCC version resolution, before returning the entity to the caller:

0. If `!config.Memory.DecayEnabled`: skip all steps below, return entity unchanged.
1. Check archived bit (Tier 2 fast path — one byte check, skip if archived and no `reveal()`).
2. Look up compiled binding for the entity's labels/edge-type (Tier 1 — single map lookup).
3. If binding exists and `NoDecay` is false:
   a. Check `now - scoreFrom > thresholdAgeNanos` (Tier 3 — integer comparison, no `math.Exp`).
   b. If below threshold and no `reveal()` in query context: suppress entity (return nil/skip).
   c. If surviving visibility: attach `RetentionResolution` to entity context for lazy score computation.
4. If `reveal()` is active for this binding: always materialize, still compute score for `decayScore()`/`decay()`.

The `scoringTime` passed to the scorer is `txn.ReadTimestamp` (already available in the MVCC read path as the snapshot timestamp).

#### 4.1.1 Legacy field fallback (pre-migration safety)

**Problem:** Phase 4 wires the scorer into the read path. Phase 7 migrates legacy data. If Phase 4 is deployed but Phase 7 migration has not yet executed, the scorer will look up `AccessMetaEntry` for existing nodes, find `nil`, and treat them as having zero access history. This causes massive unintended decay of pre-existing nodes — nodes that had `AccessCount: 500` and `LastAccessed: yesterday` in the legacy fields would suddenly compute as if they were never accessed.

**Solution:** When `AccessMetaEntry` is `nil` for a node, the scorer falls back to the legacy fields on the `Node` struct (`DecayScore`, `LastAccessed`, `AccessCount`) to compute the score. This fallback is removed in Phase 7 after migration completes.

```go
// In Scorer.ScoreNode(), after accessMeta lookup:
func (s *Scorer) resolveAccessMeta(node *Node, accessMeta *AccessMetaEntry) *AccessMetaEntry {
    if accessMeta != nil {
        return accessMeta
    }
    // Pre-migration fallback: synthesize AccessMetaEntry from legacy Node fields.
    // These fields exist on the Node struct until Phase 7 removes them.
    if node.AccessCount > 0 || !node.LastAccessed.IsZero() {
        return &AccessMetaEntry{
            TargetID:    string(node.ID),
            TargetScope: ScopeNode,
            Fixed: AccessMetaFixedFields{
                AccessCount:    node.AccessCount,
                LastAccessedAt: node.LastAccessed.UnixNano(),
            },
        }
    }
    return nil // genuinely new node with no access history
}
```

**Lifecycle:**
- Phase 4 deploys with fallback active.
- Phase 7 migration runs: converts all legacy fields to `AccessMetaEntry` records, then removes the legacy fields from the `Node` struct.
- After Phase 7, the fallback code path is dead (the legacy fields no longer exist on the struct). Remove the fallback function in the same commit that removes the legacy fields.

**Test requirement:** Add `pkg/retention/legacy_fallback_test.go` — test that a node with legacy fields but no `AccessMetaEntry` scores identically to the same node after its legacy fields have been migrated to an `AccessMetaEntry`.

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

- `pkg/storage/badger_mvcc_decay_test.go` — visibility suppression, reveal bypass, property hiding, feature-flag-off pass-through
- `pkg/cypher/reveal_test.go` — reveal() plan compilation, per-variable bypass
- `pkg/search/decay_filter_test.go` — chunked retrieval, LIMIT satisfaction
- `pkg/retention/integration_test.go` — end-to-end: DDL → score → visibility
- `pkg/retention/legacy_fallback_test.go` — node with legacy fields but no AccessMetaEntry scores identically to migrated node; fallback is no-op when AccessMetaEntry exists

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
    TargetID           string      `msgpack:"targetId"`
    TargetScope        string      `msgpack:"targetScope"`                // "NODE" or "EDGE"
    IndexKeys          [][]byte    `msgpack:"indexKeys"`                  // exact Badger keys written
    DeindexedAtVersion MVCCVersion `msgpack:"deindexedAt,omitempty"`      // set by cleanup job (§6.3.1)
}
```

Key format: `[prefixIndexEntryCatalog][entityID bytes]`

This is maintained on every index write. When a node is re-indexed (properties changed), the catalog is updated with the new key set. The `DeindexedAtVersion` field is set by the archive cleanup job after writing index tombstones (§6.3.1) and prevents re-processing on subsequent cleanup runs.

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
func (j *ArchiveCleanupJob) RunOnce(ctx context.Context) (deindexed int, err error)
```

Process: scan `prefixArchiveWorkItem` for pending items → for each item, load its `IndexEntryCatalog` → perform batched deindex writes against the exact index keys → mark work item completed → delete completed work items.

No full index scans. Idempotent. Retry-safe (exponential backoff on failures).

#### 6.3.1 MVCC-safe deindexing — no physical key wipes

**Critical constraint:** NornicDB uses bitemporal MVCC. A node archived at version V_100 must still be resolvable by time-travel queries running `AS OF V_50` that traverse through secondary indexes. Physically deleting index keys would corrupt historical state.

The "deindex" operation therefore does **not** perform a physical Badger key delete. Instead, it writes a **companion tombstone key** under a dedicated prefix that marks the original index entry as dead as of a specific MVCC version.

**Dedicated tombstone prefix:**

```go
// pkg/storage/badger.go — append to existing prefix block:
prefixIndexTombstone = byte(0x17) // idxtomb:<original-index-key> -> msgpack(IndexTombstoneEntry)
```

**Index tombstone format:**

```go
// IndexTombstoneEntry is written as a companion key alongside the original index key.
// The original index key is NOT modified or deleted.
type IndexTombstoneEntry struct {
    ArchivedAtVersion MVCCVersion `msgpack:"archivedAt"`
}
```

**Tombstone key construction:** For an original index key `K` (e.g., `[0x03][labelName][nodeID]`), the tombstone key is `[prefixIndexTombstone][K]`. The original prefix byte is preserved inside the tombstone key so that the read path can reconstruct the original key if needed.

**Deindex write:** For each key `K` in the `IndexEntryCatalog`, the cleanup job writes `[prefixIndexTombstone][K] → msgpack(IndexTombstoneEntry)` in a single batched Badger transaction. The original key `K` is untouched.

**Read-path behavior:**

- **Current-time queries (`MVCCReadLatest`):** When scanning a secondary index and finding a candidate key `K`, probe for `[prefixIndexTombstone][K]`. If the tombstone key exists, skip the entry. Cost: one additional Badger point lookup per candidate. This is acceptable because the tombstone prefix is small and Badger's bloom filters will reject the lookup in <50ns for the common case (no tombstone exists).
- **Time-travel queries (`MVCCReadSnapshot` at version V):** If the tombstone key exists and `V < ArchivedAtVersion`, the tombstone is invisible — the index entry is still live at that snapshot. Return the entry normally. If `V >= ArchivedAtVersion`, skip the entry.
- **Optimization: batch tombstone prefetch.** During label-index scans that iterate many entries, the read path can do a single prefix scan on `[prefixIndexTombstone][original-prefix][labelName]` and build a local `map[string]MVCCVersion` of tombstoned IDs for that label. Subsequent checks are map lookups instead of point reads. This is O(tombstones) not O(candidates), and is efficient because the archived population is typically small relative to the live population.

**Why a dedicated prefix, not a value overwrite:** The current secondary indexes (`0x03`–`0x06`) write `[]byte{}` as the value. Overwriting the value with a tombstone struct would couple the tombstone format to every index value format. If any future index writes non-empty values (e.g., composite indexes with score metadata), a sentinel-byte scheme creates a fragile format dependency. A dedicated prefix key is fully decoupled — the original key and value are never modified, and tombstone lifecycle is independent.

**Physical reclamation:** Both the tombstone key and the original index key are eligible for physical deletion only when the `ArchivedAtVersion` is older than the engine's `RetentionPolicy.TTL` and no active snapshot readers hold a version at or before the tombstone's version. This is handled by the existing MVCC lifecycle pruning infrastructure (`MVCCLifecycleController`), not by the archive cleanup job. The cleanup job's sole responsibility is writing the tombstone key. Reclamation is a separate lifecycle concern.

**IndexEntryCatalog update:** After writing tombstones, the catalog entry is updated with `DeindexedAtVersion` to prevent re-processing:

```go
type IndexEntryCatalog struct {
    TargetID           string      `msgpack:"targetId"`
    TargetScope        string      `msgpack:"targetScope"`
    IndexKeys          [][]byte    `msgpack:"indexKeys"`
    DeindexedAtVersion MVCCVersion `msgpack:"deindexedAt,omitempty"` // set by cleanup job
}
```

### 6.4 Archived-bit fast path

Add an `Archived` bool field to the node and edge serialization format. On read, check this bit before any profile resolution. Cost: one byte check. This is the Tier 2 fast path from the design.

### Phase 6 acceptance gate

- Index-entry catalog tracks correct keys for all index types (property, composite, vector, fulltext, range).
- Archive work items are enqueued when an entity is marked archived.
- Background cleanup writes companion tombstone keys under `prefixIndexTombstone` (not physical deletes or value overwrites) for exact index keys without scanning.
- Cleanup is idempotent — running twice produces the same tombstones, no error.
- Archived entities are skipped in read paths even before deindex completes (archived-bit fast path).
- Time-travel queries at snapshots before the archive version still resolve archived entities through the index (tombstone is invisible at earlier versions).
- Time-travel queries at snapshots at or after the archive version skip the tombstoned index entries.
- Index tombstones are physically reclaimed only by MVCC lifecycle pruning, not by the archive cleanup job.
- Cleanup interval is configurable.

### Phase 6 test files

- `pkg/storage/badger_index_catalog_test.go` — catalog CRUD, correctness, DeindexedAtVersion prevents re-processing
- `pkg/storage/badger_archive_work_test.go` — work item lifecycle
- `pkg/storage/badger_archive_cleanup_test.go` — end-to-end cleanup, idempotency, retry
- `pkg/storage/badger_index_tombstone_test.go` — companion tombstone key written under `prefixIndexTombstone`; current-time query skips tombstoned entries; time-travel query at pre-archive snapshot returns entry; time-travel at post-archive snapshot skips entry; batch tombstone prefetch optimization during label-index scan
- `pkg/storage/badger_archive_cleanup_bench_test.go` — throughput

---

## Phase 7: Seamless On-Start Migration and Legacy Removal

**Goal:** Seamless rolling upgrade — existing databases are migrated in-place on first startup with 1.1.0. No manual steps, no downtime, no data loss. After migration, remove all hardcoded tier assumptions.

**Depends on:** All previous phases.

### 7.0 Migration analysis — what actually needs migrating

The migration surface is smaller than it appears because of how NornicDB serializes data:

**Does NOT need re-serialization:**
- **Node bytes in Badger** — `vmihailenco/msgpack/v5` encodes `Node` as field-name-keyed maps (no `msgpack:` struct tags, no `StructAsArray`). When the `Node` struct drops `DecayScore`, `LastAccessed`, and `AccessCount` fields, existing stored bytes are still decodable — msgpack silently ignores unknown keys during decode. No re-serialization pass is needed.
- **Edge bytes in Badger** — Same format, no decay fields to remove.
- **MVCC version records** — `mvccNodeRecord` wraps `*Node` via `msgpack.Marshal()`. Same field-name map behavior. Old version records with legacy fields decode cleanly into the new struct — the legacy keys are ignored.
- **Embeddings** — Unchanged.
- **Schema definitions** — New retention fields are `omitempty` and default to nil/empty on old data.

**Needs migration (access state extraction):**
- **Nodes with non-zero access state** — `DecayScore`, `LastAccessed`, `AccessCount` must be extracted from each node's stored bytes and written to a new `AccessMetaEntry` under `prefixAccessMeta`. This is the only data transformation.

**Needs new on-disk state:**
- **Schema version marker** — A version marker under `prefixMVCCMeta` to track which migrations have run.
- **`Archived` bit on nodes/edges** — New field, defaults to `false` on existing data. No migration needed — the zero value is correct (nothing is archived yet).

### 7.1 Schema version marker

**`pkg/storage/badger_helpers.go`** — Add a schema version key alongside the existing MVCC sequence key:

```go
// mvccSchemaVersionKey stores the on-disk schema version for migration gating.
// Uses prefixMVCCMeta (0x10) with sub-key 0x02 (0x01 is the MVCC sequence).
func mvccSchemaVersionKey() []byte {
    return []byte{prefixMVCCMeta, 0x02}
}
```

Schema versions:

| Version | Meaning |
|---------|---------|
| absent  | Pre-1.1.0 database (no schema version key exists) |
| 1       | 1.1.0 — accessMeta extraction complete |

### 7.2 On-start migration runner

**`pkg/storage/migration_runner.go`**:

The migration runner hooks into `BadgerEngine` initialization. It runs inline during `NewBadgerEngineWithOptions()`, after the database is opened but before any reads or writes from higher layers. This follows the same pattern as the existing serializer migration (`serializer_migration.go`) but is automatic — no CLI invocation needed.

```go
// RunOnStartMigrations performs all necessary schema migrations.
// Called once during engine initialization. Idempotent — checks the schema
// version marker before each migration and skips if already applied.
func (b *BadgerEngine) RunOnStartMigrations() error {
    currentVersion := b.readSchemaVersion() // returns 0 if key absent

    if currentVersion < 1 {
        if err := b.migrateV0ToV1(); err != nil {
            return fmt.Errorf("migration v0→v1 failed: %w", err)
        }
    }

    // Future migrations: if currentVersion < 2 { ... }
    return nil
}
```

**Hook point in `nornicdb.Open()`** — Insert after `NewBadgerEngineWithOptions()` returns at `db.go:836`, before WAL, lifecycle, or retention manager initialization:

```go
badgerEngine, err := storage.NewBadgerEngineWithOptions(badgerOpts)
if err != nil { ... }

// Seamless on-start migration — runs before any higher-layer initialization.
if err := badgerEngine.RunOnStartMigrations(); err != nil {
    badgerEngine.Close()
    return nil, fmt.Errorf("on-start migration failed: %w", err)
}
```

### 7.3 Migration v0 → v1: Access state extraction

**`pkg/storage/migration_v0_to_v1.go`**:

```go
func (b *BadgerEngine) migrateV0ToV1() error
```

**Process:**

1. **Scan primary node keys** (`prefixNode`). For each node:
   a. Deserialize the node using the existing `decodeValue()` path (which handles both gob and msgpack with header auto-detection).
   b. Check if the node has non-zero access state: `DecayScore != 0 || AccessCount != 0 || !LastAccessed.IsZero()`.
   c. If yes, construct an `AccessMetaEntry`:
      ```go
      entry := &retention.AccessMetaEntry{
          TargetID:    string(node.ID),
          TargetScope: retention.ScopeNode,
          Fixed: retention.AccessMetaFixedFields{
              AccessCount:    node.AccessCount,
              LastAccessedAt: node.LastAccessed.UnixNano(),
          },
          LastMutatedAt: time.Now().UnixNano(),
          MutationCount: 1, // migration counts as one mutation
      }
      ```
   d. Write the `AccessMetaEntry` to `[prefixAccessMeta][entityID]` via `PutAccessMeta()`.
   e. **Do NOT re-serialize the node.** The old `DecayScore`/`LastAccessed`/`AccessCount` keys remain in the stored bytes. They are harmlessly ignored on future decodes because the `Node` struct no longer has those fields. This eliminates the most expensive part of the migration (re-encoding every node) and avoids any risk of data corruption from a failed re-write.

2. **Batch writes.** Accumulate `AccessMetaEntry` writes in batched Badger transactions (batch size: 1000, matching the existing serializer migration pattern). Flush after each batch.

3. **Scan MVCC node versions** (`prefixMVCCNode`). MVCC version records embed the full `Node` struct. The same legacy fields exist in historical versions. **No migration needed** — the `mvccNodeRecord` deserializer will silently ignore the legacy keys. Historical access state is not extracted because it represents point-in-time snapshots, not cumulative counters.

4. **Write schema version marker.** After all batches are flushed, write `schemaVersion = 1` to the schema version key. This is the commit point — if the process crashes before this write, the migration re-runs on next startup (idempotent because `PutAccessMeta` is an upsert).

**Why no node re-serialization is needed:**

| Concern | Resolution |
|---------|-----------|
| Old bytes have `DecayScore` key in msgpack map | Ignored by `vmihailenco/msgpack/v5` — unknown map keys are silently skipped during decode into a struct without those fields |
| Old bytes have `LastAccessed` / `AccessCount` | Same — silently skipped |
| Old gob-encoded nodes | `decodeValue()` already handles gob → struct fallback. Gob also ignores extra fields when decoding into a struct that has dropped them. The `encodeValue()` path will re-encode as msgpack (with header) on next write, completing the gob→msgpack migration lazily |
| MVCC historical versions | Never re-written. Decoded correctly with legacy fields ignored. |
| Node size unchanged after migration | The legacy keys remain as dead bytes in the stored value. They are reclaimed naturally when the node is next written (any `SET` or `UPDATE` re-serializes without the dropped fields) or when Badger compacts the LSM tree |

**Migration cost:**
- **Read cost:** One full scan of `prefixNode` keys. No scan of `prefixMVCCNode`, `prefixEdge`, or any index.
- **Write cost:** One `AccessMetaEntry` write per node with non-zero access state + one schema version marker write.
- **Memory cost:** O(batch_size) — holds at most 1000 nodes in memory at a time.
- **Time estimate:** ~10ms per 1000 nodes (dominated by Badger read I/O). A database with 1M nodes migrates in ~10 seconds.

### 7.4 Legacy field handling during transition

The `Node` struct change happens in two steps to ensure zero-downtime:

**Step 1 (Phase 4 — deployed with migration):** Keep legacy fields on `Node` struct but stop using them in the scorer. The scorer reads `AccessMetaEntry` with fallback to legacy fields (§4.1.1). `mergeInternalProperties()` and `ExtractInternalProperties()` continue to read/write `_decayScore`, `_lastAccessed`, `_accessCount` for backward compatibility with any external tooling that reads Neo4j JSON exports.

**Step 2 (Phase 7 — after migration is deployed and confirmed stable):** Remove the legacy fields from the `Node` struct:

```go
// Remove from pkg/storage/types.go:
// DecayScore      float64              `json:"-"`    // REMOVED in 1.1.0
// LastAccessed    time.Time            `json:"-"`    // REMOVED in 1.1.0
// AccessCount     int64                `json:"-"`    // REMOVED in 1.1.0
```

Remove `_decayScore`, `_lastAccessed`, `_accessCount` from `mergeInternalProperties()` and `ExtractInternalProperties()`.

Remove the legacy fallback from `Scorer.resolveAccessMeta()` (§4.1.1) — it becomes dead code since all access state is now in `AccessMetaEntry`.

### 7.5 Remove legacy types

- Delete `decay.Tier` enum (`TierEpisodic`, `TierSemantic`, `TierProcedural`) from `pkg/decay/decay.go`.
- Delete `decay.Manager` — replaced by `retention.Scorer`.
- Remove `Node.DecayScore`, `Node.LastAccessed`, `Node.AccessCount` from `pkg/storage/types.go` (Step 2 above).
- Remove `inference.EdgeDecay` from `pkg/inference/edge_decay.go`.
- Remove tier-specific fields from `pkg/replication/codec.go`.
- Update all call sites that reference removed fields.

### 7.6 CLI updates

- `nornicdb decay stats` → uses `Scorer` and `Resolver` instead of tier-based stats.
- `nornicdb decay recalculate` → uses `Scorer` to recompute, respects profiles.
- `nornicdb decay archive` → uses archive threshold from resolved profile, not CLI flag.
- Add `nornicdb retention show` as the new canonical introspection command.
- Add `nornicdb migration status` → shows current schema version and migration history.

### 7.7 Replication codec

- Stop sending `DecayScore` in replication.
- Send accessMeta entries as part of the replication stream.
- Send archived-bit changes.

### Phase 7 acceptance gate

- `RunOnStartMigrations()` completes without error on a fresh database (no-op, writes version marker).
- `RunOnStartMigrations()` completes without error on a pre-1.1.0 database with existing tier-based data.
- Migration is idempotent — running twice produces the same result, no duplicate `AccessMetaEntry` records.
- Migration is crash-safe — if the process crashes mid-migration, the next startup re-runs from the beginning (schema version marker is written last).
- Nodes with `DecayScore=0, AccessCount=0, LastAccessed=zero` produce no `AccessMetaEntry` (no empty entries).
- Nodes with non-zero access state produce correct `AccessMetaEntry` with matching values.
- After migration, the scorer reads `AccessMetaEntry` for all nodes — the legacy fallback path (§4.1.1) is never exercised.
- Old node bytes with legacy keys decode correctly into the new `Node` struct (keys silently ignored).
- MVCC historical versions with legacy keys decode correctly (no re-serialization needed).
- Gob-encoded nodes from pre-msgpack databases migrate access state correctly. The gob→msgpack serialization conversion happens lazily on next node write (existing behavior from `serializer_migration.go` / `decodeValue()` fallback).
- No reference to `TierEpisodic`, `TierSemantic`, or `TierProcedural` in any Go source file (after Step 2).
- No `DecayScore`, `LastAccessed`, or `AccessCount` on `Node` struct (after Step 2).
- CLI commands work with new resolver.
- Replication codec sends/receives accessMeta.
- All existing tests updated to use new types. Zero test failures.

### Phase 7 test files

- `pkg/storage/migration_runner_test.go` — schema version read/write, runner skips already-applied migrations, runner applies pending migrations in order
- `pkg/storage/migration_v0_to_v1_test.go` — access state extraction correctness, idempotency, crash-safety (write version marker last), batch boundaries, nodes with zero access state skipped, gob-encoded nodes handled
- `pkg/storage/migration_v0_to_v1_bench_test.go` — throughput: 10K nodes, 100K nodes, 1M nodes
- `pkg/storage/migration_legacy_decode_test.go` — old msgpack bytes with `DecayScore`/`LastAccessed`/`AccessCount` keys decode into new `Node` struct with those fields absent (keys silently ignored); old gob bytes same; MVCC version records same
- `pkg/decay/` — package can be archived or deleted after Step 2
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
- `AccessAccumulator` uses P-local sharding — each goroutine writes to the shard of the P it is scheduled on, eliminating cross-core contention entirely. No `atomic.Int64` contention even under super-node access patterns.
- Each P-local shard is mutex-protected (not lock-free) but the mutex is P-local, so contention only occurs if multiple goroutines on the same P increment in the same scheduling quantum — effectively zero contention.
- Flush goroutine is the sole Badger writer for accessMeta keys. It briefly locks each P-local shard to swap out the delta map (lock held for ~50ns per shard — map pointer swap, not iteration).
- Archive cleanup job runs single-threaded with batched writes.

### Performance budget

| Operation                                     | Target         | Mechanism                           |
| --------------------------------------------- | -------------- | ----------------------------------- |
| Visibility check (archived)                   | <10ns          | One byte check                      |
| Visibility check (non-archived, non-decaying) | <50ns          | Map lookup + NoDecay flag           |
| Visibility check (decaying, threshold)        | <100ns         | Integer subtraction on UnixNano     |
| Full score computation (lazy)                 | <500ns         | One `math.Exp` + multiply/floor/cap |
| AccessMeta increment (hot path)               | <30ns          | P-local shard mutex + map write (zero cross-core contention) |
| AccessMeta flush (cold path)                  | <5ms per batch | Batched Badger write, aggregate across P-local shards        |
| Binding table rebuild (DDL)                   | <10ms          | Map construction, rare              |

### Error handling

- Missing profile → neutral score `1.0`, not error.
- Missing accessMeta → empty map from `policy()`, not error. If legacy `Node` fields exist (pre-migration), synthesize `AccessMetaEntry` from them (§4.1.1).
- Unparsable CUSTOM scoreFromProperty → warn, fall back to CREATED.
- DDL validation errors → return Cypher error to client.
- Multi-label conflict at equal specificity and equal Order → reject DDL at creation time with clear error message (§2.3.1).
- Runtime label conflict (label added post-DDL) → warning logged on every occurrence, resolve to lower-Order binding.
- `DecayEnabled = false` → all scoring functions degrade gracefully, no errors. `decayScore()` returns `1.0`, `decay()` returns `{applies: false}`, `policy()` returns empty metadata.

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
Phase 4: Runtime Integration                      (wires scoring into read paths, legacy fallback active)
  ↓
Phase 5: Cypher Functions                         (user-facing query surface)
  ↓
Phase 6: Archival and Deindex Infrastructure      (background cleanup)
  ↓
Phase 7: Seamless On-Start Migration              (access state extraction on first 1.1.0 startup,
         and Legacy Removal                        then legacy field removal after migration confirmed)
  ↓
Phase 8: UI, Diagnostics, and Documentation       (polish)
```

Phases 2 and 3 can be developed in parallel. All other phases are sequential.

Phase 7 is internally two steps: Step 1 (migration runner + access state extraction) ships with the 1.1.0 binary and runs automatically on startup. Step 2 (legacy field removal from `Node` struct) ships in a follow-up commit after migration is confirmed stable across deployments. The legacy fallback in §4.1.1 bridges the gap between Step 1 and Step 2.

---

## Risk Register

| Risk | Mitigation |
|------|------------|
| Serialization format change breaks existing databases | No re-serialization needed — msgpack field-name maps silently ignore removed keys; access state extracted to `AccessMetaEntry` on first startup; schema version marker gates idempotent migration (§7.0–7.3) |
| Scoring overhead on hot read path | Three-tier fast path: archived-bit → no-decay flag → integer threshold comparison; entire path skipped when `!config.Memory.DecayEnabled` |
| AccessMeta flush lag causes stale WHEN predicate evaluation | Read-through path: `persisted + sum(P-local buffered deltas)`. Explicitly eventually-consistent by design — access counts are operational metadata, not MVCC-snapshotted graph state (§3.1.1) |
| Super-node accumulator contention | P-local sharding eliminates cross-core contention regardless of graph topology; benchmarked with 128 goroutines × 1 entityID (§3.1) |
| Multi-label node resolution complexity | Compile-time expansion + most-specific-match rule; equal-specificity ties broken by `Order` field; equal-specificity + equal-Order rejected at DDL time; runtime conflicts warn on every occurrence (§2.3.1) |
| Archival deindex corrupts time-travel queries | Deindex writes companion tombstone keys under dedicated `prefixIndexTombstone` (`0x17`), not physical deletes or value overwrites; original index keys untouched; time-travel at pre-archive snapshots still resolves the entity; physical reclamation deferred to MVCC lifecycle pruning (§6.3.1) |
| Phase 4 deployed before Phase 7 migration | Scorer falls back to legacy `Node.DecayScore`/`LastAccessed`/`AccessCount` fields when `AccessMetaEntry` is nil; fallback removed when Phase 7 deletes legacy fields (§4.1.1) |
| Default flag change surprises existing deployments | `DecayEnabled` default changes `true` → `false` in 1.1.0; safe direction — no unexpected scoring on upgrade; operators opt in with `NORNICDB_MEMORY_DECAY_ENABLED=true` (§1.5) |
| DDL parsing complexity | Keyword-scanning pattern (proven in constraint_contracts.go), not regex |
| Replication codec breaking change | Version bump to 1.1.0 signals incompatible break |
