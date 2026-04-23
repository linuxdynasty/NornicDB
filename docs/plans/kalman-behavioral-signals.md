# Kalman-Filtered Behavioral Signals for Anti-Sycophancy

**Status:** Designed — integrated into [knowledge-layer-persistence-plan.md](./knowledge-layer-persistence-plan.md) §4.2, §6.6, §7.4  
**Date:** April 23, 2026  
**Algorithm Source:** [kalman.md](./kalman.md) (imu-f flight controller Kalman filter)

---

## The Problem

When an LLM evaluates a memory — "how relevant is this?" or "how confident are you in this fact?" — the answer can hallucinate. A mediocre memory might get scored 0.99 confidence because the model was being agreeable (sycophancy), or because it got confused, or because it was having a bad day. If we store that score directly, a single hallucinated spike can promote garbage to the canonical knowledge layer.

This is the exact same problem a flight controller has with noisy gyroscope readings. One bad reading shouldn't flip the drone upside down.

---

## The Solution: Two-Layer Defense

### Layer 1: Session-Aware Gating (Removes Bias)

LLM-driven access patterns aren't just noisy — they can be systematically biased. If the same agent accesses the same memory 50 times in one session (a sycophancy loop), those aren't 50 independent observations. They're one observation repeated.

The engine provides **query context variables** from request headers:

| Header              | Variable    | Purpose                    |
| ------------------- | ----------- | -------------------------- |
| `X-Query-Session`   | `$_session` | Same-session deduplication |
| `X-Query-Agent`     | `$_agent`   | Per-agent tracking         |
| `X-Query-Tenant`    | `$_tenant`  | Multi-tenant isolation     |
| Any `X-Query-<Key>` | `$_<key>`   | User-defined context       |

These are generic — the engine doesn't decide what matters. You do. Use them in `ON ACCESS` blocks to gate which accesses count as real signal.

### Layer 2: Kalman Smoothing (Removes Variance)

After gating removes bias, the Kalman filter handles the remaining noise. It's the same fast scalar Kalman filter from our [flight controller implementation](./kalman.md) — velocity-based prediction, gain-limited measurement updates, and adaptive noise estimation.

The filter:

- **Trusts the trend** — if a score has been slowly rising from 0.5 to 0.7, it predicts it'll keep rising. A sudden jump to 0.99 violates the trend and gets dampened.
- **Adjusts skepticism automatically** — when measurements are consistent, it becomes more responsive. When they're all over the place, it becomes more skeptical.
- **Never rejects** — every measurement is accepted. But the stored value is always the filter's _best estimate_, not the raw reading.

---

## The Three Modes

| Syntax                          | What It Does                                                                             | When To Use                                               |
| ------------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| `WITH KALMAN`                   | Auto mode — R (measurement noise) self-adjusts based on how noisy the signal actually is | Default. Works well for most behavioral metrics.          |
| `WITH KALMAN{q: 0.05, r: 50.0}` | Manual mode — you set both Q (process noise) and R (measurement noise)                   | When you know your signal characteristics exactly.        |
| `WITH KALMAN{q: 0.05}`          | Hybrid — you set Q, R self-adjusts                                                       | When you want to tune responsiveness but let noise adapt. |

**Q (process noise):** How much the _true_ value can change between measurements. Lower = more stable signal, smoother output. Higher = signal changes fast, filter is more responsive.

**R (measurement noise):** How much you distrust each measurement. Higher = more skeptical, smoother output. In auto mode, this is calculated from the actual variance of recent measurements.

---

## What Gets Kalman-Filtered (And What Doesn't)

**Kalman filter these** — derived behavioral metrics where the input is noisy:

- Confidence scores from LLM evaluation
- Relevance assessments
- Cross-session access rates
- Agreement ratios
- Anything where the observed value fluctuates around a true state

**Don't Kalman filter these** — raw monotonic counters and timestamps:

- `accessCount` (always goes up by 1 — no noise)
- `lastAccessedAt` (a timestamp — no noise)
- `traversalCount` (a counter — no noise)

---

## Query Examples

### Example 1: Basic Promotion with Kalman-Smoothed Confidence

An LLM evaluates each memory episode during retrieval and provides a confidence score. We Kalman-smooth it to prevent hallucinated spikes from promoting bad memories.

```cypher
CREATE PROMOTION POLICY memory_quality
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    -- Raw counter: no Kalman (it's exact)
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()

    -- LLM-evaluated confidence: noisy, gets Kalman-smoothed
    -- $evaluatedConfidence comes from the LLM's assessment during retrieval
    WITH KALMAN{q: 0.05, r: 50.0} SET n.confidenceScore = $evaluatedConfidence
  }

  -- Only promote if BOTH: accessed enough AND consistently high confidence
  WHEN n.accessCount >= 5 AND n.confidenceScore >= 0.8
    APPLY PROFILE 'high_confidence'

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced'
}
```

**What happens when the LLM hallucinates:**

| Access | Raw Confidence | Kalman-Filtered | What Happened                                                                        |
| ------ | -------------- | --------------- | ------------------------------------------------------------------------------------ |
| 1      | 0.60           | 0.60            | First measurement, filter initializes                                                |
| 2      | 0.62           | 0.61            | Consistent, filter tracks smoothly                                                   |
| 3      | 0.58           | 0.60            | Small dip, barely changes estimate                                                   |
| 4      | 0.61           | 0.60            | Stable around 0.60                                                                   |
| 5      | **0.99**       | **0.63**        | **Hallucination! Filter barely moves — K is low because the signal has been stable** |
| 6      | 0.59           | 0.62            | Back to normal, filter recovers                                                      |
| 7      | 0.61           | 0.62            | Tracking the real value again                                                        |

Without the Kalman filter, access 5 would have set `confidenceScore = 0.99` and triggered `high_confidence` promotion on a mediocre memory. With it, the estimate barely budged.

### Example 2: Cross-Session Access Rate with Session Gating

Track how many _different sessions_ access a memory (not how many times in one session). Gate by session first, then Kalman-smooth the rate.

```cypher
CREATE PROMOTION POLICY cross_session_reinforcement
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1

    -- Only count new sessions. Same session = same observation.
    -- $_session comes from the X-Query-Session HTTP header.
    WITH KALMAN SET n.crossSessionRate =
      CASE WHEN n._lastSessionId <> $_session
        THEN coalesce(n.crossSessionRate, 0) + 1
        ELSE n.crossSessionRate
      END
    SET n._lastSessionId = $_session
  }

  -- Cross-session reinforcement: multiple independent observers agree
  WHEN n.crossSessionRate >= 5
    APPLY PROFILE 'multi_session_reinforced'
}
```

**What happens with a sycophancy loop:**

| Access | Session   | Raw Would Be | Gated + Kalman | Why                                       |
| ------ | --------- | ------------ | -------------- | ----------------------------------------- |
| 1      | session-A | 1            | 1.0            | New session, counts                       |
| 2      | session-A | 2            | 1.0            | Same session, **gated out**               |
| 3      | session-A | 3            | 1.0            | Same session, **gated out**               |
| ...    | session-A | ...          | 1.0            | Same session, **gated out**               |
| 50     | session-A | 50           | 1.0            | Same session, **gated out**               |
| 51     | session-B | 51           | 1.8            | New session! Kalman smooths the increment |
| 52     | session-C | 52           | 2.5            | New session! Genuine reinforcement        |

Without gating, 50 accesses from one session would have counted as 50 reinforcements. With gating, it's one. The Kalman filter then smooths the genuine cross-session signal.

### Example 3: Edge-Level Signal Smoothing

Track co-access edge strength with cross-session support. Only strong edges that are traversed across multiple independent sessions get promoted.

```cypher
CREATE PROMOTION POLICY coaccess_strength
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  ON ACCESS {
    SET r.traversalCount = coalesce(r.traversalCount, 0) + 1
    SET r.lastTraversedAt = timestamp()

    -- Cross-session support: only new sessions count
    WITH KALMAN SET r.crossSessionSupport =
      CASE WHEN r._lastSessionId <> $_session
        THEN coalesce(r.crossSessionSupport, 0) + 1
        ELSE r.crossSessionSupport
      END
    SET r._lastSessionId = $_session
  }

  WHEN r.traversalCount >= 10
    APPLY PROFILE 'reinforced_edge'

  WHEN r.traversalCount >= 50 AND r.crossSessionSupport >= 3
    APPLY PROFILE 'canonical_edge'
}
```

### Example 4: Inspecting Kalman State for Diagnostics

The `policy()` Cypher function exposes the full Kalman state for any entity. This is useful for debugging, tuning filter parameters, and understanding how a behavioral signal has evolved.

```cypher
-- See the filtered confidence score and raw filter state
MATCH (m:MemoryEpisode {id: $id})
RETURN m.id,
       policy(m).kalmanFilters.confidenceScore.filteredValue AS smoothedConfidence,
       policy(m).kalmanFilters.confidenceScore.filter.k AS kalmanGain,
       policy(m).kalmanFilters.confidenceScore.filter.p AS estimateUncertainty,
       policy(m).kalmanFilters.confidenceScore.filter.n AS observationCount,
       policy(m).kalmanFilters.confidenceScore.variance.v AS currentVariance
```

```cypher
-- Find memories where the Kalman filter is currently very skeptical
-- (high gain = trusting measurements; low gain = skeptical)
MATCH (m:MemoryEpisode)
WHERE policy(m).kalmanFilters.confidenceScore.filter.k < 0.1
RETURN m.id,
       policy(m).kalmanFilters.confidenceScore.filteredValue AS confidence,
       policy(m).kalmanFilters.confidenceScore.filter.k AS gain,
       "highly skeptical — noisy signal" AS status
```

---

## Architectural Boundaries

### What NornicDB Provides (Engine Layer)

- **Decay profiles** — half-life, scoring functions, visibility thresholds
- **Promotion policies** — `WHEN` predicates, `ON ACCESS` blocks, `APPLY PROFILE`
- **`WITH KALMAN`** — behavioral signal smoothing on `ON ACCESS SET` expressions
- **Query context passthrough** — `X-Query-*` headers → `$_<key>` variables for gating
- **`NO DECAY`** — truth-immune labels (always visible, score 1.0)

### What NornicDB Does NOT Provide (Application Layer)

- **Truth-promotion logic** — deciding when a `:MemoryEpisode` becomes a `:KnowledgeFact` is workload-specific. NornicDB tells you when a memory is a _candidate_ for consolidation (via `WHEN` predicates and promotion tiers). Your application decides whether to actually create the fact.
- **Consolidation pipelines** — creating `:KnowledgeFact` nodes, `:CONSOLIDATES_TO` edges, and deciding what constitutes "ground truth" belongs in your application. The Heimdall plugin (`pkg/heimdall/plugin.go`) serves as a reference implementation.
- **Relevance ranking across layers** — truth-immune labels (`NO DECAY`, score 1.0) must never outweigh behavioral content through promotion multipliers. Their relevance comes from semantic matching at retrieval time, not from access-pattern boosting.

---

## How It All Connects

```
Request comes in with headers:
  X-Query-Session: "session-abc"
  X-Query-Agent: "agent-007"
        │
        ▼
┌─────────────────────────────┐
│  Query Context Passthrough  │  Engine injects $_session, $_agent
│  (generic, any X-Query-*)   │  into evaluation context
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│  ON ACCESS Block Executes   │  Only for visible entities
│  (entity passed visibility) │  (suppressed entities skip this)
└──────────────┬──────────────┘
               │
     ┌─────────┴─────────┐
     │                    │
     ▼                    ▼
┌──────────┐    ┌──────────────────┐
│ Plain SET │    │ WITH KALMAN SET  │
│           │    │                  │
│ Counters  │    │ Session gating:  │
│ Timestamps│    │ CASE WHEN        │
│           │    │ $_session <>     │
│ No filter │    │ stored session   │
│           │    │                  │
│ accessCnt │    │ Then: Kalman     │
│ lastAccAt │    │ filter step      │
│           │    │                  │
│ Stored    │    │ Stored value =   │
│ directly  │    │ filter estimate  │
└──────────┘    └──────────────────┘
                         │
                         ▼
              ┌──────────────────┐
              │ KalmanFilters on │
              │ AccessMetaEntry  │
              │                  │
              │ .filteredValue   │ ← WHEN predicates see this
              │ .filter (state)  │ ← policy() exposes for diagnostics
              │ .variance (auto) │ ← adaptive R calculation
              └──────────────────┘
                         │
                         ▼
              ┌──────────────────┐
              │ WHEN Predicates  │  Use filtered values for
              │ Evaluate         │  promotion decisions
              │                  │
              │ n.confidenceScore│  ← Kalman-smoothed
              │ >= 0.8           │
              └──────────────────┘
                         │
                         ▼
              ┌──────────────────┐
              │ APPLY PROFILE    │  Promotion tier applied
              │ 'high_confidence'│  based on stable signal
              └──────────────────┘
```

---

## Summary

| Concern                         | Mechanism                                      | Layer                               |
| ------------------------------- | ---------------------------------------------- | ----------------------------------- |
| Noisy LLM evaluations           | `WITH KALMAN` smoother                         | Engine — ON ACCESS                  |
| Same-session sycophancy loops   | `$_session` gating in CASE expressions         | Engine — query context passthrough  |
| Raw counter accuracy            | Plain `SET` (no filter)                        | Engine — ON ACCESS                  |
| Truth vs behavioral separation  | `NO DECAY` + no promotion for canonical labels | Engine — decay profile + policy     |
| When to promote to ground truth | Application-specific consolidation logic       | Application — e.g., Heimdall plugin |
| Kalman state inspection         | `policy(n).kalmanFilters.<key>`                | Engine — Cypher function            |

The Kalman filter is a tool, not a decision-maker. It stabilizes the signal. Your policies decide what to do with it. Your application decides what becomes truth.
