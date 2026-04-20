# Retention Policy Feature — Finish Plan (Revised)

## Status Summary

The `pkg/retention` package is a **complete, well-tested library** (1337 lines + 886 lines of tests).
It provides `Manager`, `Policy`, `LegalHold`, `ErasureRequest`, `ProcessRecord()`,
`ProcessErasure()`, `DefaultPolicies()`, `SavePolicies()`/`LoadPolicies()`, and delete/archive
callbacks. Everything compiles and passes tests.

**However, nothing connects it to the running server.**

The `pkg/config` package parses `compliance.retention_*` fields from YAML and env vars into
`ComplianceConfig` fields (`RetentionEnabled`, `RetentionPolicyDays`, `RetentionAutoDelete`,
`RetentionExemptRoles`). These fields are **loaded but never consumed** — no code in
`pkg/nornicdb` or `cmd/` references `Compliance.*` at all.

### What exists

| Component                                      | File                              | Lines     | Status                                |
| ---------------------------------------------- | --------------------------------- | --------- | ------------------------------------- |
| `retention.Manager`                            | `pkg/retention/retention.go`      | 349–541   | Complete                              |
| `retention.Policy` (validate, expiry, archive) | `pkg/retention/retention.go`      | 143–202   | Complete                              |
| `retention.LegalHold`                          | `pkg/retention/retention.go`      | 205–270   | Complete                              |
| `retention.ErasureRequest` (GDPR Art.17)       | `pkg/retention/retention.go`      | 272–326   | Complete                              |
| `retention.DefaultPolicies()` (7 policies)     | `pkg/retention/retention.go`      | 1235–1337 | Complete                              |
| `Manager.SavePolicies()`/`LoadPolicies()`      | `pkg/retention/retention.go`      | 1008–1053 | Complete                              |
| `Manager.ProcessRecord()`                      | `pkg/retention/retention.go`      | 800–834   | Complete                              |
| `Manager.ProcessErasure()`                     | `pkg/retention/retention.go`      | 931–985   | Complete                              |
| Unit tests                                     | `pkg/retention/retention_test.go` | 886 lines | Complete                              |
| Simple compliance config fields                | `pkg/config/config.go`            | 503–515   | Parsed, never consumed                |
| GDPR HTTP handlers                             | `pkg/server/server_gdpr.go`       | 1–109     | Working, but ignore retention.Manager |

### What does NOT exist

| Gap                                                           | Impact                                                      |
| ------------------------------------------------------------- | ----------------------------------------------------------- |
| No `retentionManager` field on `DB` struct                    | Manager is never instantiated                               |
| No code in `pkg/nornicdb` or `cmd/` references `Compliance.*` | Config fields are dead                                      |
| No config-to-policy bridge                                    | `RetentionPolicyDays` never becomes a `retention.Policy`    |
| No delete/archive callbacks wired to storage                  | `ProcessRecord()` no-ops                                    |
| No background sweep goroutine                                 | Expired records never found or processed                    |
| No HTTP endpoints for retention admin                         | Cannot manage policies/holds/erasures at runtime            |
| No extended `retention.policies[]` YAML schema                | Users cannot define per-category policies                   |
| No integration with GDPR delete handler                       | `handleGDPRDelete` ignores legal holds and erasure tracking |
| No audit logging of retention actions                         | Deletions/archives not audit-logged                         |
| No retention manager shutdown/persist                         | Policies lost on restart unless manually saved              |
| No label-based exclusion                                      | Cannot mark labels as exempt from retention                 |

---

## Structural Constraints (from codebase analysis)

These are important for anyone implementing the plan:

1. **`nornicdb.Config` is a type alias**: `type Config = nornicConfig.Config` (`db.go:305`).
   You cannot add fields to it in `pkg/nornicdb`. All config additions go in
   `pkg/config/config.go` on the `Config` struct or its sub-structs.

2. **No `auditLogger` on `DB` struct**: The DB struct has no audit reference. The server has
   `s.logAudit()` (via `pkg/server`), but the DB layer cannot audit-log directly. Retention
   audit logging must happen via callbacks that the server wires.

3. **`Compliance.*` fields are completely unwired**: Zero references to `Compliance` exist in
   `pkg/nornicdb` or `cmd/`. The first task is to thread these through.

4. **`startBackgroundTask(fn func()) bool`** (`db.go:1674`): This is the established pattern
   for DB-owned goroutines. Uses `bgWg` for shutdown coordination. The function handles
   `bgWg.Add(1)` and `defer bgWg.Done()` internally — callers must NOT touch `bgWg` directly.

5. **`closeInternal()`** (`db.go:1705`): Calls `bgWg.Wait()` at line 1725. Policy persistence
   must happen **after** `bgWg.Wait()` (after sweep goroutine has stopped) to avoid concurrent
   access during save.

6. **Route registration**: `buildRouter()` (`server_router.go:20`) calls individual
   `registerXRoutes(mux)` methods. Add `s.registerRetentionRoutes(mux)` as a peer call.

7. **Storage streaming**: `storage.StreamNodesWithFallback(ctx, engine, chunkSize, fn)` is the
   iteration primitive (`storage/types.go:1142+`). Signature:
   `func StreamNodesWithFallback(ctx context.Context, engine Engine, chunkSize int, fn NodeVisitor) error`

8. **Search index removal**: The actual signature is:

   ```go
   func (db *DB) removeNodeFromSearchIndexes(ctx context.Context, dbName string, storageEngine storage.Engine, id storage.NodeID) error
   ```

   Located at `search_services.go:569`. Requires `ctx`, `dbName`, and `storageEngine` — not
   just a node ID. The sweep must pass all three.

9. **`DeleteNode` cascades edges**: `BadgerEngine.DeleteNode()` (`badger_nodes.go:568`) calls
   `deleteNodeInTxn` which deletes the node and all connected edges in the same transaction.
   No separate edge cleanup is needed.

10. **`DataRecord` has no `Labels` field**: The `retention.DataRecord` struct only has `ID`,
    `SubjectID`, `Category`, `CreatedAt`, `LastAccessedAt`, `Metadata`. Label-based exclusion
    cannot be implemented inside the retention manager — it must happen in the sweep loop
    before calling `ShouldDelete`.

---

## Neo4j-Like Developer Ergonomics — Design Principles

NornicDB should feel like Neo4j to developers using retention. That means:

1. **Labels are first-class citizens for retention**. Developers think in labels, not abstract
   categories. If you label a node `AuditLog`, that should be enough to control its retention
   without also setting a `data_category` property.

2. **`excluded_labels` is a simple, top-level config**. No YAML nesting gymnastics. If a label
   is excluded, that node is never deleted by retention — period. This mirrors how Neo4j
   constraints work: you declare them on labels, and they apply globally.

3. **Explicit beats implicit**. If a node has an excluded label AND an expired policy matches
   its category, the exclusion wins. Developers should never be surprised by a deletion.

4. **Precedence is clear and documented**:
   - Legal hold → always wins, nothing is deleted
   - Excluded label → node is retained indefinitely
   - Per-category policy → checked against `CreatedAt`
   - Default policy → fallback when no category matches
   - No policy found → node is retained (safe default)

---

## Phase 1: Wire Manager into DB Lifecycle

**Goal**: Create `retention.Manager` during `Open()`, store on DB struct, expose to server.

### 1.1 Add `retentionManager` to DB struct

**File**: `pkg/nornicdb/db.go` — after `lifecycleManager` (line ~437)

```go
import "github.com/orneryd/nornicdb/pkg/retention"

// In DB struct, after lifecycleManager field:
retentionManager *retention.Manager
```

### 1.2 Thread compliance config into Open()

**File**: `pkg/nornicdb/db.go` — inside `Open()`, after lifecycle manager setup (line ~892)

The config is already available as `config` (which is `*nornicConfig.Config`), so
`config.Compliance` is accessible. No additional config struct is needed.

```go
// Initialize retention manager if enabled
if config.Compliance.RetentionEnabled {
    rm := retention.NewManager()

    // Load persisted policies if file exists
    policiesPath := filepath.Join(dataDir, "retention-policies.json")
    if _, err := os.Stat(policiesPath); err == nil {
        if loadErr := rm.LoadPolicies(policiesPath); loadErr != nil {
            log.Printf("⚠️  Failed to load retention policies: %v", loadErr)
        }
    }

    // If no policies loaded, create a default from simple config
    if len(rm.ListPolicies()) == 0 && config.Compliance.RetentionPolicyDays > 0 {
        defaultPolicy := &retention.Policy{
            ID:       "config-default",
            Name:     "Default Retention Policy (from config)",
            Category: retention.CategoryUser,
            RetentionPeriod: retention.RetentionPeriod{
                Duration: time.Duration(config.Compliance.RetentionPolicyDays) * 24 * time.Hour,
            },
            ArchiveBeforeDelete: !config.Compliance.RetentionAutoDelete,
            ArchivePath:         filepath.Join(dataDir, "archive"),
            Active:              true,
        }
        if err := rm.AddPolicy(defaultPolicy); err != nil {
            log.Printf("⚠️  Failed to add default retention policy: %v", err)
        }
    }

    db.retentionManager = rm
    log.Printf("📋 Retention manager enabled (%d policies loaded)", len(rm.ListPolicies()))
}
```

**Seam**: Insert immediately after the `if config.Database.MVCCLifecycleEnabled { ... }` block
(line ~892) and before the WAL initialization block (line ~894).

### 1.3 Wire delete/archive callbacks to storage

**File**: `pkg/nornicdb/db.go` — immediately after manager creation in the block above

```go
rm.SetDeleteCallback(func(record *retention.DataRecord) error {
    // DeleteNode cascades edges automatically (badger_nodes.go:568)
    if err := db.storage.DeleteNode(storage.NodeID(record.ID)); err != nil {
        return err
    }
    if db.onRetentionAction != nil {
        db.onRetentionAction("RETENTION_DELETE", record.ID, string(record.Category))
    }
    return nil
})
rm.SetArchiveCallback(func(record *retention.DataRecord, archivePath string) error {
    node, err := db.storage.GetNode(storage.NodeID(record.ID))
    if err != nil {
        return err
    }
    data, err := json.Marshal(node)
    if err != nil {
        return err
    }
    if err := os.MkdirAll(archivePath, 0755); err != nil {
        return err
    }
    archiveFile := filepath.Join(archivePath, record.ID+".json")
    if err := os.WriteFile(archiveFile, data, 0644); err != nil {
        return err
    }
    if db.onRetentionAction != nil {
        db.onRetentionAction("RETENTION_ARCHIVE", record.ID, string(record.Category))
    }
    return nil
})
```

### 1.4 Expose manager via getter

**File**: `pkg/nornicdb/db.go`

```go
// GetRetentionManager returns the retention manager (nil if disabled).
func (db *DB) GetRetentionManager() *retention.Manager {
    return db.retentionManager
}
```

No lock needed — `retentionManager` is set once in `Open()` and never reassigned.

### 1.5 Persist policies on shutdown

**File**: `pkg/nornicdb/db.go` — inside `closeInternal()`, **after** `bgWg.Wait()` (line ~1725)
and before the search index persistence block (line ~1730).

**IMPORTANT**: This must be after `bgWg.Wait()`, not before. The sweep goroutine is tracked
by `bgWg` via `startBackgroundTask`. If we save policies before `bgWg.Wait()`, the sweep
could still be running and mutating the manager concurrently.

```go
// Persist retention policies after sweep goroutine has stopped
if db.retentionManager != nil && db.config != nil {
    policiesPath := filepath.Join(db.config.Database.DataDir, "retention-policies.json")
    if err := db.retentionManager.SavePolicies(policiesPath); err != nil {
        log.Printf("⚠️  Failed to save retention policies: %v", err)
    }
}
```

**Seam**: Insert after `db.bgWg.Wait()` (line 1725) and before the search index persist block
(line ~1730).

---

## Phase 2: Background Retention Sweep

**Goal**: Periodically scan storage for expired records, process them via the manager. Default is every 24 hours expressed in seconds in configuration.
The sweep must be low-allocation, cancellation-aware, and budget-limited.

### 2.1 New file: `pkg/nornicdb/db_retention.go`

```go
package nornicdb

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/orneryd/nornicdb/pkg/retention"
    "github.com/orneryd/nornicdb/pkg/storage"
)

const (
    defaultSweepInterval   = 1 * time.Hour
    defaultMaxSweepRecords = 50000 // Max records processed per sweep iteration
)

// startRetentionSweep launches a background goroutine that periodically
// scans for and processes expired data records according to retention policies.
func (db *DB) startRetentionSweep(ctx context.Context) {
    if db.retentionManager == nil {
        return
    }

    interval := defaultSweepInterval
    if db.config != nil && db.config.Retention.SweepInterval != "" {
        if d, err := time.ParseDuration(db.config.Retention.SweepInterval); err == nil && d > 0 {
            interval = d
        }
    }

    db.startBackgroundTask(func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()

        log.Printf("🔄 Retention sweep started (interval: %s)", interval)

        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                db.runRetentionSweep(ctx)
            }
        }
    })
}

// runRetentionSweep scans nodes and processes expired ones.
// It is budget-limited: at most defaultMaxSweepRecords per invocation.
func (db *DB) runRetentionSweep(ctx context.Context) {
    rm := db.retentionManager
    if rm == nil {
        return
    }

    // Build the excluded labels set from config (once per sweep, not per node)
    excludedLabels := db.retentionExcludedLabels()

    processed := 0
    deleted := 0
    skippedExcluded := 0
    sweepStart := time.Now()
    dbName := db.defaultDatabaseName()

    err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(node *storage.Node) error {
        // Budget check — stop after processing too many records in one sweep
        if processed >= defaultMaxSweepRecords {
            return fmt.Errorf("sweep budget exhausted (%d records)", defaultMaxSweepRecords)
        }

        // Cancellation check
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }

        // Label exclusion — if ANY label on this node is in the excluded set,
        // skip it entirely. This is the neo4j-like "label overrides everything" behavior.
        if hasExcludedLabel(node, excludedLabels) {
            skippedExcluded++
            processed++
            return nil
        }

        record := nodeToDataRecord(node)
        shouldDelete, _ := rm.ShouldDelete(record)
        if shouldDelete {
            if err := rm.ProcessRecord(ctx, record); err != nil {
                log.Printf("⚠️  Retention: failed to process %s: %v", node.ID, err)
            } else {
                deleted++
                // Remove from search indexes — use actual signature
                if idxErr := db.removeNodeFromSearchIndexes(ctx, dbName, db.storage, node.ID); idxErr != nil && !db.shouldIgnoreSearchIndexingError(idxErr) {
                    log.Printf("⚠️  Retention: failed to remove %s from search indexes: %v", node.ID, idxErr)
                }
            }
        }
        processed++
        return nil
    })

    sweepDuration := time.Since(sweepStart)

    // Log budget exhaustion as info, not error (it's expected for large datasets)
    if err != nil && ctx.Err() == nil {
        log.Printf("📋 Retention sweep paused: %v (will resume next interval)", err)
    }

    if deleted > 0 || skippedExcluded > 0 {
        log.Printf("📋 Retention sweep: processed=%d, deleted=%d, excluded=%d, duration=%s",
            processed, deleted, skippedExcluded, sweepDuration.Round(time.Millisecond))
    }
}

// retentionExcludedLabels returns the set of labels that should be excluded
// from retention processing. Thread-safe: reads config which is immutable after Open().
func (db *DB) retentionExcludedLabels() map[string]struct{} {
    if db.config == nil {
        return nil
    }
    labels := db.config.Retention.ExcludedLabels
    if len(labels) == 0 {
        return nil
    }
    set := make(map[string]struct{}, len(labels))
    for _, l := range labels {
        set[l] = struct{}{}
    }
    return set
}

// hasExcludedLabel returns true if any of the node's labels are in the excluded set.
func hasExcludedLabel(node *storage.Node, excluded map[string]struct{}) bool {
    if len(excluded) == 0 {
        return false
    }
    for _, label := range node.Labels {
        if _, ok := excluded[label]; ok {
            return true
        }
    }
    return false
}

// nodeToDataRecord maps a storage.Node to a retention.DataRecord.
func nodeToDataRecord(node *storage.Node) *retention.DataRecord {
    record := &retention.DataRecord{
        ID:        string(node.ID),
        CreatedAt: node.CreatedAt,
        Category:  inferCategory(node),
        Metadata:  make(map[string]string),
    }

    // Extract subject ID from common properties
    for _, prop := range []string{"owner_id", "created_by", "user_id", "author"} {
        if v, ok := node.Properties[prop]; ok {
            record.SubjectID = fmt.Sprintf("%v", v)
            break
        }
    }

    if node.UpdatedAt.After(node.CreatedAt) {
        record.LastAccessedAt = node.UpdatedAt
    }

    return record
}

// inferCategory maps node labels to retention data categories.
// This provides the neo4j-like ergonomic: label your node "PHI" and the PHI
// retention policy applies automatically.
func inferCategory(node *storage.Node) retention.DataCategory {
    for _, label := range node.Labels {
        switch label {
        case "AuditLog", "Audit":
            return retention.CategoryAudit
        case "PHI", "HealthRecord":
            return retention.CategoryPHI
        case "PII", "PersonalData":
            return retention.CategoryPII
        case "Financial", "Transaction":
            return retention.CategoryFinancial
        case "Analytics", "Metric", "Telemetry":
            return retention.CategoryAnalytics
        case "System", "Config", "Schema":
            return retention.CategorySystem
        case "Archive":
            return retention.CategoryArchive
        case "Backup":
            return retention.CategoryBackup
        }
    }

    // Check for explicit data_category property
    if cat, ok := node.Properties["data_category"]; ok {
        return retention.DataCategory(fmt.Sprintf("%v", cat))
    }

    return retention.CategoryUser
}
```

### 2.2 Start sweep in Open()

**File**: `pkg/nornicdb/db.go` — inside the `startBackgroundTask` block that begins at line 1207,
after lifecycle manager start (line ~1230)

```go
// Start retention sweep
if db.retentionManager != nil {
    db.startRetentionSweep(db.buildCtx)
}
```

**Seam**: Insert after the `if db.lifecycleManager != nil { ... }` block (line ~1223–1230)
and before the "Collect all database names" comment (line ~1232).

### 2.3 Sweep operational guarantees

The sweep implementation provides these guarantees:

| Guarantee                        | Implementation                                                            |
| -------------------------------- | ------------------------------------------------------------------------- |
| Background only                  | Runs inside `startBackgroundTask`, never blocks request path              |
| Chunked iteration                | `StreamNodesWithFallback` with `chunkSize=1000`                           |
| Per-iteration cancellation       | `select { case <-ctx.Done() }` before each node                           |
| Budget-limited per sweep         | `defaultMaxSweepRecords = 50000` — stops and resumes next interval        |
| No front-door latency regression | All retention work is async; delete callbacks use storage directly        |
| Edge cascade handled             | `storage.DeleteNode` cascades edges internally (`badger_nodes.go:568`)    |
| Search index cleanup             | Synchronous via `removeNodeFromSearchIndexes` — uses real 4-arg signature |
| Low allocation                   | No intermediate slices; streaming iteration with early-exit on budget     |

---

## Phase 3: Extended Config — Label Exclusions and Per-Category Policies

**Goal**: Let users define label exclusions and full `retention.Policy` objects in `config.yaml`.

### 3.1 Add retention section to config

**File**: `pkg/config/config.go` — add new struct and field to `Config`

```go
// RetentionPolicyConfig defines a single retention policy in YAML config.
type RetentionPolicyConfig struct {
    ID                   string   `yaml:"id"`
    Name                 string   `yaml:"name"`
    Category             string   `yaml:"category"`
    RetentionDays        int      `yaml:"retention_days"`
    Indefinite           bool     `yaml:"indefinite"`
    ArchiveBeforeDelete  bool     `yaml:"archive_before_delete"`
    ArchivePath          string   `yaml:"archive_path"`
    ComplianceFrameworks []string `yaml:"compliance_frameworks"`
    Description          string   `yaml:"description"`
    Active               *bool    `yaml:"active"` // nil = true
}

// RetentionConfig holds the extended retention configuration.
// This is the neo4j-like developer-facing config for data lifecycle management.
type RetentionConfig struct {
    // SweepInterval controls how often the retention sweep runs (e.g. "1h", "30m").
    // Default: "1h"
    SweepInterval string `yaml:"sweep_interval"`

    // ExcludedLabels lists node labels that are exempt from ALL retention policies.
    // If a node has ANY of these labels, it is retained indefinitely regardless of
    // category or policy. This is the primary developer-facing retention control —
    // think of it like a Neo4j constraint: declare it on the label, and it applies.
    //
    // Example: ["AuditLog", "System", "LegalHold", "Schema"]
    ExcludedLabels []string `yaml:"excluded_labels"`

    // PoliciesFile is an optional path to a policies JSON file for persistence.
    PoliciesFile string `yaml:"policies_file"`

    // DefaultPolicies loads the built-in HIPAA/GDPR/SOX policies on startup.
    DefaultPolicies bool `yaml:"default_policies"`

    // MaxSweepRecords limits how many records are processed per sweep iteration.
    // Prevents long-running sweeps on large datasets. Default: 50000.
    MaxSweepRecords int `yaml:"max_sweep_records"`

    // Policies defines per-category retention policies inline in YAML.
    Policies []RetentionPolicyConfig `yaml:"policies"`
}
```

Add `Retention RetentionConfig` to the `Config` struct (alongside `Compliance`):

```go
type Config struct {
    // ... existing fields ...
    Compliance ComplianceConfig

    // Retention settings (extended, label-aware)
    Retention RetentionConfig

    // ... rest ...
}
```

### 3.2 Add env var support

New env vars in `LoadFromEnv()`:

| Env Var                                | Field                       | Example                                         |
| -------------------------------------- | --------------------------- | ----------------------------------------------- |
| `NORNICDB_RETENTION_SWEEP_INTERVAL`    | `Retention.SweepInterval`   | `"30m"`                                         |
| `NORNICDB_RETENTION_EXCLUDED_LABELS`   | `Retention.ExcludedLabels`  | `"AuditLog,System,LegalHold"` (comma-separated) |
| `NORNICDB_RETENTION_POLICIES_FILE`     | `Retention.PoliciesFile`    | `"/etc/nornicdb/policies.json"`                 |
| `NORNICDB_RETENTION_DEFAULT_POLICIES`  | `Retention.DefaultPolicies` | `"true"`                                        |
| `NORNICDB_RETENTION_MAX_SWEEP_RECORDS` | `Retention.MaxSweepRecords` | `"100000"`                                      |

### 3.3 Wire into Open()

In the retention manager initialization (Phase 1.2), after loading persisted policies:

```go
// Load default policies if requested
if config.Retention.DefaultPolicies {
    for _, p := range retention.DefaultPolicies() {
        _ = rm.AddPolicy(p) // Ignore ErrAlreadyExists
    }
}

// Load policies defined in YAML config
for _, pc := range config.Retention.Policies {
    active := true
    if pc.Active != nil {
        active = *pc.Active
    }
    policy := &retention.Policy{
        ID:                   pc.ID,
        Name:                 pc.Name,
        Category:             retention.DataCategory(pc.Category),
        ArchiveBeforeDelete:  pc.ArchiveBeforeDelete,
        ArchivePath:          pc.ArchivePath,
        ComplianceFrameworks: pc.ComplianceFrameworks,
        Description:          pc.Description,
        Active:               active,
    }
    if pc.Indefinite {
        policy.RetentionPeriod = retention.RetentionPeriod{Indefinite: true}
    } else {
        policy.RetentionPeriod = retention.RetentionPeriod{
            Duration: time.Duration(pc.RetentionDays) * 24 * time.Hour,
        }
    }
    if err := rm.AddPolicy(policy); err != nil {
        log.Printf("⚠️  Failed to add retention policy %s: %v", pc.ID, err)
    }
}
```

### 3.4 Example config.yaml — the developer-facing experience

```yaml
# ─── Simple (existing, still works) ───
compliance:
  retention_enabled: true
  retention_policy_days: 365
  retention_auto_delete: false

# ─── Extended (new, neo4j-like) ───
retention:
  sweep_interval: "1h"
  max_sweep_records: 50000

  # Label exclusions — the primary developer-facing control.
  # Any node with one of these labels is NEVER deleted by retention.
  # This is the "set it and forget it" experience developers expect.
  excluded_labels:
    - AuditLog
    - System
    - LegalHold
    - Schema
    - Constraint

  # Load built-in HIPAA/GDPR/SOX policies
  default_policies: true

  # Per-category policies (override defaults)
  policies:
    - id: pii-1y
      name: "PII — 1 Year"
      category: PII
      retention_days: 365
      compliance_frameworks: ["GDPR"]

    - id: phi-6y
      name: "PHI — 6 Years"
      category: PHI
      retention_days: 2190
      archive_before_delete: true
      archive_path: "/archive/phi"
      compliance_frameworks: ["HIPAA"]

    - id: analytics-30d
      name: "Analytics — 30 Days"
      category: ANALYTICS
      retention_days: 30

    - id: system-indefinite
      name: "System — Never Delete"
      category: SYSTEM
      indefinite: true
```

### 3.5 Precedence rules (documented and enforced)

When evaluating whether a node should be deleted, the system checks in this order:

```
1. Legal hold?        → YES → RETAIN (never delete)
2. Excluded label?    → YES → RETAIN (never delete)
3. Category policy?   → FOUND → check IsExpired(createdAt)
                      → EXPIRED → DELETE (or archive-then-delete)
                      → NOT EXPIRED → RETAIN
4. No policy found?   → RETAIN (safe default — ShouldDelete returns false)
```

Step 1 is handled by `retention.Manager.ShouldDelete()` (already implemented).
Step 2 is handled by `hasExcludedLabel()` in the sweep loop (new).
Steps 3–4 are handled by `retention.Manager.ShouldDelete()` (already implemented).

**Source-of-truth rule**: When both `compliance.retention_policy_days` and `retention.policies[]`
are defined, the YAML policies take precedence. The simple config creates a fallback
`config-default` policy only when no other policies are loaded (Phase 1.2 handles this
via the `len(rm.ListPolicies()) == 0` check).

---

## Phase 4: HTTP Admin API for Retention

**Goal**: Expose retention management via HTTP endpoints (admin-only).

### 4.1 New file: `pkg/server/server_retention.go`

Endpoints:

```
GET    /admin/retention/policies            — List all policies
POST   /admin/retention/policies            — Add a policy
GET    /admin/retention/policies/{id}       — Get a policy
PUT    /admin/retention/policies/{id}       — Update a policy
DELETE /admin/retention/policies/{id}       — Delete a policy
POST   /admin/retention/policies/defaults   — Load default policies

GET    /admin/retention/holds               — List legal holds
POST   /admin/retention/holds               — Place a legal hold
DELETE /admin/retention/holds/{id}          — Release a legal hold

GET    /admin/retention/erasures            — List erasure requests
POST   /admin/retention/erasures            — Create erasure request
POST   /admin/retention/erasures/{id}/process — Process erasure

POST   /admin/retention/sweep               — Trigger immediate sweep
GET    /admin/retention/status              — Manager status/stats
```

All endpoints require `auth.PermAdmin`.

Handler access pattern: `s.db.GetRetentionManager()` returns `*retention.Manager` (or nil if
disabled). Handlers return 503 Service Unavailable when nil.

### 4.2 Register routes

**File**: `pkg/server/server_router.go` — in `buildRouter()` (line ~20)

Add `s.registerRetentionRoutes(mux)` call after `s.registerAdminRoutes(mux)` (line 28):

```go
s.registerAdminRoutes(mux)
s.registerRetentionRoutes(mux)  // ← new
s.registerGDPRRoutes(mux)
```

In `server_retention.go`:

```go
func (s *Server) registerRetentionRoutes(mux *http.ServeMux) {
    mux.HandleFunc("/admin/retention/policies", s.withAuth(s.handleRetentionPolicies, auth.PermAdmin))
    mux.HandleFunc("/admin/retention/policies/defaults", s.withAuth(s.handleRetentionPolicyDefaults, auth.PermAdmin))
    mux.HandleFunc("/admin/retention/holds", s.withAuth(s.handleRetentionHolds, auth.PermAdmin))
    mux.HandleFunc("/admin/retention/erasures", s.withAuth(s.handleRetentionErasures, auth.PermAdmin))
    mux.HandleFunc("/admin/retention/sweep", s.withAuth(s.handleRetentionSweep, auth.PermAdmin))
    mux.HandleFunc("/admin/retention/status", s.withAuth(s.handleRetentionStatus, auth.PermAdmin))
}
```

---

## Phase 5: Integrate with Existing GDPR Handlers

**Goal**: Make `handleGDPRDelete` check legal holds and track erasure requests.

### 5.1 Update handleGDPRDelete

**File**: `pkg/server/server_gdpr.go` — at the top of `handleGDPRDelete()`, after the
auth check (line ~83) and before the delete/anonymize call (line ~86)

```go
// Check legal holds and create erasure request if retention manager is active
if rm := s.db.GetRetentionManager(); rm != nil {
    if rm.IsUnderLegalHold(req.UserID, "") {
        s.writeError(w, http.StatusConflict,
            "user data is under legal hold and cannot be deleted", ErrConflict)
        return
    }

    erasureReq, err := rm.CreateErasureRequest(req.UserID, "")
    if err != nil && err != retention.ErrErasureInProgress {
        s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
        return
    }
    if erasureReq != nil {
        s.logAudit(r, req.UserID, "gdpr_erasure_created", true,
            fmt.Sprintf("request_id: %s, deadline: %s", erasureReq.ID, erasureReq.Deadline))
    }
}
```

Import `"github.com/orneryd/nornicdb/pkg/retention"` in the file.

This adds legal hold enforcement and audit-tracked erasure without changing the existing
delete path. The actual deletion still goes through `db.DeleteUserData()` / `db.AnonymizeUserData()`.

---

## Phase 6: Audit Logging Integration

**Goal**: Retention-driven deletions and archives produce audit log entries.

### 6.1 Add audit callback interface

Since the DB struct has no audit logger, the cleanest approach is a notification callback:

**File**: `pkg/nornicdb/db.go` — add to DB struct:

```go
// onRetentionAction is called when the retention manager deletes or archives a record.
// The server wires this to its audit logger.
onRetentionAction func(action, recordID, category string)
```

Add setter:

```go
func (db *DB) SetRetentionAuditCallback(fn func(action, recordID, category string)) {
    db.onRetentionAction = fn
}
```

### 6.2 Audit is wired into callbacks

The delete and archive callbacks in Phase 1.3 already call `db.onRetentionAction` when set.
No separate wiring is needed in the callbacks — it's built in from the start.

### 6.3 Server wires the callback

**File**: `pkg/server/server.go` — during `New()` or wherever DB is configured:

```go
db.SetRetentionAuditCallback(func(action, recordID, category string) {
    s.auditLogger.LogDataAccess("system", "retention-manager",
        "node", recordID, action, true, category)
})
```

---

## File Change Summary

| File                             | Change                                                                                                                                   | Phase |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| `pkg/nornicdb/db.go`             | Add `retentionManager` field, `onRetentionAction` field, instantiate in `Open()`, persist in `closeInternal()`, expose getter + setter   | 1, 6  |
| `pkg/nornicdb/db_retention.go`   | **New file** — sweep goroutine, label exclusion, `nodeToDataRecord()`, `inferCategory()`, budget-limited iteration                       | 2     |
| `pkg/config/config.go`           | Add `RetentionPolicyConfig`, `RetentionConfig` structs (with `ExcludedLabels`), add `Retention` field to `Config`, wire YAML/env parsing | 3     |
| `pkg/server/server_retention.go` | **New file** — HTTP handlers for retention admin API                                                                                     | 4     |
| `pkg/server/server_router.go`    | Add `s.registerRetentionRoutes(mux)` call (line ~28)                                                                                     | 4     |
| `pkg/server/server_gdpr.go`      | Add legal hold check + erasure tracking to `handleGDPRDelete` (line ~83)                                                                 | 5     |
| `pkg/server/server.go`           | Wire `db.SetRetentionAuditCallback()`                                                                                                    | 6     |

## New files

| File                             | Purpose                                                     |
| -------------------------------- | ----------------------------------------------------------- |
| `pkg/nornicdb/db_retention.go`   | Background sweep + label exclusion + node-to-record mapping |
| `pkg/server/server_retention.go` | HTTP admin endpoints for retention management               |

## Exact seam locations

| File                                             | Seam Point                                                                                                     | What to insert                                                          |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `pkg/nornicdb/db.go` (struct)                    | After `lifecycleManager *lifecycle.MVCCLifecycleManager` (line 437)                                            | `retentionManager *retention.Manager` and `onRetentionAction func(...)` |
| `pkg/nornicdb/db.go` (`Open()`)                  | After `MVCCLifecycleEnabled` block (line ~892), before WAL init (line ~894)                                    | Manager creation + callback wiring                                      |
| `pkg/nornicdb/db.go` (`Open()` bg task)          | After `lifecycleManager.StartLifecycle()` block (line ~1230), before "Collect all database names" (line ~1232) | `db.startRetentionSweep(db.buildCtx)`                                   |
| `pkg/nornicdb/db.go` (`closeInternal()`)         | **After** `bgWg.Wait()` (line ~1725), before search index persist (line ~1730)                                 | `retentionManager.SavePolicies()`                                       |
| `pkg/server/server_router.go` (`buildRouter()`)  | After `s.registerAdminRoutes(mux)` (line 28)                                                                   | `s.registerRetentionRoutes(mux)`                                        |
| `pkg/server/server_gdpr.go` (`handleGDPRDelete`) | After auth check (line ~83), before delete call (line ~86)                                                     | Legal hold check + erasure request                                      |

---

## Implementation Order

```
Phase 1 (Wire manager into DB)       ← Foundation
  ↓
Phase 2 (Background sweep)           ← Makes retention enforce at runtime
  ↓
Phase 3 (Extended YAML config)    ┐
Phase 4 (HTTP admin API)          ├── Independent, can parallelize
Phase 5 (GDPR integration)       │
Phase 6 (Audit logging)          ┘
```

Phases 3–6 are independent of each other and can proceed in parallel after Phase 2.

---

## Testing Strategy

| Layer       | What to test                                                  | Location                                    |
| ----------- | ------------------------------------------------------------- | ------------------------------------------- |
| Unit        | Already exists (886 lines)                                    | `pkg/retention/retention_test.go` ✅        |
| Unit        | `nodeToDataRecord()`, `inferCategory()`, `hasExcludedLabel()` | `pkg/nornicdb/db_retention_test.go` (new)   |
| Unit        | `retentionExcludedLabels()` with various configs              | `pkg/nornicdb/db_retention_test.go` (new)   |
| Integration | Manager creation from compliance config values                | `pkg/nornicdb/db_test.go`                   |
| Integration | Background sweep finds + processes expired nodes              | `pkg/nornicdb/db_retention_test.go` (new)   |
| Integration | Sweep skips nodes with excluded labels                        | `pkg/nornicdb/db_retention_test.go` (new)   |
| Integration | Sweep respects budget limit (stops at maxSweepRecords)        | `pkg/nornicdb/db_retention_test.go` (new)   |
| Integration | YAML policy loading → manager has correct policies            | `pkg/config/config_test.go`                 |
| HTTP        | Retention admin endpoints CRUD                                | `pkg/server/server_retention_test.go` (new) |
| HTTP        | GDPR delete blocked by legal hold                             | `pkg/server/server_gdpr_test.go`            |
| HTTP        | GDPR delete creates erasure request                           | `pkg/server/server_gdpr_test.go`            |

---

## Risk Notes

1. **Sweep performance on large datasets**: `StreamNodesWithFallback` iterates all nodes. The
   `defaultMaxSweepRecords = 50000` budget prevents runaway sweeps. For millions of nodes,
   consider tracking a "last swept" node ID to resume from where the previous sweep stopped
   instead of re-scanning from the beginning each time.

2. **Multi-database**: The current design sweeps the default storage only. For multi-database
   support, extend `runRetentionSweep()` to iterate
   `db.baseStorage.(storage.NamespaceLister).ListNamespaces()` and sweep each namespace's
   storage, similar to how search index building works (line ~1232).

3. **Archive storage**: The archive callback writes to local filesystem. For production, the
   `retention.Manager.SetArchiveCallback()` API supports pluggable backends — the callback
   just needs a different implementation (S3, GCS, etc.).

4. **Legal hold ↔ GDPR tension**: GDPR Art.17 requires deletion within 30 days, but legal
   holds prevent deletion. `ProcessErasure()` correctly marks these as `ErasureStatusPartial`.
   The HTTP response in Phase 5 should surface retained items and reasons to the requesting user.

5. **WAL entries**: Retention-deleted records may still appear in WAL segments. WAL compaction
   handles this over time, but operators should be aware of the lag between deletion and
   physical erasure from WAL.

6. **Config precedence**: When both `compliance.retention_policy_days` and `retention.policies[]`
   are defined, the YAML policies take precedence. The simple config creates a fallback
   `config-default` policy only when no other policies are loaded (Phase 1.2 already handles this
   via the `len(rm.ListPolicies()) == 0` check).

7. **Implicit retention for uncategorized nodes**: If a node's inferred category has no matching
   policy, `ShouldDelete()` returns `(false, "no policy found")`. This means nodes without a
   recognized label or `data_category` property default to `CategoryUser`. If no `USER` policy
   exists, those nodes are retained indefinitely. This is the safe default — explicit policy
   creation is required for deletion to occur.

8. **Edge cleanup is automatic**: `BadgerEngine.DeleteNode()` uses `deleteNodeInTxn` which
   deletes connected edges in the same transaction. The sweep does not need separate edge
   cleanup logic.

---

## Changes from Previous Version

| What Changed                                                                                | Why                                                                                   |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `closeInternal` seam moved to **after** `bgWg.Wait()`                                       | Sweep goroutine is tracked by bgWg; saving before it stops risks concurrent mutation  |
| `removeNodeFromSearchIndex` → `removeNodeFromSearchIndexes(ctx, dbName, storageEngine, id)` | Actual function signature requires 4 args, not 1                                      |
| Added `excluded_labels` config and `hasExcludedLabel()`                                     | Label-based exclusion was the primary neo4j-like ergonomic gap                        |
| Added sweep budget (`defaultMaxSweepRecords`)                                               | Prevents unbounded sweep duration on large datasets                                   |
| Removed manual `bgWg.Add(1)` / `bgWg.Done()` from sweep                                     | `startBackgroundTask` handles this internally — callers must not touch bgWg           |
| Added edge cascade documentation                                                            | Confirmed `BadgerEngine.DeleteNode` cascades; no separate edge work needed            |
| Added precedence rules section                                                              | Clear, documented evaluation order prevents developer surprise                        |
| Moved `RetentionConfig` to `pkg/config/config.go`                                           | Cannot add fields to type alias `nornicdb.Config`; all config belongs in `pkg/config` |
| Removed `db.auditLogger` references                                                         | DB struct has no audit logger; callback pattern is the correct approach               |
