# Retention Policies — Full Implementation Plan

## Overview

The `pkg/retention` package is a **complete, well-tested library** with policies, legal holds, GDPR erasure requests, archive-before-delete, and JSON persistence. However, it is **entirely disconnected from the running server**. No startup code creates a `retention.Manager`, no background goroutine enforces policies, no HTTP endpoints expose the API, and the existing `config.yaml` compliance fields (`retention_enabled`, `retention_policy_days`, etc.) are parsed but never consumed by any runtime component.

This plan wires the retention engine end-to-end: config → manager creation → background enforcement → HTTP admin API → GDPR integration → shutdown.

---

## Audit of Current State

### What exists today ✅

| Component | Location | Status |
|---|---|---|
| `retention.Manager` | `pkg/retention/retention.go:349` | Complete — policies, legal holds, erasure, callbacks |
| `retention.Policy` | `pkg/retention/retention.go:143` | Complete — validation, expiry, archive, compliance frameworks |
| `retention.LegalHold` | `pkg/retention/retention.go:205` | Complete — active/inactive, subject/category filtering |
| `retention.ErasureRequest` | `pkg/retention/retention.go:272` | Complete — GDPR Art.17, 30-day deadline, status tracking |
| `retention.DefaultPolicies()` | `pkg/retention/retention.go:1235` | Complete — 7 pre-built policies (HIPAA, GDPR, SOX, etc.) |
| `Manager.SavePolicies()` / `LoadPolicies()` | `pkg/retention/retention.go:1008-1053` | Complete — JSON file persistence |
| `Manager.ProcessRecord()` | `pkg/retention/retention.go:800` | Complete — archive-then-delete with callbacks |
| `Manager.ProcessErasure()` | `pkg/retention/retention.go:931` | Complete — respects legal holds, tracks stats |
| Unit tests | `pkg/retention/retention_test.go` | Complete — 886 lines, full coverage |
| Simple compliance config | `pkg/config/config.go:503-515` | Parsed — `RetentionEnabled`, `RetentionPolicyDays`, `RetentionAutoDelete`, `RetentionExemptRoles` |
| YAML loading | `pkg/config/config.go:2877-2888` | Working — `compliance.retention_*` fields |
| Env var loading | `pkg/config/config.go:2057-2068` | Working — `NORNICDB_RETENTION_*` vars |
| GDPR HTTP handlers | `pkg/server/server_gdpr.go` | Working — but do NOT use `retention.Manager` at all |
| GDPR delete handler | `pkg/server/server_gdpr.go:56` | Calls `db.DeleteUserData()` / `db.AnonymizeUserData()` directly |

### What does NOT exist ❌

| Gap | Impact |
|---|---|
| No `retention.Manager` field on `DB` struct | Manager is never created |
| No manager instantiation in `Open()` | Config values are loaded but ignored |
| No config-to-policy bridge | `RetentionPolicyDays` never becomes a `retention.Policy` |
| No delete/archive callbacks wired to storage | `ProcessRecord()` would no-op even if called |
| No background retention sweep goroutine | Expired records are never found or processed |
| No retention HTTP endpoints | Admin cannot manage policies, holds, or erasure via API |
| No full `retention.policies` YAML schema | Users cannot define per-category policies in config |
| No integration with GDPR delete handler | `handleGDPRDelete` does not check legal holds or use erasure tracking |
| No retention manager shutdown | No `Close()` to stop background goroutine |
| No audit logging of retention actions | Deletions/archives not audit-logged |
| No multi-database support | Retention is not namespace-aware |

---

## Implementation Phases

### Phase 1: Wire Manager into DB Lifecycle

**Goal:** Create the `retention.Manager` during `db.Open()` using config values, store it on the `DB` struct, and expose it to the server.

#### 1.1 Add `retention.Manager` to `DB` struct

**File:** `pkg/nornicdb/db.go` — near line ~437 (after `lifecycleManager`)

```go
import "github.com/orneryd/nornicdb/pkg/retention"

// In DB struct:
retentionManager *retention.Manager
```

#### 1.2 Add retention config fields to `nornicdb.Config`

**File:** `pkg/nornicdb/db.go` (or `pkg/nornicdb/config.go` if separate)

Add to `Config` struct:

```go
type Config struct {
    // ... existing fields ...

    // Retention policy enforcement
    Retention RetentionConfig
}

type RetentionConfig struct {
    Enabled          bool
    PolicyDays       int      // Default retention period (0 = indefinite)
    AutoDelete       bool     // Auto-delete vs archive
    ExemptRoles      []string // Roles exempt from retention
    SweepInterval    time.Duration // How often to run retention sweep (default: 1h)
    PoliciesFile     string   // Path to policies JSON file (optional)
}
```

#### 1.3 Instantiate manager in `Open()`

**File:** `pkg/nornicdb/db.go` — in `Open()` function, after lifecycle manager setup (~line 892)

Follow the same pattern as `lifecycleManager`:

```go
// Initialize retention manager if enabled
if config.Retention.Enabled {
    rm := retention.NewManager()

    // Load persisted policies if file exists
    policiesPath := filepath.Join(dataDir, "retention-policies.json")
    if config.Retention.PoliciesFile != "" {
        policiesPath = config.Retention.PoliciesFile
    }
    if _, err := os.Stat(policiesPath); err == nil {
        if err := rm.LoadPolicies(policiesPath); err != nil {
            log.Printf("⚠️  Failed to load retention policies: %v", err)
        }
    }

    // If no policies loaded, create a default policy from simple config
    if len(rm.ListPolicies()) == 0 && config.Retention.PolicyDays > 0 {
        defaultPolicy := &retention.Policy{
            ID:       "config-default",
            Name:     "Default Retention Policy (from config)",
            Category: retention.CategoryUser,
            RetentionPeriod: retention.RetentionPeriod{
                Duration: time.Duration(config.Retention.PolicyDays) * 24 * time.Hour,
            },
            ArchiveBeforeDelete: !config.Retention.AutoDelete,
            ArchivePath:         filepath.Join(dataDir, "archive"),
            Active:              true,
        }
        rm.AddPolicy(defaultPolicy)
    }

    db.retentionManager = rm
    log.Printf("📋 Retention manager enabled (%d policies loaded)", len(rm.ListPolicies()))
}
```

#### 1.4 Wire delete/archive callbacks to storage

**File:** `pkg/nornicdb/db.go` — immediately after manager creation in `Open()`

```go
// Wire retention callbacks to storage engine
rm.SetDeleteCallback(func(record *retention.DataRecord) error {
    return db.storage.DeleteNode(record.ID)
})
rm.SetArchiveCallback(func(record *retention.DataRecord, archivePath string) error {
    // Read node, serialize to archive file, then delete
    node, err := db.storage.GetNode(record.ID)
    if err != nil {
        return err
    }
    // Archive implementation (write to archivePath as JSON)
    archiveFile := filepath.Join(archivePath, record.ID+".json")
    data, err := json.Marshal(node)
    if err != nil {
        return err
    }
    if err := os.MkdirAll(archivePath, 0755); err != nil {
        return err
    }
    return os.WriteFile(archiveFile, data, 0644)
})
```

#### 1.5 Expose manager via getter

**File:** `pkg/nornicdb/db.go`

```go
// GetRetentionManager returns the retention manager (nil if retention is disabled).
func (db *DB) GetRetentionManager() *retention.Manager {
    db.mu.RLock()
    defer db.mu.RUnlock()
    return db.retentionManager
}
```

#### 1.6 Wire config in `cmd/nornicdb/main.go`

**File:** `cmd/nornicdb/main.go` — where `dbConfig` is populated (~line 430-440)

```go
// Retention settings from config
dbConfig.Retention.Enabled = cfg.Compliance.RetentionEnabled
dbConfig.Retention.PolicyDays = cfg.Compliance.RetentionPolicyDays
dbConfig.Retention.AutoDelete = cfg.Compliance.RetentionAutoDelete
dbConfig.Retention.ExemptRoles = cfg.Compliance.RetentionExemptRoles
dbConfig.Retention.SweepInterval = 1 * time.Hour // Default; could be config-driven
```

#### 1.7 Shutdown

**File:** `pkg/nornicdb/db.go` — in `Close()` method (~line 1715)

```go
// Persist retention policies before shutdown
if db.retentionManager != nil {
    policiesPath := filepath.Join(db.config.Database.DataDir, "retention-policies.json")
    if err := db.retentionManager.SavePolicies(policiesPath); err != nil {
        log.Printf("⚠️  Failed to save retention policies: %v", err)
    }
}
```

**Seam:** Insert after `db.embedQueue.Close()` and before `db.bgWg.Wait()`.

---

### Phase 2: Background Retention Sweep

**Goal:** Periodically scan storage for expired records and process them according to policies.

#### 2.1 Add sweep goroutine

**File:** `pkg/nornicdb/db_retention.go` (new file)

```go
package nornicdb

import (
    "context"
    "log"
    "time"

    "github.com/orneryd/nornicdb/pkg/retention"
    "github.com/orneryd/nornicdb/pkg/storage"
)

// startRetentionSweep launches a background goroutine that periodically
// scans for and processes expired data records according to retention policies.
func (db *DB) startRetentionSweep(ctx context.Context) {
    if db.retentionManager == nil {
        return
    }

    interval := db.config.Retention.SweepInterval
    if interval <= 0 {
        interval = time.Hour
    }

    db.bgWg.Add(1)
    go func() {
        defer db.bgWg.Done()
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
    }()
}

// runRetentionSweep scans all nodes and processes them against retention policies.
func (db *DB) runRetentionSweep(ctx context.Context) {
    rm := db.retentionManager
    if rm == nil {
        return
    }

    processed := 0
    deleted := 0
    archived := 0

    // Stream all nodes and check each against policies
    db.storage.StreamNodesWithFallback(func(node *storage.Node) bool {
        select {
        case <-ctx.Done():
            return false // Stop iteration
        default:
        }

        // Map storage node to retention DataRecord
        record := nodeToDataRecord(node)

        shouldDelete, _ := rm.ShouldDelete(record)
        if shouldDelete {
            if err := rm.ProcessRecord(ctx, record); err != nil {
                log.Printf("⚠️  Retention: failed to process %s: %v", node.ID, err)
            } else {
                deleted++
                // Also remove from search indexes
                db.removeFromSearchIndexes(node.ID)
            }
        }
        processed++
        return true // Continue iteration
    })

    if deleted > 0 {
        log.Printf("📋 Retention sweep: processed=%d, deleted=%d", processed, deleted)
    }
}

// nodeToDataRecord maps a storage.Node to a retention.DataRecord.
func nodeToDataRecord(node *storage.Node) *retention.DataRecord {
    record := &retention.DataRecord{
        ID:        node.ID,
        CreatedAt: node.CreatedAt,
        Metadata:  make(map[string]string),
    }

    // Extract subject ID from common properties
    for _, prop := range []string{"owner_id", "created_by", "user_id", "author"} {
        if v, ok := node.Properties[prop]; ok {
            record.SubjectID = fmt.Sprintf("%v", v)
            break
        }
    }

    // Map node labels to data categories
    record.Category = inferCategory(node)

    // Copy last access time if available
    if node.UpdatedAt.After(node.CreatedAt) {
        record.LastAccessedAt = node.UpdatedAt
    }

    return record
}

// inferCategory maps node labels/properties to retention data categories.
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

    // Check for data_category property (user-defined)
    if cat, ok := node.Properties["data_category"]; ok {
        return retention.DataCategory(fmt.Sprintf("%v", cat))
    }

    return retention.CategoryUser // Default
}
```

#### 2.2 Start sweep in `Open()`

**File:** `pkg/nornicdb/db.go` — in the `db.bgWg` startup section (~line 1223, after lifecycle manager start)

```go
// Start retention sweep after warmup
if db.retentionManager != nil {
    db.startRetentionSweep(db.buildCtx)
}
```

**Seam:** Insert after the `lifecycleManager.StartLifecycle()` block and before the k-means cluster timer setup.

---

### Phase 3: Extended Config — Per-Category Policies in YAML

**Goal:** Allow users to define full `retention.Policy` objects in `config.yaml`, not just the simple `retention_policy_days` integer.

#### 3.1 Add YAML schema

**File:** `pkg/config/config.go` — in `YAMLConfig` struct, after the existing `Compliance` section (~line 1310)

```go
// Retention policies (extended, per-category)
Retention struct {
    SweepInterval string `yaml:"sweep_interval"` // e.g. "1h", "30m"
    PoliciesFile  string `yaml:"policies_file"`   // Path to policies JSON
    Policies []struct {
        ID                  string   `yaml:"id"`
        Name                string   `yaml:"name"`
        Category            string   `yaml:"category"`
        RetentionDays       int      `yaml:"retention_days"`
        Indefinite          bool     `yaml:"indefinite"`
        ArchiveBeforeDelete bool     `yaml:"archive_before_delete"`
        ArchivePath         string   `yaml:"archive_path"`
        ComplianceFrameworks []string `yaml:"compliance_frameworks"`
        Description         string   `yaml:"description"`
        Active              *bool    `yaml:"active"` // Default true
    } `yaml:"policies"`
    DefaultPolicies bool `yaml:"default_policies"` // Load DefaultPolicies()
} `yaml:"retention"`
```

#### 3.2 Example `config.yaml`

```yaml
# Simple (existing) — still works:
compliance:
  retention_enabled: true
  retention_policy_days: 365
  retention_auto_delete: false
  retention_exempt_roles: ["admin"]

# Extended (new) — per-category:
retention:
  sweep_interval: "1h"
  default_policies: true  # Start with built-in HIPAA/GDPR/SOX policies
  policies:
    - id: pii-1y
      name: "PII — 1 Year"
      category: PII
      retention_days: 365
      archive_before_delete: false
      compliance_frameworks: ["GDPR"]
      description: "Aggressive GDPR data minimization"

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
```

#### 3.3 Wire YAML policies into `nornicdb.Config`

**File:** `pkg/config/config.go` — in `applyYAMLConfig()` (~line 2877)

Convert YAML policy structs into `nornicdb.RetentionConfig` fields, which are then loaded into `retention.Policy` objects during `Open()`.

---

### Phase 4: HTTP Admin API for Retention

**Goal:** Expose retention policy management, legal holds, and erasure requests via HTTP endpoints.

#### 4.1 New file: `pkg/server/server_retention.go`

```
Endpoints:
  GET    /admin/retention/policies          — List all policies
  POST   /admin/retention/policies          — Add a policy
  GET    /admin/retention/policies/{id}     — Get a policy
  PUT    /admin/retention/policies/{id}     — Update a policy
  DELETE /admin/retention/policies/{id}     — Delete a policy
  POST   /admin/retention/policies/defaults — Load default policies

  GET    /admin/retention/holds             — List legal holds
  POST   /admin/retention/holds             — Place a legal hold
  DELETE /admin/retention/holds/{id}        — Release a legal hold

  GET    /admin/retention/erasures          — List erasure requests
  POST   /admin/retention/erasures          — Create erasure request (GDPR Art.17)
  POST   /admin/retention/erasures/{id}/process — Process an erasure request

  POST   /admin/retention/sweep             — Trigger immediate retention sweep
  GET    /admin/retention/status            — Retention manager status/stats
```

All endpoints require `auth.PermAdmin`.

#### 4.2 Register routes

**File:** `pkg/server/server_router.go` — add `registerRetentionRoutes(mux)` call in the route registration section (~line 195)

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

**Seam:** Call `s.registerRetentionRoutes(mux)` inside the existing `registerAdminRoutes` function or as a peer call alongside it in the main route setup.

---

### Phase 5: Integrate with Existing GDPR Handlers

**Goal:** Make the existing GDPR delete handler (`/gdpr/delete`) use the retention manager for legal hold checks and erasure tracking.

#### 5.1 Update `handleGDPRDelete`

**File:** `pkg/server/server_gdpr.go:56`

Before executing the delete, check legal holds and create an erasure request:

```go
func (s *Server) handleGDPRDelete(w http.ResponseWriter, r *http.Request) {
    // ... existing validation ...

    // If retention manager is available, check legal holds first
    rm := s.db.GetRetentionManager()
    if rm != nil {
        // Check if user is under legal hold
        if rm.IsUnderLegalHold(req.UserID, "") {
            s.writeError(w, http.StatusConflict,
                "user data is under legal hold and cannot be deleted",
                ErrConflict)
            return
        }

        // Create tracked erasure request (GDPR Art.17 compliance)
        erasureReq, err := rm.CreateErasureRequest(req.UserID, "")
        if err != nil && err != retention.ErrErasureInProgress {
            s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
            return
        }

        // Log erasure request ID for audit trail
        if erasureReq != nil {
            s.logAudit(r, req.UserID, "gdpr_erasure_created", true,
                fmt.Sprintf("request_id: %s, deadline: %s", erasureReq.ID, erasureReq.Deadline))
        }
    }

    // ... existing delete/anonymize logic ...
}
```

This adds legal hold enforcement and audit-tracked erasure without changing the existing delete path.

---

### Phase 6: Audit Logging Integration

**Goal:** All retention actions (policy enforcement, legal hold changes, erasure processing) produce audit log entries.

#### 6.1 Add audit hooks to retention callbacks

**File:** `pkg/nornicdb/db.go` — when wiring the delete callback in `Open()`

Wrap the delete callback to also log to audit:

```go
rm.SetDeleteCallback(func(record *retention.DataRecord) error {
    if err := db.storage.DeleteNode(record.ID); err != nil {
        return err
    }
    // Audit log the retention-driven deletion
    if db.auditLogger != nil {
        db.auditLogger.LogDataAccess("system", "retention-manager",
            "node", record.ID, "RETENTION_DELETE", true, string(record.Category))
    }
    return nil
})
```

---

## File Change Summary

| File | Change | Phase |
|---|---|---|
| `pkg/nornicdb/db.go` | Add `retentionManager` field, instantiate in `Open()`, persist in `Close()`, expose getter | 1 |
| `pkg/nornicdb/db.go` (Config) | Add `RetentionConfig` struct | 1 |
| `pkg/nornicdb/db_retention.go` | **New file** — sweep goroutine, `nodeToDataRecord()`, `inferCategory()` | 2 |
| `pkg/config/config.go` (YAMLConfig) | Add `retention:` YAML section with `policies[]` array | 3 |
| `pkg/config/config.go` (applyYAMLConfig) | Parse YAML policies into config | 3 |
| `pkg/server/server_retention.go` | **New file** — HTTP handlers for retention admin API | 4 |
| `pkg/server/server_router.go` | Add `registerRetentionRoutes(mux)` call | 4 |
| `pkg/server/server_gdpr.go` | Add legal hold check + erasure request tracking to `handleGDPRDelete` | 5 |
| `cmd/nornicdb/main.go` | Wire `cfg.Compliance.Retention*` → `dbConfig.Retention.*` | 1 |

## New files

| File | Purpose |
|---|---|
| `pkg/nornicdb/db_retention.go` | Background sweep + node-to-record mapping |
| `pkg/server/server_retention.go` | HTTP admin endpoints for retention management |

## Existing files — exact seam locations

| File | Line/After | What to insert |
|---|---|---|
| `pkg/nornicdb/db.go` (struct) | After `lifecycleManager` (~437) | `retentionManager *retention.Manager` |
| `pkg/nornicdb/db.go` (`Open()`) | After lifecycle manager setup (~892) | Manager creation, callback wiring |
| `pkg/nornicdb/db.go` (`Close()`) | After `embedQueue.Close()` (~1720) | `retentionManager.SavePolicies()` |
| `pkg/nornicdb/db.go` (warmup) | After `lifecycleManager.StartLifecycle()` (~1228) | `db.startRetentionSweep(db.buildCtx)` |
| `cmd/nornicdb/main.go` | After WAL retention config (~439) | `dbConfig.Retention.*` assignment |
| `pkg/server/server_router.go` | After `registerAdminRoutes` (~195) | `s.registerRetentionRoutes(mux)` |
| `pkg/server/server_gdpr.go:56` | Top of `handleGDPRDelete`, after auth check | Legal hold check + erasure request |
| `pkg/config/config.go` YAMLConfig | After `Compliance` section (~1310) | `Retention` YAML struct |

---

## Testing Strategy

| Layer | What to test | Where |
|---|---|---|
| Unit | Already exists — `retention_test.go` (886 lines) | `pkg/retention/retention_test.go` ✅ |
| Integration | Manager creation from config values | `pkg/nornicdb/db_test.go` |
| Integration | Background sweep finds + processes expired nodes | `pkg/nornicdb/db_retention_test.go` (new) |
| Integration | YAML policy loading → manager has correct policies | `pkg/config/config_test.go` |
| HTTP | Retention admin endpoints return correct data | `pkg/server/server_test.go` |
| HTTP | GDPR delete respects legal holds | `pkg/server/server_test.go` |
| E2E | Create node → wait for sweep → verify deleted | Manual / CI script |

---

## Implementation Order

```
Phase 1 (Wire manager)      ← Foundation, do first
  ↓
Phase 2 (Background sweep)  ← Makes retention actually work
  ↓
Phase 3 (Extended YAML)     ← User-facing config improvements
  ↓
Phase 4 (HTTP admin API)    ← Runtime management
  ↓
Phase 5 (GDPR integration)  ← Legal hold enforcement
  ↓
Phase 6 (Audit logging)     ← Compliance trail
```

Phases 3-6 are independent of each other and can be parallelized after Phase 2 is complete.

---

## Risk Notes

1. **Sweep performance on large datasets:** `StreamNodesWithFallback` iterates all nodes. For millions of nodes, consider adding a `created_at` index or tracking "last swept" timestamps to avoid full scans.

2. **Multi-database:** The current design operates on the default database's storage. Phase 2 should be extended to iterate `dbManager.ListDatabases()` for multi-database support, similar to how `EmbedExisting` works.

3. **Archive storage:** The archive callback writes to local filesystem. For production, consider pluggable archive backends (S3, GCS, etc.). The `retention.Manager.SetArchiveCallback()` API already supports this — the callback just needs a different implementation.

4. **Legal hold ↔ GDPR tension:** GDPR Art.17 requires deletion within 30 days, but legal holds prevent deletion. The current `ProcessErasure()` correctly marks these as `ErasureStatusPartial` with retained reasons. The HTTP response should surface this to the requesting user.

5. **WAL entries:** Retention-deleted records may still appear in WAL segments. WAL compaction (already implemented) handles this over time, but operators should be aware of the lag.
