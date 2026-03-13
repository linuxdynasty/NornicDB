# GDPR Compliance Fixes — Concrete Implementation Plan

## Overview

This plan hardens NornicDB's GDPR compliance to fully satisfy Articles 15 (Right of Access), 17 (Right to Erasure), and 20 (Right to Data Portability). The current implementation has critical gaps that leave user data orphaned during export, delete, and anonymize operations. Each phase below maps directly to files and functions in the codebase with exact change descriptions, test requirements, and acceptance criteria.

---

## Audit of Current State

### What exists today

| Capability | File | Status |
|---|---|---|
| Export user nodes by `owner_id` | `pkg/nornicdb/db_admin.go:1383` `ExportUserData()` | Partial — nodes only, single property match |
| Delete user nodes by `owner_id` | `pkg/nornicdb/db_admin.go:1498` `DeleteUserData()` | Partial — nodes only, no edge cleanup |
| Anonymize user nodes | `pkg/nornicdb/db_admin.go:1534` `AnonymizeUserData()` | Partial — hardcoded PII fields, no edges |
| HTTP handlers | `pkg/server/server_gdpr.go` | Working — delegates to `db.ExportUserData` / `db.DeleteUserData` / `db.AnonymizeUserData` |
| Consent management | `pkg/nornicdb/db_privacy.go` | Working — `RecordConsent`, `HasConsent`, `RevokeConsent`, `GetUserConsents` |
| Audit logging | `pkg/audit/audit.go` | Working — `LogErasure`, `LogDataAccess`, `LogConsent`, compliance report generation |
| Auth user management | `pkg/auth/auth.go:1442` | Working — `DeleteUser`, `GetUser`, `GetUserByID`, `ListUsers` |
| Search index removal | `pkg/search/` | Working — `svc.RemoveNode(id)` removes from HNSW + BM25 |
| Compliance config | `pkg/config/config.go:422` `ComplianceConfig` | Working — `DataErasureEnabled`, `DataExportEnabled`, `DataAccessEnabled` flags |
| Storage streaming | `pkg/storage/types.go:867,901` | Working — `StreamNodesWithFallback`, `StreamEdgesWithFallback` |
| Encryption at rest | `pkg/encryption/` + BadgerDB AES-256 | Working — transparent via storage layer |

### Critical gaps

1. **Single-property user matching** — Only checks `owner_id`. Misses `created_by`, `author`, `user_id`, email-based matches, and nodes where the user is referenced by node ID.
2. **Edges completely ignored** — `ExportUserData`, `DeleteUserData`, and `AnonymizeUserData` never call `StreamEdgesWithFallback`. Relationships connected to or owned by the user are orphaned.
3. **No user account export/delete** — The `pkg/auth` `Authenticator` has `DeleteUser()` and `GetUser()` but they are never called from GDPR operations.
4. **No audit log export** — The `pkg/audit` `Reader.Query()` exists but is never called from `ExportUserData`.
5. **No consent record export** — `GetUserConsents()` exists but is not included in `ExportUserData` output.
6. **Hardcoded PII field list** — `AnonymizeUserData` only strips `email`, `name`, `username`, `ip_address`. Not configurable, misses `phone`, `address`, `date_of_birth`, custom PII fields.
7. **No embedding vector cleanup** — Node embeddings are removed from the search index, but the raw `Embedding` and `ChunkEmbeddings` fields on the node struct are not zeroed before anonymization.
8. **No WAL/txlog cleanup** — Canonical graph ledger entries referencing the user are not addressed.
9. **No multi-database support** — Operations only target the default database; users of named databases via `pkg/multidb` are not covered.

---

## Phase 1: Enhanced User Data Identification

**Goal:** Replace the single `owner_id` check with a configurable multi-property matcher that also resolves user data by node ID reference.

### 1.1 Add `GDPRConfig` to `ComplianceConfig`

**File:** `pkg/config/config.go`

Add a `GDPR` sub-struct to `ComplianceConfig`:

```go
// GDPRConfig holds GDPR-specific behavioral settings.
type GDPRConfig struct {
    // UserIDProperties lists node property names checked to identify user-owned data.
    // Default: ["owner_id", "created_by", "author", "user_id"]
    UserIDProperties []string

    // PIIProperties lists property names treated as personally identifiable information.
    // These are deleted during anonymization.
    // Default: ["email", "name", "username", "ip_address", "phone", "address", "date_of_birth"]
    PIIProperties []string

    // IncludeAuditLogs controls whether audit log entries are included in GDPR exports.
    IncludeAuditLogs bool

    // IncludeUserAccount controls whether auth system user account info is included in exports.
    IncludeUserAccount bool

    // IncludeConsents controls whether consent records are included in exports.
    IncludeConsents bool

    // DeleteUserAccount controls whether the auth system user is deleted (true) or disabled (false).
    DeleteUserAccount bool

    // AuditLogAction controls how audit logs are handled during erasure: "anonymize" or "delete".
    AuditLogAction string
}
```

Wire defaults in `DefaultConfig()`:

```go
GDPR: GDPRConfig{
    UserIDProperties:   []string{"owner_id", "created_by", "author", "user_id"},
    PIIProperties:      []string{"email", "name", "username", "ip_address", "phone", "address", "date_of_birth"},
    IncludeAuditLogs:   true,
    IncludeUserAccount: true,
    IncludeConsents:    true,
    DeleteUserAccount:  true,
    AuditLogAction:     "anonymize",
},
```

Add YAML mapping and env-var overrides (`NORNICDB_GDPR_USER_ID_PROPERTIES`, etc.) following the existing pattern in `applyEnvOverrides()`.

### 1.2 Add `isUserData()` helper

**File:** `pkg/nornicdb/db_admin.go` (new helper, near line ~1380)

```go
// isUserData checks whether a node belongs to or references the given userID
// by scanning all configured user-ID property names.
func (db *DB) isUserData(node *storage.Node, userID string) bool {
    for _, prop := range db.config.Compliance.GDPR.UserIDProperties {
        if val, ok := node.Properties[prop].(string); ok && val == userID {
            return true
        }
    }
    return false
}

// isUserEdge checks whether an edge belongs to the user directly (via properties)
// or is connected to any of the user's nodes.
func (db *DB) isUserEdge(edge *storage.Edge, userID string, userNodeIDs map[storage.NodeID]bool) bool {
    // Check edge properties
    for _, prop := range db.config.Compliance.GDPR.UserIDProperties {
        if val, ok := edge.Properties[prop].(string); ok && val == userID {
            return true
        }
    }
    // Check if connected to a user-owned node
    if userNodeIDs != nil {
        if userNodeIDs[edge.StartNode] || userNodeIDs[edge.EndNode] {
            return true
        }
    }
    return false
}
```

### 1.3 Update `ExportUserData`, `DeleteUserData`, `AnonymizeUserData`

Replace the inline `n.Properties["owner_id"]` checks with `db.isUserData(n, userID)` calls. This is a mechanical replacement in all three functions.

### Tests (Phase 1)

**File:** `pkg/nornicdb/db_privacy_test.go` — add table-driven tests:

- `TestIsUserData_MultiplePropertyNames` — node with `created_by`, `author`, `user_id` each matched
- `TestExportUserData_FindsByCreatedBy` — export finds nodes tagged with `created_by`
- `TestDeleteUserData_FindsByAuthor` — delete finds nodes tagged with `author`
- `TestAnonymizeUserData_FindsByUserID` — anonymize finds nodes tagged with `user_id`
- `TestIsUserData_NoFalsePositives` — nodes belonging to other users are not matched

### Acceptance Criteria

- [ ] `isUserData()` matches against all configured `UserIDProperties`
- [ ] All three GDPR operations use the new matcher
- [ ] Default config includes `["owner_id", "created_by", "author", "user_id"]`
- [ ] Config is overridable via YAML and env vars
- [ ] Existing `owner_id`-based tests continue to pass
- [ ] 95%+ coverage on new helper functions

---

## Phase 2: Relationship (Edge) Support

**Goal:** Export, delete, and anonymize relationships connected to or owned by the user.

### 2.1 Export edges

**File:** `pkg/nornicdb/db_admin.go` — `ExportUserData()`

After collecting user nodes, add a second streaming pass:

```go
// Collect user node IDs into a set for edge matching
userNodeIDs := make(map[storage.NodeID]bool, len(userData))
for _, d := range userData {
    userNodeIDs[storage.NodeID(d["id"].(string))] = true
}

// Collect user edges
var userEdges []map[string]interface{}
err = storage.StreamEdgesWithFallback(ctx, db.storage, 1000, func(e *storage.Edge) error {
    if db.isUserEdge(e, userID, userNodeIDs) {
        userEdges = append(userEdges, map[string]interface{}{
            "id":         string(e.ID),
            "type":       e.Type,
            "start_node": string(e.StartNode),
            "end_node":   string(e.EndNode),
            "properties": e.Properties,
            "created_at": e.CreatedAt,
        })
    }
    return nil
})
```

Include `"relationships": userEdges` in the JSON output alongside existing `"data"` key.

### 2.2 Delete edges before nodes

**File:** `pkg/nornicdb/db_admin.go` — `DeleteUserData()`

Before deleting nodes, collect and delete connected edges:

```go
userNodeIDs := make(map[storage.NodeID]bool, len(toDelete))
for _, id := range toDelete {
    userNodeIDs[id] = true
}

var edgesToDelete []storage.EdgeID
err = storage.StreamEdgesWithFallback(ctx, db.storage, 1000, func(e *storage.Edge) error {
    if db.isUserEdge(e, userID, userNodeIDs) {
        edgesToDelete = append(edgesToDelete, e.ID)
    }
    return nil
})
// Delete edges first (before nodes, to avoid dangling references)
for _, eid := range edgesToDelete {
    if err := db.storage.DeleteEdge(eid); err != nil {
        return err
    }
}
```

### 2.3 Anonymize edge properties

**File:** `pkg/nornicdb/db_admin.go` — `AnonymizeUserData()`

Add edge anonymization pass — strip PII properties from edges connected to user nodes, and replace user-ID properties with the anonymous ID:

```go
var edgesToUpdate []*storage.Edge
err = storage.StreamEdgesWithFallback(ctx, db.storage, 1000, func(e *storage.Edge) error {
    if db.isUserEdge(e, userID, userNodeIDs) {
        edgeCopy := /* deep copy edge */
        for _, prop := range db.config.Compliance.GDPR.UserIDProperties {
            if _, ok := edgeCopy.Properties[prop]; ok {
                edgeCopy.Properties[prop] = anonymousID
            }
        }
        for _, prop := range db.config.Compliance.GDPR.PIIProperties {
            delete(edgeCopy.Properties, prop)
        }
        edgesToUpdate = append(edgesToUpdate, edgeCopy)
    }
    return nil
})
```

### Tests (Phase 2)

**File:** `pkg/nornicdb/db_privacy_test.go`

- `TestExportUserData_IncludesRelationships` — create user node + edges, verify export contains `relationships` key with correct edge data
- `TestDeleteUserData_DeletesRelationships` — verify edges connected to user nodes are deleted
- `TestDeleteUserData_EdgesDeletedBeforeNodes` — verify no dangling edge errors
- `TestAnonymizeUserData_AnonymizesEdgeProperties` — verify PII stripped from edge properties
- `TestDeleteUserData_PreservesUnrelatedEdges` — edges not connected to user nodes are untouched

### Acceptance Criteria

- [ ] `ExportUserData` JSON output includes `"relationships"` array
- [ ] `DeleteUserData` removes edges before nodes
- [ ] `AnonymizeUserData` strips PII from edge properties
- [ ] CSV export includes a relationships section (or separate file)
- [ ] No orphaned edges after `DeleteUserData`
- [ ] No performance regression >10% on delete operations (benchmark)

---

## Phase 3: User Account and Auth System Integration

**Goal:** Include user account info in exports and delete/disable accounts during erasure.

### 3.1 Pass `Authenticator` reference to `DB`

**File:** `pkg/nornicdb/db.go`

Add an optional `auth *auth.Authenticator` field to the `DB` struct. Provide a setter:

```go
func (db *DB) SetAuthenticator(a *auth.Authenticator) {
    db.auth = a
}
```

Wire this in `pkg/server/server.go` during `New()` where `authenticator` is already available — call `db.SetAuthenticator(authenticator)` after constructing the server.

### 3.2 Include user account in export

**File:** `pkg/nornicdb/db_admin.go` — `ExportUserData()`

```go
var userAccount map[string]interface{}
if db.auth != nil && db.config.Compliance.GDPR.IncludeUserAccount {
    if user, err := db.auth.GetUserByID(userID); err == nil && user != nil {
        userAccount = map[string]interface{}{
            "username":   user.Username,
            "roles":      user.Roles,
            "created_at": user.CreatedAt,
            "last_login": user.LastLogin,
        }
    }
}
```

Add `"user_account": userAccount` to the JSON export output.

### 3.3 Delete or disable user account on erasure

**File:** `pkg/nornicdb/db_admin.go` — `DeleteUserData()`

After deleting nodes and edges:

```go
if db.auth != nil && db.config.Compliance.GDPR.DeleteUserAccount {
    if user, err := db.auth.GetUserByID(userID); err == nil && user != nil {
        _ = db.auth.DeleteUser(user.Username)
    }
}
```

### Tests (Phase 3)

- `TestExportUserData_IncludesUserAccount` — mock authenticator, verify `user_account` in export
- `TestDeleteUserData_DeletesUserAccount` — verify `auth.DeleteUser` called
- `TestDeleteUserData_NoAuthenticator` — verify graceful no-op when auth is nil
- `TestExportUserData_UserAccountDisabledByConfig` — set `IncludeUserAccount: false`, verify omitted

### Acceptance Criteria

- [ ] Export JSON includes `user_account` when auth is configured
- [ ] `DeleteUserData` calls `auth.DeleteUser` when `DeleteUserAccount` is true
- [ ] Operations are no-ops when `db.auth` is nil
- [ ] No import cycle between `pkg/nornicdb` and `pkg/auth`

---

## Phase 4: Audit Log and Consent Record Integration

**Goal:** Include audit logs and consent records in GDPR exports, and anonymize/delete audit entries during erasure.

### 4.1 Include audit logs in export

**File:** `pkg/nornicdb/db_admin.go` — `ExportUserData()`

The `pkg/audit.Reader.Query()` already supports filtering by `UserID`. Use it:

```go
if db.auditReader != nil && db.config.Compliance.GDPR.IncludeAuditLogs {
    auditResult, err := db.auditReader.Query(audit.Query{
        UserID: userID,
        Limit:  10000,
    })
    if err == nil && auditResult != nil {
        // Include audit events in export
    }
}
```

Add `auditReader *audit.Reader` field to `DB` struct, set during initialization from `config.Compliance.AuditLogPath`.

### 4.2 Include consent records in export

**File:** `pkg/nornicdb/db_admin.go` — `ExportUserData()`

```go
if db.config.Compliance.GDPR.IncludeConsents {
    // GetUserConsents already exists in db_privacy.go — call it directly
    // (Note: requires releasing db.mu.RLock and using a non-locking internal variant,
    //  or restructuring to avoid double-lock)
    consents := db.getUserConsentsInternal(ctx, userID)
    // Include in export
}
```

Extract `getUserConsentsInternal()` as an unexported lock-free helper called by both `GetUserConsents()` (which acquires the lock) and `ExportUserData()` (which already holds it).

### 4.3 Anonymize audit logs during erasure

**File:** `pkg/audit/audit.go` — add new method:

```go
// AnonymizeUserEntries rewrites audit log entries for the given userID,
// replacing the UserID and Username fields with anonymized values while
// preserving the event structure for compliance record-keeping.
func (r *Reader) AnonymizeUserEntries(userID, anonymousID string) error {
    // Read all entries, rewrite matching ones, write back
}
```

Call from `DeleteUserData()` / `AnonymizeUserData()` when `AuditLogAction == "anonymize"`.

### 4.4 Delete consent nodes during erasure

**File:** `pkg/nornicdb/db_admin.go` — `DeleteUserData()`

After deleting user data nodes, also delete consent nodes:

```go
// Delete consent records (prefixed with "consent:{userID}:")
prefix := fmt.Sprintf("consent:%s:", userID)
err = storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(n *storage.Node) error {
    if strings.HasPrefix(string(n.ID), prefix) {
        consentNodesToDelete = append(consentNodesToDelete, n.ID)
    }
    return nil
})
```

### Tests (Phase 4)

- `TestExportUserData_IncludesAuditLogs` — create audit entries, verify in export
- `TestExportUserData_IncludesConsents` — create consent records, verify in export
- `TestDeleteUserData_DeletesConsentNodes` — verify consent nodes removed
- `TestAnonymizeUserData_AnonymizesAuditLogs` — verify audit user fields replaced
- `TestExportUserData_AuditLogsDisabledByConfig` — set `IncludeAuditLogs: false`, verify omitted

### Acceptance Criteria

- [ ] Export JSON includes `audit_logs` array when configured
- [ ] Export JSON includes `consents` array when configured
- [ ] Consent nodes deleted during erasure
- [ ] Audit log entries anonymized (not deleted) by default during erasure
- [ ] No double-lock panics when ExportUserData reads consent records

---

## Phase 5: Configurable PII and Embedding Cleanup

**Goal:** Make PII field detection configurable and ensure embeddings are purged during anonymization.

### 5.1 Use configurable PII properties in `AnonymizeUserData`

**File:** `pkg/nornicdb/db_admin.go` — `AnonymizeUserData()`

Replace the hardcoded field deletions:

```go
// Current (hardcoded):
delete(nodeCopy.Properties, "email")
delete(nodeCopy.Properties, "name")
delete(nodeCopy.Properties, "username")
delete(nodeCopy.Properties, "ip_address")

// Replace with:
for _, piiProp := range db.config.Compliance.GDPR.PIIProperties {
    delete(nodeCopy.Properties, piiProp)
}
```

### 5.2 Zero embeddings during anonymization

**File:** `pkg/nornicdb/db_admin.go` — `AnonymizeUserData()`

After stripping PII properties, also zero embedding data:

```go
nodeCopy.Embedding = nil
nodeCopy.ChunkEmbeddings = nil
```

And remove from the search index:

```go
if svc, _ := db.getOrCreateSearchService(db.defaultDatabaseName(), db.storage); svc != nil {
    _ = svc.RemoveNode(nodeCopy.ID)
}
```

### Tests (Phase 5)

- `TestAnonymizeUserData_UsesConfiguredPIIProperties` — add custom PII property, verify deleted
- `TestAnonymizeUserData_ZerosEmbeddings` — create node with embedding, verify nil after anonymize
- `TestAnonymizeUserData_RemovesFromSearchIndex` — verify search index no longer returns anonymized node

### Acceptance Criteria

- [ ] PII properties from config are all deleted during anonymization
- [ ] `Embedding` and `ChunkEmbeddings` set to nil
- [ ] Search index entries removed for anonymized nodes
- [ ] Custom PII properties (e.g., `"social_security_number"`) can be added via config

---

## Phase 6: Multi-Database Support

**Goal:** GDPR operations scan all databases the user has data in, not just the default.

### 6.1 Iterate over all databases

**File:** `pkg/nornicdb/db_admin.go`

When `pkg/multidb` is in use, `ExportUserData` / `DeleteUserData` / `AnonymizeUserData` must resolve all constituent databases and run the data scan against each storage engine:

```go
databases := db.listAllDatabases() // returns default + named databases
for _, dbName := range databases {
    engine := db.getStorageForDatabase(dbName)
    // Run node/edge scan against this engine
}
```

This extends the existing `getOrCreateSearchService(dbName, engine)` pattern already used in `DeleteUserData` for search index cleanup.

### Tests (Phase 6)

- `TestExportUserData_MultiDatabase` — create user data in two databases, verify both exported
- `TestDeleteUserData_MultiDatabase` — verify data deleted from all databases

### Acceptance Criteria

- [ ] GDPR operations cover all named databases
- [ ] Default single-database behavior unchanged when multidb is not configured

---

## Phase 7: Comprehensive Testing and Validation

**Goal:** End-to-end GDPR compliance validation with full coverage.

### 7.1 End-to-end integration test

**File:** `pkg/nornicdb/db_privacy_test.go` — new test suite

```go
// TestGDPR_FullLifecycle exercises the complete GDPR workflow:
// 1. Create user account
// 2. Create nodes with various user-ID properties
// 3. Create relationships between user nodes and other nodes
// 4. Record consent
// 5. Generate audit trail
// 6. Export all user data (Art. 15) — verify completeness
// 7. Delete all user data (Art. 17) — verify thoroughness
// 8. Verify no residual data exists
func TestGDPR_FullLifecycle(t *testing.T) { ... }
```

### 7.2 Completeness verification test

```go
// TestGDPR_ExportCompleteness_AllDataTypes verifies that export includes:
// - Nodes with owner_id
// - Nodes with created_by
// - Nodes with author
// - Nodes with user_id
// - Relationships connected to user nodes
// - Relationships with user-ID properties
// - User account info
// - Audit log entries
// - Consent records
func TestGDPR_ExportCompleteness_AllDataTypes(t *testing.T) { ... }
```

### 7.3 Erasure thoroughness test

```go
// TestGDPR_Erasure_NoResidualData verifies that after DeleteUserData:
// - No nodes reference the user
// - No edges reference the user
// - User account is deleted
// - Search indexes return no results for user's content
// - Consent nodes are deleted
// - Audit logs are anonymized
func TestGDPR_Erasure_NoResidualData(t *testing.T) { ... }
```

### 7.4 Benchmark tests

```go
// BenchmarkGDPRExport measures export performance across data sizes
func BenchmarkGDPRExport(b *testing.B) { ... }

// BenchmarkGDPRDelete measures delete performance across data sizes
func BenchmarkGDPRDelete(b *testing.B) { ... }
```

### Test checklist

- [ ] Export includes nodes with `owner_id` ← exists, update for new properties
- [ ] Export includes nodes with `created_by` ← new
- [ ] Export includes nodes with `author` ← new
- [ ] Export includes nodes with `user_id` ← new
- [ ] Export includes relationships ← new
- [ ] Export includes user account ← new
- [ ] Export includes audit logs ← new
- [ ] Export includes consent records ← new
- [ ] Delete removes all user nodes ← exists, extend
- [ ] Delete removes all user relationships ← new
- [ ] Delete removes user account ← new
- [ ] Delete removes consent nodes ← new
- [ ] Delete handles search indexes ← exists
- [ ] Delete handles audit logs ← new
- [ ] Anonymize preserves graph structure ← exists
- [ ] Anonymize removes all configured PII ← extend
- [ ] Anonymize zeros embeddings ← new
- [ ] Anonymize handles edge properties ← new
- [ ] Multi-database coverage ← new
- [ ] No performance regression >10% ← new benchmark
- [ ] Race condition safety (`go test -race`) ← verify

### Acceptance Criteria

- [ ] All tests pass with `go test ./pkg/nornicdb/... -v`
- [ ] Coverage ≥ 90% on all GDPR functions
- [ ] `go test -race` passes
- [ ] Benchmarks documented in commit

---

## Export JSON Schema (Target)

After all phases, `ExportUserData` produces:

```json
{
  "user_id": "user-123",
  "exported_at": "2026-03-13T10:00:00Z",
  "user_account": {
    "username": "alice",
    "roles": ["editor"],
    "created_at": "2024-01-01T00:00:00Z",
    "last_login": "2026-03-12T15:30:00Z"
  },
  "nodes": [
    {
      "id": "node-1",
      "labels": ["Document"],
      "properties": {"title": "My Document", "owner_id": "user-123"},
      "created_at": "2024-01-15T10:00:00Z"
    }
  ],
  "relationships": [
    {
      "id": "edge-1",
      "type": "CREATED",
      "start_node": "user-123",
      "end_node": "node-1",
      "properties": {},
      "created_at": "2024-01-15T10:00:00Z"
    }
  ],
  "consents": [
    {
      "purpose": "analytics",
      "given": true,
      "timestamp": "2024-06-01T00:00:00Z",
      "source": "web_form"
    }
  ],
  "audit_logs": [
    {
      "timestamp": "2024-01-15T10:00:00Z",
      "type": "DATA_CREATE",
      "resource": "node",
      "resource_id": "node-1",
      "action": "CREATE",
      "success": true
    }
  ]
}
```

## Delete Summary Schema (Target)

After all phases, `DeleteUserData` returns a summary via the HTTP handler:

```json
{
  "status": "deleted",
  "user_id": "user-123",
  "deleted_at": "2026-03-13T10:00:00Z",
  "summary": {
    "nodes_deleted": 42,
    "relationships_deleted": 156,
    "consent_records_deleted": 3,
    "user_account_deleted": true,
    "audit_logs_anonymized": 1234
  }
}
```

---

## Implementation Order and Dependencies

```
Phase 1 (Enhanced Identification)
  └── Phase 2 (Edge Support) ← depends on isUserData/isUserEdge helpers
       ├── Phase 3 (Auth Integration) ← independent of edges, can parallel
       ├── Phase 4 (Audit/Consent) ← independent, can parallel
       └── Phase 5 (PII Config + Embeddings) ← independent, can parallel
            └── Phase 6 (Multi-Database) ← depends on all above
                 └── Phase 7 (E2E Testing) ← depends on all above
```

Phases 3, 4, and 5 can proceed in parallel once Phase 2 is complete.

## Files Modified (Summary)

| File | Changes |
|---|---|
| `pkg/config/config.go` | Add `GDPRConfig` struct, defaults, YAML/env mappings |
| `pkg/nornicdb/db.go` | Add `auth` and `auditReader` fields, setter methods |
| `pkg/nornicdb/db_admin.go` | Rewrite `ExportUserData`, `DeleteUserData`, `AnonymizeUserData`; add `isUserData`, `isUserEdge` helpers |
| `pkg/nornicdb/db_privacy.go` | Extract `getUserConsentsInternal` lock-free helper |
| `pkg/nornicdb/db_privacy_test.go` | Add ~20 new test cases across all phases |
| `pkg/audit/audit.go` | Add `AnonymizeUserEntries` method to `Reader` |
| `pkg/server/server.go` | Wire `db.SetAuthenticator(authenticator)` |
| `pkg/server/server_gdpr.go` | Update response to include delete summary |

## Migration Notes

- All changes are backward-compatible. Existing `owner_id`-based workflows continue to work.
- The enhanced matcher finds strictly more data than before (superset). No data that was previously exported/deleted will be missed.
- Applications should set at least one of the configured `UserIDProperties` on user-generated nodes to enable GDPR operations.
- The `GDPRConfig` defaults are opinionated. Operators can reduce the property scan list if their data model uses a single canonical user-ID field.
