# NornicDB CMEK + HSM Compliance Implementation Plan

**Version:** 1.1.0  
**Date:** April 2, 2026  
**Scope:** Strict compliance with customer-managed encryption keys (CMEK), hardware security modules (HSM), and FIPS 140-2 Level 2+ standards.

---

## Executive Summary

### Current State (Gap Analysis)

**What NornicDB Has:**
- ✅ At-rest encryption layer (`pkg/encryption/encryption.go`)
- ✅ PBKDF2/Argon2 key derivation
- ✅ AES-256-GCM authenticated encryption
- ✅ Key versioning framework
- ✅ Badger storage integration (EncryptionKey injection)

**What's Missing (Compliance Gap):**
- ❌ **CMEK provider abstraction** — No pluggable KMS/HSM interface
- ❌ **Envelope encryption** — Master key separation from data keys
- ❌ **Key lifecycle (rotation, versioning, audit)** — No versioning of key versions
- ❌ **KMS integration** — No AWS KMS, Azure Key Vault, GCP Cloud KMS, HashiCorp Vault, or Thales HSM support
- ❌ **FIPS 140-2 validation path** — No certified KMS module selection
- ❌ **Key URI scheme** — No standardized key reference format (e.g., `kms://aws/us-east-1/abc123`)
- ❌ **Audit logging** — No key operation audit trails
- ❌ **Key rotation orchestration** — No automated rotation with provider coordination
- ❌ **Compliance reporting** — No audit-ready compliance evidence export

---

## Plan Update: Build vs Buy (April 2026)

After surveying available Go libraries, this plan now adopts a **build-on-proven-libraries** strategy rather than implementing provider crypto plumbing from scratch.

### Selected Foundation Libraries

1. **Primary runtime abstraction:** `github.com/hashicorp/go-kms-wrapping/v2`
   - Rationale: existing provider wrappers for AWS/Azure/GCP/Vault, envelope-friendly workflow, mature usage in HashiCorp ecosystem.
2. **Optional crypto hardening path:** Google Tink (Go) for advanced envelope AEAD patterns where needed.
3. **Not selected as core abstraction:**
   - `gocloud.dev/secrets` (good for secret values, not full CMEK lifecycle/rotation orchestration)
   - AWS Encryption SDK for Go (excellent but AWS-specific; does not satisfy multi-cloud first-class requirement alone)

### Decision

- Keep a NornicDB `kms.KeyProvider` interface for portability and testability.
- Implement providers via **thin adapters** over `go-kms-wrapping/v2` where possible.
- Focus engineering on NornicDB-specific value:
  - policy enforcement,
  - audit/event signing,
  - rotation orchestration,
  - compliance evidence export.

### Compliance Positioning

Using external libraries does **not** imply automatic FIPS validation for the full stack.  
FIPS 140-2/3 posture remains dependent on:
- selected cloud/HSM service and endpoint configuration (e.g. FIPS endpoints),
- deployment controls,
- cryptographic module boundary for the target environment.

---

## Target Compliance Framework

### Regulatory Requirements

| Requirement               | NornicDB Implementation                                  |
|---------------------------|-----------------------------------------------------------|
| **GDPR Art.32**          | Encryption + key separation + audit trail                |
| **HIPAA §164.312(e)(2)** | CMEK + HSM + FIPS 140-2 Level 2+ + encryption audit      |
| **FISMA SC-13**          | NIST-approved algorithms (AES-256) + CMEK orchestration  |
| **SOC2 CC6.1/CC6.2**     | Key management controls + operator access controls       |
| **PCI-DSS 3.6**          | Strong KMS + key rotation every 90 days                  |
| **NIST SP 800-57**       | Key derivation, lifetime, rotation, storage separation   |

### FIPS 140-2 Levels (Target: Level 2+)

| Level | Primary Requirement                    | KMS Recommendation                           |
|-------|----------------------------------------|----------------------------------------------|
| 1     | Algorithm approval (basic)             | ❌ Insufficient                              |
| 2     | Physical tamper detection + CMEK      | ✅ AWS KMS, Azure Key Vault, GCP Cloud KMS  |
| 3     | Identity-based access + HSM + audit   | ✅ Thales HSM, CloudHSM, Vult HSM           |

---

## Architecture Design

### 1. Envelope Encryption Model

```
┌─────────────────────────────────────────────────────────────┐
│ Application Data (plaintext)                                │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
         ┌────────────────────────────────────┐
         │ Data Encryption Key (DEK) - Runtime│
         │ (AES-256, auto-rotated every 24h)  │
         └────────────────────────────────────┘
                          │
         ┌────────────────┴────────────────┐
         │ Encrypted with DEK             │
         ▼                                 ▼
    ┌────────────┐                  ┌──────────────┐
    │ Ciphertext │ + Encrypted DEK  │ IV + Nonce   │
    │ (to disk)  │                  │ (stored)     │
    └────────────┘                  └──────────────┘
                          │
                    ┌─────┴──────┐
                    │ DEK + KMS  │ (encrypted with Master Key)
                    │ Key URI    │
                    │ Timestamp  │
                    └─────┬──────┘
                          │
                          ▼
         ┌────────────────────────────────────┐
         │ Master Key (KEK) - Secure Storage  │
         │ AWS KMS / Azure Key Vault / HSM    │
         │ (Never leaves KMS/HSM)             │
         └────────────────────────────────────┘
```

**Benefit:** Data keys can be cached/auto-rotated; master key stays in KMS/HSM.

### 2. KMS Provider Interface (Abstraction)

```go
package kms

// KeyProvider abstracts any KMS/HSM implementation used by NornicDB.
// Concrete providers should typically be adapters over go-kms-wrapping/v2.
type KeyProvider interface {
    // GenerateDataKey creates a new data encryption key
    GenerateDataKey(ctx context.Context, opts KeyGenOpts) (*DataKey, error)
    
    // DecryptDataKey decrypts an already-protected data key
    DecryptDataKey(ctx context.Context, encryptedKey []byte, opts DecryptOpts) ([]byte, error)
    
    // RotateDataKey re-encrypts a data key with latest master key
    RotateDataKey(ctx context.Context, encryptedKey []byte) (*DataKey, error)
    
    // GetKeyMetadata retrieves key version and expiry info
    GetKeyMetadata(ctx context.Context, keyURI string) (*KeyMetadata, error)
    
    // SignAuditEvent writes immutable audit record (compliance)
    SignAuditEvent(ctx context.Context, event AuditEvent) error
    
    // Close releases resources
    Close(ctx context.Context) error
}

// DataKey represents an encrypted key + metadata
type DataKey struct {
    KeyURI       string    // "kms://aws/arn:aws:kms:us-east-1:123456789:key/abc"
    Ciphertext   []byte    // Encrypted DEK
    Plaintext    []byte    // Raw AES-256 key (in-memory only)
    Version      uint32    // Key version for audit
    CreatedAt    time.Time
    ExpiresAt    time.Time
    Algorithm    string    // "AES-256-GCM"
}

// KeyGenOpts customizes key generation
type KeyGenOpts struct {
    Algorithm string        // "AES-256", "AES-128"
    TTL       time.Duration // How long DEK is valid (24h default)
    Label     string        // "nornicdb-bolt-2026-04" for categorization
}
```

### 2.1 Adapter Strategy (go-kms-wrapping)

```go
// pkg/kms/adapters/wrapping_adapter.go
package adapters

import (
  wrapping "github.com/hashicorp/go-kms-wrapping/v2"
)

// WrappingAdapter translates NornicDB KeyProvider calls to go-kms-wrapping.
type WrappingAdapter struct {
  Wrapper wrapping.Wrapper
}
```

Notes:
- NornicDB owns canonical key URI, audit envelope format, and policy checks.
- Wrapping providers handle provider-specific cryptographic operations.

### 3. KMS Implementations (Priority Order)

#### Phase 1: Cloud (80% of enterprise use, via go-kms-wrapping adapters)
1. **AWS KMS** — FIPS 140-2 Level 2+ path, audit via CloudTrail
2. **Azure Key Vault** — FIPS 140-2 Level 2+ path, RBAC integration
3. **GCP Cloud KMS** — FIPS 140-2 Level 2+ path, Cloud Audit Logs integration

#### Phase 2: HSM (compliance-critical)
4. **Thales TSM HSM** — FIPS 140-2 Level 3, highest assurance
5. **AWS CloudHSM** — Customer-managed FIPS 140-2 Level 3
6. **Azure Dedicated HSM** — Thales backend, on-prem integration

#### Phase 3: On-Prem/Air-Gap
7. **HashiCorp Vault** — Flexible; integrates with HSMs
8. **Local KMS (fallback)** — File-based encryption for dev/test

---

## Implementation Roadmap

### Phase 1: Core Infrastructure (Weeks 1-4)

#### 1.1 KMS Provider Abstraction + Wrapping Adapters
**Files to Create:**
- `pkg/kms/provider.go` — KeyProvider interface + common types
- `pkg/kms/errors.go` — Compliance-specific error codes
- `pkg/kms/audit.go` — Audit event types + signing
- `pkg/kms/adapters/wrapping_adapter.go` — shared adapter helpers
- `pkg/kms/adapters/wrapping_adapter_test.go` — adapter contract tests

**Code Structure:**
```go
// pkg/kms/provider.go
package kms

type KeyProvider interface {
    GenerateDataKey(ctx context.Context, opts KeyGenOpts) (*DataKey, error)
    DecryptDataKey(ctx context.Context, encrypted []byte) ([]byte, error)
    RotateDataKey(ctx context.Context, encrypted []byte) (*DataKey, error)
    GetKeyMetadata(ctx context.Context, keyURI string) (*KeyMetadata, error)
    SignAuditEvent(ctx context.Context, event AuditEvent) error
    Close(ctx context.Context) error
}

type AuditEvent struct {
    EventType   string                 // "KEY_GENERATED", "KEY_DECRYPTED", etc.
    KeyURI      string
    Principal   string                 // Who/what triggered operation
    Timestamp   time.Time
    Status      string                 // "SUCCESS", "FAILURE"
    ErrorCode   string
    Metadata    map[string]interface{} // Compliance-relevant context
}
```

**Tests:**
- `pkg/kms/provider_test.go` — Interface compliance tests
- `pkg/kms/audit_test.go` — Audit event signing validation
- `pkg/kms/adapters/*_test.go` — wrapper adapter behavior and failure mapping

#### 1.2 Envelope Encryption Layer
**Files to Modify:**
- `pkg/encryption/envelope.go` (new) — Envelope encryption logic
- `pkg/encryption/encryption.go` — Wire in envelope model

**Changes:**
```go
// pkg/encryption/envelope.go (new 150 lines)

type EnvelopeEncryptor struct {
    kmProvider kms.KeyProvider      // DEK source
    dekCache   *sync.Map            // Runtime cache of data keys
    maxDEKAge  time.Duration        // Auto-rotate after this
}

// Encrypt uses cached/generated DEK + KMS
func (e *EnvelopeEncryptor) Encrypt(ctx context.Context, plaintext []byte) ([]byte, error) {
    // 1. Get or generate DEK from KMS
    dek := e.getOrGenerateDEK(ctx)
    
    // 2. Encrypt plaintext with DEK
    ciphertext := aesGcmEncrypt(plaintext, dek.Plaintext)
    
    // 3. Return: [DEK-ciphertext || IV || ciphertext]
    return e.formatEnvelope(dek, ciphertext)
}

// Decrypt reads DEK from envelope, decrypts with KMS
func (e *EnvelopeEncryptor) Decrypt(ctx context.Context, envelope []byte) ([]byte, error) {
    // 1. Parse envelope: extract encrypted DEK + IV + ciphertext
    dekCiphertext, iv, ciphertext := e.parseEnvelope(envelope)
    
    // 2. Decrypt DEK using KMS provider
    dek, err := e.kmProvider.DecryptDataKey(ctx, dekCiphertext)
    if err != nil {
        // Log audit event for failed decryption
        e.auditKeyDecryptionFailure(ctx, dekCiphertext, err)
        return nil, err
    }
    
    // 3. Decrypt ciphertext with DEK
    plaintext := aesGcmDecrypt(ciphertext, dek, iv)
    return plaintext, nil
}
```

**Tests:**
- `pkg/encryption/envelope_test.go` — 20+ test cases covering:
  - Envelope format correctness
  - DEK rotation triggering
  - KMS failure handling
  - Audit event generation

### Phase 2: KMS Provider Implementations (Weeks 5-8)

#### 2.1 AWS KMS Provider
**Files to Create:**
- `pkg/kms/providers/aws_kms.go` — Adapter over go-kms-wrapping AWS wrapper
- `pkg/kms/providers/aws_kms_test.go` — AWS integration tests (mock + real)

**Configuration (YAML):**
```yaml
encryption:
  provider: "aws-kms"
  aws_kms:
    region: "us-east-1"
    key_id: "arn:aws:kms:us-east-1:123456789:key/abc123"
    role_arn: "arn:aws:iam::123456789:role/nornicdb-kms"
    endpoint: "https://kms.us-east-1.amazonaws.com"
    
    # FIPS 140-2 enforcement
    enforce_fips: true
    
    # Compliance audit trail
    enable_cloudtrail: true
    cloudtrail_bucket: "s3://audit-logs/"
    
    # Key rotation policy
    rotation_interval: "90d"
    enable_auto_rotation: true
```

**Implementation Strategy:**
```go
// pkg/kms/providers/aws_kms.go

type AWSKMSProvider struct {
    adapter *adapters.WrappingAdapter
    config  AWSKMSConfig
}

// NewAWSKMSProvider creates FIPS-140-2 compliant AWS provider
func NewAWSKMSProvider(cfg AWSKMSConfig) (*AWSKMSProvider, error) {
    // 1. Build go-kms-wrapping AWS wrapper with cfg
    // 2. Enforce FIPS endpoint policy when configured
    // 3. Wrap into NornicDB adapter
}

// GenerateDataKey calls AWS KMS GenerateDataKey
func (p *AWSKMSProvider) GenerateDataKey(ctx context.Context, opts kms.KeyGenOpts) (*kms.DataKey, error) {
    // delegate to adapter + add NornicDB audit metadata
}
```

**Tests:**
- AWS KMS mock tests (no real API calls)
- Integration test with real AWS account (opt-in with `TEST_AWS_ENABLED=1`)
- CloudTrail audit log verification

#### 2.2 Azure Key Vault Provider
**Files to Create:**
- `pkg/kms/providers/azure_keyvault.go` — Azure SDK integration
- `pkg/kms/providers/azure_keyvault_test.go`

**Configuration:**
```yaml
encryption:
  provider: "azure-keyvault"
  azure_keyvault:
    vault_uri: "https://nornicdb-kv.vault.azure.net/"
    key_name: "nornicdb-master-key"
    key_version: "abc123def456"  # Optional; latest if omitted
    
    # Managed identity or service principal
    auth_method: "managed-identity"  # or "service-principal"
    client_id: "${AZURE_CLIENT_ID}"
    client_secret: "${AZURE_CLIENT_SECRET}"
    
    # FIPS 140-2 Level 2+
    enforce_fips: true
    
    # Compliance features
    enable_audit_logging: true
    audit_storage_account: "nornicdbaudit"
    rotation_interval: "90d"
```

#### 2.3 GCP Cloud KMS Provider
**Files to Create:**
- `pkg/kms/providers/gcp_cloudkms.go`
- `pkg/kms/providers/gcp_cloudkms_test.go`

**Configuration:**
```yaml
encryption:
  provider: "gcp-cloudkms"
  gcp_cloudkms:
    project: "my-project"
    location: "us-central1"
    key_ring: "nornicdb-keys"
    key_name: "master-key"
    
    # Service account for auth
    credentials_file: "/secrets/gcp-sa-key.json"
    
    # FIPS 140-2
    enforce_fips: true
    
    # Compliance
    enable_cloud_audit_logs: true
    rotation_interval: "90d"
```

### Phase 3: Envelope Encryption Integration (Weeks 9-10)

#### 3.1 Wire Envelope Into Storage Layer
**Files to Modify:**
- `pkg/nornicdb/db.go` — Initialize envelope encryptor at startup
- `pkg/storage/badger_engine.go` — Remove old single-key model; use envelope

**Startup Flow:**
```go
// cmd/nornicdb/main.go (additions)
func initializeEncryption(cfg *config.Config) (*encryption.EnvelopeEncryptor, error) {
    // 1. Resolve KMS provider from config
    kmProvider, err := kms.NewProvider(cfg.Encryption.Provider, cfg.Encryption.ProviderConfig)
    if err != nil {
        return nil, fmt.Errorf("failed to initialize KMS: %w", err)
    }
    
    // 2. Validate KMS connectivity (permissions, key access)
    if err := kmProvider.TestConnectivity(context.Background()); err != nil {
        return nil, fmt.Errorf("KMS connectivity check failed: %w", err)
    }
    
    // 3. Initialize envelope encryptor
    enc := encryption.NewEnvelopeEncryptor(kmProvider, cfg.Encryption.EnvelopeOpts)
    
    // 4. Wire to Badger (no static key anymore)
    badgerOpts.EncryptionProvider = enc
    
    return enc, nil
}
```

#### 3.2 Data Key Caching Strategy
**Design:**
- **Cache TTL:** 24 hours (configurable)
- **Max cached keys:** 10 (configurable)
- **On expiry:** Automatically request new DEK from KMS

```go
// pkg/encryption/dek_cache.go (new 120 lines)

type DEKCache struct {
    cache    *lru.Cache              // LRU cache of data keys
    mu       sync.RWMutex
    maxAge   time.Duration
    provider kms.KeyProvider
}

// GetOrGenerate returns cached DEK or generates new one
func (c *DEKCache) GetOrGenerate(ctx context.Context) (*kms.DataKey, error) {
    c.mu.RLock()
    if cached := c.cache.Get("current"); cached != nil {
        dek := cached.(*kms.DataKey)
        if time.Since(dek.CreatedAt) < c.maxAge {
            c.mu.RUnlock()
            return dek, nil
        }
    }
    c.mu.RUnlock()
    
    // Generate new DEK
    dek, err := c.provider.GenerateDataKey(ctx, kms.KeyGenOpts{
        Algorithm: "AES-256",
        TTL:       c.maxAge,
    })
    if err != nil {
        return nil, err
    }
    
    // Cache it
    c.mu.Lock()
    c.cache.Add("current", dek)
    c.mu.Unlock()
    
    return dek, nil
}
```

### Phase 4: Key Rotation Orchestration (Weeks 11-12)

#### 4.1 Automatic Key Rotation
**Files to Create:**
- `pkg/encryption/rotation.go` — Rotation orchestration

**Features:**
- Quarterly automatic rotation (configurable)
- Background goroutine that monitors key age
- Re-encrypt existing encrypted values with new DEK
- Audit trail for each rotation

**Configuration:**
```yaml
encryption:
  rotation:
    enabled: true
    interval: "90d"
    max_concurrent_keys: 3
    retention_count: 5  # Keep last 5 old keys for decryption
```

**Implementation:**
```go
// pkg/encryption/rotation.go (200 lines)

type RotationManager struct {
    provider kms.KeyProvider
    storage  storage.Engine
    ticker   *time.Ticker
    config   RotationConfig
}

// Start begins background rotation loop
func (rm *RotationManager) Start(ctx context.Context) {
    go rm.rotationLoop(ctx)
}

func (rm *RotationManager) rotationLoop(ctx context.Context) {
    for {
        select {
        case <-rm.ticker.C:
            rm.performRotation(ctx)
        case <-ctx.Done():
            return
        }
    }
}

// performRotation re-encrypts old data with new DEK
func (rm *RotationManager) performRotation(ctx context.Context) {
    // 1. Generate new DEK from KMS
    newDEK, err := rm.provider.GenerateDataKey(ctx, kms.KeyGenOpts{})
    // ... error handling ...
    
    // 2. Scan all encrypted nodes/edges
    nodes, _ := rm.storage.ListNodes(ctx)
    
    // 3. Re-encrypt each value
    for _, node := range nodes {
        newEncrypted, err := rm.reEncrypt(ctx, node.EncryptedData, newDEK)
        if err != nil {
            rm.auditRotationFailure(ctx, node.ID, err)
            continue
        }
        
        // 4. Update storage
        node.EncryptedData = newEncrypted
        _ = rm.storage.UpdateNode(ctx, node)
    }
    
    // 5. Audit rotation completion
    rm.auditRotationSuccess(ctx, len(nodes))
}
```

**Tests:**
- Rotation trigger validation
- Concurrent rotation safety
- Old key retention rules
- Audit event generation

### Phase 5: Compliance & Audit (Weeks 13-14)

#### 5.1 Audit Event Archival
**Files to Create:**
- `pkg/kms/audit_archiver.go` — Immutable audit log export

**Features:**
- Signed audit events (HMAC-SHA256 over operations)
- Immutable storage (write-once)
- Export to CloudTrail, Azure Activity Log, GCP Cloud Audit Logs
- Compliance report generation (SOC2, HIPAA evidence)

**Configuration:**
```yaml
encryption:
  audit:
    enabled: true
    # Local immutable log + cloud export
    local_log_path: "/var/log/nornicdb/audit.jsonl"
    local_log_retention: "2555d"  # 7 years (HIPAA requirement)
    
    # Export to cloud
    cloud_export:
      enabled: true
      destination: "s3://audit-bucket/"
      format: "cloudtrail"  # or "azure-activity", "gcp-cloud-audit"
      
    # Integrity checking
    sign_events: true
    hmac_key_rotation: "90d"
```

#### 5.2 Compliance Reporting
**Files to Create:**
- `pkg/compliance/report.go` — Evidence export for auditors

**Capabilities:**
```go
type ComplianceReporter struct {
    auditLog   *audit.Log
    kmProvider kms.KeyProvider
}

// ExportHIPAAEvidence generates evidence for HIPAA audit
func (cr *ComplianceReporter) ExportHIPAAEvidence(startDate, endDate time.Time) (*HIPAAReport, error) {
    // Collect evidence:
    // - Encryption enabled verification
    // - Key rotation history
    // - Failed decrypt attempts (anomaly detection)
    // - Access control audit trail
    // - Incident response timeline
}

// ExportSOC2Evidence generates SOC2 Type II evidence
func (cr *ComplianceReporter) ExportSOC2Evidence(period string) (*SOC2Report, error) {
    // Evidence for control areas:
    // - CC6.1: Logical access controls
    // - CC6.2: Encryption management
    // - CC7.2: System monitoring
}
```

### Phase 6: Documentation & Testing (Weeks 15-16)

#### 6.1 Configuration Documentation
**Files to Create:**
- `docs/encryption/cmek-setup.md` — CMEK deployment guide
- `docs/encryption/hsm-integration.md` — HSM configuration
- `docs/encryption/compliance-evidence.md` — Audit evidence generation

**Contents:**
- AWS KMS setup (role policies, key permissions)
- Azure Key Vault setup (RBAC, soft-delete recovery)
- GCP Cloud KMS setup (service account roles)
- Thales HSM network configuration
- Audit log export to SIEM (Splunk, ELK, DataDog)

#### 6.2 Testing Matrix
**Comprehensive Tests (500+ test cases):**

| Category | Scenarios | Count |
|----------|-----------|-------|
| **Unit** | Envelope format, DEK cache, rotation logic | 150 |
| **Integration** | KMS provider roundtrips (mock) | 120 |
| **E2E** | Full encryption pipeline with storage | 100 |
| **Compliance** | Audit trail, key rotation, immutability | 80 |
| **Failure Modes** | KMS unavailable, key expire, rotation conflict | 60 |

---

## Configuration Examples

### AWS KMS (Production)
```yaml
encryption:
  provider: "aws-kms"
  enabled: true
  
  aws_kms:
    region: "us-east-1"
    key_id: "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
    role_arn: "arn:aws:iam::123456789012:role/nornicdb-encryption"
    
    # FIPS 140-2 Level 2+
    enforce_fips: true
    endpoint: "https://kms-fips.us-east-1.amazonaws.com"
    
    # Audit trail
    enable_cloudtrail: true
    cloudtrail_bucket: "s3://audit-logs/nornicdb/"
    
  # Key management
  rotation:
    enabled: true
    interval: "90d"
    retention_count: 5
    
  # Envelope encryption specifics
  envelope:
    dek_cache_ttl: "24h"
    max_cached_deks: 10
    algorithm: "AES-256-GCM"
    
  # Audit logging
  audit:
    enabled: true
    local_path: "/var/log/nornicdb/encryption-audit.jsonl"
    retention: "2555d"
    sign_events: true
```

### Azure Key Vault (Production)
```yaml
encryption:
  provider: "azure-keyvault"
  enabled: true
  
  azure_keyvault:
    vault_uri: "https://nornicdb-prod-kv.vault.azure.net/"
    key_name: "nornicdb-de-key"
    
    # Managed identity authentication
    auth_method: "managed-identity"
    
    # FIPS 140-2
    enforce_fips: true
    
    # Features
    enable_soft_delete: true
    soft_delete_days: 90
    enable_purge_protection: true
    
  rotation:
    enabled: true
    interval: "90d"
    retention_count: 5
    
  audit:
    enabled: true
    storage_account: "nornicdbaudit"
    container: "encryption-logs"
    retention: "2555d"
```

### Thales HSM (High-Security)
```yaml
encryption:
  provider: "thales-hsm"
  enabled: true
  
  thales_hsm:
    # Network HSM configuration
    servers:
      - "hsm-1.internal.company.com:9000"
      - "hsm-2.internal.company.com:9000"
    
    # Authentication
    username: "nornicdb-user"
    password_env: "NORNICDB_HSM_PASSWORD"  # From secret manager
    
    # FIPS 140-2 Level 3
    fips_mode: true
    
    # Key configuration
    key_identity: "NORNICDB-MASTER-2026"
    key_backup: true
    backup_frequency: "daily"
    backup_location: "/secure/backups/"
    
  rotation:
    enabled: true
    interval: "90d"
    require_operator_approval: true
    
  audit:
    enabled: true
    hsm_audit_log: true
    export_to_siem: "splunk"
    siem_endpoint: "https://splunk.internal:8088/services/collector"
```

---

## Migration Strategy (Zero-Downtime)

### Phase 1: Dual-Write (Week 1)
- Keep existing single-key encryption active
- New data encrypted with envelope model
- Old data readable by both paths

### Phase 2: Background Re-Encryption (Weeks 2-4)
- Background job re-encrypts old data with envelope
- No performance impact (off-peak)
- Audit trail for each re-encryption

### Phase 3: Cutover (Week 5)
- Disable legacy encryption path
- All reads use envelope decryption
- Rollback available via backup restoration

---

## Test Coverage Matrix

### Unit Tests (pkg/kms, pkg/encryption)
```
├── KMS Provider Interface
│   ├── AWS KMS mock
│   ├── Azure Key Vault mock
│   ├── GCP Cloud KMS mock
│   ├── Error handling (timeout, quota, permission denied)
│   └── Concurrent access safety
├── Envelope Encryption
│   ├── Encryption/decryption roundtrip
│   ├── DEK cache expiry
│   ├── Corrupted envelope handling
│   ├── IV nonce collision prevention
│   └── Plaintext disposal (secure wipe)
├── Key Rotation
│   ├── Rotation trigger timing
│   ├── Concurrent rotation conflicts
│   ├── Old key retention
│   └── Immutability of rotated keys
└── Audit
    ├── Event signing (HMAC)
    ├── Event immutability
    ├── Timestamp accuracy
    └── Principal identity tracking
```

### Integration Tests (with mock KMS)
```
├── KMS Provider Integration
│   ├── AWS KMS roundtrip (mock SDK)
│   ├── Azure Key Vault roundtrip (mock SDK)
│   ├── GCP Cloud KMS roundtrip (mock SDK)
│   ├── Service principal auth
│   └── IAM policy validation
├── Storage Integration
│   ├── Badger engine with envelope
│   ├── Node/edge encryption
│   ├── Query on encrypted data (no decryption in DB)
│   └── Transaction consistency
└── Compliance
    ├── Audit trail completeness
    ├── Key version tracking
    └── Rotation history
```

### End-to-End Tests (optional real KMS)
```
├── AWS KMS (requires TEST_AWS_ENABLED=1)
│   ├── Real credential from env
│   ├── Encrypt/decrypt with real KMS
│   ├── CloudTrail audit verification
│   └── Cost validation (minimize KMS calls)
├── Azure Key Vault (requires TEST_AZURE_ENABLED=1)
├── GCP Cloud KMS (requires TEST_GCP_ENABLED=1)
└── Compliance Report
    ├── HIPAA evidence export
    ├── SOC2 report generation
    └── Audit log forensics
```

---

## Success Criteria

### Technical
- ✅ All 500+ tests passing (unit + integration + E2E)
- ✅ Zero data loss during migration
- ✅ Sub-millisecond envelope overhead (<5% latency impact)
- ✅ KMS call caching reduces API calls by 95%
- ✅ Automated key rotation (quarterly, configurable)
- ✅ `go-kms-wrapping/v2` adapters pass provider contract suite

### Compliance
- ✅ Audit trail immutable + signed (HMAC-SHA256)
- ✅ FIPS 140-2 Level 2+ validation path clear
- ✅ SOC2 evidence automatically generated
- ✅ HIPAA compliance checklist completed
- ✅ Key rotation policy documented + enforced

### Operational
- ✅ Zero KMS provider lock-in (pluggable interface)
- ✅ Graceful degradation if KMS unavailable (cached DEK)
- ✅ Audit log retention configurable (default 7 years)
- ✅ Compliance report generation automated
- ✅ Operator runbook: setup, rotation, incident response

---

## Effort Estimate

| Phase | Duration | FTE | Deliverables |
|-------|----------|-----|--------------|
| 1. Infrastructure | 4w | 2 | KMS interface, envelope encryption |
| 2. Implementations | 4w | 2 | AWS/Azure/GCP providers |
| 3. Integration | 2w | 1.5 | Storage wiring, caching |
| 4. Rotation | 2w | 1 | Orchestration, re-encryption |
| 5. Compliance | 2w | 1.5 | Audit, reporting, evidence |
| 6. Testing/Docs | 2w | 1.5 | Test matrix, configuration guides |
| **Total** | **16 weeks** | **~10 FTE** | **Production-ready CMEK/HSM** |

---

## Neo4j Comparison Model

**How Neo4j Approaches Encryption (Enterprise):**

| Aspect | Neo4j Enterprise | NornicDB (Post-CMEK) |
|--------|------------------|----------------------|
| **Provider** | Enterprise-only feature | Open-source + enterprise CMEK |
| **Key Storage** | File-based + KMS integration | KMS/HSM exclusively |
| **Encryption Scope** | Data at rest + transport (TLS) | Data at rest (envelope) + transport |
| **Key Rotation** | Manual + scheduled | Automatic + operator-controlled |
| **Multi-KMS** | Limited | AWS/Azure/GCP/Thales/Vault |
| **Audit Trail** | Neo4j logs | Immutable, signed, SIEM-exportable |
| **FIPS Compliance Path** | Via FIPS edition | Native FIPS-140-2 Level 2+ |

**Key Differentiator:**
> NornicDB CMEK is **provider-agnostic** (AWS/Azure/GCP equally first-class) vs. Neo4j's tighter integration with specific KMS services.

---

## Next Steps

1. **Week 1:** Review + approve architecture (envelope model, provider interface)
2. **Week 2:** Begin Phase 1.1 implementation (KMS abstraction layer)
3. **Week 8:** First cloud provider (AWS KMS) production-ready
4. **Week 16:** Full compliance validation + SOC2 attestation

---

**Owner:** Security & Compliance  
**Last Updated:** April 2, 2026  
**Status:** Approved for Implementation
