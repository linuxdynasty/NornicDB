# Retention Policies

NornicDB supports runtime retention policy enforcement, but it is disabled by default and must be explicitly enabled.

## Default Behavior

By default:

- `compliance.retention_enabled` is `false`
- no retention manager is created
- no retention sweep background worker starts
- no retention policy file is saved on shutdown
- retention admin endpoints return `503 Service Unavailable`

This keeps retention fully opt-in for deployments that do not want automatic lifecycle enforcement.

## Enable Retention

YAML:

```yaml
compliance:
  retention_enabled: true
  retention_policy_days: 30
  retention_auto_delete: false

retention:
  sweep_interval: 3600
  default_policies: false
  excluded_labels: ["AuditLog", "System"]
  max_sweep_records: 50000
```

`sweep_interval` is an integer number of seconds. Shorthand duration forms such as `"1h"` and `"30m"` are not accepted.

Environment variables:

```bash
export NORNICDB_RETENTION_ENABLED=true
export NORNICDB_RETENTION_POLICY_DAYS=30
export NORNICDB_RETENTION_AUTO_DELETE=false
```

## Behavior When Enabled

When enabled, NornicDB:

- creates a retention manager during DB startup
- loads persisted policies and optional built-in defaults
- starts a cancellable retention sweep worker tied to DB shutdown
- persists retention policy state on shutdown
- exposes retention admin endpoints for policies, legal holds, erasure requests, manual sweeps, and status

## Retention Controls

Use `retention.excluded_labels` to exempt labels globally. Exclusions win over delete decisions during sweep processing.

Use `retention.default_policies` to load built-in compliance policies at startup.

Use `retention.policies` for inline custom policies when you need category-specific durations or archive paths.

## GDPR Integration

Retention integrates with GDPR delete handling:

- legal holds block erasure
- erasure requests are tracked through the retention manager
- subject matching is configurable through compliance subject-selector settings

## Admin API Availability

Retention admin endpoints are mounted with the rest of the server routes, but they only become operational when retention is enabled. If retention is disabled, they return `503 Service Unavailable` instead of pretending retention is configured.

See [Configuration](../operations/configuration.md#retention-policies-opt-in) and [Environment Variables](../operations/environment-variables.md).