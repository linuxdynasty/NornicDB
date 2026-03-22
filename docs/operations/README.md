# Operations Guide

**Deploy, monitor, and maintain NornicDB in production.**

## 📚 Documentation

- **[Configuration](configuration.md)** - YAML and runtime tuning options
- **[CLI Commands](cli-commands.md)** - Command-line interface for database management
- **[Deployment](deployment.md)** - Production deployment guide
- **[Docker](docker.md)** - Docker and Kubernetes
- **[Low Memory Mode](low-memory-mode.md)** - Run NornicDB with minimal RAM (Docker, Pi, VMs)
- **[Monitoring](monitoring.md)** - Metrics and alerting
- **[Environment Variables](environment-variables.md)** - Canonical `NORNICDB_*` inventory
- **[Backup & Restore](backup-restore.md)** - Data protection
- **[WAL Compaction](wal-compaction.md)** - Automatic disk space management
- **[Durability Configuration](durability.md)** - Data safety vs performance tuning
- **[Storage Serialization](storage-serialization.md)** - gob vs msgpack and migration
- **[Historical Reads & MVCC Retention](../user-guides/historical-reads-mvcc-retention.md)** - Snapshot history, pruning, and retention policy
- **[Scaling](scaling.md)** - Horizontal and vertical scaling
- **[Cluster Security](cluster-security.md)** - Authentication for clusters
- **[Troubleshooting](troubleshooting.md)** - Common issues and solutions

## 🚀 Quick Start

### Docker Deployment

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal:latest
```

[Complete Docker guide →](docker.md)

### Monitoring

```bash
# Prometheus metrics
curl http://localhost:9090/metrics

# Health check
curl http://localhost:7474/health
```

[Complete monitoring guide →](monitoring.md)

### Backup

```bash
# Backup database
nornicdb backup --output=backup-$(date +%Y%m%d).tar.gz

# Restore database
nornicdb restore --input=backup-20251201.tar.gz
```

[Complete backup guide →](backup-restore.md)

### WAL Compaction

NornicDB automatically manages WAL (Write-Ahead Log) size to prevent unbounded disk growth:

```go
// Enable automatic compaction (recommended)
wal.EnableAutoCompaction("/data/snapshots")

// Manual truncation after snapshot
wal.TruncateAfterSnapshot(snapshotSequence)
```

**Benefits:**

- 99%+ disk savings vs unbounded WAL
- 300x faster crash recovery
- Automatic hourly snapshots

[Complete WAL compaction guide →](wal-compaction.md)

## 📖 Operations Topics

### Deployment

- Docker deployment
- Kubernetes deployment
- Bare metal installation
- Cloud providers (AWS, GCP, Azure)

[Deployment guide →](deployment.md)

### Monitoring

- Prometheus metrics
- Grafana dashboards
- Health checks
- Log aggregation

[Monitoring guide →](monitoring.md)

### Scaling

- Read replicas
- Sharding
- Load balancing
- Resource optimization

[Scaling guide →](scaling.md)

## 🆘 Troubleshooting

Common issues and solutions:

- Connection problems
- Performance issues
- Memory errors
- GPU problems

[Troubleshooting guide →](troubleshooting.md)

---

**Deploy to production** → **[Deployment Guide](deployment.md)**
