# kvidxkit Deployment Guide

This guide covers production deployment considerations for kvidxkit.

---

## Table of Contents

1. [Pre-Deployment Checklist](#pre-deployment-checklist)
2. [Build Configuration](#build-configuration)
3. [Backend Selection](#backend-selection)
4. [Configuration Tuning](#configuration-tuning)
5. [File System Considerations](#file-system-considerations)
6. [Backup Strategies](#backup-strategies)
7. [Monitoring](#monitoring)
8. [High Availability](#high-availability)
9. [Security](#security)
10. [Docker Deployment](#docker-deployment)
11. [Capacity Planning](#capacity-planning)
12. [Maintenance Operations](#maintenance-operations)

---

## Pre-Deployment Checklist

Before deploying to production:

- [ ] Selected appropriate backend for workload
- [ ] Configured journal mode and sync settings
- [ ] Sized cache appropriately for dataset
- [ ] Set up backup strategy
- [ ] Configured monitoring
- [ ] Load tested with production-like data volume
- [ ] Verified error handling in application code
- [ ] Planned for capacity growth
- [ ] Documented recovery procedures

---

## Build Configuration

### Release Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DKVIDXKIT_ENABLE_SQLITE3=ON \
         -DKVIDXKIT_ENABLE_LMDB=ON
make -j4
```

### Compiler Optimization

The default Release build uses `-O2`. For maximum performance:

```cmake
# In CMakeLists.txt or via cmake
set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")
```

### Static Linking

For deployment without dependencies:

```bash
# Link against static library
gcc -o myapp myapp.c -I/path/to/src \
    -L/path/to/build/src -lkvidxkit-static \
    -lsqlite3 -lpthread -ldl
```

---

## Backend Selection

### Decision Matrix

| Factor           | SQLite3      | LMDB              | RocksDB     |
| ---------------- | ------------ | ----------------- | ----------- |
| Read/Write Ratio | Balanced     | Read-heavy (90%+) | Write-heavy |
| Dataset Size     | < 1 TB       | < RAM \* 2        | Multi-TB    |
| Concurrency      | Good (WAL)   | Excellent         | Excellent   |
| Configuration    | Rich         | Minimal           | Moderate    |
| Deployment       | Single file  | Directory         | Directory   |
| Memory Usage     | Configurable | Fixed map         | Dynamic     |

### SQLite3 Production Settings

```c
kvidxConfig config = kvidxConfigDefault();
config.journalMode = KVIDX_JOURNAL_WAL;    // Required for concurrency
config.syncMode = KVIDX_SYNC_NORMAL;       // Balanced durability
config.cacheSizeBytes = 256 * 1024 * 1024; // 256 MB cache
config.busyTimeoutMs = 30000;              // 30 second lock timeout
config.mmapSizeBytes = 1024 * 1024 * 1024; // 1 GB mmap (optional)
```

### LMDB Production Settings

```c
kvidxConfig config = kvidxConfigDefault();
config.mmapSizeBytes = 10ULL * 1024 * 1024 * 1024;  // 10 GB map size
config.syncMode = KVIDX_SYNC_NORMAL;
```

**LMDB Map Size:**

- Set 2-3x expected maximum database size
- Can be increased later (requires restart)
- Does not consume RAM until used

### RocksDB Production Settings

```c
kvidxConfig config = kvidxConfigDefault();
config.syncMode = KVIDX_SYNC_NORMAL;  // Async writes for performance
```

---

## Configuration Tuning

### Durability vs. Performance

| Setting                       | Durability | Performance |
| ----------------------------- | ---------- | ----------- |
| `SYNC_FULL` + `JOURNAL_WAL`   | Maximum    | Lower       |
| `SYNC_NORMAL` + `JOURNAL_WAL` | High       | Good        |
| `SYNC_OFF` + `JOURNAL_WAL`    | Medium     | High        |
| `SYNC_OFF` + `JOURNAL_OFF`    | None       | Maximum     |

**Recommended for most production:**

```c
config.journalMode = KVIDX_JOURNAL_WAL;
config.syncMode = KVIDX_SYNC_NORMAL;
```

### Cache Sizing

Rule of thumb: 10-20% of hot dataset

| Dataset Size | Recommended Cache |
| ------------ | ----------------- |
| 100 MB       | 32 MB (default)   |
| 1 GB         | 128-256 MB        |
| 10 GB        | 512 MB - 1 GB     |
| 100 GB       | 2-4 GB            |

```c
config.cacheSizeBytes = 512 * 1024 * 1024;  // 512 MB
```

### Busy Timeout

For high-contention environments:

```c
config.busyTimeoutMs = 60000;  // 60 seconds
```

---

## File System Considerations

### Recommended File Systems

| OS      | Recommended | Notes                              |
| ------- | ----------- | ---------------------------------- |
| Linux   | ext4, XFS   | Use `noatime` mount option         |
| macOS   | APFS        | Default, works well                |
| Windows | NTFS        | Disable indexing on data directory |

### Mount Options (Linux)

```bash
# /etc/fstab entry for database volume
/dev/sdb1 /data ext4 defaults,noatime,data=ordered 0 2
```

### Directory Structure

```
/var/lib/myapp/
├── data/
│   ├── main.db           # SQLite3 database
│   ├── main.db-wal       # Write-ahead log
│   └── main.db-shm       # Shared memory
├── backups/
│   ├── daily/
│   └── weekly/
└── logs/
```

### Permissions

```bash
# Create dedicated user
useradd -r -s /bin/false kvidx

# Set permissions
chown -R kvidx:kvidx /var/lib/myapp/data
chmod 700 /var/lib/myapp/data
```

### Disk Performance

Monitor I/O:

```bash
# Linux
iostat -x 1

# Check for I/O wait
vmstat 1
```

---

## Backup Strategies

### Strategy 1: Hot Backup with Export

```c
// In application code
void daily_backup(kvidxInstance *db) {
    char filename[256];
    time_t now = time(NULL);
    strftime(filename, sizeof(filename),
             "/backups/daily/backup_%Y%m%d_%H%M%S.bin",
             localtime(&now));

    kvidxExportOptions opts = {
        .format = KVIDX_EXPORT_BINARY
    };

    kvidxError err = kvidxExport(db, filename, &opts, NULL, NULL);
    if (err != KVIDX_OK) {
        log_error("Backup failed: %s", kvidxErrorString(err));
    }
}
```

### Strategy 2: File System Snapshots

For SQLite3 with WAL mode:

```bash
#!/bin/bash
# Checkpoint and snapshot

# Force checkpoint
sqlite3 /data/main.db "PRAGMA wal_checkpoint(TRUNCATE);"

# Create snapshot (LVM example)
lvcreate -s -n db_snapshot -L 10G /dev/vg0/data

# Mount and copy
mount /dev/vg0/db_snapshot /mnt/snapshot
cp -r /mnt/snapshot/data /backups/$(date +%Y%m%d)/
umount /mnt/snapshot
lvremove -f /dev/vg0/db_snapshot
```

### Strategy 3: Incremental Backup

```c
typedef struct {
    uint64_t lastKey;
    char lastBackupFile[256];
} BackupState;

void incremental_backup(kvidxInstance *db, BackupState *state) {
    uint64_t currentMax;
    if (!kvidxMaxKey(db, &currentMax)) {
        return;  // Empty
    }

    if (currentMax <= state->lastKey) {
        return;  // No new data
    }

    char filename[256];
    snprintf(filename, sizeof(filename),
             "/backups/incremental/incr_%lu_%lu.bin",
             state->lastKey + 1, currentMax);

    kvidxExportOptions opts = {
        .format = KVIDX_EXPORT_BINARY,
        .startKey = state->lastKey + 1,
        .endKey = currentMax
    };

    if (kvidxExport(db, filename, &opts, NULL, NULL) == KVIDX_OK) {
        state->lastKey = currentMax;
        strcpy(state->lastBackupFile, filename);
    }
}
```

### Backup Rotation

```bash
#!/bin/bash
# Keep 7 daily, 4 weekly, 12 monthly backups

# Daily cleanup
find /backups/daily -mtime +7 -delete

# Weekly (keep Sundays)
find /backups/weekly -mtime +28 -delete

# Monthly (keep 1st of month)
find /backups/monthly -mtime +365 -delete
```

### Backup Verification

```c
bool verify_backup(const char *backupPath) {
    kvidxInstance testInst = {0};
    testInst.interface = kvidxInterfaceSqlite3;

    if (!kvidxOpen(&testInst, ":memory:", NULL)) {
        return false;
    }

    kvidxImportOptions opts = {
        .format = KVIDX_EXPORT_BINARY,
        .validateData = true
    };

    kvidxError err = kvidxImport(&testInst, backupPath, &opts, NULL, NULL);
    kvidxClose(&testInst);

    return err == KVIDX_OK;
}
```

---

## Monitoring

### Key Metrics to Track

| Metric             | Source                   | Alert Threshold     |
| ------------------ | ------------------------ | ------------------- |
| Key count          | `kvidxGetKeyCount()`     | Growth rate         |
| Data size          | `kvidxGetDataSize()`     | Disk usage %        |
| Database file size | `stats.databaseFileSize` | Disk capacity       |
| Free pages         | `stats.freePages`        | Fragmentation > 20% |
| WAL file size      | `stats.walFileSize`      | > 100 MB            |
| Error rate         | Application logs         | > 0.1%              |
| Latency p99        | Application metrics      | > 100ms             |

### Monitoring Function

```c
typedef struct {
    uint64_t keyCount;
    uint64_t dataSizeBytes;
    uint64_t dbFileSizeBytes;
    uint64_t walFileSizeBytes;
    uint64_t freePages;
    double fragmentationRatio;
} MonitoringMetrics;

void collect_metrics(kvidxInstance *db, MonitoringMetrics *metrics) {
    kvidxStats stats;
    if (kvidxGetStats(db, &stats) == KVIDX_OK) {
        metrics->keyCount = stats.totalKeys;
        metrics->dataSizeBytes = stats.totalDataBytes;
        metrics->dbFileSizeBytes = stats.databaseFileSize;
        metrics->walFileSizeBytes = stats.walFileSize;
        metrics->freePages = stats.freePages;

        if (stats.pageCount > 0) {
            metrics->fragmentationRatio =
                (double)stats.freePages / stats.pageCount;
        }
    }
}

void log_metrics(MonitoringMetrics *metrics) {
    printf("METRICS: keys=%lu data=%luMB file=%luMB wal=%luMB frag=%.2f%%\n",
           metrics->keyCount,
           metrics->dataSizeBytes / (1024 * 1024),
           metrics->dbFileSizeBytes / (1024 * 1024),
           metrics->walFileSizeBytes / (1024 * 1024),
           metrics->fragmentationRatio * 100);
}
```

### Prometheus Integration Example

```c
// HTTP endpoint for Prometheus scraping
void metrics_handler(struct http_request *req, struct http_response *resp) {
    MonitoringMetrics m;
    collect_metrics(&global_db, &m);

    char body[1024];
    snprintf(body, sizeof(body),
        "# HELP kvidx_keys Total number of keys\n"
        "# TYPE kvidx_keys gauge\n"
        "kvidx_keys %lu\n"
        "# HELP kvidx_data_bytes Total data size in bytes\n"
        "# TYPE kvidx_data_bytes gauge\n"
        "kvidx_data_bytes %lu\n"
        "# HELP kvidx_db_file_bytes Database file size\n"
        "# TYPE kvidx_db_file_bytes gauge\n"
        "kvidx_db_file_bytes %lu\n"
        "# HELP kvidx_fragmentation_ratio Free pages ratio\n"
        "# TYPE kvidx_fragmentation_ratio gauge\n"
        "kvidx_fragmentation_ratio %.4f\n",
        m.keyCount, m.dataSizeBytes, m.dbFileSizeBytes,
        m.fragmentationRatio);

    resp->body = body;
    resp->status = 200;
}
```

### Health Check

```c
typedef enum {
    HEALTH_OK,
    HEALTH_DEGRADED,
    HEALTH_UNHEALTHY
} HealthStatus;

HealthStatus check_health(kvidxInstance *db) {
    // Test basic operation
    kvidxError err = kvidxGetKeyCount(db, NULL);
    if (err != KVIDX_OK) {
        return HEALTH_UNHEALTHY;
    }

    // Check fragmentation
    kvidxStats stats;
    if (kvidxGetStats(db, &stats) == KVIDX_OK) {
        if (stats.pageCount > 0) {
            double frag = (double)stats.freePages / stats.pageCount;
            if (frag > 0.3) {
                return HEALTH_DEGRADED;  // High fragmentation
            }
        }
    }

    return HEALTH_OK;
}
```

---

## High Availability

kvidxkit is a single-node library. For HA, use external replication:

### Option 1: File System Replication

```bash
# DRBD (Linux)
drbdadm primary mydb
mount /dev/drbd0 /data

# On failover
drbdadm secondary mydb  # On old primary
drbdadm primary mydb    # On new primary
```

### Option 2: Export-Based Replication

```c
// Primary: Export periodically
void replicate_to_secondary(kvidxInstance *db, const char *secondaryPath) {
    kvidxExportOptions opts = { .format = KVIDX_EXPORT_BINARY };
    kvidxExport(db, "/tmp/replication.bin", &opts, NULL, NULL);

    // Transfer file to secondary (rsync, scp, etc.)
    system("rsync -az /tmp/replication.bin secondary:/tmp/");

    // Secondary applies
    // kvidxImport(secondaryDb, "/tmp/replication.bin", ...);
}
```

### Option 3: Application-Level Replication

```c
// Dual-write to primary and secondary
bool replicated_insert(kvidxInstance *primary, kvidxInstance *secondary,
                       uint64_t key, uint64_t term, uint64_t cmd,
                       const void *data, size_t len) {
    // Write to primary first
    kvidxBegin(primary);
    if (!kvidxInsert(primary, key, term, cmd, data, len)) {
        kvidxAbort(primary);
        return false;
    }
    kvidxCommit(primary);

    // Async replicate to secondary (in background thread)
    async_replicate(secondary, key, term, cmd, data, len);

    return true;
}
```

---

## Security

### Access Control

kvidxkit doesn't have built-in authentication. Secure at OS/network level:

```bash
# File permissions
chmod 600 /data/main.db*
chown appuser:appgroup /data/main.db*

# Firewall (if exposing via network)
iptables -A INPUT -p tcp --dport 8080 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 8080 -j DROP
```

### Encryption at Rest

**Option 1: SQLite Encryption Extension (SEE)**

- Commercial SQLite extension
- Transparent encryption

**Option 2: File System Encryption**

```bash
# Linux LUKS
cryptsetup luksFormat /dev/sdb1
cryptsetup luksOpen /dev/sdb1 data_encrypted
mkfs.ext4 /dev/mapper/data_encrypted
mount /dev/mapper/data_encrypted /data
```

**Option 3: Application-Level Encryption**

```c
// Encrypt before storing
void secure_insert(kvidxInstance *db, uint64_t key,
                   const void *plaintext, size_t len,
                   const uint8_t *encryptionKey) {
    // Encrypt data
    size_t cipherLen = len + 16;  // + auth tag
    uint8_t *ciphertext = malloc(cipherLen);
    encrypt_aes_gcm(plaintext, len, encryptionKey, ciphertext, &cipherLen);

    kvidxBegin(db);
    kvidxInsert(db, key, 0, 0, ciphertext, cipherLen);
    kvidxCommit(db);

    free(ciphertext);
}
```

### Audit Logging

```c
void audited_operation(kvidxInstance *db, const char *operation,
                       uint64_t key, const char *userId) {
    // Log before operation
    log_audit("START %s key=%lu user=%s", operation, key, userId);

    // Perform operation
    // ...

    // Log after
    log_audit("END %s key=%lu user=%s result=%s",
              operation, key, userId, success ? "OK" : "FAIL");
}
```

---

## Docker Deployment

### Dockerfile

```dockerfile
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libsqlite3-0 \
    liblmdb0 \
    && rm -rf /var/lib/apt/lists/*

# Copy application and library
COPY build/myapp /usr/local/bin/
COPY build/src/libkvidxkit.so* /usr/local/lib/

# Update library cache
RUN ldconfig

# Create data directory
RUN mkdir -p /data && chown 1000:1000 /data
VOLUME /data

# Run as non-root
USER 1000:1000

ENTRYPOINT ["/usr/local/bin/myapp"]
CMD ["--data-dir=/data"]
```

### Docker Compose

```yaml
version: "3.8"

services:
  app:
    build: .
    volumes:
      - db-data:/data
    environment:
      - KVIDX_CACHE_SIZE=268435456 # 256 MB
      - KVIDX_SYNC_MODE=normal
    deploy:
      resources:
        limits:
          memory: 1G
        reservations:
          memory: 512M
    healthcheck:
      test: ["CMD", "/usr/local/bin/myapp", "--health-check"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  db-data:
    driver: local
```

### Kubernetes StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kvidx-app
spec:
  serviceName: kvidx-app
  replicas: 1 # Single writer
  selector:
    matchLabels:
      app: kvidx-app
  template:
    metadata:
      labels:
        app: kvidx-app
    spec:
      containers:
        - name: app
          image: myapp:latest
          volumeMounts:
            - name: data
              mountPath: /data
          resources:
            limits:
              memory: 1Gi
            requests:
              memory: 512Mi
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /ready
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: fast-ssd
        resources:
          requests:
            storage: 100Gi
```

---

## Capacity Planning

### Estimating Storage

```
Storage = (key_count * avg_value_size) * overhead_factor

Where:
- overhead_factor ≈ 1.5 for SQLite3
- overhead_factor ≈ 1.2 for LMDB
- overhead_factor ≈ 1.3 for RocksDB (before compaction)
```

### Growth Monitoring

```c
typedef struct {
    time_t timestamp;
    uint64_t keyCount;
    uint64_t dataSize;
} GrowthSample;

void record_growth(kvidxInstance *db, GrowthSample *samples,
                   size_t *sampleCount, size_t maxSamples) {
    kvidxStats stats;
    if (kvidxGetStats(db, &stats) != KVIDX_OK) return;

    if (*sampleCount < maxSamples) {
        samples[*sampleCount].timestamp = time(NULL);
        samples[*sampleCount].keyCount = stats.totalKeys;
        samples[*sampleCount].dataSize = stats.totalDataBytes;
        (*sampleCount)++;
    }
}

double estimate_days_until_full(GrowthSample *samples, size_t count,
                                 uint64_t maxCapacity) {
    if (count < 2) return -1;

    // Calculate growth rate
    time_t timeDiff = samples[count-1].timestamp - samples[0].timestamp;
    uint64_t sizeDiff = samples[count-1].dataSize - samples[0].dataSize;

    if (timeDiff == 0 || sizeDiff == 0) return -1;

    double bytesPerSecond = (double)sizeDiff / timeDiff;
    uint64_t remaining = maxCapacity - samples[count-1].dataSize;

    return (double)remaining / bytesPerSecond / 86400;
}
```

### Scaling Considerations

| Scale Factor    | Recommendation                      |
| --------------- | ----------------------------------- |
| 10x data        | Increase cache, consider RocksDB    |
| 10x throughput  | Use batch operations, async writes  |
| 10x concurrency | Use WAL mode, increase timeout      |
| Multi-region    | Consider application-level sharding |

---

## Maintenance Operations

### WAL Checkpoint (SQLite3)

```bash
# Force checkpoint to reclaim WAL space
sqlite3 /data/main.db "PRAGMA wal_checkpoint(TRUNCATE);"
```

### Vacuum (SQLite3)

```bash
# Reclaim space from deleted records (offline operation)
sqlite3 /data/main.db "VACUUM;"
```

### Compact (RocksDB)

RocksDB compacts automatically, but can be triggered:

```c
// Via RocksDB C API if needed
rocksdb_compact_range(db, NULL, 0, NULL, 0);
```

### TTL Cleanup Schedule

```c
// Run periodically (e.g., every minute)
void scheduled_cleanup(kvidxInstance *db) {
    uint64_t expired = 0;
    kvidxExpireScan(db, 10000, &expired);  // Process up to 10K per run

    if (expired > 0) {
        log_info("Cleaned up %lu expired keys", expired);
    }
}
```

### Graceful Shutdown

```c
void graceful_shutdown(kvidxInstance *db) {
    // 1. Stop accepting new requests

    // 2. Wait for in-flight operations

    // 3. Final sync
    kvidxFsync(db);

    // 4. Close database
    kvidxClose(db);

    log_info("Database closed gracefully");
}
```

---

## Troubleshooting Production Issues

### Database Locked

```bash
# Check for processes holding locks
fuser /data/main.db*
lsof /data/main.db*

# Check for stale lock files (SQLite3 WAL)
ls -la /data/main.db-shm /data/main.db-wal
```

### Slow Performance

```c
// Enable timing
struct timespec start, end;
clock_gettime(CLOCK_MONOTONIC, &start);
kvidxSomeOperation(db, ...);
clock_gettime(CLOCK_MONOTONIC, &end);

double ms = (end.tv_sec - start.tv_sec) * 1000.0 +
            (end.tv_nsec - start.tv_nsec) / 1000000.0;
if (ms > 100) {
    log_warn("Slow operation: %.2fms", ms);
}
```

### Disk Full

```c
kvidxError err = kvidxInsert(db, key, term, cmd, data, len);
if (err == KVIDX_ERROR_DISK_FULL) {
    log_error("Disk full, cleaning up...");

    // Emergency cleanup
    kvidxExpireScan(db, 0, NULL);  // Remove all expired

    // Alert operations team
    alert("CRITICAL: Disk full on database server");
}
```

### Recovery from Corruption

```bash
# SQLite3 integrity check
sqlite3 /data/main.db "PRAGMA integrity_check;"

# If corrupted, try recovery
sqlite3 /data/main.db ".dump" > /tmp/recovery.sql
sqlite3 /data/main_new.db < /tmp/recovery.sql
mv /data/main.db /data/main.db.corrupted
mv /data/main_new.db /data/main.db
```
