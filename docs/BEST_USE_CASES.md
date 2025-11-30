# kvidxkit Best Use Cases

This document describes optimal use cases for kvidxkit and provides guidance on selecting the right storage backend.

## Overview

kvidxkit is a C library providing a unified interface for ordered key-value storage. It excels in scenarios requiring:

- **Ordered uint64 keys** for sequential access patterns
- **Multi-field entries** with term/command metadata
- **Backend flexibility** to swap storage engines without code changes
- **Transactional guarantees** with configurable durability

---

## Ideal Use Cases

### 1. Consensus Log Storage (Raft/Paxos)

kvidxkit's data model maps directly to consensus log requirements:

```
Entry: (index, term, command, data)
Maps to: (key, term, cmd, data)
```

**Why kvidxkit fits:**

- Ordered keys for log index
- Term field for Raft term tracking
- Efficient range operations for log truncation
- `CompareTermAndSwap` for conditional log updates

**Example Pattern:**

```c
// Append log entry at index
bool appendLogEntry(kvidxInstance *log, uint64_t index,
                    uint64_t term, uint64_t cmd, const void *data, size_t len) {
    // Only append if this term >= last entry's term
    return kvidxInsertEx(log, index, term, cmd, data, len,
                         KVIDX_SET_IF_NOT_EXISTS) == KVIDX_OK;
}

// Truncate log from index (inclusive)
void truncateFrom(kvidxInstance *log, uint64_t index) {
    kvidxRemoveAfterNInclusive(log, index);
}

// Get entries in range for replication
void getEntriesForReplication(kvidxInstance *log,
                              uint64_t start, uint64_t end) {
    kvidxIterator *it = kvidxIteratorCreate(log, start, end,
                                            KVIDX_ITER_FORWARD);
    // ... iterate and send
}
```

**Recommended Backend:** SQLite3 (WAL mode) or LMDB for persistence with crash recovery.

---

### 2. Time-Series Data Storage

Sequential uint64 keys work well for timestamp-based data:

```
Key: Unix timestamp in milliseconds
Value: Sensor reading, metric, or event data
```

**Why kvidxkit fits:**

- Natural ordering by time
- Efficient range queries for time windows
- Batch inserts for high-throughput ingestion
- Range deletes for data retention policies

**Example Pattern:**

```c
// Insert time-series point
void recordMetric(kvidxInstance *ts, const char *metric,
                  size_t len) {
    uint64_t timestamp = getCurrentTimeMs();
    kvidxInsert(ts, timestamp, 0, 0, metric, len);
}

// Query time range
void queryRange(kvidxInstance *ts, uint64_t startTime,
                uint64_t endTime) {
    kvidxIterator *it = kvidxIteratorCreate(ts, startTime, endTime,
                                            KVIDX_ITER_FORWARD);
    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        // Process data points
    }
    kvidxIteratorDestroy(it);
}

// Apply retention policy (delete data older than 30 days)
void applyRetention(kvidxInstance *ts) {
    uint64_t cutoff = getCurrentTimeMs() - (30ULL * 24 * 3600 * 1000);
    uint64_t deleted = 0;
    kvidxRemoveRange(ts, 0, cutoff, true, true, &deleted);
}
```

**Recommended Backend:** RocksDB for high write throughput, SQLite3 for moderate volumes with SQL query flexibility.

---

### 3. Session and Cache Storage

TTL support makes kvidxkit suitable for session management:

**Why kvidxkit fits:**

- Per-key TTL with millisecond precision
- Atomic operations for session updates
- `GetAndRemove` for session invalidation
- Configurable durability (fast writes vs. persistence)

**Example Pattern:**

```c
// Create session with 30-minute TTL
void createSession(kvidxInstance *sessions, uint64_t sessionId,
                   const char *userData, size_t len) {
    kvidxBegin(sessions);
    kvidxInsert(sessions, sessionId, 0, 0, userData, len);
    kvidxCommit(sessions);
    kvidxSetExpire(sessions, sessionId, 30 * 60 * 1000);  // 30 min
}

// Refresh session on activity
void touchSession(kvidxInstance *sessions, uint64_t sessionId) {
    kvidxSetExpire(sessions, sessionId, 30 * 60 * 1000);
}

// Periodic cleanup
void cleanupExpired(kvidxInstance *sessions) {
    uint64_t count = 0;
    kvidxExpireScan(sessions, 0, &count);  // Remove all expired
}
```

**Recommended Backend:** SQLite3 (`:memory:`) for pure cache, LMDB for persistent sessions with fast reads.

---

### 4. Configuration and State Management

Store application configuration with version tracking:

**Why kvidxkit fits:**

- Term field for version/revision tracking
- `CompareAndSwap` for optimistic locking
- Export/import for config backup and migration
- Transaction support for atomic updates

**Example Pattern:**

```c
// Update config only if version matches
bool updateConfig(kvidxInstance *cfg, uint64_t key,
                  uint64_t expectedVersion, const void *newValue, size_t len) {
    bool swapped = false;
    kvidxError err = kvidxCompareTermAndSwap(cfg, key,
        expectedVersion,           // Expected version
        expectedVersion + 1, 0,    // New version
        newValue, len,
        &swapped
    );
    return err == KVIDX_OK && swapped;
}

// Export config for backup
void backupConfig(kvidxInstance *cfg, const char *path) {
    kvidxExportOptions opts = {
        .format = KVIDX_EXPORT_JSON,
        .prettyPrint = true
    };
    kvidxExport(cfg, path, &opts, NULL, NULL);
}
```

**Recommended Backend:** SQLite3 for durability and easy inspection.

---

### 5. Event Sourcing and Audit Logs

Immutable event streams with sequential ordering:

**Why kvidxkit fits:**

- Append-only pattern with sequential keys
- Cmd field for event type classification
- Batch inserts for high event throughput
- Range queries for event replay

**Example Pattern:**

```c
typedef struct {
    uint64_t nextEventId;
    kvidxInstance db;
} EventStore;

uint64_t appendEvent(EventStore *store, uint64_t eventType,
                     const void *payload, size_t len) {
    uint64_t eventId = __atomic_fetch_add(&store->nextEventId, 1,
                                          __ATOMIC_RELAXED);
    kvidxBegin(&store->db);
    kvidxInsert(&store->db, eventId, 0, eventType, payload, len);
    kvidxCommit(&store->db);
    return eventId;
}

void replayEvents(EventStore *store, uint64_t fromId,
                  void (*handler)(uint64_t id, uint64_t type,
                                  const void *data, size_t len)) {
    kvidxIterator *it = kvidxIteratorCreate(&store->db, fromId,
                                            UINT64_MAX, KVIDX_ITER_FORWARD);
    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key, term, cmd;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, &term, &cmd, &data, &len);
        handler(key, cmd, data, len);
    }
    kvidxIteratorDestroy(it);
}
```

**Recommended Backend:** RocksDB for high-volume event streams, SQLite3 for audit logs with retention.

---

### 6. Message Queue / Job Queue

Use kvidxkit as a simple persistent queue:

**Why kvidxkit fits:**

- Ordered keys for FIFO ordering
- `GetAndRemove` for atomic dequeue
- TTL for message expiration
- Range operations for bulk cleanup

**Example Pattern:**

```c
typedef struct {
    uint64_t nextId;
    kvidxInstance db;
} MessageQueue;

// Enqueue message
uint64_t enqueue(MessageQueue *q, const void *msg, size_t len,
                 uint64_t ttlMs) {
    uint64_t id = __atomic_fetch_add(&q->nextId, 1, __ATOMIC_RELAXED);
    kvidxBegin(&q->db);
    kvidxInsert(&q->db, id, 0, 0, msg, len);
    kvidxCommit(&q->db);
    if (ttlMs > 0) {
        kvidxSetExpire(&q->db, id, ttlMs);
    }
    return id;
}

// Dequeue oldest message (atomic get and remove)
bool dequeue(MessageQueue *q, void **msg, size_t *len) {
    // Find minimum key
    uint64_t minKey;
    if (!kvidxGetMinKey(&q->db, &minKey)) {
        return false;  // Queue empty
    }

    uint64_t term, cmd;
    kvidxError err = kvidxGetAndRemove(&q->db, minKey,
                                       &term, &cmd, msg, len);
    return err == KVIDX_OK;
}

// Peek without removing
bool peek(MessageQueue *q, uint64_t *id, const void **msg, size_t *len) {
    uint64_t minKey;
    if (!kvidxGetMinKey(&q->db, &minKey)) {
        return false;
    }

    uint64_t term, cmd;
    if (kvidxGet(&q->db, minKey, &term, &cmd, (const uint8_t**)msg, len)) {
        *id = minKey;
        return true;
    }
    return false;
}
```

**Recommended Backend:** LMDB for fast dequeue, SQLite3 for durability.

---

### 7. Write-Ahead Log (WAL) Implementation

Custom WAL for other storage systems:

**Why kvidxkit fits:**

- Sequential log append pattern
- Term field for sequence numbers
- Efficient truncation with range deletes
- Configurable sync modes

**Example Pattern:**

```c
typedef struct {
    uint64_t lsn;  // Log Sequence Number
    kvidxInstance wal;
} WriteAheadLog;

// Write WAL entry
uint64_t walAppend(WriteAheadLog *wal, const void *entry, size_t len) {
    uint64_t lsn = __atomic_fetch_add(&wal->lsn, 1, __ATOMIC_RELAXED);
    kvidxBegin(&wal->wal);
    kvidxInsert(&wal->wal, lsn, 0, 0, entry, len);
    kvidxCommit(&wal->wal);
    kvidxFsync(&wal->wal);  // Ensure durability
    return lsn;
}

// Truncate WAL up to checkpoint
void walTruncate(WriteAheadLog *wal, uint64_t checkpointLsn) {
    kvidxRemoveBeforeNInclusive(&wal->wal, checkpointLsn);
}

// Replay WAL from LSN
void walReplay(WriteAheadLog *wal, uint64_t fromLsn,
               void (*apply)(uint64_t lsn, const void *data, size_t len)) {
    kvidxIterator *it = kvidxIteratorCreate(&wal->wal, fromLsn,
                                            UINT64_MAX, KVIDX_ITER_FORWARD);
    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key, term, cmd;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, &term, &cmd, &data, &len);
        apply(key, data, len);
    }
    kvidxIteratorDestroy(it);
}
```

**Recommended Backend:** SQLite3 with `KVIDX_SYNC_FULL` for maximum durability.

---

## Backend Selection Guide

### SQLite3 (Default)

**Choose when:**

- You need rich configuration options
- Data size is under 1TB
- You want human-readable debugging (SQL queries)
- Mixed read/write workloads
- Concurrent access with WAL mode
- In-memory databases for testing

**Configuration for best performance:**

```c
kvidxConfig config = kvidxConfigDefault();
config.journalMode = KVIDX_JOURNAL_WAL;
config.syncMode = KVIDX_SYNC_NORMAL;
config.cacheSizeBytes = 64 * 1024 * 1024;
```

### LMDB

**Choose when:**

- Read-heavy workloads (90%+ reads)
- You need lowest latency
- Multi-process access is required
- Memory-mapped I/O is acceptable
- Data fits in addressable memory

**Characteristics:**

- Zero-copy reads
- Fixed maximum database size
- Read transactions are nearly free
- Excellent for lookups and scans

### RocksDB

**Choose when:**

- Write-heavy workloads
- Dataset exceeds 1TB
- You need compression
- High concurrent write throughput
- LSM-tree benefits apply

**Characteristics:**

- Optimized for writes via LSM tree
- Background compaction
- Good compression ratios
- Scales to multi-terabyte datasets

---

## Performance Characteristics

### Operation Complexity

| Operation  | SQLite3  | LMDB     | RocksDB  |
| ---------- | -------- | -------- | -------- |
| Get        | O(log n) | O(log n) | O(log n) |
| Insert     | O(log n) | O(log n) | O(1)\*   |
| Delete     | O(log n) | O(log n) | O(1)\*   |
| Range Scan | O(k)     | O(k)     | O(k)     |
| Count      | O(1)     | O(n)\*\* | O(n)     |

\*Amortized, actual write happens during compaction
\*\*LMDB doesn't maintain count, requires scan

### Throughput Guidelines

| Workload                 | Recommended Backend | Expected Throughput |
| ------------------------ | ------------------- | ------------------- |
| Read-heavy (90% reads)   | LMDB                | 500K+ reads/sec     |
| Write-heavy (90% writes) | RocksDB             | 100K+ writes/sec    |
| Balanced (50/50)         | SQLite3 WAL         | 50K+ ops/sec        |
| Batch inserts            | Any with batch API  | 200K+ inserts/sec   |

---

## Anti-Patterns (When NOT to Use kvidxkit)

### 1. Complex Query Requirements

If you need:

- Multi-column indexes
- Full-text search
- Complex JOINs
- Ad-hoc SQL queries

**Better alternatives:** PostgreSQL, SQLite3 directly, Elasticsearch

### 2. Document Storage

If your data is:

- Variable-structure documents
- Deeply nested JSON
- Schema-less

**Better alternatives:** MongoDB, CouchDB, PostgreSQL JSONB

### 3. Graph Relationships

If you need:

- Node-edge traversals
- Complex relationship queries
- Path finding

**Better alternatives:** Neo4j, ArangoDB, PostgreSQL with recursive CTEs

### 4. Distributed Storage

If you need:

- Multi-node replication (built-in)
- Automatic sharding
- Geographic distribution

**Better alternatives:** CockroachDB, TiKV, FoundationDB

### 5. String/Composite Keys

If your keys are:

- Variable-length strings
- Composite (multi-part)
- Non-sequential

**Better alternatives:** LevelDB, RocksDB directly

---

## Migration Strategies

### From SQLite3 Tables

```c
// Export existing SQLite3 data
kvidxInstance src = {0};
src.interface = kvidxInterfaceSqlite3;
kvidxOpen(&src, "legacy.db", NULL);

kvidxExportOptions opts = { .format = KVIDX_EXPORT_BINARY };
kvidxExport(&src, "migration.bin", &opts, NULL, NULL);
kvidxClose(&src);

// Import to new backend
kvidxInstance dst = {0};
dst.interface = kvidxInterfaceLmdb;  // Or RocksDB
kvidxOpen(&dst, "new.lmdb", NULL);

kvidxImportOptions importOpts = { .format = KVIDX_EXPORT_BINARY };
kvidxImport(&dst, "migration.bin", &importOpts, NULL, NULL);
kvidxClose(&dst);
```

---

## Deployment Recommendations

### Production Checklist

1. **Choose appropriate backend** based on workload analysis
2. **Configure journal mode** (WAL for SQLite3)
3. **Set sync mode** based on durability requirements
4. **Size cache** appropriately (rule of thumb: 10-20% of dataset)
5. **Implement TTL cleanup** if using expiration
6. **Monitor statistics** via `kvidxGetStats`
7. **Plan backup strategy** using export/import

### High Availability

kvidxkit is a single-node library. For HA:

- Use filesystem replication (DRBD, ZFS send/recv)
- Implement application-level replication
- Use kvidxkit's export/import for periodic snapshots
- Consider kvidxkit as the storage layer for your own replication protocol

### Capacity Planning

```c
// Monitor database growth
kvidxStats stats;
kvidxGetStats(&inst, &stats);

printf("Keys: %lu\n", stats.totalKeys);
printf("Data size: %lu MB\n", stats.totalDataBytes / (1024*1024));
printf("DB file: %lu MB\n", stats.databaseFileSize / (1024*1024));
printf("Free pages: %lu (fragmentation)\n", stats.freePages);
```
