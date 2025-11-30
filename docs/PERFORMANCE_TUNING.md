# kvidxkit Performance Tuning Guide

This guide covers performance optimization strategies for kvidxkit across all supported backends.

---

## Table of Contents

1. [Benchmarking Methodology](#benchmarking-methodology)
2. [General Optimization Principles](#general-optimization-principles)
3. [SQLite3 Tuning](#sqlite3-tuning)
4. [LMDB Tuning](#lmdb-tuning)
5. [RocksDB Tuning](#rocksdb-tuning)
6. [Workload-Specific Optimizations](#workload-specific-optimizations)
7. [Memory Management](#memory-management)
8. [Monitoring and Diagnostics](#monitoring-and-diagnostics)

---

## Benchmarking Methodology

### Running Benchmarks

```bash
# Build with Release mode for accurate results
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j4

# Run benchmark suite
./src/kvidxkit-bench          # Full benchmark
./src/kvidxkit-bench quick    # Quick benchmark (fewer operations)
```

### Key Metrics

| Metric            | Description          | Target             |
| ----------------- | -------------------- | ------------------ |
| Operations/sec    | Throughput           | Higher is better   |
| Latency (p50/p99) | Response time        | Lower is better    |
| Memory usage      | RAM consumption      | Workload-dependent |
| Disk I/O          | Read/write bandwidth | Lower is better    |

### Baseline Performance

Approximate performance on modern hardware (NVMe SSD, 32GB RAM):

| Operation           | SQLite3 WAL | LMDB     | RocksDB  |
| ------------------- | ----------- | -------- | -------- |
| Sequential insert   | 50K/sec     | 100K/sec | 150K/sec |
| Random read         | 200K/sec    | 500K/sec | 300K/sec |
| Batch insert (1000) | 500K/sec    | 400K/sec | 600K/sec |
| Range scan          | 1M/sec      | 2M/sec   | 1.5M/sec |

---

## General Optimization Principles

### 1. Use Transactions Wisely

**Bad:** Individual inserts without transactions

```c
for (int i = 0; i < 10000; i++) {
    kvidxBegin(&inst);
    kvidxInsert(&inst, i, 0, 0, data, len);
    kvidxCommit(&inst);  // fsync on every commit!
}
```

**Good:** Batch operations in transactions

```c
kvidxBegin(&inst);
for (int i = 0; i < 10000; i++) {
    kvidxInsert(&inst, i, 0, 0, data, len);
}
kvidxCommit(&inst);  // Single fsync
```

**Best:** Use batch API

```c
kvidxInsertBatch(&inst, entries, 10000, &inserted);
```

**Performance Impact:** 10-100x improvement with batching

### 2. Choose the Right Commit Frequency

For bulk loads, commit every 10,000-100,000 operations:

```c
kvidxBegin(&inst);
for (uint64_t i = 1; i <= count; i++) {
    kvidxInsert(&inst, i, 0, 0, data, len);
    if (i % 50000 == 0) {
        kvidxCommit(&inst);
        kvidxBegin(&inst);
    }
}
kvidxCommit(&inst);
```

### 3. Minimize Data Copies

Use pointers from `kvidxGet()` directly instead of copying:

```c
// Good: Direct pointer use
const uint8_t *data;
size_t len;
kvidxGet(&inst, key, NULL, NULL, &data, &len);
process_data(data, len);  // Use directly

// Bad: Unnecessary copy
char buffer[1024];
memcpy(buffer, data, len);
process_data(buffer, len);
```

### 4. Pre-size for Expected Data

When using iterators for large scans, pre-allocate result buffers:

```c
// Pre-allocate for expected results
size_t estimatedCount = 10000;
Result *results = malloc(estimatedCount * sizeof(Result));
```

---

## SQLite3 Tuning

### Journal Mode

| Mode       | Safety | Speed     | Use Case          |
| ---------- | ------ | --------- | ----------------- |
| `DELETE`   | High   | Slow      | Compatibility     |
| `TRUNCATE` | High   | Medium    | General use       |
| `WAL`      | High   | Fast      | **Recommended**   |
| `MEMORY`   | Low    | Very Fast | Testing only      |
| `OFF`      | None   | Fastest   | Import operations |

```c
kvidxConfig config = kvidxConfigDefault();
config.journalMode = KVIDX_JOURNAL_WAL;  // Recommended
```

### Synchronous Mode

| Mode     | Durability | Speed   | Data Loss Risk                   |
| -------- | ---------- | ------- | -------------------------------- |
| `OFF`    | None       | Fastest | High (crash = data loss)         |
| `NORMAL` | Good       | Fast    | Low (power loss = possible loss) |
| `FULL`   | Complete   | Slow    | None                             |
| `EXTRA`  | Extra safe | Slowest | None                             |

```c
config.syncMode = KVIDX_SYNC_NORMAL;  // Balanced default
```

**Bulk Import Pattern:**

```c
// Temporarily disable sync for bulk import
config.syncMode = KVIDX_SYNC_OFF;
config.journalMode = KVIDX_JOURNAL_OFF;
kvidxOpenWithConfig(&inst, path, &config, NULL);

// Bulk import...
kvidxInsertBatch(&inst, entries, count, NULL);

// Force sync at end
kvidxFsync(&inst);
kvidxClose(&inst);

// Reopen with safe settings
config.syncMode = KVIDX_SYNC_NORMAL;
config.journalMode = KVIDX_JOURNAL_WAL;
```

### Cache Size

SQLite uses a page cache. Larger cache = fewer disk reads.

```c
// Rule of thumb: 10-20% of expected dataset size
config.cacheSizeBytes = 128 * 1024 * 1024;  // 128 MB
```

| Dataset Size  | Recommended Cache |
| ------------- | ----------------- |
| < 100 MB      | 32 MB (default)   |
| 100 MB - 1 GB | 64-128 MB         |
| 1-10 GB       | 256-512 MB        |
| > 10 GB       | 1 GB+             |

### Memory-Mapped I/O

Enable mmap for read-heavy workloads with datasets that fit in RAM:

```c
config.mmapSizeBytes = 1024 * 1024 * 1024;  // 1 GB mmap
```

**Benefits:**

- Faster reads via direct memory access
- Reduced system call overhead

**Limitations:**

- Address space constraints on 32-bit systems
- May not help write-heavy workloads

### Page Size

Optimal page size depends on storage and access patterns:

| Page Size      | Best For          |
| -------------- | ----------------- |
| 4096 (default) | General use, SSD  |
| 8192           | Large values, HDD |
| 16384          | Very large values |

```c
config.pageSize = 4096;  // Must set before database creation
```

### Busy Timeout

For concurrent access, set appropriate timeout:

```c
config.busyTimeoutMs = 10000;  // 10 second timeout
```

---

## LMDB Tuning

### Map Size

LMDB pre-allocates a memory map. Set large enough for expected data:

```c
config.mmapSizeBytes = 10ULL * 1024 * 1024 * 1024;  // 10 GB
```

**Guidelines:**

- Set 2-3x expected final database size
- Can be increased later (requires reopen)
- Doesn't consume RAM until used

### Read Optimization

LMDB excels at reads due to zero-copy access:

```c
// Data pointer is valid until next operation
const uint8_t *data;
kvidxGet(&inst, key, NULL, NULL, &data, &len);
// Use data directly - no memcpy needed
```

### Write Optimization

LMDB writes are synchronous by default. For bulk loads:

```c
config.syncMode = KVIDX_SYNC_OFF;  // MDB_NOSYNC
// Bulk insert...
kvidxFsync(&inst);  // Explicit sync at end
```

### Concurrent Access

LMDB supports multiple readers, single writer:

- Multiple processes can read simultaneously
- Only one process can write at a time
- Reads don't block writes, writes don't block reads

---

## RocksDB Tuning

### Write Optimization

RocksDB uses LSM-tree, optimized for writes:

```c
// Write batches are efficient
kvidxInsertBatch(&inst, entries, count, NULL);
```

### Sync Mode

```c
config.syncMode = KVIDX_SYNC_NORMAL;  // Async writes
config.syncMode = KVIDX_SYNC_FULL;    // Sync writes
```

### Background Compaction

RocksDB compacts data in the background. This is automatic but can cause:

- Temporary write slowdowns during compaction
- Increased disk I/O

For latency-sensitive applications, consider limiting write rate during peak times.

### Read Optimization

RocksDB caches data internally. For read-heavy workloads, ensure sufficient memory for block cache (configured at RocksDB level, not kvidxkit).

---

## Workload-Specific Optimizations

### High-Throughput Ingestion

```c
kvidxConfig config = kvidxConfigDefault();
config.journalMode = KVIDX_JOURNAL_WAL;
config.syncMode = KVIDX_SYNC_NORMAL;
config.cacheSizeBytes = 256 * 1024 * 1024;

// Use batch API
const size_t BATCH_SIZE = 10000;
kvidxEntry batch[BATCH_SIZE];
// ... fill batch
kvidxInsertBatch(&inst, batch, BATCH_SIZE, NULL);
```

### Read-Heavy (95%+ Reads)

**Best Backend:** LMDB

```c
kvidxConfig config = kvidxConfigDefault();
config.mmapSizeBytes = 2ULL * 1024 * 1024 * 1024;  // 2 GB mmap for reads
```

### Mixed Workload

**Best Backend:** SQLite3 with WAL

```c
kvidxConfig config = kvidxConfigDefault();
config.journalMode = KVIDX_JOURNAL_WAL;
config.syncMode = KVIDX_SYNC_NORMAL;
config.cacheSizeBytes = 128 * 1024 * 1024;
```

### Append-Only Log

Sequential writes with range queries:

```c
// Use sequential keys
uint64_t nextKey = kvidxMaxKey(&inst, &maxKey) ? maxKey + 1 : 1;
kvidxInsert(&inst, nextKey, term, cmd, data, len);

// Efficient range truncation
kvidxRemoveBeforeNInclusive(&inst, oldestAllowedKey);
```

### Session Store with TTL

```c
// Set TTL on insert
kvidxInsert(&inst, sessionId, 0, 0, sessionData, len);
kvidxSetExpire(&inst, sessionId, 30 * 60 * 1000);  // 30 min

// Periodic cleanup (call from background thread)
void cleanupTask(kvidxInstance *inst) {
    uint64_t expired = 0;
    kvidxExpireScan(inst, 1000, &expired);  // Process 1000 at a time
}
```

---

## Memory Management

### Avoiding Memory Leaks

Functions that allocate memory the caller must free:

| Function             | Allocated Parameter |
| -------------------- | ------------------- |
| `kvidxGetAndSet`     | `oldData`           |
| `kvidxGetAndRemove`  | `data`              |
| `kvidxGetValueRange` | `data`              |

```c
void *data = NULL;
kvidxGetAndRemove(&inst, key, NULL, NULL, &data, &len);
if (data) {
    // Use data...
    free(data);  // MUST free
}
```

### Controlling Memory Growth

1. **Set appropriate cache sizes** based on available RAM
2. **Use iterators** for large scans instead of loading all data
3. **Close iterators promptly** after use
4. **Batch TTL cleanup** to avoid memory spikes

---

## Monitoring and Diagnostics

### Statistics API

```c
kvidxStats stats;
kvidxGetStats(&inst, &stats);

printf("Keys: %lu\n", stats.totalKeys);
printf("Data size: %lu MB\n", stats.totalDataBytes / (1024 * 1024));
printf("DB file: %lu MB\n", stats.databaseFileSize / (1024 * 1024));
printf("Free pages: %lu (fragmentation indicator)\n", stats.freePages);
```

### Fragmentation

High `freePages` indicates fragmentation. For SQLite3:

```bash
# External VACUUM (not via kvidxkit API)
sqlite3 database.db "VACUUM;"
```

### Performance Debugging

1. **Enable timing:**

   ```c
   struct timespec start, end;
   clock_gettime(CLOCK_MONOTONIC, &start);
   // ... operations
   clock_gettime(CLOCK_MONOTONIC, &end);
   double elapsed_ms = (end.tv_sec - start.tv_sec) * 1000.0 +
                       (end.tv_nsec - start.tv_nsec) / 1000000.0;
   ```

2. **Profile I/O:**

   ```bash
   # Linux
   iotop -p $(pgrep myapp)

   # macOS
   fs_usage -w -f filesys myapp
   ```

3. **Check lock contention:**
   ```c
   // Increase busy timeout if seeing KVIDX_ERROR_LOCKED
   config.busyTimeoutMs = 30000;  // 30 seconds
   ```

---

## Performance Checklist

Before production deployment:

- [ ] Selected appropriate backend for workload
- [ ] Configured cache size for dataset
- [ ] Set journal mode (WAL for SQLite3)
- [ ] Set appropriate sync mode for durability needs
- [ ] Using batch operations for bulk inserts
- [ ] Using transactions to group related operations
- [ ] Iterators destroyed after use
- [ ] TTL cleanup scheduled if using expiration
- [ ] Monitoring statistics for growth/fragmentation
- [ ] Tested with production-like data volume
