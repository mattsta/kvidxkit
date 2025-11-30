# kvidxkit Troubleshooting Guide

This guide covers common issues, error codes, debugging techniques, and recovery procedures.

---

## Table of Contents

1. [Error Code Reference](#error-code-reference)
2. [Common Issues](#common-issues)
3. [Debugging Techniques](#debugging-techniques)
4. [Recovery Procedures](#recovery-procedures)
5. [FAQ](#faq)

---

## Error Code Reference

### Error Codes

| Code | Name                             | Description                | Common Cause                            |
| ---- | -------------------------------- | -------------------------- | --------------------------------------- |
| 0    | `KVIDX_OK`                       | Success                    | -                                       |
| 1    | `KVIDX_ERROR_INVALID_ARGUMENT`   | Invalid argument           | NULL pointer, invalid range             |
| 2    | `KVIDX_ERROR_DUPLICATE_KEY`      | Duplicate key              | Key already exists                      |
| 3    | `KVIDX_ERROR_NOT_FOUND`          | Key not found              | Key doesn't exist                       |
| 4    | `KVIDX_ERROR_DISK_FULL`          | Disk full                  | No space left on device                 |
| 5    | `KVIDX_ERROR_IO`                 | I/O error                  | Permission denied, file corruption      |
| 6    | `KVIDX_ERROR_CORRUPT`            | Database corruption        | Hardware failure, incomplete write      |
| 7    | `KVIDX_ERROR_TRANSACTION_ACTIVE` | Transaction already active | Nested transaction attempt              |
| 8    | `KVIDX_ERROR_NO_TRANSACTION`     | No active transaction      | Commit/abort without begin              |
| 9    | `KVIDX_ERROR_READONLY`           | Database is read-only      | Opened in read-only mode                |
| 10   | `KVIDX_ERROR_LOCKED`             | Database locked            | Another process holds lock              |
| 11   | `KVIDX_ERROR_NOMEM`              | Out of memory              | Insufficient RAM                        |
| 12   | `KVIDX_ERROR_TOO_BIG`            | Size limit exceeded        | Value too large                         |
| 13   | `KVIDX_ERROR_CONSTRAINT`         | Constraint violation       | Foreign key violation                   |
| 14   | `KVIDX_ERROR_SCHEMA`             | Schema error               | Type mismatch                           |
| 15   | `KVIDX_ERROR_RANGE`              | Range error                | Invalid range (start > end)             |
| 16   | `KVIDX_ERROR_NOT_SUPPORTED`      | Not supported              | Operation not available on this backend |
| 17   | `KVIDX_ERROR_CANCELLED`          | Operation cancelled        | User cancelled via callback             |
| 99   | `KVIDX_ERROR_INTERNAL`           | Internal error             | Bug, please report                      |
| 100  | `KVIDX_ERROR_CONDITION_FAILED`   | Condition not met          | Conditional write failed                |
| 101  | `KVIDX_ERROR_EXPIRED`            | Key expired                | TTL expired                             |

### Getting Error Details

```c
kvidxError err = kvidxInsertNX(&inst, key, term, cmd, data, len);
if (kvidxIsError(err)) {
    // Get detailed message
    const char *msg = kvidxGetLastErrorMessage(&inst);
    fprintf(stderr, "Error %d: %s\n", err, msg);

    // Or get generic description
    fprintf(stderr, "Error: %s\n", kvidxErrorString(err));

    // Clear for next operation
    kvidxClearError(&inst);
}
```

---

## Common Issues

### Database Locked (KVIDX_ERROR_LOCKED)

**Symptom:** Operations fail with "database is locked" error.

**Causes:**

1. Another process has the database open
2. Unclosed transaction holding lock
3. Crashed process left lock file

**Solutions:**

```c
// 1. Increase busy timeout
kvidxConfig config = kvidxConfigDefault();
config.busyTimeoutMs = 30000;  // 30 seconds
kvidxOpenWithConfig(&inst, path, &config, NULL);

// 2. Check for unclosed transactions
if (inst.transactionActive) {
    kvidxAbort(&inst);  // Or kvidxCommit
}
```

For SQLite3:

```bash
# Check for lock files
ls -la database.db*

# If crashed, remove lock (only if sure no other process is using it!)
rm database.db-shm database.db-wal  # Careful! May lose uncommitted data
```

For LMDB:

```bash
# Check for stale readers
ls -la database.lmdb/lock.mdb
```

### Transaction Errors

**Nested Transaction Attempt:**

```c
kvidxBegin(&inst);
kvidxBegin(&inst);  // KVIDX_ERROR_TRANSACTION_ACTIVE
```

**Solution:** Check transaction state:

```c
if (!inst.transactionActive) {
    kvidxBegin(&inst);
}
```

**Commit Without Begin:**

```c
kvidxCommit(&inst);  // KVIDX_ERROR_NO_TRANSACTION
```

**Solution:** Always pair begin/commit:

```c
if (kvidxBegin(&inst)) {
    // ... operations
    kvidxCommit(&inst);
}
```

### Disk Full (KVIDX_ERROR_DISK_FULL)

**Solution:**

1. Free disk space
2. Vacuum/compact database (SQLite3)
3. Consider archiving old data

```c
// Check disk usage via stats
kvidxStats stats;
kvidxGetStats(&inst, &stats);
printf("Database size: %lu bytes\n", stats.databaseFileSize);
```

### Memory Issues (KVIDX_ERROR_NOMEM)

**Causes:**

1. Cache size too large
2. Memory leak (unclosed iterators, unfree'd data)
3. Too many concurrent operations

**Solutions:**

```c
// 1. Reduce cache size
config.cacheSizeBytes = 16 * 1024 * 1024;  // 16 MB

// 2. Close iterators promptly
kvidxIterator *it = kvidxIteratorCreate(&inst, 0, UINT64_MAX, KVIDX_ITER_FORWARD);
// ... use iterator
kvidxIteratorDestroy(it);  // ALWAYS destroy!

// 3. Free returned data
void *data = NULL;
kvidxGetAndRemove(&inst, key, NULL, NULL, &data, &len);
free(data);  // MUST free!
```

### Conditional Write Failures

**KVIDX_ERROR_CONDITION_FAILED:**

```c
// InsertNX failed because key exists
kvidxError err = kvidxInsertNX(&inst, key, 0, 0, data, len);
if (err == KVIDX_ERROR_CONDITION_FAILED) {
    // Key already exists - handle accordingly
    // Option 1: Update instead
    kvidxInsertXX(&inst, key, 0, 0, newData, newLen);

    // Option 2: Read existing value
    kvidxGet(&inst, key, &term, &cmd, &existingData, &existingLen);
}
```

### TTL Issues

**Key Not Expiring:**

```c
// Expiration is lazy - keys aren't automatically deleted
// Call ExpireScan periodically:
void backgroundCleanup(kvidxInstance *inst) {
    uint64_t count = 0;
    kvidxExpireScan(inst, 1000, &count);  // Clean up to 1000 expired keys
}
```

**Negative TTL Returned:**

```c
int64_t ttl = kvidxGetTTL(&inst, key);
if (ttl == KVIDX_TTL_NOT_FOUND) {
    printf("Key doesn't exist\n");
} else if (ttl == KVIDX_TTL_NONE) {
    printf("Key has no expiration\n");
} else if (ttl <= 0) {
    printf("Key has expired but not yet cleaned up\n");
}
```

---

## Debugging Techniques

### Enable Verbose Error Messages

```c
// Always check and log errors
kvidxError err = some_operation();
if (kvidxIsError(err)) {
    fprintf(stderr, "[kvidxkit] %s:%d - Error %d (%s): %s\n",
            __FILE__, __LINE__,
            err, kvidxErrorString(err),
            kvidxGetLastErrorMessage(&inst));
}
```

### Transaction Debugging

```c
// Wrapper for transaction debugging
bool safe_begin(kvidxInstance *inst) {
    if (inst->transactionActive) {
        fprintf(stderr, "Warning: Transaction already active\n");
        return false;
    }
    return kvidxBegin(inst);
}

bool safe_commit(kvidxInstance *inst) {
    if (!inst->transactionActive) {
        fprintf(stderr, "Warning: No active transaction\n");
        return false;
    }
    return kvidxCommit(inst);
}
```

### Iterator Debugging

```c
// Debug iterator state
void debug_iterator(kvidxIterator *it) {
    if (!it) {
        printf("Iterator is NULL\n");
        return;
    }
    if (!kvidxIteratorValid(it)) {
        printf("Iterator is invalid/exhausted\n");
        return;
    }
    uint64_t key = kvidxIteratorKey(it);
    printf("Iterator at key: %lu\n", key);
}
```

### Backend Verification

```c
// Verify which backends are available
printf("Available backends:\n");
size_t count = kvidxGetAdapterCount();
for (size_t i = 0; i < count; i++) {
    const kvidxAdapterInfo *info = kvidxGetAdapterByIndex(i);
    printf("  - %s (path suffix: %s, directory: %s)\n",
           info->name, info->pathSuffix,
           info->isDirectory ? "yes" : "no");
}
```

### SQLite3 Specific Debugging

For SQLite3 backend, you can query the database directly:

```bash
# Check database integrity
sqlite3 database.db "PRAGMA integrity_check;"

# Check journal mode
sqlite3 database.db "PRAGMA journal_mode;"

# Check page count and size
sqlite3 database.db "PRAGMA page_count; PRAGMA page_size;"

# List tables
sqlite3 database.db ".tables"

# View schema
sqlite3 database.db ".schema log"
```

---

## Recovery Procedures

### Corrupt Database Recovery (SQLite3)

**Step 1: Check integrity**

```bash
sqlite3 database.db "PRAGMA integrity_check;"
```

**Step 2: Attempt recovery**

```bash
# Export to SQL
sqlite3 database.db ".dump" > backup.sql

# Create new database
sqlite3 database_new.db < backup.sql

# Verify
sqlite3 database_new.db "PRAGMA integrity_check;"
```

**Step 3: Use kvidxkit export/import**

```c
// If database opens but has issues
kvidxExportOptions opts = {.format = KVIDX_EXPORT_BINARY};
kvidxExport(&inst, "recovery.bin", &opts, NULL, NULL);
kvidxClose(&inst);

// Import to fresh database
kvidxInstance newInst = {0};
newInst.interface = kvidxInterfaceSqlite3;
kvidxOpen(&newInst, "database_new.db", NULL);
kvidxImportOptions importOpts = {.skipDuplicates = true};
kvidxImport(&newInst, "recovery.bin", &importOpts, NULL, NULL);
```

### LMDB Recovery

LMDB is designed to be crash-resistant. If issues occur:

```bash
# Check environment
mdb_stat -a database.lmdb/

# Copy data to new environment
mdb_copy database.lmdb/ database_new.lmdb/
```

### RocksDB Recovery

RocksDB has built-in recovery:

```bash
# Use ldb tool for diagnostics
ldb --db=database.rocks scan
ldb --db=database.rocks checkconsistency
```

### Stale Lock Recovery

**SQLite3:**

```bash
# Check for WAL files
ls -la database.db-wal database.db-shm

# Force WAL checkpoint (recovers data)
sqlite3 database.db "PRAGMA wal_checkpoint(TRUNCATE);"
```

**LMDB:**

```bash
# Clear stale readers
rm database.lmdb/lock.mdb  # Only if no other process is using!
```

---

## FAQ

### Q: Which backend should I choose?

**A:**

- **SQLite3**: Best for most use cases. Balanced performance, rich configuration, easy debugging.
- **LMDB**: Best for read-heavy workloads (90%+ reads). Fastest reads, zero-copy access.
- **RocksDB**: Best for write-heavy workloads. LSM-tree optimized for writes.

### Q: How do I handle concurrent access?

**A:**

```c
// All backends support concurrent reads
// For writes:
// - SQLite3 WAL: Multiple readers, single writer
// - LMDB: Multiple readers, single writer
// - RocksDB: Concurrent writes handled internally

// Set busy timeout for SQLite3
config.busyTimeoutMs = 10000;
```

### Q: My inserts are slow. How do I speed them up?

**A:**

1. Use batch operations: `kvidxInsertBatch()`
2. Use transactions: group operations between `begin`/`commit`
3. Reduce sync frequency: `config.syncMode = KVIDX_SYNC_NORMAL`
4. For bulk import: temporarily use `KVIDX_SYNC_OFF` and `KVIDX_JOURNAL_OFF`

### Q: How do I prevent data loss on crash?

**A:**

```c
// Use full sync mode
config.syncMode = KVIDX_SYNC_FULL;

// Use WAL journal mode (SQLite3)
config.journalMode = KVIDX_JOURNAL_WAL;

// Call fsync after critical operations
kvidxFsync(&inst);
```

### Q: How often should I call ExpireScan?

**A:**
For session-like workloads:

```c
// Every 1-10 minutes, depending on TTL duration
// Process in batches to avoid blocking
uint64_t count = 0;
kvidxExpireScan(&inst, 1000, &count);  // 1000 at a time
```

### Q: Can I use kvidxkit from multiple threads?

**A:**

- Each thread should have its own `kvidxInstance`
- Backends handle concurrent access safely
- Don't share iterators between threads

### Q: How do I migrate between backends?

**A:**

```c
// Export from source
kvidxExportOptions opts = {.format = KVIDX_EXPORT_BINARY};
kvidxExport(&srcInst, "migration.bin", &opts, NULL, NULL);

// Import to destination
kvidxImportOptions importOpts = {.format = KVIDX_EXPORT_BINARY};
kvidxImport(&dstInst, "migration.bin", &importOpts, NULL, NULL);
```

### Q: What's the maximum key/value size?

**A:**

- **Key**: uint64_t (0 to 2^64-1)
- **Value**: Backend-dependent
  - SQLite3: 1 GB (SQLITE_MAX_LENGTH)
  - LMDB: Limited by map size
  - RocksDB: Limited by available memory

### Q: How do I handle KVIDX_ERROR_LOCKED in production?

**A:**

```c
int retries = 3;
kvidxError err;
do {
    err = kvidxBegin(&inst);
    if (err == KVIDX_ERROR_LOCKED) {
        usleep(100000);  // Wait 100ms
        retries--;
    }
} while (err == KVIDX_ERROR_LOCKED && retries > 0);

if (kvidxIsError(err)) {
    // Handle permanent failure
}
```

### Q: Is kvidxkit suitable for production use?

**A:** Yes, with these considerations:

1. Extensive test suite validates correctness
2. All backends are battle-tested (SQLite3, LMDB, RocksDB)
3. Monitor database size and performance
4. Implement proper backup strategy
5. Handle errors appropriately
