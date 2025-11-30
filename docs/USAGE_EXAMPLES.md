# kvidxkit Usage Examples

This document provides comprehensive code examples for using kvidxkit.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Basic Operations](#basic-operations)
3. [Transactions](#transactions)
4. [Iteration and Range Queries](#iteration-and-range-queries)
5. [Batch Operations](#batch-operations)
6. [Configuration](#configuration)
7. [Error Handling](#error-handling)
8. [Atomic Operations](#atomic-operations)
9. [TTL and Expiration](#ttl-and-expiration)
10. [Export and Import](#export-and-import)
11. [Multi-Backend Usage](#multi-backend-usage)

---

## Getting Started

### Minimal Example

```c
#include "kvidxkit.h"

int main(void) {
    // Initialize instance with SQLite3 backend
    kvidxInstance inst = {0};
    inst.interface = kvidxInterfaceSqlite3;

    // Open database
    const char *errStr = NULL;
    if (!kvidxOpen(&inst, "mydata.db", &errStr)) {
        fprintf(stderr, "Failed to open: %s\n", errStr);
        return 1;
    }

    // Insert a key-value pair
    kvidxBegin(&inst);
    kvidxInsert(&inst, 1, 0, 0, "Hello, World!", 13);
    kvidxCommit(&inst);

    // Read it back
    uint64_t term, cmd;
    const uint8_t *data;
    size_t dataLen;

    if (kvidxGet(&inst, 1, &term, &cmd, &data, &dataLen)) {
        printf("Value: %.*s\n", (int)dataLen, data);
    }

    // Cleanup
    kvidxClose(&inst);
    return 0;
}
```

### Building and Linking

```bash
# Build kvidxkit
mkdir build && cd build
cmake ..
make

# Compile your application
gcc -o myapp myapp.c -I/path/to/kvidxkit/src \
    -L/path/to/kvidxkit/build/src -lkvidxkit-static -lsqlite3
```

---

## Basic Operations

### Insert Operations

```c
// Insert with all metadata
kvidxBegin(&inst);

uint64_t key = 100;
uint64_t term = 1;      // Version or epoch
uint64_t cmd = 42;      // Command type
const char *value = "my data";
size_t valueLen = strlen(value);

bool success = kvidxInsert(&inst, key, term, cmd, value, valueLen);

kvidxCommit(&inst);
```

### Read Operations

```c
uint64_t term, cmd;
const uint8_t *data;
size_t dataLen;

// Get by key
if (kvidxGet(&inst, 100, &term, &cmd, &data, &dataLen)) {
    printf("Found key 100: term=%lu, cmd=%lu, data=%.*s\n",
           term, cmd, (int)dataLen, data);
} else {
    printf("Key 100 not found\n");
}

// Check existence only
if (kvidxExists(&inst, 100)) {
    printf("Key 100 exists\n");
}

// Check key + term combination
if (kvidxExistsDual(&inst, 100, 1)) {
    printf("Key 100 with term 1 exists\n");
}
```

### Delete Operations

```c
kvidxBegin(&inst);

// Delete single key
bool removed = kvidxRemove(&inst, 100);

// Delete key and all greater keys
kvidxRemoveAfterNInclusive(&inst, 500);  // Removes 500, 501, 502, ...

// Delete key and all lesser keys
kvidxRemoveBeforeNInclusive(&inst, 100); // Removes 100, 99, 98, ...

kvidxCommit(&inst);
```

### Range Delete

```c
uint64_t deletedCount = 0;

kvidxError err = kvidxRemoveRange(&inst,
    100,    // Start key
    200,    // End key
    true,   // Start inclusive (include 100)
    false,  // End exclusive (exclude 200)
    &deletedCount
);

printf("Deleted %lu keys\n", deletedCount);
```

---

## Transactions

### Basic Transaction Pattern

```c
// Start transaction
if (!kvidxBegin(&inst)) {
    fprintf(stderr, "Failed to begin transaction\n");
    return;
}

// Perform multiple operations atomically
kvidxInsert(&inst, 1, 0, 0, "value1", 6);
kvidxInsert(&inst, 2, 0, 0, "value2", 6);
kvidxInsert(&inst, 3, 0, 0, "value3", 6);

// Commit all changes
if (!kvidxCommit(&inst)) {
    fprintf(stderr, "Failed to commit: %s\n",
            kvidxGetLastErrorMessage(&inst));
}
```

### Transaction Abort (Rollback)

```c
kvidxBegin(&inst);

kvidxInsert(&inst, 1, 0, 0, "tentative", 9);

// Something went wrong, abort all changes
kvidxAbort(&inst);

// Key 1 was not inserted
```

### Periodic Commit for Large Operations

```c
kvidxBegin(&inst);

for (uint64_t i = 1; i <= 1000000; i++) {
    char data[32];
    int len = snprintf(data, sizeof(data), "value-%lu", i);
    kvidxInsert(&inst, i, 0, 0, data, len);

    // Commit every 10,000 operations to avoid huge transactions
    if (i % 10000 == 0) {
        kvidxCommit(&inst);
        kvidxBegin(&inst);
        printf("Progress: %lu\n", i);
    }
}

kvidxCommit(&inst);
```

---

## Iteration and Range Queries

### Forward Iterator

```c
// Iterate all keys from 0 to UINT64_MAX
kvidxIterator *it = kvidxIteratorCreate(&inst,
    0,                  // Start key
    UINT64_MAX,         // End key
    KVIDX_ITER_FORWARD  // Direction
);

while (kvidxIteratorNext(it)) {
    if (!kvidxIteratorValid(it)) break;

    uint64_t key, term, cmd;
    const uint8_t *data;
    size_t len;

    kvidxIteratorGet(it, &key, &term, &cmd, &data, &len);
    printf("Key: %lu, Data: %.*s\n", key, (int)len, data);
}

kvidxIteratorDestroy(it);
```

### Backward Iterator

```c
// Iterate from 1000 down to 500
kvidxIterator *it = kvidxIteratorCreate(&inst,
    500, 1000, KVIDX_ITER_BACKWARD
);

while (kvidxIteratorNext(it)) {
    if (!kvidxIteratorValid(it)) break;

    uint64_t key = kvidxIteratorKey(it);
    printf("Key (descending): %lu\n", key);
}

kvidxIteratorDestroy(it);
```

### Bounded Range Iteration

```c
// Only iterate keys 100-199
kvidxIterator *it = kvidxIteratorCreate(&inst, 100, 199, KVIDX_ITER_FORWARD);

size_t count = 0;
while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
    count++;
}
printf("Found %zu keys in range [100, 199]\n", count);

kvidxIteratorDestroy(it);
```

### Simple Next/Previous Navigation

```c
// Get next key after 100
uint64_t nextKey, term, cmd;
const uint8_t *data;
size_t len;

if (kvidxGetNext(&inst, 100, &nextKey, &term, &cmd, &data, &len)) {
    printf("Next key after 100 is %lu\n", nextKey);
}

// Get previous key before 100
uint64_t prevKey;
if (kvidxGetPrev(&inst, 100, &prevKey, &term, &cmd, &data, &len)) {
    printf("Previous key before 100 is %lu\n", prevKey);
}
```

### Range Queries

```c
// Count keys in range
uint64_t count = 0;
kvidxCountRange(&inst, 100, 200, &count);
printf("Keys in [100, 200]: %lu\n", count);

// Check if any keys exist in range
bool hasKeys = false;
kvidxExistsInRange(&inst, 100, 200, &hasKeys);
if (hasKeys) {
    printf("Range [100, 200] has at least one key\n");
}

// Find maximum key
uint64_t maxKey;
if (kvidxMaxKey(&inst, &maxKey)) {
    printf("Maximum key: %lu\n", maxKey);
}
```

---

## Batch Operations

### Simple Batch Insert

```c
#define BATCH_SIZE 1000

kvidxEntry entries[BATCH_SIZE];
char dataBuffer[BATCH_SIZE][64];

// Prepare batch
for (size_t i = 0; i < BATCH_SIZE; i++) {
    int len = snprintf(dataBuffer[i], 64, "batch-value-%zu", i);
    entries[i].key = i + 1;
    entries[i].term = 1;
    entries[i].cmd = 0;
    entries[i].data = dataBuffer[i];
    entries[i].dataLen = len;
}

// Execute batch insert
size_t inserted = 0;
if (kvidxInsertBatch(&inst, entries, BATCH_SIZE, &inserted)) {
    printf("Inserted %zu entries\n", inserted);
} else {
    printf("Batch failed after %zu entries: %s\n",
           inserted, kvidxGetLastErrorMessage(&inst));
}
```

### Batch Insert with Filter Callback

```c
// Filter: only insert even keys
bool filterEvenKeys(size_t index, const kvidxEntry *entry, void *userData) {
    (void)index;
    (void)userData;
    return (entry->key % 2) == 0;
}

size_t inserted = 0;
kvidxInsertBatchEx(&inst, entries, BATCH_SIZE,
                   filterEvenKeys, NULL, &inserted);
printf("Inserted %zu even-keyed entries\n", inserted);
```

### Batch Insert with Progress Tracking

```c
typedef struct {
    size_t lastReported;
} ProgressState;

bool progressFilter(size_t index, const kvidxEntry *entry, void *userData) {
    ProgressState *state = (ProgressState *)userData;

    if (index - state->lastReported >= 1000) {
        printf("Progress: %zu entries processed\n", index);
        state->lastReported = index;
    }

    return true;  // Include all entries
}

ProgressState state = {0};
kvidxInsertBatchEx(&inst, entries, BATCH_SIZE,
                   progressFilter, &state, &inserted);
```

---

## Configuration

### Custom Configuration at Open

```c
kvidxConfig config = kvidxConfigDefault();

// Performance tuning
config.cacheSizeBytes = 128 * 1024 * 1024;  // 128 MB cache
config.journalMode = KVIDX_JOURNAL_WAL;      // Write-Ahead Logging
config.syncMode = KVIDX_SYNC_NORMAL;         // Balanced durability

// Behavior options
config.busyTimeoutMs = 10000;                // 10 second lock timeout
config.enableForeignKeys = true;             // Enable FK constraints
config.readOnly = false;                     // Read-write mode

kvidxInstance inst = {0};
inst.interface = kvidxInterfaceSqlite3;

const char *err = NULL;
if (!kvidxOpenWithConfig(&inst, "configured.db", &config, &err)) {
    fprintf(stderr, "Open failed: %s\n", err);
}
```

### Journal Mode Options

```c
// DELETE - Default SQLite behavior, delete journal after commit
config.journalMode = KVIDX_JOURNAL_DELETE;

// WAL - Write-Ahead Logging, best for concurrent access
config.journalMode = KVIDX_JOURNAL_WAL;

// MEMORY - Journal in memory only, fastest but not crash-safe
config.journalMode = KVIDX_JOURNAL_MEMORY;

// OFF - No journal, fastest but no crash recovery
config.journalMode = KVIDX_JOURNAL_OFF;
```

### Sync Mode Options

```c
// OFF - No fsync, fastest but data loss risk on crash
config.syncMode = KVIDX_SYNC_OFF;

// NORMAL - Fsync at critical moments (default)
config.syncMode = KVIDX_SYNC_NORMAL;

// FULL - Fsync after every write
config.syncMode = KVIDX_SYNC_FULL;

// EXTRA - Extra sync for maximum durability
config.syncMode = KVIDX_SYNC_EXTRA;
```

### Runtime Configuration Update

```c
// Get current configuration
kvidxConfig currentConfig;
kvidxGetConfig(&inst, &currentConfig);

// Modify and apply
currentConfig.syncMode = KVIDX_SYNC_FULL;
kvidxUpdateConfig(&inst, &currentConfig);
```

---

## Error Handling

### Checking and Retrieving Errors

```c
kvidxError err = kvidxInsertEx(&inst, key, term, cmd, data, len,
                               KVIDX_SET_IF_NOT_EXISTS);

if (err != KVIDX_OK) {
    // Get error details
    kvidxError lastErr = kvidxGetLastError(&inst);
    const char *msg = kvidxGetLastErrorMessage(&inst);

    printf("Error %d: %s\n", lastErr, msg);

    // Handle specific errors
    switch (lastErr) {
        case KVIDX_ERROR_DUPLICATE_KEY:
            printf("Key already exists\n");
            break;
        case KVIDX_ERROR_DISK_FULL:
            printf("Disk full, cannot write\n");
            break;
        case KVIDX_ERROR_LOCKED:
            printf("Database locked, try again\n");
            break;
        default:
            printf("Unexpected error: %s\n",
                   kvidxErrorString(lastErr));
    }

    // Clear error state for next operation
    kvidxClearError(&inst);
}
```

### Error Code to String Conversion

```c
// Convert any error code to human-readable string
const char *errStr = kvidxErrorString(KVIDX_ERROR_CORRUPT);
printf("Error description: %s\n", errStr);
```

### Helper Macros

```c
kvidxError result = kvidxRemoveRange(&inst, 0, 100, true, true, NULL);

if (kvidxIsOk(result)) {
    printf("Operation succeeded\n");
}

if (kvidxIsError(result)) {
    printf("Operation failed\n");
}
```

---

## Atomic Operations

### Conditional Insert (If Not Exists)

```c
// Only insert if key doesn't exist
kvidxError err = kvidxInsertNX(&inst, 100, 1, 0, "first", 5);

if (err == KVIDX_OK) {
    printf("Key 100 created\n");
} else if (err == KVIDX_ERROR_CONDITION_FAILED) {
    printf("Key 100 already exists\n");
}
```

### Conditional Update (If Exists)

```c
// Only update if key already exists
kvidxError err = kvidxInsertXX(&inst, 100, 2, 0, "updated", 7);

if (err == KVIDX_OK) {
    printf("Key 100 updated\n");
} else if (err == KVIDX_ERROR_CONDITION_FAILED) {
    printf("Key 100 does not exist\n");
}
```

### Get and Set Atomically

```c
uint64_t oldTerm, oldCmd;
void *oldData = NULL;
size_t oldLen = 0;

// Atomically get current value and set new value
kvidxError err = kvidxGetAndSet(&inst, 100,
    2, 0, "new-value", 9,           // New value
    &oldTerm, &oldCmd, &oldData, &oldLen  // Old value output
);

if (err == KVIDX_OK) {
    printf("Previous value: %.*s\n", (int)oldLen, (char*)oldData);
    free(oldData);  // Caller must free
}
```

### Get and Remove Atomically

```c
uint64_t term, cmd;
void *data = NULL;
size_t len = 0;

// Atomically get and delete
kvidxError err = kvidxGetAndRemove(&inst, 100, &term, &cmd, &data, &len);

if (err == KVIDX_OK) {
    printf("Removed value: %.*s\n", (int)len, (char*)data);
    free(data);  // Caller must free
} else if (err == KVIDX_ERROR_NOT_FOUND) {
    printf("Key 100 did not exist\n");
}
```

### Compare-and-Swap

```c
const char *expected = "current-value";
const char *newValue = "new-value";
bool swapped = false;

kvidxError err = kvidxCompareAndSwap(&inst, 100,
    expected, strlen(expected),    // Expected current value
    2, 0,                          // New term and cmd
    newValue, strlen(newValue),    // New value
    &swapped
);

if (err == KVIDX_OK && swapped) {
    printf("CAS succeeded\n");
} else {
    printf("CAS failed: value didn't match expected\n");
}
```

### Compare Term and Swap

```c
// Swap only if current term matches expected term
bool swapped = false;
kvidxError err = kvidxCompareTermAndSwap(&inst, 100,
    1,                             // Expected term
    2, 0,                          // New term and cmd
    "new-value", 9,                // New value
    &swapped
);
```

### Append and Prepend

```c
// Append data to existing value
size_t newLen = 0;
kvidxError err = kvidxAppend(&inst, 100, 0, 0,
                             "-suffix", 7, &newLen);
printf("New length after append: %zu\n", newLen);

// Prepend data to existing value
err = kvidxPrepend(&inst, 100, 0, 0,
                   "prefix-", 7, &newLen);
printf("New length after prepend: %zu\n", newLen);
```

### Partial Value Access

```c
// Read bytes 10-20 of value
void *partial = NULL;
size_t partialLen = 0;

kvidxError err = kvidxGetValueRange(&inst, 100,
    10,      // Offset
    10,      // Length (0 = to end)
    &partial, &partialLen
);

if (err == KVIDX_OK) {
    printf("Partial read: %.*s\n", (int)partialLen, (char*)partial);
    free(partial);
}

// Overwrite bytes starting at offset 5
size_t newLen = 0;
err = kvidxSetValueRange(&inst, 100,
    5,               // Offset
    "REPLACED", 8,   // Data to write
    &newLen
);
```

---

## TTL and Expiration

### Setting TTL

```c
// Set key to expire in 60 seconds (60000 ms)
kvidxError err = kvidxSetExpire(&inst, 100, 60000);

if (err == KVIDX_OK) {
    printf("Key 100 will expire in 60 seconds\n");
}
```

### Setting Absolute Expiration

```c
#include <time.h>

// Get current time in milliseconds
uint64_t nowMs = (uint64_t)time(NULL) * 1000;

// Expire at specific timestamp (30 seconds from now)
uint64_t expireAt = nowMs + 30000;
kvidxSetExpireAt(&inst, 100, expireAt);
```

### Checking TTL

```c
int64_t ttl = kvidxGetTTL(&inst, 100);

if (ttl == KVIDX_TTL_NOT_FOUND) {
    printf("Key 100 does not exist\n");
} else if (ttl == KVIDX_TTL_NONE) {
    printf("Key 100 has no expiration\n");
} else if (ttl > 0) {
    printf("Key 100 expires in %ld ms\n", ttl);
} else {
    printf("Key 100 has expired\n");
}
```

### Removing TTL (Persist)

```c
// Make key permanent again
kvidxError err = kvidxPersist(&inst, 100);

if (err == KVIDX_OK) {
    printf("Key 100 will no longer expire\n");
}
```

### Scanning for Expired Keys

```c
// Remove all expired keys
uint64_t expiredCount = 0;
kvidxError err = kvidxExpireScan(&inst, 0, &expiredCount);
printf("Removed %lu expired keys\n", expiredCount);

// Remove up to 100 expired keys (for incremental cleanup)
err = kvidxExpireScan(&inst, 100, &expiredCount);
```

### Insert with TTL Pattern

```c
// Insert and immediately set TTL
kvidxBegin(&inst);
kvidxInsert(&inst, 100, 0, 0, "temporary", 9);
kvidxCommit(&inst);
kvidxSetExpire(&inst, 100, 300000);  // 5 minutes
```

---

## Export and Import

### Export to Binary File

```c
kvidxExportOptions options = {
    .format = KVIDX_EXPORT_BINARY,
    .startKey = 0,
    .endKey = UINT64_MAX,
    .includeMetadata = true
};

bool progressCb(uint64_t current, uint64_t total, void *userData) {
    printf("Export progress: %lu/%lu\n", current, total);
    return true;  // Continue
}

kvidxError err = kvidxExport(&inst, "backup.bin", &options, progressCb, NULL);
if (err == KVIDX_OK) {
    printf("Export complete\n");
}
```

### Export to JSON

```c
kvidxExportOptions options = {
    .format = KVIDX_EXPORT_JSON,
    .startKey = 0,
    .endKey = UINT64_MAX,
    .includeMetadata = true,
    .prettyPrint = true
};

kvidxExport(&inst, "data.json", &options, NULL, NULL);
```

### Export to CSV

```c
kvidxExportOptions options = {
    .format = KVIDX_EXPORT_CSV,
    .startKey = 0,
    .endKey = UINT64_MAX,
    .includeMetadata = true
};

kvidxExport(&inst, "data.csv", &options, NULL, NULL);
```

### Import from File

```c
kvidxImportOptions options = {
    .format = KVIDX_EXPORT_BINARY,  // Auto-detect if BINARY
    .validateData = true,
    .clearBeforeImport = false,
    .skipDuplicates = true
};

bool progressCb(uint64_t current, uint64_t total, void *userData) {
    printf("Import progress: %lu/%lu\n", current, total);
    return true;
}

kvidxError err = kvidxImport(&inst, "backup.bin", &options, progressCb, NULL);
if (err == KVIDX_OK) {
    printf("Import complete\n");
}
```

---

## Multi-Backend Usage

### Compile-Time Backend Selection

```c
kvidxInstance inst = {0};

#if defined(KVIDXKIT_HAS_SQLITE3)
    inst.interface = kvidxInterfaceSqlite3;
    const char *path = "database.sqlite3";
#elif defined(KVIDXKIT_HAS_LMDB)
    inst.interface = kvidxInterfaceLmdb;
    const char *path = "database.lmdb";  // Directory path
#elif defined(KVIDXKIT_HAS_ROCKSDB)
    inst.interface = kvidxInterfaceRocksdb;
    const char *path = "database.rocks";
#else
    #error "No backend available"
#endif

kvidxOpen(&inst, path, NULL);
```

### Runtime Backend Discovery

```c
// List all available backends
printf("Available backends:\n");
size_t count = kvidxGetAdapterCount();
for (size_t i = 0; i < count; i++) {
    const kvidxAdapterInfo *info = kvidxGetAdapterByIndex(i);
    printf("  %zu: %s (suffix: %s)\n", i, info->name, info->pathSuffix);
}

// Select backend by name
const kvidxAdapterInfo *sqlite = kvidxGetAdapterByName("sqlite3");
if (sqlite) {
    kvidxInstance inst = {0};
    inst.interface = *sqlite->iface;
    kvidxOpen(&inst, "database.sqlite3", NULL);
}
```

### Cross-Backend Migration

```c
// Export from one backend
kvidxInstance srcInst = {0};
srcInst.interface = kvidxInterfaceLmdb;
kvidxOpen(&srcInst, "source.lmdb", NULL);

kvidxExportOptions exportOpts = {
    .format = KVIDX_EXPORT_BINARY
};
kvidxExport(&srcInst, "migration.bin", &exportOpts, NULL, NULL);
kvidxClose(&srcInst);

// Import to another backend
kvidxInstance dstInst = {0};
dstInst.interface = kvidxInterfaceSqlite3;
kvidxOpen(&dstInst, "destination.sqlite3", NULL);

kvidxImportOptions importOpts = {
    .format = KVIDX_EXPORT_BINARY
};
kvidxImport(&dstInst, "migration.bin", &importOpts, NULL, NULL);
kvidxClose(&dstInst);
```

---

## Statistics and Monitoring

### Getting Database Statistics

```c
kvidxStats stats;
kvidxError err = kvidxGetStats(&inst, &stats);

if (err == KVIDX_OK) {
    printf("Total keys: %lu\n", stats.totalKeys);
    printf("Min key: %lu\n", stats.minKey);
    printf("Max key: %lu\n", stats.maxKey);
    printf("Data size: %lu bytes\n", stats.totalDataBytes);
    printf("Database file: %lu bytes\n", stats.databaseFileSize);
    printf("WAL file: %lu bytes\n", stats.walFileSize);
    printf("Page count: %lu\n", stats.pageCount);
    printf("Page size: %lu bytes\n", stats.pageSize);
    printf("Free pages: %lu\n", stats.freePages);
}
```

### Quick Statistics

```c
uint64_t keyCount, minKey, dataSize;

kvidxGetKeyCount(&inst, &keyCount);
kvidxGetMinKey(&inst, &minKey);
kvidxGetDataSize(&inst, &dataSize);

printf("Keys: %lu, Min: %lu, Size: %lu\n", keyCount, minKey, dataSize);
```

---

## In-Memory Database

### SQLite3 In-Memory

```c
kvidxInstance inst = {0};
inst.interface = kvidxInterfaceSqlite3;

// Use special path for in-memory database
kvidxOpen(&inst, ":memory:", NULL);

// Use normally - all data in RAM
kvidxBegin(&inst);
kvidxInsert(&inst, 1, 0, 0, "temp", 4);
kvidxCommit(&inst);

kvidxClose(&inst);  // All data lost
```

---

## Complete Application Example

```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "kvidxkit.h"

// Simple session store with TTL
typedef struct {
    kvidxInstance db;
} SessionStore;

bool session_store_init(SessionStore *store, const char *path) {
    memset(store, 0, sizeof(*store));
    store->db.interface = kvidxInterfaceSqlite3;

    kvidxConfig config = kvidxConfigDefault();
    config.journalMode = KVIDX_JOURNAL_WAL;
    config.syncMode = KVIDX_SYNC_NORMAL;

    const char *err = NULL;
    return kvidxOpenWithConfig(&store->db, path, &config, &err);
}

void session_store_close(SessionStore *store) {
    kvidxClose(&store->db);
}

bool session_create(SessionStore *store, uint64_t sessionId,
                    const char *data, uint64_t ttlMs) {
    kvidxBegin(&store->db);
    bool ok = kvidxInsert(&store->db, sessionId, 0, 0, data, strlen(data));
    kvidxCommit(&store->db);

    if (ok && ttlMs > 0) {
        kvidxSetExpire(&store->db, sessionId, ttlMs);
    }
    return ok;
}

bool session_get(SessionStore *store, uint64_t sessionId,
                 char *buffer, size_t bufferSize) {
    // Check if expired
    int64_t ttl = kvidxGetTTL(&store->db, sessionId);
    if (ttl == KVIDX_TTL_NOT_FOUND || ttl <= 0) {
        return false;
    }

    uint64_t term, cmd;
    const uint8_t *data;
    size_t len;

    if (kvidxGet(&store->db, sessionId, &term, &cmd, &data, &len)) {
        size_t copyLen = len < bufferSize - 1 ? len : bufferSize - 1;
        memcpy(buffer, data, copyLen);
        buffer[copyLen] = '\0';
        return true;
    }
    return false;
}

bool session_refresh(SessionStore *store, uint64_t sessionId, uint64_t ttlMs) {
    if (kvidxExists(&store->db, sessionId)) {
        return kvidxSetExpire(&store->db, sessionId, ttlMs) == KVIDX_OK;
    }
    return false;
}

bool session_delete(SessionStore *store, uint64_t sessionId) {
    kvidxBegin(&store->db);
    bool ok = kvidxRemove(&store->db, sessionId);
    kvidxCommit(&store->db);
    return ok;
}

void session_cleanup_expired(SessionStore *store) {
    uint64_t count = 0;
    kvidxExpireScan(&store->db, 0, &count);
    if (count > 0) {
        printf("Cleaned up %lu expired sessions\n", count);
    }
}

int main(void) {
    SessionStore store;

    if (!session_store_init(&store, "sessions.db")) {
        fprintf(stderr, "Failed to initialize session store\n");
        return 1;
    }

    // Create sessions with 5-minute TTL
    session_create(&store, 1001, "{\"user\":\"alice\"}", 300000);
    session_create(&store, 1002, "{\"user\":\"bob\"}", 300000);

    // Read session
    char buffer[256];
    if (session_get(&store, 1001, buffer, sizeof(buffer))) {
        printf("Session 1001: %s\n", buffer);
    }

    // Refresh session TTL
    session_refresh(&store, 1001, 600000);  // Extend to 10 minutes

    // Delete session
    session_delete(&store, 1002);

    // Cleanup expired sessions (call periodically)
    session_cleanup_expired(&store);

    session_store_close(&store);
    return 0;
}
```
