# kvidxkit Cookbook

Quick recipes for common tasks. Each recipe is self-contained and copy-paste ready.

---

## Table of Contents

1. [Counters and Sequences](#counters-and-sequences)
2. [Caching Patterns](#caching-patterns)
3. [Pagination](#pagination)
4. [Locking and Synchronization](#locking-and-synchronization)
5. [Data Structures](#data-structures)
6. [Bulk Operations](#bulk-operations)
7. [Search and Filtering](#search-and-filtering)
8. [Backup and Recovery](#backup-and-recovery)
9. [Multi-Tenant Patterns](#multi-tenant-patterns)
10. [Rate Limiting](#rate-limiting)

---

## Counters and Sequences

### Atomic Counter

```c
// Increment counter and get new value
uint64_t counter_increment(kvidxInstance *db, uint64_t counterId) {
    uint64_t term;
    const uint8_t *data;
    size_t len;

    kvidxBegin(db);

    uint64_t newValue = 1;
    if (kvidxGet(db, counterId, &term, NULL, &data, &len)) {
        if (len == sizeof(uint64_t)) {
            newValue = *(uint64_t *)data + 1;
        }
    }

    kvidxInsert(db, counterId, 0, 0, &newValue, sizeof(newValue));
    kvidxCommit(db);

    return newValue;
}

// Get current counter value
uint64_t counter_get(kvidxInstance *db, uint64_t counterId) {
    uint64_t term;
    const uint8_t *data;
    size_t len;

    if (kvidxGet(db, counterId, &term, NULL, &data, &len)) {
        if (len == sizeof(uint64_t)) {
            return *(uint64_t *)data;
        }
    }
    return 0;
}

// Reset counter
void counter_reset(kvidxInstance *db, uint64_t counterId) {
    uint64_t zero = 0;
    kvidxBegin(db);
    kvidxInsert(db, counterId, 0, 0, &zero, sizeof(zero));
    kvidxCommit(db);
}
```

### Auto-Increment ID Generator

```c
uint64_t generate_id(kvidxInstance *db) {
    // Use key 0 as the ID counter
    const uint64_t ID_COUNTER_KEY = 0;

    uint64_t oldTerm, oldCmd;
    void *oldData = NULL;
    size_t oldLen = 0;

    uint64_t newId = 1;

    // Atomic get-and-set
    kvidxError err = kvidxGetAndSet(db, ID_COUNTER_KEY,
                                    0, 0, &newId, sizeof(newId),
                                    &oldTerm, &oldCmd, &oldData, &oldLen);

    if (err == KVIDX_OK && oldData && oldLen == sizeof(uint64_t)) {
        newId = *(uint64_t *)oldData + 1;
        // Update with incremented value
        kvidxBegin(db);
        kvidxInsert(db, ID_COUNTER_KEY, 0, 0, &newId, sizeof(newId));
        kvidxCommit(db);
        free(oldData);
    }

    return newId;
}
```

### Sequence with Gap Detection

```c
typedef struct {
    uint64_t expected;
    uint64_t *gaps;
    size_t gapCount;
    size_t gapCapacity;
} GapDetector;

void detect_gaps(kvidxInstance *db, uint64_t start, uint64_t end,
                 GapDetector *detector) {
    detector->expected = start;
    detector->gapCount = 0;

    kvidxIterator *it = kvidxIteratorCreate(db, start, end, KVIDX_ITER_FORWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key = kvidxIteratorKey(it);

        while (detector->expected < key) {
            // Found a gap
            if (detector->gapCount < detector->gapCapacity) {
                detector->gaps[detector->gapCount++] = detector->expected;
            }
            detector->expected++;
        }
        detector->expected = key + 1;
    }

    kvidxIteratorDestroy(it);
}
```

---

## Caching Patterns

### Simple Cache with TTL

```c
typedef struct {
    kvidxInstance db;
    uint64_t defaultTtlMs;
} Cache;

bool cache_set(Cache *cache, uint64_t key, const void *value, size_t len) {
    kvidxBegin(&cache->db);
    bool ok = kvidxInsert(&cache->db, key, 0, 0, value, len);
    kvidxCommit(&cache->db);

    if (ok && cache->defaultTtlMs > 0) {
        kvidxSetExpire(&cache->db, key, cache->defaultTtlMs);
    }
    return ok;
}

bool cache_get(Cache *cache, uint64_t key, const uint8_t **value, size_t *len) {
    // Check if expired
    int64_t ttl = kvidxGetTTL(&cache->db, key);
    if (ttl == KVIDX_TTL_NOT_FOUND || ttl <= 0) {
        return false;
    }

    return kvidxGet(&cache->db, key, NULL, NULL, value, len);
}

void cache_delete(Cache *cache, uint64_t key) {
    kvidxBegin(&cache->db);
    kvidxRemove(&cache->db, key);
    kvidxCommit(&cache->db);
}

void cache_cleanup(Cache *cache) {
    uint64_t count = 0;
    kvidxExpireScan(&cache->db, 0, &count);
}
```

### Write-Through Cache Pattern

```c
typedef bool (*DataLoader)(uint64_t key, void **data, size_t *len, void *ctx);
typedef bool (*DataWriter)(uint64_t key, const void *data, size_t len, void *ctx);

typedef struct {
    kvidxInstance db;
    DataLoader loader;
    DataWriter writer;
    void *ctx;
    uint64_t ttlMs;
} WriteThruCache;

bool cache_get_or_load(WriteThruCache *cache, uint64_t key,
                       void **data, size_t *len) {
    // Try cache first
    const uint8_t *cached;
    size_t cachedLen;
    if (kvidxGet(&cache->db, key, NULL, NULL, &cached, &cachedLen)) {
        int64_t ttl = kvidxGetTTL(&cache->db, key);
        if (ttl == KVIDX_TTL_NONE || ttl > 0) {
            *data = malloc(cachedLen);
            memcpy(*data, cached, cachedLen);
            *len = cachedLen;
            return true;
        }
    }

    // Load from source
    if (!cache->loader(key, data, len, cache->ctx)) {
        return false;
    }

    // Store in cache
    kvidxBegin(&cache->db);
    kvidxInsert(&cache->db, key, 0, 0, *data, *len);
    kvidxCommit(&cache->db);

    if (cache->ttlMs > 0) {
        kvidxSetExpire(&cache->db, key, cache->ttlMs);
    }

    return true;
}

bool cache_put_through(WriteThruCache *cache, uint64_t key,
                       const void *data, size_t len) {
    // Write to source first
    if (!cache->writer(key, data, len, cache->ctx)) {
        return false;
    }

    // Update cache
    kvidxBegin(&cache->db);
    kvidxInsert(&cache->db, key, 0, 0, data, len);
    kvidxCommit(&cache->db);

    if (cache->ttlMs > 0) {
        kvidxSetExpire(&cache->db, key, cache->ttlMs);
    }

    return true;
}
```

### LRU Approximation with TTL Refresh

```c
// Refresh TTL on every access to approximate LRU behavior
bool lru_cache_get(Cache *cache, uint64_t key, const uint8_t **value, size_t *len) {
    if (!kvidxGet(&cache->db, key, NULL, NULL, value, len)) {
        return false;
    }

    // Refresh TTL on access
    kvidxSetExpire(&cache->db, key, cache->defaultTtlMs);
    return true;
}
```

---

## Pagination

### Cursor-Based Pagination

```c
typedef struct {
    uint64_t *keys;
    size_t count;
    uint64_t nextCursor;  // 0 if no more pages
    bool hasMore;
} Page;

Page paginate(kvidxInstance *db, uint64_t cursor, size_t pageSize) {
    Page page = {0};
    page.keys = malloc(pageSize * sizeof(uint64_t));
    page.count = 0;

    uint64_t startKey = cursor == 0 ? 0 : cursor;

    kvidxIterator *it = kvidxIteratorCreate(db, startKey, UINT64_MAX,
                                            KVIDX_ITER_FORWARD);

    // Skip cursor key if it exists (we want items AFTER cursor)
    if (cursor != 0) {
        kvidxIteratorNext(it);  // Move to cursor position
        if (kvidxIteratorValid(it) && kvidxIteratorKey(it) == cursor) {
            // Skip the cursor itself
        }
    }

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it) &&
           page.count < pageSize) {
        page.keys[page.count++] = kvidxIteratorKey(it);
    }

    // Check if there are more items
    if (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        page.hasMore = true;
        page.nextCursor = page.keys[page.count - 1];
    }

    kvidxIteratorDestroy(it);
    return page;
}

void page_free(Page *page) {
    free(page->keys);
    page->keys = NULL;
    page->count = 0;
}
```

### Offset-Based Pagination

```c
Page paginate_offset(kvidxInstance *db, size_t offset, size_t limit) {
    Page page = {0};
    page.keys = malloc(limit * sizeof(uint64_t));
    page.count = 0;

    kvidxIterator *it = kvidxIteratorCreate(db, 0, UINT64_MAX,
                                            KVIDX_ITER_FORWARD);

    // Skip offset entries
    size_t skipped = 0;
    while (kvidxIteratorNext(it) && kvidxIteratorValid(it) && skipped < offset) {
        skipped++;
    }

    // Collect limit entries
    while (kvidxIteratorValid(it) && page.count < limit) {
        page.keys[page.count++] = kvidxIteratorKey(it);
        if (!kvidxIteratorNext(it)) break;
    }

    // Check for more
    page.hasMore = kvidxIteratorValid(it);

    kvidxIteratorDestroy(it);
    return page;
}
```

### Reverse Pagination (Latest First)

```c
Page paginate_latest(kvidxInstance *db, uint64_t beforeKey, size_t pageSize) {
    Page page = {0};
    page.keys = malloc(pageSize * sizeof(uint64_t));
    page.count = 0;

    uint64_t endKey = beforeKey == 0 ? UINT64_MAX : beforeKey - 1;

    kvidxIterator *it = kvidxIteratorCreate(db, 0, endKey, KVIDX_ITER_BACKWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it) &&
           page.count < pageSize) {
        page.keys[page.count++] = kvidxIteratorKey(it);
    }

    page.hasMore = kvidxIteratorNext(it) && kvidxIteratorValid(it);
    if (page.hasMore && page.count > 0) {
        page.nextCursor = page.keys[page.count - 1];
    }

    kvidxIteratorDestroy(it);
    return page;
}
```

---

## Locking and Synchronization

### Distributed Lock with TTL

```c
#define LOCK_PREFIX 0xFFFFFFFF00000000ULL

typedef struct {
    kvidxInstance *db;
    uint64_t lockKey;
    uint64_t ownerId;
    bool held;
} DistributedLock;

bool lock_acquire(DistributedLock *lock, uint64_t resourceId,
                  uint64_t ownerId, uint64_t ttlMs) {
    lock->lockKey = LOCK_PREFIX | resourceId;
    lock->ownerId = ownerId;
    lock->held = false;

    // Try to acquire with NX (only if not exists)
    kvidxError err = kvidxInsertNX(lock->db, lock->lockKey, ownerId, 0,
                                   &ownerId, sizeof(ownerId));

    if (err == KVIDX_OK) {
        kvidxSetExpire(lock->db, lock->lockKey, ttlMs);
        lock->held = true;
        return true;
    }

    return false;
}

bool lock_release(DistributedLock *lock) {
    if (!lock->held) return false;

    // Only release if we still own it (CAS on owner ID)
    bool swapped = false;
    uint64_t dummy = 0;

    kvidxError err = kvidxCompareAndSwap(lock->db, lock->lockKey,
                                         &lock->ownerId, sizeof(lock->ownerId),
                                         0, 0, &dummy, 0, &swapped);

    if (err == KVIDX_OK && swapped) {
        kvidxBegin(lock->db);
        kvidxRemove(lock->db, lock->lockKey);
        kvidxCommit(lock->db);
        lock->held = false;
        return true;
    }

    return false;
}

bool lock_extend(DistributedLock *lock, uint64_t additionalMs) {
    if (!lock->held) return false;

    // Verify we still own it
    uint64_t term;
    const uint8_t *data;
    size_t len;

    if (kvidxGet(lock->db, lock->lockKey, &term, NULL, &data, &len)) {
        if (len == sizeof(uint64_t) && *(uint64_t *)data == lock->ownerId) {
            // Extend TTL
            int64_t currentTtl = kvidxGetTTL(lock->db, lock->lockKey);
            if (currentTtl > 0) {
                kvidxSetExpire(lock->db, lock->lockKey, currentTtl + additionalMs);
                return true;
            }
        }
    }

    lock->held = false;
    return false;
}
```

### Optimistic Locking with Versions

```c
typedef struct {
    uint64_t version;
    // ... other fields
    char data[256];
} VersionedRecord;

bool optimistic_update(kvidxInstance *db, uint64_t key,
                       VersionedRecord *record,
                       bool (*modifier)(VersionedRecord *)) {
    int maxRetries = 3;

    for (int attempt = 0; attempt < maxRetries; attempt++) {
        // Read current version
        uint64_t term;
        const uint8_t *data;
        size_t len;

        if (!kvidxGet(db, key, &term, NULL, &data, &len)) {
            return false;  // Key doesn't exist
        }

        memcpy(record, data, len < sizeof(*record) ? len : sizeof(*record));
        uint64_t expectedVersion = record->version;

        // Apply modification
        if (!modifier(record)) {
            return false;  // Modification rejected
        }

        record->version = expectedVersion + 1;

        // Try to update with version check
        bool swapped = false;
        kvidxError err = kvidxCompareTermAndSwap(db, key,
                                                 expectedVersion,
                                                 record->version, 0,
                                                 record, sizeof(*record),
                                                 &swapped);

        if (err == KVIDX_OK && swapped) {
            return true;  // Success
        }

        // Version conflict, retry
    }

    return false;  // Max retries exceeded
}
```

---

## Data Structures

### Stack (LIFO)

```c
typedef struct {
    kvidxInstance db;
    uint64_t stackId;  // Prefix for this stack's keys
} Stack;

bool stack_push(Stack *s, const void *data, size_t len) {
    uint64_t topKey = 0;
    kvidxMaxKey(&s->db, &topKey);

    uint64_t newKey = (topKey & 0xFFFFFFFF00000000ULL) == (s->stackId << 32)
                    ? topKey + 1
                    : (s->stackId << 32) | 1;

    kvidxBegin(&s->db);
    bool ok = kvidxInsert(&s->db, newKey, 0, 0, data, len);
    kvidxCommit(&s->db);
    return ok;
}

bool stack_pop(Stack *s, void **data, size_t *len) {
    uint64_t startKey = s->stackId << 32;
    uint64_t endKey = ((s->stackId + 1) << 32) - 1;

    // Find max key in our range
    kvidxIterator *it = kvidxIteratorCreate(&s->db, startKey, endKey,
                                            KVIDX_ITER_BACKWARD);

    if (!kvidxIteratorNext(it) || !kvidxIteratorValid(it)) {
        kvidxIteratorDestroy(it);
        return false;  // Stack empty
    }

    uint64_t topKey = kvidxIteratorKey(it);
    kvidxIteratorDestroy(it);

    // Atomic get and remove
    uint64_t term, cmd;
    return kvidxGetAndRemove(&s->db, topKey, &term, &cmd, data, len) == KVIDX_OK;
}

bool stack_peek(Stack *s, const uint8_t **data, size_t *len) {
    uint64_t startKey = s->stackId << 32;
    uint64_t endKey = ((s->stackId + 1) << 32) - 1;

    kvidxIterator *it = kvidxIteratorCreate(&s->db, startKey, endKey,
                                            KVIDX_ITER_BACKWARD);

    if (!kvidxIteratorNext(it) || !kvidxIteratorValid(it)) {
        kvidxIteratorDestroy(it);
        return false;
    }

    uint64_t key;
    kvidxIteratorGet(it, &key, NULL, NULL, data, len);
    kvidxIteratorDestroy(it);
    return true;
}
```

### Sorted Set (Score-Based)

```c
// Store entries with score in key high bits, id in low bits
// Key format: (score << 32) | id

typedef struct {
    kvidxInstance db;
} SortedSet;

bool zset_add(SortedSet *zs, uint32_t id, uint32_t score,
              const void *data, size_t len) {
    uint64_t key = ((uint64_t)score << 32) | id;

    kvidxBegin(&zs->db);
    bool ok = kvidxInsert(&zs->db, key, 0, 0, data, len);
    kvidxCommit(&zs->db);
    return ok;
}

// Get top N by score (highest first)
size_t zset_top_n(SortedSet *zs, size_t n,
                  void (*callback)(uint32_t id, uint32_t score,
                                  const uint8_t *data, size_t len)) {
    size_t count = 0;

    kvidxIterator *it = kvidxIteratorCreate(&zs->db, 0, UINT64_MAX,
                                            KVIDX_ITER_BACKWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it) && count < n) {
        uint64_t key;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, NULL, NULL, &data, &len);

        uint32_t score = (uint32_t)(key >> 32);
        uint32_t id = (uint32_t)(key & 0xFFFFFFFF);

        callback(id, score, data, len);
        count++;
    }

    kvidxIteratorDestroy(it);
    return count;
}

// Get entries in score range
size_t zset_range_by_score(SortedSet *zs, uint32_t minScore, uint32_t maxScore,
                           void (*callback)(uint32_t id, uint32_t score,
                                           const uint8_t *data, size_t len)) {
    uint64_t startKey = (uint64_t)minScore << 32;
    uint64_t endKey = ((uint64_t)(maxScore + 1) << 32) - 1;

    size_t count = 0;
    kvidxIterator *it = kvidxIteratorCreate(&zs->db, startKey, endKey,
                                            KVIDX_ITER_FORWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, NULL, NULL, &data, &len);

        uint32_t score = (uint32_t)(key >> 32);
        uint32_t id = (uint32_t)(key & 0xFFFFFFFF);

        callback(id, score, data, len);
        count++;
    }

    kvidxIteratorDestroy(it);
    return count;
}
```

### Append-Only Log

```c
typedef struct {
    kvidxInstance db;
    uint64_t nextSeq;
} AppendLog;

bool log_init(AppendLog *log, const char *path) {
    memset(log, 0, sizeof(*log));
    log->db.interface = kvidxInterfaceSqlite3;

    if (!kvidxOpen(&log->db, path, NULL)) {
        return false;
    }

    // Find next sequence number
    uint64_t maxKey = 0;
    if (kvidxMaxKey(&log->db, &maxKey)) {
        log->nextSeq = maxKey + 1;
    } else {
        log->nextSeq = 1;
    }

    return true;
}

uint64_t log_append(AppendLog *log, const void *entry, size_t len) {
    uint64_t seq = log->nextSeq++;

    kvidxBegin(&log->db);
    kvidxInsert(&log->db, seq, 0, 0, entry, len);
    kvidxCommit(&log->db);
    kvidxFsync(&log->db);  // Ensure durability

    return seq;
}

void log_replay(AppendLog *log, uint64_t fromSeq,
                void (*handler)(uint64_t seq, const uint8_t *data, size_t len)) {
    kvidxIterator *it = kvidxIteratorCreate(&log->db, fromSeq, UINT64_MAX,
                                            KVIDX_ITER_FORWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, NULL, NULL, &data, &len);
        handler(key, data, len);
    }

    kvidxIteratorDestroy(it);
}

void log_truncate_before(AppendLog *log, uint64_t seq) {
    uint64_t deleted = 0;
    kvidxRemoveRange(&log->db, 0, seq - 1, true, true, &deleted);
}
```

---

## Bulk Operations

### Efficient Bulk Import

```c
void bulk_import(kvidxInstance *db, const char *csvPath) {
    // Optimize for bulk import
    kvidxConfig config = kvidxConfigDefault();
    config.syncMode = KVIDX_SYNC_OFF;
    config.journalMode = KVIDX_JOURNAL_OFF;
    kvidxUpdateConfig(db, &config);

    FILE *f = fopen(csvPath, "r");
    if (!f) return;

    kvidxEntry *batch = malloc(10000 * sizeof(kvidxEntry));
    char **dataBuffers = malloc(10000 * sizeof(char *));
    size_t batchSize = 0;

    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        // Parse CSV line into key, data
        uint64_t key;
        char *data = malloc(512);
        if (sscanf(line, "%lu,%511s", &key, data) == 2) {
            batch[batchSize].key = key;
            batch[batchSize].term = 0;
            batch[batchSize].cmd = 0;
            batch[batchSize].data = data;
            batch[batchSize].dataLen = strlen(data);
            dataBuffers[batchSize] = data;
            batchSize++;

            if (batchSize == 10000) {
                size_t inserted = 0;
                kvidxInsertBatch(db, batch, batchSize, &inserted);

                // Free buffers
                for (size_t i = 0; i < batchSize; i++) {
                    free(dataBuffers[i]);
                }
                batchSize = 0;
            }
        }
    }

    // Insert remaining
    if (batchSize > 0) {
        size_t inserted = 0;
        kvidxInsertBatch(db, batch, batchSize, &inserted);
        for (size_t i = 0; i < batchSize; i++) {
            free(dataBuffers[i]);
        }
    }

    fclose(f);
    free(batch);
    free(dataBuffers);

    // Restore safe settings and sync
    config.syncMode = KVIDX_SYNC_NORMAL;
    config.journalMode = KVIDX_JOURNAL_WAL;
    kvidxUpdateConfig(db, &config);
    kvidxFsync(db);
}
```

### Parallel Export with Ranges

```c
typedef struct {
    kvidxInstance *db;
    uint64_t startKey;
    uint64_t endKey;
    FILE *output;
} ExportTask;

void export_range(ExportTask *task) {
    kvidxIterator *it = kvidxIteratorCreate(task->db, task->startKey,
                                            task->endKey, KVIDX_ITER_FORWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key, term, cmd;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, &term, &cmd, &data, &len);

        // Write to file
        fprintf(task->output, "%lu,%lu,%lu,%.*s\n",
                key, term, cmd, (int)len, data);
    }

    kvidxIteratorDestroy(it);
}

// Usage: Split key space into ranges and export in parallel threads
```

---

## Search and Filtering

### Prefix Scan (Using Key Ranges)

```c
// For keys with embedded prefix: (prefix << 48) | id
size_t scan_by_prefix(kvidxInstance *db, uint16_t prefix,
                      void (*callback)(uint64_t id, const uint8_t *data,
                                      size_t len)) {
    uint64_t startKey = (uint64_t)prefix << 48;
    uint64_t endKey = ((uint64_t)(prefix + 1) << 48) - 1;

    size_t count = 0;
    kvidxIterator *it = kvidxIteratorCreate(db, startKey, endKey,
                                            KVIDX_ITER_FORWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, NULL, NULL, &data, &len);

        uint64_t id = key & 0xFFFFFFFFFFFF;
        callback(id, data, len);
        count++;
    }

    kvidxIteratorDestroy(it);
    return count;
}
```

### Filter During Iteration

```c
typedef bool (*FilterFunc)(uint64_t key, const uint8_t *data, size_t len,
                          void *ctx);

size_t scan_with_filter(kvidxInstance *db, uint64_t start, uint64_t end,
                        FilterFunc filter, void *filterCtx,
                        void (*callback)(uint64_t key, const uint8_t *data,
                                        size_t len)) {
    size_t count = 0;
    kvidxIterator *it = kvidxIteratorCreate(db, start, end, KVIDX_ITER_FORWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, NULL, NULL, &data, &len);

        if (filter(key, data, len, filterCtx)) {
            callback(key, data, len);
            count++;
        }
    }

    kvidxIteratorDestroy(it);
    return count;
}
```

---

## Backup and Recovery

### Hot Backup

```c
bool hot_backup(kvidxInstance *db, const char *backupPath) {
    kvidxExportOptions opts = {
        .format = KVIDX_EXPORT_BINARY,
        .startKey = 0,
        .endKey = UINT64_MAX
    };

    kvidxError err = kvidxExport(db, backupPath, &opts, NULL, NULL);
    return err == KVIDX_OK;
}

bool restore_backup(kvidxInstance *db, const char *backupPath) {
    kvidxImportOptions opts = {
        .format = KVIDX_EXPORT_BINARY,
        .clearBeforeImport = true,
        .skipDuplicates = false
    };

    kvidxError err = kvidxImport(db, backupPath, &opts, NULL, NULL);
    return err == KVIDX_OK;
}
```

### Incremental Backup

```c
typedef struct {
    uint64_t lastBackupKey;
    char backupDir[256];
    int backupCount;
} IncrementalBackup;

bool incremental_backup(kvidxInstance *db, IncrementalBackup *state) {
    uint64_t maxKey = 0;
    if (!kvidxMaxKey(db, &maxKey)) {
        return true;  // Empty, nothing to backup
    }

    if (maxKey <= state->lastBackupKey) {
        return true;  // No new data
    }

    char filename[512];
    snprintf(filename, sizeof(filename), "%s/backup_%d.bin",
             state->backupDir, state->backupCount++);

    kvidxExportOptions opts = {
        .format = KVIDX_EXPORT_BINARY,
        .startKey = state->lastBackupKey + 1,
        .endKey = maxKey
    };

    kvidxError err = kvidxExport(db, filename, &opts, NULL, NULL);
    if (err == KVIDX_OK) {
        state->lastBackupKey = maxKey;
        return true;
    }
    return false;
}
```

---

## Multi-Tenant Patterns

### Tenant-Prefixed Keys

```c
// Key format: (tenantId << 48) | entityId

uint64_t make_tenant_key(uint16_t tenantId, uint64_t entityId) {
    return ((uint64_t)tenantId << 48) | (entityId & 0xFFFFFFFFFFFF);
}

void parse_tenant_key(uint64_t key, uint16_t *tenantId, uint64_t *entityId) {
    *tenantId = (uint16_t)(key >> 48);
    *entityId = key & 0xFFFFFFFFFFFF;
}

// Get all keys for a tenant
size_t get_tenant_data(kvidxInstance *db, uint16_t tenantId,
                       void (*callback)(uint64_t entityId,
                                       const uint8_t *data, size_t len)) {
    uint64_t startKey = (uint64_t)tenantId << 48;
    uint64_t endKey = ((uint64_t)(tenantId + 1) << 48) - 1;

    size_t count = 0;
    kvidxIterator *it = kvidxIteratorCreate(db, startKey, endKey,
                                            KVIDX_ITER_FORWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, NULL, NULL, &data, &len);

        uint64_t entityId = key & 0xFFFFFFFFFFFF;
        callback(entityId, data, len);
        count++;
    }

    kvidxIteratorDestroy(it);
    return count;
}

// Delete all tenant data
uint64_t delete_tenant(kvidxInstance *db, uint16_t tenantId) {
    uint64_t startKey = (uint64_t)tenantId << 48;
    uint64_t endKey = ((uint64_t)(tenantId + 1) << 48) - 1;

    uint64_t deleted = 0;
    kvidxRemoveRange(db, startKey, endKey, true, true, &deleted);
    return deleted;
}
```

---

## Rate Limiting

### Sliding Window Rate Limiter

```c
typedef struct {
    kvidxInstance *db;
    uint64_t windowMs;
    uint64_t maxRequests;
} RateLimiter;

bool rate_limit_check(RateLimiter *rl, uint64_t clientId) {
    uint64_t now = current_time_ms();
    uint64_t windowStart = now - rl->windowMs;

    // Key format: (clientId << 32) | timestamp_bucket
    uint64_t keyPrefix = clientId << 32;
    uint64_t startKey = keyPrefix | (windowStart / 1000);
    uint64_t endKey = keyPrefix | (now / 1000);

    // Count requests in window
    uint64_t count = 0;
    kvidxIterator *it = kvidxIteratorCreate(rl->db, startKey, endKey,
                                            KVIDX_ITER_FORWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t term;
        kvidxIteratorGet(it, NULL, &term, NULL, NULL, NULL);
        count += term;  // term holds request count for bucket
    }
    kvidxIteratorDestroy(it);

    if (count >= rl->maxRequests) {
        return false;  // Rate limited
    }

    // Record this request
    uint64_t bucketKey = keyPrefix | (now / 1000);
    uint64_t term = 1;
    const uint8_t *data;
    size_t len;

    kvidxBegin(rl->db);
    if (kvidxGet(rl->db, bucketKey, &term, NULL, &data, &len)) {
        term++;
    }
    kvidxInsert(rl->db, bucketKey, term, 0, "", 0);
    kvidxCommit(rl->db);

    // Set TTL to auto-cleanup old buckets
    kvidxSetExpire(rl->db, bucketKey, rl->windowMs + 1000);

    return true;
}
```

### Token Bucket Rate Limiter

```c
typedef struct {
    uint64_t tokens;
    uint64_t lastRefill;
} TokenBucket;

bool token_bucket_acquire(kvidxInstance *db, uint64_t bucketId,
                          uint64_t maxTokens, uint64_t refillRate,
                          uint64_t tokensNeeded) {
    uint64_t now = current_time_ms();

    kvidxBegin(db);

    TokenBucket bucket = {maxTokens, now};
    uint64_t term;
    const uint8_t *data;
    size_t len;

    if (kvidxGet(db, bucketId, &term, NULL, &data, &len)) {
        if (len == sizeof(bucket)) {
            memcpy(&bucket, data, sizeof(bucket));
        }
    }

    // Refill tokens
    uint64_t elapsed = now - bucket.lastRefill;
    uint64_t newTokens = (elapsed * refillRate) / 1000;
    bucket.tokens = (bucket.tokens + newTokens > maxTokens)
                  ? maxTokens
                  : bucket.tokens + newTokens;
    bucket.lastRefill = now;

    // Check if enough tokens
    if (bucket.tokens < tokensNeeded) {
        kvidxAbort(db);
        return false;
    }

    // Consume tokens
    bucket.tokens -= tokensNeeded;
    kvidxInsert(db, bucketId, 0, 0, &bucket, sizeof(bucket));
    kvidxCommit(db);

    return true;
}
```

---

## Tips and Best Practices

1. **Use batch operations** for bulk inserts (10-100x faster)
2. **Set TTLs** for temporary data to enable automatic cleanup
3. **Use iterators** for large scans instead of loading all data
4. **Close iterators promptly** to release resources
5. **Use transactions** to group related operations
6. **Handle errors** - check return values and error codes
7. **Free returned data** from `GetAndSet`, `GetAndRemove`, `GetValueRange`
8. **Call ExpireScan periodically** if using TTLs
9. **Monitor statistics** to track database growth
