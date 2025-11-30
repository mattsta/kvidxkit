# kvidxkit Quick Reference

A single-page cheat sheet for kvidxkit. For detailed documentation, see [API_REFERENCE.md](API_REFERENCE.md).

---

## Setup

```c
#include "kvidxkit.h"

kvidxInstance inst = {0};
inst.interface = kvidxInterfaceSqlite3;  // or kvidxInterfaceLmdb, kvidxInterfaceRocksdb
kvidxOpen(&inst, "database.db", NULL);
// ... use database ...
kvidxClose(&inst);
```

---

## CRUD Operations

| Operation         | Function                                     | Returns |
| ----------------- | -------------------------------------------- | ------- |
| **Create/Update** | `kvidxInsert(i, key, term, cmd, data, len)`  | `bool`  |
| **Read**          | `kvidxGet(i, key, &term, &cmd, &data, &len)` | `bool`  |
| **Delete**        | `kvidxRemove(i, key)`                        | `bool`  |
| **Exists**        | `kvidxExists(i, key)`                        | `bool`  |

---

## Transactions

```c
kvidxBegin(&inst);
kvidxInsert(&inst, 1, 0, 0, "data", 4);
kvidxInsert(&inst, 2, 0, 0, "more", 4);
kvidxCommit(&inst);  // or kvidxAbort(&inst) to rollback
```

---

## Iteration

### Iterator Pattern

```c
kvidxIterator *it = kvidxIteratorCreate(&inst, 0, UINT64_MAX, KVIDX_ITER_FORWARD);
while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
    uint64_t key, term, cmd;
    const uint8_t *data;
    size_t len;
    kvidxIteratorGet(it, &key, &term, &cmd, &data, &len);
}
kvidxIteratorDestroy(it);
```

### Simple Navigation

```c
kvidxGetNext(&inst, currentKey, &nextKey, &term, &cmd, &data, &len);
kvidxGetPrev(&inst, currentKey, &prevKey, &term, &cmd, &data, &len);
```

---

## Batch Insert

```c
kvidxEntry entries[] = {
    {.key = 1, .term = 0, .cmd = 0, .data = "a", .dataLen = 1},
    {.key = 2, .term = 0, .cmd = 0, .data = "b", .dataLen = 1},
};
size_t inserted = 0;
kvidxInsertBatch(&inst, entries, 2, &inserted);
```

---

## Range Operations

| Operation       | Function                                                      |
| --------------- | ------------------------------------------------------------- |
| Delete range    | `kvidxRemoveRange(i, start, end, startIncl, endIncl, &count)` |
| Count in range  | `kvidxCountRange(i, start, end, &count)`                      |
| Exists in range | `kvidxExistsInRange(i, start, end, &exists)`                  |
| Delete >= key   | `kvidxRemoveAfterNInclusive(i, key)`                          |
| Delete <= key   | `kvidxRemoveBeforeNInclusive(i, key)`                         |

---

## Conditional Writes (v0.8.0)

```c
// Only insert if key doesn't exist
kvidxInsertNX(&inst, key, term, cmd, data, len);

// Only update if key exists
kvidxInsertXX(&inst, key, term, cmd, data, len);

// General conditional insert
kvidxInsertEx(&inst, key, term, cmd, data, len, KVIDX_SET_IF_NOT_EXISTS);
```

---

## Atomic Operations (v0.8.0)

```c
// Get and set atomically (returns old value)
kvidxGetAndSet(&inst, key, newTerm, newCmd, newData, newLen,
               &oldTerm, &oldCmd, &oldData, &oldLen);

// Get and remove atomically
kvidxGetAndRemove(&inst, key, &term, &cmd, &data, &len);

// Compare-and-swap
kvidxCompareAndSwap(&inst, key, expectedData, expectedLen,
                    newTerm, newCmd, newData, newLen, &swapped);

// Append/Prepend
kvidxAppend(&inst, key, term, cmd, appendData, appendLen, &newLen);
kvidxPrepend(&inst, key, term, cmd, prependData, prependLen, &newLen);
```

---

## TTL / Expiration (v0.8.0)

```c
kvidxSetExpire(&inst, key, 60000);      // Expire in 60 seconds
kvidxSetExpireAt(&inst, key, timestamp); // Expire at Unix ms timestamp
int64_t ttl = kvidxGetTTL(&inst, key);   // Get remaining TTL
kvidxPersist(&inst, key);                // Remove expiration
kvidxExpireScan(&inst, 0, &count);       // Clean up expired keys
```

**TTL Return Values:**

- `> 0`: Milliseconds remaining
- `-1` (`KVIDX_TTL_NONE`): No expiration set
- `-2` (`KVIDX_TTL_NOT_FOUND`): Key doesn't exist

---

## Statistics

```c
kvidxStats stats;
kvidxGetStats(&inst, &stats);
// stats.totalKeys, stats.minKey, stats.maxKey, stats.totalDataBytes

// Quick stats
kvidxGetKeyCount(&inst, &count);
kvidxGetMinKey(&inst, &minKey);
kvidxMaxKey(&inst, &maxKey);
kvidxGetDataSize(&inst, &bytes);
```

---

## Configuration

```c
kvidxConfig config = kvidxConfigDefault();
config.journalMode = KVIDX_JOURNAL_WAL;
config.syncMode = KVIDX_SYNC_NORMAL;
config.cacheSizeBytes = 64 * 1024 * 1024;
kvidxOpenWithConfig(&inst, "db.sqlite3", &config, NULL);
```

| Option           | Values                                      | Default |
| ---------------- | ------------------------------------------- | ------- |
| `journalMode`    | DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF | WAL     |
| `syncMode`       | OFF, NORMAL, FULL, EXTRA                    | NORMAL  |
| `cacheSizeBytes` | Any size_t                                  | 32 MB   |
| `busyTimeoutMs`  | Milliseconds                                | 5000    |

---

## Export / Import

```c
// Export
kvidxExportOptions opts = {.format = KVIDX_EXPORT_JSON, .prettyPrint = true};
kvidxExport(&inst, "backup.json", &opts, NULL, NULL);

// Import
kvidxImportOptions iopts = {.format = KVIDX_EXPORT_BINARY, .skipDuplicates = true};
kvidxImport(&inst, "backup.bin", &iopts, NULL, NULL);
```

**Formats:** `KVIDX_EXPORT_BINARY`, `KVIDX_EXPORT_JSON`, `KVIDX_EXPORT_CSV`

---

## Error Handling

```c
kvidxError err = kvidxInsertEx(&inst, key, term, cmd, data, len, cond);
if (kvidxIsError(err)) {
    printf("Error: %s\n", kvidxGetLastErrorMessage(&inst));
    kvidxClearError(&inst);
}
```

**Common Error Codes:**
| Code | Meaning |
|------|---------|
| `KVIDX_OK` | Success |
| `KVIDX_ERROR_NOT_FOUND` | Key doesn't exist |
| `KVIDX_ERROR_DUPLICATE_KEY` | Key already exists |
| `KVIDX_ERROR_CONDITION_FAILED` | Conditional write failed |
| `KVIDX_ERROR_LOCKED` | Database locked |
| `KVIDX_ERROR_DISK_FULL` | Disk full |

---

## Backend Selection

```c
// At compile time
#ifdef KVIDXKIT_HAS_SQLITE3
inst.interface = kvidxInterfaceSqlite3;
#endif

// At runtime
const kvidxAdapterInfo *adapter = kvidxGetAdapterByName("sqlite3");
inst.interface = *adapter->iface;
```

**Backend Comparison:**
| Backend | Best For | File Extension |
|---------|----------|----------------|
| SQLite3 | Balanced, configurable | `.db`, `.sqlite3` |
| LMDB | Read-heavy, low-latency | directory |
| RocksDB | Write-heavy, large data | directory |

---

## Memory Management

**Caller must free:**

- `oldData` from `kvidxGetAndSet()`
- `data` from `kvidxGetAndRemove()`
- `data` from `kvidxGetValueRange()`

**Library-managed (don't free):**

- `data` from `kvidxGet()`
- `data` from `kvidxIteratorGet()`

---

## Build Flags

```bash
cmake .. -DKVIDXKIT_ENABLE_SQLITE3=ON   # Default: ON
cmake .. -DKVIDXKIT_ENABLE_LMDB=ON      # Default: ON
cmake .. -DKVIDXKIT_ENABLE_ROCKSDB=ON   # Default: OFF
```

Compile definitions: `KVIDXKIT_HAS_SQLITE3`, `KVIDXKIT_HAS_LMDB`, `KVIDXKIT_HAS_ROCKSDB`
