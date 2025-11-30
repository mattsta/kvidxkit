# kvidxkit API Reference

Complete function reference for kvidxkit v0.8.0.

---

## Table of Contents

1. [Core Types](#core-types)
2. [Lifecycle Management](#lifecycle-management)
3. [Transaction Management](#transaction-management)
4. [Read Operations](#read-operations)
5. [Write Operations](#write-operations)
6. [Batch Operations](#batch-operations)
7. [Range Operations](#range-operations)
8. [Iterator API](#iterator-api)
9. [Statistics API](#statistics-api)
10. [Configuration API](#configuration-api)
11. [Export/Import API](#exportimport-api)
12. [Storage Primitives](#storage-primitives)
13. [TTL/Expiration](#ttlexpiration)
14. [Error Handling](#error-handling)
15. [Registry API](#registry-api)

---

## Core Types

### kvidxInstance

The primary database handle.

```c
typedef struct kvidxInstance {
    void *kvidxdata;               // Backend-specific private data
    void *clientdata;              // User-provided context
    kvidxInterface interface;      // Function pointer table
    kvidxError lastError;          // Last error code
    char lastErrorMessage[256];    // Last error message
    bool transactionActive;        // Transaction state
    kvidxConfig config;            // Current configuration
    bool configInitialized;        // Config initialized flag
} kvidxInstance;
```

### kvidxEntry

Entry structure for batch operations.

```c
typedef struct {
    uint64_t key;        // Entry key
    uint64_t term;       // Term/version metadata
    uint64_t cmd;        // Command/type metadata
    const void *data;    // Value data pointer
    size_t dataLen;      // Value data length
} kvidxEntry;
```

### kvidxStats

Database statistics structure.

```c
struct kvidxStats {
    uint64_t totalKeys;        // Total number of keys
    uint64_t minKey;           // Minimum key value
    uint64_t maxKey;           // Maximum key value
    uint64_t totalDataBytes;   // Total data size in bytes
    uint64_t databaseFileSize; // Database file size
    uint64_t walFileSize;      // WAL file size (if applicable)
    uint64_t pageCount;        // Total pages
    uint64_t pageSize;         // Page size in bytes
    uint64_t freePages;        // Number of free pages
};
```

---

## Lifecycle Management

### kvidxOpen

Open a database connection.

```c
bool kvidxOpen(kvidxInstance *i, const char *filename, const char **err);
```

**Parameters:**

- `i`: Instance handle (must be zero-initialized with interface set)
- `filename`: Path to database file
- `err`: Optional pointer to receive error message

**Returns:** `true` on success, `false` on failure

**Example:**

```c
kvidxInstance inst = {0};
inst.interface = kvidxInterfaceSqlite3;
const char *err = NULL;
if (!kvidxOpen(&inst, "mydb.sqlite3", &err)) {
    fprintf(stderr, "Open failed: %s\n", err);
}
```

---

### kvidxOpenWithConfig

Open a database with custom configuration.

```c
bool kvidxOpenWithConfig(kvidxInstance *i, const char *filename,
                         const kvidxConfig *config, const char **err);
```

**Parameters:**

- `i`: Instance handle
- `filename`: Path to database file
- `config`: Configuration structure (NULL for defaults)
- `err`: Optional error message pointer

**Returns:** `true` on success

---

### kvidxClose

Close database connection and release resources.

```c
bool kvidxClose(kvidxInstance *i);
```

**Parameters:**

- `i`: Instance handle

**Returns:** `true` on success

---

### kvidxFsync

Force synchronization to disk.

```c
bool kvidxFsync(kvidxInstance *i);
```

**Returns:** `true` on success

---

## Transaction Management

### kvidxBegin

Start a new transaction.

```c
bool kvidxBegin(kvidxInstance *i);
```

**Returns:** `true` on success, `false` if transaction already active

**Note:** Transactions cannot be nested. Starting a transaction while one is active will fail.

---

### kvidxCommit

Commit the current transaction.

```c
bool kvidxCommit(kvidxInstance *i);
```

**Returns:** `true` on success, `false` if no active transaction or commit failed

---

### kvidxAbort

Abort (rollback) the current transaction.

```c
bool kvidxAbort(kvidxInstance *i);
```

**Returns:** `true` on success

---

## Read Operations

### kvidxGet

Retrieve an entry by key.

```c
bool kvidxGet(kvidxInstance *i, uint64_t key, uint64_t *term, uint64_t *cmd,
              const uint8_t **data, size_t *len);
```

**Parameters:**

- `i`: Instance handle
- `key`: Key to retrieve
- `term`: Receives term value (can be NULL)
- `cmd`: Receives command value (can be NULL)
- `data`: Receives pointer to data (can be NULL)
- `len`: Receives data length (can be NULL)

**Returns:** `true` if key found, `false` if not found

**Note:** The data pointer is valid only until the next operation on this instance.

---

### kvidxGetNext

Get the next entry after a given key.

```c
bool kvidxGetNext(kvidxInstance *i, uint64_t previousKey, uint64_t *nextKey,
                  uint64_t *nextTerm, uint64_t *cmd, const uint8_t **data,
                  size_t *len);
```

**Parameters:**

- `previousKey`: Key to search after
- `nextKey`: Receives the next key

**Returns:** `true` if a next key exists, `false` if previousKey was the last key

---

### kvidxGetPrev

Get the previous entry before a given key.

```c
bool kvidxGetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                  uint64_t *prevTerm, uint64_t *cmd, const uint8_t **data,
                  size_t *len);
```

**Returns:** `true` if a previous key exists

---

### kvidxExists

Check if a key exists.

```c
bool kvidxExists(kvidxInstance *i, uint64_t key);
```

**Returns:** `true` if key exists, `false` otherwise

---

### kvidxExistsDual

Check if a key exists with a specific term.

```c
bool kvidxExistsDual(kvidxInstance *i, uint64_t key, uint64_t term);
```

**Returns:** `true` if key exists AND has the specified term

---

### kvidxMaxKey

Get the maximum key in the database.

```c
bool kvidxMaxKey(kvidxInstance *i, uint64_t *key);
```

**Returns:** `true` if database is non-empty, `false` if empty

---

## Write Operations

### kvidxInsert

Insert or update an entry.

```c
bool kvidxInsert(kvidxInstance *i, uint64_t key, uint64_t term, uint64_t cmd,
                 const void *data, size_t dataLen);
```

**Parameters:**

- `key`: Key to insert
- `term`: Term/version metadata
- `cmd`: Command/type metadata
- `data`: Value data
- `dataLen`: Length of data

**Returns:** `true` on success

---

### kvidxRemove

Delete an entry by key.

```c
bool kvidxRemove(kvidxInstance *i, uint64_t key);
```

**Returns:** `true` on success (even if key didn't exist)

---

### kvidxRemoveAfterNInclusive

Delete key and all keys greater than it.

```c
bool kvidxRemoveAfterNInclusive(kvidxInstance *i, uint64_t key);
```

**Example:** If keys are {1, 5, 10, 15}, calling with key=10 removes {10, 15}

---

### kvidxRemoveBeforeNInclusive

Delete key and all keys less than it.

```c
bool kvidxRemoveBeforeNInclusive(kvidxInstance *i, uint64_t key);
```

---

## Batch Operations

### kvidxInsertBatch

Insert multiple entries in a single transaction.

```c
bool kvidxInsertBatch(kvidxInstance *i, const kvidxEntry *entries, size_t count,
                      size_t *insertedCount);
```

**Parameters:**

- `entries`: Array of entries to insert
- `count`: Number of entries
- `insertedCount`: Receives count of successfully inserted entries

**Returns:** `true` if all entries inserted, `false` if any failed

**Note:** On failure, `insertedCount` indicates how many were inserted before the error.

---

### kvidxInsertBatchEx

Insert entries with a filter callback.

```c
typedef bool (*kvidxBatchCallback)(size_t index, const kvidxEntry *entry,
                                   void *userData);

bool kvidxInsertBatchEx(kvidxInstance *i, const kvidxEntry *entries,
                        size_t count, kvidxBatchCallback callback,
                        void *userData, size_t *insertedCount);
```

**Callback Returns:** `true` to insert entry, `false` to skip

---

## Range Operations

### kvidxRemoveRange

Remove keys in a range with configurable boundaries.

```c
kvidxError kvidxRemoveRange(kvidxInstance *i, uint64_t startKey,
                            uint64_t endKey, bool startInclusive,
                            bool endInclusive, uint64_t *deletedCount);
```

**Parameters:**

- `startKey`: Start of range
- `endKey`: End of range
- `startInclusive`: Include startKey in deletion
- `endInclusive`: Include endKey in deletion
- `deletedCount`: Receives number of deleted entries

---

### kvidxCountRange

Count keys in a range.

```c
kvidxError kvidxCountRange(kvidxInstance *i, uint64_t startKey, uint64_t endKey,
                           uint64_t *count);
```

**Note:** Both boundaries are inclusive.

---

### kvidxExistsInRange

Check if any keys exist in a range.

```c
kvidxError kvidxExistsInRange(kvidxInstance *i, uint64_t startKey,
                              uint64_t endKey, bool *exists);
```

---

## Iterator API

### kvidxIteratorCreate

Create an iterator for a key range.

```c
typedef enum {
    KVIDX_ITER_FORWARD,  // Ascending order
    KVIDX_ITER_BACKWARD  // Descending order
} kvidxIterDirection;

kvidxIterator *kvidxIteratorCreate(kvidxInstance *i, uint64_t startKey,
                                   uint64_t endKey, kvidxIterDirection direction);
```

**Returns:** Iterator handle, or NULL on error

**Note:** Iterator starts before the first element. Call `kvidxIteratorNext()` to advance.

---

### kvidxIteratorNext

Advance to the next entry.

```c
bool kvidxIteratorNext(kvidxIterator *it);
```

**Returns:** `true` if advanced, `false` if no more entries

---

### kvidxIteratorGet

Get the current entry.

```c
bool kvidxIteratorGet(const kvidxIterator *it, uint64_t *key, uint64_t *term,
                      uint64_t *cmd, const uint8_t **data, size_t *len);
```

**Note:** Data pointer valid until next iterator operation or destruction.

---

### kvidxIteratorKey

Get current key only.

```c
uint64_t kvidxIteratorKey(const kvidxIterator *it);
```

---

### kvidxIteratorValid

Check if iterator is at a valid entry.

```c
bool kvidxIteratorValid(const kvidxIterator *it);
```

---

### kvidxIteratorSeek

Seek to a specific key.

```c
bool kvidxIteratorSeek(kvidxIterator *it, uint64_t key);
```

If exact key doesn't exist, positions at next (forward) or previous (backward) key.

---

### kvidxIteratorDestroy

Free iterator resources.

```c
void kvidxIteratorDestroy(kvidxIterator *it);
```

---

## Statistics API

### kvidxGetStats

Get comprehensive database statistics.

```c
kvidxError kvidxGetStats(kvidxInstance *i, kvidxStats *stats);
```

---

### kvidxGetKeyCount

Get total number of keys.

```c
kvidxError kvidxGetKeyCount(kvidxInstance *i, uint64_t *count);
```

---

### kvidxGetMinKey

Get minimum key value.

```c
kvidxError kvidxGetMinKey(kvidxInstance *i, uint64_t *key);
```

**Returns:** `KVIDX_ERROR_NOT_FOUND` if database is empty

---

### kvidxGetDataSize

Get total data size in bytes.

```c
kvidxError kvidxGetDataSize(kvidxInstance *i, uint64_t *bytes);
```

---

## Configuration API

### kvidxConfigDefault

Get default configuration.

```c
kvidxConfig kvidxConfigDefault(void);
```

**Default Values:**
| Field | Default |
|-------|---------|
| `cacheSizeBytes` | 33554432 (32 MB) |
| `journalMode` | `KVIDX_JOURNAL_WAL` |
| `syncMode` | `KVIDX_SYNC_NORMAL` |
| `busyTimeoutMs` | 5000 |
| `enableRecursiveTriggers` | `true` |
| `enableForeignKeys` | `false` |
| `readOnly` | `false` |
| `pageSize` | 4096 |

---

### kvidxUpdateConfig

Update configuration on open database.

```c
kvidxError kvidxUpdateConfig(kvidxInstance *i, const kvidxConfig *config);
```

**Note:** Some settings (like `pageSize`) cannot be changed after database creation.

---

### kvidxGetConfig

Get current configuration.

```c
kvidxError kvidxGetConfig(const kvidxInstance *i, kvidxConfig *config);
```

---

### kvidxJournalMode

```c
typedef enum {
    KVIDX_JOURNAL_DELETE,   // Delete journal after commit
    KVIDX_JOURNAL_TRUNCATE, // Truncate journal to zero
    KVIDX_JOURNAL_PERSIST,  // Keep journal, zero header
    KVIDX_JOURNAL_MEMORY,   // Journal in memory (fast, unsafe)
    KVIDX_JOURNAL_WAL,      // Write-Ahead Log (recommended)
    KVIDX_JOURNAL_OFF       // No journal (dangerous)
} kvidxJournalMode;
```

---

### kvidxSyncMode

```c
typedef enum {
    KVIDX_SYNC_OFF,    // No sync (fast, data loss risk)
    KVIDX_SYNC_NORMAL, // Sync at critical moments
    KVIDX_SYNC_FULL,   // Full sync guarantee
    KVIDX_SYNC_EXTRA   // Extra safety checks
} kvidxSyncMode;
```

---

## Export/Import API

### kvidxExport

Export database to file.

```c
kvidxError kvidxExport(kvidxInstance *i, const char *filename,
                       const kvidxExportOptions *options,
                       kvidxProgressCallback callback, void *userData);
```

---

### kvidxImport

Import database from file.

```c
kvidxError kvidxImport(kvidxInstance *i, const char *filename,
                       const kvidxImportOptions *options,
                       kvidxProgressCallback callback, void *userData);
```

---

### kvidxExportOptions

```c
typedef struct {
    kvidxExportFormat format;  // BINARY, JSON, or CSV
    uint64_t startKey;         // Range start (0 = beginning)
    uint64_t endKey;           // Range end (UINT64_MAX = end)
    bool includeMetadata;      // Include term/cmd (JSON/CSV)
    bool prettyPrint;          // Pretty-print JSON
} kvidxExportOptions;
```

---

### kvidxImportOptions

```c
typedef struct {
    kvidxExportFormat format;  // Format (BINARY = auto-detect)
    bool validateData;         // Validate during import
    bool skipDuplicates;       // Skip duplicates vs. fail
    bool clearBeforeImport;    // Clear database first
} kvidxImportOptions;
```

---

### kvidxProgressCallback

```c
typedef bool (*kvidxProgressCallback)(uint64_t current, uint64_t total,
                                      void *userData);
```

**Returns:** `true` to continue, `false` to abort operation

---

## Storage Primitives

### kvidxInsertEx

Insert with condition.

```c
kvidxError kvidxInsertEx(kvidxInstance *i, uint64_t key, uint64_t term,
                         uint64_t cmd, const void *data, size_t dataLen,
                         kvidxSetCondition condition);
```

**Conditions:**

```c
typedef enum {
    KVIDX_SET_ALWAYS = 0,        // Always write
    KVIDX_SET_IF_NOT_EXISTS = 1, // Only if key absent (NX)
    KVIDX_SET_IF_EXISTS = 2      // Only if key exists (XX)
} kvidxSetCondition;
```

**Returns:** `KVIDX_ERROR_CONDITION_FAILED` if condition not met

---

### kvidxInsertNX

Insert only if key doesn't exist.

```c
kvidxError kvidxInsertNX(kvidxInstance *i, uint64_t key, uint64_t term,
                         uint64_t cmd, const void *data, size_t dataLen);
```

Equivalent to `kvidxInsertEx(..., KVIDX_SET_IF_NOT_EXISTS)`

---

### kvidxInsertXX

Update only if key exists.

```c
kvidxError kvidxInsertXX(kvidxInstance *i, uint64_t key, uint64_t term,
                         uint64_t cmd, const void *data, size_t dataLen);
```

---

### kvidxGetAndSet

Atomically get and set.

```c
kvidxError kvidxGetAndSet(kvidxInstance *i, uint64_t key, uint64_t term,
                          uint64_t cmd, const void *data, size_t dataLen,
                          uint64_t *oldTerm, uint64_t *oldCmd, void **oldData,
                          size_t *oldDataLen);
```

**Note:** Caller must `free(oldData)`.

---

### kvidxGetAndRemove

Atomically get and remove.

```c
kvidxError kvidxGetAndRemove(kvidxInstance *i, uint64_t key, uint64_t *term,
                             uint64_t *cmd, void **data, size_t *dataLen);
```

**Note:** Caller must `free(data)`.

---

### kvidxCompareAndSwap

Compare value and swap if match.

```c
kvidxError kvidxCompareAndSwap(kvidxInstance *i, uint64_t key,
                               const void *expectedData, size_t expectedLen,
                               uint64_t newTerm, uint64_t newCmd,
                               const void *newData, size_t newDataLen,
                               bool *swapped);
```

---

### kvidxCompareTermAndSwap

Compare term and swap if match.

```c
kvidxError kvidxCompareTermAndSwap(kvidxInstance *i, uint64_t key,
                                   uint64_t expectedTerm, uint64_t newTerm,
                                   uint64_t newCmd, const void *newData,
                                   size_t newDataLen, bool *swapped);
```

---

### kvidxAppend

Append data to value.

```c
kvidxError kvidxAppend(kvidxInstance *i, uint64_t key, uint64_t term,
                       uint64_t cmd, const void *data, size_t dataLen,
                       size_t *newLen);
```

Creates key if it doesn't exist.

---

### kvidxPrepend

Prepend data to value.

```c
kvidxError kvidxPrepend(kvidxInstance *i, uint64_t key, uint64_t term,
                        uint64_t cmd, const void *data, size_t dataLen,
                        size_t *newLen);
```

---

### kvidxGetValueRange

Read portion of value.

```c
kvidxError kvidxGetValueRange(kvidxInstance *i, uint64_t key, size_t offset,
                              size_t length, void **data, size_t *actualLen);
```

**Note:** Caller must `free(data)`.

---

### kvidxSetValueRange

Write to portion of value.

```c
kvidxError kvidxSetValueRange(kvidxInstance *i, uint64_t key, size_t offset,
                              const void *data, size_t dataLen, size_t *newLen);
```

---

## TTL/Expiration

### kvidxSetExpire

Set relative expiration.

```c
kvidxError kvidxSetExpire(kvidxInstance *i, uint64_t key, uint64_t ttlMs);
```

**Parameters:**

- `ttlMs`: Time-to-live in milliseconds from now

---

### kvidxSetExpireAt

Set absolute expiration.

```c
kvidxError kvidxSetExpireAt(kvidxInstance *i, uint64_t key, uint64_t timestampMs);
```

**Parameters:**

- `timestampMs`: Unix timestamp in milliseconds

---

### kvidxGetTTL

Get remaining TTL.

```c
int64_t kvidxGetTTL(kvidxInstance *i, uint64_t key);
```

**Returns:**

- `> 0`: Milliseconds remaining
- `KVIDX_TTL_NONE` (-1): No expiration set
- `KVIDX_TTL_NOT_FOUND` (-2): Key doesn't exist

---

### kvidxPersist

Remove expiration from key.

```c
kvidxError kvidxPersist(kvidxInstance *i, uint64_t key);
```

---

### kvidxExpireScan

Scan and remove expired keys.

```c
kvidxError kvidxExpireScan(kvidxInstance *i, uint64_t maxKeys,
                           uint64_t *expiredCount);
```

**Parameters:**

- `maxKeys`: Maximum keys to scan (0 = all)
- `expiredCount`: Receives count of removed keys

---

## Error Handling

### kvidxError

```c
typedef enum {
    KVIDX_OK = 0,
    KVIDX_ERROR_INVALID_ARGUMENT = 1,
    KVIDX_ERROR_DUPLICATE_KEY = 2,
    KVIDX_ERROR_NOT_FOUND = 3,
    KVIDX_ERROR_DISK_FULL = 4,
    KVIDX_ERROR_IO = 5,
    KVIDX_ERROR_CORRUPT = 6,
    KVIDX_ERROR_TRANSACTION_ACTIVE = 7,
    KVIDX_ERROR_NO_TRANSACTION = 8,
    KVIDX_ERROR_READONLY = 9,
    KVIDX_ERROR_LOCKED = 10,
    KVIDX_ERROR_NOMEM = 11,
    KVIDX_ERROR_TOO_BIG = 12,
    KVIDX_ERROR_CONSTRAINT = 13,
    KVIDX_ERROR_SCHEMA = 14,
    KVIDX_ERROR_RANGE = 15,
    KVIDX_ERROR_NOT_SUPPORTED = 16,
    KVIDX_ERROR_CANCELLED = 17,
    KVIDX_ERROR_INTERNAL = 99,
    KVIDX_ERROR_CONDITION_FAILED = 100,
    KVIDX_ERROR_EXPIRED = 101
} kvidxError;
```

---

### kvidxGetLastError

```c
kvidxError kvidxGetLastError(const kvidxInstance *i);
```

---

### kvidxGetLastErrorMessage

```c
const char *kvidxGetLastErrorMessage(kvidxInstance *i);
```

---

### kvidxClearError

```c
void kvidxClearError(kvidxInstance *i);
```

---

### kvidxErrorString

Convert error code to string.

```c
const char *kvidxErrorString(kvidxError err);
```

---

### kvidxIsOk / kvidxIsError

```c
static inline bool kvidxIsOk(kvidxError err);
static inline bool kvidxIsError(kvidxError err);
```

---

## Registry API

Runtime discovery of available backends.

### kvidxAdapterInfo

```c
typedef struct kvidxAdapterInfo {
    const char *name;                    // e.g., "sqlite3"
    const struct kvidxInterface *iface;  // Interface pointer
    const char *pathSuffix;              // e.g., ".db"
    bool isDirectory;                    // true for LMDB/RocksDB
} kvidxAdapterInfo;
```

---

### kvidxGetAdapterCount

```c
size_t kvidxGetAdapterCount(void);
```

---

### kvidxGetAdapterByIndex

```c
const kvidxAdapterInfo *kvidxGetAdapterByIndex(size_t index);
```

---

### kvidxGetAdapterByName

```c
const kvidxAdapterInfo *kvidxGetAdapterByName(const char *name);
```

Case-insensitive lookup.

---

### kvidxHasAdapter

```c
bool kvidxHasAdapter(const char *name);
```

---

## Available Interfaces

Conditionally available based on compile flags:

```c
#ifdef KVIDXKIT_HAS_SQLITE3
extern const kvidxInterface kvidxInterfaceSqlite3;
#endif

#ifdef KVIDXKIT_HAS_LMDB
extern const kvidxInterface kvidxInterfaceLmdb;
#endif

#ifdef KVIDXKIT_HAS_ROCKSDB
extern const kvidxInterface kvidxInterfaceRocksdb;
#endif
```
