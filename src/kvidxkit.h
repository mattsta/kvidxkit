#pragma once

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "kvidxkitConfig.h"
#include "kvidxkitErrors.h"
#include "kvidxkitExport.h"
#include "kvidxkitIterator.h"

__BEGIN_DECLS

/* Pre-declare instance because Interface<->Instance use each other. */
struct kvidxInstance;

/* Pre-declare stats structure */
typedef struct kvidxStats kvidxStats;

typedef struct kvidxInterface {
    /* CACHE LINE 1 */
    bool (*begin)(struct kvidxInstance *i);
    bool (*commit)(struct kvidxInstance *i);

    bool (*get)(struct kvidxInstance *i, uint64_t key, uint64_t *term,
                uint64_t *cmd, const uint8_t **data, size_t *len);

    bool (*getPrev)(struct kvidxInstance *i, uint64_t previousKey,
                    uint64_t *nextKey, uint64_t *nextTerm, uint64_t *cmd,
                    const uint8_t **data, size_t *len);
    bool (*getNext)(struct kvidxInstance *i, uint64_t previousKey,
                    uint64_t *nextKey, uint64_t *nextTerm, uint64_t *cmd,
                    const uint8_t **data, size_t *len);

    bool (*exists)(struct kvidxInstance *i, uint64_t key);
    bool (*existsDual)(struct kvidxInstance *i, uint64_t key, uint64_t term);

    bool (*maxKey)(struct kvidxInstance *i, uint64_t *key);

    /* CACHE LINE 2 */
    bool (*insert)(struct kvidxInstance *i, uint64_t key, uint64_t term,
                   uint64_t cmd, const void *data, size_t dataLen);

    bool (*remove)(struct kvidxInstance *i, uint64_t key);
    bool (*removeAfterNInclusive)(struct kvidxInstance *i, uint64_t key);
    bool (*removeBeforeNInclusive)(struct kvidxInstance *i, uint64_t key);

    bool (*fsync)(struct kvidxInstance *i);

    bool (*copyStorageForReplication)(struct kvidxInstance *i,
                                      void (*networkWrite)(void *, void *,
                                                           size_t),
                                      void *networkState);
    bool (*copyStorageForReplicationReceive)(struct kvidxInstance *i);

    bool (*copyStorageForBackup)(struct kvidxInstance *i, void *storageTarget);
    bool (*applyToStateMachine)(struct kvidxInstance *i, uint64_t key);

    /* CACHE LINE 3 */
    bool (*open)(struct kvidxInstance *i, const char *filename,
                 const char **err);
    bool (*close)(struct kvidxInstance *i);

    /* Statistics (v0.5.0) */
    kvidxError (*getStats)(struct kvidxInstance *i, kvidxStats *stats);
    kvidxError (*getKeyCount)(struct kvidxInstance *i, uint64_t *count);
    kvidxError (*getMinKey)(struct kvidxInstance *i, uint64_t *key);
    kvidxError (*getDataSize)(struct kvidxInstance *i, uint64_t *bytes);

    /* Range Operations (v0.5.0) */
    kvidxError (*removeRange)(struct kvidxInstance *i, uint64_t startKey,
                              uint64_t endKey, bool startInclusive,
                              bool endInclusive, uint64_t *deletedCount);
    kvidxError (*countRange)(struct kvidxInstance *i, uint64_t startKey,
                             uint64_t endKey, uint64_t *count);
    kvidxError (*existsInRange)(struct kvidxInstance *i, uint64_t startKey,
                                uint64_t endKey, bool *exists);

    /* Export/Import (v0.6.0) */
    kvidxError (*exportData)(struct kvidxInstance *i, const char *filename,
                             const kvidxExportOptions *options,
                             kvidxProgressCallback callback, void *userData);
    kvidxError (*importData)(struct kvidxInstance *i, const char *filename,
                             const kvidxImportOptions *options,
                             kvidxProgressCallback callback, void *userData);

    /* Storage Primitives (v0.8.0) */
    /* Conditional writes */
    kvidxError (*insertEx)(struct kvidxInstance *i, uint64_t key, uint64_t term,
                           uint64_t cmd, const void *data, size_t dataLen,
                           kvidxSetCondition condition);

    /* Transaction abort */
    bool (*abort)(struct kvidxInstance *i);

    /* Atomic operations */
    kvidxError (*getAndSet)(struct kvidxInstance *i, uint64_t key,
                            uint64_t term, uint64_t cmd, const void *data,
                            size_t dataLen, uint64_t *oldTerm, uint64_t *oldCmd,
                            void **oldData, size_t *oldDataLen);
    kvidxError (*getAndRemove)(struct kvidxInstance *i, uint64_t key,
                               uint64_t *term, uint64_t *cmd, void **data,
                               size_t *dataLen);

    /* Compare-and-swap */
    kvidxError (*compareAndSwap)(struct kvidxInstance *i, uint64_t key,
                                 const void *expectedData, size_t expectedLen,
                                 uint64_t newTerm, uint64_t newCmd,
                                 const void *newData, size_t newDataLen,
                                 bool *swapped);

    /* Append/Prepend */
    kvidxError (*append)(struct kvidxInstance *i, uint64_t key, uint64_t term,
                         uint64_t cmd, const void *data, size_t dataLen,
                         size_t *newLen);
    kvidxError (*prepend)(struct kvidxInstance *i, uint64_t key, uint64_t term,
                          uint64_t cmd, const void *data, size_t dataLen,
                          size_t *newLen);

    /* Partial value access */
    kvidxError (*getValueRange)(struct kvidxInstance *i, uint64_t key,
                                size_t offset, size_t length, void **data,
                                size_t *actualLen);
    kvidxError (*setValueRange)(struct kvidxInstance *i, uint64_t key,
                                size_t offset, const void *data, size_t dataLen,
                                size_t *newLen);

    /* TTL/Expiration */
    kvidxError (*setExpire)(struct kvidxInstance *i, uint64_t key,
                            uint64_t ttlMs);
    kvidxError (*setExpireAt)(struct kvidxInstance *i, uint64_t key,
                              uint64_t timestampMs);
    int64_t (*getTTL)(struct kvidxInstance *i, uint64_t key);
    kvidxError (*persist)(struct kvidxInstance *i, uint64_t key);
    kvidxError (*expireScan)(struct kvidxInstance *i, uint64_t maxKeys,
                             uint64_t *expiredCount);
} kvidxInterface;

typedef struct kvidxInterfaceStateMachine {
    bool (*applyToStateMachine)(struct kvidxInstance *i, uint64_t key);
    bool (*resurrectStateMachineFromStorage)(struct kvidxInstance *i);
} kvidxInterfaceStateMachine;

typedef struct kvidxInstance {
    void *kvidxdata;
    void *clientdata;
    kvidxInterface interface;
    kvidxInterfaceStateMachine state;
    bool (*customInit)(struct kvidxInstance *i);

    /* Error tracking (added in v0.4.0) */
    kvidxError lastError;
    char lastErrorMessage[256];
    bool transactionActive;

    /* Configuration (added in v0.5.0) */
    kvidxConfig config;
    bool configInitialized;
} kvidxInstance;

/* Export identifier for interfaces distributed inside kvidxkit itself.
 * Each adapter is conditionally available based on KVIDXKIT_HAS_* defines
 * set at compile time via CMake options (KVIDXKIT_ENABLE_*).
 */
#ifdef KVIDXKIT_HAS_SQLITE3
extern const kvidxInterface kvidxInterfaceSqlite3;
#endif

#ifdef KVIDXKIT_HAS_LMDB
extern const kvidxInterface kvidxInterfaceLmdb;
#endif

#ifdef KVIDXKIT_HAS_ROCKSDB
extern const kvidxInterface kvidxInterfaceRocksdb;
#endif

/* Open / Close / Management */
bool kvidxOpen(kvidxInstance *i, const char *filename, const char **err);
bool kvidxClose(kvidxInstance *i);
bool kvidxFsync(kvidxInstance *i);

/* Transactional Management */
bool kvidxBegin(struct kvidxInstance *i);
bool kvidxCommit(struct kvidxInstance *i);
bool kvidxAbort(struct kvidxInstance *i);

/* Reading */
bool kvidxGet(kvidxInstance *i, uint64_t key, uint64_t *term, uint64_t *cmd,
              const uint8_t **data, size_t *len);
bool kvidxGetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                  uint64_t *prevTerm, uint64_t *cmd, const uint8_t **data,
                  size_t *len);
bool kvidxGetNext(kvidxInstance *i, uint64_t previousKey, uint64_t *nextKey,
                  uint64_t *nextTerm, uint64_t *cmd, const uint8_t **data,
                  size_t *len);
bool kvidxExists(kvidxInstance *i, uint64_t key);
bool kvidxExistsDual(kvidxInstance *i, uint64_t key, uint64_t term);
bool kvidxMaxKey(kvidxInstance *i, uint64_t *key);
bool kvidxInsert(kvidxInstance *i, uint64_t key, uint64_t term, uint64_t cmd,
                 const void *data, size_t dataLen);

/* Deleting */
bool kvidxRemove(kvidxInstance *i, uint64_t key);
bool kvidxRemoveAfterNInclusive(kvidxInstance *i, uint64_t key);
bool kvidxRemoveBeforeNInclusive(kvidxInstance *i, uint64_t key);

/* ====================================================================
 * Batch Operations (Added in v0.4.0)
 * ==================================================================== */

/**
 * Entry structure for batch operations
 */
typedef struct {
    uint64_t key;
    uint64_t term;
    uint64_t cmd;
    const void *data;
    size_t dataLen;
} kvidxEntry;

/**
 * Insert multiple entries in a single transaction
 *
 * This is significantly faster than individual inserts as it:
 * - Wraps all inserts in a single BEGIN/COMMIT
 * - Only one fsync at the end
 * - Reduced overhead per operation
 *
 * @param i Instance handle
 * @param entries Array of entries to insert
 * @param count Number of entries in array
 * @param insertedCount Optional: receives number of successfully inserted
 * entries
 * @return true if all entries inserted, false if any failed
 *
 * @note On failure, insertedCount indicates how many were inserted before error
 * @note If a duplicate key is encountered, insertion stops at that point
 */
bool kvidxInsertBatch(kvidxInstance *i, const kvidxEntry *entries, size_t count,
                      size_t *insertedCount);

/**
 * Callback for filtering/validating batch inserts
 *
 * @param index Index of entry being processed
 * @param entry Pointer to entry being inserted
 * @param userData User-provided context
 * @return true to insert this entry, false to skip
 */
typedef bool (*kvidxBatchCallback)(size_t index, const kvidxEntry *entry,
                                   void *userData);

/**
 * Insert multiple entries with callback for filtering
 *
 * @param i Instance handle
 * @param entries Array of entries to insert
 * @param count Number of entries in array
 * @param callback Optional callback to filter/validate each entry
 * @param userData User data passed to callback
 * @param insertedCount Optional: receives number of successfully inserted
 * entries
 * @return true if all non-filtered entries inserted, false if any failed
 */
bool kvidxInsertBatchEx(kvidxInstance *i, const kvidxEntry *entries,
                        size_t count, kvidxBatchCallback callback,
                        void *userData, size_t *insertedCount);

/* ====================================================================
 * Range Operations (Added in v0.5.0)
 * ==================================================================== */

/**
 * Remove range of keys with inclusive/exclusive boundaries
 *
 * @param i Instance handle
 * @param startKey Start of range
 * @param endKey End of range
 * @param startInclusive Include startKey in deletion
 * @param endInclusive Include endKey in deletion
 * @param deletedCount Optional: receives number of deleted entries
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxRemoveRange(kvidxInstance *i, uint64_t startKey,
                            uint64_t endKey, bool startInclusive,
                            bool endInclusive, uint64_t *deletedCount);

/**
 * Count keys in specified range
 *
 * @param i Instance handle
 * @param startKey Start of range (inclusive)
 * @param endKey End of range (inclusive)
 * @param count Receives count of keys in range
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxCountRange(kvidxInstance *i, uint64_t startKey, uint64_t endKey,
                           uint64_t *count);

/**
 * Check if any keys exist in specified range
 *
 * @param i Instance handle
 * @param startKey Start of range (inclusive)
 * @param endKey End of range (inclusive)
 * @param exists Receives true if any keys exist in range
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxExistsInRange(kvidxInstance *i, uint64_t startKey,
                              uint64_t endKey, bool *exists);

/* ====================================================================
 * Error Handling (Added in v0.4.0)
 * ==================================================================== */

/**
 * Get the last error code for this instance
 *
 * @param i Instance handle
 * @return Last error code, or KVIDX_OK if no error
 */
kvidxError kvidxGetLastError(const kvidxInstance *i);

/**
 * Get the last error message for this instance
 *
 * @param i Instance handle
 * @return Human-readable error message (static buffer, valid until next
 * operation)
 */
const char *kvidxGetLastErrorMessage(kvidxInstance *i);

/**
 * Clear the last error for this instance
 *
 * @param i Instance handle
 */
void kvidxClearError(kvidxInstance *i);

/**
 * Set error for this instance (internal helper)
 *
 * @param i Instance handle
 * @param err Error code
 * @param fmt Format string for error message (printf-style)
 */
void kvidxSetError(kvidxInstance *i, kvidxError err, const char *fmt, ...);

/* ====================================================================
 * Statistics API (Added in v0.5.0)
 * ==================================================================== */

/**
 * Database statistics structure
 */
struct kvidxStats {
    uint64_t totalKeys;        /**< COUNT(*) from log */
    uint64_t minKey;           /**< MIN(id) */
    uint64_t maxKey;           /**< MAX(id) */
    uint64_t totalDataBytes;   /**< SUM(LENGTH(data)) */
    uint64_t databaseFileSize; /**< File size on disk */
    uint64_t walFileSize;      /**< WAL file size (if applicable) */
    uint64_t pageCount;        /**< Total pages */
    uint64_t pageSize;         /**< Page size in bytes */
    uint64_t freePages;        /**< Free pages */
};

/**
 * Get comprehensive database statistics
 *
 * @param i Instance handle
 * @param stats Receives statistics structure
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxGetStats(kvidxInstance *i, kvidxStats *stats);

/**
 * Get total number of keys in database
 *
 * @param i Instance handle
 * @param count Receives key count
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxGetKeyCount(kvidxInstance *i, uint64_t *count);

/**
 * Get minimum key in database
 *
 * @param i Instance handle
 * @param key Receives minimum key
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if empty
 */
kvidxError kvidxGetMinKey(kvidxInstance *i, uint64_t *key);

/**
 * Get total data size in bytes
 *
 * @param i Instance handle
 * @param bytes Receives total data size
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxGetDataSize(kvidxInstance *i, uint64_t *bytes);

/* ====================================================================
 * Configuration API (Added in v0.5.0)
 * ==================================================================== */

/**
 * Get default configuration values
 *
 * @return Configuration structure with default values
 */
kvidxConfig kvidxConfigDefault(void);

/**
 * Open database with custom configuration
 *
 * @param i Instance handle (must be zeroed before call)
 * @param filename Path to database file
 * @param config Configuration to use (NULL for defaults)
 * @param err Optional error message pointer
 * @return true on success, false on failure
 */
bool kvidxOpenWithConfig(kvidxInstance *i, const char *filename,
                         const kvidxConfig *config, const char **err);

/**
 * Update configuration on open database (where possible)
 * Note: Some settings like pageSize cannot be changed after database creation
 *
 * @param i Instance handle
 * @param config New configuration values
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxUpdateConfig(kvidxInstance *i, const kvidxConfig *config);

/**
 * Get current configuration
 *
 * @param i Instance handle
 * @param config Receives current configuration
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxGetConfig(const kvidxInstance *i, kvidxConfig *config);

/* ====================================================================
 * Export/Import API (Added in v0.6.0)
 * ==================================================================== */

/**
 * Export database to file
 *
 * @param i Instance handle
 * @param filename Output filename
 * @param options Export options (NULL for defaults)
 * @param callback Optional progress callback
 * @param userData User data for progress callback
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxExport(kvidxInstance *i, const char *filename,
                       const kvidxExportOptions *options,
                       kvidxProgressCallback callback, void *userData);

/**
 * Import database from file
 *
 * @param i Instance handle
 * @param filename Input filename
 * @param options Import options (NULL for defaults)
 * @param callback Optional progress callback
 * @param userData User data for progress callback
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxImport(kvidxInstance *i, const char *filename,
                       const kvidxImportOptions *options,
                       kvidxProgressCallback callback, void *userData);

/* ====================================================================
 * Storage Primitives API (Added in v0.8.0)
 * ==================================================================== */

/* --- Conditional Writes --- */

/**
 * Insert with condition check
 *
 * @param i Instance handle
 * @param key Key to insert
 * @param term Term value
 * @param cmd Command value
 * @param data Data to store
 * @param dataLen Length of data
 * @param condition When to perform the write
 * @return KVIDX_OK on success, KVIDX_ERROR_CONDITION_FAILED if condition
 *         not met, other error code on failure
 */
kvidxError kvidxInsertEx(kvidxInstance *i, uint64_t key, uint64_t term,
                         uint64_t cmd, const void *data, size_t dataLen,
                         kvidxSetCondition condition);

/**
 * Insert only if key does NOT exist (NX)
 * Convenience wrapper for kvidxInsertEx with KVIDX_SET_IF_NOT_EXISTS
 */
kvidxError kvidxInsertNX(kvidxInstance *i, uint64_t key, uint64_t term,
                         uint64_t cmd, const void *data, size_t dataLen);

/**
 * Insert/update only if key DOES exist (XX)
 * Convenience wrapper for kvidxInsertEx with KVIDX_SET_IF_EXISTS
 */
kvidxError kvidxInsertXX(kvidxInstance *i, uint64_t key, uint64_t term,
                         uint64_t cmd, const void *data, size_t dataLen);

/* --- Atomic Read-Modify-Write --- */

/**
 * Atomically get current value and set new value
 *
 * @param i Instance handle
 * @param key Key to operate on
 * @param term New term value
 * @param cmd New cmd value
 * @param data New data to store
 * @param dataLen Length of new data
 * @param oldTerm Optional: receives previous term
 * @param oldCmd Optional: receives previous cmd
 * @param oldData Optional: receives previous data (caller must free)
 * @param oldDataLen Optional: receives previous data length
 * @return KVIDX_OK on success
 */
kvidxError kvidxGetAndSet(kvidxInstance *i, uint64_t key, uint64_t term,
                          uint64_t cmd, const void *data, size_t dataLen,
                          uint64_t *oldTerm, uint64_t *oldCmd, void **oldData,
                          size_t *oldDataLen);

/**
 * Atomically get and remove a key
 *
 * @param i Instance handle
 * @param key Key to remove
 * @param term Optional: receives term of removed entry
 * @param cmd Optional: receives cmd of removed entry
 * @param data Optional: receives data (caller must free)
 * @param dataLen Optional: receives data length
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key doesn't exist
 */
kvidxError kvidxGetAndRemove(kvidxInstance *i, uint64_t key, uint64_t *term,
                             uint64_t *cmd, void **data, size_t *dataLen);

/* --- Compare-And-Swap --- */

/**
 * Compare-and-swap: update only if current value matches expected
 *
 * Atomically compares the current data at key with expectedData.
 * If they match (same length and content), replaces with newData.
 *
 * @param i Instance handle
 * @param key Key to operate on
 * @param expectedData Expected current data (NULL to match empty/any)
 * @param expectedLen Expected data length
 * @param newTerm New term value
 * @param newCmd New cmd value
 * @param newData New data to store
 * @param newDataLen Length of new data
 * @param swapped Receives true if swap occurred
 * @return KVIDX_OK on success (check swapped for result),
 *         KVIDX_ERROR_NOT_FOUND if key doesn't exist
 */
kvidxError kvidxCompareAndSwap(kvidxInstance *i, uint64_t key,
                               const void *expectedData, size_t expectedLen,
                               uint64_t newTerm, uint64_t newCmd,
                               const void *newData, size_t newDataLen,
                               bool *swapped);

/**
 * Compare-and-swap based on term (version-based CAS)
 *
 * Atomically compares the current term at key with expectedTerm.
 * If they match, replaces with new values.
 */
kvidxError kvidxCompareTermAndSwap(kvidxInstance *i, uint64_t key,
                                   uint64_t expectedTerm, uint64_t newTerm,
                                   uint64_t newCmd, const void *newData,
                                   size_t newDataLen, bool *swapped);

/* --- Append/Prepend --- */

/**
 * Append data to existing value
 *
 * If key exists, appends data to existing value.
 * If key doesn't exist, creates new entry with data as value.
 *
 * @param i Instance handle
 * @param key Key to append to
 * @param term Term value (used only if creating new entry)
 * @param cmd Cmd value (used only if creating new entry)
 * @param data Data to append
 * @param dataLen Length of data to append
 * @param newLen Optional: receives new total data length
 * @return KVIDX_OK on success
 */
kvidxError kvidxAppend(kvidxInstance *i, uint64_t key, uint64_t term,
                       uint64_t cmd, const void *data, size_t dataLen,
                       size_t *newLen);

/**
 * Prepend data to existing value
 */
kvidxError kvidxPrepend(kvidxInstance *i, uint64_t key, uint64_t term,
                        uint64_t cmd, const void *data, size_t dataLen,
                        size_t *newLen);

/* --- Partial Value Access --- */

/**
 * Read a portion of a value
 *
 * @param i Instance handle
 * @param key Key to read from
 * @param offset Byte offset to start reading from
 * @param length Maximum bytes to read
 * @param data Receives pointer to data (caller must free)
 * @param actualLen Receives actual bytes read
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key doesn't exist
 */
kvidxError kvidxGetValueRange(kvidxInstance *i, uint64_t key, size_t offset,
                              size_t length, void **data, size_t *actualLen);

/**
 * Write to a portion of a value
 *
 * Overwrites bytes starting at offset. If offset + dataLen exceeds
 * current value length, the value is extended.
 *
 * @param i Instance handle
 * @param key Key to write to
 * @param offset Byte offset to start writing at
 * @param data Data to write
 * @param dataLen Length of data to write
 * @param newLen Optional: receives new total data length
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key doesn't exist
 */
kvidxError kvidxSetValueRange(kvidxInstance *i, uint64_t key, size_t offset,
                              const void *data, size_t dataLen, size_t *newLen);

/* --- TTL/Expiration --- */

/**
 * Set expiration time for a key (relative)
 *
 * @param i Instance handle
 * @param key Key to set expiration on
 * @param ttlMs Time-to-live in milliseconds from now
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key doesn't exist
 */
kvidxError kvidxSetExpire(kvidxInstance *i, uint64_t key, uint64_t ttlMs);

/**
 * Set expiration time for a key (absolute)
 *
 * @param i Instance handle
 * @param key Key to set expiration on
 * @param timestampMs Unix timestamp in milliseconds when key should expire
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key doesn't exist
 */
kvidxError kvidxSetExpireAt(kvidxInstance *i, uint64_t key,
                            uint64_t timestampMs);

/**
 * Get remaining TTL for a key
 *
 * @param i Instance handle
 * @param key Key to check
 * @return Remaining TTL in milliseconds, KVIDX_TTL_NONE if no expiration,
 *         KVIDX_TTL_NOT_FOUND if key doesn't exist
 */
int64_t kvidxGetTTL(kvidxInstance *i, uint64_t key);

/**
 * Remove expiration from a key (make it persistent)
 *
 * @param i Instance handle
 * @param key Key to persist
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key doesn't exist
 */
kvidxError kvidxPersist(kvidxInstance *i, uint64_t key);

/**
 * Scan and remove expired keys
 *
 * @param i Instance handle
 * @param maxKeys Maximum number of keys to scan (0 = scan all)
 * @param expiredCount Optional: receives count of expired keys removed
 * @return KVIDX_OK on success
 */
kvidxError kvidxExpireScan(kvidxInstance *i, uint64_t maxKeys,
                           uint64_t *expiredCount);

__END_DECLS
