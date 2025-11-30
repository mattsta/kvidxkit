#include "kvidxkit.h"

/* Conditional adapter includes based on compile-time configuration */
#ifdef KVIDXKIT_HAS_SQLITE3
#include "kvidxkitAdapterSqlite3.h"
#endif

#ifdef KVIDXKIT_HAS_LMDB
#include "kvidxkitAdapterLmdb.h"
#endif

#ifdef KVIDXKIT_HAS_ROCKSDB
#include "kvidxkitAdapterRocksdb.h"
#endif

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

/* ====================================================================
 * Sqlite3 Implementation
 * ==================================================================== */
#ifdef KVIDXKIT_HAS_SQLITE3
const kvidxInterface kvidxInterfaceSqlite3 = {
    .begin = kvidxSqlite3Begin,
    .commit = kvidxSqlite3Commit,
    .get = kvidxSqlite3Get,
    .getPrev = kvidxSqlite3GetPrev,
    .getNext = kvidxSqlite3GetNext,
    .exists = kvidxSqlite3Exists,
    .existsDual = kvidxSqlite3ExistsDual,
    .maxKey = kvidxSqlite3Max,
    .insert = kvidxSqlite3Insert,
    .remove = kvidxSqlite3Remove,
    .removeAfterNInclusive = kvidxSqlite3RemoveAfterNInclusive,
    .removeBeforeNInclusive = kvidxSqlite3RemoveBeforeNInclusive,
    .fsync = kvidxSqlite3Fsync,
    .open = kvidxSqlite3Open,
    .close = kvidxSqlite3Close,
    .getStats = kvidxSqlite3GetStats,
    .getKeyCount = kvidxSqlite3GetKeyCount,
    .getMinKey = kvidxSqlite3GetMinKey,
    .getDataSize = kvidxSqlite3GetDataSize,
    .removeRange = kvidxSqlite3RemoveRange,
    .countRange = kvidxSqlite3CountRange,
    .existsInRange = kvidxSqlite3ExistsInRange,
    .exportData = kvidxSqlite3Export,
    .importData = kvidxSqlite3Import,
    /* Storage Primitives (v0.8.0) */
    .insertEx = kvidxSqlite3InsertEx,
    .abort = kvidxSqlite3Abort,
    .getAndSet = kvidxSqlite3GetAndSet,
    .getAndRemove = kvidxSqlite3GetAndRemove,
    .compareAndSwap = kvidxSqlite3CompareAndSwap,
    .append = kvidxSqlite3Append,
    .prepend = kvidxSqlite3Prepend,
    .getValueRange = kvidxSqlite3GetValueRange,
    .setValueRange = kvidxSqlite3SetValueRange,
    .setExpire = kvidxSqlite3SetExpire,
    .setExpireAt = kvidxSqlite3SetExpireAt,
    .getTTL = kvidxSqlite3GetTTL,
    .persist = kvidxSqlite3Persist,
    .expireScan = kvidxSqlite3ExpireScan};
#endif

/* ====================================================================
 * LMDB Implementation
 * ==================================================================== */
#ifdef KVIDXKIT_HAS_LMDB
const kvidxInterface kvidxInterfaceLmdb = {
    .begin = kvidxLmdbBegin,
    .commit = kvidxLmdbCommit,
    .get = kvidxLmdbGet,
    .getPrev = kvidxLmdbGetPrev,
    .getNext = kvidxLmdbGetNext,
    .exists = kvidxLmdbExists,
    .existsDual = kvidxLmdbExistsDual,
    .maxKey = kvidxLmdbMax,
    .insert = kvidxLmdbInsert,
    .remove = kvidxLmdbRemove,
    .removeAfterNInclusive = kvidxLmdbRemoveAfterNInclusive,
    .removeBeforeNInclusive = kvidxLmdbRemoveBeforeNInclusive,
    .fsync = kvidxLmdbFsync,
    .open = kvidxLmdbOpen,
    .close = kvidxLmdbClose,
    .getStats = kvidxLmdbGetStats,
    .getKeyCount = kvidxLmdbGetKeyCount,
    .getMinKey = kvidxLmdbGetMinKey,
    .getDataSize = kvidxLmdbGetDataSize,
    .removeRange = kvidxLmdbRemoveRange,
    .countRange = kvidxLmdbCountRange,
    .existsInRange = kvidxLmdbExistsInRange,
    .exportData = kvidxLmdbExport,
    .importData = kvidxLmdbImport,
    /* Storage Primitives (v0.8.0) */
    .insertEx = kvidxLmdbInsertEx,
    .abort = kvidxLmdbAbort,
    .getAndSet = kvidxLmdbGetAndSet,
    .getAndRemove = kvidxLmdbGetAndRemove,
    .compareAndSwap = kvidxLmdbCompareAndSwap,
    .append = kvidxLmdbAppend,
    .prepend = kvidxLmdbPrepend,
    .getValueRange = kvidxLmdbGetValueRange,
    .setValueRange = kvidxLmdbSetValueRange,
    .setExpire = kvidxLmdbSetExpire,
    .setExpireAt = kvidxLmdbSetExpireAt,
    .getTTL = kvidxLmdbGetTTL,
    .persist = kvidxLmdbPersist,
    .expireScan = kvidxLmdbExpireScan};
#endif

/* ====================================================================
 * RocksDB Implementation
 * ==================================================================== */
#ifdef KVIDXKIT_HAS_ROCKSDB
const kvidxInterface kvidxInterfaceRocksdb = {
    .begin = kvidxRocksdbBegin,
    .commit = kvidxRocksdbCommit,
    .get = kvidxRocksdbGet,
    .getPrev = kvidxRocksdbGetPrev,
    .getNext = kvidxRocksdbGetNext,
    .exists = kvidxRocksdbExists,
    .existsDual = kvidxRocksdbExistsDual,
    .maxKey = kvidxRocksdbMax,
    .insert = kvidxRocksdbInsert,
    .remove = kvidxRocksdbRemove,
    .removeAfterNInclusive = kvidxRocksdbRemoveAfterNInclusive,
    .removeBeforeNInclusive = kvidxRocksdbRemoveBeforeNInclusive,
    .fsync = kvidxRocksdbFsync,
    .open = kvidxRocksdbOpen,
    .close = kvidxRocksdbClose,
    .getStats = kvidxRocksdbGetStats,
    .getKeyCount = kvidxRocksdbGetKeyCount,
    .getMinKey = kvidxRocksdbGetMinKey,
    .getDataSize = kvidxRocksdbGetDataSize,
    .removeRange = kvidxRocksdbRemoveRange,
    .countRange = kvidxRocksdbCountRange,
    .existsInRange = kvidxRocksdbExistsInRange,
    .exportData = kvidxRocksdbExport,
    .importData = kvidxRocksdbImport,
    /* Storage Primitives (v0.8.0) */
    .insertEx = kvidxRocksdbInsertEx,
    .abort = kvidxRocksdbAbort,
    .getAndSet = kvidxRocksdbGetAndSet,
    .getAndRemove = kvidxRocksdbGetAndRemove,
    .compareAndSwap = kvidxRocksdbCompareAndSwap,
    .append = kvidxRocksdbAppend,
    .prepend = kvidxRocksdbPrepend,
    .getValueRange = kvidxRocksdbGetValueRange,
    .setValueRange = kvidxRocksdbSetValueRange,
    .setExpire = kvidxRocksdbSetExpire,
    .setExpireAt = kvidxRocksdbSetExpireAt,
    .getTTL = kvidxRocksdbGetTTL,
    .persist = kvidxRocksdbPersist,
    .expireScan = kvidxRocksdbExpireScan};
#endif

/* ====================================================================
 * User API
 * ==================================================================== */
/* Lazy debug flag to see where callers hit our API. */
#ifdef KVIDX_BASIC_DEBUG
#include <stdio.h>
#define VERBOSE_TAG()                                                          \
    do {                                                                       \
        printf("%s();\n", __func__);                                           \
    } while (0)
#else
#define VERBOSE_TAG()
#endif

bool kvidxOpen(kvidxInstance *i, const char *filename, const char **err) {
    VERBOSE_TAG();
    return i->interface.open(i, filename, err);
}

bool kvidxClose(kvidxInstance *i) {
    VERBOSE_TAG();
    return i->interface.close(i);
}

bool kvidxBegin(kvidxInstance *i) {
    VERBOSE_TAG();
    return i->interface.begin(i);
}

bool kvidxCommit(kvidxInstance *i) {
    VERBOSE_TAG();
    return i->interface.commit(i);
}

bool kvidxGet(kvidxInstance *i, uint64_t key, uint64_t *term, uint64_t *cmd,
              const uint8_t **data, size_t *len) {
    VERBOSE_TAG();
    return i->interface.get(i, key, term, cmd, data, len);
}

bool kvidxGetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                  uint64_t *prevTerm, uint64_t *cmd, const uint8_t **data,
                  size_t *len) {
    VERBOSE_TAG();
    return i->interface.getPrev(i, nextKey, prevKey, prevTerm, cmd, data, len);
}

bool kvidxGetNext(kvidxInstance *i, uint64_t previousKey, uint64_t *nextKey,
                  uint64_t *nextTerm, uint64_t *cmd, const uint8_t **data,
                  size_t *len) {
    VERBOSE_TAG();
    return i->interface.getNext(i, previousKey, nextKey, nextTerm, cmd, data,
                                len);
}

bool kvidxExists(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    return i->interface.exists(i, key);
}

bool kvidxExistsDual(kvidxInstance *i, uint64_t key, uint64_t term) {
    VERBOSE_TAG();
    return i->interface.existsDual(i, key, term);
}

bool kvidxMaxKey(kvidxInstance *i, uint64_t *key) {
    VERBOSE_TAG();
    return i->interface.maxKey(i, key);
}

bool kvidxInsert(kvidxInstance *i, uint64_t key, uint64_t term, uint64_t cmd,
                 const void *data, size_t dataLen) {
    VERBOSE_TAG();
    return i->interface.insert(i, key, term, cmd, data, dataLen);
}

bool kvidxRemove(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    return i->interface.remove(i, key);
}

bool kvidxRemoveAfterNInclusive(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    return i->interface.removeAfterNInclusive(i, key);
}

bool kvidxRemoveBeforeNInclusive(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    return i->interface.removeBeforeNInclusive(i, key);
}

bool kvidxFsync(kvidxInstance *i) {
    VERBOSE_TAG();
    return i->interface.fsync(i);
}

bool kvidxApplyToStateMachine(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    return i->state.applyToStateMachine(i, key);
}

/* ====================================================================
 * Batch Operations Implementation
 * ==================================================================== */

bool kvidxInsertBatch(kvidxInstance *i, const kvidxEntry *entries, size_t count,
                      size_t *insertedCount) {
    return kvidxInsertBatchEx(i, entries, count, NULL, NULL, insertedCount);
}

bool kvidxInsertBatchEx(kvidxInstance *i, const kvidxEntry *entries,
                        size_t count, kvidxBatchCallback callback,
                        void *userData, size_t *insertedCount) {
    if (!i || !entries) {
        if (insertedCount) {
            *insertedCount = 0;
        }
        return false;
    }

    if (count == 0) {
        if (insertedCount) {
            *insertedCount = 0;
        }
        return true; /* Vacuously true */
    }

    size_t inserted = 0;
    bool success = true;

    /* Begin transaction for batch */
    if (!kvidxBegin(i)) {
        if (insertedCount) {
            *insertedCount = 0;
        }
        return false;
    }

    /* Insert all entries */
    for (size_t idx = 0; idx < count; idx++) {
        const kvidxEntry *entry = &entries[idx];

        /* Check callback filter */
        if (callback && !callback(idx, entry, userData)) {
            /* Skip this entry */
            continue;
        }

        /* Insert entry */
        bool result = kvidxInsert(i, entry->key, entry->term, entry->cmd,
                                  entry->data, entry->dataLen);

        if (!result) {
            /* Insert failed - rollback would be ideal but we don't have it */
            /* In SQLite, the transaction remains active even after error */
            success = false;
            break;
        }

        inserted++;
    }

    /* Commit transaction */
    if (!kvidxCommit(i)) {
        /* Commit failed - this is bad */
        if (insertedCount) {
            *insertedCount = 0;
        }
        return false;
    }

    if (insertedCount) {
        *insertedCount = inserted;
    }

    return success;
}

/* ====================================================================
 * Error Handling Implementation
 * ==================================================================== */

kvidxError kvidxGetLastError(const kvidxInstance *i) {
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    return i->lastError;
}

const char *kvidxGetLastErrorMessage(kvidxInstance *i) {
    if (!i) {
        return "Invalid instance (NULL)";
    }
    if (i->lastError == KVIDX_OK) {
        return "Success";
    }
    return i->lastErrorMessage[0] ? i->lastErrorMessage
                                  : kvidxErrorString(i->lastError);
}

void kvidxClearError(kvidxInstance *i) {
    if (i) {
        i->lastError = KVIDX_OK;
        i->lastErrorMessage[0] = '\0';
    }
}

/* Internal helper to set error */
void kvidxSetError(kvidxInstance *i, kvidxError err, const char *fmt, ...) {
    if (!i) {
        return;
    }

    i->lastError = err;

    if (fmt) {
        va_list args;
        va_start(args, fmt);
        vsnprintf(i->lastErrorMessage, sizeof(i->lastErrorMessage), fmt, args);
        va_end(args);
    } else {
        /* Use default error string */
        snprintf(i->lastErrorMessage, sizeof(i->lastErrorMessage), "%s",
                 kvidxErrorString(err));
    }
}

/* ====================================================================
 * Statistics API Implementation
 * ==================================================================== */

kvidxError kvidxGetStats(kvidxInstance *i, kvidxStats *stats) {
    if (!i || !stats) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    return i->interface.getStats(i, stats);
}

kvidxError kvidxGetKeyCount(kvidxInstance *i, uint64_t *count) {
    if (!i || !count) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    return i->interface.getKeyCount(i, count);
}

kvidxError kvidxGetMinKey(kvidxInstance *i, uint64_t *key) {
    if (!i || !key) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    return i->interface.getMinKey(i, key);
}

kvidxError kvidxGetDataSize(kvidxInstance *i, uint64_t *bytes) {
    if (!i || !bytes) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    return i->interface.getDataSize(i, bytes);
}

/* ====================================================================
 * Configuration API Implementation
 * ==================================================================== */

kvidxConfig kvidxConfigDefault(void) {
    kvidxConfig config = {
        .cacheSizeBytes = 32 * 1024 * 1024, /* 32 MB default */
        .vfsName = NULL,                    /* Default VFS */
        .journalMode = KVIDX_JOURNAL_WAL,   /* WAL mode recommended */
        .syncMode = KVIDX_SYNC_NORMAL,      /* Balanced */
        .enableRecursiveTriggers = true,
        .enableForeignKeys = false,
        .readOnly = false,
        .busyTimeoutMs = 5000, /* 5 seconds */
        .mmapSizeBytes = 0,    /* Disabled by default */
        .pageSize = 0          /* Use SQLite default (4096) */
    };
    return config;
}

bool kvidxOpenWithConfig(kvidxInstance *i, const char *filename,
                         const kvidxConfig *config, const char **err) {
    if (!i || !filename) {
        if (err) {
            *err = "Invalid arguments: instance and filename required";
        }
        return false;
    }

    /* Use default config if none provided */
    kvidxConfig defaultConfig;
    if (!config) {
        defaultConfig = kvidxConfigDefault();
        config = &defaultConfig;
    }

    /* Store config in instance before opening */
    i->config = *config;
    i->configInitialized = true;

    /* Open with standard API, then apply configuration */
    bool opened = kvidxOpen(i, filename, err);
    if (!opened) {
        i->configInitialized = false;
        return false;
    }

    /* Apply configuration settings */
    kvidxError configErr = kvidxUpdateConfig(i, config);
    if (configErr != KVIDX_OK) {
        if (err) {
            *err = kvidxGetLastErrorMessage(i);
        }
        kvidxClose(i);
        i->configInitialized = false;
        return false;
    }

    return true;
}

kvidxError kvidxUpdateConfig(kvidxInstance *i, const kvidxConfig *config) {
    if (!i || !config) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Store updated config in instance */
    i->config = *config;
    i->configInitialized = true;

    /* Apply configuration via SQLite adapter (if available) */
#ifdef KVIDXKIT_HAS_SQLITE3
    kvidxError err = kvidxSqlite3ApplyConfig(i, config);
    if (err != KVIDX_OK) {
        return err;
    }
#endif

    return KVIDX_OK;
}

kvidxError kvidxGetConfig(const kvidxInstance *i, kvidxConfig *config) {
    if (!i || !config) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Return stored config if available, otherwise return defaults */
    if (i->configInitialized) {
        *config = i->config;
    } else {
        *config = kvidxConfigDefault();
    }

    return KVIDX_OK;
}
/* ====================================================================
 * Range Operations Implementation (v0.5.0)
 * ==================================================================== */

kvidxError kvidxRemoveRange(kvidxInstance *i, uint64_t startKey,
                            uint64_t endKey, bool startInclusive,
                            bool endInclusive, uint64_t *deletedCount) {
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    return i->interface.removeRange(i, startKey, endKey, startInclusive,
                                    endInclusive, deletedCount);
}

kvidxError kvidxCountRange(kvidxInstance *i, uint64_t startKey, uint64_t endKey,
                           uint64_t *count) {
    if (!i || !count) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    return i->interface.countRange(i, startKey, endKey, count);
}

kvidxError kvidxExistsInRange(kvidxInstance *i, uint64_t startKey,
                              uint64_t endKey, bool *exists) {
    if (!i || !exists) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    return i->interface.existsInRange(i, startKey, endKey, exists);
}

/* ====================================================================
 * Export/Import Implementation (v0.6.0)
 * ==================================================================== */

kvidxExportOptions kvidxExportOptionsDefault(void) {
    kvidxExportOptions options = {.format = KVIDX_EXPORT_BINARY,
                                  .startKey = 0,
                                  .endKey = UINT64_MAX,
                                  .includeMetadata = true,
                                  .prettyPrint = false};
    return options;
}

kvidxImportOptions kvidxImportOptionsDefault(void) {
    kvidxImportOptions options = {.format =
                                      KVIDX_EXPORT_BINARY, /* Auto-detect */
                                  .validateData = true,
                                  .skipDuplicates = false,
                                  .clearBeforeImport = false};
    return options;
}

kvidxError kvidxExport(kvidxInstance *i, const char *filename,
                       const kvidxExportOptions *options,
                       kvidxProgressCallback callback, void *userData) {
    if (!i || !filename) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Use default options if not provided */
    kvidxExportOptions defaultOptions;
    if (!options) {
        defaultOptions = kvidxExportOptionsDefault();
        options = &defaultOptions;
    }

    /* Delegate to backend implementation */
    if (i->interface.exportData) {
        return i->interface.exportData(i, filename, options, callback,
                                       userData);
    }

    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "Export not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

kvidxError kvidxImport(kvidxInstance *i, const char *filename,
                       const kvidxImportOptions *options,
                       kvidxProgressCallback callback, void *userData) {
    if (!i || !filename) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Use default options if not provided */
    kvidxImportOptions defaultOptions;
    if (!options) {
        defaultOptions = kvidxImportOptionsDefault();
        options = &defaultOptions;
    }

    /* Delegate to backend implementation */
    if (i->interface.importData) {
        return i->interface.importData(i, filename, options, callback,
                                       userData);
    }

    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "Import not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

/* ====================================================================
 * Storage Primitives Implementation (v0.8.0)
 * ==================================================================== */

/* --- Transaction Abort --- */

bool kvidxAbort(kvidxInstance *i) {
    VERBOSE_TAG();
    if (!i) {
        return false;
    }
    if (i->interface.abort) {
        return i->interface.abort(i);
    }
    /* Default: abort not supported, but succeed if no transaction active */
    if (!i->transactionActive) {
        return true;
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "Abort not supported by this backend");
    return false;
}

/* --- Conditional Writes --- */

kvidxError kvidxInsertEx(kvidxInstance *i, uint64_t key, uint64_t term,
                         uint64_t cmd, const void *data, size_t dataLen,
                         kvidxSetCondition condition) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.insertEx) {
        return i->interface.insertEx(i, key, term, cmd, data, dataLen,
                                     condition);
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "InsertEx not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

kvidxError kvidxInsertNX(kvidxInstance *i, uint64_t key, uint64_t term,
                         uint64_t cmd, const void *data, size_t dataLen) {
    return kvidxInsertEx(i, key, term, cmd, data, dataLen,
                         KVIDX_SET_IF_NOT_EXISTS);
}

kvidxError kvidxInsertXX(kvidxInstance *i, uint64_t key, uint64_t term,
                         uint64_t cmd, const void *data, size_t dataLen) {
    return kvidxInsertEx(i, key, term, cmd, data, dataLen, KVIDX_SET_IF_EXISTS);
}

/* --- Atomic Operations --- */

kvidxError kvidxGetAndSet(kvidxInstance *i, uint64_t key, uint64_t term,
                          uint64_t cmd, const void *data, size_t dataLen,
                          uint64_t *oldTerm, uint64_t *oldCmd, void **oldData,
                          size_t *oldDataLen) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.getAndSet) {
        return i->interface.getAndSet(i, key, term, cmd, data, dataLen, oldTerm,
                                      oldCmd, oldData, oldDataLen);
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "GetAndSet not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

kvidxError kvidxGetAndRemove(kvidxInstance *i, uint64_t key, uint64_t *term,
                             uint64_t *cmd, void **data, size_t *dataLen) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.getAndRemove) {
        return i->interface.getAndRemove(i, key, term, cmd, data, dataLen);
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "GetAndRemove not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

/* --- Compare-And-Swap --- */

kvidxError kvidxCompareAndSwap(kvidxInstance *i, uint64_t key,
                               const void *expectedData, size_t expectedLen,
                               uint64_t newTerm, uint64_t newCmd,
                               const void *newData, size_t newDataLen,
                               bool *swapped) {
    VERBOSE_TAG();
    if (!i || !swapped) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.compareAndSwap) {
        return i->interface.compareAndSwap(i, key, expectedData, expectedLen,
                                           newTerm, newCmd, newData, newDataLen,
                                           swapped);
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "CompareAndSwap not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

kvidxError kvidxCompareTermAndSwap(kvidxInstance *i, uint64_t key,
                                   uint64_t expectedTerm, uint64_t newTerm,
                                   uint64_t newCmd, const void *newData,
                                   size_t newDataLen, bool *swapped) {
    VERBOSE_TAG();
    if (!i || !swapped) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    /* Implement via read-compare-write if backend doesn't provide native CAS */
    if (!i->interface.compareAndSwap) {
        kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                      "CompareTermAndSwap not supported by this backend");
        return KVIDX_ERROR_NOT_SUPPORTED;
    }

    /* Get current data to compare term */
    uint64_t currentTerm;
    const uint8_t *currentData;
    size_t currentLen;
    if (!kvidxGet(i, key, &currentTerm, NULL, &currentData, &currentLen)) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    if (currentTerm != expectedTerm) {
        *swapped = false;
        return KVIDX_OK;
    }

    /* Term matches, use data-based CAS with current data as expected */
    return i->interface.compareAndSwap(i, key, currentData, currentLen, newTerm,
                                       newCmd, newData, newDataLen, swapped);
}

/* --- Append/Prepend --- */

kvidxError kvidxAppend(kvidxInstance *i, uint64_t key, uint64_t term,
                       uint64_t cmd, const void *data, size_t dataLen,
                       size_t *newLen) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.append) {
        return i->interface.append(i, key, term, cmd, data, dataLen, newLen);
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "Append not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

kvidxError kvidxPrepend(kvidxInstance *i, uint64_t key, uint64_t term,
                        uint64_t cmd, const void *data, size_t dataLen,
                        size_t *newLen) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.prepend) {
        return i->interface.prepend(i, key, term, cmd, data, dataLen, newLen);
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "Prepend not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

/* --- Partial Value Access --- */

kvidxError kvidxGetValueRange(kvidxInstance *i, uint64_t key, size_t offset,
                              size_t length, void **data, size_t *actualLen) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.getValueRange) {
        return i->interface.getValueRange(i, key, offset, length, data,
                                          actualLen);
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "GetValueRange not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

kvidxError kvidxSetValueRange(kvidxInstance *i, uint64_t key, size_t offset,
                              const void *data, size_t dataLen,
                              size_t *newLen) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.setValueRange) {
        return i->interface.setValueRange(i, key, offset, data, dataLen,
                                          newLen);
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "SetValueRange not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

/* --- TTL/Expiration --- */

kvidxError kvidxSetExpire(kvidxInstance *i, uint64_t key, uint64_t ttlMs) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.setExpire) {
        return i->interface.setExpire(i, key, ttlMs);
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "SetExpire not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

kvidxError kvidxSetExpireAt(kvidxInstance *i, uint64_t key,
                            uint64_t timestampMs) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.setExpireAt) {
        return i->interface.setExpireAt(i, key, timestampMs);
    }
    kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                  "SetExpireAt not supported by this backend");
    return KVIDX_ERROR_NOT_SUPPORTED;
}

int64_t kvidxGetTTL(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_TTL_NOT_FOUND;
    }
    if (i->interface.getTTL) {
        return i->interface.getTTL(i, key);
    }
    /* TTL not supported - check if key exists and return no expiration */
    if (kvidxExists(i, key)) {
        return KVIDX_TTL_NONE;
    }
    return KVIDX_TTL_NOT_FOUND;
}

kvidxError kvidxPersist(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.persist) {
        return i->interface.persist(i, key);
    }
    /* Persist not supported - check if key exists, then succeed (no-op) */
    if (!kvidxExists(i, key)) {
        return KVIDX_ERROR_NOT_FOUND;
    }
    return KVIDX_OK; /* No TTL system means key is already persistent */
}

kvidxError kvidxExpireScan(kvidxInstance *i, uint64_t maxKeys,
                           uint64_t *expiredCount) {
    VERBOSE_TAG();
    if (!i) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
    if (i->interface.expireScan) {
        return i->interface.expireScan(i, maxKeys, expiredCount);
    }
    /* No TTL system, nothing to expire */
    if (expiredCount) {
        *expiredCount = 0;
    }
    return KVIDX_OK;
}
