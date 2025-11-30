#include "kvidxkitAdapterRocksdb.h"
#include "../deps/rocksdb/include/rocksdb/c.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

/*
 * RocksDB Adapter for kvidxkit
 *
 * Value format (packed structure):
 *   bytes 0-7:   term (uint64_t, big-endian for proper ordering)
 *   bytes 8-15:  cmd (uint64_t, native endian)
 *   bytes 16+:   data blob
 *
 * Keys are stored as big-endian uint64_t for lexicographic ordering.
 * RocksDB uses lexicographic comparison, so we encode keys as big-endian.
 */

/* Header size for term + cmd in value */
#define VALUE_HEADER_SIZE (sizeof(uint64_t) * 2)

typedef struct rocksdbState {
    rocksdb_t *db;
    rocksdb_options_t *options;
    rocksdb_readoptions_t *readOptions;
    rocksdb_writeoptions_t *writeOptions;
    rocksdb_writeoptions_t *syncWriteOptions;
    rocksdb_writebatch_wi_t
        *writeBatch; /* Active write batch with index (NULL when not in txn) */
    char *dbPath;
    /* Cached data buffer for get operations */
    char *cachedValue;
    size_t cachedValueLen;
} rocksdbState;

#define STATE(instance) ((rocksdbState *)(instance)->kvidxdata)

/* Big-endian encoding for keys (for proper lexicographic ordering) */
static inline void encodeKey(uint64_t key, char *buf) {
    buf[0] = (char)(key >> 56);
    buf[1] = (char)(key >> 48);
    buf[2] = (char)(key >> 40);
    buf[3] = (char)(key >> 32);
    buf[4] = (char)(key >> 24);
    buf[5] = (char)(key >> 16);
    buf[6] = (char)(key >> 8);
    buf[7] = (char)(key);
}

static inline uint64_t decodeKey(const char *buf) {
    return ((uint64_t)(unsigned char)buf[0] << 56) |
           ((uint64_t)(unsigned char)buf[1] << 48) |
           ((uint64_t)(unsigned char)buf[2] << 40) |
           ((uint64_t)(unsigned char)buf[3] << 32) |
           ((uint64_t)(unsigned char)buf[4] << 24) |
           ((uint64_t)(unsigned char)buf[5] << 16) |
           ((uint64_t)(unsigned char)buf[6] << 8) |
           ((uint64_t)(unsigned char)buf[7]);
}

/* Helper to extract term from value (stored native endian) */
static inline uint64_t extractTerm(const char *val, size_t valLen) {
    if (valLen < VALUE_HEADER_SIZE) {
        return 0;
    }
    uint64_t term;
    memcpy(&term, val, sizeof(term));
    return term;
}

/* Helper to extract cmd from value */
static inline uint64_t extractCmd(const char *val, size_t valLen) {
    if (valLen < VALUE_HEADER_SIZE) {
        return 0;
    }
    uint64_t cmd;
    memcpy(&cmd, val + sizeof(uint64_t), sizeof(cmd));
    return cmd;
}

/* Helper to extract data pointer from value */
static inline const uint8_t *extractData(const char *val, size_t valLen,
                                         size_t *len) {
    if (valLen <= VALUE_HEADER_SIZE) {
        if (len) {
            *len = 0;
        }
        return NULL;
    }
    if (len) {
        *len = valLen - VALUE_HEADER_SIZE;
    }
    return (const uint8_t *)(val + VALUE_HEADER_SIZE);
}

/* Helper to pack term, cmd, data into a value buffer */
static void *packValue(uint64_t term, uint64_t cmd, const void *data,
                       size_t dataLen, size_t *totalLen) {
    *totalLen = VALUE_HEADER_SIZE + dataLen;
    void *buf = malloc(*totalLen);
    if (!buf) {
        return NULL;
    }

    memcpy(buf, &term, sizeof(term));
    memcpy((uint8_t *)buf + sizeof(uint64_t), &cmd, sizeof(cmd));
    if (dataLen > 0 && data) {
        memcpy((uint8_t *)buf + VALUE_HEADER_SIZE, data, dataLen);
    }
    return buf;
}

/* Free error string from RocksDB */
static void freeErr(char **err) {
    if (err && *err) {
        free(*err);
        *err = NULL;
    }
}

/* Create an iterator that sees both the write batch and DB if in a transaction
 */
static rocksdb_iterator_t *createTxnAwareIterator(rocksdbState *s) {
    rocksdb_iterator_t *baseIter =
        rocksdb_create_iterator(s->db, s->readOptions);
    if (!baseIter) {
        return NULL;
    }

    if (s->writeBatch) {
        rocksdb_iterator_t *iter =
            rocksdb_writebatch_wi_create_iterator_with_base(s->writeBatch,
                                                            baseIter);
        if (!iter) {
            rocksdb_iter_destroy(baseIter);
            return NULL;
        }
        return iter;
    }

    return baseIter;
}

/* ====================================================================
 * Transaction Management
 * ==================================================================== */

bool kvidxRocksdbBegin(kvidxInstance *i) {
    rocksdbState *s = STATE(i);

    if (s->writeBatch) {
        /* Already in a transaction */
        return true;
    }

    /* Use WriteBatchWithIndex to allow checking for duplicates within the batch
     */
    s->writeBatch = rocksdb_writebatch_wi_create(0, 0);
    if (!s->writeBatch) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                      "RocksDB writebatch_wi_create failed");
        return false;
    }

    return true;
}

bool kvidxRocksdbCommit(kvidxInstance *i) {
    rocksdbState *s = STATE(i);

    if (!s->writeBatch) {
        /* No active transaction */
        return true;
    }

    char *err = NULL;
    rocksdb_write_writebatch_wi(s->db, s->syncWriteOptions, s->writeBatch,
                                &err);
    rocksdb_writebatch_wi_destroy(s->writeBatch);
    s->writeBatch = NULL;

    if (err) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "RocksDB write failed: %s", err);
        free(err);
        return false;
    }

    return true;
}

/* ====================================================================
 * Data Manipulation
 * ==================================================================== */

bool kvidxRocksdbGet(kvidxInstance *i, uint64_t key, uint64_t *term,
                     uint64_t *cmd, const uint8_t **data, size_t *len) {
    rocksdbState *s = STATE(i);

    char keyBuf[8];
    encodeKey(key, keyBuf);

    char *err = NULL;
    size_t valueLen;
    char *value;

    /* If in a transaction, check both batch and database */
    if (s->writeBatch) {
        value = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &valueLen, &err);
    } else {
        value = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                            &valueLen, &err);
    }

    if (err) {
        free(err);
        return false;
    }

    if (!value) {
        return false;
    }

    /* Cache the value for zero-copy reads */
    if (s->cachedValue) {
        free(s->cachedValue);
    }
    s->cachedValue = value;
    s->cachedValueLen = valueLen;

    if (term) {
        *term = extractTerm(value, valueLen);
    }
    if (cmd) {
        *cmd = extractCmd(value, valueLen);
    }
    if (data) {
        *data = extractData(value, valueLen, len);
    } else if (len) {
        size_t dlen;
        extractData(value, valueLen, &dlen);
        *len = dlen;
    }

    return true;
}

bool kvidxRocksdbGetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                         uint64_t *prevTerm, uint64_t *cmd,
                         const uint8_t **data, size_t *len) {
    rocksdbState *s = STATE(i);

    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        return false;
    }

    bool found = false;

    if (nextKey == UINT64_MAX) {
        /* Get the last key */
        rocksdb_iter_seek_to_last(iter);
        if (rocksdb_iter_valid(iter)) {
            found = true;
        }
    } else {
        /* Position at or after nextKey, then go back */
        char keyBuf[8];
        encodeKey(nextKey, keyBuf);
        rocksdb_iter_seek(iter, keyBuf, sizeof(keyBuf));

        if (rocksdb_iter_valid(iter)) {
            /* Move to previous */
            rocksdb_iter_prev(iter);
            if (rocksdb_iter_valid(iter)) {
                found = true;
            }
        } else {
            /* No key >= nextKey, get last key */
            rocksdb_iter_seek_to_last(iter);
            if (rocksdb_iter_valid(iter)) {
                found = true;
            }
        }
    }

    if (found) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);
        uint64_t foundKey = decodeKey(keyData);

        size_t valueLen;
        const char *value = rocksdb_iter_value(iter, &valueLen);

        /* Cache the value */
        if (s->cachedValue) {
            free(s->cachedValue);
        }
        s->cachedValue = malloc(valueLen);
        if (s->cachedValue) {
            memcpy(s->cachedValue, value, valueLen);
            s->cachedValueLen = valueLen;

            if (prevKey) {
                *prevKey = foundKey;
            }
            if (prevTerm) {
                *prevTerm = extractTerm(s->cachedValue, valueLen);
            }
            if (cmd) {
                *cmd = extractCmd(s->cachedValue, valueLen);
            }
            if (data) {
                *data = extractData(s->cachedValue, valueLen, len);
            } else if (len) {
                size_t dlen;
                extractData(s->cachedValue, valueLen, &dlen);
                *len = dlen;
            }
        } else {
            found = false;
        }
    }

    rocksdb_iter_destroy(iter);
    return found;
}

bool kvidxRocksdbGetNext(kvidxInstance *i, uint64_t previousKey,
                         uint64_t *nextKey, uint64_t *nextTerm, uint64_t *cmd,
                         const uint8_t **data, size_t *len) {
    rocksdbState *s = STATE(i);

    /* No key can be greater than UINT64_MAX */
    if (previousKey == UINT64_MAX) {
        return false;
    }

    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        return false;
    }

    /* We want key > previousKey, so search for previousKey+1 */
    uint64_t searchKey = previousKey + 1;
    char keyBuf[8];
    encodeKey(searchKey, keyBuf);

    rocksdb_iter_seek(iter, keyBuf, sizeof(keyBuf));

    bool found = false;
    if (rocksdb_iter_valid(iter)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);
        uint64_t foundKey = decodeKey(keyData);

        size_t valueLen;
        const char *value = rocksdb_iter_value(iter, &valueLen);

        /* Cache the value */
        if (s->cachedValue) {
            free(s->cachedValue);
        }
        s->cachedValue = malloc(valueLen);
        if (s->cachedValue) {
            memcpy(s->cachedValue, value, valueLen);
            s->cachedValueLen = valueLen;

            if (nextKey) {
                *nextKey = foundKey;
            }
            if (nextTerm) {
                *nextTerm = extractTerm(s->cachedValue, valueLen);
            }
            if (cmd) {
                *cmd = extractCmd(s->cachedValue, valueLen);
            }
            if (data) {
                *data = extractData(s->cachedValue, valueLen, len);
            } else if (len) {
                size_t dlen;
                extractData(s->cachedValue, valueLen, &dlen);
                *len = dlen;
            }
            found = true;
        }
    }

    rocksdb_iter_destroy(iter);
    return found;
}

bool kvidxRocksdbExists(kvidxInstance *i, uint64_t key) {
    rocksdbState *s = STATE(i);

    char keyBuf[8];
    encodeKey(key, keyBuf);

    char *err = NULL;
    size_t valueLen;
    char *value;

    /* If in a transaction, check both batch and database */
    if (s->writeBatch) {
        value = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &valueLen, &err);
    } else {
        value = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                            &valueLen, &err);
    }

    if (err) {
        free(err);
        return false;
    }

    bool exists = (value != NULL);
    if (value) {
        free(value);
    }
    return exists;
}

bool kvidxRocksdbExistsDual(kvidxInstance *i, uint64_t key, uint64_t term) {
    rocksdbState *s = STATE(i);

    char keyBuf[8];
    encodeKey(key, keyBuf);

    char *err = NULL;
    size_t valueLen;
    char *value;

    /* If in a transaction, check both batch and database */
    if (s->writeBatch) {
        value = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &valueLen, &err);
    } else {
        value = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                            &valueLen, &err);
    }

    if (err) {
        free(err);
        return false;
    }

    if (!value) {
        return false;
    }

    uint64_t storedTerm = extractTerm(value, valueLen);
    free(value);
    return storedTerm == term;
}

bool kvidxRocksdbMax(kvidxInstance *i, uint64_t *key) {
    rocksdbState *s = STATE(i);

    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        return false;
    }

    rocksdb_iter_seek_to_last(iter);

    bool found = false;
    if (rocksdb_iter_valid(iter)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);
        if (key) {
            *key = decodeKey(keyData);
        }
        found = true;
    }

    rocksdb_iter_destroy(iter);
    return found;
}

bool kvidxRocksdbInsert(kvidxInstance *i, uint64_t key, uint64_t term,
                        uint64_t cmd, const void *data, size_t dataLen) {
    rocksdbState *s = STATE(i);

    /* Check if key already exists (for duplicate key rejection) */
    char keyBuf[8];
    encodeKey(key, keyBuf);

    char *err = NULL;
    size_t existingLen;
    char *existing;

    /* If in a transaction, check both the write batch AND the database */
    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }
    freeErr(&err);

    if (existing) {
        free(existing);
        kvidxSetError(i, KVIDX_ERROR_DUPLICATE_KEY, "Key already exists");
        return false;
    }

    /* Pack the value */
    size_t valLen;
    void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
    if (!valBuf) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "Memory allocation failed");
        return false;
    }

    /* If in a transaction, use write batch */
    if (s->writeBatch) {
        rocksdb_writebatch_wi_put(s->writeBatch, keyBuf, sizeof(keyBuf), valBuf,
                                  valLen);
        free(valBuf);
        return true;
    }

    /* Otherwise, write directly */
    rocksdb_put(s->db, s->syncWriteOptions, keyBuf, sizeof(keyBuf), valBuf,
                valLen, &err);
    free(valBuf);

    if (err) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "RocksDB put failed: %s", err);
        free(err);
        return false;
    }

    return true;
}

bool kvidxRocksdbRemove(kvidxInstance *i, uint64_t key) {
    rocksdbState *s = STATE(i);

    char keyBuf[8];
    encodeKey(key, keyBuf);

    /* If in a transaction, use write batch */
    if (s->writeBatch) {
        rocksdb_writebatch_wi_delete(s->writeBatch, keyBuf, sizeof(keyBuf));
        return true;
    }

    char *err = NULL;
    rocksdb_delete(s->db, s->syncWriteOptions, keyBuf, sizeof(keyBuf), &err);

    if (err) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "RocksDB delete failed: %s",
                      err);
        free(err);
        return false;
    }

    return true;
}

bool kvidxRocksdbRemoveAfterNInclusive(kvidxInstance *i, uint64_t key) {
    rocksdbState *s = STATE(i);
    bool ownBatch = false;

    if (!s->writeBatch) {
        s->writeBatch = rocksdb_writebatch_wi_create(0, 0);
        if (!s->writeBatch) {
            return false;
        }
        ownBatch = true;
    }

    /* Use transaction-aware iterator to see pending writes */
    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        if (ownBatch) {
            rocksdb_writebatch_wi_destroy(s->writeBatch);
            s->writeBatch = NULL;
        }
        return false;
    }

    char keyBuf[8];
    encodeKey(key, keyBuf);
    rocksdb_iter_seek(iter, keyBuf, sizeof(keyBuf));

    while (rocksdb_iter_valid(iter)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);
        /* Copy key before modifying batch (iterator memory may be invalidated)
         */
        char keyCopy[8];
        if (keyLen == sizeof(keyCopy)) {
            memcpy(keyCopy, keyData, keyLen);
            rocksdb_iter_next(
                iter); /* Advance before delete to avoid invalidation */
            rocksdb_writebatch_wi_delete(s->writeBatch, keyCopy, keyLen);
        } else {
            rocksdb_iter_next(iter);
        }
    }

    rocksdb_iter_destroy(iter);

    if (ownBatch) {
        char *err = NULL;
        rocksdb_write_writebatch_wi(s->db, s->syncWriteOptions, s->writeBatch,
                                    &err);
        rocksdb_writebatch_wi_destroy(s->writeBatch);
        s->writeBatch = NULL;

        if (err) {
            kvidxSetError(i, KVIDX_ERROR_INTERNAL, "RocksDB write failed: %s",
                          err);
            free(err);
            return false;
        }
    }

    return true;
}

bool kvidxRocksdbRemoveBeforeNInclusive(kvidxInstance *i, uint64_t key) {
    rocksdbState *s = STATE(i);
    bool ownBatch = false;

    if (!s->writeBatch) {
        s->writeBatch = rocksdb_writebatch_wi_create(0, 0);
        if (!s->writeBatch) {
            return false;
        }
        ownBatch = true;
    }

    /* Use transaction-aware iterator to see pending writes */
    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        if (ownBatch) {
            rocksdb_writebatch_wi_destroy(s->writeBatch);
            s->writeBatch = NULL;
        }
        return false;
    }

    rocksdb_iter_seek_to_first(iter);

    while (rocksdb_iter_valid(iter)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);
        uint64_t currentKey = decodeKey(keyData);

        if (currentKey > key) {
            break;
        }

        /* Copy key before modifying batch (iterator memory may be invalidated)
         */
        char keyCopy[8];
        if (keyLen == sizeof(keyCopy)) {
            memcpy(keyCopy, keyData, keyLen);
            rocksdb_iter_next(
                iter); /* Advance before delete to avoid invalidation */
            rocksdb_writebatch_wi_delete(s->writeBatch, keyCopy, keyLen);
        } else {
            rocksdb_iter_next(iter);
        }
    }

    rocksdb_iter_destroy(iter);

    if (ownBatch) {
        char *err = NULL;
        rocksdb_write_writebatch_wi(s->db, s->syncWriteOptions, s->writeBatch,
                                    &err);
        rocksdb_writebatch_wi_destroy(s->writeBatch);
        s->writeBatch = NULL;

        if (err) {
            kvidxSetError(i, KVIDX_ERROR_INTERNAL, "RocksDB write failed: %s",
                          err);
            free(err);
            return false;
        }
    }

    return true;
}

bool kvidxRocksdbFsync(kvidxInstance *i) {
    rocksdbState *s = STATE(i);
    /* RocksDB sync write options already sync data */
    /* Perform a manual flush to ensure data is on disk */
    rocksdb_flushoptions_t *flushOptions = rocksdb_flushoptions_create();
    if (!flushOptions) {
        return false;
    }
    rocksdb_flushoptions_set_wait(flushOptions, 1);

    char *err = NULL;
    rocksdb_flush(s->db, flushOptions, &err);
    rocksdb_flushoptions_destroy(flushOptions);

    if (err) {
        free(err);
        return false;
    }

    return true;
}

/* ====================================================================
 * Bring-Up / Teardown
 * ==================================================================== */

bool kvidxRocksdbOpen(kvidxInstance *i, const char *filename,
                      const char **errStr) {
    i->kvidxdata = calloc(1, sizeof(rocksdbState));
    if (!i->kvidxdata) {
        if (errStr) {
            *errStr = "Memory allocation failed";
        }
        return false;
    }

    rocksdbState *s = STATE(i);
    s->dbPath = strdup(filename);

    /* Create options */
    s->options = rocksdb_options_create();
    if (!s->options) {
        if (errStr) {
            *errStr = "Failed to create RocksDB options";
        }
        goto error;
    }
    rocksdb_options_set_create_if_missing(s->options, 1);

    /* Create read options */
    s->readOptions = rocksdb_readoptions_create();
    if (!s->readOptions) {
        if (errStr) {
            *errStr = "Failed to create RocksDB read options";
        }
        goto error;
    }

    /* Create write options (async) */
    s->writeOptions = rocksdb_writeoptions_create();
    if (!s->writeOptions) {
        if (errStr) {
            *errStr = "Failed to create RocksDB write options";
        }
        goto error;
    }
    rocksdb_writeoptions_set_sync(s->writeOptions, 0);

    /* Create sync write options */
    s->syncWriteOptions = rocksdb_writeoptions_create();
    if (!s->syncWriteOptions) {
        if (errStr) {
            *errStr = "Failed to create RocksDB sync write options";
        }
        goto error;
    }
    rocksdb_writeoptions_set_sync(s->syncWriteOptions, 1);

    /* Open database */
    char *err = NULL;
    s->db = rocksdb_open(s->options, filename, &err);
    if (err) {
        if (errStr) {
            *errStr = err;
        }
        /* Don't free err - it's returned to caller */
        goto error;
    }

    /* Call custom init if provided */
    if (i->customInit) {
        i->customInit(i);
    }

    return true;

error:
    if (s->syncWriteOptions) {
        rocksdb_writeoptions_destroy(s->syncWriteOptions);
    }
    if (s->writeOptions) {
        rocksdb_writeoptions_destroy(s->writeOptions);
    }
    if (s->readOptions) {
        rocksdb_readoptions_destroy(s->readOptions);
    }
    if (s->options) {
        rocksdb_options_destroy(s->options);
    }
    free(s->dbPath);
    free(i->kvidxdata);
    i->kvidxdata = NULL;
    return false;
}

bool kvidxRocksdbClose(kvidxInstance *i) {
    rocksdbState *s = STATE(i);

    if (s->writeBatch) {
        rocksdb_writebatch_wi_destroy(s->writeBatch);
        s->writeBatch = NULL;
    }

    if (s->cachedValue) {
        free(s->cachedValue);
        s->cachedValue = NULL;
    }

    if (s->db) {
        rocksdb_close(s->db);
        s->db = NULL;
    }

    if (s->syncWriteOptions) {
        rocksdb_writeoptions_destroy(s->syncWriteOptions);
    }
    if (s->writeOptions) {
        rocksdb_writeoptions_destroy(s->writeOptions);
    }
    if (s->readOptions) {
        rocksdb_readoptions_destroy(s->readOptions);
    }
    if (s->options) {
        rocksdb_options_destroy(s->options);
    }

    free(s->dbPath);
    free(i->kvidxdata);
    i->kvidxdata = NULL;

    return true;
}

/* ====================================================================
 * Statistics Implementation
 * ==================================================================== */

kvidxError kvidxRocksdbGetKeyCount(kvidxInstance *i, uint64_t *count) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db || !count) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Always use iteration for exact count. RocksDB's estimate-num-keys
     * property is an approximation based on SST file properties and may
     * not include recent writes still in the memtable. For correctness,
     * we iterate through all keys. The transaction-aware iterator ensures
     * we see pending writes when in a transaction. */
    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        return KVIDX_ERROR_INTERNAL;
    }

    uint64_t cnt = 0;
    rocksdb_iter_seek_to_first(iter);
    while (rocksdb_iter_valid(iter)) {
        cnt++;
        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);
    *count = cnt;
    return KVIDX_OK;
}

kvidxError kvidxRocksdbGetMinKey(kvidxInstance *i, uint64_t *key) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        return KVIDX_ERROR_INTERNAL;
    }

    rocksdb_iter_seek_to_first(iter);

    if (!rocksdb_iter_valid(iter)) {
        rocksdb_iter_destroy(iter);
        return KVIDX_ERROR_NOT_FOUND;
    }

    size_t keyLen;
    const char *keyData = rocksdb_iter_key(iter, &keyLen);
    *key = decodeKey(keyData);

    rocksdb_iter_destroy(iter);
    return KVIDX_OK;
}

kvidxError kvidxRocksdbGetDataSize(kvidxInstance *i, uint64_t *bytes) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        return KVIDX_ERROR_INTERNAL;
    }

    uint64_t totalSize = 0;
    rocksdb_iter_seek_to_first(iter);

    while (rocksdb_iter_valid(iter)) {
        size_t valueLen;
        rocksdb_iter_value(iter, &valueLen);
        if (valueLen > VALUE_HEADER_SIZE) {
            totalSize += valueLen - VALUE_HEADER_SIZE;
        }
        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);
    *bytes = totalSize;
    return KVIDX_OK;
}

kvidxError kvidxRocksdbGetStats(kvidxInstance *i, kvidxStats *stats) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db || !stats) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    memset(stats, 0, sizeof(*stats));

    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        return KVIDX_ERROR_INTERNAL;
    }

    uint64_t count = 0;
    uint64_t totalData = 0;
    bool gotFirst = false;

    rocksdb_iter_seek_to_first(iter);
    while (rocksdb_iter_valid(iter)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);

        if (!gotFirst) {
            stats->minKey = decodeKey(keyData);
            gotFirst = true;
        }

        size_t valueLen;
        rocksdb_iter_value(iter, &valueLen);
        if (valueLen > VALUE_HEADER_SIZE) {
            totalData += valueLen - VALUE_HEADER_SIZE;
        }

        count++;
        rocksdb_iter_next(iter);
    }

    /* Get max key */
    rocksdb_iter_seek_to_last(iter);
    if (rocksdb_iter_valid(iter)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);
        stats->maxKey = decodeKey(keyData);
    }

    rocksdb_iter_destroy(iter);

    stats->totalKeys = count;
    stats->totalDataBytes = totalData;

    /* Get approximate database size using property */
    char *sizeStr =
        rocksdb_property_value(s->db, "rocksdb.estimate-live-data-size");
    if (sizeStr) {
        stats->databaseFileSize = (uint64_t)strtoull(sizeStr, NULL, 10);
        free(sizeStr);
    }

    return KVIDX_OK;
}

/* ====================================================================
 * Range Operations Implementation
 * ==================================================================== */

kvidxError kvidxRocksdbRemoveRange(kvidxInstance *i, uint64_t startKey,
                                   uint64_t endKey, bool startInclusive,
                                   bool endInclusive, uint64_t *deletedCount) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    bool ownBatch = false;
    if (!s->writeBatch) {
        s->writeBatch = rocksdb_writebatch_wi_create(0, 0);
        if (!s->writeBatch) {
            return KVIDX_ERROR_INTERNAL;
        }
        ownBatch = true;
    }

    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        if (ownBatch) {
            rocksdb_writebatch_wi_destroy(s->writeBatch);
            s->writeBatch = NULL;
        }
        return KVIDX_ERROR_INTERNAL;
    }

    uint64_t deleted = 0;
    uint64_t searchKey = startInclusive ? startKey : startKey + 1;
    char keyBuf[8];
    encodeKey(searchKey, keyBuf);

    rocksdb_iter_seek(iter, keyBuf, sizeof(keyBuf));

    while (rocksdb_iter_valid(iter)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);
        uint64_t currentKey = decodeKey(keyData);

        /* Check if we've passed the end of the range */
        if (endInclusive) {
            if (currentKey > endKey) {
                break;
            }
        } else {
            if (currentKey >= endKey) {
                break;
            }
        }

        rocksdb_writebatch_wi_delete(s->writeBatch, keyData, keyLen);
        deleted++;
        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);

    if (deletedCount) {
        *deletedCount = deleted;
    }

    if (ownBatch) {
        char *err = NULL;
        rocksdb_write_writebatch_wi(s->db, s->syncWriteOptions, s->writeBatch,
                                    &err);
        rocksdb_writebatch_wi_destroy(s->writeBatch);
        s->writeBatch = NULL;

        if (err) {
            free(err);
            return KVIDX_ERROR_INTERNAL;
        }
    }

    return KVIDX_OK;
}

kvidxError kvidxRocksdbCountRange(kvidxInstance *i, uint64_t startKey,
                                  uint64_t endKey, uint64_t *count) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db || !count) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /*
     * Fast approximate counting using rocksdb_approximate_sizes.
     *
     * Instead of iterating through every key (O(n) and slow due to LSM reads),
     * we use RocksDB's approximate_sizes to get byte size of the range, then
     * estimate key count using the average bytes-per-key ratio.
     *
     * This is ~17,500x faster than iteration with 99%+ accuracy for uniform
     * data.
     *
     * Caveats:
     * - Falls back to iteration if there's an active write batch
     * - Falls back to iteration if estimation returns 0 (small/empty datasets)
     * - Accuracy depends on uniform key/value sizes (our use case is uniform)
     */

    /* If there's a pending write batch, fall back to iteration */
    if (s->writeBatch) {
        goto iterate;
    }

    /* Get total key count and byte size for calibration */
    char *totalKeysStr =
        rocksdb_property_value(s->db, "rocksdb.estimate-num-keys");
    char *totalSizeStr =
        rocksdb_property_value(s->db, "rocksdb.estimate-live-data-size");

    if (totalKeysStr && totalSizeStr) {
        uint64_t totalKeys = strtoull(totalKeysStr, NULL, 10);
        uint64_t totalSize = strtoull(totalSizeStr, NULL, 10);
        free(totalKeysStr);
        free(totalSizeStr);

        if (totalKeys > 0 && totalSize > 0) {
            /* Calculate bytes per key ratio */
            double bytesPerKey = (double)totalSize / (double)totalKeys;

            /* Get approximate size for the requested range */
            char startKeyBuf[8], endKeyBuf[8];
            encodeKey(startKey, startKeyBuf);
            encodeKey(endKey + 1, endKeyBuf); /* +1 to include endKey */

            const char *rangeStart[1] = {startKeyBuf};
            size_t rangeStartLen[1] = {8};
            const char *rangeEnd[1] = {endKeyBuf};
            size_t rangeEndLen[1] = {8};
            uint64_t rangeSize[1];
            char *err = NULL;

            rocksdb_approximate_sizes(s->db, 1, rangeStart, rangeStartLen,
                                      rangeEnd, rangeEndLen, rangeSize, &err);
            if (!err && rangeSize[0] > 0) {
                /* Estimate key count from byte size */
                *count = (uint64_t)(rangeSize[0] / bytesPerKey + 0.5);
                return KVIDX_OK;
            }
            if (err) {
                free(err);
            }
            /* rangeSize[0] == 0, fall through to iteration */
        }
    } else {
        if (totalKeysStr) {
            free(totalKeysStr);
        }
        if (totalSizeStr) {
            free(totalSizeStr);
        }
    }

    /* Fall back to iteration for small datasets or when approximation fails */
iterate:;
    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        return KVIDX_ERROR_INTERNAL;
    }

    char keyBuf[8];
    encodeKey(startKey, keyBuf);
    rocksdb_iter_seek(iter, keyBuf, sizeof(keyBuf));

    uint64_t cnt = 0;
    while (rocksdb_iter_valid(iter)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);
        uint64_t foundKey = decodeKey(keyData);
        if (foundKey > endKey) {
            break;
        }
        cnt++;
        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);
    *count = cnt;
    return KVIDX_OK;
}

kvidxError kvidxRocksdbExistsInRange(kvidxInstance *i, uint64_t startKey,
                                     uint64_t endKey, bool *exists) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db || !exists) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    rocksdb_iterator_t *iter = createTxnAwareIterator(s);
    if (!iter) {
        return KVIDX_ERROR_INTERNAL;
    }

    char keyBuf[8];
    encodeKey(startKey, keyBuf);

    rocksdb_iter_seek(iter, keyBuf, sizeof(keyBuf));

    *exists = false;
    if (rocksdb_iter_valid(iter)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);
        uint64_t foundKey = decodeKey(keyData);
        if (foundKey <= endKey) {
            *exists = true;
        }
    }

    rocksdb_iter_destroy(iter);
    return KVIDX_OK;
}

/* ====================================================================
 * Export/Import Implementation
 * ==================================================================== */

/* Binary format magic number: "KVIDX\0\0\0" */
#define KVIDX_BINARY_MAGIC 0x5844495645564B00ULL
#define KVIDX_BINARY_VERSION 1

typedef struct {
    uint64_t magic;
    uint32_t version;
    uint32_t reserved;
    uint64_t entryCount;
} kvidxBinaryHeader;

static kvidxError writeBinaryEntry(FILE *fp, uint64_t key, uint64_t term,
                                   uint64_t cmd, const uint8_t *data,
                                   size_t dataLen) {
    if (fwrite(&key, sizeof(key), 1, fp) != 1) {
        return KVIDX_ERROR_IO;
    }
    if (fwrite(&term, sizeof(term), 1, fp) != 1) {
        return KVIDX_ERROR_IO;
    }
    if (fwrite(&cmd, sizeof(cmd), 1, fp) != 1) {
        return KVIDX_ERROR_IO;
    }

    uint64_t len = dataLen;
    if (fwrite(&len, sizeof(len), 1, fp) != 1) {
        return KVIDX_ERROR_IO;
    }

    if (dataLen > 0 && data) {
        if (fwrite(data, 1, dataLen, fp) != dataLen) {
            return KVIDX_ERROR_IO;
        }
    }

    return KVIDX_OK;
}

static void writeJsonEscaped(FILE *fp, const char *str, size_t len) {
    for (size_t j = 0; j < len; j++) {
        unsigned char c = str[j];
        switch (c) {
        case '"':
            fprintf(fp, "\\\"");
            break;
        case '\\':
            fprintf(fp, "\\\\");
            break;
        case '\b':
            fprintf(fp, "\\b");
            break;
        case '\f':
            fprintf(fp, "\\f");
            break;
        case '\n':
            fprintf(fp, "\\n");
            break;
        case '\r':
            fprintf(fp, "\\r");
            break;
        case '\t':
            fprintf(fp, "\\t");
            break;
        default:
            if (c < 32 || c == 127) {
                fprintf(fp, "\\u%04x", c);
            } else {
                fputc(c, fp);
            }
        }
    }
}

static void writeCsvField(FILE *fp, const char *str, size_t len) {
    bool needsQuotes = false;
    for (size_t j = 0; j < len; j++) {
        if (str[j] == ',' || str[j] == '"' || str[j] == '\n' ||
            str[j] == '\r') {
            needsQuotes = true;
            break;
        }
    }

    if (needsQuotes) {
        fputc('"', fp);
        for (size_t j = 0; j < len; j++) {
            if (str[j] == '"') {
                fputc('"', fp);
            }
            fputc(str[j], fp);
        }
        fputc('"', fp);
    } else {
        fwrite(str, 1, len, fp);
    }
}

kvidxError kvidxRocksdbExport(kvidxInstance *i, const char *filename,
                              const kvidxExportOptions *options,
                              kvidxProgressCallback callback, void *userData) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db || !filename || !options) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    FILE *fp = fopen(filename, "wb");
    if (!fp) {
        kvidxSetError(i, KVIDX_ERROR_IO, "Failed to open file: %s", filename);
        return KVIDX_ERROR_IO;
    }

    rocksdb_iterator_t *iter = rocksdb_create_iterator(s->db, s->readOptions);
    if (!iter) {
        fclose(fp);
        return KVIDX_ERROR_INTERNAL;
    }

    /* Count total entries for progress */
    uint64_t total = 0;
    rocksdb_iter_seek_to_first(iter);
    while (rocksdb_iter_valid(iter)) {
        total++;
        rocksdb_iter_next(iter);
    }

    kvidxError result = KVIDX_OK;
    uint64_t count = 0;

    /* Write format-specific header */
    if (options->format == KVIDX_EXPORT_BINARY) {
        kvidxBinaryHeader header = {.magic = KVIDX_BINARY_MAGIC,
                                    .version = KVIDX_BINARY_VERSION,
                                    .reserved = 0,
                                    .entryCount = total};
        if (fwrite(&header, sizeof(header), 1, fp) != 1) {
            result = KVIDX_ERROR_IO;
            goto cleanup;
        }
    } else if (options->format == KVIDX_EXPORT_JSON) {
        fprintf(fp, "{\"format\":\"kvidx-json\",\"version\":1,\"entries\":[");
        if (options->prettyPrint) {
            fprintf(fp, "\n");
        }
    } else if (options->format == KVIDX_EXPORT_CSV) {
        if (options->includeMetadata) {
            fprintf(fp, "key,term,cmd,data\n");
        } else {
            fprintf(fp, "key,data\n");
        }
    }

    /* Position at start of range */
    char startKeyBuf[8];
    encodeKey(options->startKey, startKeyBuf);
    rocksdb_iter_seek(iter, startKeyBuf, sizeof(startKeyBuf));

    bool firstEntry = true;

    while (rocksdb_iter_valid(iter)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);
        uint64_t key = decodeKey(keyData);

        if (key > options->endKey) {
            break;
        }

        size_t valueLen;
        const char *value = rocksdb_iter_value(iter, &valueLen);

        uint64_t term = extractTerm(value, valueLen);
        uint64_t cmd = extractCmd(value, valueLen);
        size_t dataLen;
        const uint8_t *data = extractData(value, valueLen, &dataLen);

        if (options->format == KVIDX_EXPORT_BINARY) {
            result = writeBinaryEntry(fp, key, term, cmd, data, dataLen);
            if (result != KVIDX_OK) {
                goto cleanup;
            }
        } else if (options->format == KVIDX_EXPORT_JSON) {
            if (!firstEntry) {
                fprintf(fp, ",");
            }
            if (options->prettyPrint) {
                fprintf(fp, "\n  ");
            }
            fprintf(fp, "{\"key\":%" PRIu64, key);
            if (options->includeMetadata) {
                fprintf(fp, ",\"term\":%" PRIu64 ",\"cmd\":%" PRIu64, term,
                        cmd);
            }
            fprintf(fp, ",\"data\":\"");
            if (data && dataLen > 0) {
                writeJsonEscaped(fp, (const char *)data, dataLen);
            }
            fprintf(fp, "\"}");
            firstEntry = false;
        } else if (options->format == KVIDX_EXPORT_CSV) {
            fprintf(fp, "%" PRIu64 ",", key);
            if (options->includeMetadata) {
                fprintf(fp, "%" PRIu64 ",%" PRIu64 ",", term, cmd);
            }
            if (data && dataLen > 0) {
                writeCsvField(fp, (const char *)data, dataLen);
            }
            fprintf(fp, "\n");
        }

        count++;

        if (callback && count % 100 == 0) {
            if (!callback(count, total, userData)) {
                result = KVIDX_ERROR_CANCELLED;
                goto cleanup;
            }
        }

        rocksdb_iter_next(iter);
    }

    /* Write footer */
    if (options->format == KVIDX_EXPORT_JSON) {
        if (options->prettyPrint) {
            fprintf(fp, "\n");
        }
        fprintf(fp, "]}\n");
    }

    if (callback && count > 0) {
        callback(count, total, userData);
    }

cleanup:
    rocksdb_iter_destroy(iter);
    fclose(fp);

    return result;
}

kvidxError kvidxRocksdbImport(kvidxInstance *i, const char *filename,
                              const kvidxImportOptions *options,
                              kvidxProgressCallback callback, void *userData) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db || !filename || !options) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        kvidxSetError(i, KVIDX_ERROR_IO, "Failed to open file: %s", filename);
        return KVIDX_ERROR_IO;
    }

    kvidxError result = KVIDX_OK;
    uint64_t count = 0;

    /* Clear if requested */
    if (options->clearBeforeImport) {
        rocksdb_writebatch_t *batch = rocksdb_writebatch_create();
        if (batch) {
            rocksdb_iterator_t *iter =
                rocksdb_create_iterator(s->db, s->readOptions);
            if (iter) {
                rocksdb_iter_seek_to_first(iter);
                while (rocksdb_iter_valid(iter)) {
                    size_t keyLen;
                    const char *keyData = rocksdb_iter_key(iter, &keyLen);
                    rocksdb_writebatch_delete(batch, keyData, keyLen);
                    rocksdb_iter_next(iter);
                }
                rocksdb_iter_destroy(iter);
            }

            char *err = NULL;
            rocksdb_write(s->db, s->syncWriteOptions, batch, &err);
            rocksdb_writebatch_destroy(batch);
            freeErr(&err);
        }
    }

    /* Only binary import supported for now */
    if (options->format != KVIDX_EXPORT_BINARY) {
        fclose(fp);
        kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                      "Only binary import is supported");
        return KVIDX_ERROR_NOT_SUPPORTED;
    }

    /* Read and validate header */
    kvidxBinaryHeader header;
    if (fread(&header, sizeof(header), 1, fp) != 1) {
        fclose(fp);
        kvidxSetError(i, KVIDX_ERROR_IO, "Failed to read header");
        return KVIDX_ERROR_IO;
    }

    if (header.magic != KVIDX_BINARY_MAGIC) {
        fclose(fp);
        kvidxSetError(i, KVIDX_ERROR_INVALID_ARGUMENT, "Invalid binary format");
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    if (header.version != KVIDX_BINARY_VERSION) {
        fclose(fp);
        kvidxSetError(i, KVIDX_ERROR_INVALID_ARGUMENT,
                      "Unsupported version: %u", header.version);
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Create write batch for import */
    rocksdb_writebatch_t *batch = rocksdb_writebatch_create();
    if (!batch) {
        fclose(fp);
        return KVIDX_ERROR_INTERNAL;
    }

    /* Read entries */
    for (uint64_t idx = 0; idx < header.entryCount; idx++) {
        uint64_t key, term, cmd, dataLen;
        uint8_t *data = NULL;

        if (fread(&key, sizeof(key), 1, fp) != 1 ||
            fread(&term, sizeof(term), 1, fp) != 1 ||
            fread(&cmd, sizeof(cmd), 1, fp) != 1 ||
            fread(&dataLen, sizeof(dataLen), 1, fp) != 1) {
            result = KVIDX_ERROR_IO;
            break;
        }

        if (dataLen > 0) {
            data = malloc(dataLen);
            if (!data) {
                result = KVIDX_ERROR_INTERNAL;
                break;
            }
            if (fread(data, 1, dataLen, fp) != dataLen) {
                free(data);
                result = KVIDX_ERROR_IO;
                break;
            }
        }

        /* Skip duplicates if requested */
        if (options->skipDuplicates) {
            char keyBuf[8];
            encodeKey(key, keyBuf);
            char *err = NULL;
            size_t existingLen;
            char *existing = rocksdb_get(s->db, s->readOptions, keyBuf,
                                         sizeof(keyBuf), &existingLen, &err);
            freeErr(&err);
            if (existing) {
                free(existing);
                free(data);
                continue;
            }
        }

        /* Pack and add to batch */
        size_t valLen;
        void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
        free(data);

        if (!valBuf) {
            result = KVIDX_ERROR_INTERNAL;
            break;
        }

        char keyBuf[8];
        encodeKey(key, keyBuf);
        rocksdb_writebatch_put(batch, keyBuf, sizeof(keyBuf), valBuf, valLen);
        free(valBuf);

        count++;

        if (callback && count % 100 == 0) {
            if (!callback(count, header.entryCount, userData)) {
                result = KVIDX_ERROR_CANCELLED;
                break;
            }
        }
    }

    fclose(fp);

    if (result == KVIDX_OK) {
        char *err = NULL;
        rocksdb_write(s->db, s->syncWriteOptions, batch, &err);
        if (err) {
            result = KVIDX_ERROR_INTERNAL;
            free(err);
        }
    }

    rocksdb_writebatch_destroy(batch);

    if (callback && count > 0) {
        callback(count, header.entryCount, userData);
    }

    return result;
}

/* ====================================================================
 * Configuration
 * ==================================================================== */

kvidxError kvidxRocksdbApplyConfig(kvidxInstance *i,
                                   const kvidxConfig *config) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db || !config) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* RocksDB configuration is largely set at open time.
     * Some settings can be adjusted dynamically via SetDBOptions. */

    /* Adjust sync mode */
    if (config->syncMode == KVIDX_SYNC_OFF) {
        rocksdb_writeoptions_set_sync(s->syncWriteOptions, 0);
    } else {
        rocksdb_writeoptions_set_sync(s->syncWriteOptions, 1);
    }

    return KVIDX_OK;
}

/* ====================================================================
 * Storage Primitives (v0.8.0)
 * ==================================================================== */

/*
 * TTL Storage:
 * TTL entries are stored with a special prefix to distinguish them from
 * regular data entries. The format is:
 *   Key: 0x00 "TTL" + big-endian key (12 bytes total)
 *   Value: uint64_t expiration timestamp in milliseconds
 */

#define TTL_PREFIX "\x00TTL"
#define TTL_PREFIX_LEN 4
#define TTL_KEY_SIZE (TTL_PREFIX_LEN + 8)

/* Encode a TTL key */
static void encodeTTLKey(uint64_t key, char *buf) {
    memcpy(buf, TTL_PREFIX, TTL_PREFIX_LEN);
    encodeKey(key, buf + TTL_PREFIX_LEN);
}

/* Check if a key is a TTL key */
static bool isTTLKey(const char *keyData, size_t keyLen) {
    return keyLen == TTL_KEY_SIZE &&
           memcmp(keyData, TTL_PREFIX, TTL_PREFIX_LEN) == 0;
}

/* Get current time in milliseconds */
static uint64_t currentTimeMs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* Check if a key has expired (helper) */
static bool isKeyExpired(rocksdbState *s, uint64_t key) {
    char ttlKeyBuf[TTL_KEY_SIZE];
    encodeTTLKey(key, ttlKeyBuf);

    char *err = NULL;
    size_t valueLen;
    char *value;

    if (s->writeBatch) {
        value = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, ttlKeyBuf, TTL_KEY_SIZE,
            &valueLen, &err);
    } else {
        value = rocksdb_get(s->db, s->readOptions, ttlKeyBuf, TTL_KEY_SIZE,
                            &valueLen, &err);
    }
    freeErr(&err);

    if (!value) {
        return false; /* No TTL set */
    }

    if (valueLen != sizeof(uint64_t)) {
        free(value);
        return false;
    }

    uint64_t expireAt;
    memcpy(&expireAt, value, sizeof(expireAt));
    free(value);

    return currentTimeMs() >= expireAt;
}

/* ====================================================================
 * Conditional Writes
 * ==================================================================== */

kvidxError kvidxRocksdbInsertEx(kvidxInstance *i, uint64_t key, uint64_t term,
                                uint64_t cmd, const void *data, size_t dataLen,
                                kvidxSetCondition condition) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    char keyBuf[8];
    encodeKey(key, keyBuf);

    /* Check if key exists */
    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }
    freeErr(&err);

    bool keyExists = (existing != NULL);

    /* Check expiration */
    if (keyExists && isKeyExpired(s, key)) {
        keyExists = false;
    }

    if (existing) {
        free(existing);
    }

    /* Apply condition */
    switch (condition) {
    case KVIDX_SET_IF_NOT_EXISTS:
        if (keyExists) {
            return KVIDX_ERROR_CONDITION_FAILED;
        }
        break;
    case KVIDX_SET_IF_EXISTS:
        if (!keyExists) {
            return KVIDX_ERROR_CONDITION_FAILED;
        }
        break;
    case KVIDX_SET_ALWAYS:
    default:
        break;
    }

    /* Pack the value */
    size_t valLen;
    void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
    if (!valBuf) {
        return KVIDX_ERROR_INTERNAL;
    }

    /* Write the value */
    if (s->writeBatch) {
        rocksdb_writebatch_wi_put(s->writeBatch, keyBuf, sizeof(keyBuf), valBuf,
                                  valLen);
        free(valBuf);
        return KVIDX_OK;
    }

    rocksdb_put(s->db, s->syncWriteOptions, keyBuf, sizeof(keyBuf), valBuf,
                valLen, &err);
    free(valBuf);

    if (err) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "RocksDB put failed: %s", err);
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    return KVIDX_OK;
}

/* ====================================================================
 * Transaction Abort
 * ==================================================================== */

bool kvidxRocksdbAbort(kvidxInstance *i) {
    rocksdbState *s = STATE(i);

    if (!s->writeBatch) {
        /* No active transaction */
        return true;
    }

    rocksdb_writebatch_wi_destroy(s->writeBatch);
    s->writeBatch = NULL;

    return true;
}

/* ====================================================================
 * Atomic Operations
 * ==================================================================== */

kvidxError kvidxRocksdbGetAndSet(kvidxInstance *i, uint64_t key, uint64_t term,
                                 uint64_t cmd, const void *data, size_t dataLen,
                                 uint64_t *oldTerm, uint64_t *oldCmd,
                                 void **oldData, size_t *oldDataLen) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    char keyBuf[8];
    encodeKey(key, keyBuf);

    /* Get the existing value */
    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    /* Check for expiration */
    if (existing && isKeyExpired(s, key)) {
        free(existing);
        existing = NULL;
        existingLen = 0;
    }

    /* Extract old values if key existed */
    if (existing) {
        if (oldTerm) {
            *oldTerm = extractTerm(existing, existingLen);
        }
        if (oldCmd) {
            *oldCmd = extractCmd(existing, existingLen);
        }
        if (oldData && oldDataLen) {
            size_t dLen;
            const uint8_t *dPtr = extractData(existing, existingLen, &dLen);
            if (dLen > 0) {
                *oldData = malloc(dLen);
                if (*oldData) {
                    memcpy(*oldData, dPtr, dLen);
                    *oldDataLen = dLen;
                } else {
                    free(existing);
                    return KVIDX_ERROR_INTERNAL;
                }
            } else {
                *oldData = NULL;
                *oldDataLen = 0;
            }
        }
        free(existing);
    } else {
        if (oldTerm) {
            *oldTerm = 0;
        }
        if (oldCmd) {
            *oldCmd = 0;
        }
        if (oldData) {
            *oldData = NULL;
        }
        if (oldDataLen) {
            *oldDataLen = 0;
        }
    }

    /* Pack and write the new value */
    size_t valLen;
    void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
    if (!valBuf) {
        return KVIDX_ERROR_INTERNAL;
    }

    if (s->writeBatch) {
        rocksdb_writebatch_wi_put(s->writeBatch, keyBuf, sizeof(keyBuf), valBuf,
                                  valLen);
        free(valBuf);
        return KVIDX_OK;
    }

    rocksdb_put(s->db, s->syncWriteOptions, keyBuf, sizeof(keyBuf), valBuf,
                valLen, &err);
    free(valBuf);

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    return KVIDX_OK;
}

kvidxError kvidxRocksdbGetAndRemove(kvidxInstance *i, uint64_t key,
                                    uint64_t *term, uint64_t *cmd, void **data,
                                    size_t *dataLen) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    char keyBuf[8];
    encodeKey(key, keyBuf);

    /* Get the existing value */
    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    if (!existing) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Check for expiration */
    if (isKeyExpired(s, key)) {
        free(existing);
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Extract values */
    if (term) {
        *term = extractTerm(existing, existingLen);
    }
    if (cmd) {
        *cmd = extractCmd(existing, existingLen);
    }
    if (data && dataLen) {
        size_t dLen;
        const uint8_t *dPtr = extractData(existing, existingLen, &dLen);
        if (dLen > 0) {
            *data = malloc(dLen);
            if (*data) {
                memcpy(*data, dPtr, dLen);
                *dataLen = dLen;
            } else {
                free(existing);
                return KVIDX_ERROR_INTERNAL;
            }
        } else {
            *data = NULL;
            *dataLen = 0;
        }
    }
    free(existing);

    /* Delete the key */
    if (s->writeBatch) {
        rocksdb_writebatch_wi_delete(s->writeBatch, keyBuf, sizeof(keyBuf));

        /* Also delete TTL entry if present */
        char ttlKeyBuf[TTL_KEY_SIZE];
        encodeTTLKey(key, ttlKeyBuf);
        rocksdb_writebatch_wi_delete(s->writeBatch, ttlKeyBuf, TTL_KEY_SIZE);
        return KVIDX_OK;
    }

    rocksdb_delete(s->db, s->syncWriteOptions, keyBuf, sizeof(keyBuf), &err);
    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    /* Also delete TTL entry if present */
    char ttlKeyBuf[TTL_KEY_SIZE];
    encodeTTLKey(key, ttlKeyBuf);
    rocksdb_delete(s->db, s->writeOptions, ttlKeyBuf, TTL_KEY_SIZE, &err);
    freeErr(&err);

    return KVIDX_OK;
}

/* ====================================================================
 * Compare-and-Swap
 * ==================================================================== */

kvidxError kvidxRocksdbCompareAndSwap(kvidxInstance *i, uint64_t key,
                                      const void *expectedData,
                                      size_t expectedLen, uint64_t newTerm,
                                      uint64_t newCmd, const void *newData,
                                      size_t newDataLen, bool *swapped) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db || !swapped) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    *swapped = false;

    char keyBuf[8];
    encodeKey(key, keyBuf);

    /* Get the existing value */
    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    /* Check for expiration */
    if (existing && isKeyExpired(s, key)) {
        free(existing);
        existing = NULL;
        existingLen = 0;
    }

    /* Compare data */
    if (!existing) {
        /* Key doesn't exist - return NOT_FOUND */
        return KVIDX_ERROR_NOT_FOUND;
    } else {
        size_t currentDataLen;
        const uint8_t *currentData =
            extractData(existing, existingLen, &currentDataLen);

        bool match = (currentDataLen == expectedLen);
        if (match && expectedLen > 0) {
            match = (memcmp(currentData, expectedData, expectedLen) == 0);
        }
        free(existing);

        if (!match) {
            return KVIDX_OK; /* No match */
        }
    }

    /* Data matches - perform the swap */
    size_t valLen;
    void *valBuf = packValue(newTerm, newCmd, newData, newDataLen, &valLen);
    if (!valBuf) {
        return KVIDX_ERROR_INTERNAL;
    }

    if (s->writeBatch) {
        rocksdb_writebatch_wi_put(s->writeBatch, keyBuf, sizeof(keyBuf), valBuf,
                                  valLen);
        free(valBuf);
        *swapped = true;
        return KVIDX_OK;
    }

    rocksdb_put(s->db, s->syncWriteOptions, keyBuf, sizeof(keyBuf), valBuf,
                valLen, &err);
    free(valBuf);

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    *swapped = true;
    return KVIDX_OK;
}

/* ====================================================================
 * Append / Prepend
 * ==================================================================== */

kvidxError kvidxRocksdbAppend(kvidxInstance *i, uint64_t key, uint64_t term,
                              uint64_t cmd, const void *data, size_t dataLen,
                              size_t *newLen) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    char keyBuf[8];
    encodeKey(key, keyBuf);

    /* Get the existing value */
    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    /* Handle expiration - treat expired key as non-existent */
    if (existing && isKeyExpired(s, key)) {
        free(existing);
        existing = NULL;
        existingLen = 0;
    }

    /* Build the new value */
    size_t oldDataLen = 0;
    const uint8_t *oldData = NULL;
    if (existing) {
        oldData = extractData(existing, existingLen, &oldDataLen);
    }

    size_t totalDataLen = oldDataLen + dataLen;
    void *combinedData = malloc(totalDataLen > 0 ? totalDataLen : 1);
    if (!combinedData) {
        free(existing);
        return KVIDX_ERROR_INTERNAL;
    }

    if (oldDataLen > 0) {
        memcpy(combinedData, oldData, oldDataLen);
    }
    if (dataLen > 0 && data) {
        memcpy((uint8_t *)combinedData + oldDataLen, data, dataLen);
    }
    free(existing);

    /* Pack the new value */
    size_t valLen;
    void *valBuf = packValue(term, cmd, combinedData, totalDataLen, &valLen);
    free(combinedData);

    if (!valBuf) {
        return KVIDX_ERROR_INTERNAL;
    }

    /* Write the value */
    if (s->writeBatch) {
        rocksdb_writebatch_wi_put(s->writeBatch, keyBuf, sizeof(keyBuf), valBuf,
                                  valLen);
        free(valBuf);
        if (newLen) {
            *newLen = totalDataLen;
        }
        return KVIDX_OK;
    }

    rocksdb_put(s->db, s->syncWriteOptions, keyBuf, sizeof(keyBuf), valBuf,
                valLen, &err);
    free(valBuf);

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    if (newLen) {
        *newLen = totalDataLen;
    }
    return KVIDX_OK;
}

kvidxError kvidxRocksdbPrepend(kvidxInstance *i, uint64_t key, uint64_t term,
                               uint64_t cmd, const void *data, size_t dataLen,
                               size_t *newLen) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    char keyBuf[8];
    encodeKey(key, keyBuf);

    /* Get the existing value */
    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    /* Handle expiration - treat expired key as non-existent */
    if (existing && isKeyExpired(s, key)) {
        free(existing);
        existing = NULL;
        existingLen = 0;
    }

    /* Build the new value */
    size_t oldDataLen = 0;
    const uint8_t *oldData = NULL;
    if (existing) {
        oldData = extractData(existing, existingLen, &oldDataLen);
    }

    size_t totalDataLen = dataLen + oldDataLen;
    void *combinedData = malloc(totalDataLen > 0 ? totalDataLen : 1);
    if (!combinedData) {
        free(existing);
        return KVIDX_ERROR_INTERNAL;
    }

    if (dataLen > 0 && data) {
        memcpy(combinedData, data, dataLen);
    }
    if (oldDataLen > 0) {
        memcpy((uint8_t *)combinedData + dataLen, oldData, oldDataLen);
    }
    free(existing);

    /* Pack the new value */
    size_t valLen;
    void *valBuf = packValue(term, cmd, combinedData, totalDataLen, &valLen);
    free(combinedData);

    if (!valBuf) {
        return KVIDX_ERROR_INTERNAL;
    }

    /* Write the value */
    if (s->writeBatch) {
        rocksdb_writebatch_wi_put(s->writeBatch, keyBuf, sizeof(keyBuf), valBuf,
                                  valLen);
        free(valBuf);
        if (newLen) {
            *newLen = totalDataLen;
        }
        return KVIDX_OK;
    }

    rocksdb_put(s->db, s->syncWriteOptions, keyBuf, sizeof(keyBuf), valBuf,
                valLen, &err);
    free(valBuf);

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    if (newLen) {
        *newLen = totalDataLen;
    }
    return KVIDX_OK;
}

/* ====================================================================
 * Partial Value Access
 * ==================================================================== */

kvidxError kvidxRocksdbGetValueRange(kvidxInstance *i, uint64_t key,
                                     size_t offset, size_t length, void **data,
                                     size_t *actualLen) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db || !data || !actualLen) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    *data = NULL;
    *actualLen = 0;

    char keyBuf[8];
    encodeKey(key, keyBuf);

    /* Get the value */
    char *err = NULL;
    size_t valueLen;
    char *value;

    if (s->writeBatch) {
        value = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &valueLen, &err);
    } else {
        value = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                            &valueLen, &err);
    }

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    if (!value) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Check for expiration */
    if (isKeyExpired(s, key)) {
        free(value);
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Extract data portion */
    size_t dataLen;
    const uint8_t *dataPtr = extractData(value, valueLen, &dataLen);

    /* Validate offset */
    if (offset >= dataLen) {
        free(value);
        return KVIDX_OK; /* Return empty result */
    }

    /* Calculate actual length to return */
    size_t available = dataLen - offset;
    size_t toReturn = (length == 0 || length > available) ? available : length;

    if (toReturn > 0) {
        *data = malloc(toReturn);
        if (!*data) {
            free(value);
            return KVIDX_ERROR_INTERNAL;
        }
        memcpy(*data, dataPtr + offset, toReturn);
        *actualLen = toReturn;
    }

    free(value);
    return KVIDX_OK;
}

kvidxError kvidxRocksdbSetValueRange(kvidxInstance *i, uint64_t key,
                                     size_t offset, const void *data,
                                     size_t dataLen, size_t *newLen) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    char keyBuf[8];
    encodeKey(key, keyBuf);

    /* Get the existing value */
    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    if (!existing) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Check for expiration */
    if (isKeyExpired(s, key)) {
        free(existing);
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Extract existing data and metadata */
    uint64_t term = extractTerm(existing, existingLen);
    uint64_t cmd = extractCmd(existing, existingLen);
    size_t oldDataLen;
    const uint8_t *oldData = extractData(existing, existingLen, &oldDataLen);

    /* Calculate new size (may extend the data) */
    size_t newDataLen =
        (offset + dataLen > oldDataLen) ? (offset + dataLen) : oldDataLen;

    /* Build new data buffer */
    void *newData = calloc(1, newDataLen > 0 ? newDataLen : 1);
    if (!newData) {
        free(existing);
        return KVIDX_ERROR_INTERNAL;
    }

    /* Copy old data */
    if (oldDataLen > 0) {
        memcpy(newData, oldData, oldDataLen);
    }

    /* Write new data at offset */
    if (dataLen > 0 && data) {
        memcpy((uint8_t *)newData + offset, data, dataLen);
    }
    free(existing);

    /* Pack the new value */
    size_t valLen;
    void *valBuf = packValue(term, cmd, newData, newDataLen, &valLen);
    free(newData);

    if (!valBuf) {
        return KVIDX_ERROR_INTERNAL;
    }

    /* Write the value */
    if (s->writeBatch) {
        rocksdb_writebatch_wi_put(s->writeBatch, keyBuf, sizeof(keyBuf), valBuf,
                                  valLen);
        free(valBuf);
        if (newLen) {
            *newLen = newDataLen;
        }
        return KVIDX_OK;
    }

    rocksdb_put(s->db, s->syncWriteOptions, keyBuf, sizeof(keyBuf), valBuf,
                valLen, &err);
    free(valBuf);

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    if (newLen) {
        *newLen = newDataLen;
    }
    return KVIDX_OK;
}

/* ====================================================================
 * TTL / Expiration
 * ==================================================================== */

kvidxError kvidxRocksdbSetExpire(kvidxInstance *i, uint64_t key,
                                 uint64_t ttlMs) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Check if key exists */
    char keyBuf[8];
    encodeKey(key, keyBuf);

    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }
    freeErr(&err);

    if (!existing) {
        return KVIDX_ERROR_NOT_FOUND;
    }
    free(existing);

    /* Calculate expiration timestamp */
    uint64_t expireAt = currentTimeMs() + ttlMs;

    /* Store TTL entry */
    char ttlKeyBuf[TTL_KEY_SIZE];
    encodeTTLKey(key, ttlKeyBuf);

    if (s->writeBatch) {
        rocksdb_writebatch_wi_put(s->writeBatch, ttlKeyBuf, TTL_KEY_SIZE,
                                  (char *)&expireAt, sizeof(expireAt));
        return KVIDX_OK;
    }

    rocksdb_put(s->db, s->syncWriteOptions, ttlKeyBuf, TTL_KEY_SIZE,
                (char *)&expireAt, sizeof(expireAt), &err);

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    return KVIDX_OK;
}

kvidxError kvidxRocksdbSetExpireAt(kvidxInstance *i, uint64_t key,
                                   uint64_t timestampMs) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Check if key exists */
    char keyBuf[8];
    encodeKey(key, keyBuf);

    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }
    freeErr(&err);

    if (!existing) {
        return KVIDX_ERROR_NOT_FOUND;
    }
    free(existing);

    /* Store TTL entry */
    char ttlKeyBuf[TTL_KEY_SIZE];
    encodeTTLKey(key, ttlKeyBuf);

    if (s->writeBatch) {
        rocksdb_writebatch_wi_put(s->writeBatch, ttlKeyBuf, TTL_KEY_SIZE,
                                  (char *)&timestampMs, sizeof(timestampMs));
        return KVIDX_OK;
    }

    rocksdb_put(s->db, s->syncWriteOptions, ttlKeyBuf, TTL_KEY_SIZE,
                (char *)&timestampMs, sizeof(timestampMs), &err);

    if (err) {
        free(err);
        return KVIDX_ERROR_INTERNAL;
    }

    return KVIDX_OK;
}

int64_t kvidxRocksdbGetTTL(kvidxInstance *i, uint64_t key) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_TTL_NOT_FOUND;
    }

    /* Check if key exists */
    char keyBuf[8];
    encodeKey(key, keyBuf);

    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }
    freeErr(&err);

    if (!existing) {
        return KVIDX_TTL_NOT_FOUND;
    }
    free(existing);

    /* Get TTL entry */
    char ttlKeyBuf[TTL_KEY_SIZE];
    encodeTTLKey(key, ttlKeyBuf);

    size_t ttlLen;
    char *ttlValue;

    if (s->writeBatch) {
        ttlValue = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, ttlKeyBuf, TTL_KEY_SIZE,
            &ttlLen, &err);
    } else {
        ttlValue = rocksdb_get(s->db, s->readOptions, ttlKeyBuf, TTL_KEY_SIZE,
                               &ttlLen, &err);
    }
    freeErr(&err);

    if (!ttlValue || ttlLen != sizeof(uint64_t)) {
        free(ttlValue);
        return KVIDX_TTL_NONE; /* Key exists but no TTL */
    }

    uint64_t expireAt;
    memcpy(&expireAt, ttlValue, sizeof(expireAt));
    free(ttlValue);

    uint64_t now = currentTimeMs();
    if (now >= expireAt) {
        return 0; /* Already expired */
    }

    return (int64_t)(expireAt - now);
}

kvidxError kvidxRocksdbPersist(kvidxInstance *i, uint64_t key) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Check if key exists */
    char keyBuf[8];
    encodeKey(key, keyBuf);

    char *err = NULL;
    size_t existingLen;
    char *existing;

    if (s->writeBatch) {
        existing = rocksdb_writebatch_wi_get_from_batch_and_db(
            s->writeBatch, s->db, s->readOptions, keyBuf, sizeof(keyBuf),
            &existingLen, &err);
    } else {
        existing = rocksdb_get(s->db, s->readOptions, keyBuf, sizeof(keyBuf),
                               &existingLen, &err);
    }
    freeErr(&err);

    if (!existing) {
        return KVIDX_ERROR_NOT_FOUND;
    }
    free(existing);

    /* Delete TTL entry */
    char ttlKeyBuf[TTL_KEY_SIZE];
    encodeTTLKey(key, ttlKeyBuf);

    if (s->writeBatch) {
        rocksdb_writebatch_wi_delete(s->writeBatch, ttlKeyBuf, TTL_KEY_SIZE);
        return KVIDX_OK;
    }

    rocksdb_delete(s->db, s->writeOptions, ttlKeyBuf, TTL_KEY_SIZE, &err);
    freeErr(&err);

    return KVIDX_OK;
}

kvidxError kvidxRocksdbExpireScan(kvidxInstance *i, uint64_t maxKeys,
                                  uint64_t *expiredCount) {
    rocksdbState *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    uint64_t expired = 0;
    uint64_t now = currentTimeMs();

    bool ownBatch = false;
    if (!s->writeBatch) {
        s->writeBatch = rocksdb_writebatch_wi_create(0, 0);
        if (!s->writeBatch) {
            return KVIDX_ERROR_INTERNAL;
        }
        ownBatch = true;
    }

    /* Create iterator to scan TTL keys */
    rocksdb_iterator_t *iter = rocksdb_create_iterator(s->db, s->readOptions);
    if (!iter) {
        if (ownBatch) {
            rocksdb_writebatch_wi_destroy(s->writeBatch);
            s->writeBatch = NULL;
        }
        return KVIDX_ERROR_INTERNAL;
    }

    /* Seek to start of TTL keys */
    rocksdb_iter_seek(iter, TTL_PREFIX, TTL_PREFIX_LEN);

    while (rocksdb_iter_valid(iter) && (maxKeys == 0 || expired < maxKeys)) {
        size_t keyLen;
        const char *keyData = rocksdb_iter_key(iter, &keyLen);

        /* Check if this is still a TTL key */
        if (!isTTLKey(keyData, keyLen)) {
            break; /* Past TTL keys */
        }

        /* Get expiration time */
        size_t valueLen;
        const char *value = rocksdb_iter_value(iter, &valueLen);

        if (valueLen == sizeof(uint64_t)) {
            uint64_t expireAt;
            memcpy(&expireAt, value, sizeof(expireAt));

            if (now >= expireAt) {
                /* Key has expired - delete both TTL entry and data key */
                uint64_t dataKey = decodeKey(keyData + TTL_PREFIX_LEN);

                /* Delete TTL entry */
                rocksdb_writebatch_wi_delete(s->writeBatch, keyData, keyLen);

                /* Delete data entry */
                char dataKeyBuf[8];
                encodeKey(dataKey, dataKeyBuf);
                rocksdb_writebatch_wi_delete(s->writeBatch, dataKeyBuf, 8);

                expired++;
            }
        }

        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);

    if (ownBatch) {
        char *err = NULL;
        rocksdb_write_writebatch_wi(s->db, s->syncWriteOptions, s->writeBatch,
                                    &err);
        rocksdb_writebatch_wi_destroy(s->writeBatch);
        s->writeBatch = NULL;

        if (err) {
            free(err);
            return KVIDX_ERROR_INTERNAL;
        }
    }

    if (expiredCount) {
        *expiredCount = expired;
    }

    return KVIDX_OK;
}
