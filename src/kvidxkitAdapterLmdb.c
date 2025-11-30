/**
 * @file kvidxkitAdapterLmdb.c
 * @brief LMDB backend adapter for kvidxkit
 *
 * This adapter implements the kvidxInterface using LMDB (Lightning
 * Memory-Mapped Database) as the storage backend. LMDB provides extremely fast
 * read performance through memory-mapped I/O with zero-copy data access.
 *
 * ## Architecture
 *
 * LMDB differs fundamentally from SQLite in several ways:
 *
 * 1. **Memory-Mapped Storage**: The entire database is memory-mapped, allowing
 *    read operations to return pointers directly into the mapped memory without
 *    copying data. This makes LMDB extremely fast for read-heavy workloads.
 *
 * 2. **Single Writer, Multiple Readers**: LMDB supports one active write
 *    transaction and unlimited concurrent read transactions. Write transactions
 *    block other writers but never block readers.
 *
 * 3. **Environment-Based**: LMDB uses an "environment" (a directory) containing
 *    data.mdb and lock.mdb files, rather than a single file like SQLite.
 *
 * 4. **Named Databases**: Multiple B-trees can exist within one environment.
 *    We use "_kvidx_data" for the main data and "_kvidx_ttl" for TTL metadata.
 *
 * ## Value Format
 *
 * Values are stored in a packed binary format:
 *   - Bytes 0-7:   term (uint64_t, native endian)
 *   - Bytes 8-15:  cmd (uint64_t, native endian)
 *   - Bytes 16+:   data blob
 *
 * This allows extracting metadata without parsing, while keeping all record
 * information in a single LMDB value.
 *
 * ## Transaction Model
 *
 * The adapter manages two transaction types:
 *
 * - **Read Transaction** (readTxn): A persistent read transaction that gets
 *   renewed for each read operation. This provides consistent snapshots while
 *   allowing the returned data pointers to remain valid.
 *
 * - **Write Transaction** (writeTxn): Created on-demand for write operations.
 *   If the user explicitly calls Begin(), the transaction stays open until
 *   Commit() or Abort(). Otherwise, each write auto-commits.
 *
 * ## Key Format
 *
 * Keys are uint64_t values stored with MDB_INTEGERKEY flag, which tells LMDB
 * to use native integer comparison (faster than byte-by-byte comparison).
 *
 * ## Thread Safety
 *
 * LMDB has complex threading requirements:
 * - We use MDB_NOTLS to allow transaction handles to be used across threads
 * - However, a single transaction should not be used concurrently
 * - The environment can be shared across threads
 *
 * ## Memory Management
 *
 * Data pointers returned from Get operations point directly into LMDB's
 * memory-mapped region. These pointers are valid only until:
 * - The read transaction is reset/renewed
 * - Another Get/GetPrev/GetNext call is made
 * - The environment is closed
 *
 * For long-term retention, callers must copy the data.
 *
 * ## Performance Characteristics
 *
 * - Reads: O(log n), zero-copy from mmap
 * - Writes: O(log n), copy-on-write B-tree
 * - Range scans: Very efficient via cursor iteration
 * - Space: Copy-on-write means deleted space isn't immediately reclaimed
 */

#include "kvidxkitAdapterLmdb.h"
#include "../deps/lmdb/libraries/liblmdb/lmdb.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/** Size of term + cmd header prefixed to all values */
#define VALUE_HEADER_SIZE (sizeof(uint64_t) * 2)

/** Default memory map size: 1GB. LMDB pre-allocates address space but not disk.
 */
#define DEFAULT_MAP_SIZE (1UL << 30)

/**
 * Internal state for an LMDB-backed kvidx instance.
 *
 * This structure maintains all LMDB handles and transaction state for a
 * single database instance. Each kvidxInstance has exactly one lmdbState.
 */
typedef struct lmdbState {
    MDB_env *env;   /**< LMDB environment handle (the database) */
    MDB_dbi dbi;    /**< Database handle for main data ("_kvidx_data") */
    MDB_dbi ttlDbi; /**< Database handle for TTL metadata ("_kvidx_ttl") */
    bool ttlDbiInitialized; /**< Whether TTL database has been opened */
    MDB_txn *readTxn;  /**< Persistent read transaction for zero-copy reads */
    MDB_txn *writeTxn; /**< Active write transaction (NULL when not in txn) */
    char *envPath;     /**< Path to environment directory */
} lmdbState;

#define STATE(instance) ((lmdbState *)(instance)->kvidxdata)

/* ====================================================================
 * Value Packing/Unpacking Helpers
 * ====================================================================
 * LMDB stores each record as a single key-value pair. We pack term, cmd,
 * and data into the value with a fixed header for efficient extraction.
 */

/**
 * Extract the term field from a packed LMDB value.
 *
 * @param val  LMDB value containing packed data
 * @return The term value, or 0 if value is malformed
 */
static inline uint64_t extractTerm(const MDB_val *val) {
    if (val->mv_size < VALUE_HEADER_SIZE) {
        return 0;
    }
    uint64_t term;
    memcpy(&term, val->mv_data, sizeof(term));
    return term;
}

/**
 * Extract the cmd field from a packed LMDB value.
 *
 * @param val  LMDB value containing packed data
 * @return The cmd value, or 0 if value is malformed
 */
static inline uint64_t extractCmd(const MDB_val *val) {
    if (val->mv_size < VALUE_HEADER_SIZE) {
        return 0;
    }
    uint64_t cmd;
    memcpy(&cmd, (uint8_t *)val->mv_data + sizeof(uint64_t), sizeof(cmd));
    return cmd;
}

/**
 * Extract the data portion from a packed LMDB value.
 *
 * Returns a pointer directly into the LMDB memory-mapped region (zero-copy).
 * The pointer remains valid until the transaction is reset or another
 * operation is performed.
 *
 * @param val  LMDB value containing packed data
 * @param len  OUT: Length of the data portion, or NULL if not needed
 * @return Pointer to data (may be NULL if empty), or NULL if value is empty
 */
static inline const uint8_t *extractData(const MDB_val *val, size_t *len) {
    if (val->mv_size <= VALUE_HEADER_SIZE) {
        if (len) {
            *len = 0;
        }
        return NULL;
    }
    if (len) {
        *len = val->mv_size - VALUE_HEADER_SIZE;
    }
    return (const uint8_t *)val->mv_data + VALUE_HEADER_SIZE;
}

/**
 * Pack term, cmd, and data into a single buffer for LMDB storage.
 *
 * Allocates a new buffer containing the packed representation. The caller
 * is responsible for freeing this buffer after the LMDB put operation.
 *
 * @param term      The term value to pack
 * @param cmd       The cmd value to pack
 * @param data      The data to pack (may be NULL if dataLen is 0)
 * @param dataLen   Length of the data
 * @param totalLen  OUT: Total length of the packed buffer
 * @return Allocated buffer containing packed data, or NULL on allocation
 * failure
 */
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

/* ====================================================================
 * Transaction Management Helpers
 * ====================================================================
 * LMDB requires explicit transaction management. Read operations need a
 * read transaction, write operations need a write transaction. We maintain
 * persistent transactions for efficiency and zero-copy data access.
 */

/**
 * Ensure a valid read transaction exists.
 *
 * LMDB requires a transaction for all operations, including reads. This
 * function ensures we have a valid read transaction, creating or renewing
 * one as needed.
 *
 * If a write transaction is active, it's used for reads too (LMDB supports
 * this). Otherwise, we maintain a persistent read transaction that gets
 * renewed for each operation to provide a fresh snapshot.
 *
 * The read transaction is reset after use to release the read lock and
 * allow writers to reclaim space.
 *
 * @param i  The kvidx instance
 * @return true if read transaction is ready, false on error
 */
static bool ensureReadTxn(kvidxInstance *i) {
    lmdbState *s = STATE(i);

    /* If write txn is active, use that for reads */
    if (s->writeTxn) {
        return true;
    }

    /* If read txn exists, renew it for fresh snapshot */
    if (s->readTxn) {
        int rc = mdb_txn_renew(s->readTxn);
        if (rc != MDB_SUCCESS) {
            /* Transaction invalid, need to recreate */
            mdb_txn_abort(s->readTxn);
            s->readTxn = NULL;
        } else {
            return true;
        }
    }

    /* Create new read transaction */
    int rc = mdb_txn_begin(s->env, NULL, MDB_RDONLY, &s->readTxn);
    return rc == MDB_SUCCESS;
}

/**
 * Get the currently active transaction for database operations.
 *
 * Returns the write transaction if one is active (inside Begin/Commit),
 * otherwise returns the read transaction. This allows operations to work
 * correctly whether called inside an explicit transaction or standalone.
 *
 * @param i  The kvidx instance
 * @return The active transaction handle
 */
static MDB_txn *getActiveTxn(kvidxInstance *i) {
    lmdbState *s = STATE(i);
    return s->writeTxn ? s->writeTxn : s->readTxn;
}

/**
 * Reset the read transaction after use to release the read lock.
 *
 * LMDB read transactions hold a "read lock" that prevents old pages from
 * being reclaimed. If a long-running read transaction is not reset, the
 * database file can grow unbounded as writers accumulate new pages.
 *
 * This function resets (pauses) the read transaction, releasing the lock
 * while keeping the transaction handle for later reuse via mdb_txn_renew().
 *
 * IMPORTANT: Do NOT call this if data pointers from Get() are still needed,
 * as they become invalid when the transaction is reset.
 *
 * @param i  The kvidx instance
 */
static void resetReadTxn(kvidxInstance *i) {
    lmdbState *s = STATE(i);
    if (s->readTxn && !s->writeTxn) {
        mdb_txn_reset(s->readTxn);
    }
}

/* ====================================================================
 * Transaction Management
 * ==================================================================== */

/**
 * Begin a new write transaction.
 *
 * LMDB supports only one write transaction at a time (single-writer model).
 * Calling Begin() when already in a transaction is a no-op (returns true).
 *
 * While a write transaction is active:
 * - All reads see the uncommitted writes (read-your-writes)
 * - Other processes/threads are blocked from writing
 * - Changes are not visible to other readers until commit
 *
 * Write transactions should be kept short to avoid blocking other writers
 * and to minimize the risk of running out of map space.
 *
 * @param i  The kvidx instance
 * @return true on success, false on error (e.g., out of memory)
 */
bool kvidxLmdbBegin(kvidxInstance *i) {
    lmdbState *s = STATE(i);

    if (s->writeTxn) {
        /* Already in a transaction */
        return true;
    }

    int rc = mdb_txn_begin(s->env, NULL, 0, &s->writeTxn);
    if (rc != MDB_SUCCESS) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "LMDB txn_begin failed: %s",
                      mdb_strerror(rc));
        return false;
    }

    return true;
}

/**
 * Commit the current write transaction.
 *
 * Atomically makes all changes visible to other readers and writers.
 * LMDB uses copy-on-write, so commits are very fast - they just update
 * a pointer to the new root page.
 *
 * After commit, the write transaction handle is invalidated. A new Begin()
 * is required for further writes.
 *
 * If no write transaction is active, this is a no-op (returns true).
 *
 * IMPORTANT: Unlike SQLite, LMDB commits are durable by default. The data
 * is synced to disk unless MDB_NOSYNC was set.
 *
 * @param i  The kvidx instance
 * @return true on success, false on error (data may be lost on error)
 */
bool kvidxLmdbCommit(kvidxInstance *i) {
    lmdbState *s = STATE(i);

    if (!s->writeTxn) {
        /* No active transaction */
        return true;
    }

    int rc = mdb_txn_commit(s->writeTxn);
    s->writeTxn = NULL;

    if (rc != MDB_SUCCESS) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "LMDB txn_commit failed: %s",
                      mdb_strerror(rc));
        return false;
    }

    return true;
}

/* ====================================================================
 * Data Manipulation
 * ==================================================================== */

/**
 * Retrieve a record by its exact key.
 *
 * Returns a pointer directly into LMDB's memory-mapped storage (zero-copy).
 * This makes reads extremely fast, but the returned pointer has a limited
 * lifetime.
 *
 * IMPORTANT: The returned data pointer is only valid until:
 * - The next Get/GetPrev/GetNext call on this instance
 * - The transaction is reset or committed
 * - The environment is closed
 *
 * Copy the data if you need to retain it beyond these operations.
 *
 * @param i     The kvidx instance
 * @param key   The uint64_t key to look up
 * @param term  OUT: The term/version number, or NULL if not needed
 * @param cmd   OUT: The command/type identifier, or NULL if not needed
 * @param data  OUT: Pointer to data (zero-copy into mmap), or NULL if not
 * needed
 * @param len   OUT: Length of the data, or NULL if not needed
 * @return true if key was found, false if not found
 */
bool kvidxLmdbGet(kvidxInstance *i, uint64_t key, uint64_t *term, uint64_t *cmd,
                  const uint8_t **data, size_t *len) {
    lmdbState *s = STATE(i);

    if (!ensureReadTxn(i)) {
        return false;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    int rc = mdb_get(getActiveTxn(i), s->dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        resetReadTxn(i);
        return false;
    }
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return false;
    }

    if (term) {
        *term = extractTerm(&mval);
    }
    if (cmd) {
        *cmd = extractCmd(&mval);
    }
    if (data) {
        *data = extractData(&mval, len);
    } else if (len) {
        size_t dlen;
        extractData(&mval, &dlen);
        *len = dlen;
    }

    /* Don't reset - data pointer must stay valid */
    return true;
}

/**
 * Find the record with the largest key less than the given key.
 *
 * Uses an LMDB cursor to position at or after the given key, then moves
 * to the previous entry. This enables reverse iteration through the keyspace.
 *
 * Special case: When nextKey is UINT64_MAX, this finds the maximum key
 * in the database by positioning at MDB_LAST.
 *
 * Like Get(), returns a zero-copy pointer into the memory-mapped region.
 *
 * @param i         The kvidx instance
 * @param nextKey   Find records with keys strictly less than this value
 * @param prevKey   OUT: The found key, or NULL if not needed
 * @param prevTerm  OUT: The term/version, or NULL if not needed
 * @param cmd       OUT: The command/type, or NULL if not needed
 * @param data      OUT: Pointer to data (zero-copy), or NULL if not needed
 * @param len       OUT: Length of data, or NULL if not needed
 * @return true if a previous record was found, false if at beginning
 */
bool kvidxLmdbGetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                      uint64_t *prevTerm, uint64_t *cmd, const uint8_t **data,
                      size_t *len) {
    lmdbState *s = STATE(i);

    if (!ensureReadTxn(i)) {
        return false;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(getActiveTxn(i), s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return false;
    }

    MDB_val mkey = {.mv_size = sizeof(nextKey), .mv_data = &nextKey};
    MDB_val mval;
    bool found = false;

    if (nextKey == UINT64_MAX) {
        /* Get the last key */
        rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_LAST);
        if (rc == MDB_SUCCESS) {
            found = true;
        }
    } else {
        /* Position at or after nextKey, then go back */
        rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_SET_RANGE);
        if (rc == MDB_SUCCESS) {
            /* If we landed on exact key, or any key, go to previous */
            rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_PREV);
            if (rc == MDB_SUCCESS) {
                found = true;
            }
        } else if (rc == MDB_NOTFOUND) {
            /* No key >= nextKey, so get the last key */
            rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_LAST);
            if (rc == MDB_SUCCESS) {
                found = true;
            }
        }
    }

    if (found) {
        uint64_t foundKey;
        memcpy(&foundKey, mkey.mv_data, sizeof(foundKey));

        if (prevKey) {
            *prevKey = foundKey;
        }
        if (prevTerm) {
            *prevTerm = extractTerm(&mval);
        }
        if (cmd) {
            *cmd = extractCmd(&mval);
        }
        if (data) {
            *data = extractData(&mval, len);
        } else if (len) {
            size_t dlen;
            extractData(&mval, &dlen);
            *len = dlen;
        }
    }

    mdb_cursor_close(cursor);
    /* Don't reset if found - data pointer must stay valid */
    if (!found) {
        resetReadTxn(i);
    }
    return found;
}

/**
 * Find the record with the smallest key greater than the given key.
 *
 * Uses an LMDB cursor with MDB_SET_RANGE to find the first key >=
 * previousKey+1. This enables forward iteration through the keyspace.
 *
 * Special case: When previousKey is UINT64_MAX, always returns false since
 * no key can be greater than UINT64_MAX.
 *
 * Like Get(), returns a zero-copy pointer into the memory-mapped region.
 *
 * @param i            The kvidx instance
 * @param previousKey  Find records with keys strictly greater than this value
 * @param nextKey      OUT: The found key, or NULL if not needed
 * @param nextTerm     OUT: The term/version, or NULL if not needed
 * @param cmd          OUT: The command/type, or NULL if not needed
 * @param data         OUT: Pointer to data (zero-copy), or NULL if not needed
 * @param len          OUT: Length of data, or NULL if not needed
 * @return true if a next record was found, false if at end
 */
bool kvidxLmdbGetNext(kvidxInstance *i, uint64_t previousKey, uint64_t *nextKey,
                      uint64_t *nextTerm, uint64_t *cmd, const uint8_t **data,
                      size_t *len) {
    lmdbState *s = STATE(i);

    /* No key can be greater than UINT64_MAX */
    if (previousKey == UINT64_MAX) {
        return false;
    }

    if (!ensureReadTxn(i)) {
        return false;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(getActiveTxn(i), s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return false;
    }

    /* We want key > previousKey, so search for previousKey+1 */
    uint64_t searchKey = previousKey + 1;
    MDB_val mkey = {.mv_size = sizeof(searchKey), .mv_data = &searchKey};
    MDB_val mval;
    bool found = false;

    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_SET_RANGE);
    if (rc == MDB_SUCCESS) {
        found = true;
    }

    if (found) {
        uint64_t foundKey;
        memcpy(&foundKey, mkey.mv_data, sizeof(foundKey));

        if (nextKey) {
            *nextKey = foundKey;
        }
        if (nextTerm) {
            *nextTerm = extractTerm(&mval);
        }
        if (cmd) {
            *cmd = extractCmd(&mval);
        }
        if (data) {
            *data = extractData(&mval, len);
        } else if (len) {
            size_t dlen;
            extractData(&mval, &dlen);
            *len = dlen;
        }
    }

    mdb_cursor_close(cursor);
    if (!found) {
        resetReadTxn(i);
    }
    return found;
}

/**
 * Check if a key exists in the database.
 *
 * Performs a simple lookup without extracting the value data. Since LMDB
 * operations are already very fast, this is only marginally faster than
 * Get() but uses slightly less code.
 *
 * @param i    The kvidx instance
 * @param key  The key to check
 * @return true if the key exists, false otherwise
 */
bool kvidxLmdbExists(kvidxInstance *i, uint64_t key) {
    lmdbState *s = STATE(i);

    if (!ensureReadTxn(i)) {
        return false;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    int rc = mdb_get(getActiveTxn(i), s->dbi, &mkey, &mval);
    resetReadTxn(i);
    return rc == MDB_SUCCESS;
}

/**
 * Check if a key exists with a specific term/version.
 *
 * Looks up the key and compares the stored term against the expected value.
 * Useful for optimistic concurrency control or version checking.
 *
 * @param i     The kvidx instance
 * @param key   The key to check
 * @param term  The expected term/version value
 * @return true if key exists with matching term, false otherwise
 */
bool kvidxLmdbExistsDual(kvidxInstance *i, uint64_t key, uint64_t term) {
    lmdbState *s = STATE(i);

    if (!ensureReadTxn(i)) {
        return false;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    int rc = mdb_get(getActiveTxn(i), s->dbi, &mkey, &mval);
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return false;
    }

    uint64_t storedTerm = extractTerm(&mval);
    resetReadTxn(i);
    return storedTerm == term;
}

/**
 * Get the maximum (largest) key in the database.
 *
 * Uses a cursor positioned at MDB_LAST to efficiently find the maximum key.
 * LMDB's B-tree structure makes this O(log n).
 *
 * @param i    The kvidx instance
 * @param key  OUT: The maximum key value, or NULL if just checking existence
 * @return true if database has at least one record, false if empty
 */
bool kvidxLmdbMax(kvidxInstance *i, uint64_t *key) {
    lmdbState *s = STATE(i);

    if (!ensureReadTxn(i)) {
        return false;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(getActiveTxn(i), s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return false;
    }

    MDB_val mkey, mval;
    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_LAST);
    mdb_cursor_close(cursor);

    if (rc == MDB_SUCCESS) {
        if (key) {
            memcpy(key, mkey.mv_data, sizeof(*key));
        }
        resetReadTxn(i);
        return true;
    }

    resetReadTxn(i);
    return false;
}

/**
 * Insert a new record into the database.
 *
 * Creates a new record with the given key, metadata, and data. Uses
 * MDB_NOOVERWRITE to fail on duplicate keys, matching SQLite behavior.
 *
 * If no transaction is active, creates an auto-commit transaction for
 * this single operation. Otherwise, uses the existing transaction.
 *
 * The data is copied into LMDB's storage (LMDB does not support zero-copy
 * writes in the same way it supports zero-copy reads).
 *
 * @param i        The kvidx instance
 * @param key      The uint64_t key for this record
 * @param term     Application-defined term/version number
 * @param cmd      Application-defined command/type identifier
 * @param data     Pointer to the data to store (may be NULL if dataLen is 0)
 * @param dataLen  Length of the data in bytes
 * @return true on success, false if key exists or other error
 */
bool kvidxLmdbInsert(kvidxInstance *i, uint64_t key, uint64_t term,
                     uint64_t cmd, const void *data, size_t dataLen) {
    lmdbState *s = STATE(i);
    bool ownTxn = false;

    if (!s->writeTxn) {
        if (!kvidxLmdbBegin(i)) {
            return false;
        }
        ownTxn = true;
    }

    size_t valLen;
    void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
    if (!valBuf) {
        if (ownTxn) {
            mdb_txn_abort(s->writeTxn);
            s->writeTxn = NULL;
        }
        return false;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval = {.mv_size = valLen, .mv_data = valBuf};

    /* Use MDB_NOOVERWRITE to fail on duplicate keys (match SQLite behavior) */
    int rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, MDB_NOOVERWRITE);
    free(valBuf);

    if (rc == MDB_KEYEXIST) {
        /* Duplicate key - return false but don't abort transaction */
        if (ownTxn) {
            mdb_txn_abort(s->writeTxn);
            s->writeTxn = NULL;
        }
        kvidxSetError(i, KVIDX_ERROR_DUPLICATE_KEY, "Key already exists");
        return false;
    }

    if (rc != MDB_SUCCESS) {
        if (ownTxn) {
            mdb_txn_abort(s->writeTxn);
            s->writeTxn = NULL;
        }
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "LMDB put failed: %s",
                      mdb_strerror(rc));
        return false;
    }

    if (ownTxn) {
        return kvidxLmdbCommit(i);
    }

    return true;
}

/**
 * Delete a record by key.
 *
 * Removes the record with the specified key from the database. This is
 * idempotent - removing a non-existent key still returns true.
 *
 * If no transaction is active, creates an auto-commit transaction.
 *
 * @param i    The kvidx instance
 * @param key  The key to remove
 * @return true on success (including if key didn't exist), false on error
 */
bool kvidxLmdbRemove(kvidxInstance *i, uint64_t key) {
    lmdbState *s = STATE(i);
    bool ownTxn = false;

    if (!s->writeTxn) {
        if (!kvidxLmdbBegin(i)) {
            return false;
        }
        ownTxn = true;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};

    int rc = mdb_del(s->writeTxn, s->dbi, &mkey, NULL);

    if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
        if (ownTxn) {
            mdb_txn_abort(s->writeTxn);
            s->writeTxn = NULL;
        }
        return false;
    }

    if (ownTxn) {
        return kvidxLmdbCommit(i);
    }

    return true;
}

/**
 * Delete all records with keys greater than or equal to the given key.
 *
 * Uses a cursor to iterate from the starting key to the end, deleting
 * each entry. LMDB cursor deletion is efficient - after delete, the cursor
 * automatically points to the next entry.
 *
 * @param i    The kvidx instance
 * @param key  The starting key (inclusive) - all keys >= this are deleted
 * @return true on success, false on error
 */
bool kvidxLmdbRemoveAfterNInclusive(kvidxInstance *i, uint64_t key) {
    lmdbState *s = STATE(i);
    bool ownTxn = false;

    if (!s->writeTxn) {
        if (!kvidxLmdbBegin(i)) {
            return false;
        }
        ownTxn = true;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(s->writeTxn, s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        if (ownTxn) {
            mdb_txn_abort(s->writeTxn);
            s->writeTxn = NULL;
        }
        return false;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    /* Position at first key >= key */
    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_SET_RANGE);

    while (rc == MDB_SUCCESS) {
        rc = mdb_cursor_del(cursor, 0);
        if (rc != MDB_SUCCESS) {
            break;
        }
        rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT);
    }

    mdb_cursor_close(cursor);

    if (ownTxn) {
        return kvidxLmdbCommit(i);
    }

    return true;
}

/**
 * Delete all records with keys less than or equal to the given key.
 *
 * Uses a cursor starting from MDB_FIRST, deleting entries until the key
 * exceeds the specified bound. After each deletion, we re-fetch the current
 * position since LMDB cursor behavior after delete can vary.
 *
 * @param i    The kvidx instance
 * @param key  The ending key (inclusive) - all keys <= this are deleted
 * @return true on success, false on error
 */
bool kvidxLmdbRemoveBeforeNInclusive(kvidxInstance *i, uint64_t key) {
    lmdbState *s = STATE(i);
    bool ownTxn = false;

    if (!s->writeTxn) {
        if (!kvidxLmdbBegin(i)) {
            return false;
        }
        ownTxn = true;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(s->writeTxn, s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        if (ownTxn) {
            mdb_txn_abort(s->writeTxn);
            s->writeTxn = NULL;
        }
        return false;
    }

    MDB_val mkey, mval;

    /* Start from the first key */
    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_FIRST);

    while (rc == MDB_SUCCESS) {
        uint64_t currentKey;
        memcpy(&currentKey, mkey.mv_data, sizeof(currentKey));

        if (currentKey > key) {
            break;
        }

        rc = mdb_cursor_del(cursor, 0);
        if (rc != MDB_SUCCESS) {
            break;
        }

        /* After delete, cursor is at next key (or invalid) */
        rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_GET_CURRENT);
        if (rc == MDB_NOTFOUND) {
            /* Try to get first again in case there are more */
            rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_FIRST);
        }
    }

    mdb_cursor_close(cursor);

    if (ownTxn) {
        return kvidxLmdbCommit(i);
    }

    return true;
}

/**
 * Force synchronization of the database to disk.
 *
 * LMDB normally syncs data automatically at transaction commit (unless
 * MDB_NOSYNC is set). This function forces an explicit sync.
 *
 * The parameter to mdb_env_sync(1) means "force sync even if not needed".
 *
 * @param i  The kvidx instance
 * @return true on success, false on error
 */
bool kvidxLmdbFsync(kvidxInstance *i) {
    lmdbState *s = STATE(i);
    int rc = mdb_env_sync(s->env, 1);
    return rc == MDB_SUCCESS;
}

/* ====================================================================
 * Bring-Up / Teardown
 * ==================================================================== */

/**
 * Open or create an LMDB-backed kvidx database.
 *
 * LMDB uses a directory (not a single file) to store its data. This function:
 * 1. Creates the directory if it doesn't exist
 * 2. Creates and configures the LMDB environment
 * 3. Opens/creates the main data database (_kvidx_data)
 * 4. Opens/creates the TTL metadata database (_kvidx_ttl)
 *
 * Configuration:
 * - Map size: 1GB (can grow dynamically)
 * - Max databases: 2 (data + TTL)
 * - MDB_NOTLS: Allows transactions to be used across threads
 *
 * The directory will contain:
 * - data.mdb: The actual database file
 * - lock.mdb: Lock file for multi-process coordination
 *
 * @param i        The kvidx instance to initialize
 * @param filename Path to the database directory (created if missing)
 * @param errStr   OUT: Error message on failure, or NULL if not needed
 * @return true on success, false on error
 */
bool kvidxLmdbOpen(kvidxInstance *i, const char *filename,
                   const char **errStr) {
    int rc;

    /* LMDB needs a directory for its data.mdb and lock.mdb files.
     * If the path doesn't exist, create it. */
    struct stat st;
    if (stat(filename, &st) != 0) {
        if (mkdir(filename, 0755) != 0) {
            if (errStr) {
                *errStr = "Failed to create LMDB directory";
            }
            return false;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        if (errStr) {
            *errStr = "LMDB path exists but is not a directory";
        }
        return false;
    }

    i->kvidxdata = calloc(1, sizeof(lmdbState));
    if (!i->kvidxdata) {
        if (errStr) {
            *errStr = "Memory allocation failed";
        }
        return false;
    }

    lmdbState *s = STATE(i);
    s->envPath = strdup(filename);

    rc = mdb_env_create(&s->env);
    if (rc != MDB_SUCCESS) {
        if (errStr) {
            *errStr = mdb_strerror(rc);
        }
        free(s->envPath);
        free(i->kvidxdata);
        i->kvidxdata = NULL;
        return false;
    }

    /* Set map size - 1GB default, can grow */
    mdb_env_set_mapsize(s->env, DEFAULT_MAP_SIZE);

    /* Allow up to 2 named databases (main + TTL) */
    mdb_env_set_maxdbs(s->env, 2);

    /* Open environment - use MDB_NOTLS for flexibility with transaction reuse
     */
    unsigned int flags = MDB_NOTLS;
    rc = mdb_env_open(s->env, filename, flags, 0644);
    if (rc != MDB_SUCCESS) {
        if (errStr) {
            *errStr = mdb_strerror(rc);
        }
        mdb_env_close(s->env);
        free(s->envPath);
        free(i->kvidxdata);
        i->kvidxdata = NULL;
        return false;
    }

    /* Open a transaction to create/open the database */
    MDB_txn *txn;
    rc = mdb_txn_begin(s->env, NULL, 0, &txn);
    if (rc != MDB_SUCCESS) {
        if (errStr) {
            *errStr = mdb_strerror(rc);
        }
        mdb_env_close(s->env);
        free(s->envPath);
        free(i->kvidxdata);
        i->kvidxdata = NULL;
        return false;
    }

    /* Open main data database with integer keys (named to allow TTL database)
     */
    rc = mdb_dbi_open(txn, "_kvidx_data", MDB_INTEGERKEY | MDB_CREATE, &s->dbi);
    if (rc != MDB_SUCCESS) {
        if (errStr) {
            *errStr = mdb_strerror(rc);
        }
        mdb_txn_abort(txn);
        mdb_env_close(s->env);
        free(s->envPath);
        free(i->kvidxdata);
        i->kvidxdata = NULL;
        return false;
    }

    /* Pre-open TTL database to ensure it's available */
    rc = mdb_dbi_open(txn, "_kvidx_ttl", MDB_INTEGERKEY | MDB_CREATE,
                      &s->ttlDbi);
    if (rc != MDB_SUCCESS) {
        if (errStr) {
            *errStr = mdb_strerror(rc);
        }
        mdb_txn_abort(txn);
        mdb_env_close(s->env);
        free(s->envPath);
        free(i->kvidxdata);
        i->kvidxdata = NULL;
        return false;
    }
    s->ttlDbiInitialized = true;

    rc = mdb_txn_commit(txn);
    if (rc != MDB_SUCCESS) {
        if (errStr) {
            *errStr = mdb_strerror(rc);
        }
        mdb_env_close(s->env);
        free(s->envPath);
        free(i->kvidxdata);
        i->kvidxdata = NULL;
        return false;
    }

    /* Call custom init if provided */
    if (i->customInit) {
        i->customInit(i);
    }

    return true;
}

/**
 * Close an LMDB-backed kvidx database.
 *
 * Cleanly shuts down the database by:
 * 1. Aborting any active write transaction
 * 2. Aborting the read transaction
 * 3. Closing database handles
 * 4. Closing the environment
 * 5. Freeing memory
 *
 * After this call, the kvidx instance is invalid and must not be used.
 *
 * IMPORTANT: All data pointers returned from Get() become invalid after close.
 *
 * @param i  The kvidx instance to close
 * @return true on success
 */
bool kvidxLmdbClose(kvidxInstance *i) {
    lmdbState *s = STATE(i);

    if (s->writeTxn) {
        mdb_txn_abort(s->writeTxn);
        s->writeTxn = NULL;
    }

    if (s->readTxn) {
        mdb_txn_abort(s->readTxn);
        s->readTxn = NULL;
    }

    mdb_dbi_close(s->env, s->dbi);
    if (s->ttlDbiInitialized) {
        mdb_dbi_close(s->env, s->ttlDbi);
    }
    mdb_env_close(s->env);

    free(s->envPath);
    free(i->kvidxdata);
    i->kvidxdata = NULL;

    return true;
}

/* ====================================================================
 * Statistics Implementation
 * ==================================================================== */

/**
 * Get the total number of records in the database.
 *
 * Uses LMDB's mdb_stat() which returns statistics including entry count.
 * This is O(1) since LMDB maintains this count internally.
 *
 * @param i      The kvidx instance
 * @param count  OUT: The number of records
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxLmdbGetKeyCount(kvidxInstance *i, uint64_t *count) {
    lmdbState *s = STATE(i);
    if (!s || !s->env) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    if (!ensureReadTxn(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_stat stat;
    int rc = mdb_stat(getActiveTxn(i), s->dbi, &stat);
    resetReadTxn(i);

    if (rc != MDB_SUCCESS) {
        return KVIDX_ERROR_INTERNAL;
    }

    *count = stat.ms_entries;
    return KVIDX_OK;
}

/**
 * Get the minimum (smallest) key in the database.
 *
 * Uses a cursor positioned at MDB_FIRST to find the smallest key.
 *
 * @param i    The kvidx instance
 * @param key  OUT: The minimum key value
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if database is empty
 */
kvidxError kvidxLmdbGetMinKey(kvidxInstance *i, uint64_t *key) {
    lmdbState *s = STATE(i);
    if (!s || !s->env) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    if (!ensureReadTxn(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(getActiveTxn(i), s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey, mval;
    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_FIRST);
    mdb_cursor_close(cursor);
    resetReadTxn(i);

    if (rc == MDB_NOTFOUND) {
        return KVIDX_ERROR_NOT_FOUND;
    }
    if (rc != MDB_SUCCESS) {
        return KVIDX_ERROR_INTERNAL;
    }

    memcpy(key, mkey.mv_data, sizeof(*key));
    return KVIDX_OK;
}

/**
 * Get the total size of all data blobs in the database.
 *
 * Iterates through all entries to sum data lengths. Unlike mdb_stat(),
 * there's no O(1) way to get this information, so this scans all entries.
 *
 * Note: Only counts the data portion, not the term/cmd header overhead.
 *
 * @param i      The kvidx instance
 * @param bytes  OUT: Total bytes of data stored
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxLmdbGetDataSize(kvidxInstance *i, uint64_t *bytes) {
    lmdbState *s = STATE(i);
    if (!s || !s->env) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    if (!ensureReadTxn(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(getActiveTxn(i), s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return KVIDX_ERROR_INTERNAL;
    }

    uint64_t totalSize = 0;
    MDB_val mkey, mval;

    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_FIRST);
    while (rc == MDB_SUCCESS) {
        /* Data size is value size minus header */
        if (mval.mv_size > VALUE_HEADER_SIZE) {
            totalSize += mval.mv_size - VALUE_HEADER_SIZE;
        }
        rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT);
    }

    mdb_cursor_close(cursor);
    resetReadTxn(i);

    *bytes = totalSize;
    return KVIDX_OK;
}

/**
 * Get comprehensive database statistics in a single call.
 *
 * Gathers all available statistics about the database:
 * - Entry count from mdb_stat()
 * - Page size and count from mdb_stat()
 * - Map size from mdb_env_info()
 * - Min/max keys and total data size from cursor scan
 *
 * @param i      The kvidx instance
 * @param stats  OUT: Structure to fill with statistics
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxLmdbGetStats(kvidxInstance *i, kvidxStats *stats) {
    lmdbState *s = STATE(i);
    if (!s || !s->env || !stats) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    memset(stats, 0, sizeof(*stats));

    if (!ensureReadTxn(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    /* Get database statistics */
    MDB_stat dbStat;
    int rc = mdb_stat(getActiveTxn(i), s->dbi, &dbStat);
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return KVIDX_ERROR_INTERNAL;
    }

    stats->totalKeys = dbStat.ms_entries;
    stats->pageSize = dbStat.ms_psize;
    stats->pageCount = dbStat.ms_branch_pages + dbStat.ms_leaf_pages +
                       dbStat.ms_overflow_pages;

    /* Get environment info for map size */
    MDB_envinfo envInfo;
    rc = mdb_env_info(s->env, &envInfo);
    if (rc == MDB_SUCCESS) {
        stats->databaseFileSize = envInfo.me_mapsize;
    }

    /* Get min/max keys and data size via cursor */
    MDB_cursor *cursor;
    rc = mdb_cursor_open(getActiveTxn(i), s->dbi, &cursor);
    if (rc == MDB_SUCCESS) {
        MDB_val mkey, mval;

        /* Get min key */
        if (mdb_cursor_get(cursor, &mkey, &mval, MDB_FIRST) == MDB_SUCCESS) {
            memcpy(&stats->minKey, mkey.mv_data, sizeof(stats->minKey));

            /* Calculate total data size while we're iterating */
            uint64_t totalData = 0;
            do {
                if (mval.mv_size > VALUE_HEADER_SIZE) {
                    totalData += mval.mv_size - VALUE_HEADER_SIZE;
                }
            } while (mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT) ==
                     MDB_SUCCESS);

            stats->totalDataBytes = totalData;
        }

        /* Get max key */
        if (mdb_cursor_get(cursor, &mkey, &mval, MDB_LAST) == MDB_SUCCESS) {
            memcpy(&stats->maxKey, mkey.mv_data, sizeof(stats->maxKey));
        }

        mdb_cursor_close(cursor);
    }

    resetReadTxn(i);
    return KVIDX_OK;
}

/* ====================================================================
 * Range Operations Implementation
 * ==================================================================== */

kvidxError kvidxLmdbRemoveRange(kvidxInstance *i, uint64_t startKey,
                                uint64_t endKey, bool startInclusive,
                                bool endInclusive, uint64_t *deletedCount) {
    lmdbState *s = STATE(i);
    if (!s || !s->env) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    bool ownTxn = false;
    if (!s->writeTxn) {
        if (!kvidxLmdbBegin(i)) {
            return KVIDX_ERROR_INTERNAL;
        }
        ownTxn = true;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(s->writeTxn, s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        if (ownTxn) {
            mdb_txn_abort(s->writeTxn);
            s->writeTxn = NULL;
        }
        return KVIDX_ERROR_INTERNAL;
    }

    uint64_t deleted = 0;
    uint64_t searchKey = startInclusive ? startKey : startKey + 1;
    MDB_val mkey = {.mv_size = sizeof(searchKey), .mv_data = &searchKey};
    MDB_val mval;

    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_SET_RANGE);

    while (rc == MDB_SUCCESS) {
        uint64_t currentKey;
        memcpy(&currentKey, mkey.mv_data, sizeof(currentKey));

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

        rc = mdb_cursor_del(cursor, 0);
        if (rc != MDB_SUCCESS) {
            break;
        }
        deleted++;

        rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_GET_CURRENT);
        if (rc == MDB_NOTFOUND) {
            rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_FIRST);
        }
    }

    mdb_cursor_close(cursor);

    if (deletedCount) {
        *deletedCount = deleted;
    }

    if (ownTxn) {
        if (!kvidxLmdbCommit(i)) {
            return KVIDX_ERROR_INTERNAL;
        }
    }

    return KVIDX_OK;
}

kvidxError kvidxLmdbCountRange(kvidxInstance *i, uint64_t startKey,
                               uint64_t endKey, uint64_t *count) {
    lmdbState *s = STATE(i);
    if (!s || !s->env || !count) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    if (!ensureReadTxn(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(getActiveTxn(i), s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return KVIDX_ERROR_INTERNAL;
    }

    uint64_t cnt = 0;
    MDB_val mkey = {.mv_size = sizeof(startKey), .mv_data = &startKey};
    MDB_val mval;

    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_SET_RANGE);

    while (rc == MDB_SUCCESS) {
        uint64_t currentKey;
        memcpy(&currentKey, mkey.mv_data, sizeof(currentKey));

        if (currentKey > endKey) {
            break;
        }
        cnt++;

        rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT);
    }

    mdb_cursor_close(cursor);
    resetReadTxn(i);

    *count = cnt;
    return KVIDX_OK;
}

kvidxError kvidxLmdbExistsInRange(kvidxInstance *i, uint64_t startKey,
                                  uint64_t endKey, bool *exists) {
    lmdbState *s = STATE(i);
    if (!s || !s->env || !exists) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    if (!ensureReadTxn(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(getActiveTxn(i), s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey = {.mv_size = sizeof(startKey), .mv_data = &startKey};
    MDB_val mval;

    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_SET_RANGE);

    *exists = false;
    if (rc == MDB_SUCCESS) {
        uint64_t foundKey;
        memcpy(&foundKey, mkey.mv_data, sizeof(foundKey));
        if (foundKey <= endKey) {
            *exists = true;
        }
    }

    mdb_cursor_close(cursor);
    resetReadTxn(i);

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

kvidxError kvidxLmdbExport(kvidxInstance *i, const char *filename,
                           const kvidxExportOptions *options,
                           kvidxProgressCallback callback, void *userData) {
    lmdbState *s = STATE(i);
    if (!s || !s->env || !filename || !options) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    FILE *fp = fopen(filename, "wb");
    if (!fp) {
        kvidxSetError(i, KVIDX_ERROR_IO, "Failed to open file: %s", filename);
        return KVIDX_ERROR_IO;
    }

    if (!ensureReadTxn(i)) {
        fclose(fp);
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(getActiveTxn(i), s->dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        fclose(fp);
        resetReadTxn(i);
        return KVIDX_ERROR_INTERNAL;
    }

    /* Get total count for progress */
    MDB_stat dbStat;
    mdb_stat(getActiveTxn(i), s->dbi, &dbStat);
    uint64_t total = dbStat.ms_entries;

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
    MDB_val mkey = {.mv_size = sizeof(options->startKey),
                    .mv_data = (void *)&options->startKey};
    MDB_val mval;
    bool firstEntry = true;

    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_SET_RANGE);

    while (rc == MDB_SUCCESS) {
        uint64_t key;
        memcpy(&key, mkey.mv_data, sizeof(key));

        if (key > options->endKey) {
            break;
        }

        uint64_t term = extractTerm(&mval);
        uint64_t cmd = extractCmd(&mval);
        size_t dataLen;
        const uint8_t *data = extractData(&mval, &dataLen);

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

        rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT);
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
    mdb_cursor_close(cursor);
    resetReadTxn(i);
    fclose(fp);

    return result;
}

kvidxError kvidxLmdbImport(kvidxInstance *i, const char *filename,
                           const kvidxImportOptions *options,
                           kvidxProgressCallback callback, void *userData) {
    lmdbState *s = STATE(i);
    if (!s || !s->env || !filename || !options) {
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
        if (!kvidxLmdbBegin(i)) {
            fclose(fp);
            return KVIDX_ERROR_INTERNAL;
        }

        MDB_cursor *cursor;
        int rc = mdb_cursor_open(s->writeTxn, s->dbi, &cursor);
        if (rc == MDB_SUCCESS) {
            MDB_val mkey, mval;
            while (mdb_cursor_get(cursor, &mkey, &mval, MDB_FIRST) ==
                   MDB_SUCCESS) {
                mdb_cursor_del(cursor, 0);
            }
            mdb_cursor_close(cursor);
        }

        if (!kvidxLmdbCommit(i)) {
            fclose(fp);
            return KVIDX_ERROR_INTERNAL;
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

    /* Begin transaction */
    if (!kvidxLmdbBegin(i)) {
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

        /* Pack and insert */
        size_t valLen;
        void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
        free(data);

        if (!valBuf) {
            result = KVIDX_ERROR_INTERNAL;
            break;
        }

        MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
        MDB_val mval = {.mv_size = valLen, .mv_data = valBuf};

        int flags = options->skipDuplicates ? MDB_NOOVERWRITE : 0;
        int rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, flags);
        free(valBuf);

        if (rc != MDB_SUCCESS && rc != MDB_KEYEXIST) {
            result = KVIDX_ERROR_INTERNAL;
            break;
        }

        if (rc == MDB_SUCCESS) {
            count++;
        }

        if (callback && count % 100 == 0) {
            if (!callback(count, header.entryCount, userData)) {
                result = KVIDX_ERROR_CANCELLED;
                break;
            }
        }
    }

    fclose(fp);

    if (result == KVIDX_OK) {
        if (!kvidxLmdbCommit(i)) {
            return KVIDX_ERROR_INTERNAL;
        }
    } else {
        mdb_txn_abort(s->writeTxn);
        s->writeTxn = NULL;
    }

    if (callback && count > 0) {
        callback(count, header.entryCount, userData);
    }

    return result;
}

/* ====================================================================
 * Configuration (stub - LMDB has different config model)
 * ==================================================================== */

kvidxError kvidxLmdbApplyConfig(kvidxInstance *i, const kvidxConfig *config) {
    lmdbState *s = STATE(i);
    if (!s || !s->env || !config) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* LMDB configuration is largely set at environment open time.
     * We can adjust map size dynamically if needed. */
    if (config->mmapSizeBytes > 0) {
        int rc = mdb_env_set_mapsize(s->env, config->mmapSizeBytes);
        if (rc != MDB_SUCCESS) {
            kvidxSetError(i, KVIDX_ERROR_INTERNAL, "Failed to set map size: %s",
                          mdb_strerror(rc));
            return KVIDX_ERROR_INTERNAL;
        }
    }

    /* Sync mode can be adjusted via flags */
    if (config->syncMode == KVIDX_SYNC_OFF) {
        mdb_env_set_flags(s->env, MDB_NOSYNC, 1);
    } else {
        mdb_env_set_flags(s->env, MDB_NOSYNC, 0);
    }

    return KVIDX_OK;
}

/* ====================================================================
 * Storage Primitives Implementation (v0.8.0)
 * ==================================================================== */

/**
 * @section Storage Primitives
 *
 * Advanced key-value operations for LMDB. These mirror the SQLite adapter's
 * primitives but are implemented using LMDB's API.
 *
 * Key differences from SQLite implementation:
 * - All operations use LMDB transactions (write txn auto-created if needed)
 * - Zero-copy reads where possible (data copied only when necessary)
 * - TTL stored in separate named database (_kvidx_ttl)
 */

#include <time.h>

/**
 * Get current wall-clock time in milliseconds since epoch.
 *
 * Used for TTL calculations.
 *
 * @return Current time in milliseconds
 */
static uint64_t currentTimeMsLmdb(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/**
 * Check if TTL database is available.
 *
 * The TTL database is pre-opened during kvidxLmdbOpen(), so this just
 * checks the initialization flag.
 *
 * @param i  The kvidx instance
 * @return true if TTL database is ready, false otherwise
 */
static bool ensureTTLDb(kvidxInstance *i) {
    lmdbState *s = STATE(i);
    return s->ttlDbiInitialized;
}

/* --- Transaction Abort --- */

/**
 * Abort the current transaction and roll back all changes.
 *
 * Discards all modifications made since the last Begin() call. Unlike SQLite,
 * LMDB's abort is very fast due to copy-on-write - it simply discards the
 * uncommitted pages.
 *
 * @param i  The kvidx instance
 * @return true on success (even if no transaction was active)
 */
bool kvidxLmdbAbort(kvidxInstance *i) {
    lmdbState *s = STATE(i);

    if (!s->writeTxn) {
        /* No active transaction */
        return true;
    }

    mdb_txn_abort(s->writeTxn);
    s->writeTxn = NULL;
    return true;
}

/* --- Conditional Writes --- */

/**
 * Insert or update a record with conditional behavior.
 *
 * Supports different write modes:
 * - KVIDX_SET_ALWAYS: Upsert (insert or replace)
 * - KVIDX_SET_IF_NOT_EXISTS: Only insert if key doesn't exist (uses
 * MDB_NOOVERWRITE)
 * - KVIDX_SET_IF_EXISTS: Only update if key already exists
 *
 * @param i          The kvidx instance
 * @param key        The record key
 * @param term       Application-defined term/version
 * @param cmd        Application-defined command/type
 * @param data       Data to store
 * @param dataLen    Length of data
 * @param condition  Write condition
 * @return KVIDX_OK on success, KVIDX_ERROR_CONDITION_FAILED if condition not
 * met
 */
kvidxError kvidxLmdbInsertEx(kvidxInstance *i, uint64_t key, uint64_t term,
                             uint64_t cmd, const void *data, size_t dataLen,
                             kvidxSetCondition condition) {
    lmdbState *s = STATE(i);
    bool ownTxn = (s->writeTxn == NULL);

    if (ownTxn && !kvidxLmdbBegin(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    kvidxError result = KVIDX_OK;

    switch (condition) {
    case KVIDX_SET_ALWAYS: {
        /* Normal insert/replace */
        size_t valLen;
        void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
        if (!valBuf) {
            result = KVIDX_ERROR_NOMEM;
            break;
        }

        MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
        MDB_val mval = {.mv_size = valLen, .mv_data = valBuf};
        int rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, 0);
        free(valBuf);

        if (rc != MDB_SUCCESS) {
            result = KVIDX_ERROR_INTERNAL;
        }
        break;
    }

    case KVIDX_SET_IF_NOT_EXISTS: {
        /* Insert only if key doesn't exist */
        size_t valLen;
        void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
        if (!valBuf) {
            result = KVIDX_ERROR_NOMEM;
            break;
        }

        MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
        MDB_val mval = {.mv_size = valLen, .mv_data = valBuf};
        int rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, MDB_NOOVERWRITE);
        free(valBuf);

        if (rc == MDB_KEYEXIST) {
            result = KVIDX_ERROR_CONDITION_FAILED;
        } else if (rc != MDB_SUCCESS) {
            result = KVIDX_ERROR_INTERNAL;
        }
        break;
    }

    case KVIDX_SET_IF_EXISTS: {
        /* Update only if key exists */
        MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
        MDB_val mval;

        /* Check if exists */
        int rc = mdb_get(s->writeTxn, s->dbi, &mkey, &mval);
        if (rc == MDB_NOTFOUND) {
            result = KVIDX_ERROR_CONDITION_FAILED;
            break;
        }

        /* Exists, update */
        size_t valLen;
        void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
        if (!valBuf) {
            result = KVIDX_ERROR_NOMEM;
            break;
        }

        mval.mv_size = valLen;
        mval.mv_data = valBuf;
        rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, 0);
        free(valBuf);

        if (rc != MDB_SUCCESS) {
            result = KVIDX_ERROR_INTERNAL;
        }
        break;
    }

    default:
        result = KVIDX_ERROR_INVALID_ARGUMENT;
    }

    if (ownTxn) {
        if (result == KVIDX_OK) {
            if (!kvidxLmdbCommit(i)) {
                return KVIDX_ERROR_INTERNAL;
            }
        } else {
            kvidxLmdbAbort(i);
        }
    }

    return result;
}

/* --- Atomic Operations --- */

kvidxError kvidxLmdbGetAndSet(kvidxInstance *i, uint64_t key, uint64_t term,
                              uint64_t cmd, const void *data, size_t dataLen,
                              uint64_t *oldTerm, uint64_t *oldCmd,
                              void **oldData, size_t *oldDataLen) {
    lmdbState *s = STATE(i);
    bool ownTxn = (s->writeTxn == NULL);

    /* Initialize all outputs */
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

    if (ownTxn && !kvidxLmdbBegin(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    /* Try to get existing value */
    int rc = mdb_get(s->writeTxn, s->dbi, &mkey, &mval);
    if (rc == MDB_SUCCESS) {
        /* Copy old value */
        if (oldTerm) {
            *oldTerm = extractTerm(&mval);
        }
        if (oldCmd) {
            *oldCmd = extractCmd(&mval);
        }

        size_t dlen;
        const uint8_t *dptr = extractData(&mval, &dlen);
        if (oldData && dlen > 0) {
            *oldData = malloc(dlen);
            if (*oldData) {
                memcpy(*oldData, dptr, dlen);
                if (oldDataLen) {
                    *oldDataLen = dlen;
                }
            }
        } else if (oldDataLen) {
            *oldDataLen = dlen;
        }
    }

    /* Set new value */
    size_t valLen;
    void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
    if (!valBuf) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_NOMEM;
    }

    mval.mv_size = valLen;
    mval.mv_data = valBuf;
    rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, 0);
    free(valBuf);

    if (rc != MDB_SUCCESS) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_INTERNAL;
    }

    if (ownTxn && !kvidxLmdbCommit(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    return KVIDX_OK;
}

kvidxError kvidxLmdbGetAndRemove(kvidxInstance *i, uint64_t key, uint64_t *term,
                                 uint64_t *cmd, void **data, size_t *dataLen) {
    lmdbState *s = STATE(i);
    bool ownTxn = (s->writeTxn == NULL);

    /* Initialize outputs */
    if (data) {
        *data = NULL;
    }
    if (dataLen) {
        *dataLen = 0;
    }

    if (ownTxn && !kvidxLmdbBegin(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    /* Get existing value */
    int rc = mdb_get(s->writeTxn, s->dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_NOT_FOUND;
    }
    if (rc != MDB_SUCCESS) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_INTERNAL;
    }

    /* Copy value before delete */
    if (term) {
        *term = extractTerm(&mval);
    }
    if (cmd) {
        *cmd = extractCmd(&mval);
    }

    size_t dlen;
    const uint8_t *dptr = extractData(&mval, &dlen);
    if (data && dlen > 0) {
        *data = malloc(dlen);
        if (*data) {
            memcpy(*data, dptr, dlen);
            if (dataLen) {
                *dataLen = dlen;
            }
        }
    } else if (dataLen) {
        *dataLen = dlen;
    }

    /* Delete */
    rc = mdb_del(s->writeTxn, s->dbi, &mkey, NULL);
    if (rc != MDB_SUCCESS) {
        if (data && *data) {
            free(*data);
            *data = NULL;
        }
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_INTERNAL;
    }

    if (ownTxn && !kvidxLmdbCommit(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    return KVIDX_OK;
}

/* --- Compare-And-Swap --- */

kvidxError kvidxLmdbCompareAndSwap(kvidxInstance *i, uint64_t key,
                                   const void *expectedData, size_t expectedLen,
                                   uint64_t newTerm, uint64_t newCmd,
                                   const void *newData, size_t newDataLen,
                                   bool *swapped) {
    lmdbState *s = STATE(i);
    bool ownTxn = (s->writeTxn == NULL);
    *swapped = false;

    if (ownTxn && !kvidxLmdbBegin(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    /* Get current value */
    int rc = mdb_get(s->writeTxn, s->dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_NOT_FOUND;
    }
    if (rc != MDB_SUCCESS) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_INTERNAL;
    }

    /* Compare data */
    size_t currentLen;
    const uint8_t *currentData = extractData(&mval, &currentLen);

    bool matches = false;
    if (expectedData == NULL && currentLen == 0) {
        matches = true;
    } else if (expectedLen == currentLen) {
        if (expectedLen == 0) {
            matches = true;
        } else if (expectedData && currentData) {
            matches = (memcmp(expectedData, currentData, expectedLen) == 0);
        }
    }

    if (!matches) {
        *swapped = false;
        if (ownTxn) {
            kvidxLmdbCommit(i); /* Commit even though we didn't change */
        }
        return KVIDX_OK;
    }

    /* Data matches, perform update */
    size_t valLen;
    void *valBuf = packValue(newTerm, newCmd, newData, newDataLen, &valLen);
    if (!valBuf) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_NOMEM;
    }

    mval.mv_size = valLen;
    mval.mv_data = valBuf;
    rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, 0);
    free(valBuf);

    if (rc != MDB_SUCCESS) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_INTERNAL;
    }

    *swapped = true;

    if (ownTxn && !kvidxLmdbCommit(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    return KVIDX_OK;
}

/* --- Append/Prepend --- */

kvidxError kvidxLmdbAppend(kvidxInstance *i, uint64_t key, uint64_t term,
                           uint64_t cmd, const void *data, size_t dataLen,
                           size_t *newLen) {
    lmdbState *s = STATE(i);
    bool ownTxn = (s->writeTxn == NULL);

    if (ownTxn && !kvidxLmdbBegin(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    /* Try to get existing value */
    int rc = mdb_get(s->writeTxn, s->dbi, &mkey, &mval);

    kvidxError result = KVIDX_OK;

    if (rc == MDB_NOTFOUND) {
        /* Key doesn't exist, create new */
        size_t valLen;
        void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
        if (!valBuf) {
            result = KVIDX_ERROR_NOMEM;
        } else {
            mval.mv_size = valLen;
            mval.mv_data = valBuf;
            rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, 0);
            free(valBuf);
            if (rc != MDB_SUCCESS) {
                result = KVIDX_ERROR_INTERNAL;
            } else if (newLen) {
                *newLen = dataLen;
            }
        }
    } else if (rc == MDB_SUCCESS) {
        /* Key exists, append */
        uint64_t existingTerm = extractTerm(&mval);
        uint64_t existingCmd = extractCmd(&mval);
        size_t existingLen;
        const uint8_t *existingData = extractData(&mval, &existingLen);

        size_t totalLen = existingLen + dataLen;
        void *combinedData = malloc(totalLen);
        if (!combinedData) {
            result = KVIDX_ERROR_NOMEM;
        } else {
            if (existingLen > 0 && existingData) {
                memcpy(combinedData, existingData, existingLen);
            }
            if (dataLen > 0 && data) {
                memcpy((uint8_t *)combinedData + existingLen, data, dataLen);
            }

            size_t valLen;
            void *valBuf = packValue(existingTerm, existingCmd, combinedData,
                                     totalLen, &valLen);
            free(combinedData);

            if (!valBuf) {
                result = KVIDX_ERROR_NOMEM;
            } else {
                mval.mv_size = valLen;
                mval.mv_data = valBuf;
                rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, 0);
                free(valBuf);
                if (rc != MDB_SUCCESS) {
                    result = KVIDX_ERROR_INTERNAL;
                } else if (newLen) {
                    *newLen = totalLen;
                }
            }
        }
    } else {
        result = KVIDX_ERROR_INTERNAL;
    }

    if (ownTxn) {
        if (result == KVIDX_OK) {
            if (!kvidxLmdbCommit(i)) {
                return KVIDX_ERROR_INTERNAL;
            }
        } else {
            kvidxLmdbAbort(i);
        }
    }

    return result;
}

kvidxError kvidxLmdbPrepend(kvidxInstance *i, uint64_t key, uint64_t term,
                            uint64_t cmd, const void *data, size_t dataLen,
                            size_t *newLen) {
    lmdbState *s = STATE(i);
    bool ownTxn = (s->writeTxn == NULL);

    if (ownTxn && !kvidxLmdbBegin(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    int rc = mdb_get(s->writeTxn, s->dbi, &mkey, &mval);

    kvidxError result = KVIDX_OK;

    if (rc == MDB_NOTFOUND) {
        /* Key doesn't exist, create new */
        size_t valLen;
        void *valBuf = packValue(term, cmd, data, dataLen, &valLen);
        if (!valBuf) {
            result = KVIDX_ERROR_NOMEM;
        } else {
            mval.mv_size = valLen;
            mval.mv_data = valBuf;
            rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, 0);
            free(valBuf);
            if (rc != MDB_SUCCESS) {
                result = KVIDX_ERROR_INTERNAL;
            } else if (newLen) {
                *newLen = dataLen;
            }
        }
    } else if (rc == MDB_SUCCESS) {
        /* Key exists, prepend */
        uint64_t existingTerm = extractTerm(&mval);
        uint64_t existingCmd = extractCmd(&mval);
        size_t existingLen;
        const uint8_t *existingData = extractData(&mval, &existingLen);

        size_t totalLen = existingLen + dataLen;
        void *combinedData = malloc(totalLen);
        if (!combinedData) {
            result = KVIDX_ERROR_NOMEM;
        } else {
            if (dataLen > 0 && data) {
                memcpy(combinedData, data, dataLen);
            }
            if (existingLen > 0 && existingData) {
                memcpy((uint8_t *)combinedData + dataLen, existingData,
                       existingLen);
            }

            size_t valLen;
            void *valBuf = packValue(existingTerm, existingCmd, combinedData,
                                     totalLen, &valLen);
            free(combinedData);

            if (!valBuf) {
                result = KVIDX_ERROR_NOMEM;
            } else {
                mval.mv_size = valLen;
                mval.mv_data = valBuf;
                rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, 0);
                free(valBuf);
                if (rc != MDB_SUCCESS) {
                    result = KVIDX_ERROR_INTERNAL;
                } else if (newLen) {
                    *newLen = totalLen;
                }
            }
        }
    } else {
        result = KVIDX_ERROR_INTERNAL;
    }

    if (ownTxn) {
        if (result == KVIDX_OK) {
            if (!kvidxLmdbCommit(i)) {
                return KVIDX_ERROR_INTERNAL;
            }
        } else {
            kvidxLmdbAbort(i);
        }
    }

    return result;
}

/* --- Partial Value Access --- */

kvidxError kvidxLmdbGetValueRange(kvidxInstance *i, uint64_t key, size_t offset,
                                  size_t length, void **data,
                                  size_t *actualLen) {
    /* Initialize outputs */
    if (data) {
        *data = NULL;
    }
    if (actualLen) {
        *actualLen = 0;
    }

    if (!ensureReadTxn(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    lmdbState *s = STATE(i);
    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    int rc = mdb_get(getActiveTxn(i), s->dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        resetReadTxn(i);
        return KVIDX_ERROR_NOT_FOUND;
    }
    if (rc != MDB_SUCCESS) {
        resetReadTxn(i);
        return KVIDX_ERROR_INTERNAL;
    }

    size_t currentLen;
    const uint8_t *currentData = extractData(&mval, &currentLen);

    /* Check if offset is valid */
    if (offset >= currentLen) {
        resetReadTxn(i);
        if (actualLen) {
            *actualLen = 0;
        }
        return KVIDX_OK;
    }

    /* Calculate bytes to return (length=0 means read to end) */
    size_t available = currentLen - offset;
    size_t toReturn = (length == 0 || length > available) ? available : length;

    if (data && toReturn > 0) {
        *data = malloc(toReturn);
        if (!*data) {
            resetReadTxn(i);
            return KVIDX_ERROR_NOMEM;
        }
        memcpy(*data, currentData + offset, toReturn);
    }

    if (actualLen) {
        *actualLen = toReturn;
    }
    resetReadTxn(i);
    return KVIDX_OK;
}

kvidxError kvidxLmdbSetValueRange(kvidxInstance *i, uint64_t key, size_t offset,
                                  const void *data, size_t dataLen,
                                  size_t *newLen) {
    lmdbState *s = STATE(i);
    bool ownTxn = (s->writeTxn == NULL);

    if (ownTxn && !kvidxLmdbBegin(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    /* Get current value */
    int rc = mdb_get(s->writeTxn, s->dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_NOT_FOUND;
    }
    if (rc != MDB_SUCCESS) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_INTERNAL;
    }

    uint64_t existingTerm = extractTerm(&mval);
    uint64_t existingCmd = extractCmd(&mval);
    size_t currentLen;
    const uint8_t *currentData = extractData(&mval, &currentLen);

    /* Calculate new size */
    size_t newSize = offset + dataLen;
    if (newSize < currentLen) {
        newSize = currentLen;
    }

    /* Allocate new buffer */
    void *newData = calloc(1, newSize);
    if (!newData && newSize > 0) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_NOMEM;
    }

    /* Copy existing data */
    if (currentLen > 0 && currentData) {
        memcpy(newData, currentData, currentLen);
    }

    /* Write new data at offset */
    if (dataLen > 0 && data) {
        memcpy((uint8_t *)newData + offset, data, dataLen);
    }

    /* Pack and store */
    size_t valLen;
    void *valBuf =
        packValue(existingTerm, existingCmd, newData, newSize, &valLen);
    free(newData);

    if (!valBuf) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_NOMEM;
    }

    mval.mv_size = valLen;
    mval.mv_data = valBuf;
    rc = mdb_put(s->writeTxn, s->dbi, &mkey, &mval, 0);
    free(valBuf);

    if (rc != MDB_SUCCESS) {
        if (ownTxn) {
            kvidxLmdbAbort(i);
        }
        return KVIDX_ERROR_INTERNAL;
    }

    if (newLen) {
        *newLen = newSize;
    }

    if (ownTxn && !kvidxLmdbCommit(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    return KVIDX_OK;
}

/* --- TTL/Expiration --- */

kvidxError kvidxLmdbSetExpire(kvidxInstance *i, uint64_t key, uint64_t ttlMs) {
    lmdbState *s = STATE(i);

    /* Check if key exists */
    if (!kvidxLmdbExists(i, key)) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Ensure TTL db is open */
    if (!ensureTTLDb(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    uint64_t expiresAt = currentTimeMsLmdb() + ttlMs;

    /* Start transaction */
    MDB_txn *txn;
    int rc = mdb_txn_begin(s->env, NULL, 0, &txn);
    if (rc != MDB_SUCCESS) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval = {.mv_size = sizeof(expiresAt), .mv_data = &expiresAt};

    rc = mdb_put(txn, s->ttlDbi, &mkey, &mval, 0);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        return KVIDX_ERROR_INTERNAL;
    }

    rc = mdb_txn_commit(txn);
    return (rc == MDB_SUCCESS) ? KVIDX_OK : KVIDX_ERROR_INTERNAL;
}

kvidxError kvidxLmdbSetExpireAt(kvidxInstance *i, uint64_t key,
                                uint64_t timestampMs) {
    lmdbState *s = STATE(i);

    if (!kvidxLmdbExists(i, key)) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    if (!ensureTTLDb(i)) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_txn *txn;
    int rc = mdb_txn_begin(s->env, NULL, 0, &txn);
    if (rc != MDB_SUCCESS) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval = {.mv_size = sizeof(timestampMs), .mv_data = &timestampMs};

    rc = mdb_put(txn, s->ttlDbi, &mkey, &mval, 0);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        return KVIDX_ERROR_INTERNAL;
    }

    rc = mdb_txn_commit(txn);
    return (rc == MDB_SUCCESS) ? KVIDX_OK : KVIDX_ERROR_INTERNAL;
}

int64_t kvidxLmdbGetTTL(kvidxInstance *i, uint64_t key) {
    lmdbState *s = STATE(i);

    if (!kvidxLmdbExists(i, key)) {
        return KVIDX_TTL_NOT_FOUND;
    }

    if (!ensureTTLDb(i)) {
        return KVIDX_TTL_NONE;
    }

    MDB_txn *txn;
    int rc = mdb_txn_begin(s->env, NULL, MDB_RDONLY, &txn);
    if (rc != MDB_SUCCESS) {
        return KVIDX_TTL_NONE;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    MDB_val mval;

    rc = mdb_get(txn, s->ttlDbi, &mkey, &mval);
    mdb_txn_abort(txn);

    if (rc == MDB_NOTFOUND) {
        return KVIDX_TTL_NONE;
    }
    if (rc != MDB_SUCCESS) {
        return KVIDX_TTL_NONE;
    }

    uint64_t expiresAt;
    memcpy(&expiresAt, mval.mv_data, sizeof(expiresAt));
    uint64_t now = currentTimeMsLmdb();

    if (expiresAt <= now) {
        return 0; /* Already expired */
    }
    return (int64_t)(expiresAt - now);
}

kvidxError kvidxLmdbPersist(kvidxInstance *i, uint64_t key) {
    lmdbState *s = STATE(i);

    if (!kvidxLmdbExists(i, key)) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    if (!ensureTTLDb(i)) {
        return KVIDX_OK; /* No TTL db means already persistent */
    }

    MDB_txn *txn;
    int rc = mdb_txn_begin(s->env, NULL, 0, &txn);
    if (rc != MDB_SUCCESS) {
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_val mkey = {.mv_size = sizeof(key), .mv_data = &key};
    rc = mdb_del(txn, s->ttlDbi, &mkey, NULL);

    if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return KVIDX_ERROR_INTERNAL;
    }

    rc = mdb_txn_commit(txn);
    return (rc == MDB_SUCCESS) ? KVIDX_OK : KVIDX_ERROR_INTERNAL;
}

kvidxError kvidxLmdbExpireScan(kvidxInstance *i, uint64_t maxKeys,
                               uint64_t *expiredCount) {
    lmdbState *s = STATE(i);
    uint64_t expired = 0;

    if (!ensureTTLDb(i)) {
        if (expiredCount) {
            *expiredCount = 0;
        }
        return KVIDX_OK;
    }

    uint64_t now = currentTimeMsLmdb();

    /* Scan TTL db for expired keys */
    MDB_txn *txn;
    int rc = mdb_txn_begin(s->env, NULL, 0, &txn);
    if (rc != MDB_SUCCESS) {
        if (expiredCount) {
            *expiredCount = 0;
        }
        return KVIDX_ERROR_INTERNAL;
    }

    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, s->ttlDbi, &cursor);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        if (expiredCount) {
            *expiredCount = 0;
        }
        return KVIDX_ERROR_INTERNAL;
    }

    /* Collect expired keys */
    MDB_val mkey, mval;
    uint64_t *keysToDelete = NULL;
    size_t keyCount = 0;
    size_t keyCapacity = 64;
    keysToDelete = malloc(keyCapacity * sizeof(uint64_t));
    if (!keysToDelete) {
        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        return KVIDX_ERROR_NOMEM;
    }

    rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_FIRST);
    while (rc == MDB_SUCCESS) {
        if (maxKeys > 0 && keyCount >= maxKeys) {
            break;
        }

        uint64_t expiresAt;
        memcpy(&expiresAt, mval.mv_data, sizeof(expiresAt));

        if (expiresAt <= now) {
            /* Expired - add to delete list */
            if (keyCount >= keyCapacity) {
                keyCapacity *= 2;
                uint64_t *newKeys =
                    realloc(keysToDelete, keyCapacity * sizeof(uint64_t));
                if (!newKeys) {
                    free(keysToDelete);
                    mdb_cursor_close(cursor);
                    mdb_txn_abort(txn);
                    return KVIDX_ERROR_NOMEM;
                }
                keysToDelete = newKeys;
            }
            uint64_t key;
            memcpy(&key, mkey.mv_data, sizeof(key));
            keysToDelete[keyCount++] = key;
        }

        rc = mdb_cursor_get(cursor, &mkey, &mval, MDB_NEXT);
    }

    mdb_cursor_close(cursor);

    /* Delete expired keys */
    for (size_t idx = 0; idx < keyCount; idx++) {
        uint64_t key = keysToDelete[idx];

        /* Delete from main db */
        MDB_val delKey = {.mv_size = sizeof(key), .mv_data = &key};
        mdb_del(txn, s->dbi, &delKey, NULL);

        /* Delete from TTL db */
        mdb_del(txn, s->ttlDbi, &delKey, NULL);

        expired++;
    }

    free(keysToDelete);

    rc = mdb_txn_commit(txn);
    if (expiredCount) {
        *expiredCount = expired;
    }

    return (rc == MDB_SUCCESS) ? KVIDX_OK : KVIDX_ERROR_INTERNAL;
}
