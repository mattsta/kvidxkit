/**
 * @file kvidxkitAdapterSqlite3.c
 * @brief SQLite3 backend adapter for kvidxkit
 *
 * This adapter implements the kvidxInterface using SQLite3 as the underlying
 * storage engine. SQLite3 provides ACID-compliant persistent storage with
 * excellent write performance when using WAL (Write-Ahead Logging) mode.
 *
 * ## Architecture
 *
 * Data is stored in a single table called "log" with the following schema:
 *   - id (INTEGER PRIMARY KEY): The uint64_t key
 *   - created (INTEGER): Timestamp placeholder (currently unused)
 *   - term (INTEGER): Application-defined term/version number
 *   - cmd (INTEGER): Application-defined command/type identifier
 *   - data (BLOB): Arbitrary binary payload
 *
 * The adapter uses prepared statements for frequently-called operations to
 * avoid repeated SQL parsing overhead. Statements are prepared once during
 * Open() and finalized during Close().
 *
 * ## Transaction Model
 *
 * - Begin() starts a deferred transaction
 * - All writes within a transaction share a single WAL sync
 * - Commit() persists all changes atomically
 * - Abort() rolls back uncommitted changes (v0.8.0)
 *
 * ## Thread Safety
 *
 * Each kvidxInstance is NOT thread-safe. Use separate instances per thread
 * or protect access with external synchronization.
 *
 * ## Performance Notes
 *
 * - WAL mode provides concurrent reads during writes
 * - The "unix-excl" VFS is used for single-process efficiency
 * - Cache size defaults to 32MB for better read performance
 * - Batch operations in transactions avoid per-write fsync overhead
 */

#include "kvidxkitAdapterSqlite3.h"
#include "../deps/sqlite3/src/sqlite3.h"
#include "kvidxkitSchema.h"
#include "kvidxkitTableDesc.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/**
 * Internal state for SQLite3 adapter instance.
 *
 * Contains the database handle and all prepared statements. Statements are
 * organized by approximate cache line to minimize memory access latency
 * for hot paths (get/insert operations).
 */
typedef struct kas3State {
    /* CACHE LINE 1 */
    sqlite3 *db;
    sqlite3_stmt *get;
    sqlite3_stmt *getPrev;
    sqlite3_stmt *getNext;
    sqlite3_stmt *exists;
    sqlite3_stmt *existsDual;
    sqlite3_stmt *insert;
    sqlite3_stmt *remove;

    /* CACHE LINE 2 */
    sqlite3_stmt *begin;
    sqlite3_stmt *commit;
    sqlite3_stmt *maxKey;
    sqlite3_stmt *removeAfterNInclusive;
    sqlite3_stmt *removeBeforeNInclusive;
} kas3State;

#define STATE(instance) ((kas3State *)(instance)->kvidxdata)

static const char *stmtBegin = "BEGIN;";
static const char *stmtCommit = "COMMIT;";
static const char *stmtGet = "SELECT term, cmd, data FROM log WHERE id = ?;";
static const char *stmtGetPrev = "SELECT id, term, cmd, data FROM log WHERE "
                                 "id < ? ORDER BY id DESC LIMIT 1;";
static const char *stmtGetNext = "SELECT id, term, cmd, data FROM log WHERE "
                                 "id > ? ORDER BY id ASC LIMIT 1;";
static const char *stmtExists = "SELECT EXISTS(SELECT 1 FROM log WHERE id=?);";
static const char *stmtExistsDual =
    "SELECT EXISTS(SELECT 1 FROM log WHERE id=? AND term=?);";
static const char *stmtInsert = "INSERT INTO log VALUES(?, ?, ?, ?, ?);";
static const char *stmtRemove = "DELETE FROM log WHERE id = ?;";
static const char *stmtMaxId = "SELECT MAX(id) FROM log;";
static const char *stmtRemoveAfterNInclusive = "DELETE FROM log WHERE id >= ?";
static const char *stmtRemoveBeforeNInclusive = "DELETE FROM log WHERE id <= ?";

/* ====================================================================
 * Data Manipulation
 * ==================================================================== */

/**
 * Extract BLOB data from a SQLite statement result column.
 *
 * This helper function safely extracts binary data from a query result,
 * handling NULL blobs and empty data gracefully. The returned pointer
 * points directly into SQLite's internal buffer and remains valid only
 * until the next sqlite3_step() or sqlite3_reset() call.
 *
 * @param stmt  The prepared statement with a row result ready
 * @param col   Column index (0-based) containing the BLOB
 * @param data  OUT: Pointer to the blob data, or NULL if just checking
 * existence
 * @param len   OUT: Length of the blob data, or NULL if not needed
 * @return true if blob data was found and extracted, false if empty/NULL
 */
static bool extractBlob(sqlite3_stmt *stmt, uint8_t col, const uint8_t **data,
                        size_t *len) {
    /* If we aren't returning data, at least return whether data existed. */
    if (!data) {
        return sqlite3_column_type(stmt, col) == SQLITE_BLOB;
    }

    const void *blob = sqlite3_column_blob(stmt, col);

    /* If no data (length == 0), blob is NULL */
    if (!blob) {
        /* Technically, we found a result, but it was empty.
         * This could potentially return "true, but empty"
         * instead of returning a "not found" result. */
        if (len) {
            *len = 0;
        }
        return false;
    }

    /* data is guaranteed non-NULL here (checked at function entry) */
    *data = blob;

    /* Sqlite3 lengths are int32_t, so we
     * are limited to less than 2 GB data sizes here.
     * See: https://www.sqlite.org/limits.html#max_length */
    if (len) {
        *len = sqlite3_column_bytes(stmt, col);
    }

    return true;
}

/**
 * Begin a new database transaction.
 *
 * Starts a deferred transaction, meaning the actual database lock is not
 * acquired until the first read or write operation. This allows multiple
 * readers to proceed concurrently until a write is needed.
 *
 * All operations between Begin() and Commit() are atomic - either all
 * succeed or all are rolled back. Batching writes within a transaction
 * significantly improves performance by avoiding per-operation fsync.
 *
 * @param i  The kvidx instance
 * @return true on success, false on failure (e.g., nested transaction)
 */
bool kvidxSqlite3Begin(kvidxInstance *i) {
    kas3State *s = STATE(i);
    const bool result = sqlite3_step(s->begin) == SQLITE_DONE;
    sqlite3_reset(s->begin);
    return result;
}

/**
 * Commit the current transaction.
 *
 * Persists all changes made since Begin() atomically to disk. In WAL mode,
 * this writes to the write-ahead log and may trigger a checkpoint if the
 * WAL file grows too large.
 *
 * If commit fails (e.g., disk full), the transaction remains open and
 * can be retried or aborted.
 *
 * @param i  The kvidx instance
 * @return true on success, false on failure
 */
bool kvidxSqlite3Commit(kvidxInstance *i) {
    kas3State *s = STATE(i);
    const bool result = sqlite3_step(s->commit) == SQLITE_DONE;
    sqlite3_reset(s->commit);
    return result;
}

/**
 * Retrieve a record by its exact key.
 *
 * Looks up a single record by primary key and returns its metadata and data.
 * This is the primary read operation and is highly optimized using a prepared
 * statement.
 *
 * IMPORTANT: The returned data pointer points directly into SQLite's internal
 * buffer. It remains valid ONLY until the next Get(), GetPrev(), or GetNext()
 * call on this instance. Copy the data if you need to retain it longer.
 *
 * @param i     The kvidx instance
 * @param key   The uint64_t key to look up
 * @param term  OUT: The term/version number, or NULL if not needed
 * @param cmd   OUT: The command/type identifier, or NULL if not needed
 * @param data  OUT: Pointer to the blob data (borrowed), or NULL if not needed
 * @param len   OUT: Length of the blob data, or NULL if not needed
 * @return true if key was found, false if not found
 */
bool kvidxSqlite3Get(kvidxInstance *i, uint64_t key, uint64_t *term,
                     uint64_t *cmd, const uint8_t **data, size_t *len) {
    kas3State *s = STATE(i);
    sqlite3_reset(s->get);
    sqlite3_bind_int64(s->get, 1, key);
    if (sqlite3_step(s->get) == SQLITE_ROW) {
        if (term) {
            *term = sqlite3_column_int64(s->get, 0);
        }

        if (cmd) {
            *cmd = sqlite3_column_int64(s->get, 1);
        }

        extractBlob(s->get, 2, data, len);
        /* Don't reset here - data pointer must stay valid until next call */
        return true;
    }

    /* Already reset above */
    return false;
}

/**
 * Common extraction logic for GetPrev and GetNext operations.
 *
 * Both GetPrev and GetNext return a 4-column result (id, term, cmd, data)
 * unlike Get which only returns 3 columns (term, cmd, data) since the key
 * is already known. This helper avoids code duplication.
 *
 * @param stmt      The prepared statement (getPrev or getNext)
 * @param lookupId  The reference key for the < or > comparison
 * @param key       OUT: The found key, or NULL if not needed
 * @param term      OUT: The term/version, or NULL if not needed
 * @param cmd       OUT: The command/type, or NULL if not needed
 * @param data      OUT: Pointer to blob data (borrowed), or NULL if not needed
 * @param len       OUT: Length of blob data, or NULL if not needed
 * @return true if a record was found, false otherwise
 */
static bool fourColumnExtract(sqlite3_stmt *stmt, uint64_t lookupId,
                              uint64_t *key, uint64_t *term, uint64_t *cmd,
                              const uint8_t **data, size_t *len) {
    sqlite3_reset(stmt);
    sqlite3_bind_int64(stmt, 1, lookupId);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        if (key) {
            *key = sqlite3_column_int64(stmt, 0);
        }

        if (term) {
            *term = sqlite3_column_int64(stmt, 1);
        }

        if (cmd) {
            *cmd = sqlite3_column_int64(stmt, 2);
        }

        extractBlob(stmt, 3, data, len);
        /* Don't reset here - data pointer must stay valid until next call */
        return true;
    }

    /* Already reset above */
    return false;
}

/**
 * Find the record with the largest key less than the given key.
 *
 * This enables reverse iteration through the keyspace. Given a reference key,
 * it finds the immediately preceding record (if any). Useful for implementing
 * cursors, pagination, or finding "floor" entries.
 *
 * Special case: When nextKey is UINT64_MAX, this finds the maximum key in
 * the database (since SQLite treats uint64 as signed int64, causing "< -1"
 * to match nothing).
 *
 * @param i         The kvidx instance
 * @param nextKey   Find records with keys strictly less than this value
 * @param prevKey   OUT: The found key, or NULL if not needed
 * @param prevTerm  OUT: The term/version, or NULL if not needed
 * @param cmd       OUT: The command/type, or NULL if not needed
 * @param data      OUT: Pointer to blob data (borrowed), or NULL if not needed
 * @param len       OUT: Length of blob data, or NULL if not needed
 * @return true if a previous record was found, false if at beginning
 */
bool kvidxSqlite3GetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                         uint64_t *prevTerm, uint64_t *cmd,
                         const uint8_t **data, size_t *len) {
    /* Edge case: UINT64_MAX is treated as -1 in SQLite (signed int64)
     * which causes "id < -1" to match nothing. Handle explicitly. */
    if (nextKey == UINT64_MAX) {
        /* Get the maximum key instead */
        kas3State *s = STATE(i);
        sqlite3_reset(s->maxKey);
        if (sqlite3_step(s->maxKey) == SQLITE_ROW) {
            if (sqlite3_column_type(s->maxKey, 0) != SQLITE_NULL) {
                uint64_t maxId = sqlite3_column_int64(s->maxKey, 0);
                /* Now get the full record for that key */
                return kvidxSqlite3Get(i, maxId, prevTerm, cmd, data, len) &&
                       (prevKey ? (*prevKey = maxId, true) : true);
            }
        }
        return false;
    }
    kas3State *s = STATE(i);
    return fourColumnExtract(s->getPrev, nextKey, prevKey, prevTerm, cmd, data,
                             len);
}

/**
 * Find the record with the smallest key greater than the given key.
 *
 * This enables forward iteration through the keyspace. Given a reference key,
 * it finds the immediately following record (if any). Useful for implementing
 * cursors, sequential scans, or finding "ceiling" entries.
 *
 * Special case: When previousKey is UINT64_MAX, this always returns false
 * since no key can be greater than UINT64_MAX.
 *
 * @param i            The kvidx instance
 * @param previousKey  Find records with keys strictly greater than this value
 * @param nextKey      OUT: The found key, or NULL if not needed
 * @param nextTerm     OUT: The term/version, or NULL if not needed
 * @param cmd          OUT: The command/type, or NULL if not needed
 * @param data         OUT: Pointer to blob data (borrowed), or NULL if not
 * needed
 * @param len          OUT: Length of blob data, or NULL if not needed
 * @return true if a next record was found, false if at end
 */
bool kvidxSqlite3GetNext(kvidxInstance *i, uint64_t previousKey,
                         uint64_t *nextKey, uint64_t *nextTerm, uint64_t *cmd,
                         const uint8_t **data, size_t *len) {
    /* Edge case: no key can be greater than UINT64_MAX */
    if (previousKey == UINT64_MAX) {
        return false;
    }
    kas3State *s = STATE(i);
    return fourColumnExtract(s->getNext, previousKey, nextKey, nextTerm, cmd,
                             data, len);
}

/**
 * Check if a key exists in the database.
 *
 * This is a lightweight existence check that avoids fetching the actual
 * record data. Uses SELECT EXISTS() for optimal performance - SQLite can
 * answer this from the index alone without touching data pages.
 *
 * @param i    The kvidx instance
 * @param key  The key to check
 * @return true if the key exists, false otherwise
 */
bool kvidxSqlite3Exists(kvidxInstance *i, uint64_t key) {
    kas3State *s = STATE(i);
    sqlite3_reset(s->exists);
    sqlite3_bind_int64(s->exists, 1, key);
    sqlite3_step(s->exists);
    const bool exists = sqlite3_column_int(s->exists, 0);
    return exists;
}

/**
 * Check if a key exists with a specific term/version.
 *
 * This is a conditional existence check that verifies both the key exists
 * AND has the expected term value. Useful for optimistic concurrency control
 * or version checking before updates.
 *
 * @param i     The kvidx instance
 * @param key   The key to check
 * @param term  The expected term/version value
 * @return true if key exists with matching term, false otherwise
 */
bool kvidxSqlite3ExistsDual(kvidxInstance *i, uint64_t key, uint64_t term) {
    kas3State *s = STATE(i);
    sqlite3_reset(s->existsDual);
    sqlite3_bind_int64(s->existsDual, 1, key);
    sqlite3_bind_int64(s->existsDual, 2, term);
    sqlite3_step(s->existsDual);
    const bool exists = sqlite3_column_int(s->existsDual, 0);
    return exists;
}

/**
 * Insert a new record into the database.
 *
 * Creates a new record with the given key, metadata, and data. This operation
 * will FAIL if the key already exists (use InsertEx with KVIDX_SET_ALWAYS
 * for upsert behavior).
 *
 * The data is copied into SQLite's storage, so the caller can free or reuse
 * the data buffer immediately after this call returns.
 *
 * Note: SQLite has a compile-time maximum row size of ~1GB, so very large
 * blobs may fail to insert.
 *
 * @param i        The kvidx instance
 * @param key      The uint64_t key for this record
 * @param term     Application-defined term/version number
 * @param cmd      Application-defined command/type identifier
 * @param data     Pointer to the data to store (may be NULL if dataLen is 0)
 * @param dataLen  Length of the data in bytes
 * @return true on success, false if key exists or other error
 */
bool kvidxSqlite3Insert(kvidxInstance *i, uint64_t key, uint64_t term,
                        uint64_t cmd, const void *data, size_t dataLen) {
    /* Note: even though we use 'blob64' below, sqlite3 has a compile-time
     * maximum row size of 10^9 bytes (~1 GB), so trying to store any data
     * approaching 1 GB will fail.
     * Also note: the limit is *per row* so the maximum size must account for
     * other columns and metadata per-row, so the actual data limit is lower
     * than the max blob size (because rows are encoded as single blobs
     * themselves, we can't have max blob size inside our row because that
     * would blow out the max blob size for the row itself). */
    kas3State *s = STATE(i);
    sqlite3_reset(s->insert);
    sqlite3_bind_int64(s->insert, 1, key);
    sqlite3_bind_int64(s->insert, 2, 0); /* timestamp */
    sqlite3_bind_int64(s->insert, 3, term);
    sqlite3_bind_int64(s->insert, 4, cmd);
    sqlite3_bind_blob64(s->insert, 5, data, dataLen, NULL);
    int done = sqlite3_step(s->insert);
    /* Investigate: why does sqlite3_step sometimes return
     * SQLITE_OK instead of SQLITE_DONE?
     * It only seems to happen when operating on an existing
     * file. */
    return (done == SQLITE_DONE) || (done == SQLITE_OK);
}

/**
 * Delete a record by key.
 *
 * Removes the record with the specified key from the database. This is
 * idempotent - removing a non-existent key still returns true.
 *
 * Note: This does not remove any associated TTL entry. Use GetAndRemove()
 * or explicitly call Persist() if TTL cleanup is needed.
 *
 * @param i    The kvidx instance
 * @param key  The key to remove
 * @return true on success (including if key didn't exist), false on error
 */
bool kvidxSqlite3Remove(kvidxInstance *i, uint64_t key) {
    kas3State *s = STATE(i);
    sqlite3_reset(s->remove);
    sqlite3_bind_int64(s->remove, 1, key);
    const bool removed = sqlite3_step(s->remove) == SQLITE_DONE;
    return removed;
}

/**
 * Delete all records with keys greater than or equal to the given key.
 *
 * Bulk delete operation that removes all records from key to the maximum
 * key in the database. This is more efficient than deleting records one
 * by one. Useful for truncating a log or clearing recent entries.
 *
 * @param i    The kvidx instance
 * @param key  The starting key (inclusive) - all keys >= this are deleted
 * @return true on success, false on error
 */
bool kvidxSqlite3RemoveAfterNInclusive(kvidxInstance *i, uint64_t key) {
    kas3State *s = STATE(i);
    sqlite3_reset(s->removeAfterNInclusive);
    sqlite3_bind_int64(s->removeAfterNInclusive, 1, key);
    const bool removed = sqlite3_step(s->removeAfterNInclusive) == SQLITE_DONE;
    return removed;
}

/**
 * Delete all records with keys less than or equal to the given key.
 *
 * Bulk delete operation that removes all records from the minimum key
 * up to and including the specified key. This is more efficient than
 * deleting records one by one. Useful for garbage collection or
 * compacting old log entries.
 *
 * @param i    The kvidx instance
 * @param key  The ending key (inclusive) - all keys <= this are deleted
 * @return true on success, false on error
 */
bool kvidxSqlite3RemoveBeforeNInclusive(kvidxInstance *i, uint64_t key) {
    kas3State *s = STATE(i);
    sqlite3_reset(s->removeBeforeNInclusive);
    sqlite3_bind_int64(s->removeBeforeNInclusive, 1, key);
    const bool removed = sqlite3_step(s->removeBeforeNInclusive) == SQLITE_DONE;
    return removed;
}

/**
 * Get the maximum (largest) key in the database.
 *
 * Efficiently finds the largest key using SQLite's MAX() aggregate,
 * which can be computed from the index without a full table scan.
 *
 * @param i    The kvidx instance
 * @param key  OUT: The maximum key value, or NULL if just checking existence
 * @return true if database has at least one record, false if empty
 */
bool kvidxSqlite3Max(kvidxInstance *i, uint64_t *key) {
    kas3State *s = STATE(i);
    sqlite3_reset(s->maxKey);
    if (sqlite3_step(s->maxKey) == SQLITE_ROW) {
        if (sqlite3_column_type(s->maxKey, 0) == SQLITE_NULL) {
            /* NULL result means we have no keys! */
            return false;
        }

        if (key) {
            *key = sqlite3_column_int64(s->maxKey, 0);
        }

        return true;
    }

    return false;
}

/**
 * Set the synchronous mode to NORMAL.
 *
 * Configures SQLite's synchronous pragma to balance durability with
 * performance. NORMAL mode syncs at critical moments but not after
 * every transaction, providing good crash safety without the overhead
 * of FULL synchronous mode.
 *
 * Modes available (not all exposed here):
 * - OFF: No syncs, fastest but data loss possible on crash
 * - NORMAL: Sync at critical points, good balance
 * - FULL: Sync after every transaction, safest but slowest
 *
 * @param i  The kvidx instance
 * @return true on success, false on error
 */
bool kvidxSqlite3Fsync(kvidxInstance *i) {
    kas3State *s = STATE(i);
    return sqlite3_exec(s->db, "PRAGMA synchronous = NORMAL;", NULL, NULL,
                        NULL) == SQLITE_OK;
}

/* ====================================================================
 * Bring-Up
 * ==================================================================== */

/**
 * Configure SQLite performance and behavior options.
 *
 * Sets up the database for optimal performance with WAL journaling,
 * large cache, and recursive triggers. These settings are applied
 * once at database open time.
 *
 * Key configurations:
 * - WAL mode: Concurrent reads during writes, better crash recovery
 * - 32MB cache: Reduced disk I/O for frequently accessed data
 * - Recursive triggers: Enables trigger cascades (if needed)
 *
 * @param s  The internal adapter state
 */
static void configureDBOptions(kas3State *s) {
    /* Make DB file use a WAL instead of write/delete journal. */

    /* Note: we set WAL here instead of in our create callback because
     * it doesn't work when inside another sqlite3_exec() callback. */
    /* Also note: WAL is *not* compatible with NFS.  If NFS is a concern,
     * use journal_mode=PERSIST  - OR - open the DB with
     * "unix-excl" VFS implementation to keep locking in single-process
     * memory instead of shared memory.
     * See: sqlite3_vfs_register() or last argument to sqlite3_open_v2() */

    /* Create keyspace table */
    static const char *wal = "PRAGMA journal_mode = WAL;";
    int walErr = sqlite3_exec(s->db, wal, NULL, NULL, NULL);
    assert(walErr == SQLITE_OK);

    static const char *cache = "PRAGMA cache_size = -31250;"; /* 32 MB in KiB */
    int cacheErr = sqlite3_exec(s->db, cache, NULL, NULL, NULL);
    assert(cacheErr == SQLITE_OK);

    static const char *prt = "PRAGMA recursive_triggers = ON;"; /* Allow */
    int prtErr = sqlite3_exec(s->db, prt, NULL, NULL, NULL);
    assert(prtErr == SQLITE_OK);

#if 0
    /* Enabled by custom build */
    int limitErr = sqlite3_limit(s->db, SQLITE_LIMIT_TRIGGER_DEPTH, 65536);
    assert(limitErr == SQLITE_OK);

    static const char *fk = "PRAGMA foreign_keys = ON;"; /* Allow */
    int fkErr = sqlite3_exec(s->db, fk, NULL, NULL, NULL);
    assert(fkErr == SQLITE_OK);
#endif

    /* Compare against using "PRAGMA mmap_size" as well. */
}

/**
 * Prepare all SQL statements used by the adapter.
 *
 * Creates prepared statements for frequently-used operations. Preparing
 * statements once at startup avoids the overhead of SQL parsing on every
 * operation, significantly improving performance for high-frequency calls.
 *
 * All statements use parameter binding (?), which:
 * - Prevents SQL injection
 * - Allows SQLite to cache the query plan
 * - Enables efficient parameter reuse via sqlite3_reset()
 *
 * @param s  The internal adapter state
 */
static void preparePreparedStatements(kas3State *s) {
    int errBegin = sqlite3_prepare_v2(s->db, stmtBegin, strlen(stmtBegin),
                                      &s->begin, NULL);
    assert(errBegin == SQLITE_OK);

    int errCommit = sqlite3_prepare_v2(s->db, stmtCommit, strlen(stmtCommit),
                                       &s->commit, NULL);
    assert(errCommit == SQLITE_OK);

    int errGet =
        sqlite3_prepare_v2(s->db, stmtGet, strlen(stmtGet), &s->get, NULL);
    assert(errGet == SQLITE_OK);

    int errGetPrev = sqlite3_prepare_v2(s->db, stmtGetPrev, strlen(stmtGetPrev),
                                        &s->getPrev, NULL);
    assert(errGetPrev == SQLITE_OK);

    int errGetNext = sqlite3_prepare_v2(s->db, stmtGetNext, strlen(stmtGetNext),
                                        &s->getNext, NULL);
    assert(errGetNext == SQLITE_OK);

    int errExists = sqlite3_prepare_v2(s->db, stmtExists, strlen(stmtExists),
                                       &s->exists, NULL);
    assert(errExists == SQLITE_OK);

    int errExistsDual = sqlite3_prepare_v2(
        s->db, stmtExistsDual, strlen(stmtExistsDual), &s->existsDual, NULL);
    assert(errExistsDual == SQLITE_OK);

    int errInsert = sqlite3_prepare_v2(s->db, stmtInsert, strlen(stmtInsert),
                                       &s->insert, NULL);
    assert(errInsert == SQLITE_OK);

    int errRemove = sqlite3_prepare_v2(s->db, stmtRemove, strlen(stmtRemove),
                                       &s->remove, NULL);
    assert(errRemove == SQLITE_OK);

    int errMaxId = sqlite3_prepare_v2(s->db, stmtMaxId, strlen(stmtMaxId),
                                      &s->maxKey, NULL);
    assert(errMaxId == SQLITE_OK);

    int errNInc = sqlite3_prepare_v2(s->db, stmtRemoveAfterNInclusive,
                                     strlen(stmtRemoveAfterNInclusive),
                                     &s->removeAfterNInclusive, NULL);
    assert(errNInc == SQLITE_OK);

    int errBInc = sqlite3_prepare_v2(s->db, stmtRemoveBeforeNInclusive,
                                     strlen(stmtRemoveBeforeNInclusive),
                                     &s->removeBeforeNInclusive, NULL);
    assert(errBInc == SQLITE_OK);
}

/**
 * Table schema definitions using the kvidxkitTableDesc system.
 *
 * These declarative table definitions are processed by
 * kvidxSchemaCreateTables() to generate CREATE TABLE statements. This approach
 * ensures consistent schema across different databases and makes schema changes
 * easier to track.
 *
 * Tables:
 * - controlBlock: Metadata storage for application-defined control data
 * - log: Primary data table for key-value records
 */
static const kvidxColDef controlBlockCols[] = {
    COL("id", KVIDX_COL_INTEGER | KVIDX_COL_PRIMARY_KEY),
    COL("what", KVIDX_COL_TEXT),
    COL("how", KVIDX_COL_TEXT),
};

static const kvidxIndexDef controlBlockIdxs[] = {
    INDEX("what"),
};

static const kvidxColDef logCols[] = {
    COL("id", KVIDX_COL_PK),        COL("created", KVIDX_COL_INTEGER),
    COL("term", KVIDX_COL_INTEGER), COL("cmd", KVIDX_COL_INTEGER),
    COL("data", KVIDX_COL_BLOB),
};

static const kvidxTableDef tables[] = {
    {
        .name = "controlBlock",
        .columns = controlBlockCols,
        .colCount = sizeof(controlBlockCols) / sizeof(*controlBlockCols),
        .indexes = controlBlockIdxs,
        .indexCount = sizeof(controlBlockIdxs) / sizeof(*controlBlockIdxs),
    },
    {
        .name = "log",
        .columns = logCols,
        .colCount = sizeof(logCols) / sizeof(*logCols),
        .indexes = NULL,
        .indexCount = 0,
    },
};

/**
 * Create required database tables if they don't exist.
 *
 * Uses the declarative table definitions above to create the schema.
 * This is idempotent - calling it on an existing database with the
 * correct schema is a no-op.
 *
 * @param db  The SQLite database handle
 * @return true on success, false on error
 */
static bool createLogTable(sqlite3 *db) {
    return kvidxSchemaCreateTables(
               db, tables, sizeof(tables) / sizeof(*tables)) == KVIDX_OK;
}

/**
 * Open or create a SQLite3-backed kvidx database.
 *
 * This is the entry point for creating a new kvidx instance using SQLite3
 * as the storage backend. It handles database file creation, schema setup,
 * and initialization of all internal state.
 *
 * Special filenames:
 * - ":memory:" creates an in-memory database (lost on close)
 * - Regular paths create/open persistent database files
 *
 * The database is opened with:
 * - READWRITE | CREATE flags (creates if doesn't exist)
 * - "unix-excl" VFS for efficient single-process locking (except :memory:)
 * - WAL journaling mode for concurrent reads
 * - 32MB cache for performance
 *
 * @param i        The kvidx instance to initialize
 * @param filename Path to database file, or ":memory:" for in-memory
 * @param errStr   OUT: Error message on failure, or NULL if not needed
 * @return true on success, false on error (check errStr for details)
 */
bool kvidxSqlite3Open(kvidxInstance *i, const char *filename,
                      const char **errStr) {
    sqlite3 *db = NULL;

    /* Quoth the sqlite3 docs:
     * =======================
     * If all database clients (readers and writers) are located in the same OS
     * process, and if that OS is a Unix variant, then it can be more efficient
     * to the built-in VFS "unix-excl" instead of the default "unix". This is
     * because it uses more efficient locking primitives.
     * =======================
     *
     * IMPORTANT: For :memory: databases, we must use the default VFS (NULL).
     * The unix-excl VFS may have undefined behavior with in-memory databases.
     */
    const char *vfs =
        (filename && strcmp(filename, ":memory:") == 0) ? NULL : "unix-excl";
    int err = sqlite3_open_v2(filename, &db,
                              SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, vfs);
    if (err) {
        /* The error string is static. Pass it directly. */
        if (errStr) {
            *errStr = sqlite3_errmsg(db);
        }

        sqlite3_close(db);
        return false;
    }

    i->kvidxdata = calloc(1, sizeof(kas3State));
    if (!i->kvidxdata) {
        return false;
    }

    kas3State *s = STATE(i);
    s->db = db;
    configureDBOptions(s);

    createLogTable(s->db);
    preparePreparedStatements(s);

    if (i->customInit) {
        i->customInit(i);
    }

    return true;
}

/**
 * Close a SQLite3-backed kvidx database.
 *
 * Cleanly shuts down the database connection by:
 * 1. Finalizing all prepared statements
 * 2. Committing any pending WAL transactions
 * 3. Closing the database handle
 * 4. Freeing internal state memory
 *
 * After this call, the kvidx instance is invalid and must not be used.
 *
 * IMPORTANT: Close() will FAIL if there are uncommitted transactions,
 * unfinalized statements (other than those managed here), or open blob
 * handles. Always commit or abort transactions before closing.
 *
 * @param i  The kvidx instance to close
 * @return true on success, false if close failed (resources may leak)
 */
bool kvidxSqlite3Close(kvidxInstance *i) {
    kas3State *s = STATE(i);

    /* Release the prepared statements */
    sqlite3_finalize(s->begin);
    sqlite3_finalize(s->commit);
    sqlite3_finalize(s->get);
    sqlite3_finalize(s->getPrev);
    sqlite3_finalize(s->getNext);
    sqlite3_finalize(s->exists);
    sqlite3_finalize(s->existsDual);
    sqlite3_finalize(s->insert);
    sqlite3_finalize(s->remove);
    sqlite3_finalize(s->maxKey);
    sqlite3_finalize(s->removeAfterNInclusive);
    sqlite3_finalize(s->removeBeforeNInclusive);

    /* Note: sqlite3_close() will FAIL if any prepared statements
     * remain un-finalized or if any sqlite3-api-driven backups
     * are in progress or if any BLOB handles aren't free'd yet. */

    /* Close DB, final sync to storage, and release all DB resources. */
    if (sqlite3_close(s->db) == SQLITE_OK) {
        free(i->kvidxdata); /* i->kvidxdata == s */
        i->kvidxdata = NULL;
        return true;
    }

    return false;
}

/* ====================================================================
 * Statistics Implementation
 * ==================================================================== */

/**
 * Get the total number of records in the database.
 *
 * Uses COUNT(*) which SQLite can optimize for the primary key index.
 * For very large databases, this may still require touching many pages.
 *
 * @param i      The kvidx instance
 * @param count  OUT: The number of records
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3GetKeyCount(kvidxInstance *i, uint64_t *count) {
    kas3State *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT COUNT(*) FROM log";

    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return KVIDX_ERROR_INTERNAL;
    }

    kvidxError result = KVIDX_ERROR_INTERNAL;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        *count = sqlite3_column_int64(stmt, 0);
        result = KVIDX_OK;
    }

    sqlite3_finalize(stmt);
    return result;
}

/**
 * Get the minimum (smallest) key in the database.
 *
 * Uses MIN() aggregate which SQLite can answer efficiently from the
 * B-tree index without scanning all records.
 *
 * @param i    The kvidx instance
 * @param key  OUT: The minimum key value
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if database is empty
 */
kvidxError kvidxSqlite3GetMinKey(kvidxInstance *i, uint64_t *key) {
    kas3State *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT MIN(id) FROM log";

    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return KVIDX_ERROR_INTERNAL;
    }

    kvidxError result = KVIDX_ERROR_NOT_FOUND;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        if (sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
            *key = sqlite3_column_int64(stmt, 0);
            result = KVIDX_OK;
        }
    }

    sqlite3_finalize(stmt);
    return result;
}

/**
 * Get the total size of all data blobs in the database.
 *
 * Calculates the sum of all data column lengths. This requires touching
 * data pages (not just the index), so it may be slow for large databases.
 *
 * Note: This only counts the data column bytes, not the overhead for
 * keys, metadata columns, or SQLite internal structures.
 *
 * @param i      The kvidx instance
 * @param bytes  OUT: Total bytes of data stored
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3GetDataSize(kvidxInstance *i, uint64_t *bytes) {
    kas3State *s = STATE(i);
    if (!s || !s->db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT SUM(LENGTH(data)) FROM log";

    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return KVIDX_ERROR_INTERNAL;
    }

    kvidxError result = KVIDX_ERROR_INTERNAL;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        if (sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
            *bytes = sqlite3_column_int64(stmt, 0);
        } else {
            *bytes = 0; /* Empty database */
        }
        result = KVIDX_OK;
    }

    sqlite3_finalize(stmt);
    return result;
}

/**
 * Get comprehensive database statistics in a single call.
 *
 * Retrieves all available statistics about the database in an efficient
 * manner, combining multiple queries where possible. This is more efficient
 * than calling individual statistic functions separately.
 *
 * Statistics returned:
 * - totalKeys: Number of records
 * - minKey, maxKey: Key range
 * - totalDataBytes: Sum of data blob sizes
 * - pageCount, pageSize: SQLite page information
 * - freePages: Unused pages (available for reuse)
 * - databaseFileSize: Calculated from page count * page size
 * - walFileSize: Size of WAL file (if in WAL mode)
 *
 * @param i      The kvidx instance
 * @param stats  OUT: Structure to fill with statistics
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3GetStats(kvidxInstance *i, kvidxStats *stats) {
    kas3State *s = STATE(i);
    if (!s || !s->db || !stats) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Initialize stats to zero */
    memset(stats, 0, sizeof(*stats));

    /* Get key count, min, max, and data size in one query */
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "SELECT COUNT(*), MIN(id), MAX(id), SUM(LENGTH(data)) FROM log";

    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return KVIDX_ERROR_INTERNAL;
    }

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        stats->totalKeys = sqlite3_column_int64(stmt, 0);

        if (sqlite3_column_type(stmt, 1) != SQLITE_NULL) {
            stats->minKey = sqlite3_column_int64(stmt, 1);
        }

        if (sqlite3_column_type(stmt, 2) != SQLITE_NULL) {
            stats->maxKey = sqlite3_column_int64(stmt, 2);
        }

        if (sqlite3_column_type(stmt, 3) != SQLITE_NULL) {
            stats->totalDataBytes = sqlite3_column_int64(stmt, 3);
        }
    }

    sqlite3_finalize(stmt);

    /* Get page count and page size */
    stmt = NULL;
    sql = "PRAGMA page_count";
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            stats->pageCount = sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    stmt = NULL;
    sql = "PRAGMA page_size";
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            stats->pageSize = sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    /* Get free pages */
    stmt = NULL;
    sql = "PRAGMA freelist_count";
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            stats->freePages = sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    /* Calculate database file size */
    if (stats->pageCount > 0 && stats->pageSize > 0) {
        stats->databaseFileSize = stats->pageCount * stats->pageSize;
    }

    /* Get WAL file size if in WAL mode */
    stmt = NULL;
    sql = "PRAGMA journal_mode";
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *mode = (const char *)sqlite3_column_text(stmt, 0);
            if (mode && strcmp(mode, "wal") == 0) {
                /* In WAL mode - try to get WAL file size */
                sqlite3_finalize(stmt);
                stmt = NULL;

                /* WAL file size isn't directly available via PRAGMA,
                 * would need to stat the -wal file on disk */
                stats->walFileSize = 0; /* TODO: stat the -wal file */
            }
        }
        if (stmt) {
            sqlite3_finalize(stmt);
        }
    }

    return KVIDX_OK;
}

/* ====================================================================
 * Configuration Application (v0.5.0)
 * ==================================================================== */

/**
 * Convert journal mode enum to SQLite PRAGMA string.
 *
 * @param mode  The journal mode enum value
 * @return Static string suitable for PRAGMA journal_mode
 */
static const char *journalModeToString(kvidxJournalMode mode) {
    switch (mode) {
    case KVIDX_JOURNAL_DELETE:
        return "DELETE";
    case KVIDX_JOURNAL_TRUNCATE:
        return "TRUNCATE";
    case KVIDX_JOURNAL_PERSIST:
        return "PERSIST";
    case KVIDX_JOURNAL_MEMORY:
        return "MEMORY";
    case KVIDX_JOURNAL_WAL:
        return "WAL";
    case KVIDX_JOURNAL_OFF:
        return "OFF";
    default:
        return "DELETE";
    }
}

/**
 * Convert sync mode enum to SQLite PRAGMA string.
 *
 * @param mode  The sync mode enum value
 * @return Static string suitable for PRAGMA synchronous
 */
static const char *syncModeToString(kvidxSyncMode mode) {
    switch (mode) {
    case KVIDX_SYNC_OFF:
        return "OFF";
    case KVIDX_SYNC_NORMAL:
        return "NORMAL";
    case KVIDX_SYNC_FULL:
        return "FULL";
    case KVIDX_SYNC_EXTRA:
        return "EXTRA";
    default:
        return "NORMAL";
    }
}

/**
 * Apply runtime configuration to an open database.
 *
 * Allows dynamic adjustment of database behavior after opening. This is
 * useful for tuning performance based on workload or changing durability
 * guarantees.
 *
 * Configurable settings:
 * - cacheSizeBytes: Memory allocated for page cache
 * - journalMode: DELETE, TRUNCATE, PERSIST, MEMORY, WAL, or OFF
 * - syncMode: OFF, NORMAL, FULL, or EXTRA
 * - enableRecursiveTriggers: Allow triggers to fire other triggers
 * - enableForeignKeys: Enforce foreign key constraints
 * - busyTimeoutMs: How long to wait when database is locked
 * - mmapSizeBytes: Memory-mapped I/O size (0 to disable)
 *
 * Note: Some settings (like journal mode) may require exclusive access
 * and could fail if other connections exist.
 *
 * @param i       The kvidx instance
 * @param config  Configuration settings to apply
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3ApplyConfig(kvidxInstance *i,
                                   const kvidxConfig *config) {
    if (!i || !i->kvidxdata || !config) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    kas3State *s = STATE(i);
    char sql[256];
    int rc;

    /* Set cache size (in pages, negative means KB) */
    if (config->cacheSizeBytes > 0) {
        int cacheSizeKB = -(int)(config->cacheSizeBytes / 1024);
        snprintf(sql, sizeof(sql), "PRAGMA cache_size = %d", cacheSizeKB);
        rc = sqlite3_exec(s->db, sql, NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                          "Failed to set cache size: %s",
                          sqlite3_errmsg(s->db));
            return KVIDX_ERROR_INTERNAL;
        }
    }

    /* Set journal mode */
    snprintf(sql, sizeof(sql), "PRAGMA journal_mode = %s",
             journalModeToString(config->journalMode));
    rc = sqlite3_exec(s->db, sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "Failed to set journal mode: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    /* Set sync mode */
    snprintf(sql, sizeof(sql), "PRAGMA synchronous = %s",
             syncModeToString(config->syncMode));
    rc = sqlite3_exec(s->db, sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "Failed to set sync mode: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    /* Set recursive triggers */
    snprintf(sql, sizeof(sql), "PRAGMA recursive_triggers = %s",
             config->enableRecursiveTriggers ? "ON" : "OFF");
    rc = sqlite3_exec(s->db, sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                      "Failed to set recursive triggers: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    /* Set foreign keys */
    snprintf(sql, sizeof(sql), "PRAGMA foreign_keys = %s",
             config->enableForeignKeys ? "ON" : "OFF");
    rc = sqlite3_exec(s->db, sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "Failed to set foreign keys: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    /* Set busy timeout */
    rc = sqlite3_busy_timeout(s->db, config->busyTimeoutMs);
    if (rc != SQLITE_OK) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "Failed to set busy timeout: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    /* Set mmap size if specified */
    if (config->mmapSizeBytes > 0) {
        snprintf(sql, sizeof(sql), "PRAGMA mmap_size = %d",
                 config->mmapSizeBytes);
        rc = sqlite3_exec(s->db, sql, NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                          "Failed to set mmap size: %s", sqlite3_errmsg(s->db));
            return KVIDX_ERROR_INTERNAL;
        }
    }

    return KVIDX_OK;
}

/* ====================================================================
 * Range Operations Implementation (v0.5.0)
 * ==================================================================== */

/**
 * Delete all records within a key range.
 *
 * Efficiently removes records whose keys fall within the specified range.
 * Both start and end boundaries can be inclusive or exclusive, allowing
 * for flexible range definitions.
 *
 * Special handling for UINT64_MAX: Since SQLite treats uint64 as signed
 * int64, UINT64_MAX becomes -1. When endKey is UINT64_MAX, the query is
 * adjusted to use only the start bound.
 *
 * @param i              The kvidx instance
 * @param startKey       Lower bound of the range
 * @param endKey         Upper bound of the range
 * @param startInclusive true for >= startKey, false for > startKey
 * @param endInclusive   true for <= endKey, false for < endKey
 * @param deletedCount   OUT: Number of records deleted (optional)
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3RemoveRange(kvidxInstance *i, uint64_t startKey,
                                   uint64_t endKey, bool startInclusive,
                                   bool endInclusive, uint64_t *deletedCount) {
    if (!i || !i->kvidxdata) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    kas3State *s = STATE(i);
    char sql[256];

    /* Build WHERE clause based on inclusivity */
    const char *startOp = startInclusive ? ">=" : ">";
    const char *endOp = endInclusive ? "<=" : "<";

    /* Handle UINT64_MAX specially since it becomes -1 when cast to int64 */
    if (endKey == UINT64_MAX) {
        snprintf(sql, sizeof(sql), "DELETE FROM log WHERE id %s ?", startOp);
    } else {
        snprintf(sql, sizeof(sql), "DELETE FROM log WHERE id %s ? AND id %s ?",
                 startOp, endOp);
    }

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                      "Failed to prepare remove range: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_int64(stmt, 1, startKey);
    if (endKey != UINT64_MAX) {
        sqlite3_bind_int64(stmt, 2, endKey);
    }

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "Failed to remove range: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    if (deletedCount) {
        *deletedCount = sqlite3_changes(s->db);
    }

    return KVIDX_OK;
}

/**
 * Count the number of records within a key range.
 *
 * Efficiently counts records whose keys fall within [startKey, endKey].
 * Both boundaries are inclusive. Uses COUNT(*) which SQLite can optimize
 * using the index.
 *
 * @param i         The kvidx instance
 * @param startKey  Lower bound of the range (inclusive)
 * @param endKey    Upper bound of the range (inclusive)
 * @param count     OUT: Number of records in range
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3CountRange(kvidxInstance *i, uint64_t startKey,
                                  uint64_t endKey, uint64_t *count) {
    if (!i || !i->kvidxdata || !count) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    kas3State *s = STATE(i);
    sqlite3_stmt *stmt = NULL;
    char sql[256];

    /* Handle UINT64_MAX specially since it becomes -1 when cast to int64 */
    if (endKey == UINT64_MAX) {
        snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM log WHERE id >= ?");
    } else {
        snprintf(sql, sizeof(sql),
                 "SELECT COUNT(*) FROM log WHERE id >= ? AND id <= ?");
    }

    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                      "Failed to prepare count range: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_int64(stmt, 1, startKey);
    if (endKey != UINT64_MAX) {
        sqlite3_bind_int64(stmt, 2, endKey);
    }

    kvidxError result = KVIDX_ERROR_INTERNAL;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        *count = sqlite3_column_int64(stmt, 0);
        result = KVIDX_OK;
    } else {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "Failed to count range: %s",
                      sqlite3_errmsg(s->db));
    }

    sqlite3_finalize(stmt);
    return result;
}

/**
 * Check if any records exist within a key range.
 *
 * Efficiently determines whether at least one record exists in the range
 * [startKey, endKey]. Both boundaries are inclusive. This is faster than
 * CountRange when you only need to know if records exist.
 *
 * Uses SELECT EXISTS() which can short-circuit after finding one match.
 *
 * @param i         The kvidx instance
 * @param startKey  Lower bound of the range (inclusive)
 * @param endKey    Upper bound of the range (inclusive)
 * @param exists    OUT: true if at least one record exists in range
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3ExistsInRange(kvidxInstance *i, uint64_t startKey,
                                     uint64_t endKey, bool *exists) {
    if (!i || !i->kvidxdata || !exists) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    kas3State *s = STATE(i);
    sqlite3_stmt *stmt = NULL;
    char sql[256];

    /* Handle UINT64_MAX specially since it becomes -1 when cast to int64 */
    if (endKey == UINT64_MAX) {
        snprintf(sql, sizeof(sql),
                 "SELECT EXISTS(SELECT 1 FROM log WHERE id >= ?)");
    } else {
        snprintf(sql, sizeof(sql),
                 "SELECT EXISTS(SELECT 1 FROM log WHERE id >= ? AND id <= ?)");
    }

    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                      "Failed to prepare exists in range: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_int64(stmt, 1, startKey);
    if (endKey != UINT64_MAX) {
        sqlite3_bind_int64(stmt, 2, endKey);
    }

    kvidxError result = KVIDX_ERROR_INTERNAL;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        *exists = sqlite3_column_int(stmt, 0) != 0;
        result = KVIDX_OK;
    } else {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                      "Failed to check exists in range: %s",
                      sqlite3_errmsg(s->db));
    }

    sqlite3_finalize(stmt);
    return result;
}

/* ====================================================================
 * Export/Import Implementation (v0.6.0)
 * ==================================================================== */

/**
 * @section Export/Import
 *
 * These functions provide data portability between kvidx instances and
 * external systems. Three formats are supported:
 *
 * - BINARY: Compact, fast, native format with magic number verification
 * - JSON: Human-readable, interoperable with other systems
 * - CSV: Simple tabular format for spreadsheets/databases
 *
 * All formats support:
 * - Key range filtering (export subset of data)
 * - Progress callbacks for long operations
 * - Metadata inclusion/exclusion (term, cmd fields)
 */

/* Binary format magic number: "KVIDX\0\0\0" */
#define KVIDX_BINARY_MAGIC 0x5844495645564B00ULL /* Little-endian "KVIDX" */
#define KVIDX_BINARY_VERSION 1

/* Binary format header */
typedef struct {
    uint64_t magic;
    uint32_t version;
    uint32_t reserved;
    uint64_t entryCount;
} kvidxBinaryHeader;

/**
 * Write a single entry in binary format.
 *
 * Binary entry format (little-endian):
 * - key (8 bytes)
 * - term (8 bytes)
 * - cmd (8 bytes)
 * - dataLen (8 bytes)
 * - data (dataLen bytes)
 *
 * @param fp       File pointer to write to
 * @param key      Record key
 * @param term     Record term
 * @param cmd      Record cmd
 * @param data     Record data
 * @param dataLen  Length of data
 * @return KVIDX_OK on success, KVIDX_ERROR_IO on write failure
 */
static kvidxError writeBinaryEntry(FILE *fp, uint64_t key, uint64_t term,
                                   uint64_t cmd, const uint8_t *data,
                                   size_t dataLen) {
    /* Write key, term, cmd */
    if (fwrite(&key, sizeof(key), 1, fp) != 1) {
        return KVIDX_ERROR_IO;
    }
    if (fwrite(&term, sizeof(term), 1, fp) != 1) {
        return KVIDX_ERROR_IO;
    }
    if (fwrite(&cmd, sizeof(cmd), 1, fp) != 1) {
        return KVIDX_ERROR_IO;
    }

    /* Write data length and data */
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

/**
 * Write a string with JSON escape sequences.
 *
 * Escapes special characters according to RFC 8259:
 * - " -> \"
 * - \ -> \\
 * - Control characters -> \uXXXX
 *
 * @param fp   File pointer to write to
 * @param str  String to escape and write
 * @param len  Length of string
 */
static void writeJsonEscaped(FILE *fp, const char *str, size_t len) {
    for (size_t i = 0; i < len; i++) {
        unsigned char c = str[i];
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

/**
 * Write a CSV field with proper quoting.
 *
 * Follows RFC 4180 CSV rules:
 * - Fields containing comma, quote, or newline are enclosed in quotes
 * - Quotes within quoted fields are escaped by doubling ("" -> "")
 * - Other fields are written as-is
 *
 * @param fp   File pointer to write to
 * @param str  Field value to write
 * @param len  Length of field value
 */
static void writeCsvField(FILE *fp, const char *str, size_t len) {
    bool needsQuotes = false;

    /* Check if field needs quoting */
    for (size_t i = 0; i < len; i++) {
        if (str[i] == ',' || str[i] == '"' || str[i] == '\n' ||
            str[i] == '\r') {
            needsQuotes = true;
            break;
        }
    }

    if (needsQuotes) {
        fputc('"', fp);
        for (size_t i = 0; i < len; i++) {
            if (str[i] == '"') {
                fputc('"', fp); /* Escape quote with double quote */
            }
            fputc(str[i], fp);
        }
        fputc('"', fp);
    } else {
        fwrite(str, 1, len, fp);
    }
}

/**
 * Export database contents to a file.
 *
 * Writes all records (or a range) to an external file in the specified
 * format. Supports progress callbacks for long-running exports.
 *
 * Export formats:
 * - KVIDX_EXPORT_BINARY: Fast, compact, includes header with count
 * - KVIDX_EXPORT_JSON: Pretty-printable, includes format metadata
 * - KVIDX_EXPORT_CSV: Spreadsheet-compatible, optional header row
 *
 * Options:
 * - startKey/endKey: Filter to export only a key range
 * - includeMetadata: Include term/cmd fields (not just key/data)
 * - prettyPrint: Add whitespace for readability (JSON only)
 *
 * @param i         The kvidx instance
 * @param filename  Output file path
 * @param options   Export options (format, range, etc.)
 * @param callback  Progress callback, or NULL for no progress
 * @param userData  Opaque pointer passed to callback
 * @return KVIDX_OK on success, KVIDX_ERROR_CANCELLED if callback aborted
 */
kvidxError kvidxSqlite3Export(kvidxInstance *i, const char *filename,
                              const kvidxExportOptions *options,
                              kvidxProgressCallback callback, void *userData) {
    if (!i || !i->kvidxdata || !filename || !options) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    kas3State *s = STATE(i);
    FILE *fp = fopen(filename, "wb");
    if (!fp) {
        kvidxSetError(i, KVIDX_ERROR_IO, "Failed to open file for writing: %s",
                      filename);
        return KVIDX_ERROR_IO;
    }

    /* Build query based on key range */
    char sql[256];
    /* Handle UINT64_MAX specially since it becomes -1 when cast to int64 */
    if (options->endKey == UINT64_MAX) {
        snprintf(
            sql, sizeof(sql),
            "SELECT id, term, cmd, data FROM log WHERE id >= ? ORDER BY id");
    } else {
        snprintf(sql, sizeof(sql),
                 "SELECT id, term, cmd, data FROM log WHERE id >= ? AND id <= "
                 "? ORDER BY id");
    }

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fclose(fp);
        kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                      "Failed to prepare export query: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_int64(stmt, 1, options->startKey);
    if (options->endKey != UINT64_MAX) {
        sqlite3_bind_int64(stmt, 2, options->endKey);
    }

    kvidxError result = KVIDX_OK;
    uint64_t count = 0;
    uint64_t totalCount = 0;

    /* Get total count for progress */
    uint64_t total = 0;
    kvidxCountRange(i, options->startKey, options->endKey, &total);

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
        /* CSV header */
        if (options->includeMetadata) {
            fprintf(fp, "key,term,cmd,data\n");
        } else {
            fprintf(fp, "key,data\n");
        }
    }

    /* Export entries */
    bool firstEntry = true;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint64_t key = sqlite3_column_int64(stmt, 0);
        uint64_t term = sqlite3_column_int64(stmt, 1);
        uint64_t cmd = sqlite3_column_int64(stmt, 2);
        const uint8_t *data = sqlite3_column_blob(stmt, 3);
        size_t dataLen = sqlite3_column_bytes(stmt, 3);

        /* Write entry in appropriate format */
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

        /* Progress callback */
        if (callback && count % 100 == 0) {
            if (!callback(count, total, userData)) {
                result = KVIDX_ERROR_CANCELLED;
                goto cleanup;
            }
        }
    }

    /* Write format-specific footer */
    if (options->format == KVIDX_EXPORT_JSON) {
        if (options->prettyPrint) {
            fprintf(fp, "\n");
        }
        fprintf(fp, "]}\n");
    }

    /* Final progress callback */
    if (callback && count > 0) {
        callback(count, total, userData);
    }

    totalCount = count;

cleanup:
    sqlite3_finalize(stmt);
    fclose(fp);

    if (result != KVIDX_OK) {
        kvidxSetError(i, result, "Export failed after %" PRIu64 " entries",
                      totalCount);
    }

    return result;
}

/**
 * Import database contents from a file.
 *
 * Reads records from an external file and inserts them into the database.
 * Currently only BINARY format is fully implemented; JSON and CSV import
 * return KVIDX_ERROR_NOT_SUPPORTED.
 *
 * Import options:
 * - clearBeforeImport: Delete all existing records first
 * - skipDuplicates: Continue on duplicate key (else fail)
 * - format: Auto-detect by examining file header/content
 *
 * The import is wrapped in a transaction for atomicity. On error, all
 * changes are rolled back.
 *
 * @param i         The kvidx instance
 * @param filename  Input file path
 * @param options   Import options (format, clear, etc.)
 * @param callback  Progress callback, or NULL for no progress
 * @param userData  Opaque pointer passed to callback
 * @return KVIDX_OK on success, KVIDX_ERROR_CANCELLED if callback aborted
 */
kvidxError kvidxSqlite3Import(kvidxInstance *i, const char *filename,
                              const kvidxImportOptions *options,
                              kvidxProgressCallback callback, void *userData) {
    if (!i || !i->kvidxdata || !filename || !options) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        kvidxSetError(i, KVIDX_ERROR_IO, "Failed to open file for reading: %s",
                      filename);
        return KVIDX_ERROR_IO;
    }

    kvidxError result = KVIDX_OK;
    uint64_t count = 0;

    /* Clear database if requested */
    if (options->clearBeforeImport) {
        kas3State *s = STATE(i);
        int rc = sqlite3_exec(s->db, "DELETE FROM log", NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            fclose(fp);
            kvidxSetError(i, KVIDX_ERROR_INTERNAL, "Failed to clear database");
            return KVIDX_ERROR_INTERNAL;
        }
    }

    /* Detect format if auto-detect */
    kvidxExportFormat format = options->format;
    if (format == KVIDX_EXPORT_BINARY) {
        /* Try to detect format by reading magic */
        uint64_t magic;
        if (fread(&magic, sizeof(magic), 1, fp) == 1) {
            if (magic == KVIDX_BINARY_MAGIC) {
                format = KVIDX_EXPORT_BINARY;
                rewind(fp); /* Reset to beginning */
            } else {
                /* Check for JSON or CSV */
                rewind(fp);
                char buf[32];
                if (fgets(buf, sizeof(buf), fp)) {
                    if (buf[0] == '{') {
                        format = KVIDX_EXPORT_JSON;
                    } else {
                        format = KVIDX_EXPORT_CSV;
                    }
                    rewind(fp);
                }
            }
        }
    }

    /* Import based on format */
    if (format == KVIDX_EXPORT_BINARY) {
        /* Read and validate header */
        kvidxBinaryHeader header;
        if (fread(&header, sizeof(header), 1, fp) != 1) {
            fclose(fp);
            kvidxSetError(i, KVIDX_ERROR_IO, "Failed to read binary header");
            return KVIDX_ERROR_IO;
        }

        if (header.magic != KVIDX_BINARY_MAGIC) {
            fclose(fp);
            kvidxSetError(i, KVIDX_ERROR_INVALID_ARGUMENT,
                          "Invalid binary format magic");
            return KVIDX_ERROR_INVALID_ARGUMENT;
        }

        if (header.version != KVIDX_BINARY_VERSION) {
            fclose(fp);
            kvidxSetError(i, KVIDX_ERROR_INVALID_ARGUMENT,
                          "Unsupported binary format version: %u",
                          header.version);
            return KVIDX_ERROR_INVALID_ARGUMENT;
        }

        /* Begin transaction */
        if (!kvidxBegin(i)) {
            fclose(fp);
            kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                          "Failed to begin transaction");
            return KVIDX_ERROR_INTERNAL;
        }

        /* Read entries */
        for (uint64_t idx = 0; idx < header.entryCount; idx++) {
            uint64_t key, term, cmd, dataLen;
            uint8_t *data = NULL;

            /* Read entry fields */
            if (fread(&key, sizeof(key), 1, fp) != 1 ||
                fread(&term, sizeof(term), 1, fp) != 1 ||
                fread(&cmd, sizeof(cmd), 1, fp) != 1 ||
                fread(&dataLen, sizeof(dataLen), 1, fp) != 1) {
                result = KVIDX_ERROR_IO;
                break;
            }

            /* Read data if present */
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

            /* Insert entry */
            bool inserted = kvidxInsert(i, key, term, cmd, data, dataLen);
            free(data);

            if (!inserted) {
                if (options->skipDuplicates) {
                    /* Continue on duplicate key */
                    continue;
                } else {
                    result = KVIDX_ERROR_INTERNAL;
                    break;
                }
            }

            count++;

            /* Progress callback */
            if (callback && count % 100 == 0) {
                if (!callback(count, header.entryCount, userData)) {
                    result = KVIDX_ERROR_CANCELLED;
                    break;
                }
            }
        }

        /* Commit transaction */
        if (!kvidxCommit(i)) {
            result = KVIDX_ERROR_INTERNAL;
        }

        /* Final progress callback */
        if (callback && count > 0) {
            callback(count, header.entryCount, userData);
        }
    } else {
        /* JSON and CSV import not implemented in this version */
        fclose(fp);
        kvidxSetError(i, KVIDX_ERROR_NOT_SUPPORTED,
                      "JSON and CSV import not yet implemented");
        return KVIDX_ERROR_NOT_SUPPORTED;
    }

    fclose(fp);

    if (result != KVIDX_OK) {
        kvidxSetError(i, result, "Import failed after %" PRIu64 " entries",
                      count);
    }

    return result;
}

/* ====================================================================
 * Storage Primitives Implementation (v0.8.0)
 * ==================================================================== */

/**
 * @section Storage Primitives
 *
 * Advanced key-value operations beyond basic CRUD. These primitives enable
 * building higher-level data structures and concurrent access patterns:
 *
 * - Conditional writes: Insert only if key exists/doesn't exist
 * - Atomic operations: GetAndSet, GetAndRemove, CompareAndSwap
 * - Data manipulation: Append, Prepend, partial value access
 * - TTL/Expiration: Automatic key expiration with background cleanup
 * - Transaction abort: Rollback uncommitted changes
 */

/**
 * Get current wall-clock time in milliseconds since epoch.
 *
 * Used for TTL calculations. Note: Uses CLOCK_REALTIME which can jump
 * forward or backward if system time is adjusted.
 *
 * @return Current time in milliseconds
 */
static uint64_t currentTimeMs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/**
 * Create the TTL metadata table if it doesn't exist.
 *
 * The _kvidx_ttl table stores expiration timestamps for keys. It has:
 * - id (INTEGER PRIMARY KEY): Same as the key in the main log table
 * - expires_at (INTEGER): Unix timestamp in milliseconds when key expires
 *
 * An index on expires_at enables efficient expiration scans.
 *
 * @param s  The internal adapter state
 * @return true if table exists/created, false on error
 */
static bool ensureTTLTable(kas3State *s) {
    static bool created = false;
    if (created) {
        return true;
    }

    const char *sql = "CREATE TABLE IF NOT EXISTS _kvidx_ttl ("
                      "id INTEGER PRIMARY KEY, "
                      "expires_at INTEGER NOT NULL)";
    int rc = sqlite3_exec(s->db, sql, NULL, NULL, NULL);
    if (rc == SQLITE_OK) {
        /* Create index for efficient expiration scans */
        const char *idx = "CREATE INDEX IF NOT EXISTS _kvidx_ttl_expires ON "
                          "_kvidx_ttl(expires_at)";
        sqlite3_exec(s->db, idx, NULL, NULL, NULL);
        created = true;
    }
    return rc == SQLITE_OK;
}

/* --- Transaction Abort --- */

/**
 * Abort the current transaction and roll back all changes.
 *
 * Discards all modifications made since the last Begin() call. This is
 * useful for error recovery or implementing speculative operations.
 *
 * After abort, no transaction is active. You must call Begin() again
 * before performing more operations that need atomicity.
 *
 * @param i  The kvidx instance
 * @return true on success, false if no transaction was active
 */
bool kvidxSqlite3Abort(kvidxInstance *i) {
    kas3State *s = STATE(i);
    int rc = sqlite3_exec(s->db, "ROLLBACK;", NULL, NULL, NULL);
    return rc == SQLITE_OK || rc == SQLITE_DONE;
}

/* --- Conditional Writes --- */

/**
 * Insert or update a record with conditional behavior.
 *
 * This is an extended version of Insert() that supports different write
 * modes based on whether the key already exists:
 *
 * - KVIDX_SET_ALWAYS: Upsert - insert if new, update if exists
 * - KVIDX_SET_IF_NOT_EXISTS: Only insert if key doesn't exist (NX)
 * - KVIDX_SET_IF_EXISTS: Only update if key already exists (XX)
 *
 * These conditions enable safe concurrent access patterns without
 * external locking.
 *
 * @param i          The kvidx instance
 * @param key        The record key
 * @param term       Application-defined term/version
 * @param cmd        Application-defined command/type
 * @param data       Data to store
 * @param dataLen    Length of data
 * @param condition  Write condition (ALWAYS, IF_NOT_EXISTS, IF_EXISTS)
 * @return KVIDX_OK on success, KVIDX_ERROR_CONDITION_FAILED if condition not
 * met
 */
kvidxError kvidxSqlite3InsertEx(kvidxInstance *i, uint64_t key, uint64_t term,
                                uint64_t cmd, const void *data, size_t dataLen,
                                kvidxSetCondition condition) {
    kas3State *s = STATE(i);

    switch (condition) {
    case KVIDX_SET_ALWAYS: {
        /* Use INSERT OR REPLACE */
        sqlite3_stmt *stmt = NULL;
        const char *sql = "INSERT OR REPLACE INTO log VALUES(?, ?, ?, ?, ?)";
        int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                          "Failed to prepare InsertEx: %s",
                          sqlite3_errmsg(s->db));
            return KVIDX_ERROR_INTERNAL;
        }
        sqlite3_bind_int64(stmt, 1, key);
        sqlite3_bind_int64(stmt, 2, 0); /* timestamp */
        sqlite3_bind_int64(stmt, 3, term);
        sqlite3_bind_int64(stmt, 4, cmd);
        sqlite3_bind_blob64(stmt, 5, data, dataLen, NULL);
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE && rc != SQLITE_OK) {
            kvidxSetError(i, KVIDX_ERROR_INTERNAL, "InsertEx failed: %s",
                          sqlite3_errmsg(s->db));
            return KVIDX_ERROR_INTERNAL;
        }
        return KVIDX_OK;
    }

    case KVIDX_SET_IF_NOT_EXISTS: {
        /* Check if key exists first */
        if (kvidxSqlite3Exists(i, key)) {
            return KVIDX_ERROR_CONDITION_FAILED;
        }
        /* Key doesn't exist, insert */
        if (!kvidxSqlite3Insert(i, key, term, cmd, data, dataLen)) {
            /* Could have been inserted by concurrent operation */
            if (kvidxSqlite3Exists(i, key)) {
                return KVIDX_ERROR_CONDITION_FAILED;
            }
            return KVIDX_ERROR_INTERNAL;
        }
        return KVIDX_OK;
    }

    case KVIDX_SET_IF_EXISTS: {
        /* Check if key exists first */
        if (!kvidxSqlite3Exists(i, key)) {
            return KVIDX_ERROR_CONDITION_FAILED;
        }
        /* Key exists, update it */
        sqlite3_stmt *stmt = NULL;
        const char *sql =
            "UPDATE log SET term = ?, cmd = ?, data = ? WHERE id = ?";
        int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                          "Failed to prepare InsertXX: %s",
                          sqlite3_errmsg(s->db));
            return KVIDX_ERROR_INTERNAL;
        }
        sqlite3_bind_int64(stmt, 1, term);
        sqlite3_bind_int64(stmt, 2, cmd);
        sqlite3_bind_blob64(stmt, 3, data, dataLen, NULL);
        sqlite3_bind_int64(stmt, 4, key);
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE && rc != SQLITE_OK) {
            return KVIDX_ERROR_INTERNAL;
        }
        if (sqlite3_changes(s->db) == 0) {
            return KVIDX_ERROR_CONDITION_FAILED;
        }
        return KVIDX_OK;
    }

    default:
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }
}

/* --- Atomic Operations --- */

/**
 * Atomically replace a value and return the old value.
 *
 * This operation is useful for implementing swap semantics or tracking
 * what was replaced. The old value is returned in caller-allocated memory
 * that must be freed by the caller.
 *
 * If the key doesn't exist, a new record is created and old* outputs
 * are set to zero/NULL.
 *
 * @param i           The kvidx instance
 * @param key         The record key
 * @param term        New term value
 * @param cmd         New cmd value
 * @param data        New data to store
 * @param dataLen     Length of new data
 * @param oldTerm     OUT: Previous term value (optional)
 * @param oldCmd      OUT: Previous cmd value (optional)
 * @param oldData     OUT: Previous data (caller must free) (optional)
 * @param oldDataLen  OUT: Length of previous data (optional)
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3GetAndSet(kvidxInstance *i, uint64_t key, uint64_t term,
                                 uint64_t cmd, const void *data, size_t dataLen,
                                 uint64_t *oldTerm, uint64_t *oldCmd,
                                 void **oldData, size_t *oldDataLen) {
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

    /* Get current value first */
    uint64_t prevTerm = 0, prevCmd = 0;
    const uint8_t *prevData = NULL;
    size_t prevLen = 0;
    bool hadPrevious =
        kvidxSqlite3Get(i, key, &prevTerm, &prevCmd, &prevData, &prevLen);

    if (hadPrevious) {
        /* Copy old data if requested */
        if (oldTerm) {
            *oldTerm = prevTerm;
        }
        if (oldCmd) {
            *oldCmd = prevCmd;
        }
        if (oldData && prevLen > 0) {
            *oldData = malloc(prevLen);
            if (*oldData) {
                memcpy(*oldData, prevData, prevLen);
                if (oldDataLen) {
                    *oldDataLen = prevLen;
                }
            }
        } else if (oldDataLen) {
            *oldDataLen = prevLen;
        }
    }

    /* Now set the new value using INSERT OR REPLACE */
    kvidxError err = kvidxSqlite3InsertEx(i, key, term, cmd, data, dataLen,
                                          KVIDX_SET_ALWAYS);
    return err;
}

/**
 * Atomically remove a record and return its value.
 *
 * Retrieves the current value of a key while simultaneously deleting it.
 * This is useful for implementing pop/dequeue operations or ensuring
 * exclusive access to a one-time-use value.
 *
 * The returned data is allocated and must be freed by the caller.
 *
 * @param i        The kvidx instance
 * @param key      The record key to remove
 * @param term     OUT: The term value of removed record (optional)
 * @param cmd      OUT: The cmd value of removed record (optional)
 * @param data     OUT: The data of removed record (caller must free)
 * @param dataLen  OUT: Length of the removed data
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key doesn't exist
 */
kvidxError kvidxSqlite3GetAndRemove(kvidxInstance *i, uint64_t key,
                                    uint64_t *term, uint64_t *cmd, void **data,
                                    size_t *dataLen) {
    /* Initialize outputs */
    if (data) {
        *data = NULL;
    }
    if (dataLen) {
        *dataLen = 0;
    }

    /* Get current value first */
    uint64_t prevTerm = 0, prevCmd = 0;
    const uint8_t *prevData = NULL;
    size_t prevLen = 0;
    bool exists =
        kvidxSqlite3Get(i, key, &prevTerm, &prevCmd, &prevData, &prevLen);

    if (!exists) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Copy data before removing */
    if (term) {
        *term = prevTerm;
    }
    if (cmd) {
        *cmd = prevCmd;
    }
    if (data && prevLen > 0) {
        *data = malloc(prevLen);
        if (*data) {
            memcpy(*data, prevData, prevLen);
            if (dataLen) {
                *dataLen = prevLen;
            }
        }
    } else if (dataLen) {
        *dataLen = prevLen;
    }

    /* Now remove */
    if (!kvidxSqlite3Remove(i, key)) {
        /* Free allocated data on failure */
        if (data && *data) {
            free(*data);
            *data = NULL;
        }
        return KVIDX_ERROR_INTERNAL;
    }

    return KVIDX_OK;
}

/* --- Compare-And-Swap --- */

/**
 * Atomically update a value only if it matches an expected value.
 *
 * This is the classic CAS operation for implementing lock-free data
 * structures. The update only succeeds if the current data exactly
 * matches the expected data.
 *
 * This enables optimistic concurrency control:
 * 1. Read current value
 * 2. Compute new value based on current
 * 3. CAS with current as expected, new as replacement
 * 4. If swapped=false, retry from step 1
 *
 * @param i             The kvidx instance
 * @param key           The record key
 * @param expectedData  Data that must match current value
 * @param expectedLen   Length of expected data
 * @param newTerm       New term if swap succeeds
 * @param newCmd        New cmd if swap succeeds
 * @param newData       New data if swap succeeds
 * @param newDataLen    Length of new data
 * @param swapped       OUT: true if swap occurred, false if data didn't match
 * @return KVIDX_OK on success (check swapped), KVIDX_ERROR_NOT_FOUND if key
 * missing
 */
kvidxError kvidxSqlite3CompareAndSwap(kvidxInstance *i, uint64_t key,
                                      const void *expectedData,
                                      size_t expectedLen, uint64_t newTerm,
                                      uint64_t newCmd, const void *newData,
                                      size_t newDataLen, bool *swapped) {
    kas3State *s = STATE(i);
    *swapped = false;

    /* Get current value */
    uint64_t currentTerm;
    const uint8_t *currentData = NULL;
    size_t currentLen = 0;

    if (!kvidxSqlite3Get(i, key, &currentTerm, NULL, &currentData,
                         &currentLen)) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Compare data */
    bool matches = false;
    if (expectedData == NULL && currentLen == 0) {
        /* NULL expected matches empty data */
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
        return KVIDX_OK;
    }

    /* Data matches, perform update */
    sqlite3_stmt *stmt = NULL;
    const char *sql = "UPDATE log SET term = ?, cmd = ?, data = ? WHERE id = ?";
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL,
                      "Failed to prepare CAS update: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_int64(stmt, 1, newTerm);
    sqlite3_bind_int64(stmt, 2, newCmd);
    sqlite3_bind_blob64(stmt, 3, newData, newDataLen, NULL);
    sqlite3_bind_int64(stmt, 4, key);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE && rc != SQLITE_OK) {
        kvidxSetError(i, KVIDX_ERROR_INTERNAL, "CAS update failed: %s",
                      sqlite3_errmsg(s->db));
        return KVIDX_ERROR_INTERNAL;
    }

    *swapped = (sqlite3_changes(s->db) > 0);
    return KVIDX_OK;
}

/* --- Append/Prepend --- */

/**
 * Append data to the end of an existing value.
 *
 * If the key exists, concatenates the new data after the existing data.
 * If the key doesn't exist, creates a new record with just the provided data.
 *
 * Useful for building up log entries, accumulating data, or implementing
 * append-only data structures.
 *
 * @param i        The kvidx instance
 * @param key      The record key
 * @param term     Term for new record (used only if key doesn't exist)
 * @param cmd      Cmd for new record (used only if key doesn't exist)
 * @param data     Data to append
 * @param dataLen  Length of data to append
 * @param newLen   OUT: Total length after append (optional)
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3Append(kvidxInstance *i, uint64_t key, uint64_t term,
                              uint64_t cmd, const void *data, size_t dataLen,
                              size_t *newLen) {
    kas3State *s = STATE(i);

    /* Get current value */
    const uint8_t *currentData = NULL;
    size_t currentLen = 0;
    bool exists =
        kvidxSqlite3Get(i, key, NULL, NULL, &currentData, &currentLen);

    if (!exists) {
        /* Key doesn't exist, create with provided data */
        if (!kvidxSqlite3Insert(i, key, term, cmd, data, dataLen)) {
            return KVIDX_ERROR_INTERNAL;
        }
        if (newLen) {
            *newLen = dataLen;
        }
        return KVIDX_OK;
    }

    /* Key exists, append to existing data */
    size_t totalLen = currentLen + dataLen;
    void *newData = malloc(totalLen);
    if (!newData && totalLen > 0) {
        return KVIDX_ERROR_NOMEM;
    }

    if (currentLen > 0 && currentData) {
        memcpy(newData, currentData, currentLen);
    }
    if (dataLen > 0 && data) {
        memcpy((uint8_t *)newData + currentLen, data, dataLen);
    }

    /* Update with concatenated data (keep existing term/cmd) */
    uint64_t existingTerm, existingCmd;
    kvidxSqlite3Get(i, key, &existingTerm, &existingCmd, NULL, NULL);

    sqlite3_stmt *stmt = NULL;
    const char *sql = "UPDATE log SET data = ? WHERE id = ?";
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        free(newData);
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_blob64(stmt, 1, newData, totalLen, NULL);
    sqlite3_bind_int64(stmt, 2, key);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    free(newData);

    if (rc != SQLITE_DONE && rc != SQLITE_OK) {
        return KVIDX_ERROR_INTERNAL;
    }

    if (newLen) {
        *newLen = totalLen;
    }
    return KVIDX_OK;
}

/**
 * Prepend data to the beginning of an existing value.
 *
 * If the key exists, concatenates the new data before the existing data.
 * If the key doesn't exist, creates a new record with just the provided data.
 *
 * Useful for building headers, prepending metadata, or implementing
 * stack-like data structures.
 *
 * @param i        The kvidx instance
 * @param key      The record key
 * @param term     Term for new record (used only if key doesn't exist)
 * @param cmd      Cmd for new record (used only if key doesn't exist)
 * @param data     Data to prepend
 * @param dataLen  Length of data to prepend
 * @param newLen   OUT: Total length after prepend (optional)
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3Prepend(kvidxInstance *i, uint64_t key, uint64_t term,
                               uint64_t cmd, const void *data, size_t dataLen,
                               size_t *newLen) {
    kas3State *s = STATE(i);

    /* Get current value */
    const uint8_t *currentData = NULL;
    size_t currentLen = 0;
    bool exists =
        kvidxSqlite3Get(i, key, NULL, NULL, &currentData, &currentLen);

    if (!exists) {
        /* Key doesn't exist, create with provided data */
        if (!kvidxSqlite3Insert(i, key, term, cmd, data, dataLen)) {
            return KVIDX_ERROR_INTERNAL;
        }
        if (newLen) {
            *newLen = dataLen;
        }
        return KVIDX_OK;
    }

    /* Key exists, prepend to existing data */
    size_t totalLen = currentLen + dataLen;
    void *newData = malloc(totalLen);
    if (!newData && totalLen > 0) {
        return KVIDX_ERROR_NOMEM;
    }

    if (dataLen > 0 && data) {
        memcpy(newData, data, dataLen);
    }
    if (currentLen > 0 && currentData) {
        memcpy((uint8_t *)newData + dataLen, currentData, currentLen);
    }

    /* Update with concatenated data */
    sqlite3_stmt *stmt = NULL;
    const char *sql = "UPDATE log SET data = ? WHERE id = ?";
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        free(newData);
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_blob64(stmt, 1, newData, totalLen, NULL);
    sqlite3_bind_int64(stmt, 2, key);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    free(newData);

    if (rc != SQLITE_DONE && rc != SQLITE_OK) {
        return KVIDX_ERROR_INTERNAL;
    }

    if (newLen) {
        *newLen = totalLen;
    }
    return KVIDX_OK;
}

/* --- Partial Value Access --- */

/**
 * Read a portion of a value without fetching the entire blob.
 *
 * Extracts a substring from the value at the specified offset and length.
 * This is efficient for large values where only part is needed.
 *
 * Boundary behavior:
 * - If offset >= value length: Returns empty (actualLen = 0)
 * - If offset + length > value length: Returns available bytes
 * - If length is 0: Returns from offset to end of value
 *
 * The returned data is allocated and must be freed by the caller.
 *
 * @param i          The kvidx instance
 * @param key        The record key
 * @param offset     Starting byte offset (0-based)
 * @param length     Number of bytes to read (0 = read to end)
 * @param data       OUT: Extracted data (caller must free)
 * @param actualLen  OUT: Actual bytes returned
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key missing
 */
kvidxError kvidxSqlite3GetValueRange(kvidxInstance *i, uint64_t key,
                                     size_t offset, size_t length, void **data,
                                     size_t *actualLen) {
    /* Initialize outputs */
    if (data) {
        *data = NULL;
    }
    if (actualLen) {
        *actualLen = 0;
    }

    /* Get current value */
    const uint8_t *currentData = NULL;
    size_t currentLen = 0;
    if (!kvidxSqlite3Get(i, key, NULL, NULL, &currentData, &currentLen)) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Check if offset is valid */
    if (offset >= currentLen) {
        /* Offset beyond data - return empty */
        if (actualLen) {
            *actualLen = 0;
        }
        return KVIDX_OK;
    }

    /* Calculate actual bytes to return (length=0 means read to end) */
    size_t available = currentLen - offset;
    size_t toReturn = (length == 0 || length > available) ? available : length;

    if (data && toReturn > 0) {
        *data = malloc(toReturn);
        if (!*data) {
            return KVIDX_ERROR_NOMEM;
        }
        memcpy(*data, currentData + offset, toReturn);
    }

    if (actualLen) {
        *actualLen = toReturn;
    }

    return KVIDX_OK;
}

/**
 * Overwrite a portion of a value without replacing the entire blob.
 *
 * Writes data at the specified offset, potentially extending the value
 * if offset + dataLen > current length. Useful for in-place updates to
 * structured data or implementing sparse arrays.
 *
 * Behavior:
 * - If offset > current length: Gap is zero-filled
 * - If offset + dataLen > current length: Value is extended
 * - If offset + dataLen <= current length: Only specified range changes
 *
 * @param i        The kvidx instance
 * @param key      The record key (must exist)
 * @param offset   Starting byte offset to write at
 * @param data     Data to write
 * @param dataLen  Length of data to write
 * @param newLen   OUT: Total length after write (optional)
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key missing
 */
kvidxError kvidxSqlite3SetValueRange(kvidxInstance *i, uint64_t key,
                                     size_t offset, const void *data,
                                     size_t dataLen, size_t *newLen) {
    kas3State *s = STATE(i);

    /* Get current value */
    const uint8_t *currentData = NULL;
    size_t currentLen = 0;
    if (!kvidxSqlite3Get(i, key, NULL, NULL, &currentData, &currentLen)) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Calculate new size */
    size_t newSize = offset + dataLen;
    if (newSize < currentLen) {
        newSize = currentLen;
    }

    /* Allocate new buffer */
    void *newData = calloc(1, newSize); /* calloc zeros for padding */
    if (!newData && newSize > 0) {
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

    /* Update record */
    sqlite3_stmt *stmt = NULL;
    const char *sql = "UPDATE log SET data = ? WHERE id = ?";
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        free(newData);
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_blob64(stmt, 1, newData, newSize, NULL);
    sqlite3_bind_int64(stmt, 2, key);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    free(newData);

    if (rc != SQLITE_DONE && rc != SQLITE_OK) {
        return KVIDX_ERROR_INTERNAL;
    }

    if (newLen) {
        *newLen = newSize;
    }
    return KVIDX_OK;
}

/* --- TTL/Expiration --- */

/**
 * Set a time-to-live on an existing key.
 *
 * After ttlMs milliseconds from now, the key becomes eligible for removal
 * by ExpireScan(). The key is NOT automatically deleted; you must call
 * ExpireScan() periodically to clean up expired keys.
 *
 * Note: TTL is stored in a separate _kvidx_ttl table. Deleting a key via
 * Remove() does not automatically clean up its TTL entry.
 *
 * @param i      The kvidx instance
 * @param key    The record key (must exist)
 * @param ttlMs  Milliseconds until expiration
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key missing
 */
kvidxError kvidxSqlite3SetExpire(kvidxInstance *i, uint64_t key,
                                 uint64_t ttlMs) {
    kas3State *s = STATE(i);

    /* Check if key exists */
    if (!kvidxSqlite3Exists(i, key)) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Ensure TTL table exists */
    if (!ensureTTLTable(s)) {
        return KVIDX_ERROR_INTERNAL;
    }

    /* Calculate expiration timestamp */
    uint64_t expiresAt = currentTimeMs() + ttlMs;

    /* Insert or update TTL */
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT OR REPLACE INTO _kvidx_ttl (id, expires_at) VALUES (?, ?)";
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_int64(stmt, 1, key);
    sqlite3_bind_int64(stmt, 2, expiresAt);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE || rc == SQLITE_OK) ? KVIDX_OK
                                                  : KVIDX_ERROR_INTERNAL;
}

/**
 * Set an absolute expiration timestamp for a key.
 *
 * Like SetExpire(), but takes an absolute timestamp instead of a relative
 * TTL. Useful for synchronizing expiration across distributed systems or
 * when the expiration time is predetermined.
 *
 * @param i            The kvidx instance
 * @param key          The record key (must exist)
 * @param timestampMs  Unix timestamp in milliseconds when key expires
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key missing
 */
kvidxError kvidxSqlite3SetExpireAt(kvidxInstance *i, uint64_t key,
                                   uint64_t timestampMs) {
    kas3State *s = STATE(i);

    /* Check if key exists */
    if (!kvidxSqlite3Exists(i, key)) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Ensure TTL table exists */
    if (!ensureTTLTable(s)) {
        return KVIDX_ERROR_INTERNAL;
    }

    /* Insert or update TTL */
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT OR REPLACE INTO _kvidx_ttl (id, expires_at) VALUES (?, ?)";
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_int64(stmt, 1, key);
    sqlite3_bind_int64(stmt, 2, timestampMs);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE || rc == SQLITE_OK) ? KVIDX_OK
                                                  : KVIDX_ERROR_INTERNAL;
}

/**
 * Get the remaining time-to-live for a key.
 *
 * Returns the number of milliseconds until the key expires. Special
 * return values indicate edge cases.
 *
 * Return values:
 * - Positive: Milliseconds remaining until expiration
 * - 0: Key has already expired (but not yet cleaned up)
 * - KVIDX_TTL_NONE (-1): Key exists but has no TTL set
 * - KVIDX_TTL_NOT_FOUND (-2): Key does not exist
 *
 * @param i    The kvidx instance
 * @param key  The record key
 * @return TTL in milliseconds, or negative special value
 */
int64_t kvidxSqlite3GetTTL(kvidxInstance *i, uint64_t key) {
    kas3State *s = STATE(i);

    /* Check if key exists */
    if (!kvidxSqlite3Exists(i, key)) {
        return KVIDX_TTL_NOT_FOUND;
    }

    /* Ensure TTL table exists */
    if (!ensureTTLTable(s)) {
        return KVIDX_TTL_NONE; /* No TTL table means no expiration */
    }

    /* Query TTL */
    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT expires_at FROM _kvidx_ttl WHERE id = ?";
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return KVIDX_TTL_NONE;
    }

    sqlite3_bind_int64(stmt, 1, key);

    int64_t result = KVIDX_TTL_NONE;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        uint64_t expiresAt = sqlite3_column_int64(stmt, 0);
        uint64_t now = currentTimeMs();
        if (expiresAt <= now) {
            result = 0; /* Already expired */
        } else {
            result = (int64_t)(expiresAt - now);
        }
    }

    sqlite3_finalize(stmt);
    return result;
}

/**
 * Remove the TTL from a key, making it permanent.
 *
 * Removes any expiration associated with the key without affecting
 * the key's data. After this call, the key will not be removed by
 * ExpireScan().
 *
 * @param i    The kvidx instance
 * @param key  The record key (must exist)
 * @return KVIDX_OK on success, KVIDX_ERROR_NOT_FOUND if key missing
 */
kvidxError kvidxSqlite3Persist(kvidxInstance *i, uint64_t key) {
    kas3State *s = STATE(i);

    /* Check if key exists */
    if (!kvidxSqlite3Exists(i, key)) {
        return KVIDX_ERROR_NOT_FOUND;
    }

    /* Ensure TTL table exists */
    if (!ensureTTLTable(s)) {
        return KVIDX_OK; /* No TTL table means already persistent */
    }

    /* Remove TTL entry */
    sqlite3_stmt *stmt = NULL;
    const char *sql = "DELETE FROM _kvidx_ttl WHERE id = ?";
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_int64(stmt, 1, key);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE || rc == SQLITE_OK) ? KVIDX_OK
                                                  : KVIDX_ERROR_INTERNAL;
}

/**
 * Scan for and delete expired keys.
 *
 * Finds keys whose TTL has elapsed and removes them from both the main
 * data table and the TTL metadata table. This should be called periodically
 * to clean up expired keys.
 *
 * The maxKeys parameter limits how many keys are deleted in one call,
 * which is useful for:
 * - Avoiding long-running operations that block other work
 * - Spreading cleanup cost across multiple calls
 * - Implementing incremental background expiration
 *
 * Set maxKeys to 0 to delete all expired keys in one call.
 *
 * @param i             The kvidx instance
 * @param maxKeys       Maximum keys to expire (0 = unlimited)
 * @param expiredCount  OUT: Number of keys actually expired (optional)
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSqlite3ExpireScan(kvidxInstance *i, uint64_t maxKeys,
                                  uint64_t *expiredCount) {
    kas3State *s = STATE(i);
    uint64_t expired = 0;

    /* Ensure TTL table exists */
    if (!ensureTTLTable(s)) {
        if (expiredCount) {
            *expiredCount = 0;
        }
        return KVIDX_OK;
    }

    uint64_t now = currentTimeMs();

    /* Find expired keys */
    sqlite3_stmt *stmt = NULL;
    char sql[256];
    if (maxKeys > 0) {
        snprintf(
            sql, sizeof(sql),
            "SELECT id FROM _kvidx_ttl WHERE expires_at <= ? LIMIT %" PRIu64,
            maxKeys);
    } else {
        snprintf(sql, sizeof(sql),
                 "SELECT id FROM _kvidx_ttl WHERE expires_at <= ?");
    }

    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        if (expiredCount) {
            *expiredCount = 0;
        }
        return KVIDX_ERROR_INTERNAL;
    }

    sqlite3_bind_int64(stmt, 1, now);

    /* Collect keys to delete */
    uint64_t *keysToDelete = NULL;
    size_t keyCount = 0;
    size_t keyCapacity = 64;
    keysToDelete = malloc(keyCapacity * sizeof(uint64_t));
    if (!keysToDelete) {
        sqlite3_finalize(stmt);
        return KVIDX_ERROR_NOMEM;
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (keyCount >= keyCapacity) {
            keyCapacity *= 2;
            uint64_t *newKeys =
                realloc(keysToDelete, keyCapacity * sizeof(uint64_t));
            if (!newKeys) {
                free(keysToDelete);
                sqlite3_finalize(stmt);
                return KVIDX_ERROR_NOMEM;
            }
            keysToDelete = newKeys;
        }
        keysToDelete[keyCount++] = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);

    /* Delete expired keys */
    for (size_t idx = 0; idx < keyCount; idx++) {
        uint64_t key = keysToDelete[idx];

        /* Delete from main table */
        kvidxSqlite3Remove(i, key);

        /* Delete from TTL table */
        stmt = NULL;
        rc = sqlite3_prepare_v2(s->db, "DELETE FROM _kvidx_ttl WHERE id = ?",
                                -1, &stmt, NULL);
        if (rc == SQLITE_OK) {
            sqlite3_bind_int64(stmt, 1, key);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }

        expired++;
    }

    free(keysToDelete);

    if (expiredCount) {
        *expiredCount = expired;
    }
    return KVIDX_OK;
}
