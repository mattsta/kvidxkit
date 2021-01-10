#include "kvidxkitAdapterSqlite3.h"
#include "../deps/sqlite3/src/sqlite3.h"
#include "kvidxkitAdapterSqlite3Helper.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

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
        return false;
    }

    if (data) {
        *data = blob;
    }

    /* Sqlite3 lengths are int32_t, so we
     * are limited to less than 2 GB data sizes here.
     * See: https://www.sqlite.org/limits.html#max_length */
    if (len) {
        *len = sqlite3_column_bytes(stmt, col);
    }

    return true;
}

bool kvidxSqlite3Begin(kvidxInstance *i) {
    kas3State *s = STATE(i);
    return sqlite3_step(s->begin) == SQLITE_DONE;
}

bool kvidxSqlite3Commit(kvidxInstance *i) {
    kas3State *s = STATE(i);
    return sqlite3_step(s->commit) == SQLITE_DONE;
}

/* Note: return parameter 'data' is ONLY valid until the next Get() call. */
bool kvidxSqlite3Get(kvidxInstance *i, uint64_t key, uint64_t *term,
                     uint64_t *cmd, const uint8_t **data, size_t *len) {
    kas3State *s = STATE(i);
    sqlite3_bind_int64(s->get, 1, key);
    if (sqlite3_step(s->get) == SQLITE_ROW) {
        if (term) {
            *term = sqlite3_column_int64(s->get, 0);
        }

        if (cmd) {
            *cmd = sqlite3_column_int64(s->get, 1);
        }

        extractBlob(s->get, 2, data, len);
        sqlite3_reset(s->get);
        return true;
    }

    sqlite3_reset(s->get);
    return false;
}

/* abstraction for get{Prev,Next} to avoid copy/paste same function body. */
static bool fourColumnExtract(sqlite3_stmt *stmt, uint64_t lookupId,
                              uint64_t *key, uint64_t *term, uint64_t *cmd,
                              const uint8_t **data, size_t *len) {
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
        sqlite3_reset(stmt);
        return true;
    }

    sqlite3_reset(stmt);
    return false;
}

bool kvidxSqlite3GetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                         uint64_t *prevTerm, uint64_t *cmd,
                         const uint8_t **data, size_t *len) {
    kas3State *s = STATE(i);
    return fourColumnExtract(s->getPrev, nextKey, prevKey, prevTerm, cmd, data,
                             len);
}

bool kvidxSqlite3GetNext(kvidxInstance *i, uint64_t previousKey,
                         uint64_t *nextKey, uint64_t *nextTerm, uint64_t *cmd,
                         const uint8_t **data, size_t *len) {
    kas3State *s = STATE(i);
    return fourColumnExtract(s->getNext, previousKey, nextKey, nextTerm, cmd,
                             data, len);
}

bool kvidxSqlite3Exists(kvidxInstance *i, uint64_t key) {
    kas3State *s = STATE(i);
    sqlite3_bind_int64(s->exists, 1, key);
    sqlite3_step(s->exists);
    const bool exists = sqlite3_column_int(s->exists, 0);
    sqlite3_reset(s->exists);
    return exists;
}

bool kvidxSqlite3ExistsDual(kvidxInstance *i, uint64_t key, uint64_t term) {
    kas3State *s = STATE(i);
    sqlite3_bind_int64(s->existsDual, 1, key);
    sqlite3_bind_int64(s->existsDual, 2, term);
    sqlite3_step(s->existsDual);
    const bool exists = sqlite3_column_int(s->existsDual, 0);
    sqlite3_reset(s->existsDual);
    return exists;
}

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
    sqlite3_reset(s->insert);
    return (done == SQLITE_DONE) || (done == SQLITE_OK);
}

bool kvidxSqlite3Remove(kvidxInstance *i, uint64_t key) {
    kas3State *s = STATE(i);
    sqlite3_bind_int64(s->remove, 1, key);
    const bool removed = sqlite3_step(s->remove) == SQLITE_DONE;
    sqlite3_reset(s->remove);
    return removed;
}

bool kvidxSqlite3RemoveAfterNInclusive(kvidxInstance *i, uint64_t key) {
    kas3State *s = STATE(i);
    sqlite3_bind_int64(s->removeAfterNInclusive, 1, key);
    const bool removed = sqlite3_step(s->removeAfterNInclusive) == SQLITE_DONE;
    sqlite3_reset(s->removeAfterNInclusive);
    return removed;
}

bool kvidxSqlite3RemoveBeforeNInclusive(kvidxInstance *i, uint64_t key) {
    kas3State *s = STATE(i);
    sqlite3_bind_int64(s->removeBeforeNInclusive, 1, key);
    const bool removed = sqlite3_step(s->removeBeforeNInclusive) == SQLITE_DONE;
    sqlite3_reset(s->removeBeforeNInclusive);
    return removed;
}

bool kvidxSqlite3Max(kvidxInstance *i, uint64_t *key) {
    kas3State *s = STATE(i);
    if (sqlite3_step(s->maxKey) == SQLITE_ROW) {
        if (sqlite3_column_type(s->maxKey, 0) == SQLITE_NULL) {
            /* NULL result means we have no keys! */
            sqlite3_reset(s->maxKey);
            return false;
        }

        if (key) {
            *key = sqlite3_column_int64(s->maxKey, 0);
        }

        sqlite3_reset(s->maxKey);
        return true;
    }

    sqlite3_reset(s->maxKey);
    return false;
}

/* need to allow modes: OFF, SAFE|NORMAL, PARANOID|FULL */
bool kvidxSqlite3Fsync(kvidxInstance *i) {
    kas3State *s = STATE(i);
    return sqlite3_exec(s->db, "PRAGMA synchronous = NORMAL;", NULL, NULL,
                        NULL) == SQLITE_OK;
}

/* ====================================================================
 * Bring-Up
 * ==================================================================== */
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

static const kvidxkitAdapterSqlite3HelperTableDesc tables[] = {
    {.name = "controlBlock",
     .colCount = 3,
     .col = COLS{"id", "what", "how"},
     .type = TYPES{HT_INTEGER | HT_PRIMARY_KEY, HT_STRING, HT_STRING},
     .indexCount = 1,
     .index = REFS{"what"}},

    {.name = "log",
     .colCount = 5,
     .col = COLS{"id", "created", "term", "cmd", "data"},
     .type = TYPES{HT_IPK, HT_INTEGER, HT_INTEGER, HT_INTEGER, HT_BLOB}}};

bool createLogTable(sqlite3 *db) {
    kvidxkitAdapterSqlite3HelperTablesCreate(db, tables,
                                             sizeof(tables) / sizeof(*tables));
    return true;
}

bool kvidxSqlite3Open(kvidxInstance *i, const char *filename,
                      const char **errStr) {
    sqlite3 *db = NULL;

    /* Quoth the sqlite3 docs:
     * =======================
     * If all database clients (readers and writers) are located in the same OS
     * process, and if that OS is a Unix variant, then it can be more efficient
     * to the built-in VFS "unix-excl" instead of the default "unix". This is
     * because it uses more efficient locking primitives.
     * ======================= */
    int err = sqlite3_open_v2(
        filename, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix-excl");
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
