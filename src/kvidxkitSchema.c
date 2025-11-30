#include "kvidxkitSchema.h"

#include "../deps/sqlite3/src/sqlite3.h"

#include <stdio.h>
#include <string.h>

/* ============================================================================
 * Schema Table DDL
 * ============================================================================
 */

static const char *SCHEMA_TABLE_SQL =
    "CREATE TABLE IF NOT EXISTS _kvidx_schema ("
    "version INTEGER PRIMARY KEY, "
    "applied_at TEXT DEFAULT CURRENT_TIMESTAMP, "
    "description TEXT"
    ");";

static const char *GET_VERSION_SQL = "SELECT MAX(version) FROM _kvidx_schema;";

static const char *INSERT_VERSION_SQL =
    "INSERT INTO _kvidx_schema (version, description) VALUES (?, ?);";

static const char *GET_VERSIONS_SQL =
    "SELECT version FROM _kvidx_schema ORDER BY version ASC;";

/* ============================================================================
 * Schema Initialization
 * ============================================================================
 */

kvidxError kvidxSchemaInit(sqlite3 *db) {
    if (!db) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    char *errMsg = NULL;
    int rc = sqlite3_exec(db, SCHEMA_TABLE_SQL, NULL, NULL, &errMsg);
    if (rc != SQLITE_OK) {
        if (errMsg) {
            sqlite3_free(errMsg);
        }
        return KVIDX_ERROR_IO;
    }

    return KVIDX_OK;
}

/* ============================================================================
 * Version Queries
 * ============================================================================
 */

kvidxError kvidxSchemaVersion(sqlite3 *db, uint32_t *version) {
    if (!db || !version) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Ensure schema table exists */
    kvidxError err = kvidxSchemaInit(db);
    if (err != KVIDX_OK) {
        return err;
    }

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, GET_VERSION_SQL, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return KVIDX_ERROR_IO;
    }

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        /* NULL if no rows in table */
        if (sqlite3_column_type(stmt, 0) == SQLITE_NULL) {
            *version = 0;
        } else {
            *version = (uint32_t)sqlite3_column_int64(stmt, 0);
        }
    } else {
        *version = 0;
    }

    sqlite3_finalize(stmt);
    return KVIDX_OK;
}

bool kvidxSchemaNeedsMigration(sqlite3 *db, uint32_t targetVersion) {
    uint32_t current = 0;
    if (kvidxSchemaVersion(db, &current) != KVIDX_OK) {
        return true; /* Assume migration needed on error */
    }
    return current < targetVersion;
}

/* ============================================================================
 * Migration Application
 * ============================================================================
 */

static kvidxError applyMigration(sqlite3 *db, const kvidxMigration *migration) {
    char *errMsg = NULL;

    /* Begin transaction */
    int rc = sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, &errMsg);
    if (rc != SQLITE_OK) {
        if (errMsg) {
            sqlite3_free(errMsg);
        }
        return KVIDX_ERROR_IO;
    }

    /* Apply migration SQL */
    if (migration->upSQL && migration->upSQL[0] != '\0') {
        rc = sqlite3_exec(db, migration->upSQL, NULL, NULL, &errMsg);
        if (rc != SQLITE_OK) {
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            if (errMsg) {
                sqlite3_free(errMsg);
            }
            return KVIDX_ERROR_IO;
        }
    }

    /* Record migration */
    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db, INSERT_VERSION_SQL, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return KVIDX_ERROR_IO;
    }

    sqlite3_bind_int64(stmt, 1, migration->version);
    sqlite3_bind_text(stmt, 2,
                      migration->description ? migration->description : "", -1,
                      SQLITE_STATIC);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return KVIDX_ERROR_IO;
    }

    /* Commit transaction */
    rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, &errMsg);
    if (rc != SQLITE_OK) {
        if (errMsg) {
            sqlite3_free(errMsg);
        }
        return KVIDX_ERROR_IO;
    }

    return KVIDX_OK;
}

kvidxError kvidxSchemaApply(sqlite3 *db, const kvidxMigration *migrations,
                            size_t count, uint32_t targetVersion) {
    return kvidxSchemaApplyWithCallback(db, migrations, count, targetVersion,
                                        NULL, NULL);
}

kvidxError kvidxSchemaApplyWithCallback(sqlite3 *db,
                                        const kvidxMigration *migrations,
                                        size_t count, uint32_t targetVersion,
                                        kvidxMigrationCallback callback,
                                        void *userData) {
    if (!db || (count > 0 && !migrations)) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Ensure schema table exists */
    kvidxError err = kvidxSchemaInit(db);
    if (err != KVIDX_OK) {
        return err;
    }

    /* Get current version */
    uint32_t currentVersion = 0;
    err = kvidxSchemaVersion(db, &currentVersion);
    if (err != KVIDX_OK) {
        return err;
    }

    /* Apply migrations in order */
    for (size_t i = 0; i < count; i++) {
        const kvidxMigration *m = &migrations[i];

        /* Skip already-applied migrations */
        if (m->version <= currentVersion) {
            continue;
        }

        /* Stop at target version */
        if (m->version > targetVersion) {
            break;
        }

        /* Apply migration */
        err = applyMigration(db, m);

        if (callback) {
            callback(m->version, m->description, err == KVIDX_OK, userData);
        }

        if (err != KVIDX_OK) {
            return err;
        }

        currentVersion = m->version;
    }

    return KVIDX_OK;
}

/* ============================================================================
 * Table Creation Helper
 * ============================================================================
 */

kvidxError kvidxSchemaCreateTables(sqlite3 *db, const kvidxTableDef *tables,
                                   size_t count) {
    if (!db || (count > 0 && !tables)) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    char sqlBuf[65536];
    char *errMsg = NULL;

    for (size_t i = 0; i < count; i++) {
        /* Generate CREATE TABLE */
        if (!kvidxGenCreateTable(&tables[i], sqlBuf, sizeof(sqlBuf))) {
            return KVIDX_ERROR_INTERNAL;
        }

        int rc = sqlite3_exec(db, sqlBuf, NULL, NULL, &errMsg);
        if (rc != SQLITE_OK) {
            if (errMsg) {
                sqlite3_free(errMsg);
            }
            return KVIDX_ERROR_IO;
        }

        /* Generate CREATE INDEX statements */
        if (tables[i].indexCount > 0) {
            if (!kvidxGenCreateIndexes(&tables[i], sqlBuf, sizeof(sqlBuf))) {
                return KVIDX_ERROR_INTERNAL;
            }

            rc = sqlite3_exec(db, sqlBuf, NULL, NULL, &errMsg);
            if (rc != SQLITE_OK) {
                if (errMsg) {
                    sqlite3_free(errMsg);
                }
                return KVIDX_ERROR_IO;
            }
        }
    }

    return KVIDX_OK;
}

/* ============================================================================
 * Version History
 * ============================================================================
 */

kvidxError kvidxSchemaGetAppliedVersions(sqlite3 *db, uint32_t *versions,
                                         size_t maxCount, size_t *actualCount) {
    if (!db || !versions || !actualCount) {
        return KVIDX_ERROR_INVALID_ARGUMENT;
    }

    /* Ensure schema table exists */
    kvidxError err = kvidxSchemaInit(db);
    if (err != KVIDX_OK) {
        return err;
    }

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, GET_VERSIONS_SQL, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return KVIDX_ERROR_IO;
    }

    size_t count = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW && count < maxCount) {
        versions[count++] = (uint32_t)sqlite3_column_int64(stmt, 0);
    }

    sqlite3_finalize(stmt);
    *actualCount = count;

    return KVIDX_OK;
}
