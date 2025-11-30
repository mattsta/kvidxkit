#include "../deps/sqlite3/src/sqlite3.h"
#include "ctest.h"
#include "kvidxkitSchema.h"
#include "kvidxkitTableDesc.h"

#include <string.h>
#include <unistd.h>

/* ============================================================================
 * Test Helpers
 * ============================================================================
 */

static void makeTestFilename(char *buf, size_t bufSize, const char *testName) {
    snprintf(buf, bufSize, "test-tabledesc-%s-%d.sqlite3", testName, getpid());
}

static void cleanupTestFile(const char *filename) {
    unlink(filename);
    /* Also clean up WAL and SHM files */
    char walFile[256];
    char shmFile[256];
    snprintf(walFile, sizeof(walFile), "%s-wal", filename);
    snprintf(shmFile, sizeof(shmFile), "%s-shm", filename);
    unlink(walFile);
    unlink(shmFile);
}

/* ============================================================================
 * Flag Decoding Tests
 * ============================================================================
 */

static void testFlagDecoding(uint32_t *err) {
    char buf[1024];

    TEST("Flag: INTEGER base type") {
        kvidxColDef col = COL("x", KVIDX_COL_INTEGER);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strcmp(buf, "INTEGER") != 0) {
            ERR("Expected 'INTEGER', got '%s'", buf);
        }
    }

    TEST("Flag: TEXT base type") {
        kvidxColDef col = COL("x", KVIDX_COL_TEXT);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strcmp(buf, "TEXT") != 0) {
            ERR("Expected 'TEXT', got '%s'", buf);
        }
    }

    TEST("Flag: BLOB base type") {
        kvidxColDef col = COL("x", KVIDX_COL_BLOB);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strcmp(buf, "BLOB") != 0) {
            ERR("Expected 'BLOB', got '%s'", buf);
        }
    }

    TEST("Flag: REAL base type") {
        kvidxColDef col = COL("x", KVIDX_COL_REAL);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strcmp(buf, "REAL") != 0) {
            ERR("Expected 'REAL', got '%s'", buf);
        }
    }

    TEST("Flag: INTEGER PRIMARY KEY") {
        kvidxColDef col = COL("id", KVIDX_COL_PK);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strcmp(buf, "INTEGER PRIMARY KEY") != 0) {
            ERR("Expected 'INTEGER PRIMARY KEY', got '%s'", buf);
        }
    }

    TEST("Flag: INTEGER PRIMARY KEY AUTOINCREMENT") {
        kvidxColDef col = COL("id", KVIDX_COL_PK_AUTO);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strcmp(buf, "INTEGER PRIMARY KEY AUTOINCREMENT") != 0) {
            ERR("Expected 'INTEGER PRIMARY KEY AUTOINCREMENT', got '%s'", buf);
        }
    }

    TEST("Flag: TEXT NOT NULL") {
        kvidxColDef col = COL("name", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strcmp(buf, "TEXT NOT NULL") != 0) {
            ERR("Expected 'TEXT NOT NULL', got '%s'", buf);
        }
    }

    TEST("Flag: INTEGER UNIQUE") {
        kvidxColDef col = COL("code", KVIDX_COL_INTEGER | KVIDX_COL_UNIQUE);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strcmp(buf, "INTEGER UNIQUE") != 0) {
            ERR("Expected 'INTEGER UNIQUE', got '%s'", buf);
        }
    }

    TEST("Flag: INTEGER NOT NULL UNIQUE (multiple constraints)") {
        kvidxColDef col =
            COL("x", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL | KVIDX_COL_UNIQUE);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strcmp(buf, "INTEGER NOT NULL UNIQUE") != 0) {
            ERR("Expected 'INTEGER NOT NULL UNIQUE', got '%s'", buf);
        }
    }
}

/* ============================================================================
 * Foreign Key Tests
 * ============================================================================
 */

static void testForeignKeys(uint32_t *err) {
    char buf[1024];

    TEST("FK: Simple reference") {
        kvidxColDef col = COL_FK("user_id", KVIDX_COL_FK, "users");
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strstr(buf, "INTEGER") == NULL ||
                   strstr(buf, "REFERENCES users") == NULL) {
            ERR("Expected 'INTEGER ... REFERENCES users', got '%s'", buf);
        }
    }

    TEST("FK: With CASCADE DELETE") {
        kvidxColDef col = COL_FK("user_id", KVIDX_COL_FK_CASCADE, "users");
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strstr(buf, "REFERENCES users") == NULL ||
                   strstr(buf, "ON DELETE CASCADE") == NULL) {
            ERR("Expected REFERENCES and CASCADE, got '%s'", buf);
        }
    }

    TEST("FK: With DEFERRED") {
        kvidxColDef col =
            COL_FK("user_id", KVIDX_COL_FK_CASCADE_DEFERRED, "users");
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strstr(buf, "DEFERRABLE INITIALLY DEFERRED") == NULL) {
            ERR("Expected DEFERRABLE, got '%s'", buf);
        }
    }
}

/* ============================================================================
 * Default Value Tests
 * ============================================================================
 */

static void testDefaultValues(uint32_t *err) {
    char buf[1024];

    TEST("Default: Integer value") {
        kvidxColDef col = COL_DEFAULT_INT("status", KVIDX_COL_INTEGER, 0);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strstr(buf, "DEFAULT 0") == NULL) {
            ERR("Expected 'DEFAULT 0', got '%s'", buf);
        }
    }

    TEST("Default: Negative integer") {
        kvidxColDef col = COL_DEFAULT_INT("offset", KVIDX_COL_INTEGER, -100);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strstr(buf, "DEFAULT -100") == NULL) {
            ERR("Expected 'DEFAULT -100', got '%s'", buf);
        }
    }

    TEST("Default: Text value") {
        kvidxColDef col = COL_DEFAULT_TEXT("name", KVIDX_COL_TEXT, "unknown");
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strstr(buf, "DEFAULT 'unknown'") == NULL) {
            ERR("Expected \"DEFAULT 'unknown'\", got '%s'", buf);
        }
    }

    TEST("Default: NULL value") {
        kvidxColDef col = COL_DEFAULT_NULL("optional", KVIDX_COL_TEXT);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strstr(buf, "DEFAULT NULL") == NULL) {
            ERR("Expected 'DEFAULT NULL', got '%s'", buf);
        }
    }

    TEST("Default: Expression (CURRENT_TIMESTAMP)") {
        kvidxColDef col =
            COL_DEFAULT_EXPR("created", KVIDX_COL_TEXT, "CURRENT_TIMESTAMP");
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strstr(buf, "DEFAULT CURRENT_TIMESTAMP") == NULL) {
            ERR("Expected 'DEFAULT CURRENT_TIMESTAMP', got '%s'", buf);
        }
    }

    TEST("Default: Real value") {
        kvidxColDef col = COL_DEFAULT_REAL("rate", KVIDX_COL_REAL, 3.14);
        int len = kvidxGenColTypeSql(&col, buf, sizeof(buf));
        if (len < 0) {
            ERRR("Generation failed");
        } else if (strstr(buf, "DEFAULT 3.14") == NULL) {
            ERR("Expected 'DEFAULT 3.14', got '%s'", buf);
        }
    }
}

/* ============================================================================
 * Validation Tests
 * ============================================================================
 */

static void testValidation(uint32_t *err) {
    TEST("Validation: Valid simple column") {
        kvidxColDef col = COL("x", KVIDX_COL_INTEGER);
        if (!kvidxColDefIsValid(&col)) {
            ERRR("Should be valid");
        }
    }

    TEST("Validation: Invalid - no base type") {
        kvidxColDef col = COL("x", KVIDX_COL_NOT_NULL);
        if (kvidxColDefIsValid(&col)) {
            ERRR("Should be invalid (no base type)");
        }
    }

    TEST("Validation: Invalid - multiple base types") {
        kvidxColDef col = COL("x", KVIDX_COL_INTEGER | KVIDX_COL_TEXT);
        if (kvidxColDefIsValid(&col)) {
            ERRR("Should be invalid (multiple base types)");
        }
    }

    TEST("Validation: Invalid - AUTOINCREMENT without PK") {
        kvidxColDef col = COL("x", KVIDX_COL_INTEGER | KVIDX_COL_AUTOINCREMENT);
        if (kvidxColDefIsValid(&col)) {
            ERRR("Should be invalid (AUTOINCREMENT requires PRIMARY KEY)");
        }
    }

    TEST("Validation: Invalid - REFERENCES without refTable") {
        kvidxColDef col = COL("x", KVIDX_COL_INTEGER | KVIDX_COL_REFERENCES);
        if (kvidxColDefIsValid(&col)) {
            ERRR("Should be invalid (REFERENCES needs refTable)");
        }
    }

    TEST("Validation: Invalid - CASCADE without REFERENCES") {
        kvidxColDef col =
            COL("x", KVIDX_COL_INTEGER | KVIDX_COL_CASCADE_DELETE);
        if (kvidxColDefIsValid(&col)) {
            ERRR("Should be invalid (CASCADE needs REFERENCES)");
        }
    }

    TEST("Validation: Invalid - NULL default with NOT NULL") {
        kvidxColDef col =
            COL_DEFAULT_NULL("x", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL);
        if (kvidxColDefIsValid(&col)) {
            ERRR("Should be invalid (NULL default with NOT NULL)");
        }
    }

    TEST("Validation: Valid table") {
        kvidxColDef cols[] = {
            COL("id", KVIDX_COL_PK),
            COL("name", KVIDX_COL_TEXT),
        };
        kvidxTableDef table = {
            .name = "test",
            .columns = cols,
            .colCount = 2,
        };
        if (!kvidxTableDefIsValid(&table)) {
            ERRR("Table should be valid");
        }
    }

    TEST("Validation: Invalid table - no name") {
        kvidxColDef cols[] = {COL("id", KVIDX_COL_PK)};
        kvidxTableDef table = {
            .name = NULL,
            .columns = cols,
            .colCount = 1,
        };
        if (kvidxTableDefIsValid(&table)) {
            ERRR("Table should be invalid (no name)");
        }
    }

    TEST("Validation: Invalid table - index references non-existent column") {
        kvidxColDef cols[] = {COL("id", KVIDX_COL_PK)};
        kvidxIndexDef idxs[] = {INDEX("nonexistent")};
        kvidxTableDef table = {
            .name = "test",
            .columns = cols,
            .colCount = 1,
            .indexes = idxs,
            .indexCount = 1,
        };
        if (kvidxTableDefIsValid(&table)) {
            ERRR("Table should be invalid (bad index column)");
        }
    }
}

/* ============================================================================
 * CREATE TABLE Generation Tests
 * ============================================================================
 */

static void testCreateTable(uint32_t *err) {
    char buf[4096];

    TEST("CREATE TABLE: Simple table") {
        kvidxColDef cols[] = {
            COL("id", KVIDX_COL_PK),
            COL("name", KVIDX_COL_TEXT),
        };
        kvidxTableDef table = {
            .name = "users",
            .columns = cols,
            .colCount = 2,
        };

        if (!kvidxGenCreateTable(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strstr(buf, "CREATE TABLE IF NOT EXISTS users") == NULL) {
                ERR("Missing CREATE TABLE, got '%s'", buf);
            }
            if (strstr(buf, "id INTEGER PRIMARY KEY") == NULL) {
                ERR("Missing id column, got '%s'", buf);
            }
            if (strstr(buf, "name TEXT") == NULL) {
                ERR("Missing name column, got '%s'", buf);
            }
        }
    }

    TEST("CREATE TABLE: With all column types") {
        kvidxColDef cols[] = {
            COL("id", KVIDX_COL_PK_AUTO),
            COL("count", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL),
            COL("name", KVIDX_COL_TEXT | KVIDX_COL_UNIQUE),
            COL("data", KVIDX_COL_BLOB),
            COL("rate", KVIDX_COL_REAL),
        };
        kvidxTableDef table = {
            .name = "mixed",
            .columns = cols,
            .colCount = 5,
        };

        if (!kvidxGenCreateTable(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        }
    }

    TEST("CREATE TABLE: WITHOUT ROWID") {
        kvidxColDef cols[] = {
            COL("key", KVIDX_COL_TEXT | KVIDX_COL_PRIMARY_KEY),
            COL("value", KVIDX_COL_BLOB),
        };
        kvidxTableDef table = {
            .name = "kv",
            .columns = cols,
            .colCount = 2,
            .withoutRowid = true,
        };

        if (!kvidxGenCreateTable(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else if (strstr(buf, "WITHOUT ROWID") == NULL) {
            ERR("Missing WITHOUT ROWID, got '%s'", buf);
        }
    }
}

/* ============================================================================
 * CREATE INDEX Generation Tests
 * ============================================================================
 */

static void testCreateIndex(uint32_t *err) {
    char buf[4096];

    TEST("CREATE INDEX: Single column") {
        kvidxColDef cols[] = {
            COL("id", KVIDX_COL_PK),
            COL("email", KVIDX_COL_TEXT),
        };
        kvidxIndexDef idxs[] = {INDEX("email")};
        kvidxTableDef table = {
            .name = "users",
            .columns = cols,
            .colCount = 2,
            .indexes = idxs,
            .indexCount = 1,
        };

        if (!kvidxGenCreateIndexes(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strstr(buf, "CREATE INDEX IF NOT EXISTS") == NULL) {
                ERR("Missing CREATE INDEX, got '%s'", buf);
            }
            if (strstr(buf, "users_email_idx") == NULL) {
                ERR("Missing auto-generated name, got '%s'", buf);
            }
            if (strstr(buf, "ON users (email)") == NULL) {
                ERR("Missing ON clause, got '%s'", buf);
            }
        }
    }

    TEST("CREATE INDEX: UNIQUE") {
        kvidxColDef cols[] = {
            COL("id", KVIDX_COL_PK),
            COL("email", KVIDX_COL_TEXT),
        };
        kvidxIndexDef idxs[] = {INDEX_UNIQUE("email")};
        kvidxTableDef table = {
            .name = "users",
            .columns = cols,
            .colCount = 2,
            .indexes = idxs,
            .indexCount = 1,
        };

        if (!kvidxGenCreateIndexes(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else if (strstr(buf, "CREATE UNIQUE INDEX") == NULL) {
            ERR("Missing UNIQUE, got '%s'", buf);
        }
    }

    TEST("CREATE INDEX: Multi-column") {
        kvidxColDef cols[] = {
            COL("id", KVIDX_COL_PK),
            COL("tenant", KVIDX_COL_INTEGER),
            COL("user", KVIDX_COL_INTEGER),
        };
        kvidxIndexDef idxs[] = {INDEX("tenant", "user")};
        kvidxTableDef table = {
            .name = "access",
            .columns = cols,
            .colCount = 3,
            .indexes = idxs,
            .indexCount = 1,
        };

        if (!kvidxGenCreateIndexes(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strstr(buf, "access_tenant_user_idx") == NULL) {
                ERR("Missing composite name, got '%s'", buf);
            }
            if (strstr(buf, "(tenant, user)") == NULL) {
                ERR("Missing columns, got '%s'", buf);
            }
        }
    }

    TEST("CREATE INDEX: Named index") {
        kvidxColDef cols[] = {
            COL("id", KVIDX_COL_PK),
            COL("email", KVIDX_COL_TEXT),
        };
        kvidxIndexDef idxs[] = {INDEX_NAMED("my_custom_idx", "email")};
        kvidxTableDef table = {
            .name = "users",
            .columns = cols,
            .colCount = 2,
            .indexes = idxs,
            .indexCount = 1,
        };

        if (!kvidxGenCreateIndexes(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else if (strstr(buf, "my_custom_idx") == NULL) {
            ERR("Missing custom name, got '%s'", buf);
        }
    }
}

/* ============================================================================
 * Statement Generation Tests
 * ============================================================================
 */

static void testStatementGeneration(uint32_t *err) {
    char buf[4096];

    kvidxColDef cols[] = {
        COL("id", KVIDX_COL_PK),
        COL("name", KVIDX_COL_TEXT),
        COL("age", KVIDX_COL_INTEGER),
    };
    kvidxTableDef table = {
        .name = "users",
        .columns = cols,
        .colCount = 3,
    };

    TEST("Statement: INSERT") {
        if (!kvidxGenInsert(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strstr(buf, "INSERT INTO users") == NULL) {
                ERR("Missing INSERT, got '%s'", buf);
            }
            if (strstr(buf, "(id, name, age)") == NULL) {
                ERR("Missing columns, got '%s'", buf);
            }
            if (strstr(buf, "VALUES (?, ?, ?)") == NULL) {
                ERR("Missing placeholders, got '%s'", buf);
            }
        }
    }

    TEST("Statement: SELECT BY ID") {
        if (!kvidxGenSelectById(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strstr(buf, "SELECT * FROM users WHERE id = ?") == NULL) {
                ERR("Wrong query, got '%s'", buf);
            }
        }
    }

    TEST("Statement: SELECT ALL") {
        if (!kvidxGenSelectAll(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strcmp(buf, "SELECT * FROM users;") != 0) {
                ERR("Wrong query, got '%s'", buf);
            }
        }
    }

    TEST("Statement: UPDATE BY ID") {
        if (!kvidxGenUpdateById(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strstr(buf, "UPDATE users SET") == NULL) {
                ERR("Missing UPDATE, got '%s'", buf);
            }
            if (strstr(buf, "name = ?") == NULL) {
                ERR("Missing name column, got '%s'", buf);
            }
            if (strstr(buf, "age = ?") == NULL) {
                ERR("Missing age column, got '%s'", buf);
            }
            if (strstr(buf, "WHERE id = ?") == NULL) {
                ERR("Missing WHERE, got '%s'", buf);
            }
        }
    }

    TEST("Statement: DELETE BY ID") {
        if (!kvidxGenDeleteById(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strstr(buf, "DELETE FROM users WHERE id = ?") == NULL) {
                ERR("Wrong query, got '%s'", buf);
            }
        }
    }

    TEST("Statement: COUNT") {
        if (!kvidxGenCount(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strcmp(buf, "SELECT COUNT(*) FROM users;") != 0) {
                ERR("Wrong query, got '%s'", buf);
            }
        }
    }

    TEST("Statement: MAX ID") {
        if (!kvidxGenMaxId(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strcmp(buf, "SELECT MAX(id) FROM users;") != 0) {
                ERR("Wrong query, got '%s'", buf);
            }
        }
    }

    TEST("Statement: MIN ID") {
        if (!kvidxGenMinId(&table, buf, sizeof(buf))) {
            ERRR("Generation failed");
        } else {
            if (strcmp(buf, "SELECT MIN(id) FROM users;") != 0) {
                ERR("Wrong query, got '%s'", buf);
            }
        }
    }
}

/* ============================================================================
 * Schema Versioning Tests
 * ============================================================================
 */

static void testSchemaVersioning(uint32_t *err) {
    char filename[256];
    makeTestFilename(filename, sizeof(filename), "schema");

    sqlite3 *db = NULL;
    int rc = sqlite3_open(filename, &db);
    if (rc != SQLITE_OK) {
        ERRR("Failed to open database");
        return;
    }

    TEST("Schema: Initial version is 0") {
        uint32_t version = 999;
        kvidxError e = kvidxSchemaVersion(db, &version);
        if (e != KVIDX_OK) {
            ERRR("kvidxSchemaVersion failed");
        } else if (version != 0) {
            ERR("Expected version 0, got %u", version);
        }
    }

    TEST("Schema: Needs migration when empty") {
        if (!kvidxSchemaNeedsMigration(db, 1)) {
            ERRR("Should need migration");
        }
    }

    kvidxMigration migrations[] = {
        {
            .version = 1,
            .description = "Create users table",
            .upSQL = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
        },
        {
            .version = 2,
            .description = "Add email column",
            .upSQL = "ALTER TABLE users ADD COLUMN email TEXT;",
        },
    };

    TEST("Schema: Apply migration 1") {
        kvidxError e = kvidxSchemaApply(db, migrations, 2, 1);
        if (e != KVIDX_OK) {
            ERRR("Migration failed");
        }
        uint32_t version = 0;
        kvidxSchemaVersion(db, &version);
        if (version != 1) {
            ERR("Expected version 1, got %u", version);
        }
    }

    TEST("Schema: Apply migration 2") {
        kvidxError e = kvidxSchemaApply(db, migrations, 2, 2);
        if (e != KVIDX_OK) {
            ERRR("Migration failed");
        }
        uint32_t version = 0;
        kvidxSchemaVersion(db, &version);
        if (version != 2) {
            ERR("Expected version 2, got %u", version);
        }
    }

    TEST("Schema: No migration needed at target version") {
        if (kvidxSchemaNeedsMigration(db, 2)) {
            ERRR("Should not need migration");
        }
    }

    TEST("Schema: Re-apply is idempotent") {
        kvidxError e = kvidxSchemaApply(db, migrations, 2, 2);
        if (e != KVIDX_OK) {
            ERRR("Re-apply failed");
        }
        uint32_t version = 0;
        kvidxSchemaVersion(db, &version);
        if (version != 2) {
            ERR("Expected version 2, got %u", version);
        }
    }

    TEST("Schema: Get applied versions") {
        uint32_t versions[10];
        size_t count = 0;
        kvidxError e = kvidxSchemaGetAppliedVersions(db, versions, 10, &count);
        if (e != KVIDX_OK) {
            ERRR("Get versions failed");
        } else if (count != 2) {
            ERR("Expected 2 versions, got %zu", count);
        } else if (versions[0] != 1 || versions[1] != 2) {
            ERR("Wrong versions: %u, %u", versions[0], versions[1]);
        }
    }

    sqlite3_close(db);
    cleanupTestFile(filename);
}

/* ============================================================================
 * Schema Table Creation Tests
 * ============================================================================
 */

static void testSchemaCreateTables(uint32_t *err) {
    char filename[256];
    makeTestFilename(filename, sizeof(filename), "create");

    sqlite3 *db = NULL;
    int rc = sqlite3_open(filename, &db);
    if (rc != SQLITE_OK) {
        ERRR("Failed to open database");
        return;
    }

    kvidxColDef userCols[] = {
        COL("id", KVIDX_COL_PK_AUTO),
        COL("name", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
        COL("email", KVIDX_COL_TEXT | KVIDX_COL_UNIQUE),
        COL_DEFAULT_INT("status", KVIDX_COL_INTEGER, 0),
    };
    kvidxIndexDef userIdxs[] = {
        INDEX_UNIQUE("email"),
        INDEX("status"),
    };
    kvidxTableDef tables[] = {
        {
            .name = "users",
            .columns = userCols,
            .colCount = 4,
            .indexes = userIdxs,
            .indexCount = 2,
        },
    };

    TEST("Schema: Create tables from definitions") {
        kvidxError e = kvidxSchemaCreateTables(db, tables, 1);
        if (e != KVIDX_OK) {
            ERRR("Table creation failed");
        }
    }

    TEST("Schema: Insert into created table") {
        const char *sql = "INSERT INTO users (name, email) VALUES ('Alice', "
                          "'alice@test.com');";
        char *errMsg = NULL;
        rc = sqlite3_exec(db, sql, NULL, NULL, &errMsg);
        if (rc != SQLITE_OK) {
            ERR("Insert failed: %s", errMsg ? errMsg : "unknown");
            if (errMsg) {
                sqlite3_free(errMsg);
            }
        }
    }

    TEST("Schema: Default value applied") {
        const char *sql = "SELECT status FROM users WHERE name = 'Alice';";
        sqlite3_stmt *stmt = NULL;
        rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            ERRR("Prepare failed");
        } else {
            rc = sqlite3_step(stmt);
            if (rc != SQLITE_ROW) {
                ERRR("No row returned");
            } else {
                int status = sqlite3_column_int(stmt, 0);
                if (status != 0) {
                    ERR("Expected default 0, got %d", status);
                }
            }
            sqlite3_finalize(stmt);
        }
    }

    TEST("Schema: Unique constraint works") {
        const char *sql =
            "INSERT INTO users (name, email) VALUES ('Bob', 'alice@test.com');";
        char *errMsg = NULL;
        rc = sqlite3_exec(db, sql, NULL, NULL, &errMsg);
        if (rc == SQLITE_OK) {
            ERRR("Should have failed (duplicate email)");
        }
        if (errMsg) {
            sqlite3_free(errMsg);
        }
    }

    sqlite3_close(db);
    cleanupTestFile(filename);
}

/* ============================================================================
 * Edge Case Tests
 * ============================================================================
 */

static void testEdgeCases(uint32_t *err) {
    char buf[4096];

    TEST("Edge: NULL column definition") {
        if (kvidxGenColTypeSql(NULL, buf, sizeof(buf)) >= 0) {
            ERRR("Should fail with NULL col");
        }
    }

    TEST("Edge: NULL buffer") {
        kvidxColDef col = COL("x", KVIDX_COL_INTEGER);
        if (kvidxGenColTypeSql(&col, NULL, 0) >= 0) {
            ERRR("Should fail with NULL buffer");
        }
    }

    TEST("Edge: Zero-size buffer") {
        kvidxColDef col = COL("x", KVIDX_COL_INTEGER);
        if (kvidxGenColTypeSql(&col, buf, 0) >= 0) {
            ERRR("Should fail with zero-size buffer");
        }
    }

    TEST("Edge: Buffer too small") {
        kvidxColDef col = COL("x", KVIDX_COL_INTEGER);
        char smallBuf[3]; /* Too small for "INTEGER" */
        if (kvidxGenColTypeSql(&col, smallBuf, sizeof(smallBuf)) >= 0) {
            ERRR("Should fail with small buffer");
        }
    }

    TEST("Edge: Empty column name validation") {
        kvidxColDef col = {.name = "", .type = KVIDX_COL_INTEGER};
        if (kvidxColDefIsValid(&col)) {
            ERRR("Should be invalid (empty name)");
        }
    }

    TEST("Edge: Table with no columns") {
        kvidxTableDef table = {.name = "empty", .columns = NULL, .colCount = 0};
        if (kvidxTableDefIsValid(&table)) {
            ERRR("Should be invalid (no columns)");
        }
    }
}

/* ============================================================================
 * Main
 * ============================================================================
 */

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;
    uint32_t err = 0;

    testFlagDecoding(&err);
    testForeignKeys(&err);
    testDefaultValues(&err);
    testValidation(&err);
    testCreateTable(&err);
    testCreateIndex(&err);
    testStatementGeneration(&err);
    testSchemaVersioning(&err);
    testSchemaCreateTables(&err);
    testEdgeCases(&err);

    TEST_FINAL_RESULT;
}
