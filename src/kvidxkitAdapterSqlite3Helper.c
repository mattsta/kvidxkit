#include "kvidxkitAdapterSqlite3Helper.h"
#include "kvidxkit.h"

#include <stdio.h> /* snprintf */

static const char *types[] = {
    "INTEGER",
    "STRING",
    "BLOB",
    "PRIMARY KEY",
    "REFERENCES",
    "ON DELETE CASCADE",
    "DEFERRABLE INITIALLY DEFERRED",
    "AUTOINCREMENT",
    "UNIQUE",
};

#define genType(type)                                                          \
    do {                                                                       \
        typeBufOffset = 0;                                                     \
        switch (type) {                                                        \
        case HT_IPK:                                                           \
            typeBufOffset += snprintf(                                         \
                typeBuf + typeBufOffset, sizeof(typeBuf) - typeBufOffset,      \
                "%s %s", types[TR_INTEGER], types[TR_PRIMARY_KEY]);            \
            break;                                                             \
        case HT_IPKAUTO:                                                       \
            typeBufOffset += snprintf(                                         \
                typeBuf + typeBufOffset, sizeof(typeBuf) - typeBufOffset,      \
                "%s %s %s", types[TR_INTEGER], types[TR_PRIMARY_KEY],          \
                types[TR_AUTOINCREMENT]);                                      \
            break;                                                             \
        case HT_IREF:                                                          \
            typeBufOffset += snprintf(                                         \
                typeBuf + typeBufOffset, sizeof(typeBuf) - typeBufOffset,      \
                "%s %s %s", types[TR_INTEGER], types[TR_REFERENCES],           \
                desc->tableRef[referenceNext++]);                              \
            break;                                                             \
        case HT_IREF_DEL:                                                      \
            typeBufOffset += snprintf(                                         \
                typeBuf + typeBufOffset, sizeof(typeBuf) - typeBufOffset,      \
                "%s %s %s %s", types[TR_INTEGER], types[TR_REFERENCES],        \
                desc->tableRef[referenceNext++], types[TR_CASCADE_DELETE]);    \
            break;                                                             \
        case HT_IREF_DEL_DEF:                                                  \
            typeBufOffset += snprintf(                                         \
                typeBuf + typeBufOffset, sizeof(typeBuf) - typeBufOffset,      \
                "%s %s %s %s %s", types[TR_INTEGER], types[TR_REFERENCES],     \
                desc->tableRef[referenceNext++], types[TR_CASCADE_DELETE],     \
                types[TR_DEFERRED]);                                           \
            break;                                                             \
        case HT_INTEGER:                                                       \
            typeBufOffset += snprintf(typeBuf + typeBufOffset,                 \
                                      sizeof(typeBuf) - typeBufOffset, "%s",   \
                                      types[TR_INTEGER]);                      \
            break;                                                             \
        case HT_INT_UNIQUE:                                                    \
            typeBufOffset += snprintf(                                         \
                typeBuf + typeBufOffset, sizeof(typeBuf) - typeBufOffset,      \
                "%s %s", types[TR_INTEGER], types[TR_UNIQUE]);                 \
            break;                                                             \
        case HT_STRING:                                                        \
            typeBufOffset += snprintf(typeBuf + typeBufOffset,                 \
                                      sizeof(typeBuf) - typeBufOffset, "%s",   \
                                      types[TR_STRING]);                       \
            break;                                                             \
        case HT_BLOB:                                                          \
            typeBufOffset += snprintf(typeBuf + typeBufOffset,                 \
                                      sizeof(typeBuf) - typeBufOffset, "%s",   \
                                      types[TR_BLOB]);                         \
            break;                                                             \
        case HT_BLOB_UNIQUE:                                                   \
            typeBufOffset += snprintf(                                         \
                typeBuf + typeBufOffset, sizeof(typeBuf) - typeBufOffset,      \
                "%s %s", types[TR_BLOB], types[TR_UNIQUE]);                    \
            break;                                                             \
        default:                                                               \
            assert(NULL && "unexpected column desc!");                         \
            __builtin_unreachable();                                           \
        }                                                                      \
    } while (0)

bool kvidxkitAdapterSqlite3HelperTablesCreate(
    sqlite3 *db, const kvidxkitAdapterSqlite3HelperTableDesc *tables,
    size_t tableCount) {
    static const char *tableFmt = "CREATE TABLE IF NOT EXISTS %s(%s %s)%s;\n";
    static const char *idxFmt =
        "CREATE%sINDEX IF NOT EXISTS %s_%s on %s(%s);\n";

    for (size_t i = 0; i < tableCount; i++) {
        /* If these grow too big we'll die during testing since
         * table definitions don't change across builds. */
        char tableBuf[65536] = {0};
        char typeBuf[65536] = {0};
        char colsBuf[65536] = {0};
        size_t tableBufOffset = 0;
        size_t typeBufOffset = 0;
        size_t colsBufOffset = 0;
        size_t referenceNext = 0;
        const kvidxkitAdapterSqlite3HelperTableDesc *desc = &tables[i];

        /* Create column descriptions with name and type and optional comma */
        for (size_t j = 0; j < desc->colCount; j++) {
            genType(desc->type[j]);
            colsBufOffset += snprintf(
                colsBuf + colsBufOffset, sizeof(colsBuf) - colsBufOffset,
                "%s %s%s", desc->col[j], typeBuf,
                /* Only remove trailing comma if we don't have extra explicit
                 * table content provided by tableAddendum */
                (j == desc->colCount - 1) && !desc->tableAddendum ? "" : ",");
        }

        /* Add table definition with columns to buffer */
        tableBufOffset += snprintf(
            tableBuf, sizeof(tableBuf) - tableBufOffset, tableFmt, desc->name,
            colsBuf, desc->tableAddendum ? desc->tableAddendum : "",
            desc->withoutRowid ? "WITHOUT ROWID" : "");

        /* Add extra indexes to buffer */
        for (size_t j = 0; j < desc->indexCount; j++) {
            /* Take index name and remove any ungood characters so
             * we can use the index definition(ish) as the index name */
            char cleanIndexName[128] = {0};
            size_t cleanPos = 0;
            const size_t idxLen = strlen(desc->index[j]);
            for (size_t k = 0;
                 k < idxLen && cleanPos < (sizeof(cleanIndexName) - 1); k++) {
                const char current = desc->index[j][k];
                if (current == ' ' || current == '(' || current == ')' ||
                    current == ',') {
                    cleanIndexName[cleanPos++] = '_';
                } else {
                    cleanIndexName[cleanPos++] = current;
                }
            }

/* Hack alert:
 *  For UNIQUE index, prepend capital U to the index definition.
 *  We remove the 'U' for the index name and column definitions. */
#define Uor(what, yes, no) (what[0] == 'U' ? yes : no)

            tableBufOffset += snprintf(
                tableBuf + tableBufOffset, sizeof(tableBuf) - tableBufOffset,
                idxFmt, Uor(cleanIndexName, " UNIQUE ", " "), desc->name,
                Uor(cleanIndexName, cleanIndexName + 1, cleanIndexName),
                desc->name,
                Uor(desc->index[j], desc->index[j] + 1, desc->index[j]));
        }

#if 0
        printf("Buf is: %s\n", tableBuf);
#endif

        /* Run table creation and index creation */
        char *err = NULL;
        int result = sqlite3_exec(db, tableBuf, NULL, NULL, &err);
        if (result != SQLITE_OK) {
            fprintf(stderr, "Failed to create [%d (%s)]: %s\n", result, err,
                    tableBuf);
            return false;
        }
    }

    return true;
}

/* NOTE: 'stmt' parameter is a pointer to a SINGLE stmt, and NOT a pointer
 *       to the entire 'stmts' array. */
bool kvidxkitAdapterSqlite3HelperStmtsCreate(
    sqlite3 *restrict const db, const char *restrict const tableName,
    const size_t colCount,
    kvidxkitAdapterSqlite3HelperTableStmt *restrict const stmt) {
    char buf[4096] = {0};

    snprintf(buf, sizeof(buf), "SELECT * FROM %s WHERE id = ?", tableName);
    sprepare(db, buf, strlen(buf), &stmt->selectById, NULL);

    /* Generate correct number of '?, ?, ?' parameters */
    char qs[1024] = {0};
    size_t qsOffset = 0;
    for (size_t j = 0; j < colCount; j++) {
        qsOffset += snprintf(qs + qsOffset, sizeof(qs) - qsOffset, "?,");
    }

    /* remove trailing comma */
    qs[qsOffset - 1] = ' ';

    snprintf(buf, sizeof(buf), "INSERT INTO %s VALUES(%s)", tableName, qs);
    sprepare(db, buf, strlen(buf), &stmt->insert, NULL);

    snprintf(buf, sizeof(buf), "SELECT MAX(id) FROM %s", tableName);
    sprepare(db, buf, strlen(buf), &stmt->getHighestId, NULL);

    return true;
}

bool kvidxkitAdapterSqlite3HelperStmtsRelease(
    sqlite3 *db, const kvidxkitAdapterSqlite3HelperTableDesc *tables,
    size_t tableIdx, kvidxkitAdapterSqlite3HelperTableStmt *stmt) {
    (void)db;

    const kvidxkitAdapterSqlite3HelperTableDesc *desc = &tables[tableIdx];

    if (!desc->forType) {
        return false;
    }

    sqlite3_finalize(stmt->getHighestId);
    sqlite3_finalize(stmt->selectById);
    sqlite3_finalize(stmt->selectByKeyspaceId);
    sqlite3_finalize(stmt->insert);

    return true;
}
