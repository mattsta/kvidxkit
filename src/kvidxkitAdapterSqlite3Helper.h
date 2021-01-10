#pragma once

#include "../deps/sqlite3/src/sqlite3.h"

#include "kvidxkitAdapterSqlite3.h"
#include "sqliteHelper.h"

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/* These must be in-order with '*types[]' inside Sqlite3Helper.c */
typedef enum typeRef {
    TR_INTEGER = 0,
    TR_STRING,
    TR_BLOB,
    TR_PRIMARY_KEY,
    TR_REFERENCES,
    TR_CASCADE_DELETE,
    TR_DEFERRED,
    TR_AUTOINCREMENT,
    TR_UNIQUE,
} typeRef;

typedef enum kvidxkitAdapterSqlite3HelperTypes {
    HT_INTEGER = (1 << 1),
    HT_STRING = (1 << 2),
    HT_BLOB = (1 << 3),
    HT_PRIMARY_KEY = (1 << 4),
    HT_REFERENCES = (1 << 5),
    HT_CASCADE_DELETE = (1 << 6),
    HT_DEFERRED = (1 << 7),
    HT_AUTOINCREMENT = (1 << 8),
    HT_UNIQUE = (1 << 9),

    HT_IPK = HT_INTEGER | HT_PRIMARY_KEY,
    HT_IPKAUTO = HT_IPK | HT_AUTOINCREMENT,
    HT_IREF = HT_INTEGER | HT_REFERENCES,
    HT_IREF_DEL = HT_IREF | HT_CASCADE_DELETE,
    HT_IREF_DEL_DEF = HT_IREF_DEL | HT_DEFERRED,
    HT_BLOB_UNIQUE = HT_BLOB | HT_UNIQUE,
    HT_INT_UNIQUE = HT_INTEGER | HT_UNIQUE,
} kvidxkitAdapterSqlite3HelperTypes;

typedef struct kvidxkitAdapterSqlite3HelperTableDesc {
    int forType; /* a kvidxType defined elsewhere */
    char *name;
    size_t colCount;
    char **col;
    kvidxkitAdapterSqlite3HelperTypes *type;
    char **tableRef;
    size_t indexCount;
    char **index;
    char *tableAddendum;
    bool withoutRowid;
} kvidxkitAdapterSqlite3HelperTableDesc;

typedef struct kvidxkitAdapterSqlite3HelperTableStmt {
    sqlite3_stmt *getHighestId;
    sqlite3_stmt *selectById;
    sqlite3_stmt *selectByKeyspaceId;
    sqlite3_stmt *insert;
} kvidxkitAdapterSqlite3HelperTableStmt;

#define COLS (char *[])
#define TYPES (kvidxkitAdapterSqlite3HelperTypes[])
#define REFS (char *[])

bool kvidxkitAdapterSqlite3HelperTablesCreate(
    sqlite3 *db, const kvidxkitAdapterSqlite3HelperTableDesc *tables,
    size_t tableCount);
bool kvidxkitAdapterSqlite3HelperStmtsCreate(
    sqlite3 *restrict const db, const char *restrict const tableName,
    const size_t colCount,
    kvidxkitAdapterSqlite3HelperTableStmt *restrict const stmt);

bool kvidxkitAdapterSqlite3HelperStmtsRelease(
    sqlite3 *db, const kvidxkitAdapterSqlite3HelperTableDesc *tables,
    size_t tableIdx, kvidxkitAdapterSqlite3HelperTableStmt *stmtPerType);
