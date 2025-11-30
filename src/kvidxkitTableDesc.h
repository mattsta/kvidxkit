#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

__BEGIN_DECLS

/* ============================================================================
 * Column Type Flags
 * ============================================================================
 * Column types are composed using bitflags. Each column must have exactly one
 * base type (INTEGER, TEXT, BLOB, or REAL) and zero or more constraints.
 */

typedef enum kvidxColType {
    /* Base Types (mutually exclusive - bits 0-3) */
    KVIDX_COL_INTEGER = (1 << 0),
    KVIDX_COL_TEXT = (1 << 1),
    KVIDX_COL_BLOB = (1 << 2),
    KVIDX_COL_REAL = (1 << 3),

    /* Constraints (bits 4-11) */
    KVIDX_COL_PRIMARY_KEY = (1 << 4),
    KVIDX_COL_NOT_NULL = (1 << 5),
    KVIDX_COL_UNIQUE = (1 << 6),
    KVIDX_COL_AUTOINCREMENT = (1 << 7),
    KVIDX_COL_REFERENCES = (1 << 8),
    KVIDX_COL_CASCADE_DELETE = (1 << 9),
    KVIDX_COL_DEFERRED = (1 << 10),
    KVIDX_COL_HAS_DEFAULT = (1 << 11),
} kvidxColType;

/* Mask for extracting base type */
#define KVIDX_COL_BASE_TYPE_MASK                                               \
    (KVIDX_COL_INTEGER | KVIDX_COL_TEXT | KVIDX_COL_BLOB | KVIDX_COL_REAL)

/* Convenience combinations for common patterns */
#define KVIDX_COL_PK (KVIDX_COL_INTEGER | KVIDX_COL_PRIMARY_KEY)
#define KVIDX_COL_PK_AUTO (KVIDX_COL_PK | KVIDX_COL_AUTOINCREMENT)
#define KVIDX_COL_FK (KVIDX_COL_INTEGER | KVIDX_COL_REFERENCES)
#define KVIDX_COL_FK_CASCADE (KVIDX_COL_FK | KVIDX_COL_CASCADE_DELETE)
#define KVIDX_COL_FK_CASCADE_DEFERRED                                          \
    (KVIDX_COL_FK_CASCADE | KVIDX_COL_DEFERRED)

/* ============================================================================
 * Default Value Types
 * ============================================================================
 */

typedef enum kvidxDefaultKind {
    KVIDX_DEFAULT_NONE = 0,
    KVIDX_DEFAULT_NULL,
    KVIDX_DEFAULT_INT,
    KVIDX_DEFAULT_REAL,
    KVIDX_DEFAULT_TEXT,
    KVIDX_DEFAULT_EXPR, /* Raw SQL expression like CURRENT_TIMESTAMP */
} kvidxDefaultKind;

/* ============================================================================
 * Column Definition
 * ============================================================================
 */

typedef struct kvidxColDef {
    const char *name;     /* Column name */
    kvidxColType type;    /* Type flags */
    const char *refTable; /* Foreign key target table (NULL if none) */
    kvidxDefaultKind defaultKind;
    union {
        int64_t intVal;
        double realVal;
        const char *textVal;
        const char *exprVal;
    } defaultVal;
} kvidxColDef;

/* Column definition macros for clean declarative syntax
 * These use compound literals so they work both in array initializers
 * and as standalone assignments */
#define COL(colname, coltype)                                                  \
    (kvidxColDef) {                                                            \
        .name = (colname), .type = (coltype), .refTable = NULL,                \
        .defaultKind = KVIDX_DEFAULT_NONE                                      \
    }

#define COL_FK(colname, coltype, reftable)                                     \
    (kvidxColDef) {                                                            \
        .name = (colname), .type = (coltype) | KVIDX_COL_REFERENCES,           \
        .refTable = (reftable), .defaultKind = KVIDX_DEFAULT_NONE              \
    }

#define COL_DEFAULT_NULL(colname, coltype)                                     \
    (kvidxColDef) {                                                            \
        .name = (colname), .type = (coltype) | KVIDX_COL_HAS_DEFAULT,          \
        .defaultKind = KVIDX_DEFAULT_NULL                                      \
    }

#define COL_DEFAULT_INT(colname, coltype, val)                                 \
    (kvidxColDef) {                                                            \
        .name = (colname), .type = (coltype) | KVIDX_COL_HAS_DEFAULT,          \
        .defaultKind = KVIDX_DEFAULT_INT, .defaultVal.intVal = (val)           \
    }

#define COL_DEFAULT_REAL(colname, coltype, val)                                \
    (kvidxColDef) {                                                            \
        .name = (colname), .type = (coltype) | KVIDX_COL_HAS_DEFAULT,          \
        .defaultKind = KVIDX_DEFAULT_REAL, .defaultVal.realVal = (val)         \
    }

#define COL_DEFAULT_TEXT(colname, coltype, val)                                \
    (kvidxColDef) {                                                            \
        .name = (colname), .type = (coltype) | KVIDX_COL_HAS_DEFAULT,          \
        .defaultKind = KVIDX_DEFAULT_TEXT, .defaultVal.textVal = (val)         \
    }

#define COL_DEFAULT_EXPR(colname, coltype, expr)                               \
    (kvidxColDef) {                                                            \
        .name = (colname), .type = (coltype) | KVIDX_COL_HAS_DEFAULT,          \
        .defaultKind = KVIDX_DEFAULT_EXPR, .defaultVal.exprVal = (expr)        \
    }

/* ============================================================================
 * Index Definition
 * ============================================================================
 */

typedef struct kvidxIndexDef {
    const char *name;     /* Index name (NULL = auto-generate) */
    bool unique;          /* UNIQUE index */
    size_t colCount;      /* Number of columns */
    const char **columns; /* Array of column names */
} kvidxIndexDef;

/* Index definition macros */
#define INDEX(...)                                                             \
    (kvidxIndexDef) {                                                          \
        .name = NULL, .unique = false,                                         \
        .columns = (const char *[]){__VA_ARGS__},                              \
        .colCount =                                                            \
            sizeof((const char *[]){__VA_ARGS__}) / sizeof(const char *)       \
    }

#define INDEX_UNIQUE(...)                                                      \
    (kvidxIndexDef) {                                                          \
        .name = NULL, .unique = true,                                          \
        .columns = (const char *[]){__VA_ARGS__},                              \
        .colCount =                                                            \
            sizeof((const char *[]){__VA_ARGS__}) / sizeof(const char *)       \
    }

#define INDEX_NAMED(indexName, ...)                                            \
    (kvidxIndexDef) {                                                          \
        .name = (indexName), .unique = false,                                  \
        .columns = (const char *[]){__VA_ARGS__},                              \
        .colCount =                                                            \
            sizeof((const char *[]){__VA_ARGS__}) / sizeof(const char *)       \
    }

#define INDEX_UNIQUE_NAMED(indexName, ...)                                     \
    (kvidxIndexDef) {                                                          \
        .name = (indexName), .unique = true,                                   \
        .columns = (const char *[]){__VA_ARGS__},                              \
        .colCount =                                                            \
            sizeof((const char *[]){__VA_ARGS__}) / sizeof(const char *)       \
    }

/* ============================================================================
 * Table Definition
 * ============================================================================
 */

typedef struct kvidxTableDef {
    const char *name;           /* Table name */
    const kvidxColDef *columns; /* Array of column definitions */
    size_t colCount;            /* Number of columns */
    const kvidxIndexDef
        *indexes;      /* Array of index definitions (NULL if none) */
    size_t indexCount; /* Number of indexes */
    bool withoutRowid; /* WITHOUT ROWID table */
} kvidxTableDef;

/* ============================================================================
 * SQL Generation API
 * ============================================================================
 */

/**
 * Generate column type SQL fragment (e.g., "INTEGER PRIMARY KEY NOT NULL")
 *
 * @param col Column definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return Number of characters written (excluding null terminator), or -1 on
 * error
 */
int kvidxGenColTypeSql(const kvidxColDef *col, char *buf, size_t size);

/**
 * Generate CREATE TABLE statement
 *
 * @param table Table definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return true on success, false if buffer too small or invalid definition
 */
bool kvidxGenCreateTable(const kvidxTableDef *table, char *buf, size_t size);

/**
 * Generate CREATE INDEX statements for a table
 *
 * @param table Table definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return true on success, false if buffer too small
 */
bool kvidxGenCreateIndexes(const kvidxTableDef *table, char *buf, size_t size);

/**
 * Generate INSERT statement with placeholders
 *
 * @param table Table definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return true on success
 */
bool kvidxGenInsert(const kvidxTableDef *table, char *buf, size_t size);

/**
 * Generate SELECT * WHERE id = ? statement
 *
 * @param table Table definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return true on success
 */
bool kvidxGenSelectById(const kvidxTableDef *table, char *buf, size_t size);

/**
 * Generate SELECT * statement (all rows)
 *
 * @param table Table definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return true on success
 */
bool kvidxGenSelectAll(const kvidxTableDef *table, char *buf, size_t size);

/**
 * Generate UPDATE ... WHERE id = ? statement
 *
 * @param table Table definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return true on success
 */
bool kvidxGenUpdateById(const kvidxTableDef *table, char *buf, size_t size);

/**
 * Generate DELETE WHERE id = ? statement
 *
 * @param table Table definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return true on success
 */
bool kvidxGenDeleteById(const kvidxTableDef *table, char *buf, size_t size);

/**
 * Generate SELECT COUNT(*) statement
 *
 * @param table Table definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return true on success
 */
bool kvidxGenCount(const kvidxTableDef *table, char *buf, size_t size);

/**
 * Generate SELECT MAX(id) statement
 *
 * @param table Table definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return true on success
 */
bool kvidxGenMaxId(const kvidxTableDef *table, char *buf, size_t size);

/**
 * Generate SELECT MIN(id) statement
 *
 * @param table Table definition
 * @param buf Output buffer
 * @param size Buffer size
 * @return true on success
 */
bool kvidxGenMinId(const kvidxTableDef *table, char *buf, size_t size);

/* ============================================================================
 * Validation
 * ============================================================================
 */

/**
 * Validate column definition
 *
 * Checks:
 * - Exactly one base type
 * - AUTOINCREMENT only with INTEGER PRIMARY KEY
 * - REFERENCES has refTable set
 * - Default value type matches column type
 *
 * @param col Column definition to validate
 * @return true if valid, false otherwise
 */
bool kvidxColDefIsValid(const kvidxColDef *col);

/**
 * Validate table definition
 *
 * @param table Table definition to validate
 * @return true if valid, false otherwise
 */
bool kvidxTableDefIsValid(const kvidxTableDef *table);

__END_DECLS
