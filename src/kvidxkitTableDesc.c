#include "kvidxkitTableDesc.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

/* ============================================================================
 * Flag-to-SQL Mapping
 * ============================================================================
 * Maps type flags to SQL fragments. Order field determines output sequence.
 * Base types have order 0, constraints follow in logical SQL order.
 */

typedef struct {
    kvidxColType flag;
    const char *sql;
    int order;
    bool isBaseType;
} FlagMapping;

static const FlagMapping flagMappings[] = {
    /* Base types (order 0) - exactly one required */
    {KVIDX_COL_INTEGER, "INTEGER", 0, true},
    {KVIDX_COL_TEXT, "TEXT", 0, true},
    {KVIDX_COL_BLOB, "BLOB", 0, true},
    {KVIDX_COL_REAL, "REAL", 0, true},

    /* Constraints in SQL-standard order */
    {KVIDX_COL_PRIMARY_KEY, "PRIMARY KEY", 1, false},
    {KVIDX_COL_AUTOINCREMENT, "AUTOINCREMENT", 2, false},
    {KVIDX_COL_NOT_NULL, "NOT NULL", 3, false},
    {KVIDX_COL_UNIQUE, "UNIQUE", 4, false},
    /* REFERENCES handled specially - needs table name */
    {KVIDX_COL_CASCADE_DELETE, "ON DELETE CASCADE", 6, false},
    {KVIDX_COL_DEFERRED, "DEFERRABLE INITIALLY DEFERRED", 7, false},
    /* HAS_DEFAULT handled specially - needs value */
};

static const size_t flagMappingCount =
    sizeof(flagMappings) / sizeof(flagMappings[0]);

/* ============================================================================
 * Helper Functions
 * ============================================================================
 */

/**
 * Count number of set bits (popcount) in the base type mask
 */
static int countBaseTypes(kvidxColType type) {
    int count = 0;
    kvidxColType baseType = type & KVIDX_COL_BASE_TYPE_MASK;
    while (baseType) {
        count += baseType & 1;
        baseType >>= 1;
    }
    return count;
}

/**
 * Append string to buffer with bounds checking
 * Returns new offset, or -1 if buffer would overflow
 */
static int appendStr(char *buf, size_t size, int offset, const char *str) {
    if (offset < 0) {
        return -1;
    }
    size_t len = strlen(str);
    if ((size_t)offset + len >= size) {
        return -1;
    }
    memcpy(buf + offset, str, len);
    buf[offset + len] = '\0';
    return offset + (int)len;
}

/**
 * Append formatted string to buffer with bounds checking
 */
static int appendFmt(char *buf, size_t size, int offset, const char *fmt, ...) {
    if (offset < 0) {
        return -1;
    }
    va_list args;
    va_start(args, fmt);
    int written = vsnprintf(buf + offset, size - offset, fmt, args);
    va_end(args);
    if (written < 0 || (size_t)(offset + written) >= size) {
        return -1;
    }
    return offset + written;
}

/* ============================================================================
 * Validation
 * ============================================================================
 */

bool kvidxColDefIsValid(const kvidxColDef *col) {
    if (!col || !col->name || col->name[0] == '\0') {
        return false;
    }

    /* Must have exactly one base type */
    int baseTypeCount = countBaseTypes(col->type);
    if (baseTypeCount != 1) {
        return false;
    }

    /* AUTOINCREMENT requires INTEGER PRIMARY KEY */
    if (col->type & KVIDX_COL_AUTOINCREMENT) {
        if (!(col->type & KVIDX_COL_INTEGER) ||
            !(col->type & KVIDX_COL_PRIMARY_KEY)) {
            return false;
        }
    }

    /* REFERENCES requires refTable */
    if (col->type & KVIDX_COL_REFERENCES) {
        if (!col->refTable || col->refTable[0] == '\0') {
            return false;
        }
    }

    /* CASCADE_DELETE and DEFERRED require REFERENCES */
    if ((col->type & KVIDX_COL_CASCADE_DELETE) ||
        (col->type & KVIDX_COL_DEFERRED)) {
        if (!(col->type & KVIDX_COL_REFERENCES)) {
            return false;
        }
    }

    /* Validate default value type matches column type */
    if (col->type & KVIDX_COL_HAS_DEFAULT) {
        switch (col->defaultKind) {
        case KVIDX_DEFAULT_NONE:
            return false; /* HAS_DEFAULT set but no default kind */
        case KVIDX_DEFAULT_NULL:
            /* NULL default not allowed with NOT NULL */
            if (col->type & KVIDX_COL_NOT_NULL) {
                return false;
            }
            break;
        case KVIDX_DEFAULT_INT:
            if (!(col->type & KVIDX_COL_INTEGER)) {
                return false;
            }
            break;
        case KVIDX_DEFAULT_REAL:
            if (!(col->type & KVIDX_COL_REAL)) {
                return false;
            }
            break;
        case KVIDX_DEFAULT_TEXT:
            if (!(col->type & KVIDX_COL_TEXT)) {
                return false;
            }
            break;
        case KVIDX_DEFAULT_EXPR:
            /* Expression defaults allowed for any type */
            break;
        }
    }

    return true;
}

bool kvidxTableDefIsValid(const kvidxTableDef *table) {
    if (!table || !table->name || table->name[0] == '\0') {
        return false;
    }

    if (table->colCount == 0 || !table->columns) {
        return false;
    }

    /* Validate all columns */
    for (size_t i = 0; i < table->colCount; i++) {
        if (!kvidxColDefIsValid(&table->columns[i])) {
            return false;
        }
    }

    /* Validate indexes if present */
    if (table->indexCount > 0 && !table->indexes) {
        return false;
    }

    for (size_t i = 0; i < table->indexCount; i++) {
        const kvidxIndexDef *idx = &table->indexes[i];
        if (idx->colCount == 0 || !idx->columns) {
            return false;
        }
        /* Verify each indexed column exists in table */
        for (size_t j = 0; j < idx->colCount; j++) {
            if (!idx->columns[j]) {
                return false;
            }
            bool found = false;
            for (size_t k = 0; k < table->colCount; k++) {
                if (strcmp(idx->columns[j], table->columns[k].name) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
    }

    return true;
}

/* ============================================================================
 * SQL Generation - Column Type
 * ============================================================================
 */

int kvidxGenColTypeSql(const kvidxColDef *col, char *buf, size_t size) {
    if (!col || !buf || size == 0) {
        return -1;
    }

    buf[0] = '\0';
    int offset = 0;

    /* Generate SQL fragments in order */
    for (int order = 0; order <= 7; order++) {
        for (size_t i = 0; i < flagMappingCount; i++) {
            const FlagMapping *m = &flagMappings[i];
            if (m->order == order && (col->type & m->flag)) {
                if (offset > 0) {
                    offset = appendStr(buf, size, offset, " ");
                }
                offset = appendStr(buf, size, offset, m->sql);
                if (offset < 0) {
                    return -1;
                }
            }
        }

        /* Handle REFERENCES specially (order 5) */
        if (order == 5 && (col->type & KVIDX_COL_REFERENCES) && col->refTable) {
            if (offset > 0) {
                offset = appendStr(buf, size, offset, " ");
            }
            offset = appendStr(buf, size, offset, "REFERENCES ");
            offset = appendStr(buf, size, offset, col->refTable);
            if (offset < 0) {
                return -1;
            }
        }
    }

    /* Handle DEFAULT value */
    if (col->type & KVIDX_COL_HAS_DEFAULT) {
        offset = appendStr(buf, size, offset, " DEFAULT ");
        if (offset < 0) {
            return -1;
        }

        switch (col->defaultKind) {
        case KVIDX_DEFAULT_NONE:
            return -1; /* Invalid state */
        case KVIDX_DEFAULT_NULL:
            offset = appendStr(buf, size, offset, "NULL");
            break;
        case KVIDX_DEFAULT_INT:
            offset = appendFmt(buf, size, offset, "%lld",
                               (long long)col->defaultVal.intVal);
            break;
        case KVIDX_DEFAULT_REAL:
            offset =
                appendFmt(buf, size, offset, "%g", col->defaultVal.realVal);
            break;
        case KVIDX_DEFAULT_TEXT:
            offset = appendStr(buf, size, offset, "'");
            /* TODO: Escape single quotes in text */
            offset = appendStr(buf, size, offset, col->defaultVal.textVal);
            offset = appendStr(buf, size, offset, "'");
            break;
        case KVIDX_DEFAULT_EXPR:
            offset = appendStr(buf, size, offset, col->defaultVal.exprVal);
            break;
        }
    }

    return offset;
}

/* ============================================================================
 * SQL Generation - CREATE TABLE
 * ============================================================================
 */

bool kvidxGenCreateTable(const kvidxTableDef *table, char *buf, size_t size) {
    if (!table || !buf || size == 0) {
        return false;
    }

    buf[0] = '\0';
    int offset = 0;

    offset = appendStr(buf, size, offset, "CREATE TABLE IF NOT EXISTS ");
    offset = appendStr(buf, size, offset, table->name);
    offset = appendStr(buf, size, offset, " (");

    /* Generate column definitions */
    char colTypeBuf[1024];
    for (size_t i = 0; i < table->colCount; i++) {
        if (i > 0) {
            offset = appendStr(buf, size, offset, ", ");
        }
        offset = appendStr(buf, size, offset, table->columns[i].name);
        offset = appendStr(buf, size, offset, " ");

        int colLen = kvidxGenColTypeSql(&table->columns[i], colTypeBuf,
                                        sizeof(colTypeBuf));
        if (colLen < 0) {
            return false;
        }
        offset = appendStr(buf, size, offset, colTypeBuf);
    }

    offset = appendStr(buf, size, offset, ")");

    /* WITHOUT ROWID */
    if (table->withoutRowid) {
        offset = appendStr(buf, size, offset, " WITHOUT ROWID");
    }

    offset = appendStr(buf, size, offset, ";");

    return offset >= 0;
}

/* ============================================================================
 * SQL Generation - CREATE INDEX
 * ============================================================================
 */

bool kvidxGenCreateIndexes(const kvidxTableDef *table, char *buf, size_t size) {
    if (!table || !buf || size == 0) {
        return false;
    }

    buf[0] = '\0';
    int offset = 0;

    for (size_t i = 0; i < table->indexCount; i++) {
        const kvidxIndexDef *idx = &table->indexes[i];

        if (i > 0) {
            offset = appendStr(buf, size, offset, "\n");
        }

        offset = appendStr(buf, size, offset, "CREATE ");
        if (idx->unique) {
            offset = appendStr(buf, size, offset, "UNIQUE ");
        }
        offset = appendStr(buf, size, offset, "INDEX IF NOT EXISTS ");

        /* Generate index name */
        if (idx->name) {
            offset = appendStr(buf, size, offset, idx->name);
        } else {
            /* Auto-generate: tablename_col1_col2_idx */
            offset = appendStr(buf, size, offset, table->name);
            offset = appendStr(buf, size, offset, "_");
            for (size_t j = 0; j < idx->colCount; j++) {
                if (j > 0) {
                    offset = appendStr(buf, size, offset, "_");
                }
                offset = appendStr(buf, size, offset, idx->columns[j]);
            }
            offset = appendStr(buf, size, offset, "_idx");
        }

        offset = appendStr(buf, size, offset, " ON ");
        offset = appendStr(buf, size, offset, table->name);
        offset = appendStr(buf, size, offset, " (");

        for (size_t j = 0; j < idx->colCount; j++) {
            if (j > 0) {
                offset = appendStr(buf, size, offset, ", ");
            }
            offset = appendStr(buf, size, offset, idx->columns[j]);
        }

        offset = appendStr(buf, size, offset, ");");
    }

    return offset >= 0;
}

/* ============================================================================
 * SQL Generation - Statements
 * ============================================================================
 */

bool kvidxGenInsert(const kvidxTableDef *table, char *buf, size_t size) {
    if (!table || !buf || size == 0) {
        return false;
    }

    int offset = 0;
    offset = appendStr(buf, size, offset, "INSERT INTO ");
    offset = appendStr(buf, size, offset, table->name);
    offset = appendStr(buf, size, offset, " (");

    /* Column names */
    for (size_t i = 0; i < table->colCount; i++) {
        if (i > 0) {
            offset = appendStr(buf, size, offset, ", ");
        }
        offset = appendStr(buf, size, offset, table->columns[i].name);
    }

    offset = appendStr(buf, size, offset, ") VALUES (");

    /* Placeholders */
    for (size_t i = 0; i < table->colCount; i++) {
        if (i > 0) {
            offset = appendStr(buf, size, offset, ", ");
        }
        offset = appendStr(buf, size, offset, "?");
    }

    offset = appendStr(buf, size, offset, ");");

    return offset >= 0;
}

bool kvidxGenSelectById(const kvidxTableDef *table, char *buf, size_t size) {
    if (!table || !buf || size == 0 || table->colCount == 0) {
        return false;
    }

    int offset = 0;
    offset = appendStr(buf, size, offset, "SELECT * FROM ");
    offset = appendStr(buf, size, offset, table->name);
    offset = appendStr(buf, size, offset, " WHERE ");
    offset = appendStr(buf, size, offset, table->columns[0].name);
    offset = appendStr(buf, size, offset, " = ?;");

    return offset >= 0;
}

bool kvidxGenSelectAll(const kvidxTableDef *table, char *buf, size_t size) {
    if (!table || !buf || size == 0) {
        return false;
    }

    int offset = 0;
    offset = appendStr(buf, size, offset, "SELECT * FROM ");
    offset = appendStr(buf, size, offset, table->name);
    offset = appendStr(buf, size, offset, ";");

    return offset >= 0;
}

bool kvidxGenUpdateById(const kvidxTableDef *table, char *buf, size_t size) {
    if (!table || !buf || size == 0 || table->colCount < 2) {
        return false;
    }

    int offset = 0;
    offset = appendStr(buf, size, offset, "UPDATE ");
    offset = appendStr(buf, size, offset, table->name);
    offset = appendStr(buf, size, offset, " SET ");

    /* All columns except the first (assumed to be id) */
    bool first = true;
    for (size_t i = 1; i < table->colCount; i++) {
        if (!first) {
            offset = appendStr(buf, size, offset, ", ");
        }
        first = false;
        offset = appendStr(buf, size, offset, table->columns[i].name);
        offset = appendStr(buf, size, offset, " = ?");
    }

    offset = appendStr(buf, size, offset, " WHERE ");
    offset = appendStr(buf, size, offset, table->columns[0].name);
    offset = appendStr(buf, size, offset, " = ?;");

    return offset >= 0;
}

bool kvidxGenDeleteById(const kvidxTableDef *table, char *buf, size_t size) {
    if (!table || !buf || size == 0 || table->colCount == 0) {
        return false;
    }

    int offset = 0;
    offset = appendStr(buf, size, offset, "DELETE FROM ");
    offset = appendStr(buf, size, offset, table->name);
    offset = appendStr(buf, size, offset, " WHERE ");
    offset = appendStr(buf, size, offset, table->columns[0].name);
    offset = appendStr(buf, size, offset, " = ?;");

    return offset >= 0;
}

bool kvidxGenCount(const kvidxTableDef *table, char *buf, size_t size) {
    if (!table || !buf || size == 0) {
        return false;
    }

    int offset = 0;
    offset = appendStr(buf, size, offset, "SELECT COUNT(*) FROM ");
    offset = appendStr(buf, size, offset, table->name);
    offset = appendStr(buf, size, offset, ";");

    return offset >= 0;
}

bool kvidxGenMaxId(const kvidxTableDef *table, char *buf, size_t size) {
    if (!table || !buf || size == 0 || table->colCount == 0) {
        return false;
    }

    int offset = 0;
    offset = appendStr(buf, size, offset, "SELECT MAX(");
    offset = appendStr(buf, size, offset, table->columns[0].name);
    offset = appendStr(buf, size, offset, ") FROM ");
    offset = appendStr(buf, size, offset, table->name);
    offset = appendStr(buf, size, offset, ";");

    return offset >= 0;
}

bool kvidxGenMinId(const kvidxTableDef *table, char *buf, size_t size) {
    if (!table || !buf || size == 0 || table->colCount == 0) {
        return false;
    }

    int offset = 0;
    offset = appendStr(buf, size, offset, "SELECT MIN(");
    offset = appendStr(buf, size, offset, table->columns[0].name);
    offset = appendStr(buf, size, offset, ") FROM ");
    offset = appendStr(buf, size, offset, table->name);
    offset = appendStr(buf, size, offset, ";");

    return offset >= 0;
}
