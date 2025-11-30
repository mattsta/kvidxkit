# kvidxkit: Key-Value Index Kit

kvidxkit is a C interface for ordered key-value storage manipulation with pluggable backends.

kvidxkit ships with **SQLite3**, **LMDB**, and **RocksDB** adapters, but you can add any other
in-memory or persistent datastore as long as it conforms to the `kvidxInterface` struct of pointers.

## Facy Features

kvidx also includes a relatively fancy data language for creating SQL tables from C struct
descriptions. For example, here's creating two tables with names, types, and indexes and it's
all fairly easy to read:

```c
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
```

The table descriptions are fed into `kvidxkitAdapterSqlite3Helper.c:kvidxkitAdapterSqlite3HelperTablesCreate()` where
all the actual conversion from the table description into SQL create statements happens.

## History

kvidxkit was made between 2016 and 2019 to store the internal log state of a
raft implementation, which is why it supports range deletes
(for destroying logs from a non-quorum peer) as well as other easy to use
persist-resume and logical data manipulation hooks.

I added more features, capabilities, backends, documentation, and examples again in 2025.


## Building

```bash
mkdir build
cd build
cmake ..
make -j4
```

## Testing
```bash
./build/src/kvidxkit-test
```

### Core Operations

- **Key-Value Storage** - Ordered uint64_t keys with arbitrary blob values
- **Navigation** - Get, GetPrev, GetNext for sequential access
- **Range Deletes** - Remove keys before/after a boundary
- **Transactions** - BEGIN/COMMIT for batched writes with single fsync

### Error Handling (v0.4.0)

- Detailed error codes (`kvidxError` enum)
- Human-readable error messages per instance
- Error state tracking

### Batch Operations (v0.4.0)

- `kvidxInsertBatch()` for efficient bulk inserts
- `kvidxInsertBatchEx()` with callback for filtering/validation
- 100x+ performance improvement over individual inserts

### Iterator API

- Forward and backward iteration
- Range-bounded iteration
- Seek to specific keys
- Zero-copy data access

### Statistics API (v0.5.0)

- Key count, min/max keys, data size
- Database file size, WAL size
- Page count and fragmentation info

### Configuration API (v0.5.0)

- Journal mode (WAL, DELETE, TRUNCATE, etc.)
- Sync mode (OFF, NORMAL, FULL, EXTRA)
- Cache size, page size, VFS selection
- Read-only mode, busy timeout

### Range Operations (v0.5.0)

- `kvidxRemoveRange()` with inclusive/exclusive boundaries
- `kvidxCountRange()` for counting keys in range
- `kvidxExistsInRange()` for existence checks

### Export/Import (v0.6.0)

- Binary, JSON, and CSV export formats
- Range-bounded exports
- Progress callbacks
- Import with duplicate handling options

### Storage Primitives (v0.8.0)

- **Conditional Writes** - `InsertEx` with ALWAYS, IF_NOT_EXISTS, IF_EXISTS modes
- **Atomic Operations** - `GetAndSet`, `GetAndRemove` for atomic read-modify-write
- **Compare-And-Swap** - `CompareAndSwap` for optimistic concurrency control
- **Append/Prepend** - Atomic data concatenation operations
- **Partial Value Access** - `GetValueRange`, `SetValueRange` for substring operations
- **TTL/Expiration** - `SetExpire`, `SetExpireAt`, `GetTTL`, `Persist`, `ExpireScan`
- **Transaction Abort** - `Abort` to rollback uncommitted changes

### Table Description System (v0.7.0)

- Declarative table definitions using C structs
- Dynamic flag-based column type composition
- All SQLite column types: INTEGER, TEXT, BLOB, REAL
- Constraints: PRIMARY KEY, NOT NULL, UNIQUE, AUTOINCREMENT
- Foreign keys with CASCADE DELETE and DEFERRED options
- DEFAULT values: integers, text, expressions (CURRENT_TIMESTAMP)
- Structured index definitions (single and multi-column)
- Automatic SQL generation for CREATE TABLE, INSERT, UPDATE, DELETE, SELECT

### Schema Versioning (v0.7.0)

- Migration tracking with version history
- Automatic schema table management
- Idempotent migration application
- Progress callbacks for migration monitoring

---

## Table Description System

The Table Description System provides a declarative C-based DSL for defining SQL tables. Instead of writing SQL strings, you define tables using C structs and macros, and the system generates properly-formatted SQL statements.

### Quick Start

```c
#include "kvidxkitTableDesc.h"
#include "kvidxkitSchema.h"

/* 1. Define columns */
static const kvidxColDef userCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL("name", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL("email", KVIDX_COL_TEXT | KVIDX_COL_UNIQUE),
};

/* 2. Define table */
static const kvidxTableDef userTable = {
    .name = "users",
    .columns = userCols,
    .colCount = sizeof(userCols) / sizeof(*userCols),
};

/* 3. Create the table */
kvidxSchemaCreateTables(db, &userTable, 1);
```

**Generated SQL:**

```sql
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);
```

---

## Column Type System

Columns are defined using bitflags that compose together. Each column requires exactly one **base type** and zero or more **constraints**.

### Base Types (mutually exclusive)

| Flag                | SQL Type  | Description                |
| ------------------- | --------- | -------------------------- |
| `KVIDX_COL_INTEGER` | `INTEGER` | 64-bit signed integer      |
| `KVIDX_COL_TEXT`    | `TEXT`    | UTF-8 string               |
| `KVIDX_COL_BLOB`    | `BLOB`    | Binary data                |
| `KVIDX_COL_REAL`    | `REAL`    | 64-bit IEEE floating point |

### Constraint Flags

| Flag                       | SQL Output                      | Description                                  |
| -------------------------- | ------------------------------- | -------------------------------------------- |
| `KVIDX_COL_PRIMARY_KEY`    | `PRIMARY KEY`                   | Primary key column                           |
| `KVIDX_COL_NOT_NULL`       | `NOT NULL`                      | Disallow NULL values                         |
| `KVIDX_COL_UNIQUE`         | `UNIQUE`                        | Enforce uniqueness                           |
| `KVIDX_COL_AUTOINCREMENT`  | `AUTOINCREMENT`                 | Auto-generate IDs (INTEGER PRIMARY KEY only) |
| `KVIDX_COL_REFERENCES`     | `REFERENCES table`              | Foreign key reference                        |
| `KVIDX_COL_CASCADE_DELETE` | `ON DELETE CASCADE`             | Cascade deletes to referencing rows          |
| `KVIDX_COL_DEFERRED`       | `DEFERRABLE INITIALLY DEFERRED` | Defer FK constraint checks                   |
| `KVIDX_COL_HAS_DEFAULT`    | `DEFAULT value`                 | Column has a default value                   |

### Convenience Combinations

Pre-defined combinations for common patterns:

| Macro                           | Expands To                                            | Use Case                      |
| ------------------------------- | ----------------------------------------------------- | ----------------------------- |
| `KVIDX_COL_PK`                  | `INTEGER PRIMARY KEY`                                 | Simple primary key            |
| `KVIDX_COL_PK_AUTO`             | `INTEGER PRIMARY KEY AUTOINCREMENT`                   | Auto-incrementing primary key |
| `KVIDX_COL_FK`                  | `INTEGER REFERENCES`                                  | Basic foreign key             |
| `KVIDX_COL_FK_CASCADE`          | `INTEGER REFERENCES ... ON DELETE CASCADE`            | FK with cascade delete        |
| `KVIDX_COL_FK_CASCADE_DEFERRED` | `INTEGER REFERENCES ... ON DELETE CASCADE DEFERRABLE` | Deferred FK with cascade      |

---

## Column Definition Macros

### Basic Column: `COL(name, type)`

Define a column with name and type flags.

```c
/* INTEGER column */
COL("count", KVIDX_COL_INTEGER)
/* → count INTEGER */

/* TEXT column with NOT NULL */
COL("name", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL)
/* → name TEXT NOT NULL */

/* BLOB column */
COL("data", KVIDX_COL_BLOB)
/* → data BLOB */

/* REAL column */
COL("price", KVIDX_COL_REAL)
/* → price REAL */

/* INTEGER with multiple constraints */
COL("code", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL | KVIDX_COL_UNIQUE)
/* → code INTEGER NOT NULL UNIQUE */

/* Primary key */
COL("id", KVIDX_COL_PK)
/* → id INTEGER PRIMARY KEY */

/* Auto-incrementing primary key */
COL("id", KVIDX_COL_PK_AUTO)
/* → id INTEGER PRIMARY KEY AUTOINCREMENT */

/* TEXT primary key (for WITHOUT ROWID tables) */
COL("key", KVIDX_COL_TEXT | KVIDX_COL_PRIMARY_KEY)
/* → key TEXT PRIMARY KEY */
```

### Foreign Key Column: `COL_FK(name, type, refTable)`

Define a foreign key column referencing another table.

```c
/* Basic foreign key */
COL_FK("user_id", KVIDX_COL_FK, "users")
/* → user_id INTEGER REFERENCES users */

/* Foreign key with cascade delete */
COL_FK("user_id", KVIDX_COL_FK_CASCADE, "users")
/* → user_id INTEGER REFERENCES users ON DELETE CASCADE */

/* Foreign key with cascade and deferred checking */
COL_FK("user_id", KVIDX_COL_FK_CASCADE_DEFERRED, "users")
/* → user_id INTEGER REFERENCES users ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED */

/* NOT NULL foreign key with cascade */
COL_FK("user_id", KVIDX_COL_FK_CASCADE | KVIDX_COL_NOT_NULL, "users")
/* → user_id INTEGER NOT NULL REFERENCES users ON DELETE CASCADE */
```

### Default Value Columns

#### `COL_DEFAULT_INT(name, type, value)` - Integer default

```c
/* Default to 0 */
COL_DEFAULT_INT("status", KVIDX_COL_INTEGER, 0)
/* → status INTEGER DEFAULT 0 */

/* Default to 1 (active flag) */
COL_DEFAULT_INT("active", KVIDX_COL_INTEGER, 1)
/* → active INTEGER DEFAULT 1 */

/* Negative default */
COL_DEFAULT_INT("offset", KVIDX_COL_INTEGER, -100)
/* → offset INTEGER DEFAULT -100 */

/* Integer default with NOT NULL */
COL_DEFAULT_INT("priority", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL, 5)
/* → priority INTEGER NOT NULL DEFAULT 5 */
```

#### `COL_DEFAULT_REAL(name, type, value)` - Floating point default

```c
/* Default percentage */
COL_DEFAULT_REAL("rate", KVIDX_COL_REAL, 0.0)
/* → rate REAL DEFAULT 0 */

/* Pi constant */
COL_DEFAULT_REAL("multiplier", KVIDX_COL_REAL, 3.14159)
/* → multiplier REAL DEFAULT 3.14159 */

/* Default price */
COL_DEFAULT_REAL("price", KVIDX_COL_REAL | KVIDX_COL_NOT_NULL, 9.99)
/* → price REAL NOT NULL DEFAULT 9.99 */
```

#### `COL_DEFAULT_TEXT(name, type, value)` - Text string default

```c
/* Default status string */
COL_DEFAULT_TEXT("status", KVIDX_COL_TEXT, "pending")
/* → status TEXT DEFAULT 'pending' */

/* Default empty string */
COL_DEFAULT_TEXT("notes", KVIDX_COL_TEXT, "")
/* → notes TEXT DEFAULT '' */

/* Default with NOT NULL */
COL_DEFAULT_TEXT("role", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL, "user")
/* → role TEXT NOT NULL DEFAULT 'user' */
```

#### `COL_DEFAULT_NULL(name, type)` - Explicit NULL default

```c
/* Explicitly default to NULL */
COL_DEFAULT_NULL("deleted_at", KVIDX_COL_TEXT)
/* → deleted_at TEXT DEFAULT NULL */

COL_DEFAULT_NULL("parent_id", KVIDX_COL_INTEGER)
/* → parent_id INTEGER DEFAULT NULL */
```

**Note:** Cannot combine `COL_DEFAULT_NULL` with `KVIDX_COL_NOT_NULL` - this is a validation error.

#### `COL_DEFAULT_EXPR(name, type, expr)` - SQL Expression default

Use raw SQL expressions for computed defaults.

```c
/* Current timestamp */
COL_DEFAULT_EXPR("created_at", KVIDX_COL_TEXT, "CURRENT_TIMESTAMP")
/* → created_at TEXT DEFAULT CURRENT_TIMESTAMP */

/* Current date */
COL_DEFAULT_EXPR("date", KVIDX_COL_TEXT, "CURRENT_DATE")
/* → date TEXT DEFAULT CURRENT_DATE */

/* Current time */
COL_DEFAULT_EXPR("time", KVIDX_COL_TEXT, "CURRENT_TIME")
/* → time TEXT DEFAULT CURRENT_TIME */

/* Unix timestamp */
COL_DEFAULT_EXPR("unix_ts", KVIDX_COL_INTEGER, "(strftime('%s', 'now'))")
/* → unix_ts INTEGER DEFAULT (strftime('%s', 'now')) */

/* Random UUID-like value */
COL_DEFAULT_EXPR("uuid", KVIDX_COL_TEXT, "(lower(hex(randomblob(16))))")
/* → uuid TEXT DEFAULT (lower(hex(randomblob(16)))) */

/* Expression default with NOT NULL */
COL_DEFAULT_EXPR("updated_at", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL, "CURRENT_TIMESTAMP")
/* → updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP */
```

---

## Index Definition Macros

### Basic Index: `INDEX(...)`

Create a non-unique index on one or more columns.

```c
/* Single column index */
INDEX("email")
/* → CREATE INDEX IF NOT EXISTS tablename_email_idx ON tablename (email); */

/* Multi-column composite index */
INDEX("tenant_id", "user_id")
/* → CREATE INDEX IF NOT EXISTS tablename_tenant_id_user_id_idx ON tablename (tenant_id, user_id); */

/* Three-column index */
INDEX("year", "month", "day")
/* → CREATE INDEX IF NOT EXISTS tablename_year_month_day_idx ON tablename (year, month, day); */
```

### Unique Index: `INDEX_UNIQUE(...)`

Create a unique index enforcing uniqueness across the indexed columns.

```c
/* Unique single column */
INDEX_UNIQUE("email")
/* → CREATE UNIQUE INDEX IF NOT EXISTS tablename_email_idx ON tablename (email); */

/* Unique composite (combination must be unique) */
INDEX_UNIQUE("tenant_id", "username")
/* → CREATE UNIQUE INDEX IF NOT EXISTS tablename_tenant_id_username_idx ON tablename (tenant_id, username); */
```

### Named Index: `INDEX_NAMED(name, ...)`

Specify a custom index name instead of auto-generated.

```c
INDEX_NAMED("idx_user_email", "email")
/* → CREATE INDEX IF NOT EXISTS idx_user_email ON tablename (email); */

INDEX_NAMED("idx_access_lookup", "tenant_id", "resource_id", "permission")
/* → CREATE INDEX IF NOT EXISTS idx_access_lookup ON tablename (tenant_id, resource_id, permission); */
```

### Named Unique Index: `INDEX_UNIQUE_NAMED(name, ...)`

Custom-named unique index.

```c
INDEX_UNIQUE_NAMED("uniq_user_email", "email")
/* → CREATE UNIQUE INDEX IF NOT EXISTS uniq_user_email ON tablename (email); */
```

---

## Table Definition

Tables are defined using the `kvidxTableDef` struct:

```c
typedef struct kvidxTableDef {
    const char *name;               /* Table name */
    const kvidxColDef *columns;     /* Array of column definitions */
    size_t colCount;                /* Number of columns */
    const kvidxIndexDef *indexes;   /* Array of index definitions (NULL if none) */
    size_t indexCount;              /* Number of indexes */
    bool withoutRowid;              /* WITHOUT ROWID table */
} kvidxTableDef;
```

### Basic Table

```c
static const kvidxColDef cols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL("name", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
};

static const kvidxTableDef table = {
    .name = "items",
    .columns = cols,
    .colCount = sizeof(cols) / sizeof(*cols),
};
```

### Table with Indexes

```c
static const kvidxColDef cols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL("email", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL("status", KVIDX_COL_INTEGER),
    COL_DEFAULT_EXPR("created", KVIDX_COL_TEXT, "CURRENT_TIMESTAMP"),
};

static const kvidxIndexDef indexes[] = {
    INDEX_UNIQUE("email"),
    INDEX("status"),
    INDEX("status", "created"),
};

static const kvidxTableDef table = {
    .name = "users",
    .columns = cols,
    .colCount = sizeof(cols) / sizeof(*cols),
    .indexes = indexes,
    .indexCount = sizeof(indexes) / sizeof(*indexes),
};
```

### WITHOUT ROWID Table

For key-value stores or tables where you don't need SQLite's implicit rowid.

```c
static const kvidxColDef cols[] = {
    COL("key", KVIDX_COL_TEXT | KVIDX_COL_PRIMARY_KEY),
    COL("value", KVIDX_COL_BLOB),
};

static const kvidxTableDef kvTable = {
    .name = "keyvalue",
    .columns = cols,
    .colCount = 2,
    .withoutRowid = true,  /* Creates WITHOUT ROWID table */
};
```

**Generated SQL:**

```sql
CREATE TABLE IF NOT EXISTS keyvalue (key TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID;
```

---

## SQL Generation Functions

### Generate CREATE TABLE

```c
char buf[4096];
if (kvidxGenCreateTable(&table, buf, sizeof(buf))) {
    printf("SQL: %s\n", buf);
}
```

### Generate CREATE INDEX

```c
char buf[4096];
if (kvidxGenCreateIndexes(&table, buf, sizeof(buf))) {
    printf("SQL: %s\n", buf);
}
```

### Generate INSERT Statement

Generates parameterized INSERT with `?` placeholders.

```c
char buf[1024];
kvidxGenInsert(&table, buf, sizeof(buf));
/* → INSERT INTO tablename (col1, col2, col3) VALUES (?, ?, ?); */
```

### Generate SELECT Statements

```c
/* Select by ID (first column) */
kvidxGenSelectById(&table, buf, sizeof(buf));
/* → SELECT * FROM tablename WHERE id = ?; */

/* Select all rows */
kvidxGenSelectAll(&table, buf, sizeof(buf));
/* → SELECT * FROM tablename; */
```

### Generate UPDATE Statement

Updates all non-primary-key columns, filtering by first column.

```c
kvidxGenUpdateById(&table, buf, sizeof(buf));
/* → UPDATE tablename SET col2 = ?, col3 = ? WHERE id = ?; */
```

### Generate DELETE Statement

```c
kvidxGenDeleteById(&table, buf, sizeof(buf));
/* → DELETE FROM tablename WHERE id = ?; */
```

### Generate Aggregate Queries

```c
kvidxGenCount(&table, buf, sizeof(buf));
/* → SELECT COUNT(*) FROM tablename; */

kvidxGenMaxId(&table, buf, sizeof(buf));
/* → SELECT MAX(id) FROM tablename; */

kvidxGenMinId(&table, buf, sizeof(buf));
/* → SELECT MIN(id) FROM tablename; */
```

### Generate Column Type SQL Fragment

Get just the column type portion (useful for ALTER TABLE):

```c
kvidxColDef col = COL("status", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL);
char typeBuf[256];
kvidxGenColTypeSql(&col, typeBuf, sizeof(typeBuf));
/* → INTEGER NOT NULL */
```

---

## Validation Functions

### Validate Column Definition

```c
kvidxColDef col = COL("x", KVIDX_COL_INTEGER);
if (kvidxColDefIsValid(&col)) {
    /* Column is valid */
}
```

**Validation checks:**

- Exactly one base type (INTEGER, TEXT, BLOB, or REAL)
- AUTOINCREMENT only with INTEGER PRIMARY KEY
- REFERENCES has refTable set
- CASCADE_DELETE and DEFERRED require REFERENCES
- Default value type matches column type
- NULL default not combined with NOT NULL

### Validate Table Definition

```c
if (kvidxTableDefIsValid(&table)) {
    /* Table is valid */
}
```

**Validation checks:**

- Table has a name
- At least one column
- All columns are valid
- All indexed columns exist in table

---

## Real-World Examples

### Example 1: User Management System

```c
/* Users table */
static const kvidxColDef userCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL("username", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL | KVIDX_COL_UNIQUE),
    COL("email", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL | KVIDX_COL_UNIQUE),
    COL("password_hash", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL_DEFAULT_INT("status", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL, 1),
    COL_DEFAULT_INT("login_count", KVIDX_COL_INTEGER, 0),
    COL_DEFAULT_EXPR("created_at", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL, "CURRENT_TIMESTAMP"),
    COL_DEFAULT_EXPR("updated_at", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL, "CURRENT_TIMESTAMP"),
    COL_DEFAULT_NULL("last_login_at", KVIDX_COL_TEXT),
    COL_DEFAULT_NULL("deleted_at", KVIDX_COL_TEXT),
};

static const kvidxIndexDef userIndexes[] = {
    INDEX_UNIQUE("username"),
    INDEX_UNIQUE("email"),
    INDEX("status"),
    INDEX("created_at"),
};

static const kvidxTableDef usersTable = {
    .name = "users",
    .columns = userCols,
    .colCount = sizeof(userCols) / sizeof(*userCols),
    .indexes = userIndexes,
    .indexCount = sizeof(userIndexes) / sizeof(*userIndexes),
};
```

**Generated SQL:**

```sql
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    status INTEGER NOT NULL DEFAULT 1,
    login_count INTEGER DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login_at TEXT DEFAULT NULL,
    deleted_at TEXT DEFAULT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS users_username_idx ON users (username);
CREATE UNIQUE INDEX IF NOT EXISTS users_email_idx ON users (email);
CREATE INDEX IF NOT EXISTS users_status_idx ON users (status);
CREATE INDEX IF NOT EXISTS users_created_at_idx ON users (created_at);
```

### Example 2: E-Commerce Order System

```c
/* Products table */
static const kvidxColDef productCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL("sku", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL | KVIDX_COL_UNIQUE),
    COL("name", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL("description", KVIDX_COL_TEXT),
    COL("price", KVIDX_COL_REAL | KVIDX_COL_NOT_NULL),
    COL_DEFAULT_INT("stock", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL, 0),
    COL_DEFAULT_INT("active", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL, 1),
};

/* Orders table */
static const kvidxColDef orderCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL_FK("user_id", KVIDX_COL_FK | KVIDX_COL_NOT_NULL, "users"),
    COL_DEFAULT_TEXT("status", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL, "pending"),
    COL("total", KVIDX_COL_REAL | KVIDX_COL_NOT_NULL),
    COL_DEFAULT_EXPR("ordered_at", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL, "CURRENT_TIMESTAMP"),
    COL_DEFAULT_NULL("shipped_at", KVIDX_COL_TEXT),
    COL_DEFAULT_NULL("delivered_at", KVIDX_COL_TEXT),
};

static const kvidxIndexDef orderIndexes[] = {
    INDEX("user_id"),
    INDEX("status"),
    INDEX("ordered_at"),
    INDEX("user_id", "status"),
};

/* Order items (line items) */
static const kvidxColDef orderItemCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL_FK("order_id", KVIDX_COL_FK_CASCADE | KVIDX_COL_NOT_NULL, "orders"),
    COL_FK("product_id", KVIDX_COL_FK | KVIDX_COL_NOT_NULL, "products"),
    COL("quantity", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL),
    COL("unit_price", KVIDX_COL_REAL | KVIDX_COL_NOT_NULL),
};

static const kvidxIndexDef orderItemIndexes[] = {
    INDEX("order_id"),
    INDEX("product_id"),
    INDEX_UNIQUE("order_id", "product_id"),  /* One line item per product per order */
};
```

### Example 3: Multi-Tenant SaaS Application

```c
/* Tenants */
static const kvidxColDef tenantCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL("name", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL("slug", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL | KVIDX_COL_UNIQUE),
    COL_DEFAULT_TEXT("plan", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL, "free"),
    COL_DEFAULT_INT("active", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL, 1),
    COL_DEFAULT_EXPR("created_at", KVIDX_COL_TEXT, "CURRENT_TIMESTAMP"),
};

/* Tenant users (with deferred FK for circular references) */
static const kvidxColDef tenantUserCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL_FK("tenant_id", KVIDX_COL_FK_CASCADE | KVIDX_COL_NOT_NULL, "tenants"),
    COL("email", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL_DEFAULT_TEXT("role", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL, "member"),
    COL_DEFAULT_INT("active", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL, 1),
};

static const kvidxIndexDef tenantUserIndexes[] = {
    INDEX("tenant_id"),
    INDEX_UNIQUE("tenant_id", "email"),  /* Email unique per tenant */
    INDEX("tenant_id", "role"),
};

/* Tenant resources */
static const kvidxColDef resourceCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL_FK("tenant_id", KVIDX_COL_FK_CASCADE | KVIDX_COL_NOT_NULL, "tenants"),
    COL("type", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL("name", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL("data", KVIDX_COL_BLOB),
    COL_DEFAULT_EXPR("created_at", KVIDX_COL_TEXT, "CURRENT_TIMESTAMP"),
    COL_DEFAULT_EXPR("updated_at", KVIDX_COL_TEXT, "CURRENT_TIMESTAMP"),
};

static const kvidxIndexDef resourceIndexes[] = {
    INDEX("tenant_id"),
    INDEX("tenant_id", "type"),
    INDEX_UNIQUE("tenant_id", "type", "name"),  /* Unique name per type per tenant */
};
```

### Example 4: Key-Value Cache (WITHOUT ROWID)

```c
/* High-performance key-value cache */
static const kvidxColDef cacheCols[] = {
    COL("key", KVIDX_COL_TEXT | KVIDX_COL_PRIMARY_KEY),
    COL("value", KVIDX_COL_BLOB | KVIDX_COL_NOT_NULL),
    COL("expires_at", KVIDX_COL_INTEGER),  /* Unix timestamp */
    COL_DEFAULT_EXPR("created_at", KVIDX_COL_INTEGER, "(strftime('%s', 'now'))"),
};

static const kvidxIndexDef cacheIndexes[] = {
    INDEX("expires_at"),  /* For cleanup queries */
};

static const kvidxTableDef cacheTable = {
    .name = "cache",
    .columns = cacheCols,
    .colCount = sizeof(cacheCols) / sizeof(*cacheCols),
    .indexes = cacheIndexes,
    .indexCount = sizeof(cacheIndexes) / sizeof(*cacheIndexes),
    .withoutRowid = true,  /* Optimized for key lookups */
};
```

### Example 5: Audit Log with Foreign Keys

```c
/* Audit log with cascading deletes */
static const kvidxColDef auditCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL_FK("user_id", KVIDX_COL_FK_CASCADE, "users"),  /* NULL if user deleted */
    COL("action", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL("table_name", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL("record_id", KVIDX_COL_INTEGER),
    COL("old_values", KVIDX_COL_TEXT),  /* JSON */
    COL("new_values", KVIDX_COL_TEXT),  /* JSON */
    COL("ip_address", KVIDX_COL_TEXT),
    COL_DEFAULT_EXPR("created_at", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL, "CURRENT_TIMESTAMP"),
};

static const kvidxIndexDef auditIndexes[] = {
    INDEX("user_id"),
    INDEX("action"),
    INDEX("table_name", "record_id"),
    INDEX("created_at"),
    INDEX_NAMED("idx_audit_lookup", "table_name", "record_id", "created_at"),
};
```

### Example 6: Self-Referencing Tree Structure

```c
/* Categories with parent-child hierarchy */
static const kvidxColDef categoryCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL_FK("parent_id", KVIDX_COL_FK_CASCADE, "categories"),  /* Self-reference, NULL for root */
    COL("name", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL("slug", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL_DEFAULT_INT("sort_order", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL, 0),
    COL_DEFAULT_INT("depth", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL, 0),
    COL("path", KVIDX_COL_TEXT),  /* Materialized path like "/1/5/12/" */
};

static const kvidxIndexDef categoryIndexes[] = {
    INDEX("parent_id"),
    INDEX_UNIQUE("parent_id", "slug"),  /* Unique slug per parent */
    INDEX("path"),  /* For subtree queries */
    INDEX("parent_id", "sort_order"),
};
```

---

## Schema Migrations

The Schema Versioning system tracks and applies database migrations.

### Defining Migrations

```c
static const kvidxMigration migrations[] = {
    {
        .version = 1,
        .description = "Create users table",
        .upSQL = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
        .downSQL = "DROP TABLE users;",  /* Optional rollback */
    },
    {
        .version = 2,
        .description = "Add email column",
        .upSQL = "ALTER TABLE users ADD COLUMN email TEXT;",
        .downSQL = NULL,  /* SQLite doesn't support DROP COLUMN easily */
    },
    {
        .version = 3,
        .description = "Add email index",
        .upSQL = "CREATE INDEX idx_users_email ON users (email);",
        .downSQL = "DROP INDEX idx_users_email;",
    },
    {
        .version = 4,
        .description = "Create orders table",
        .upSQL =
            "CREATE TABLE orders ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  user_id INTEGER NOT NULL REFERENCES users,"
            "  total REAL NOT NULL,"
            "  created_at TEXT DEFAULT CURRENT_TIMESTAMP"
            ");"
            "CREATE INDEX idx_orders_user ON orders (user_id);",
    },
};
```

### Applying Migrations

```c
/* Apply all migrations up to version 4 */
kvidxError err = kvidxSchemaApply(db, migrations, 4, 4);
if (err != KVIDX_OK) {
    /* Handle error */
}
```

### Checking Current Version

```c
uint32_t version;
kvidxSchemaVersion(db, &version);
printf("Current schema version: %u\n", version);
```

### Check If Migration Needed

```c
if (kvidxSchemaNeedsMigration(db, 4)) {
    printf("Database needs migration to version 4\n");
}
```

### Migration with Progress Callback

```c
void onMigration(uint32_t version, const char *description, bool success, void *userData) {
    if (success) {
        printf("✓ Applied migration %u: %s\n", version, description);
    } else {
        printf("✗ Failed migration %u: %s\n", version, description);
    }
}

kvidxSchemaApplyWithCallback(db, migrations, 4, 4, onMigration, NULL);
```

### Get Applied Versions

```c
uint32_t versions[100];
size_t count;
kvidxSchemaGetAppliedVersions(db, versions, 100, &count);

printf("Applied versions: ");
for (size_t i = 0; i < count; i++) {
    printf("%u ", versions[i]);
}
```

### Creating Tables from Definitions

Convenience function to create tables directly from `kvidxTableDef` array:

```c
static const kvidxTableDef allTables[] = {
    usersTable,
    productsTable,
    ordersTable,
    orderItemsTable,
};

kvidxError err = kvidxSchemaCreateTables(db, allTables,
    sizeof(allTables) / sizeof(*allTables));
```

---

## Complete Application Example

```c
#include "kvidxkitTableDesc.h"
#include "kvidxkitSchema.h"
#include <sqlite3.h>
#include <stdio.h>

/* Define all columns */
static const kvidxColDef userCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL("username", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL | KVIDX_COL_UNIQUE),
    COL("email", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL | KVIDX_COL_UNIQUE),
    COL_DEFAULT_INT("active", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL, 1),
    COL_DEFAULT_EXPR("created_at", KVIDX_COL_TEXT, "CURRENT_TIMESTAMP"),
};

static const kvidxColDef postCols[] = {
    COL("id", KVIDX_COL_PK_AUTO),
    COL_FK("user_id", KVIDX_COL_FK_CASCADE | KVIDX_COL_NOT_NULL, "users"),
    COL("title", KVIDX_COL_TEXT | KVIDX_COL_NOT_NULL),
    COL("body", KVIDX_COL_TEXT),
    COL_DEFAULT_INT("published", KVIDX_COL_INTEGER | KVIDX_COL_NOT_NULL, 0),
    COL_DEFAULT_EXPR("created_at", KVIDX_COL_TEXT, "CURRENT_TIMESTAMP"),
};

/* Define all indexes */
static const kvidxIndexDef userIndexes[] = {
    INDEX_UNIQUE("username"),
    INDEX_UNIQUE("email"),
    INDEX("active"),
};

static const kvidxIndexDef postIndexes[] = {
    INDEX("user_id"),
    INDEX("published"),
    INDEX("user_id", "published"),
    INDEX("created_at"),
};

/* Define all tables */
static const kvidxTableDef tables[] = {
    {
        .name = "users",
        .columns = userCols,
        .colCount = sizeof(userCols) / sizeof(*userCols),
        .indexes = userIndexes,
        .indexCount = sizeof(userIndexes) / sizeof(*userIndexes),
    },
    {
        .name = "posts",
        .columns = postCols,
        .colCount = sizeof(postCols) / sizeof(*postCols),
        .indexes = postIndexes,
        .indexCount = sizeof(postIndexes) / sizeof(*postIndexes),
    },
};

int main(void) {
    sqlite3 *db;

    /* Open database */
    if (sqlite3_open("blog.db", &db) != SQLITE_OK) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }

    /* Enable foreign keys */
    sqlite3_exec(db, "PRAGMA foreign_keys = ON;", NULL, NULL, NULL);

    /* Create all tables */
    kvidxError err = kvidxSchemaCreateTables(db, tables,
        sizeof(tables) / sizeof(*tables));

    if (err != KVIDX_OK) {
        fprintf(stderr, "Failed to create tables\n");
        sqlite3_close(db);
        return 1;
    }

    /* Generate and print INSERT statement for users */
    char buf[1024];
    kvidxGenInsert(&tables[0], buf, sizeof(buf));
    printf("Users INSERT: %s\n", buf);

    /* Generate UPDATE statement */
    kvidxGenUpdateById(&tables[0], buf, sizeof(buf));
    printf("Users UPDATE: %s\n", buf);

    /* Use the generated statements with sqlite3_prepare_v2... */

    sqlite3_close(db);
    return 0;
}
```

---

## Error Handling

All generation functions return status indicators:

- `kvidxGenColTypeSql()` returns character count on success, -1 on error
- `kvidxGenCreateTable()` returns true on success, false on error
- `kvidxGenCreateIndexes()` returns true on success, false on error
- `kvidxGenInsert()` and other statement generators return true/false
- Schema functions return `kvidxError` enum values

**Common errors:**

- Buffer too small for generated SQL
- Invalid column definition (multiple base types, missing refTable, etc.)
- Invalid table definition (no columns, invalid columns)
- Index references non-existent column

---

## Storage Primitives API (v0.8.0)

Storage primitives provide atomic operations for key-value manipulation.

### Conditional Writes

```c
/* Insert only if key doesn't exist (NX) */
kvidxError err = kvidxInsertEx(i, key, term, cmd, data, len, KVIDX_SET_IF_NOT_EXISTS);
if (err == KVIDX_ERROR_CONDITION_FAILED) {
    /* Key already exists */
}

/* Update only if key exists (XX) */
err = kvidxInsertEx(i, key, term, cmd, data, len, KVIDX_SET_IF_EXISTS);

/* Always set (default behavior) */
err = kvidxInsertEx(i, key, term, cmd, data, len, KVIDX_SET_ALWAYS);
```

### Atomic Get-And-Set / Get-And-Remove

```c
/* Atomically replace value and get old value */
uint64_t oldTerm, oldCmd;
void *oldData;
size_t oldLen;
kvidxGetAndSet(i, key, newTerm, newCmd, newData, newLen,
               &oldTerm, &oldCmd, &oldData, &oldLen);
/* oldData must be freed by caller */

/* Atomically remove and get value */
kvidxGetAndRemove(i, key, &term, &cmd, &data, &len);
```

### Compare-And-Swap (CAS)

```c
/* Optimistic concurrency control */
bool swapped;
kvidxError err = kvidxCompareAndSwap(i, key,
    expectedData, expectedLen,   /* Expected current value */
    newTerm, newCmd, newData, newLen,  /* New value if match */
    &swapped);

if (err == KVIDX_ERROR_NOT_FOUND) {
    /* Key doesn't exist */
} else if (!swapped) {
    /* Value didn't match expected - someone else modified it */
}
```

### Append/Prepend

```c
/* Append data to existing value (or create new) */
size_t newLen;
kvidxAppend(i, key, term, cmd, "suffix", 6, &newLen);

/* Prepend data to existing value (or create new) */
kvidxPrepend(i, key, term, cmd, "prefix", 6, &newLen);
```

### Partial Value Access

```c
/* Read substring from value */
void *data;
size_t actualLen;
kvidxGetValueRange(i, key, offset, length, &data, &actualLen);
/* length=0 means read to end */

/* Overwrite portion of value */
kvidxSetValueRange(i, key, offset, newData, newLen, &resultLen);
```

### TTL/Expiration

```c
/* Set key to expire in 60 seconds */
kvidxSetExpire(i, key, 60000);  /* milliseconds */

/* Set absolute expiration timestamp */
kvidxSetExpireAt(i, key, timestampMs);

/* Get remaining TTL */
int64_t ttl = kvidxGetTTL(i, key);
/* Returns: milliseconds remaining, KVIDX_TTL_NONE (-1), or KVIDX_TTL_NOT_FOUND (-2) */

/* Remove expiration (make persistent) */
kvidxPersist(i, key);

/* Scan and delete expired keys */
uint64_t expiredCount;
kvidxExpireScan(i, maxKeys, &expiredCount);  /* maxKeys=0 for unlimited */
```

### Transaction Abort

```c
/* Begin transaction */
kvidxBegin(i);

/* Make some changes */
kvidxInsert(i, key1, ...);
kvidxRemove(i, key2);

/* Oops, need to rollback */
kvidxAbort(i);  /* Discards all uncommitted changes */
```

---

## History

kvidxkit was made between 2016 and 2019 to store the internal log state of a
raft implementation, which is why it supports range deletes
(for destroying logs from a non-quorum peer) as well as other easy to use
persist-resume and logical data manipulation hooks.

## Building

```bash
git submodule init
git submodule update
mkdir build
cd build
cmake ..
make -j4
```

## Testing

Run all tests via CTest:

```bash
cd build
ctest --output-on-failure
```

Or run individual test suites:

```bash
./build/src/kvidxkit-test                # Basic tests
./build/src/kvidxkit-test-comprehensive  # Edge cases, stress tests
./build/src/kvidxkit-test-errors         # Error handling
./build/src/kvidxkit-test-batch          # Batch operations
./build/src/kvidxkit-test-iterator       # Iterator API
./build/src/kvidxkit-test-stats          # Statistics API
./build/src/kvidxkit-test-config         # Configuration API
./build/src/kvidxkit-test-range          # Range operations
./build/src/kvidxkit-test-export         # Export/Import
./build/src/kvidxkit-test-tabledesc      # Table Description System
./build/src/kvidxkit-test-primitives     # Storage primitives (v0.8.0)
./build/src/kvidxkit-test-lmdb           # LMDB adapter tests
./build/src/kvidxkit-test-rocksdb        # RocksDB adapter tests
./build/src/kvidxkit-fuzz [seed]         # Fuzzer framework (seed optional)
./build/src/kvidxkit-bench [count|quick] # Performance benchmarks
```

---

## Backend Adapters

### SQLite3 Adapter

The default backend using SQLite3 for persistent storage. Best for:

- Write-heavy workloads (WAL mode)
- SQL query flexibility
- Broad platform compatibility
- Smaller datasets (< 1TB)

```c
kvidxInstance inst = {0};
inst.interface = kvidxInterfaceSqlite3;
kvidxOpen(&inst, "mydata.sqlite3", NULL);
```

### LMDB Adapter

Lightning Memory-Mapped Database backend for high-performance read-heavy workloads. Best for:

- Read-heavy workloads (zero-copy reads)
- Memory-mapped I/O performance
- Multi-process concurrent access
- Larger datasets

```c
kvidxInstance inst = {0};
inst.interface = kvidxInterfaceLmdb;
kvidxOpen(&inst, "mydata.lmdb", NULL);  // Creates directory
```

**LMDB Characteristics:**

- Creates a directory with `data.mdb` and `lock.mdb` files
- Zero-copy reads via memory mapping
- Single-writer, multiple-reader concurrency
- Ordered keys using `MDB_INTEGERKEY` for uint64_t performance
- Crash-safe with copy-on-write B+tree

### RocksDB Adapter

Facebook's RocksDB backend for high-volume write workloads. Best for:

- Write-heavy workloads (LSM-tree optimized)
- Large datasets (multi-terabyte scale)
- High compression ratios
- Write amplification tolerance

```c
kvidxInstance inst = {0};
inst.interface = kvidxInterfaceRocksdb;
kvidxOpen(&inst, "mydata.rocks", NULL);  // Creates directory
```

**RocksDB Characteristics:**

- Creates a directory with SST files, WAL, and metadata
- LSM-tree architecture optimized for writes
- Configurable compression (LZ4, Snappy, Zstd)
- Built-in bloom filters for fast negative lookups
- Background compaction for space reclamation

**Build with RocksDB:**

```bash
cmake .. -DKVIDXKIT_ENABLE_ROCKSDB=ON
```

### Adding New Adapters

To add a new backend adapter:

1. Create header file (e.g., `kvidxkitAdapterMyBackend.h`) declaring all interface functions
2. Implement all `kvidxInterface` function pointers
3. Add extern declaration to `kvidxkit.h`:
   ```c
   extern const kvidxInterface kvidxInterfaceMyBackend;
   ```
4. Define the interface vtable in your implementation:
   ```c
   const kvidxInterface kvidxInterfaceMyBackend = {
       .begin = myBackendBegin,
       .commit = myBackendCommit,
       .get = myBackendGet,
       // ... all function pointers
   };
   ```
5. Add to `src/CMakeLists.txt`
6. Add to fuzzer's adapter registry in `kvidxkit-fuzz.c`:
   ```c
   static const AdapterDesc g_adapters[] = {
       {"SQLite3", &kvidxInterfaceSqlite3, ".sqlite3", false},
       {"LMDB",    &kvidxInterfaceLmdb,    "",         true },
       {"MyBackend", &kvidxInterfaceMyBackend, ".myext", false},
   };
   ```

The fuzzer will automatically test your new adapter for ACID compliance and cross-adapter consistency.

---

## Fuzzer Framework

kvidxkit includes a comprehensive fuzzer for testing adapter stability, consistency, and ACID compliance.

### Running the Fuzzer

```bash
# Run with random seed (printed for reproduction)
./build/src/kvidxkit-fuzz

# Run with specific seed for reproducibility
./build/src/kvidxkit-fuzz 12345
```

### Test Categories

1. **Operation Sequence Fuzzer** - Random operations (insert, get, remove, etc.) to find crashes or inconsistencies
2. **ACID Compliance Tests**
   - **Atomicity** - Batch operations are all-or-nothing
   - **Consistency** - Database constraints maintained after operations
   - **Durability** - Data persists across close/reopen cycles
3. **Stress/Endurance Tests** - High-volume operations measuring throughput
4. **Boundary Testing** - Edge cases (key boundaries, data sizes, empty database)
5. **Cross-Adapter Consistency** - Same operation sequence produces identical results across all adapters

### Extending the Fuzzer

The fuzzer uses a generic adapter registry. To add a new adapter to fuzzing:

```c
// In kvidxkit-fuzz.c, add to g_adapters array:
static const AdapterDesc g_adapters[] = {
    {"SQLite3", &kvidxInterfaceSqlite3, ".sqlite3", false},
    {"LMDB",    &kvidxInterfaceLmdb,    "",         true },
    // Add new adapters here - they'll be tested automatically
};
```

All registered adapters are automatically tested for:

- Individual correctness (operation sequences, ACID, stress, boundaries)
- Cross-adapter consistency (identical behavior across all backends)

---

## Performance Benchmarks

kvidxkit includes a comprehensive benchmark framework for comparing adapter performance.

### Running Benchmarks

```bash
# Full benchmark suite (100,000 operations per test)
./build/src/kvidxkit-bench

# Quick benchmark (10,000 operations per test)
./build/src/kvidxkit-bench quick

# Custom operation count
./build/src/kvidxkit-bench 50000
```

### Benchmark Categories

| Benchmark         | Description                         |
| ----------------- | ----------------------------------- |
| Sequential Insert | Insert keys in order (1, 2, 3, ...) |
| Sequential Read   | Read keys in order                  |
| Random Insert     | Insert keys in random order         |
| Random Read       | Read keys in random order           |
| Mixed 80/20 R/W   | 80% reads, 20% writes workload      |
| Batch Insert      | Bulk insert using kvidxInsertBatch  |
| Range Count Query | Count keys in random ranges         |
| Iterator Scan     | Full forward scan using getNext     |
| Large Data (4KB)  | Insert/read 4KB blobs               |
| Delete            | Delete all keys                     |

### Sample Results

Results from quick benchmark (10,000 ops, 64-byte data):

| Benchmark         | SQLite3      | LMDB         |
| ----------------- | ------------ | ------------ |
| Sequential Insert | 2.5M ops/sec | 0.9M ops/sec |
| Sequential Read   | 2.5M ops/sec | 1.3M ops/sec |
| Random Read       | 2.1M ops/sec | 1.2M ops/sec |
| Range Count Query | 12K ops/sec  | 33K ops/sec  |
| Large Data (4KB)  | 215 MB/s     | 411 MB/s     |

**Key Observations:**

- SQLite3 excels at small, transactional operations
- LMDB excels at range queries and large data throughput
- Both adapters perform well when operations are batched in transactions

### Extending Benchmarks

The benchmark uses the same adapter registry as the fuzzer. To add a new adapter:

```c
// In kvidxkit-bench.c, add to g_adapters array:
static const AdapterDesc g_adapters[] = {
    {"SQLite3", &kvidxInterfaceSqlite3, ".sqlite3", false},
    {"LMDB",    &kvidxInterfaceLmdb,    "",         true },
    // Add new adapters here - they'll be benchmarked automatically
};
```
