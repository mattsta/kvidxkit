# kvidxkit Architecture

This document describes the internal architecture of kvidxkit, a C library providing a unified interface for ordered key-value storage with pluggable backends.

## Overview

kvidxkit uses a **plugin architecture** to abstract multiple storage engines behind a common C interface. Applications write to a single API while choosing from SQLite3, LMDB, or RocksDB backends at compile time or runtime.

```
┌─────────────────────────────────────────────────────────┐
│                    Application Code                     │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│                   kvidxkit Core API                     │
│  (kvidxOpen, kvidxInsert, kvidxGet, kvidxBegin, ...)    │
└─────────────────────────────────────────────────────────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│ SQLite3       │ │ LMDB          │ │ RocksDB       │
│ Adapter       │ │ Adapter       │ │ Adapter       │
└───────────────┘ └───────────────┘ └───────────────┘
            │              │              │
            ▼              ▼              ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│ SQLite3 DB    │ │ LMDB Env      │ │ RocksDB Store │
└───────────────┘ └───────────────┘ └───────────────┘
```

## Core Design Patterns

### Virtual Table (Interface) Pattern

The core abstraction is `kvidxInterface`, a struct of function pointers that each adapter implements:

```c
struct kvidxInterface {
    // Lifecycle
    bool (*open)(kvidxInstance *, const char *path, const char **err);
    void (*close)(kvidxInstance *);

    // Transactions
    bool (*begin)(kvidxInstance *);
    bool (*commit)(kvidxInstance *);
    bool (*abort)(kvidxInstance *);

    // CRUD Operations
    bool (*get)(kvidxInstance *, uint64_t key, ...);
    bool (*insert)(kvidxInstance *, uint64_t key, ...);
    bool (*remove)(kvidxInstance *, uint64_t key);

    // ... additional operations
};
```

This enables runtime polymorphism in C - the same application code works with any backend.

### Registry Pattern

The adapter registry (`kvidxkitRegistry.c`) provides runtime discovery of compiled-in adapters:

```c
// Discover available backends at runtime
size_t count = kvidxGetAdapterCount();
for (size_t i = 0; i < count; i++) {
    const kvidxAdapterInfo *info = kvidxGetAdapterByIndex(i);
    printf("Available: %s\n", info->name);
}
```

### Instance-Based State

Each database connection is encapsulated in a `kvidxInstance`:

```c
struct kvidxInstance {
    void *kvidxdata;           // Backend-specific private state
    void *clientdata;          // User-provided context
    kvidxInterface interface;  // Virtual table

    // Error tracking
    kvidxError lastError;
    char lastErrorMessage[256];
    bool transactionActive;

    // Configuration
    kvidxConfig config;
    bool configInitialized;
};
```

## Data Model

### Key-Value Entry Structure

Each entry stores four components:

| Field  | Type       | Purpose                      |
| ------ | ---------- | ---------------------------- |
| `key`  | `uint64_t` | Primary index (ordered)      |
| `term` | `uint64_t` | Version/epoch identifier     |
| `cmd`  | `uint64_t` | Command type or metadata tag |
| `data` | `void*`    | Arbitrary binary payload     |

The `term` and `cmd` fields support use cases like Raft consensus logs where entries need version tracking and command classification.

### Ordered Key Access

Keys are always ordered as unsigned 64-bit integers, enabling:

- Sequential iteration (`kvidxGetNext`, `kvidxGetPrev`)
- Range queries (`kvidxCountRange`, `kvidxRemoveRange`)
- Min/max key discovery (`kvidxGetMinKey`, `kvidxMaxKey`)

## Module Organization

```
src/
├── kvidxkit.h               # Public API header
├── kvidxkit.c               # Core implementation
├── kvidxkitConfig.h         # Configuration types
├── kvidxkitErrors.h         # Error code definitions
├── kvidxkitErrors.c         # Error string conversion
├── kvidxkitIterator.h       # Iterator types
├── kvidxkitIterator.c       # Iterator implementation
├── kvidxkitExport.h         # Export/import types
├── kvidxkitRegistry.h       # Adapter registry API
├── kvidxkitRegistry.c       # Registry implementation
├── kvidxkitTableDesc.h      # Table description DSL
├── kvidxkitTableDesc.c      # Schema generation
├── kvidxkitSchema.h         # Schema versioning
├── kvidxkitSchema.c         # Migration tracking
├── kvidxkitAdapterSqlite3.* # SQLite3 backend
├── kvidxkitAdapterLmdb.*    # LMDB backend
└── kvidxkitAdapterRocksdb.* # RocksDB backend
```

## Storage Adapters

### SQLite3 Adapter

**Storage Model:**

- Single database file
- Explicit SQL schema with `log` table
- Separate `_kvidx_ttl` table for expiration metadata

**Schema:**

```sql
CREATE TABLE log (
    id INTEGER PRIMARY KEY,
    term INTEGER,
    cmd INTEGER,
    data BLOB
);

CREATE TABLE _kvidx_ttl (
    id INTEGER,
    expires_at INTEGER
);
CREATE INDEX idx_ttl ON _kvidx_ttl(expires_at);
```

**Performance Characteristics:**

- Pre-compiled prepared statements for all operations
- WAL mode by default for concurrent access
- Extensive configuration options (journal mode, sync mode, cache size)
- Support for in-memory databases (`:memory:`)

**Best For:** Rich configuration, SQL debugging, data integrity guarantees

### LMDB Adapter

**Storage Model:**

- Directory-based (creates `data.mdb`, `lock.mdb`)
- Two named databases: `_kvidx_data`, `_kvidx_ttl`
- Keys stored with `MDB_INTEGERKEY` flag for efficient integer comparison

**Value Packing:**

```
┌──────────────┬──────────────┬───────────────────┐
│ term (8B)    │ cmd (8B)     │ data (variable)   │
└──────────────┴──────────────┴───────────────────┘
```

**Performance Characteristics:**

- Zero-copy reads via memory-mapped I/O
- Persistent read transactions for minimal overhead
- Copy-on-write for fast transaction aborts
- Configurable map size (default: 1GB)

**Best For:** Read-heavy workloads, lowest latency, multi-process access

### RocksDB Adapter

**Storage Model:**

- LSM-tree based storage
- Big-endian key encoding for lexicographic ordering
- TTL entries use prefixed keys (`\x00TTL` + key)

**Value Packing:**

```
┌──────────────────┬──────────────┬───────────────────┐
│ term (8B BE)     │ cmd (8B)     │ data (variable)   │
└──────────────────┴──────────────┴───────────────────┘
```

**Performance Characteristics:**

- Write batch with index for transaction-aware iteration
- Separate sync/async write options
- Automatic lazy expiration on reads
- Background compaction for sustained throughput

**Best For:** Write-heavy workloads, large datasets, compression needs

## Error Handling Architecture

### Error Categories

```c
typedef enum {
    KVIDX_OK = 0,

    // Argument errors
    KVIDX_ERROR_INVALID_ARGUMENT,

    // Data errors
    KVIDX_ERROR_DUPLICATE_KEY,
    KVIDX_ERROR_NOT_FOUND,
    KVIDX_ERROR_CORRUPT,

    // Resource errors
    KVIDX_ERROR_DISK_FULL,
    KVIDX_ERROR_IO,
    KVIDX_ERROR_NOMEM,
    KVIDX_ERROR_TOO_BIG,

    // Transaction errors
    KVIDX_ERROR_TRANSACTION_ACTIVE,
    KVIDX_ERROR_NO_TRANSACTION,

    // Access errors
    KVIDX_ERROR_READONLY,
    KVIDX_ERROR_LOCKED,

    // Logic errors
    KVIDX_ERROR_CONSTRAINT,
    KVIDX_ERROR_SCHEMA,
    KVIDX_ERROR_RANGE,
    KVIDX_ERROR_NOT_SUPPORTED,
    KVIDX_ERROR_CANCELLED,

    // Conditional operation errors
    KVIDX_ERROR_CONDITION_FAILED,
    KVIDX_ERROR_EXPIRED,

    // Internal errors
    KVIDX_ERROR_INTERNAL = 99
} kvidxError;
```

### Per-Instance Error Tracking

Each instance maintains error state:

- `lastError`: Most recent error code
- `lastErrorMessage[256]`: Formatted error message with context

```c
kvidxError err = kvidxGetLastError(&inst);
const char *msg = kvidxGetLastErrorMessage(&inst);
kvidxClearError(&inst);
```

## Configuration System

The `kvidxConfig` structure provides tuning options:

| Option                    | Default | Description                |
| ------------------------- | ------- | -------------------------- |
| `cacheSizeBytes`          | 32 MB   | SQLite page cache          |
| `journalMode`             | WAL     | Journal strategy           |
| `syncMode`                | NORMAL  | Durability vs. performance |
| `busyTimeoutMs`           | 5000    | Lock wait timeout          |
| `enableForeignKeys`       | false   | FK constraint enforcement  |
| `enableRecursiveTriggers` | true    | Recursive trigger support  |
| `readOnly`                | false   | Read-only mode             |
| `mmapSizeBytes`           | 0       | Memory-mapped I/O size     |
| `pageSize`                | 4096    | Page size in bytes         |

## Transaction Model

### ACID Properties

- **Atomicity**: All operations within `begin`/`commit` are atomic
- **Consistency**: Schema constraints enforced (if enabled)
- **Isolation**: Write transactions are serialized
- **Durability**: Configurable via `syncMode`

### Transaction Flow

```
┌─────────────────┐
│    kvidxBegin   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Operations    │◄────┐
│ (insert/remove) │     │
└────────┬────────┘     │
         │              │
         ▼              │
┌─────────────────┐     │
│ More operations?├─────┘
└────────┬────────┘
         │ No
         ▼
    ┌────┴────┐
    │         │
    ▼         ▼
┌────────┐ ┌────────┐
│ Commit │ │ Abort  │
└────────┘ └────────┘
```

## Iterator Architecture

The iterator provides efficient range traversal:

```c
struct kvidxIterator {
    kvidxInstance *inst;
    uint64_t startKey, endKey;
    kvidxIterDirection direction;
    void *internalState;  // Backend-specific cursor
};
```

**Operations:**

- `kvidxIteratorCreate`: Initialize with bounds and direction
- `kvidxIteratorNext`: Advance to next entry
- `kvidxIteratorValid`: Check if positioned at valid entry
- `kvidxIteratorSeek`: Jump to specific key
- `kvidxIteratorGet`: Read current entry
- `kvidxIteratorDestroy`: Free resources

## Advanced Features

### Storage Primitives (v0.8.0)

| Category           | Operations                                                    |
| ------------------ | ------------------------------------------------------------- |
| Conditional Writes | `InsertEx`, `InsertNX`, `InsertXX`                            |
| Atomic RMW         | `GetAndSet`, `GetAndRemove`                                   |
| Compare-and-Swap   | `CompareAndSwap`, `CompareTermAndSwap`                        |
| String Operations  | `Append`, `Prepend`, `GetValueRange`, `SetValueRange`         |
| TTL/Expiration     | `SetExpire`, `SetExpireAt`, `GetTTL`, `Persist`, `ExpireScan` |

### Export/Import System

Supports three formats:

- **Binary**: Compact, fastest
- **JSON**: Human-readable
- **CSV**: Spreadsheet-compatible

Progress callbacks enable monitoring long operations:

```c
bool progressCallback(uint64_t current, uint64_t total, void *userData) {
    printf("Progress: %lu/%lu\n", current, total);
    return true;  // Return false to abort
}
```

## Build Configuration

CMake options control which adapters are compiled:

| Option                    | Default | Description             |
| ------------------------- | ------- | ----------------------- |
| `KVIDXKIT_ENABLE_SQLITE3` | ON      | Include SQLite3 adapter |
| `KVIDXKIT_ENABLE_LMDB`    | ON      | Include LMDB adapter    |
| `KVIDXKIT_ENABLE_ROCKSDB` | OFF     | Include RocksDB adapter |

At least one adapter must be enabled. Compile definitions are propagated to consuming code:

- `KVIDXKIT_HAS_SQLITE3`
- `KVIDXKIT_HAS_LMDB`
- `KVIDXKIT_HAS_ROCKSDB`

## Performance Considerations

### Cache-Line Optimization

The `kvidxInterface` struct organizes function pointers into three cache lines, grouping frequently-used operations together for better locality.

### Batch Operations

Batch inserts (`kvidxInsertBatch`, `kvidxInsertBatchEx`) wrap multiple operations in a single transaction, achieving 10-100x better throughput than individual inserts.

### Memory Management

- **SQLite3**: Dynamic allocation with configurable cache
- **LMDB**: Pre-allocated memory map
- **RocksDB**: Dynamic with internal caching

## Version History

| Version | Features                                      |
| ------- | --------------------------------------------- |
| Core    | Basic CRUD, transactions, iteration           |
| v0.4.0  | Batch operations, error handling              |
| v0.5.0  | Range operations, statistics, configuration   |
| v0.6.0  | Export/import functionality                   |
| v0.7.0  | Table description DSL, schema versioning      |
| v0.8.0  | Storage primitives (CAS, TTL, append/prepend) |
