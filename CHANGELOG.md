# Changelog

notable changes

---

## [0.8.0] - Storage Primitives

### Added

- **Conditional writes**: `kvidxInsertEx()`, `kvidxInsertNX()`, `kvidxInsertXX()`
  - `KVIDX_SET_ALWAYS` - unconditional write (default)
  - `KVIDX_SET_IF_NOT_EXISTS` - create only if key absent
  - `KVIDX_SET_IF_EXISTS` - update only if key exists
- **Atomic operations**:
  - `kvidxGetAndSet()` - atomically get current value and set new value
  - `kvidxGetAndRemove()` - atomically get and delete
- **Compare-and-swap**:
  - `kvidxCompareAndSwap()` - CAS based on data content
  - `kvidxCompareTermAndSwap()` - CAS based on term/version
- **String operations**:
  - `kvidxAppend()` - append data to existing value
  - `kvidxPrepend()` - prepend data to existing value
  - `kvidxGetValueRange()` - read portion of value
  - `kvidxSetValueRange()` - write to portion of value
- **TTL/Expiration**:
  - `kvidxSetExpire()` - set relative TTL in milliseconds
  - `kvidxSetExpireAt()` - set absolute expiration timestamp
  - `kvidxGetTTL()` - get remaining TTL
  - `kvidxPersist()` - remove expiration from key
  - `kvidxExpireScan()` - scan and remove expired keys
- New error codes: `KVIDX_ERROR_CONDITION_FAILED`, `KVIDX_ERROR_EXPIRED`
- TTL special values: `KVIDX_TTL_NONE`, `KVIDX_TTL_NOT_FOUND`
- Transaction abort: `kvidxAbort()` for rollback

---

## [0.7.0] - Table Descriptions and Schema

### Added

- **Table Description DSL** (`kvidxkitTableDesc.h`):
  - Declarative table definitions in C
  - Column types: INTEGER, TEXT, BLOB, REAL
  - Constraints: PRIMARY KEY, AUTOINCREMENT, NOT NULL, UNIQUE
  - Foreign key support with CASCADE DELETE
  - Default values including SQL expressions
  - Index definitions
  - Automatic SQL generation
- **Schema Versioning** (`kvidxkitSchema.h`):
  - Migration tracking with version history
  - Automatic schema management
  - Idempotent migration application
  - Progress callbacks for migrations

---

## [0.6.0] - Export/Import

### Added

- **Export API** (`kvidxExport()`):
  - Binary format (compact, fastest)
  - JSON format (human-readable)
  - CSV format (spreadsheet-compatible)
  - Key range filtering
  - Progress callbacks
- **Import API** (`kvidxImport()`):
  - Format auto-detection
  - Data validation
  - Duplicate key handling (fail or skip)
  - Clear-before-import option
  - Progress callbacks

### New Types

- `kvidxExportFormat` enum
- `kvidxExportOptions` structure
- `kvidxImportOptions` structure
- `kvidxProgressCallback` function pointer

---

## [0.5.0] - Statistics, Configuration, and Range Operations

### Added

- **Statistics API**:
  - `kvidxGetStats()` - comprehensive database statistics
  - `kvidxGetKeyCount()` - total number of keys
  - `kvidxGetMinKey()` - minimum key value
  - `kvidxGetDataSize()` - total data size in bytes
  - `kvidxStats` structure with detailed metrics
- **Configuration API**:
  - `kvidxConfigDefault()` - get default configuration
  - `kvidxOpenWithConfig()` - open with custom configuration
  - `kvidxUpdateConfig()` - modify runtime configuration
  - `kvidxGetConfig()` - retrieve current configuration
  - `kvidxConfig` structure with tuning options
- **Range Operations**:
  - `kvidxRemoveRange()` - delete keys in range with boundary control
  - `kvidxCountRange()` - count keys in range
  - `kvidxExistsInRange()` - check if any keys exist in range

### New Configuration Options

- `cacheSizeBytes` - SQLite page cache size
- `journalMode` - journal strategy (DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF)
- `syncMode` - sync strategy (OFF, NORMAL, FULL, EXTRA)
- `busyTimeoutMs` - lock wait timeout
- `enableForeignKeys` - FK constraint enforcement
- `enableRecursiveTriggers` - recursive trigger support
- `readOnly` - read-only mode
- `mmapSizeBytes` - memory-mapped I/O size
- `pageSize` - page size in bytes

---

## [0.4.0] - Batch Operations and Error Handling

### Added

- **Batch Operations**:
  - `kvidxInsertBatch()` - insert multiple entries in single transaction
  - `kvidxInsertBatchEx()` - batch insert with filter callback
  - `kvidxEntry` structure for batch operations
  - `kvidxBatchCallback` function pointer for filtering
- **Error Handling**:
  - Per-instance error tracking (`lastError`, `lastErrorMessage`)
  - `kvidxGetLastError()` - retrieve last error code
  - `kvidxGetLastErrorMessage()` - retrieve formatted error message
  - `kvidxClearError()` - reset error state
  - `kvidxSetError()` - internal error setting with printf-style formatting
  - `kvidxErrorString()` - convert error code to string
  - `kvidxIsOk()` / `kvidxIsError()` - helper macros

### New Error Codes

- `KVIDX_ERROR_INVALID_ARGUMENT`
- `KVIDX_ERROR_DUPLICATE_KEY`
- `KVIDX_ERROR_NOT_FOUND`
- `KVIDX_ERROR_DISK_FULL`
- `KVIDX_ERROR_IO`
- `KVIDX_ERROR_CORRUPT`
- `KVIDX_ERROR_TRANSACTION_ACTIVE`
- `KVIDX_ERROR_NO_TRANSACTION`
- `KVIDX_ERROR_READONLY`
- `KVIDX_ERROR_LOCKED`
- `KVIDX_ERROR_NOMEM`
- `KVIDX_ERROR_TOO_BIG`
- `KVIDX_ERROR_CONSTRAINT`
- `KVIDX_ERROR_SCHEMA`
- `KVIDX_ERROR_RANGE`
- `KVIDX_ERROR_NOT_SUPPORTED`
- `KVIDX_ERROR_CANCELLED`
- `KVIDX_ERROR_INTERNAL`

---

## [0.3.0] - Current Release

### Features

- Core key-value operations with ordered uint64 keys
- Three storage backends: SQLite3, LMDB, RocksDB
- Transaction support (begin, commit)
- Iterator API for range traversal
- Adapter registry for runtime backend discovery

### Storage Backends

- **SQLite3** (default): File-based, configurable, WAL mode support
- **LMDB**: Memory-mapped, zero-copy reads, multi-process support
- **RocksDB**: LSM-tree based, optimized for writes

### Core API

- `kvidxOpen()` / `kvidxClose()` - lifecycle management
- `kvidxBegin()` / `kvidxCommit()` - transaction control
- `kvidxInsert()` - create/update entries
- `kvidxGet()` - retrieve by key
- `kvidxGetNext()` / `kvidxGetPrev()` - navigation
- `kvidxExists()` / `kvidxExistsDual()` - existence checks
- `kvidxRemove()` - delete by key
- `kvidxRemoveAfterNInclusive()` / `kvidxRemoveBeforeNInclusive()` - bulk delete
- `kvidxMaxKey()` - get maximum key
- `kvidxFsync()` - force sync to disk

### Iterator API

- `kvidxIteratorCreate()` - create range iterator
- `kvidxIteratorNext()` - advance iterator
- `kvidxIteratorGet()` - read current entry
- `kvidxIteratorKey()` - get current key
- `kvidxIteratorValid()` - check validity
- `kvidxIteratorSeek()` - seek to key
- `kvidxIteratorDestroy()` - free iterator

### Registry API

- `kvidxGetAdapterCount()` - count available backends
- `kvidxGetAdapterByIndex()` - get backend by index
- `kvidxGetAdapterByName()` - get backend by name
- `kvidxHasAdapter()` - check backend availability

---

## [0.2.0] - Multi-Backend Support

### Added

- LMDB adapter implementation
- RocksDB adapter implementation
- CMake options for enabling/disabling backends
- Adapter registry for runtime discovery

---

## [0.1.0] - Initial Release

### Added

- Core kvidxkit library
- SQLite3 adapter implementation
- Basic CRUD operations
- Transaction support
- Iterator API
- Test suite and benchmarks
