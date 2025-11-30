# kvidxkit Examples

This directory contains complete working examples demonstrating common kvidxkit use cases.

## Building Examples

First, build kvidxkit:

```bash
cd /path/to/kvidxkit
mkdir build && cd build
cmake ..
make -j4
```

Then build individual examples:

```bash
cd examples/raft-log
gcc -o raft-log main.c -I../../src -L../../build/src -lkvidxkit-static -lsqlite3
./raft-log
```

## Examples

### raft-log

Demonstrates using kvidxkit as a Raft consensus log store.

**Features demonstrated:**
- Log append with term tracking
- Log truncation for conflict resolution
- Entry replication queries
- Commit and apply pattern

**Run:**
```bash
cd raft-log && ./raft-log
```

---

### session-store

Demonstrates using kvidxkit as a session store with TTL.

**Features demonstrated:**
- Session creation with expiration
- TTL refresh on activity
- Automatic session cleanup
- Atomic session operations

**Run:**
```bash
cd session-store && ./session-store
```

---

### timeseries

Demonstrates using kvidxkit for time-series data storage.

**Features demonstrated:**
- Timestamp-based keys
- Batch insert for high throughput
- Range queries and aggregation
- Retention policy with range delete
- JSON export

**Run:**
```bash
cd timeseries && ./timeseries
```

---

### message-queue

Demonstrates using kvidxkit as a persistent message/job queue.

**Features demonstrated:**
- FIFO message ordering
- Priority levels (via cmd field)
- Atomic dequeue (GetAndRemove)
- Message expiration (TTL)
- Batch enqueue

**Run:**
```bash
cd message-queue && ./message-queue
```

---

## Common Patterns

### Initialization

```c
kvidxInstance inst = {0};
inst.interface = kvidxInterfaceSqlite3;

kvidxConfig config = kvidxConfigDefault();
config.journalMode = KVIDX_JOURNAL_WAL;

kvidxOpenWithConfig(&inst, "database.db", &config, NULL);
```

### Cleanup

```c
kvidxClose(&inst);
```

### Error Handling

```c
kvidxError err = kvidxSomeOperation(&inst, ...);
if (kvidxIsError(err)) {
    fprintf(stderr, "Error: %s\n", kvidxGetLastErrorMessage(&inst));
    kvidxClearError(&inst);
}
```

## Using Different Backends

Examples default to SQLite3, but can use other backends:

```c
// LMDB (directory-based)
#ifdef KVIDXKIT_HAS_LMDB
inst.interface = kvidxInterfaceLmdb;
kvidxOpen(&inst, "database.lmdb", NULL);
#endif

// RocksDB (directory-based)
#ifdef KVIDXKIT_HAS_ROCKSDB
inst.interface = kvidxInterfaceRocksdb;
kvidxOpen(&inst, "database.rocks", NULL);
#endif
```

## Notes

- Examples create and clean up their own database files
- For production use, implement proper error handling
- See [../docs/](../docs/) for comprehensive documentation
