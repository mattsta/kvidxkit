kvidxkit: Key-Value Index Kit
=============================

kvidxkit is a C interface for ordered key-value storage manipulation with pluggable backends.

Currently kvidxkit comes with an `sqlite3` adapter for persisting changes, but you can add any other
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
```bash
./build/src/kvidxkit-test
```
