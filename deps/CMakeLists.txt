# Conditionally build dependencies based on enabled adapters
if(KVIDXKIT_ENABLE_SQLITE3)
    add_subdirectory(sqlite3)
endif()

if(KVIDXKIT_ENABLE_LMDB)
    add_subdirectory(lmdb)
endif()

if(KVIDXKIT_ENABLE_ROCKSDB)
    add_subdirectory(rocksdb/wrapper)
endif()

# vi:ai et sw=4 ts=4:
