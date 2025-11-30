cmake_minimum_required(VERSION 4.1)
project(kvidxkit C)

if (TARGET ${PROJECT_NAME})
    return()
endif()

# ============================================================
# Adapter Selection Options
# ============================================================
# Each adapter can be independently enabled/disabled.
# SQLite3 is ON by default (minimal, pure C).
# LMDB and RocksDB are OFF by default (opt-in).
# RocksDB requires C++ stdlib linking.
option(KVIDXKIT_ENABLE_SQLITE3 "Build SQLite3 adapter" ON)
option(KVIDXKIT_ENABLE_LMDB    "Build LMDB adapter"    ON)
option(KVIDXKIT_ENABLE_ROCKSDB "Build RocksDB adapter" OFF)

# Print configuration summary
message(STATUS "kvidxkit adapter configuration:")
message(STATUS "  SQLite3: ${KVIDXKIT_ENABLE_SQLITE3}")
message(STATUS "  LMDB:    ${KVIDXKIT_ENABLE_LMDB}")
message(STATUS "  RocksDB: ${KVIDXKIT_ENABLE_ROCKSDB}")

# Validate at least one adapter is enabled
if(NOT KVIDXKIT_ENABLE_SQLITE3 AND NOT KVIDXKIT_ENABLE_LMDB AND NOT KVIDXKIT_ENABLE_ROCKSDB)
    message(FATAL_ERROR "At least one adapter must be enabled. Use -DKVIDXKIT_ENABLE_SQLITE3=ON, -DKVIDXKIT_ENABLE_LMDB=ON, or -DKVIDXKIT_ENABLE_ROCKSDB=ON")
endif()

# Enable testing support
enable_testing()

add_subdirectory(deps)
add_subdirectory(src)

# vi:ai et sw=4 ts=4:
