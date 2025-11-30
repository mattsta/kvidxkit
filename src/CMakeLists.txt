set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(DK_VERSION 0.3.0)
set(DK_ABI_UPDATE 0.3.0)

if(APPLE)
    set(CMAKE_SHARED_MODULE_CREATE_C_FLAGS
        "${CMAKE_SHARED_MODULE_CREATE_C_FLAGS} -undefined dynamic_lookup")
    cmake_policy(SET CMP0042 NEW)
endif()

# If you need debug, build with:
# cmake -DCMAKE_BUILD_TYPE=Debug ..
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -pedantic -std=c99 -Wno-missing-field-initializers")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wstrict-aliasing -Wstrict-overflow")

if(CMAKE_BUILD_TYPE MATCHES Debug)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2")
endif()

# ============================================================
# Core Sources (always built)
# ============================================================
set(KVIDXKIT_SOURCES
    kvidxkit.c
    kvidxkitErrors.c
    kvidxkitIterator.c
    kvidxkitTableDesc.c
    kvidxkitSchema.c
    kvidxkitRegistry.c
)

# ============================================================
# Conditional Adapter Sources
# ============================================================
if(KVIDXKIT_ENABLE_SQLITE3)
    list(APPEND KVIDXKIT_SOURCES kvidxkitAdapterSqlite3.c)
endif()

if(KVIDXKIT_ENABLE_LMDB)
    list(APPEND KVIDXKIT_SOURCES kvidxkitAdapterLmdb.c)
endif()

if(KVIDXKIT_ENABLE_ROCKSDB)
    list(APPEND KVIDXKIT_SOURCES kvidxkitAdapterRocksdb.c)
endif()

add_library(kvidxkit OBJECT ${KVIDXKIT_SOURCES})

# ============================================================
# Compile Definitions for Adapter Availability
# ============================================================
if(KVIDXKIT_ENABLE_SQLITE3)
    target_compile_definitions(kvidxkit PUBLIC KVIDXKIT_HAS_SQLITE3=1)
endif()

if(KVIDXKIT_ENABLE_LMDB)
    target_compile_definitions(kvidxkit PUBLIC KVIDXKIT_HAS_LMDB=1)
endif()

if(KVIDXKIT_ENABLE_ROCKSDB)
    target_compile_definitions(kvidxkit PUBLIC KVIDXKIT_HAS_ROCKSDB=1)
    target_include_directories(kvidxkit PRIVATE ${CMAKE_SOURCE_DIR}/deps/rocksdb/include)
endif()

# ============================================================
# Library Variants
# ============================================================
add_library(kvidxkit-shared  MODULE $<TARGET_OBJECTS:kvidxkit>)
add_library(kvidxkit-static  STATIC $<TARGET_OBJECTS:kvidxkit>)
add_library(kvidxkit-library SHARED $<TARGET_OBJECTS:kvidxkit>)

set_target_properties(kvidxkit-shared PROPERTIES PREFIX "") # don't prefix "lib"

set_target_properties(kvidxkit-shared  PROPERTIES OUTPUT_NAME kvidxkit)
set_target_properties(kvidxkit-static  PROPERTIES OUTPUT_NAME kvidxkit)
set_target_properties(kvidxkit-library PROPERTIES OUTPUT_NAME kvidxkit)

# ============================================================
# Conditional Linking
# ============================================================
set(KVIDXKIT_LINK_DEPS "")

if(KVIDXKIT_ENABLE_SQLITE3)
    list(APPEND KVIDXKIT_LINK_DEPS sqlite3-static)
endif()

if(KVIDXKIT_ENABLE_LMDB)
    list(APPEND KVIDXKIT_LINK_DEPS lmdb-static)
endif()

if(KVIDXKIT_ENABLE_ROCKSDB)
    list(APPEND KVIDXKIT_LINK_DEPS rocksdb)
    # C++ standard library only needed for RocksDB
    if(APPLE)
        list(APPEND KVIDXKIT_LINK_DEPS "-lc++")
    else()
        list(APPEND KVIDXKIT_LINK_DEPS "-lstdc++")
    endif()
endif()

target_link_libraries(kvidxkit-library ${KVIDXKIT_LINK_DEPS})
target_link_libraries(kvidxkit-static ${KVIDXKIT_LINK_DEPS})

# Propagate compile definitions to consumers of static/shared libraries
if(KVIDXKIT_ENABLE_SQLITE3)
    target_compile_definitions(kvidxkit-static PUBLIC KVIDXKIT_HAS_SQLITE3=1)
    target_compile_definitions(kvidxkit-library PUBLIC KVIDXKIT_HAS_SQLITE3=1)
endif()
if(KVIDXKIT_ENABLE_LMDB)
    target_compile_definitions(kvidxkit-static PUBLIC KVIDXKIT_HAS_LMDB=1)
    target_compile_definitions(kvidxkit-library PUBLIC KVIDXKIT_HAS_LMDB=1)
endif()
if(KVIDXKIT_ENABLE_ROCKSDB)
    target_compile_definitions(kvidxkit-static PUBLIC KVIDXKIT_HAS_ROCKSDB=1)
    target_compile_definitions(kvidxkit-library PUBLIC KVIDXKIT_HAS_ROCKSDB=1)
endif()

# SOVERSION only needs to increment when introducing *breaking* changes.
# Otherwise, just increase VERSION with normal feature additions or maint.
set_target_properties(kvidxkit-library PROPERTIES VERSION ${DK_VERSION} SOVERSION ${DK_ABI_UPDATE})

# ============================================================
# Test Executables - SQLite3 Tests
# ============================================================
if(KVIDXKIT_ENABLE_SQLITE3)
    add_executable(kvidxkit-test kvidxkit-test.c)
    target_link_libraries(kvidxkit-test kvidxkit-static)

    add_executable(kvidxkit-test-comprehensive kvidxkit-test-comprehensive.c)
    target_link_libraries(kvidxkit-test-comprehensive kvidxkit-static)

    add_executable(kvidxkit-test-errors kvidxkit-test-errors.c)
    target_link_libraries(kvidxkit-test-errors kvidxkit-static)

    add_executable(kvidxkit-test-batch kvidxkit-test-batch.c)
    target_link_libraries(kvidxkit-test-batch kvidxkit-static)

    add_executable(kvidxkit-test-iterator kvidxkit-test-iterator.c)
    target_link_libraries(kvidxkit-test-iterator kvidxkit-static)

    add_executable(kvidxkit-test-stats kvidxkit-test-stats.c)
    target_link_libraries(kvidxkit-test-stats kvidxkit-static)

    add_executable(kvidxkit-test-config kvidxkit-test-config.c)
    target_link_libraries(kvidxkit-test-config kvidxkit-static)

    add_executable(kvidxkit-test-range kvidxkit-test-range.c)
    target_link_libraries(kvidxkit-test-range kvidxkit-static)

    add_executable(kvidxkit-test-export kvidxkit-test-export.c)
    target_link_libraries(kvidxkit-test-export kvidxkit-static)

    add_executable(kvidxkit-test-tabledesc kvidxkit-test-tabledesc.c)
    target_link_libraries(kvidxkit-test-tabledesc kvidxkit-static)

    add_executable(kvidxkit-test-primitives kvidxkit-test-primitives.c)
    target_link_libraries(kvidxkit-test-primitives kvidxkit-static)

    # Register SQLite3 tests with CTest
    add_test(NAME kvidxkit-basic-tests COMMAND kvidxkit-test)
    add_test(NAME kvidxkit-comprehensive-tests COMMAND kvidxkit-test-comprehensive)
    add_test(NAME kvidxkit-error-handling-tests COMMAND kvidxkit-test-errors)
    add_test(NAME kvidxkit-batch-operation-tests COMMAND kvidxkit-test-batch)
    add_test(NAME kvidxkit-iterator-tests COMMAND kvidxkit-test-iterator)
    add_test(NAME kvidxkit-statistics-tests COMMAND kvidxkit-test-stats)
    add_test(NAME kvidxkit-configuration-tests COMMAND kvidxkit-test-config)
    add_test(NAME kvidxkit-range-operation-tests COMMAND kvidxkit-test-range)
    add_test(NAME kvidxkit-export-import-tests COMMAND kvidxkit-test-export)
    add_test(NAME kvidxkit-tabledesc-tests COMMAND kvidxkit-test-tabledesc)
    add_test(NAME kvidxkit-primitives-tests COMMAND kvidxkit-test-primitives)

    if(APPLE)
        add_custom_command(TARGET kvidxkit-test POST_BUILD COMMAND dsymutil kvidxkit-test COMMENT "Generating OS X Debug Info")
        add_custom_command(TARGET kvidxkit-test-comprehensive POST_BUILD COMMAND dsymutil kvidxkit-test-comprehensive COMMENT "Generating OS X Debug Info")
        add_custom_command(TARGET kvidxkit-test-errors POST_BUILD COMMAND dsymutil kvidxkit-test-errors COMMENT "Generating OS X Debug Info")
        add_custom_command(TARGET kvidxkit-test-batch POST_BUILD COMMAND dsymutil kvidxkit-test-batch COMMENT "Generating OS X Debug Info")
        add_custom_command(TARGET kvidxkit-test-iterator POST_BUILD COMMAND dsymutil kvidxkit-test-iterator COMMENT "Generating OS X Debug Info")
        add_custom_command(TARGET kvidxkit-test-stats POST_BUILD COMMAND dsymutil kvidxkit-test-stats COMMENT "Generating OS X Debug Info")
        add_custom_command(TARGET kvidxkit-test-config POST_BUILD COMMAND dsymutil kvidxkit-test-config COMMENT "Generating OS X Debug Info")
        add_custom_command(TARGET kvidxkit-test-range POST_BUILD COMMAND dsymutil kvidxkit-test-range COMMENT "Generating OS X Debug Info")
        add_custom_command(TARGET kvidxkit-test-export POST_BUILD COMMAND dsymutil kvidxkit-test-export COMMENT "Generating OS X Debug Info")
        add_custom_command(TARGET kvidxkit-test-tabledesc POST_BUILD COMMAND dsymutil kvidxkit-test-tabledesc COMMENT "Generating OS X Debug Info")
        add_custom_command(TARGET kvidxkit-test-primitives POST_BUILD COMMAND dsymutil kvidxkit-test-primitives COMMENT "Generating OS X Debug Info")
    endif()
endif()

# ============================================================
# Test Executables - LMDB Tests
# ============================================================
if(KVIDXKIT_ENABLE_LMDB)
    add_executable(kvidxkit-test-lmdb kvidxkit-test-lmdb.c)
    target_link_libraries(kvidxkit-test-lmdb kvidxkit-static)
    add_test(NAME kvidxkit-lmdb-adapter-tests COMMAND kvidxkit-test-lmdb)

    if(APPLE)
        add_custom_command(TARGET kvidxkit-test-lmdb POST_BUILD COMMAND dsymutil kvidxkit-test-lmdb COMMENT "Generating OS X Debug Info")
    endif()
endif()

# ============================================================
# Test Executables - RocksDB Tests
# ============================================================
if(KVIDXKIT_ENABLE_ROCKSDB)
    add_executable(kvidxkit-test-rocksdb kvidxkit-test-rocksdb.c)
    target_link_libraries(kvidxkit-test-rocksdb kvidxkit-static)
    add_test(NAME kvidxkit-rocksdb-adapter-tests COMMAND kvidxkit-test-rocksdb)

    if(APPLE)
        add_custom_command(TARGET kvidxkit-test-rocksdb POST_BUILD COMMAND dsymutil kvidxkit-test-rocksdb COMMENT "Generating OS X Debug Info")
    endif()
endif()

# ============================================================
# Fuzzer and Benchmark (always built - use registry API)
# ============================================================
add_executable(kvidxkit-fuzz kvidxkit-fuzz.c)
target_link_libraries(kvidxkit-fuzz kvidxkit-static)

add_executable(kvidxkit-bench kvidxkit-bench.c)
target_link_libraries(kvidxkit-bench kvidxkit-static)

add_test(NAME kvidxkit-fuzzer-tests COMMAND kvidxkit-fuzz 12345)
set_tests_properties(kvidxkit-fuzzer-tests PROPERTIES TIMEOUT 600)

if(APPLE)
    add_custom_command(TARGET kvidxkit-fuzz POST_BUILD COMMAND dsymutil kvidxkit-fuzz COMMENT "Generating OS X Debug Info")
    add_custom_command(TARGET kvidxkit-bench POST_BUILD COMMAND dsymutil kvidxkit-bench COMMENT "Generating OS X Debug Info")
endif()

# vi:ai et sw=4 ts=4:
