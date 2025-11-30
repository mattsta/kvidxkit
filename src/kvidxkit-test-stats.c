/**
 * Comprehensive statistics tests for kvidxkit
 * Tests statistics gathering, accuracy, and performance
 */

#include "ctest.h"
#include "kvidxkit.h"

#include <string.h>
#include <unistd.h>
/* cppcheck-suppress constParameterPointer */
static void makeTestFilename(char *buf, size_t bufSize, const char *testName) {
    snprintf(buf, bufSize, "test-stats-%s-%d.sqlite3", testName, getpid());
}

static void cleanupTestFile(const char *filename) {
    unlink(filename);
}

static bool openFresh(kvidxInstance *i, const char *filename) {
    memset(i, 0, sizeof(*i));
    i->interface = kvidxInterfaceSqlite3;
    return kvidxOpen(i, filename, NULL);
}

/* ====================================================================
 * TEST SUITE 1: Empty Database Statistics
 * ==================================================================== */
static void testEmptyDatabaseStats(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "empty");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for empty stats tests");
        return;
    }

    TEST("Empty Database: Get key count") {
        uint64_t count = 999;
        kvidxError result = kvidxGetKeyCount(i, &count);

        if (result != KVIDX_OK) {
            ERR("GetKeyCount failed with error %d", result);
        }

        if (count != 0) {
            ERR("Empty database should have 0 keys, got %" PRIu64, count);
        }
    }

    TEST("Empty Database: Get min key") {
        uint64_t key = 999;
        kvidxError result = kvidxGetMinKey(i, &key);

        if (result != KVIDX_ERROR_NOT_FOUND) {
            ERR("GetMinKey on empty database should return NOT_FOUND, got %d",
                result);
        }
    }

    TEST("Empty Database: Get data size") {
        uint64_t bytes = 999;
        kvidxError result = kvidxGetDataSize(i, &bytes);

        if (result != KVIDX_OK) {
            ERR("GetDataSize failed with error %d", result);
        }

        if (bytes != 0) {
            ERR("Empty database should have 0 bytes, got %" PRIu64, bytes);
        }
    }

    TEST("Empty Database: Get full stats") {
        kvidxStats stats;
        kvidxError result = kvidxGetStats(i, &stats);

        if (result != KVIDX_OK) {
            ERR("GetStats failed with error %d", result);
        }

        if (stats.totalKeys != 0) {
            ERR("Expected 0 keys, got %" PRIu64, stats.totalKeys);
        }

        if (stats.totalDataBytes != 0) {
            ERR("Expected 0 data bytes, got %" PRIu64, stats.totalDataBytes);
        }

        if (stats.pageSize == 0) {
            ERRR("Page size should not be 0");
        }

        if (stats.pageCount == 0) {
            ERRR("Page count should not be 0 (at least 1 for header)");
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 2: Statistics After Inserts
 * ==================================================================== */
static void testStatsAfterInserts(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "inserts");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for insert stats tests");
        return;
    }

    TEST("After Inserts: Insert 1000 entries") {
        for (uint64_t j = 1; j <= 1000; j++) {
            if (!kvidxInsert(i, j, 1, 1, "testdata", 8)) {
                ERR("Failed to insert key %" PRIu64, j);
                break;
            }
        }

        uint64_t count;
        kvidxError result = kvidxGetKeyCount(i, &count);

        if (result != KVIDX_OK) {
            ERR("GetKeyCount failed with error %d", result);
        }

        if (count != 1000) {
            ERR("Expected 1000 keys, got %" PRIu64, count);
        }
    }

    TEST("After Inserts: Verify min key") {
        uint64_t key;
        kvidxError result = kvidxGetMinKey(i, &key);

        if (result != KVIDX_OK) {
            ERR("GetMinKey failed with error %d", result);
        }

        if (key != 1) {
            ERR("Expected min key 1, got %" PRIu64, key);
        }
    }

    TEST("After Inserts: Verify max key") {
        kvidxStats stats;
        kvidxError result = kvidxGetStats(i, &stats);

        if (result != KVIDX_OK) {
            ERR("GetStats failed with error %d", result);
        }

        if (stats.maxKey != 1000) {
            ERR("Expected max key 1000, got %" PRIu64, stats.maxKey);
        }
    }

    TEST("After Inserts: Verify data size") {
        uint64_t bytes;
        kvidxError result = kvidxGetDataSize(i, &bytes);

        if (result != KVIDX_OK) {
            ERR("GetDataSize failed with error %d", result);
        }

        /* 1000 entries * 8 bytes each = 8000 bytes */
        if (bytes != 8000) {
            ERR("Expected 8000 data bytes, got %" PRIu64, bytes);
        }
    }

    TEST("After Inserts: Full stats consistency") {
        kvidxStats stats;
        kvidxError result = kvidxGetStats(i, &stats);

        if (result != KVIDX_OK) {
            ERR("GetStats failed with error %d", result);
        }

        if (stats.totalKeys != 1000) {
            ERR("Expected 1000 keys, got %" PRIu64, stats.totalKeys);
        }

        if (stats.minKey != 1) {
            ERR("Expected min key 1, got %" PRIu64, stats.minKey);
        }

        if (stats.maxKey != 1000) {
            ERR("Expected max key 1000, got %" PRIu64, stats.maxKey);
        }

        if (stats.totalDataBytes != 8000) {
            ERR("Expected 8000 data bytes, got %" PRIu64, stats.totalDataBytes);
        }

        if (stats.databaseFileSize == 0) {
            ERRR("Database file size should not be 0");
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 3: Statistics After Deletions
 * ==================================================================== */
static void testStatsAfterDeletions(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "deletions");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for deletion stats tests");
        return;
    }

    /* Insert 100 entries */
    for (uint64_t j = 1; j <= 100; j++) {
        if (!kvidxInsert(i, j, 1, 1, "data", 4)) {
            ERR("Failed to insert key %" PRIu64, j);
            break;
        }
    }

    TEST("After Deletions: Delete first 50 entries") {
        for (uint64_t j = 1; j <= 50; j++) {
            if (!kvidxRemove(i, j)) {
                ERR("Failed to remove key %" PRIu64, j);
                break;
            }
        }

        uint64_t count;
        kvidxError result = kvidxGetKeyCount(i, &count);

        if (result != KVIDX_OK) {
            ERR("GetKeyCount failed with error %d", result);
        }

        if (count != 50) {
            ERR("Expected 50 keys after deletion, got %" PRIu64, count);
        }
    }

    TEST("After Deletions: Verify new min key") {
        uint64_t key;
        kvidxError result = kvidxGetMinKey(i, &key);

        if (result != KVIDX_OK) {
            ERR("GetMinKey failed with error %d", result);
        }

        if (key != 51) {
            ERR("Expected min key 51 after deletions, got %" PRIu64, key);
        }
    }

    TEST("After Deletions: Delete all remaining entries") {
        for (uint64_t j = 51; j <= 100; j++) {
            if (!kvidxRemove(i, j)) {
                ERR("Failed to remove key %" PRIu64, j);
                break;
            }
        }

        uint64_t count;
        kvidxError result = kvidxGetKeyCount(i, &count);

        if (result != KVIDX_OK) {
            ERR("GetKeyCount failed with error %d", result);
        }

        if (count != 0) {
            ERR("Expected 0 keys after all deletions, got %" PRIu64, count);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 4: Statistics with Variable Data Sizes
 * ==================================================================== */
static void testStatsVariableDataSizes(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "variable");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for variable size tests");
        return;
    }

    TEST("Variable Sizes: Insert entries with different data sizes") {
        /* Key 1: 10 bytes */
        if (!kvidxInsert(i, 1, 1, 1, "0123456789", 10)) {
            ERRR("Failed to insert key 1");
        }

        /* Key 2: 100 bytes */
        char data100[100];
        memset(data100, 'X', 100);
        if (!kvidxInsert(i, 2, 1, 1, data100, 100)) {
            ERRR("Failed to insert key 2");
        }

        /* Key 3: 0 bytes (empty) */
        if (!kvidxInsert(i, 3, 1, 1, "", 0)) {
            ERRR("Failed to insert key 3");
        }

        /* Total: 10 + 100 + 0 = 110 bytes */
        uint64_t bytes;
        kvidxError result = kvidxGetDataSize(i, &bytes);

        if (result != KVIDX_OK) {
            ERR("GetDataSize failed with error %d", result);
        }

        if (bytes != 110) {
            ERR("Expected 110 data bytes, got %" PRIu64, bytes);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 5: Error Handling
 * ==================================================================== */
static void testStatsErrorHandling(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "errors");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for error handling tests");
        return;
    }

    TEST("Error Handling: NULL parameters") {
        kvidxError result;

        result = kvidxGetKeyCount(NULL, NULL);
        if (result != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("NULL instance should return INVALID_ARGUMENT, got %d", result);
        }

        result = kvidxGetStats(i, NULL);
        if (result != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("NULL stats should return INVALID_ARGUMENT, got %d", result);
        }

        result = kvidxGetMinKey(i, NULL);
        if (result != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("NULL key should return INVALID_ARGUMENT, got %d", result);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * MAIN TEST RUNNER
 * ==================================================================== */
int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;
    uint32_t err = 0;

    printf("=======================================================\n");
    printf("STATISTICS TESTS FOR KVIDXKIT\n");
    printf("=======================================================\n\n");

    printf("Running Suite 1: Empty Database Statistics\n");
    printf("-------------------------------------------------------\n");
    testEmptyDatabaseStats(&err);
    printf("\n");

    printf("Running Suite 2: Statistics After Inserts\n");
    printf("-------------------------------------------------------\n");
    testStatsAfterInserts(&err);
    printf("\n");

    printf("Running Suite 3: Statistics After Deletions\n");
    printf("-------------------------------------------------------\n");
    testStatsAfterDeletions(&err);
    printf("\n");

    printf("Running Suite 4: Statistics with Variable Data Sizes\n");
    printf("-------------------------------------------------------\n");
    testStatsVariableDataSizes(&err);
    printf("\n");

    printf("Running Suite 5: Error Handling\n");
    printf("-------------------------------------------------------\n");
    testStatsErrorHandling(&err);
    printf("\n");

    printf("=======================================================\n");
    if (err == 0) {
        printf("ALL STATISTICS TESTS PASSED!\n");
        printf("=======================================================\n");
        return 0;
    } else {
        printf("FAILED: %u test(s) failed\n", err);
        printf("=======================================================\n");
        return 1;
    }
}
