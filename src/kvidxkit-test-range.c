/**
 * Comprehensive range operation tests for kvidxkit
 * Tests removeRange, countRange, and existsInRange functionality
 */

#include "ctest.h"
#include "kvidxkit.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
/* cppcheck-suppress constParameterPointer */
static void makeTestFilename(char *buf, size_t bufSize, const char *testName) {
    snprintf(buf, bufSize, "test-range-%s-%d.sqlite3", testName, getpid());
}

static void cleanupTestFile(const char *filename) {
    unlink(filename);
}

/* Helper to populate database with test data */
static bool populateRange(kvidxInstance *i, uint64_t start, uint64_t end) {
    for (uint64_t key = start; key <= end; key++) {
        char data[32];
        snprintf(data, sizeof(data), "data-%" PRIu64, key);
        if (!kvidxInsert(i, key, 1, 1, data, strlen(data))) {
            return false;
        }
    }
    return true;
}

/* ====================================================================
 * TEST SUITE 1: Remove Range - Inclusive/Exclusive Boundaries
 * ==================================================================== */
static void testRemoveRangeInclusivity(uint32_t *err) {
    char filename[128];

    TEST("RemoveRange: Both inclusive [10, 20]") {
        makeTestFilename(filename, sizeof(filename), "remove-both-inc");
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, filename, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Populate keys 1-30 */
            if (!populateRange(i, 1, 30)) {
                ERRR("Failed to populate range");
            }

            /* Remove [10, 20] inclusive */
            uint64_t deleted = 0;
            kvidxError e = kvidxRemoveRange(i, 10, 20, true, true, &deleted);
            if (e != KVIDX_OK) {
                ERR("RemoveRange failed: %d", e);
            }

            if (deleted != 11) { /* 10, 11, 12, ..., 20 = 11 keys */
                ERR("Expected 11 deletions, got %" PRIu64, deleted);
            }

            /* Verify 9 and 21 still exist */
            if (!kvidxExists(i, 9)) {
                ERRR("Key 9 should still exist");
            }
            if (!kvidxExists(i, 21)) {
                ERRR("Key 21 should still exist");
            }

            /* Verify 10 and 20 are deleted */
            if (kvidxExists(i, 10)) {
                ERRR("Key 10 should be deleted");
            }
            if (kvidxExists(i, 20)) {
                ERRR("Key 20 should be deleted");
            }

            kvidxClose(i);
        }
        cleanupTestFile(filename);
    }

    TEST("RemoveRange: Start exclusive (10, 20]") {
        makeTestFilename(filename, sizeof(filename), "remove-start-exc");
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, filename, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            if (!populateRange(i, 1, 30)) {
                ERRR("Failed to populate range");
            }

            /* Remove (10, 20] - exclude 10, include 20 */
            uint64_t deleted = 0;
            kvidxError e = kvidxRemoveRange(i, 10, 20, false, true, &deleted);
            if (e != KVIDX_OK) {
                ERR("RemoveRange failed: %d", e);
            }

            if (deleted != 10) { /* 11, 12, ..., 20 = 10 keys */
                ERR("Expected 10 deletions, got %" PRIu64, deleted);
            }

            /* Verify 10 still exists */
            if (!kvidxExists(i, 10)) {
                ERRR("Key 10 should still exist");
            }

            /* Verify 20 is deleted */
            if (kvidxExists(i, 20)) {
                ERRR("Key 20 should be deleted");
            }

            kvidxClose(i);
        }
        cleanupTestFile(filename);
    }

    TEST("RemoveRange: End exclusive [10, 20)") {
        makeTestFilename(filename, sizeof(filename), "remove-end-exc");
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, filename, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            if (!populateRange(i, 1, 30)) {
                ERRR("Failed to populate range");
            }

            /* Remove [10, 20) - include 10, exclude 20 */
            uint64_t deleted = 0;
            kvidxError e = kvidxRemoveRange(i, 10, 20, true, false, &deleted);
            if (e != KVIDX_OK) {
                ERR("RemoveRange failed: %d", e);
            }

            if (deleted != 10) { /* 10, 11, ..., 19 = 10 keys */
                ERR("Expected 10 deletions, got %" PRIu64, deleted);
            }

            /* Verify 10 is deleted */
            if (kvidxExists(i, 10)) {
                ERRR("Key 10 should be deleted");
            }

            /* Verify 20 still exists */
            if (!kvidxExists(i, 20)) {
                ERRR("Key 20 should still exist");
            }

            kvidxClose(i);
        }
        cleanupTestFile(filename);
    }

    TEST("RemoveRange: Both exclusive (10, 20)") {
        makeTestFilename(filename, sizeof(filename), "remove-both-exc");
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, filename, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            if (!populateRange(i, 1, 30)) {
                ERRR("Failed to populate range");
            }

            /* Remove (10, 20) - exclude both */
            uint64_t deleted = 0;
            kvidxError e = kvidxRemoveRange(i, 10, 20, false, false, &deleted);
            if (e != KVIDX_OK) {
                ERR("RemoveRange failed: %d", e);
            }

            if (deleted != 9) { /* 11, 12, ..., 19 = 9 keys */
                ERR("Expected 9 deletions, got %" PRIu64, deleted);
            }

            /* Verify 10 and 20 still exist */
            if (!kvidxExists(i, 10)) {
                ERRR("Key 10 should still exist");
            }
            if (!kvidxExists(i, 20)) {
                ERRR("Key 20 should still exist");
            }

            kvidxClose(i);
        }
        cleanupTestFile(filename);
    }
}

/* ====================================================================
 * TEST SUITE 2: Count Range
 * ==================================================================== */
static void testCountRange(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "count-range");

    TEST("CountRange: Count keys in range") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, filename, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Populate keys 5, 10, 15, 20, 25, 30 (sparse) */
            for (uint64_t key = 5; key <= 30; key += 5) {
                if (!kvidxInsert(i, key, 1, 1, "data", 4)) {
                    ERR("Failed to insert key %" PRIu64, key);
                    break;
                }
            }

            /* Count range [1, 30] - should find 6 keys */
            uint64_t count = 0;
            kvidxError e = kvidxCountRange(i, 1, 30, &count);
            if (e != KVIDX_OK) {
                ERR("CountRange failed: %d", e);
            }

            if (count != 6) {
                ERR("Expected 6 keys, got %" PRIu64, count);
            }

            /* Count range [8, 22] - should find 3 keys (10, 15, 20) */
            e = kvidxCountRange(i, 8, 22, &count);
            if (e != KVIDX_OK) {
                ERR("CountRange failed: %d", e);
            }

            if (count != 3) {
                ERR("Expected 3 keys, got %" PRIu64, count);
            }

            /* Count range [100, 200] - should find 0 keys */
            e = kvidxCountRange(i, 100, 200, &count);
            if (e != KVIDX_OK) {
                ERR("CountRange failed: %d", e);
            }

            if (count != 0) {
                ERR("Expected 0 keys, got %" PRIu64, count);
            }

            kvidxClose(i);
        }
    }

    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 3: Exists In Range
 * ==================================================================== */
static void testExistsInRange(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "exists-range");

    TEST("ExistsInRange: Check if keys exist in range") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, filename, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Populate keys 10-20 */
            if (!populateRange(i, 10, 20)) {
                ERRR("Failed to populate range");
            }

            /* Check range [10, 20] - should exist */
            bool exists = false;
            kvidxError e = kvidxExistsInRange(i, 10, 20, &exists);
            if (e != KVIDX_OK) {
                ERR("ExistsInRange failed: %d", e);
            }

            if (!exists) {
                ERRR("Expected keys to exist in [10, 20]");
            }

            /* Check range [15, 18] - should exist */
            e = kvidxExistsInRange(i, 15, 18, &exists);
            if (e != KVIDX_OK) {
                ERR("ExistsInRange failed: %d", e);
            }

            if (!exists) {
                ERRR("Expected keys to exist in [15, 18]");
            }

            /* Check range [100, 200] - should not exist */
            e = kvidxExistsInRange(i, 100, 200, &exists);
            if (e != KVIDX_OK) {
                ERR("ExistsInRange failed: %d", e);
            }

            if (exists) {
                ERRR("Expected no keys in [100, 200]");
            }

            /* Check range [1, 5] - should not exist */
            e = kvidxExistsInRange(i, 1, 5, &exists);
            if (e != KVIDX_OK) {
                ERR("ExistsInRange failed: %d", e);
            }

            if (exists) {
                ERRR("Expected no keys in [1, 5]");
            }

            kvidxClose(i);
        }
    }

    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 4: Edge Cases
 * ==================================================================== */
static void testRangeEdgeCases(uint32_t *err) {
    char filename[128];

    TEST("Range Edge Cases: Empty range") {
        makeTestFilename(filename, sizeof(filename), "edge-empty");
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, filename, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            if (!populateRange(i, 1, 10)) {
                ERRR("Failed to populate range");
            }

            /* Empty range: start > end with both inclusive */
            uint64_t deleted = 0;
            kvidxError e = kvidxRemoveRange(i, 20, 10, true, true, &deleted);
            if (e != KVIDX_OK) {
                ERR("RemoveRange failed: %d", e);
            }

            if (deleted != 0) {
                ERR("Expected 0 deletions for empty range, got %" PRIu64,
                    deleted);
            }

            /* Verify all keys still exist */
            for (uint64_t key = 1; key <= 10; key++) {
                if (!kvidxExists(i, key)) {
                    ERR("Key %" PRIu64 " should still exist", key);
                    break;
                }
            }

            kvidxClose(i);
        }
        cleanupTestFile(filename);
    }

    TEST("Range Edge Cases: Single element range") {
        makeTestFilename(filename, sizeof(filename), "edge-single");
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, filename, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            if (!populateRange(i, 1, 10)) {
                ERRR("Failed to populate range");
            }

            /* Single element range: [5, 5] with both inclusive */
            uint64_t deleted = 0;
            kvidxError e = kvidxRemoveRange(i, 5, 5, true, true, &deleted);
            if (e != KVIDX_OK) {
                ERR("RemoveRange failed: %d", e);
            }

            if (deleted != 1) {
                ERR("Expected 1 deletion, got %" PRIu64, deleted);
            }

            /* Verify key 5 is deleted */
            if (kvidxExists(i, 5)) {
                ERRR("Key 5 should be deleted");
            }

            /* Verify adjacent keys still exist */
            if (!kvidxExists(i, 4) || !kvidxExists(i, 6)) {
                ERRR("Adjacent keys should still exist");
            }

            kvidxClose(i);
        }
        cleanupTestFile(filename);
    }

    TEST("Range Edge Cases: Empty database") {
        makeTestFilename(filename, sizeof(filename), "edge-empty-db");
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, filename, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Try operations on empty database */
            uint64_t deleted = 0;
            kvidxError e = kvidxRemoveRange(i, 1, 100, true, true, &deleted);
            if (e != KVIDX_OK) {
                ERR("RemoveRange on empty DB failed: %d", e);
            }

            if (deleted != 0) {
                ERR("Expected 0 deletions on empty DB, got %" PRIu64, deleted);
            }

            uint64_t count = 0;
            e = kvidxCountRange(i, 1, 100, &count);
            if (e != KVIDX_OK) {
                ERR("CountRange on empty DB failed: %d", e);
            }

            if (count != 0) {
                ERR("Expected 0 count on empty DB, got %" PRIu64, count);
            }

            bool exists = true;
            e = kvidxExistsInRange(i, 1, 100, &exists);
            if (e != KVIDX_OK) {
                ERR("ExistsInRange on empty DB failed: %d", e);
            }

            if (exists) {
                ERRR("Expected no keys in empty DB");
            }

            kvidxClose(i);
        }
        cleanupTestFile(filename);
    }
}

/* ====================================================================
 * TEST SUITE 5: Large Range Operations
 * ==================================================================== */
static void testLargeRanges(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "large-range");

    TEST("Large Range: Remove 1000 keys") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, filename, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Populate 1000 keys */
            if (!populateRange(i, 1, 1000)) {
                ERRR("Failed to populate large range");
            }

            /* Count before deletion */
            uint64_t countBefore = 0;
            kvidxError e = kvidxCountRange(i, 1, 1000, &countBefore);
            if (e != KVIDX_OK) {
                ERR("CountRange failed: %d", e);
            }

            if (countBefore != 1000) {
                ERR("Expected 1000 keys before deletion, got %" PRIu64,
                    countBefore);
            }

            /* Remove [100, 900] inclusive */
            uint64_t deleted = 0;
            e = kvidxRemoveRange(i, 100, 900, true, true, &deleted);
            if (e != KVIDX_OK) {
                ERR("RemoveRange failed: %d", e);
            }

            if (deleted != 801) { /* 100 to 900 inclusive = 801 keys */
                ERR("Expected 801 deletions, got %" PRIu64, deleted);
            }

            /* Count after deletion */
            uint64_t countAfter = 0;
            e = kvidxCountRange(i, 1, 1000, &countAfter);
            if (e != KVIDX_OK) {
                ERR("CountRange failed: %d", e);
            }

            if (countAfter != 199) { /* 1-99 + 901-1000 = 199 keys */
                ERR("Expected 199 keys after deletion, got %" PRIu64,
                    countAfter);
            }

            /* Verify boundaries */
            if (!kvidxExists(i, 99)) {
                ERRR("Key 99 should still exist");
            }
            if (kvidxExists(i, 100)) {
                ERRR("Key 100 should be deleted");
            }
            if (kvidxExists(i, 900)) {
                ERRR("Key 900 should be deleted");
            }
            if (!kvidxExists(i, 901)) {
                ERRR("Key 901 should still exist");
            }

            kvidxClose(i);
        }
    }

    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 6: Error Handling
 * ==================================================================== */
static void testRangeErrorHandling(uint32_t *err) {
    TEST("Range Errors: NULL parameters") {
        kvidxError e;
        uint64_t count = 0;
        bool exists = false;
        uint64_t deleted = 0;

        /* NULL instance */
        e = kvidxRemoveRange(NULL, 1, 10, true, true, &deleted);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL instance, got %d", e);
        }

        e = kvidxCountRange(NULL, 1, 10, &count);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL instance, got %d", e);
        }

        e = kvidxExistsInRange(NULL, 1, 10, &exists);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL instance, got %d", e);
        }

        /* NULL output parameters */
        kvidxInstance inst = {0};
        inst.interface = kvidxInterfaceSqlite3;

        e = kvidxCountRange(&inst, 1, 10, NULL);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL count, got %d", e);
        }

        e = kvidxExistsInRange(&inst, 1, 10, NULL);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL exists, got %d", e);
        }

        /* NULL deletedCount is allowed (optional parameter) */
        const char *errMsg = NULL;
        char filename[128];
        makeTestFilename(filename, sizeof(filename), "error-null");

        if (kvidxOpen(&inst, filename, &errMsg)) {
            e = kvidxRemoveRange(&inst, 1, 10, true, true, NULL);
            if (e != KVIDX_OK) {
                ERR("RemoveRange should succeed with NULL deletedCount, got %d",
                    e);
            }
            kvidxClose(&inst);
        }

        cleanupTestFile(filename);
    }
}

/* ====================================================================
 * MAIN TEST RUNNER
 * ==================================================================== */
int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;
    uint32_t err = 0;

    printf("=======================================================\n");
    printf("RANGE OPERATION TESTS FOR KVIDXKIT\n");
    printf("=======================================================\n\n");

    printf("Running Suite 1: Remove Range Inclusivity\n");
    printf("-------------------------------------------------------\n");
    testRemoveRangeInclusivity(&err);
    printf("\n");

    printf("Running Suite 2: Count Range\n");
    printf("-------------------------------------------------------\n");
    testCountRange(&err);
    printf("\n");

    printf("Running Suite 3: Exists In Range\n");
    printf("-------------------------------------------------------\n");
    testExistsInRange(&err);
    printf("\n");

    printf("Running Suite 4: Edge Cases\n");
    printf("-------------------------------------------------------\n");
    testRangeEdgeCases(&err);
    printf("\n");

    printf("Running Suite 5: Large Ranges\n");
    printf("-------------------------------------------------------\n");
    testLargeRanges(&err);
    printf("\n");

    printf("Running Suite 6: Error Handling\n");
    printf("-------------------------------------------------------\n");
    testRangeErrorHandling(&err);
    printf("\n");

    printf("=======================================================\n");
    if (err == 0) {
        printf("ALL RANGE OPERATION TESTS PASSED!\n");
        printf("=======================================================\n");
        return 0;
    } else {
        printf("FAILED: %u test(s) failed\n", err);
        printf("=======================================================\n");
        return 1;
    }
}
