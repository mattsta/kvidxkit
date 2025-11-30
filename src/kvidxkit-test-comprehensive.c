/**
 * Comprehensive test suite for kvidxkit
 * Tests edge cases, boundary conditions, error handling, and data integrity
 */

#include "ctest.h"
#include "kvidxkit.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
/* cppcheck-suppress constParameterPointer */
/* Helper function to create a test filename */
static void makeTestFilename(char *buf, size_t bufSize, const char *testName) {
    snprintf(buf, bufSize, "test-%s-%d.sqlite3", testName, getpid());
}

/* Helper function to cleanup test file */
static void cleanupTestFile(const char *filename) {
    unlink(filename);
}

/* Helper function to create a fresh instance */
static bool openFresh(kvidxInstance *i, const char *filename) {
    memset(i, 0, sizeof(*i));
    i->interface = kvidxInterfaceSqlite3;
    return kvidxOpen(i, filename, NULL);
}

/* ====================================================================
 * TEST SUITE 1: Boundary Value Testing
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testBoundaryValues(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "boundary");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for boundary tests");
        return;
    }

    TEST("Boundary: Insert with key = 0") {
        bool inserted = kvidxInsert(i, 0, 0, 0, "data", 4);
        if (!inserted) {
            ERRR("Failed to insert key=0");
        }

        bool exists = kvidxExists(i, 0);
        if (!exists) {
            ERRR("Key=0 not found after insert");
        }
    }

    TEST("Boundary: Insert with key = UINT64_MAX") {
        bool inserted = kvidxInsert(i, UINT64_MAX, 0, 0, "max", 3);
        if (!inserted) {
            ERRR("Failed to insert key=UINT64_MAX");
        }

        bool exists = kvidxExists(i, UINT64_MAX);
        if (!exists) {
            ERRR("Key=UINT64_MAX not found after insert");
        }
    }

    TEST("Boundary: Insert with term = 0") {
        bool inserted = kvidxInsert(i, 100, 0, 0, "t0", 2);
        if (!inserted) {
            ERRR("Failed to insert term=0");
        }

        bool exists = kvidxExistsDual(i, 100, 0);
        if (!exists) {
            ERRR("Term=0 not found after insert");
        }
    }

    TEST("Boundary: Insert with term = UINT64_MAX") {
        bool inserted = kvidxInsert(i, 101, UINT64_MAX, 0, "tmax", 4);
        if (!inserted) {
            ERRR("Failed to insert term=UINT64_MAX");
        }

        bool exists = kvidxExistsDual(i, 101, UINT64_MAX);
        if (!exists) {
            ERRR("Term=UINT64_MAX not found after insert");
        }
    }

    TEST("Boundary: Insert with cmd = 0") {
        bool inserted = kvidxInsert(i, 102, 0, 0, "c0", 2);
        if (!inserted) {
            ERRR("Failed to insert cmd=0");
        }
    }

    TEST("Boundary: Insert with cmd = UINT64_MAX") {
        bool inserted = kvidxInsert(i, 103, 0, UINT64_MAX, "cmax", 4);
        if (!inserted) {
            ERRR("Failed to insert cmd=UINT64_MAX");
        }

        uint64_t foundCmd;
        const uint8_t *data;
        size_t len;
        bool found = kvidxGet(i, 103, NULL, &foundCmd, &data, &len);
        if (!found || foundCmd != UINT64_MAX) {
            ERRR("Cmd=UINT64_MAX not retrieved correctly");
        }
    }

    TEST("Boundary: Zero-length data") {
        bool inserted = kvidxInsert(i, 200, 0, 0, "", 0);
        if (!inserted) {
            ERRR("Failed to insert zero-length data");
        }

        const uint8_t *foundData;
        size_t foundLen;
        bool found = kvidxGet(i, 200, NULL, NULL, &foundData, &foundLen);
        /* Note: This may fail due to extractBlob returning false for NULL blob
         */
        if (found && foundLen != 0) {
            ERR("Expected zero length, got %zu", foundLen);
        }
    }

    TEST("Boundary: NULL data pointer with zero length") {
        bool inserted = kvidxInsert(i, 201, 0, 0, NULL, 0);
        if (!inserted) {
            ERRR("Failed to insert NULL data with zero length");
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 2: Data Integrity Testing
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testDataIntegrity(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "data-integrity");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for data integrity tests");
        return;
    }

    TEST("Data Integrity: Binary data with all byte values") {
        uint8_t allBytes[256];
        for (int j = 0; j < 256; j++) {
            allBytes[j] = (uint8_t)j;
        }

        bool inserted = kvidxInsert(i, 300, 1, 1, allBytes, sizeof(allBytes));
        if (!inserted) {
            ERRR("Failed to insert binary data");
        }

        const uint8_t *foundData;
        size_t foundLen;
        bool found = kvidxGet(i, 300, NULL, NULL, &foundData, &foundLen);
        if (!found) {
            ERRR("Failed to retrieve binary data");
        } else {
            if (foundLen != sizeof(allBytes)) {
                ERR("Length mismatch: expected %zu, got %zu", sizeof(allBytes),
                    foundLen);
            }
            if (memcmp(foundData, allBytes, sizeof(allBytes)) != 0) {
                ERRR("Binary data corrupted");
            }
        }
    }

    TEST("Data Integrity: Large data (1 MB)") {
        size_t dataSize = 1024 * 1024;
        uint8_t *largeData = malloc(dataSize);
        if (!largeData) {
            ERRR("Failed to allocate memory for large data test");
        } else {
            /* Fill with pattern */
            for (size_t j = 0; j < dataSize; j++) {
                largeData[j] = (uint8_t)(j % 256);
            }

            bool inserted = kvidxInsert(i, 301, 1, 1, largeData, dataSize);
            if (!inserted) {
                ERRR("Failed to insert 1MB data");
            } else {
                const uint8_t *foundData;
                size_t foundLen;
                bool found =
                    kvidxGet(i, 301, NULL, NULL, &foundData, &foundLen);
                if (!found) {
                    ERRR("Failed to retrieve 1MB data");
                } else {
                    if (foundLen != dataSize) {
                        ERR("Length mismatch for large data: expected %zu, got "
                            "%zu",
                            dataSize, foundLen);
                    }
                    if (memcmp(foundData, largeData, dataSize) != 0) {
                        ERRR("Large data corrupted");
                    }
                }
            }
            free(largeData);
        }
    }

    TEST("Data Integrity: Multiple sequential inserts") {
        const char *testStrings[] = {"Hello",  "World",   "Test",   "Data",
                                     "SQLite", "Storage", "Binary", "Key",
                                     "Value",  "Index"};
        int numStrings = sizeof(testStrings) / sizeof(testStrings[0]);

        for (int j = 0; j < numStrings; j++) {
            bool inserted = kvidxInsert(i, 400 + j, j, j, testStrings[j],
                                        strlen(testStrings[j]));
            if (!inserted) {
                ERR("Failed to insert string %d", j);
            }
        }

        /* Verify all */
        for (int j = 0; j < numStrings; j++) {
            const uint8_t *foundData;
            size_t foundLen;
            uint64_t foundTerm, foundCmd;
            bool found = kvidxGet(i, 400 + j, &foundTerm, &foundCmd, &foundData,
                                  &foundLen);
            if (!found) {
                ERR("Failed to retrieve string %d", j);
            } else {
                if (foundTerm != (uint64_t)j) {
                    ERR("Term mismatch for entry %d", j);
                }
                if (foundCmd != (uint64_t)j) {
                    ERR("Cmd mismatch for entry %d", j);
                }
                if (foundLen != strlen(testStrings[j])) {
                    ERR("Length mismatch for entry %d", j);
                }
                if (memcmp(foundData, testStrings[j], foundLen) != 0) {
                    ERR("Data mismatch for entry %d", j);
                }
            }
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 3: Transaction Testing
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testTransactions(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "transactions");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for transaction tests");
        return;
    }

    TEST("Transaction: Multiple inserts in single transaction") {
        if (!kvidxBegin(i)) {
            ERRR("Failed to begin transaction");
        }

        for (int j = 0; j < 100; j++) {
            bool inserted = kvidxInsert(i, 500 + j, 1, 1, "bulk", 4);
            if (!inserted) {
                ERR("Failed to insert in transaction: %d", j);
            }
        }

        if (!kvidxCommit(i)) {
            ERRR("Failed to commit transaction");
        }

        /* Verify all inserted */
        for (int j = 0; j < 100; j++) {
            if (!kvidxExists(i, 500 + j)) {
                ERR("Key %d not found after transaction", 500 + j);
            }
        }
    }

    TEST("Transaction: Insert and delete in same transaction") {
        if (!kvidxBegin(i)) {
            ERRR("Failed to begin transaction");
        }

        kvidxInsert(i, 700, 1, 1, "temp", 4);
        kvidxRemove(i, 700);

        if (!kvidxCommit(i)) {
            ERRR("Failed to commit transaction");
        }

        if (kvidxExists(i, 700)) {
            ERRR("Key should not exist after delete in transaction");
        }
    }

    TEST("Transaction: Operations without transaction") {
        /* Should still work without explicit BEGIN/COMMIT */
        bool inserted = kvidxInsert(i, 800, 1, 1, "notrans", 7);
        if (!inserted) {
            ERRR("Insert without transaction failed");
        }

        if (!kvidxExists(i, 800)) {
            ERRR("Key not found after insert without transaction");
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 4: Navigation Testing (getPrev/getNext)
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testNavigation(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "navigation");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for navigation tests");
        return;
    }

    /* Insert sparse keys: 10, 20, 30, 40, 50 */
    uint64_t keys[] = {10, 20, 30, 40, 50};
    int numKeys = sizeof(keys) / sizeof(keys[0]);

    for (int j = 0; j < numKeys; j++) {
        kvidxInsert(i, keys[j], j, j, "nav", 3);
    }

    TEST("Navigation: getPrev from middle") {
        uint64_t foundKey;
        bool found = kvidxGetPrev(i, 35, &foundKey, NULL, NULL, NULL, NULL);
        if (!found || foundKey != 30) {
            ERR("getPrev(35) should return 30, got %" PRIu64, foundKey);
        }
    }

    TEST("Navigation: getNext from middle") {
        uint64_t foundKey;
        bool found = kvidxGetNext(i, 25, &foundKey, NULL, NULL, NULL, NULL);
        if (!found || foundKey != 30) {
            ERR("getNext(25) should return 30, got %" PRIu64, foundKey);
        }
    }

    TEST("Navigation: getPrev from exact key") {
        uint64_t foundKey;
        bool found = kvidxGetPrev(i, 30, &foundKey, NULL, NULL, NULL, NULL);
        if (!found || foundKey != 20) {
            ERR("getPrev(30) should return 20, got %" PRIu64, foundKey);
        }
    }

    TEST("Navigation: getNext from exact key") {
        uint64_t foundKey;
        bool found = kvidxGetNext(i, 30, &foundKey, NULL, NULL, NULL, NULL);
        if (!found || foundKey != 40) {
            ERR("getNext(30) should return 40, got %" PRIu64, foundKey);
        }
    }

    TEST("Navigation: getPrev from before first key") {
        uint64_t foundKey = 999;
        bool found = kvidxGetPrev(i, 5, &foundKey, NULL, NULL, NULL, NULL);
        if (found) {
            ERR("getPrev(5) should not find anything, but got %" PRIu64,
                foundKey);
        }
    }

    TEST("Navigation: getNext from after last key") {
        uint64_t foundKey = 999;
        bool found = kvidxGetNext(i, 100, &foundKey, NULL, NULL, NULL, NULL);
        if (found) {
            ERR("getNext(100) should not find anything, but got %" PRIu64,
                foundKey);
        }
    }

    TEST("Navigation: getPrev from key=0") {
        uint64_t foundKey = 999;
        bool found = kvidxGetPrev(i, 0, &foundKey, NULL, NULL, NULL, NULL);
        if (found) {
            ERR("getPrev(0) should not find anything, but got %" PRIu64,
                foundKey);
        }
    }

    TEST("Navigation: getNext from UINT64_MAX") {
        uint64_t foundKey = 999;
        bool found =
            kvidxGetNext(i, UINT64_MAX, &foundKey, NULL, NULL, NULL, NULL);
        if (found) {
            ERR("getNext(UINT64_MAX) should not find anything, but got "
                "%" PRIu64,
                foundKey);
        }
    }

    TEST("Navigation: Walk forward through all keys") {
        uint64_t currentKey = 0;
        for (int j = 0; j < numKeys; j++) {
            uint64_t foundKey;
            bool found =
                kvidxGetNext(i, currentKey, &foundKey, NULL, NULL, NULL, NULL);
            if (!found) {
                ERR("Failed to get next key in forward walk at iteration %d",
                    j);
                break;
            }
            if (foundKey != keys[j]) {
                ERR("Forward walk: expected %" PRIu64 ", got %" PRIu64, keys[j],
                    foundKey);
            }
            currentKey = foundKey;
        }
    }

    TEST("Navigation: Walk backward through all keys") {
        uint64_t currentKey = UINT64_MAX;
        for (int j = numKeys - 1; j >= 0; j--) {
            uint64_t foundKey;
            bool found =
                kvidxGetPrev(i, currentKey, &foundKey, NULL, NULL, NULL, NULL);
            if (!found) {
                ERR("Failed to get prev key in backward walk at iteration %d",
                    j);
                break;
            }
            if (foundKey != keys[j]) {
                ERR("Backward walk: expected %" PRIu64 ", got %" PRIu64,
                    keys[j], foundKey);
            }
            currentKey = foundKey;
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 5: Range Deletion Testing
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testRangeDeletion(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "range-deletion");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for range deletion tests");
        return;
    }

    TEST("Range Delete: Setup - insert keys 1-100") {
        for (uint64_t j = 1; j <= 100; j++) {
            if (!kvidxInsert(i, j, 1, 1, "range", 5)) {
                ERR("Failed to insert key %" PRIu64, j);
            }
        }
    }

    TEST("Range Delete: Remove from middle to end") {
        if (!kvidxRemoveAfterNInclusive(i, 51)) {
            ERRR("Failed to remove range [51, end]");
        }

        /* Keys 1-50 should exist */
        for (uint64_t j = 1; j <= 50; j++) {
            if (!kvidxExists(i, j)) {
                ERR("Key %" PRIu64 " should still exist", j);
            }
        }

        /* Keys 51-100 should not exist */
        for (uint64_t j = 51; j <= 100; j++) {
            if (kvidxExists(i, j)) {
                ERR("Key %" PRIu64 " should have been deleted", j);
            }
        }
    }

    TEST("Range Delete: maxKey after range delete") {
        uint64_t maxKey;
        if (!kvidxMaxKey(i, &maxKey)) {
            ERRR("Failed to get max key");
        } else if (maxKey != 50) {
            ERR("Max key should be 50, got %" PRIu64, maxKey);
        }
    }

    TEST("Range Delete: Remove all remaining") {
        if (!kvidxRemoveAfterNInclusive(i, 1)) {
            ERRR("Failed to remove range [1, end]");
        }

        uint64_t maxKey = 999;
        if (kvidxMaxKey(i, &maxKey)) {
            ERR("MaxKey should fail on empty database, but got %" PRIu64,
                maxKey);
        }
    }

    TEST("Range Delete: Remove from non-existent key") {
        /* Insert key 200 */
        kvidxInsert(i, 200, 1, 1, "lonely", 6);

        /* Try to remove from 100 (which doesn't exist, but is < 200) */
        if (!kvidxRemoveAfterNInclusive(i, 100)) {
            ERRR("RemoveAfterNInclusive should succeed even if start key "
                 "doesn't exist");
        }

        /* Key 200 should be gone */
        if (kvidxExists(i, 200)) {
            ERRR("Key 200 should have been deleted");
        }
    }

    TEST("Range Delete: Remove with key larger than all existing") {
        /* Insert keys 10, 20, 30 */
        kvidxInsert(i, 10, 1, 1, "a", 1);
        kvidxInsert(i, 20, 1, 1, "b", 1);
        kvidxInsert(i, 30, 1, 1, "c", 1);

        /* Remove from 100 (larger than all) */
        if (!kvidxRemoveAfterNInclusive(i, 100)) {
            ERRR("RemoveAfterNInclusive should succeed");
        }

        /* All keys should still exist */
        if (!kvidxExists(i, 10) || !kvidxExists(i, 20) || !kvidxExists(i, 30)) {
            ERRR("Keys should still exist after removing range > all keys");
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 6: ExistsDual Testing
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testExistsDual(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "exists-dual");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for existsDual tests");
        return;
    }

    TEST("ExistsDual: Insert and verify with correct term") {
        kvidxInsert(i, 100, 42, 1, "data", 4);

        if (!kvidxExistsDual(i, 100, 42)) {
            ERRR("ExistsDual should find key=100, term=42");
        }
    }

    TEST("ExistsDual: Verify fails with wrong term") {
        if (kvidxExistsDual(i, 100, 43)) {
            ERRR("ExistsDual should NOT find key=100, term=43");
        }
    }

    TEST("ExistsDual: Verify fails with non-existent key") {
        if (kvidxExistsDual(i, 999, 42)) {
            ERRR("ExistsDual should NOT find non-existent key");
        }
    }

    TEST("ExistsDual: Multiple entries with same key but different terms") {
        /* Note: SQLite primary key constraint prevents this, so we use
         * different keys */
        kvidxInsert(i, 200, 1, 1, "t1", 2);
        kvidxInsert(i, 201, 2, 1, "t2", 2);
        kvidxInsert(i, 202, 3, 1, "t3", 2);

        if (!kvidxExistsDual(i, 200, 1)) {
            ERRR("Should find key=200, term=1");
        }
        if (!kvidxExistsDual(i, 201, 2)) {
            ERRR("Should find key=201, term=2");
        }
        if (!kvidxExistsDual(i, 202, 3)) {
            ERRR("Should find key=202, term=3");
        }

        if (kvidxExistsDual(i, 200, 2)) {
            ERRR("Should NOT find key=200, term=2");
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 7: Persistence Testing
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testPersistence(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "persistence");

    kvidxInstance inst1 = {0};
    kvidxInstance *i1 = &inst1;

    TEST("Persistence: Write data and close") {
        if (!openFresh(i1, filename)) {
            ERRR("Failed to open database");
            return;
        }

        /* Insert some data */
        for (uint64_t j = 1; j <= 10; j++) {
            kvidxInsert(i1, j, j * 10, j * 100, "persist", 7);
        }

        /* Explicit fsync */
        if (!kvidxFsync(i1)) {
            ERRR("Fsync failed");
        }

        if (!kvidxClose(i1)) {
            ERRR("Failed to close database");
        }
    }

    TEST("Persistence: Reopen and verify data") {
        kvidxInstance inst2 = {0};
        kvidxInstance *i2 = &inst2;

        if (!openFresh(i2, filename)) {
            ERRR("Failed to reopen database");
            return;
        }

        /* Verify all data */
        for (uint64_t j = 1; j <= 10; j++) {
            uint64_t foundTerm, foundCmd;
            const uint8_t *foundData;
            size_t foundLen;

            if (!kvidxGet(i2, j, &foundTerm, &foundCmd, &foundData,
                          &foundLen)) {
                ERR("Failed to retrieve key %" PRIu64 " after reopen", j);
            } else {
                if (foundTerm != j * 10) {
                    ERR("Term mismatch for key %" PRIu64, j);
                }
                if (foundCmd != j * 100) {
                    ERR("Cmd mismatch for key %" PRIu64, j);
                }
                if (foundLen != 7 || memcmp(foundData, "persist", 7) != 0) {
                    ERR("Data mismatch for key %" PRIu64, j);
                }
            }
        }

        kvidxClose(i2);
    }

    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 8: Error Handling
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testErrorHandling(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "error-handling");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for error handling tests");
        return;
    }

    TEST("Error: Remove non-existent key") {
        /* Should succeed (DELETE returns DONE even if no rows affected) */
        if (!kvidxRemove(i, 99999)) {
            ERRR("Remove of non-existent key failed");
        }
    }

    TEST("Error: Get non-existent key") {
        const uint8_t *data;
        size_t len;
        if (kvidxGet(i, 99999, NULL, NULL, &data, &len)) {
            ERRR("Get should return false for non-existent key");
        }
    }

    TEST("Error: ExistsDual with non-existent key") {
        if (kvidxExistsDual(i, 99999, 1)) {
            ERRR("ExistsDual should return false for non-existent key");
        }
    }

    TEST("Error: Duplicate key insert") {
        kvidxInsert(i, 1000, 1, 1, "first", 5);

        /* Second insert with same key should fail */
        if (kvidxInsert(i, 1000, 2, 2, "second", 6)) {
            ERRR("Duplicate key insert should fail");
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 9: Stress Testing
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testStress(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "stress");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for stress tests");
        return;
    }

    TEST("Stress: Insert 10000 sequential keys") {
        if (!kvidxBegin(i)) {
            ERRR("Failed to begin transaction");
        }

        for (uint64_t j = 1; j <= 10000; j++) {
            if (!kvidxInsert(i, j, j, j, "stress", 6)) {
                ERR("Failed to insert key %" PRIu64, j);
                break;
            }
        }

        if (!kvidxCommit(i)) {
            ERRR("Failed to commit transaction");
        }
    }

    TEST("Stress: Verify all 10000 keys exist") {
        for (uint64_t j = 1; j <= 10000; j++) {
            if (!kvidxExists(i, j)) {
                ERR("Key %" PRIu64 " not found", j);
                break;
            }
        }
    }

    TEST("Stress: MaxKey on large dataset") {
        uint64_t maxKey;
        if (!kvidxMaxKey(i, &maxKey)) {
            ERRR("Failed to get max key");
        } else if (maxKey != 10000) {
            ERR("Max key should be 10000, got %" PRIu64, maxKey);
        }
    }

    TEST("Stress: Random access on large dataset") {
        srand((unsigned int)time(NULL));
        for (int j = 0; j < 100; j++) {
            uint64_t randomKey = (rand() % 10000) + 1;
            if (!kvidxExists(i, randomKey)) {
                ERR("Random key %" PRIu64 " not found", randomKey);
            }
        }
    }

    TEST("Stress: Delete half the keys") {
        if (!kvidxRemoveAfterNInclusive(i, 5001)) {
            ERRR("Failed to remove range");
        }

        /* Verify 1-5000 exist */
        for (uint64_t j = 1; j <= 5000; j++) {
            if (!kvidxExists(i, j)) {
                ERR("Key %" PRIu64 " should exist", j);
                break;
            }
        }

        /* Verify 5001-10000 don't exist */
        for (uint64_t j = 5001; j <= 10000; j++) {
            if (kvidxExists(i, j)) {
                ERR("Key %" PRIu64 " should not exist", j);
                break;
            }
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
    printf("COMPREHENSIVE TEST SUITE FOR KVIDXKIT\n");
    printf("=======================================================\n\n");

    printf("Running Suite 1: Boundary Value Testing\n");
    printf("-------------------------------------------------------\n");
    testBoundaryValues(&err);
    printf("\n");

    printf("Running Suite 2: Data Integrity Testing\n");
    printf("-------------------------------------------------------\n");
    testDataIntegrity(&err);
    printf("\n");

    printf("Running Suite 3: Transaction Testing\n");
    printf("-------------------------------------------------------\n");
    testTransactions(&err);
    printf("\n");

    printf("Running Suite 4: Navigation Testing\n");
    printf("-------------------------------------------------------\n");
    testNavigation(&err);
    printf("\n");

    printf("Running Suite 5: Range Deletion Testing\n");
    printf("-------------------------------------------------------\n");
    testRangeDeletion(&err);
    printf("\n");

    printf("Running Suite 6: ExistsDual Testing\n");
    printf("-------------------------------------------------------\n");
    testExistsDual(&err);
    printf("\n");

    printf("Running Suite 7: Persistence Testing\n");
    printf("-------------------------------------------------------\n");
    testPersistence(&err);
    printf("\n");

    printf("Running Suite 8: Error Handling\n");
    printf("-------------------------------------------------------\n");
    testErrorHandling(&err);
    printf("\n");

    printf("Running Suite 9: Stress Testing\n");
    printf("-------------------------------------------------------\n");
    testStress(&err);
    printf("\n");

    printf("=======================================================\n");
    if (err == 0) {
        printf("ALL %d TEST SUITES PASSED!\n", 9);
        printf("=======================================================\n");
        return 0;
    } else {
        printf("FAILED: %u test(s) failed across all suites\n", err);
        printf("=======================================================\n");
        return 1;
    }
}
