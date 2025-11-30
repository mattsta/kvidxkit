/**
 * Comprehensive iterator tests for kvidxkit
 * Tests iterator functionality, edge cases, and performance
 */

#include "ctest.h"
#include "kvidxkit.h"
#include "kvidxkitIterator.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
/* cppcheck-suppress constParameterPointer */
static void makeTestFilename(char *buf, size_t bufSize, const char *testName) {
    snprintf(buf, bufSize, "test-iterator-%s-%d.sqlite3", testName, getpid());
}

static void cleanupTestFile(const char *filename) {
    unlink(filename);
}

static bool openFresh(kvidxInstance *i, const char *filename) {
    memset(i, 0, sizeof(*i));
    i->interface = kvidxInterfaceSqlite3;
    return kvidxOpen(i, filename, NULL);
}

/* Helper to insert test data */
static bool insertTestData(kvidxInstance *i, uint64_t start, uint64_t count) {
    for (uint64_t j = 0; j < count; j++) {
        uint64_t key = start + j;
        if (!kvidxInsert(i, key, j, j * 2, "data", 4)) {
            return false;
        }
    }
    return true;
}

/* ====================================================================
 * TEST SUITE 1: Basic Forward Iteration
 * ==================================================================== */
static void testForwardIteration(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "forward");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for forward iteration tests");
        return;
    }

    TEST("Forward Iteration: Full range (100 entries)") {
        /* Insert 100 entries: keys 1-100 */
        if (!insertTestData(i, 1, 100)) {
            ERRR("Failed to insert test data");
        }

        /* Create forward iterator for full range */
        kvidxIterator *it =
            kvidxIteratorCreate(i, 0, UINT64_MAX, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        /* Iterate through all entries */
        uint64_t count = 0;
        uint64_t expectedKey = 1;
        while (kvidxIteratorNext(it)) {
            if (!kvidxIteratorValid(it)) {
                ERRR("Iterator should be valid after Next returned true");
                break;
            }

            uint64_t key = kvidxIteratorKey(it);
            if (key != expectedKey) {
                ERR("Expected key %" PRIu64 ", got %" PRIu64 "", expectedKey,
                    key);
            }

            count++;
            expectedKey++;
        }

        if (count != 100) {
            ERR("Expected 100 entries, got %" PRIu64 "", count);
        }

        kvidxIteratorDestroy(it);
    }

    TEST("Forward Iteration: Partial range") {
        /* Iterate keys 25-75 */
        kvidxIterator *it = kvidxIteratorCreate(i, 25, 75, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        uint64_t count = 0;
        uint64_t expectedKey = 25;
        while (kvidxIteratorNext(it)) {
            uint64_t key = kvidxIteratorKey(it);
            if (key != expectedKey) {
                ERR("Expected key %" PRIu64 ", got %" PRIu64 "", expectedKey,
                    key);
            }

            count++;
            expectedKey++;

            if (key > 75) {
                ERR("Iterator went past end of range: key %" PRIu64 "", key);
                break;
            }
        }

        if (count != 51) { /* 25-75 inclusive = 51 entries */
            ERR("Expected 51 entries, got %" PRIu64 "", count);
        }

        kvidxIteratorDestroy(it);
    }

    TEST("Forward Iteration: Get full entry data") {
        kvidxIterator *it = kvidxIteratorCreate(i, 10, 20, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        if (!kvidxIteratorNext(it)) {
            ERRR("Should have first entry");
        }

        uint64_t key, term, cmd;
        const uint8_t *data;
        size_t len;

        if (!kvidxIteratorGet(it, &key, &term, &cmd, &data, &len)) {
            ERRR("Failed to get iterator data");
        }

        if (key != 10) {
            ERR("Expected key 10, got %" PRIu64 "", key);
        }

        if (len != 4 || memcmp(data, "data", 4) != 0) {
            ERRR("Data mismatch");
        }

        kvidxIteratorDestroy(it);
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 2: Backward Iteration
 * ==================================================================== */
static void testBackwardIteration(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "backward");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for backward iteration tests");
        return;
    }

    TEST("Backward Iteration: Full range (100 entries)") {
        /* Insert 100 entries: keys 1-100 */
        if (!insertTestData(i, 1, 100)) {
            ERRR("Failed to insert test data");
        }

        /* Create backward iterator for full range */
        kvidxIterator *it =
            kvidxIteratorCreate(i, 0, UINT64_MAX, KVIDX_ITER_BACKWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        /* Iterate backward through all entries */
        uint64_t count = 0;
        uint64_t expectedKey = 100; /* Start from highest */
        while (kvidxIteratorNext(it)) {
            uint64_t key = kvidxIteratorKey(it);
            if (key != expectedKey) {
                ERR("Expected key %" PRIu64 ", got %" PRIu64 "", expectedKey,
                    key);
            }

            count++;
            expectedKey--;
        }

        if (count != 100) {
            ERR("Expected 100 entries, got %" PRIu64 "", count);
        }

        kvidxIteratorDestroy(it);
    }

    TEST("Backward Iteration: Partial range") {
        /* Iterate keys 75-25 (backward) */
        kvidxIterator *it = kvidxIteratorCreate(i, 25, 75, KVIDX_ITER_BACKWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        uint64_t count = 0;
        uint64_t expectedKey = 75; /* Start from highest in range */
        while (kvidxIteratorNext(it)) {
            uint64_t key = kvidxIteratorKey(it);
            if (key != expectedKey) {
                ERR("Expected key %" PRIu64 ", got %" PRIu64 "", expectedKey,
                    key);
            }

            count++;
            expectedKey--;

            if (key < 25) {
                ERR("Iterator went past start of range: key %" PRIu64 "", key);
                break;
            }
        }

        if (count != 51) {
            ERR("Expected 51 entries, got %" PRIu64 "", count);
        }

        kvidxIteratorDestroy(it);
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 3: Edge Cases
 * ==================================================================== */
static void testEdgeCases(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "edge");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for edge case tests");
        return;
    }

    TEST("Edge Case: Empty database") {
        kvidxIterator *it =
            kvidxIteratorCreate(i, 0, UINT64_MAX, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        bool hasNext = kvidxIteratorNext(it);
        if (hasNext) {
            ERRR("Empty database should have no entries");
        }

        if (kvidxIteratorValid(it)) {
            ERRR("Iterator should be invalid on empty database");
        }

        kvidxIteratorDestroy(it);
    }

    TEST("Edge Case: Single entry") {
        if (!kvidxInsert(i, 42, 1, 1, "single", 6)) {
            ERRR("Failed to insert single entry");
        }

        kvidxIterator *it =
            kvidxIteratorCreate(i, 0, UINT64_MAX, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        uint64_t count = 0;
        while (kvidxIteratorNext(it)) {
            uint64_t key = kvidxIteratorKey(it);
            if (key != 42) {
                ERR("Expected key 42, got %" PRIu64 "", key);
            }
            count++;
        }

        if (count != 1) {
            ERR("Expected 1 entry, got %" PRIu64 "", count);
        }

        kvidxIteratorDestroy(it);
    }

    TEST("Edge Case: Range with no matching keys") {
        /* Database has key 42, request range 100-200 */
        kvidxIterator *it =
            kvidxIteratorCreate(i, 100, 200, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        bool hasNext = kvidxIteratorNext(it);
        if (hasNext) {
            ERRR("Range with no keys should have no entries");
        }

        kvidxIteratorDestroy(it);
    }

    TEST("Edge Case: NULL iterator operations") {
        bool result = kvidxIteratorNext(NULL);
        if (result) {
            ERRR("Next on NULL should return false");
        }

        if (kvidxIteratorValid(NULL)) {
            ERRR("Valid on NULL should return false");
        }

        uint64_t key = kvidxIteratorKey(NULL);
        if (key != 0) {
            ERR("Key on NULL should return 0, got %" PRIu64 "", key);
        }

        /* Should not crash */
        kvidxIteratorDestroy(NULL);
    }

    TEST("Edge Case: Invalid range (start > end)") {
        kvidxIterator *it = kvidxIteratorCreate(i, 100, 50, KVIDX_ITER_FORWARD);
        if (it != NULL) {
            ERRR("Should not create iterator with invalid range");
            kvidxIteratorDestroy(it);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 4: Seek Functionality
 * ==================================================================== */
static void testSeek(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "seek");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for seek tests");
        return;
    }

    /* Insert entries at 10, 20, 30, 40, 50 */
    TEST("Seek: Exact key match") {
        for (uint64_t k = 10; k <= 50; k += 10) {
            if (!kvidxInsert(i, k, 1, 1, "data", 4)) {
                ERRR("Failed to insert test data");
            }
        }

        kvidxIterator *it =
            kvidxIteratorCreate(i, 0, UINT64_MAX, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        /* Seek to key 30 */
        if (!kvidxIteratorSeek(it, 30)) {
            ERRR("Seek to existing key should succeed");
        }

        if (!kvidxIteratorValid(it)) {
            ERRR("Iterator should be valid after seek");
        }

        uint64_t key = kvidxIteratorKey(it);
        if (key != 30) {
            ERR("Expected key 30 after seek, got %" PRIu64 "", key);
        }

        kvidxIteratorDestroy(it);
    }

    TEST("Seek: Non-existent key (forward)") {
        kvidxIterator *it =
            kvidxIteratorCreate(i, 0, UINT64_MAX, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        /* Seek to 25 (doesn't exist, should position at 30) */
        if (!kvidxIteratorSeek(it, 25)) {
            ERRR("Seek should succeed even if key doesn't exist");
        }

        uint64_t key = kvidxIteratorKey(it);
        if (key != 30) {
            ERR("Expected key 30 (next after 25), got %" PRIu64 "", key);
        }

        kvidxIteratorDestroy(it);
    }

    TEST("Seek: Non-existent key (backward)") {
        kvidxIterator *it =
            kvidxIteratorCreate(i, 0, UINT64_MAX, KVIDX_ITER_BACKWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        /* Seek to 25 (doesn't exist, should position at 20) */
        if (!kvidxIteratorSeek(it, 25)) {
            ERRR("Seek should succeed even if key doesn't exist");
        }

        uint64_t key = kvidxIteratorKey(it);
        if (key != 20) {
            ERR("Expected key 20 (prev before 25), got %" PRIu64 "", key);
        }

        kvidxIteratorDestroy(it);
    }

    TEST("Seek: Outside range") {
        kvidxIterator *it = kvidxIteratorCreate(i, 20, 40, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        /* Seek to 100 (outside range) */
        bool result = kvidxIteratorSeek(it, 100);
        if (result) {
            ERRR("Seek outside range should fail");
        }

        if (kvidxIteratorValid(it)) {
            ERRR("Iterator should be invalid after failed seek");
        }

        kvidxIteratorDestroy(it);
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 5: Sparse Data Iteration
 * ==================================================================== */
static void testSparseData(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "sparse");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for sparse data tests");
        return;
    }

    TEST("Sparse Data: Large gaps between keys") {
        /* Insert keys at 1, 1000, 1000000, 1000000000000 (1 trillion) */
        /* Note: Can't use UINT64_MAX-1 due to SQLite signed int64 limitation */
        uint64_t keys[] = {1, 1000, 1000000, 1000000000000ULL};
        size_t numKeys = sizeof(keys) / sizeof(keys[0]);

        for (size_t j = 0; j < numKeys; j++) {
            if (!kvidxInsert(i, keys[j], j, j, "data", 4)) {
                ERR("Failed to insert key %" PRIu64 "", keys[j]);
            }
        }

        /* Verify all keys exist */
        for (size_t j = 0; j < numKeys; j++) {
            if (!kvidxExists(i, keys[j])) {
                ERR("Key %" PRIu64 " does not exist after insert", keys[j]);
            }
        }

        /* Forward iteration */
        kvidxIterator *it =
            kvidxIteratorCreate(i, 0, UINT64_MAX, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        uint64_t count = 0;
        size_t keyIndex = 0;
        while (kvidxIteratorNext(it)) {
            uint64_t key = kvidxIteratorKey(it);
            if (keyIndex >= numKeys) {
                ERRR("Too many keys returned");
                break;
            }

            if (key != keys[keyIndex]) {
                ERR("Expected key %" PRIu64 ", got %" PRIu64 "", keys[keyIndex],
                    key);
            }

            count++;
            keyIndex++;
        }

        if (count != numKeys) {
            ERR("Expected %zu entries, got %" PRIu64 "", numKeys, count);
        }

        kvidxIteratorDestroy(it);
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 6: Iterator Reuse
 * ==================================================================== */
static void testIteratorReuse(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "reuse");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for reuse tests");
        return;
    }

    TEST("Iterator Reuse: Multiple iterations with same iterator") {
        if (!insertTestData(i, 1, 10)) {
            ERRR("Failed to insert test data");
        }

        kvidxIterator *it = kvidxIteratorCreate(i, 1, 10, KVIDX_ITER_FORWARD);
        if (!it) {
            ERRR("Failed to create iterator");
        }

        /* First iteration */
        uint64_t count1 = 0;
        while (kvidxIteratorNext(it)) {
            count1++;
        }

        /* Iterator should be exhausted */
        if (kvidxIteratorValid(it)) {
            ERRR("Iterator should be invalid after exhaustion");
        }

        /* Trying to iterate again should not yield more results */
        uint64_t count2 = 0;
        while (kvidxIteratorNext(it)) {
            count2++;
        }

        if (count2 != 0) {
            ERR("Exhausted iterator should not yield more results, got %" PRIu64
                "",
                count2);
        }

        /* Seek should re-enable the iterator */
        if (!kvidxIteratorSeek(it, 5)) {
            ERRR("Seek should succeed");
        }

        if (!kvidxIteratorValid(it)) {
            ERRR("Iterator should be valid after seek");
        }

        kvidxIteratorDestroy(it);
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 7: Memory and Resource Management
 * ==================================================================== */
static void testMemoryManagement(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "memory");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for memory tests");
        return;
    }

    TEST("Memory: Create and destroy 1000 iterators") {
        if (!insertTestData(i, 1, 100)) {
            ERRR("Failed to insert test data");
        }

        for (int j = 0; j < 1000; j++) {
            kvidxIterator *it =
                kvidxIteratorCreate(i, 0, UINT64_MAX, KVIDX_ITER_FORWARD);
            if (!it) {
                ERR("Failed to create iterator %d", j);
                break;
            }

            /* Use the iterator briefly */
            kvidxIteratorNext(it);

            kvidxIteratorDestroy(it);
        }

        /* If we got here without crashing, memory management is probably OK */
    }

    TEST("Memory: Concurrent iterators") {
        /* Create 10 iterators simultaneously */
        kvidxIterator *iterators[10];
        for (int j = 0; j < 10; j++) {
            iterators[j] =
                kvidxIteratorCreate(i, 0, UINT64_MAX, KVIDX_ITER_FORWARD);
            if (!iterators[j]) {
                ERR("Failed to create iterator %d", j);
            }
        }

        /* Use them all */
        for (int j = 0; j < 10; j++) {
            if (iterators[j]) {
                kvidxIteratorNext(iterators[j]);
            }
        }

        /* Destroy them all */
        for (int j = 0; j < 10; j++) {
            kvidxIteratorDestroy(iterators[j]);
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
    printf("ITERATOR TESTS FOR KVIDXKIT\n");
    printf("=======================================================\n\n");

    printf("Running Suite 1: Basic Forward Iteration\n");
    printf("-------------------------------------------------------\n");
    testForwardIteration(&err);
    printf("\n");

    printf("Running Suite 2: Backward Iteration\n");
    printf("-------------------------------------------------------\n");
    testBackwardIteration(&err);
    printf("\n");

    printf("Running Suite 3: Edge Cases\n");
    printf("-------------------------------------------------------\n");
    testEdgeCases(&err);
    printf("\n");

    printf("Running Suite 4: Seek Functionality\n");
    printf("-------------------------------------------------------\n");
    testSeek(&err);
    printf("\n");

    printf("Running Suite 5: Sparse Data Iteration\n");
    printf("-------------------------------------------------------\n");
    testSparseData(&err);
    printf("\n");

    printf("Running Suite 6: Iterator Reuse\n");
    printf("-------------------------------------------------------\n");
    testIteratorReuse(&err);
    printf("\n");

    printf("Running Suite 7: Memory and Resource Management\n");
    printf("-------------------------------------------------------\n");
    testMemoryManagement(&err);
    printf("\n");

    printf("=======================================================\n");
    if (err == 0) {
        printf("ALL ITERATOR TESTS PASSED!\n");
        printf("=======================================================\n");
        return 0;
    } else {
        printf("FAILED: %u test(s) failed\n", err);
        printf("=======================================================\n");
        return 1;
    }
}
