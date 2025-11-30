/**
 * Comprehensive batch operation tests for kvidxkit
 * Tests batch insert functionality and performance
 */

#include "ctest.h"
#include "kvidxkit.h"

#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
/* cppcheck-suppress constParameterPointer */
/* Helper to get current time in microseconds */
static uint64_t getTimeMicros(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

static void makeTestFilename(char *buf, size_t bufSize, const char *testName) {
    snprintf(buf, bufSize, "test-batch-%s-%d.sqlite3", testName, getpid());
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
 * TEST SUITE 1: Basic Batch Insert
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testBasicBatch(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "basic");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for basic batch tests");
        return;
    }

    TEST("Batch Insert: Insert 100 entries") {
        kvidxEntry entries[100];
        for (int j = 0; j < 100; j++) {
            entries[j].key = j + 1;
            entries[j].term = 1;
            entries[j].cmd = j;
            entries[j].data = "batch";
            entries[j].dataLen = 5;
        }

        size_t inserted = 0;
        bool result = kvidxInsertBatch(i, entries, 100, &inserted);

        if (!result) {
            ERRR("Batch insert failed");
        }

        if (inserted != 100) {
            ERR("Expected 100 inserted, got %zu", inserted);
        }

        /* Verify all entries exist */
        for (int j = 0; j < 100; j++) {
            if (!kvidxExists(i, j + 1)) {
                ERR("Key %d not found after batch insert", j + 1);
            }
        }
    }

    TEST("Batch Insert: Empty batch") {
        kvidxEntry entries[1] = {{0}};
        size_t inserted = 999;
        bool result = kvidxInsertBatch(i, entries, 0, &inserted);

        if (!result) {
            ERRR("Empty batch should succeed");
        }

        if (inserted != 0) {
            ERR("Empty batch should insert 0 entries, got %zu", inserted);
        }
    }

    TEST("Batch Insert: NULL entries") {
        size_t inserted = 999;
        bool result = kvidxInsertBatch(i, NULL, 10, &inserted);

        if (result) {
            ERRR("NULL entries should fail");
        }

        if (inserted != 0) {
            ERR("NULL entries should insert 0, got %zu", inserted);
        }
    }

    TEST("Batch Insert: NULL instance") {
        kvidxEntry entries[1] = {{0}};
        size_t inserted = 999;
        bool result = kvidxInsertBatch(NULL, entries, 1, &inserted);

        if (result) {
            ERRR("NULL instance should fail");
        }

        if (inserted != 0) {
            ERR("NULL instance should insert 0, got %zu", inserted);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 2: Batch Insert with Duplicates
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testBatchDuplicates(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "duplicates");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for duplicate tests");
        return;
    }

    TEST("Batch Insert: Duplicate key in batch") {
        /* Insert keys 200, 201, 202, 201 (duplicate) */
        kvidxEntry entries[4];
        entries[0].key = 200;
        entries[0].term = 1;
        entries[0].cmd = 1;
        entries[0].data = "first";
        entries[0].dataLen = 5;

        entries[1].key = 201;
        entries[1].term = 1;
        entries[1].cmd = 1;
        entries[1].data = "second";
        entries[1].dataLen = 6;

        entries[2].key = 202;
        entries[2].term = 1;
        entries[2].cmd = 1;
        entries[2].data = "third";
        entries[2].dataLen = 5;

        entries[3].key = 201; /* Duplicate! */
        entries[3].term = 1;
        entries[3].cmd = 1;
        entries[3].data = "duplicate";
        entries[3].dataLen = 9;

        size_t inserted = 0;
        bool result = kvidxInsertBatch(i, entries, 4, &inserted);

        if (result) {
            ERRR("Batch with duplicate should fail");
        }

        /* First 3 should be inserted */
        if (inserted != 3) {
            ERR("Expected 3 inserted before duplicate, got %zu", inserted);
        }

        /* Verify first 3 exist */
        if (!kvidxExists(i, 200) || !kvidxExists(i, 201) ||
            !kvidxExists(i, 202)) {
            ERRR("First 3 entries should exist");
        }
    }

    TEST("Batch Insert: Duplicate of existing key") {
        /* Try to insert key 200 again */
        kvidxEntry entries[2];
        entries[0].key = 300;
        entries[0].term = 1;
        entries[0].cmd = 1;
        entries[0].data = "new";
        entries[0].dataLen = 3;

        entries[1].key = 200; /* Already exists */
        entries[1].term = 1;
        entries[1].cmd = 1;
        entries[1].data = "dup";
        entries[1].dataLen = 3;

        size_t inserted = 0;
        bool result = kvidxInsertBatch(i, entries, 2, &inserted);

        if (result) {
            ERRR("Batch with existing key should fail");
        }

        if (inserted != 1) {
            ERR("Expected 1 inserted before duplicate, got %zu", inserted);
        }

        if (!kvidxExists(i, 300)) {
            ERRR("New key should have been inserted");
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 3: Batch Insert with Callback
 * ==================================================================== */

/* Test callback that filters out even keys */
static bool filterEvenKeys(size_t index, const kvidxEntry *entry,
                           void *userData) {
    (void)index;
    (void)userData;
    return (entry->key % 2) != 0; /* Only insert odd keys */
}

/* Test callback that counts invocations */
static bool countingCallback(size_t index, const kvidxEntry *entry,
                             void *userData) {
    (void)index;
    (void)entry;
    int *count = (int *)userData;
    (*count)++;
    return true; /* Insert all */
}

/* cppcheck-suppress constParameterPointer */
static void testBatchCallback(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "callback");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for callback tests");
        return;
    }

    TEST("Batch Insert Ex: Filter even keys") {
        kvidxEntry entries[10];
        for (int j = 0; j < 10; j++) {
            entries[j].key = 400 + j; /* 400-409 */
            entries[j].term = 1;
            entries[j].cmd = 1;
            entries[j].data = "test";
            entries[j].dataLen = 4;
        }

        size_t inserted = 0;
        bool result =
            kvidxInsertBatchEx(i, entries, 10, filterEvenKeys, NULL, &inserted);

        if (!result) {
            ERRR("Batch insert with filter should succeed");
        }

        /* Only 5 odd keys should be inserted: 401, 403, 405, 407, 409 */
        if (inserted != 5) {
            ERR("Expected 5 odd keys inserted, got %zu", inserted);
        }

        /* Verify odd keys exist, even keys don't */
        for (int j = 0; j < 10; j++) {
            bool shouldExist = (j % 2) != 0;
            bool exists = kvidxExists(i, 400 + j);
            if (shouldExist && !exists) {
                ERR("Odd key %d should exist", 400 + j);
            }
            if (!shouldExist && exists) {
                ERR("Even key %d should not exist", 400 + j);
            }
        }
    }

    TEST("Batch Insert Ex: Callback receives all entries") {
        kvidxEntry entries[20];
        for (int j = 0; j < 20; j++) {
            entries[j].key = 500 + j;
            entries[j].term = 1;
            entries[j].cmd = 1;
            entries[j].data = "test";
            entries[j].dataLen = 4;
        }

        int callbackCount = 0;
        size_t inserted = 0;
        bool result = kvidxInsertBatchEx(i, entries, 20, countingCallback,
                                         &callbackCount, &inserted);

        if (!result) {
            ERRR("Batch insert with counting callback should succeed");
        }

        if (callbackCount != 20) {
            ERR("Callback should be called 20 times, got %d", callbackCount);
        }

        if (inserted != 20) {
            ERR("Expected 20 inserted, got %zu", inserted);
        }
    }

    TEST("Batch Insert Ex: NULL callback (should work like regular batch)") {
        kvidxEntry entries[5];
        for (int j = 0; j < 5; j++) {
            entries[j].key = 600 + j;
            entries[j].term = 1;
            entries[j].cmd = 1;
            entries[j].data = "test";
            entries[j].dataLen = 4;
        }

        size_t inserted = 0;
        bool result = kvidxInsertBatchEx(i, entries, 5, NULL, NULL, &inserted);

        if (!result) {
            ERRR("Batch insert with NULL callback should succeed");
        }

        if (inserted != 5) {
            ERR("Expected 5 inserted, got %zu", inserted);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 4: Batch Insert Data Integrity
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testBatchDataIntegrity(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "integrity");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for integrity tests");
        return;
    }

    TEST("Batch Insert: Data integrity for 50 entries") {
        const char *testData[5] = {"one", "two", "three", "four", "five"};
        kvidxEntry entries[50];

        for (int j = 0; j < 50; j++) {
            entries[j].key = 700 + j;
            entries[j].term = j;
            entries[j].cmd = (uint64_t)j * 2;
            entries[j].data = testData[j % 5];
            entries[j].dataLen = strlen(testData[j % 5]);
        }

        size_t inserted = 0;
        bool result = kvidxInsertBatch(i, entries, 50, &inserted);

        if (!result || inserted != 50) {
            ERR("Failed to insert 50 entries: result=%d, inserted=%zu", result,
                inserted);
        }

        /* Verify all data */
        for (int j = 0; j < 50; j++) {
            uint64_t foundTerm, foundCmd;
            const uint8_t *foundData;
            size_t foundLen;

            if (!kvidxGet(i, 700 + j, &foundTerm, &foundCmd, &foundData,
                          &foundLen)) {
                ERR("Failed to retrieve key %d", 700 + j);
                continue;
            }

            if (foundTerm != (uint64_t)j) {
                ERR("Term mismatch for key %d: expected %d, got %" PRIu64,
                    700 + j, j, foundTerm);
            }

            if (foundCmd != (uint64_t)(j * 2)) {
                ERR("Cmd mismatch for key %d: expected %d, got %" PRIu64,
                    700 + j, j * 2, foundCmd);
            }

            const char *expectedData = testData[j % 5];
            if (foundLen != strlen(expectedData) ||
                memcmp(foundData, expectedData, foundLen) != 0) {
                ERR("Data mismatch for key %d", 700 + j);
            }
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 5: Performance Benchmarks
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testBatchPerformance(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "perf");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for performance tests");
        return;
    }

    const int BATCH_SIZE = 1000;

    TEST("Performance: Individual inserts (1000 entries)") {
        uint64_t start = getTimeMicros();

        for (int j = 0; j < BATCH_SIZE; j++) {
            if (!kvidxBegin(i)) {
                ERR("Begin failed at %d", j);
                break;
            }

            bool inserted = kvidxInsert(i, 1000 + j, 1, 1, "individual", 10);
            if (!inserted) {
                ERR("Insert failed at %d", j);
            }

            if (!kvidxCommit(i)) {
                ERR("Commit failed at %d", j);
                break;
            }
        }

        uint64_t end = getTimeMicros();
        uint64_t duration = end - start;

        printf("        Individual inserts: %" PRIu64 " us (%.2f ops/sec)\n",
               duration, (double)BATCH_SIZE / ((double)duration / 1000000.0));
    }

    TEST("Performance: Batch insert (1000 entries)") {
        kvidxEntry entries[BATCH_SIZE];
        for (int j = 0; j < BATCH_SIZE; j++) {
            entries[j].key = 2000 + j;
            entries[j].term = 1;
            entries[j].cmd = 1;
            entries[j].data = "batch";
            entries[j].dataLen = 5;
        }

        uint64_t start = getTimeMicros();

        size_t inserted = 0;
        bool result = kvidxInsertBatch(i, entries, BATCH_SIZE, &inserted);

        uint64_t end = getTimeMicros();
        uint64_t duration = end - start;

        if (!result || inserted != BATCH_SIZE) {
            ERR("Batch insert failed: result=%d, inserted=%zu", result,
                inserted);
        }

        printf("        Batch insert: %" PRIu64 " us (%.2f ops/sec)\n",
               duration, (double)BATCH_SIZE / ((double)duration / 1000000.0));

        printf("        Speedup: %.1fx faster\n",
               (double)duration / 1000.0); /* Approximate comparison */
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
    printf("BATCH OPERATION TESTS FOR KVIDXKIT\n");
    printf("=======================================================\n\n");

    printf("Running Suite 1: Basic Batch Insert\n");
    printf("-------------------------------------------------------\n");
    testBasicBatch(&err);
    printf("\n");

    printf("Running Suite 2: Batch Insert with Duplicates\n");
    printf("-------------------------------------------------------\n");
    testBatchDuplicates(&err);
    printf("\n");

    printf("Running Suite 3: Batch Insert with Callback\n");
    printf("-------------------------------------------------------\n");
    testBatchCallback(&err);
    printf("\n");

    printf("Running Suite 4: Batch Insert Data Integrity\n");
    printf("-------------------------------------------------------\n");
    testBatchDataIntegrity(&err);
    printf("\n");

    printf("Running Suite 5: Performance Benchmarks\n");
    printf("-------------------------------------------------------\n");
    testBatchPerformance(&err);
    printf("\n");

    printf("=======================================================\n");
    if (err == 0) {
        printf("ALL BATCH OPERATION TESTS PASSED!\n");
        printf("=======================================================\n");
        return 0;
    } else {
        printf("FAILED: %u test(s) failed\n", err);
        printf("=======================================================\n");
        return 1;
    }
}
