/**
 * Comprehensive test suite for LMDB adapter
 *
 * Tests all functionality of the kvidxkit LMDB backend adapter including:
 * - Basic CRUD operations
 * - Transactions
 * - Range operations
 * - Statistics
 * - Export/Import
 * - Edge cases
 */

#include "ctest.h"
#include "kvidxkit.h"

#include <dirent.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Helper to recursively remove a directory */
static int removeDir(const char *path) {
    DIR *d = opendir(path);
    if (!d) {
        return -1;
    }

    struct dirent *entry;
    char filepath[512];

    while ((entry = readdir(d)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 ||
            strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        snprintf(filepath, sizeof(filepath), "%s/%s", path, entry->d_name);

        struct stat st;
        if (stat(filepath, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                removeDir(filepath);
            } else {
                unlink(filepath);
            }
        }
    }

    closedir(d);
    return rmdir(path);
}

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;
    uint32_t err = 0;

    printf("=== LMDB Adapter Test Suite ===\n\n");

    /* ================================================================
     * Basic Open/Close Tests
     * ================================================================ */
    {
        kvidxInstance pre = {0};
        kvidxInstance *i = &pre;
        i->interface = kvidxInterfaceLmdb;

        char dirname[64] = {0};
        snprintf(dirname, sizeof(dirname), "test-lmdb-openclose-%d", getpid());

        printf("Testing LMDB open/close in directory: %s\n", dirname);

        TEST("LMDB open/close...") {
            const char *errStr = NULL;
            bool opened = kvidxOpen(i, dirname, &errStr);
            if (!opened) {
                ERR("Failed to open LMDB! Error: %s",
                    errStr ? errStr : "unknown");
            }

            bool closed = kvidxClose(i);
            if (!closed) {
                ERRR("Failed to close LMDB!");
            }
        }

        removeDir(dirname);
    }

    /* ================================================================
     * Core CRUD Operations
     * ================================================================ */
    {
        kvidxInstance pre = {0};
        kvidxInstance *i = &pre;
        i->interface = kvidxInterfaceLmdb;

        char dirname[64] = {0};
        snprintf(dirname, sizeof(dirname), "test-lmdb-crud-%d", getpid());

        printf("\nTesting LMDB CRUD operations in: %s\n", dirname);
        kvidxOpen(i, dirname, NULL);

        uint64_t key = 100;
        uint64_t term = 500;
        uint64_t cmd = 42;
        const char *testData = "Hello, LMDB!";
        size_t dataLen = strlen(testData);

        TEST("LMDB exists with no data...") {
            bool exists = kvidxExists(i, key);
            if (exists) {
                ERRR("Found key when it shouldn't exist!");
            }
        }

        TEST("LMDB insert and exists...") {
            bool inserted = kvidxInsert(i, key, term, cmd, testData, dataLen);
            if (!inserted) {
                ERRR("Insert failed!");
            }

            bool exists = kvidxExists(i, key);
            if (!exists) {
                ERRR("Key not found after insert!");
            }
        }

        TEST("LMDB existsDual...") {
            bool exists = kvidxExistsDual(i, key, term);
            if (!exists) {
                ERRR("Key+term pair not found!");
            }

            bool wrongTerm = kvidxExistsDual(i, key, term + 1);
            if (wrongTerm) {
                ERRR("Found key with wrong term!");
            }
        }

        TEST("LMDB get and verify data...") {
            uint64_t gotTerm, gotCmd;
            const uint8_t *gotData;
            size_t gotLen;

            bool got = kvidxGet(i, key, &gotTerm, &gotCmd, &gotData, &gotLen);
            if (!got) {
                ERRR("Get failed!");
            }

            if (gotTerm != term) {
                ERR("Term mismatch: got %" PRIu64 ", expected %" PRIu64 "",
                    gotTerm, term);
            }

            if (gotCmd != cmd) {
                ERR("Cmd mismatch: got %" PRIu64 ", expected %" PRIu64 "",
                    gotCmd, cmd);
            }

            if (gotLen != dataLen) {
                ERR("Data length mismatch: got %zu, expected %zu", gotLen,
                    dataLen);
            }

            if (memcmp(gotData, testData, dataLen) != 0) {
                ERRR("Data content mismatch!");
            }
        }

        TEST("LMDB maxKey...") {
            uint64_t maxKey;
            bool got = kvidxMaxKey(i, &maxKey);
            if (!got) {
                ERRR("MaxKey failed!");
            }
            if (maxKey != key) {
                ERR("MaxKey mismatch: got %" PRIu64 ", expected %" PRIu64 "",
                    maxKey, key);
            }
        }

        TEST("LMDB remove...") {
            bool removed = kvidxRemove(i, key);
            if (!removed) {
                ERRR("Remove failed!");
            }

            bool exists = kvidxExists(i, key);
            if (exists) {
                ERRR("Key still exists after remove!");
            }
        }

        kvidxClose(i);
        removeDir(dirname);
    }

    /* ================================================================
     * Transaction Tests
     * ================================================================ */
    {
        kvidxInstance pre = {0};
        kvidxInstance *i = &pre;
        i->interface = kvidxInterfaceLmdb;

        char dirname[64] = {0};
        snprintf(dirname, sizeof(dirname), "test-lmdb-txn-%d", getpid());

        printf("\nTesting LMDB transactions in: %s\n", dirname);
        kvidxOpen(i, dirname, NULL);

        TEST("LMDB transaction commit...") {
            if (!kvidxBegin(i)) {
                ERRR("Begin failed!");
            }

            for (uint64_t k = 1; k <= 10; k++) {
                if (!kvidxInsert(i, k, k * 10, k * 100, NULL, 0)) {
                    ERR("Insert %" PRIu64 " failed!", k);
                }
            }

            if (!kvidxCommit(i)) {
                ERRR("Commit failed!");
            }

            /* Verify all inserted */
            for (uint64_t k = 1; k <= 10; k++) {
                if (!kvidxExists(i, k)) {
                    ERR("Key %" PRIu64 " not found after commit!", k);
                }
            }
        }

        TEST("LMDB batch insert performance...") {
            kvidxEntry entries[100];
            for (int j = 0; j < 100; j++) {
                entries[j].key = 1000 + j;
                entries[j].term = j;
                entries[j].cmd = j * 2;
                entries[j].data = NULL;
                entries[j].dataLen = 0;
            }

            size_t inserted = 0;
            bool ok = kvidxInsertBatch(i, entries, 100, &inserted);
            if (!ok || inserted != 100) {
                ERR("Batch insert failed: inserted %zu of 100", inserted);
            }
        }

        kvidxClose(i);
        removeDir(dirname);
    }

    /* ================================================================
     * Navigation Tests (GetPrev, GetNext)
     * ================================================================ */
    {
        kvidxInstance pre = {0};
        kvidxInstance *i = &pre;
        i->interface = kvidxInterfaceLmdb;

        char dirname[64] = {0};
        snprintf(dirname, sizeof(dirname), "test-lmdb-nav-%d", getpid());

        printf("\nTesting LMDB navigation in: %s\n", dirname);
        kvidxOpen(i, dirname, NULL);

        /* Insert keys: 10, 20, 30, 40, 50 */
        kvidxBegin(i);
        for (uint64_t k = 10; k <= 50; k += 10) {
            kvidxInsert(i, k, k, k, NULL, 0);
        }
        kvidxCommit(i);

        TEST("LMDB getNext...") {
            uint64_t nextKey, nextTerm;

            /* Get first key > 0 */
            if (!kvidxGetNext(i, 0, &nextKey, &nextTerm, NULL, NULL, NULL)) {
                ERRR("GetNext from 0 failed!");
            }
            if (nextKey != 10) {
                ERR("Expected 10, got %" PRIu64 "", nextKey);
            }

            /* Get key > 10 */
            if (!kvidxGetNext(i, 10, &nextKey, &nextTerm, NULL, NULL, NULL)) {
                ERRR("GetNext from 10 failed!");
            }
            if (nextKey != 20) {
                ERR("Expected 20, got %" PRIu64 "", nextKey);
            }

            /* Get key > 25 (should be 30) */
            if (!kvidxGetNext(i, 25, &nextKey, &nextTerm, NULL, NULL, NULL)) {
                ERRR("GetNext from 25 failed!");
            }
            if (nextKey != 30) {
                ERR("Expected 30, got %" PRIu64 "", nextKey);
            }

            /* Get key > 50 (should fail) */
            if (kvidxGetNext(i, 50, &nextKey, &nextTerm, NULL, NULL, NULL)) {
                ERRR("GetNext from 50 should fail!");
            }
        }

        TEST("LMDB getPrev...") {
            uint64_t prevKey, prevTerm;

            /* Get key < UINT64_MAX (should be 50) */
            if (!kvidxGetPrev(i, UINT64_MAX, &prevKey, &prevTerm, NULL, NULL,
                              NULL)) {
                ERRR("GetPrev from UINT64_MAX failed!");
            }
            if (prevKey != 50) {
                ERR("Expected 50, got %" PRIu64 "", prevKey);
            }

            /* Get key < 50 */
            if (!kvidxGetPrev(i, 50, &prevKey, &prevTerm, NULL, NULL, NULL)) {
                ERRR("GetPrev from 50 failed!");
            }
            if (prevKey != 40) {
                ERR("Expected 40, got %" PRIu64 "", prevKey);
            }

            /* Get key < 25 (should be 20) */
            if (!kvidxGetPrev(i, 25, &prevKey, &prevTerm, NULL, NULL, NULL)) {
                ERRR("GetPrev from 25 failed!");
            }
            if (prevKey != 20) {
                ERR("Expected 20, got %" PRIu64 "", prevKey);
            }

            /* Get key < 10 (should fail) */
            if (kvidxGetPrev(i, 10, &prevKey, &prevTerm, NULL, NULL, NULL)) {
                ERRR("GetPrev from 10 should fail!");
            }
        }

        kvidxClose(i);
        removeDir(dirname);
    }

    /* ================================================================
     * Range Delete Tests
     * ================================================================ */
    {
        kvidxInstance pre = {0};
        kvidxInstance *i = &pre;
        i->interface = kvidxInterfaceLmdb;

        char dirname[64] = {0};
        snprintf(dirname, sizeof(dirname), "test-lmdb-range-%d", getpid());

        printf("\nTesting LMDB range operations in: %s\n", dirname);
        kvidxOpen(i, dirname, NULL);

        TEST("LMDB removeAfterNInclusive...") {
            /* Insert 1-10 */
            kvidxBegin(i);
            for (uint64_t k = 1; k <= 10; k++) {
                kvidxInsert(i, k, k, k, NULL, 0);
            }
            kvidxCommit(i);

            /* Remove >= 6 */
            kvidxRemoveAfterNInclusive(i, 6);

            /* Check 1-5 exist, 6-10 don't */
            for (uint64_t k = 1; k <= 5; k++) {
                if (!kvidxExists(i, k)) {
                    ERR("Key %" PRIu64 " should exist!", k);
                }
            }
            for (uint64_t k = 6; k <= 10; k++) {
                if (kvidxExists(i, k)) {
                    ERR("Key %" PRIu64 " should NOT exist!", k);
                }
            }
        }

        TEST("LMDB removeBeforeNInclusive...") {
            /* Re-insert 6-10 */
            kvidxBegin(i);
            for (uint64_t k = 6; k <= 10; k++) {
                kvidxInsert(i, k, k, k, NULL, 0);
            }
            kvidxCommit(i);

            /* Remove <= 3 */
            kvidxRemoveBeforeNInclusive(i, 3);

            /* Check 1-3 don't exist, 4-10 do */
            for (uint64_t k = 1; k <= 3; k++) {
                if (kvidxExists(i, k)) {
                    ERR("Key %" PRIu64 " should NOT exist!", k);
                }
            }
            for (uint64_t k = 4; k <= 10; k++) {
                if (!kvidxExists(i, k)) {
                    ERR("Key %" PRIu64 " should exist!", k);
                }
            }
        }

        kvidxClose(i);
        removeDir(dirname);
    }

    /* ================================================================
     * Statistics Tests
     * ================================================================ */
    {
        kvidxInstance pre = {0};
        kvidxInstance *i = &pre;
        i->interface = kvidxInterfaceLmdb;

        char dirname[64] = {0};
        snprintf(dirname, sizeof(dirname), "test-lmdb-stats-%d", getpid());

        printf("\nTesting LMDB statistics in: %s\n", dirname);
        kvidxOpen(i, dirname, NULL);

        /* Insert some data */
        kvidxBegin(i);
        const char *data1 = "Hello";
        const char *data2 = "World!";
        kvidxInsert(i, 100, 1, 1, data1, strlen(data1));
        kvidxInsert(i, 200, 2, 2, data2, strlen(data2));
        kvidxInsert(i, 300, 3, 3, NULL, 0);
        kvidxCommit(i);

        TEST("LMDB getKeyCount...") {
            uint64_t count;
            kvidxError err = kvidxGetKeyCount(i, &count);
            if (err != KVIDX_OK) {
                ERRR("GetKeyCount failed!");
            }
            if (count != 3) {
                ERR("Expected 3 keys, got %" PRIu64 "", count);
            }
        }

        TEST("LMDB getMinKey...") {
            uint64_t minKey;
            kvidxError err = kvidxGetMinKey(i, &minKey);
            if (err != KVIDX_OK) {
                ERRR("GetMinKey failed!");
            }
            if (minKey != 100) {
                ERR("Expected minKey=100, got %" PRIu64 "", minKey);
            }
        }

        TEST("LMDB getDataSize...") {
            uint64_t dataSize;
            kvidxError err = kvidxGetDataSize(i, &dataSize);
            if (err != KVIDX_OK) {
                ERRR("GetDataSize failed!");
            }
            /* 5 + 6 + 0 = 11 */
            if (dataSize != 11) {
                ERR("Expected dataSize=11, got %" PRIu64 "", dataSize);
            }
        }

        TEST("LMDB getStats...") {
            kvidxStats stats;
            kvidxError err = kvidxGetStats(i, &stats);
            if (err != KVIDX_OK) {
                ERRR("GetStats failed!");
            }
            if (stats.totalKeys != 3) {
                ERR("Expected 3 keys, got %" PRIu64 "", stats.totalKeys);
            }
            if (stats.minKey != 100) {
                ERR("Expected minKey=100, got %" PRIu64 "", stats.minKey);
            }
            if (stats.maxKey != 300) {
                ERR("Expected maxKey=300, got %" PRIu64 "", stats.maxKey);
            }
        }

        kvidxClose(i);
        removeDir(dirname);
    }

    /* ================================================================
     * Range Query Tests
     * ================================================================ */
    {
        kvidxInstance pre = {0};
        kvidxInstance *i = &pre;
        i->interface = kvidxInterfaceLmdb;

        char dirname[64] = {0};
        snprintf(dirname, sizeof(dirname), "test-lmdb-rangeq-%d", getpid());

        printf("\nTesting LMDB range queries in: %s\n", dirname);
        kvidxOpen(i, dirname, NULL);

        /* Insert keys 10, 20, 30, 40, 50 */
        kvidxBegin(i);
        for (uint64_t k = 10; k <= 50; k += 10) {
            kvidxInsert(i, k, k, k, NULL, 0);
        }
        kvidxCommit(i);

        TEST("LMDB countRange...") {
            uint64_t count;

            /* Count 20-40 inclusive */
            kvidxError err = kvidxCountRange(i, 20, 40, &count);
            if (err != KVIDX_OK) {
                ERRR("CountRange failed!");
            }
            if (count != 3) {
                ERR("Expected 3 keys in range, got %" PRIu64 "", count);
            }

            /* Count 15-35 (should be 20, 30 = 2) */
            err = kvidxCountRange(i, 15, 35, &count);
            if (err != KVIDX_OK) {
                ERRR("CountRange failed!");
            }
            if (count != 2) {
                ERR("Expected 2 keys in range, got %" PRIu64 "", count);
            }
        }

        TEST("LMDB existsInRange...") {
            bool exists;

            /* 20-40 should have keys */
            kvidxError err = kvidxExistsInRange(i, 20, 40, &exists);
            if (err != KVIDX_OK) {
                ERRR("ExistsInRange failed!");
            }
            if (!exists) {
                ERRR("Should find keys in 20-40!");
            }

            /* 11-19 should NOT have keys */
            err = kvidxExistsInRange(i, 11, 19, &exists);
            if (err != KVIDX_OK) {
                ERRR("ExistsInRange failed!");
            }
            if (exists) {
                ERRR("Should NOT find keys in 11-19!");
            }
        }

        TEST("LMDB removeRange...") {
            uint64_t deleted;

            /* Remove 20-30 inclusive */
            kvidxError err = kvidxRemoveRange(i, 20, 30, true, true, &deleted);
            if (err != KVIDX_OK) {
                ERRR("RemoveRange failed!");
            }
            if (deleted != 2) {
                ERR("Expected to delete 2 keys, deleted %" PRIu64 "", deleted);
            }

            /* Verify 10, 40, 50 remain */
            if (!kvidxExists(i, 10)) {
                ERRR("Key 10 should exist!");
            }
            if (kvidxExists(i, 20)) {
                ERRR("Key 20 should NOT exist!");
            }
            if (kvidxExists(i, 30)) {
                ERRR("Key 30 should NOT exist!");
            }
            if (!kvidxExists(i, 40)) {
                ERRR("Key 40 should exist!");
            }
            if (!kvidxExists(i, 50)) {
                ERRR("Key 50 should exist!");
            }
        }

        kvidxClose(i);
        removeDir(dirname);
    }

    /* ================================================================
     * Export/Import Tests
     * ================================================================ */
    {
        kvidxInstance pre = {0};
        kvidxInstance *i = &pre;
        i->interface = kvidxInterfaceLmdb;

        char dirname[64] = {0};
        snprintf(dirname, sizeof(dirname), "test-lmdb-export-%d", getpid());
        char exportFile[64] = {0};
        snprintf(exportFile, sizeof(exportFile), "test-lmdb-export-%d.bin",
                 getpid());

        printf("\nTesting LMDB export/import in: %s\n", dirname);
        kvidxOpen(i, dirname, NULL);

        /* Insert test data */
        kvidxBegin(i);
        for (uint64_t k = 1; k <= 5; k++) {
            char buf[32];
            snprintf(buf, sizeof(buf), "Value-%" PRIu64 "", k);
            kvidxInsert(i, k * 100, k, k * 2, buf, strlen(buf));
        }
        kvidxCommit(i);

        TEST("LMDB export binary...") {
            kvidxExportOptions opts = kvidxExportOptionsDefault();
            kvidxError err = kvidxExport(i, exportFile, &opts, NULL, NULL);
            if (err != KVIDX_OK) {
                ERRR("Export failed!");
            }
        }

        kvidxClose(i);

        /* Open a new database and import */
        char dirname2[64] = {0};
        snprintf(dirname2, sizeof(dirname2), "test-lmdb-import-%d", getpid());

        kvidxInstance pre2 = {0};
        kvidxInstance *i2 = &pre2;
        i2->interface = kvidxInterfaceLmdb;
        kvidxOpen(i2, dirname2, NULL);

        TEST("LMDB import binary...") {
            kvidxImportOptions opts = kvidxImportOptionsDefault();
            kvidxError err = kvidxImport(i2, exportFile, &opts, NULL, NULL);
            if (err != KVIDX_OK) {
                ERRR("Import failed!");
            }

            /* Verify data */
            uint64_t count;
            kvidxGetKeyCount(i2, &count);
            if (count != 5) {
                ERR("Expected 5 keys after import, got %" PRIu64 "", count);
            }

            /* Check specific entry */
            uint64_t term, cmd;
            const uint8_t *data;
            size_t len;
            if (!kvidxGet(i2, 300, &term, &cmd, &data, &len)) {
                ERRR("Key 300 not found after import!");
            }
            if (term != 3 || cmd != 6) {
                ERR("Metadata mismatch for key 300: term=%" PRIu64
                    ", cmd=%" PRIu64 "",
                    term, cmd);
            }
            if (len != 7 || memcmp(data, "Value-3", 7) != 0) {
                ERRR("Data mismatch for key 300!");
            }
        }

        kvidxClose(i2);
        removeDir(dirname);
        removeDir(dirname2);
        unlink(exportFile);
    }

    /* ================================================================
     * Edge Cases
     * ================================================================ */
    {
        kvidxInstance pre = {0};
        kvidxInstance *i = &pre;
        i->interface = kvidxInterfaceLmdb;

        char dirname[64] = {0};
        snprintf(dirname, sizeof(dirname), "test-lmdb-edge-%d", getpid());

        printf("\nTesting LMDB edge cases in: %s\n", dirname);
        kvidxOpen(i, dirname, NULL);

        TEST("LMDB large key values...") {
            uint64_t largeKey = UINT64_MAX - 1;
            bool inserted = kvidxInsert(i, largeKey, 1, 1, "test", 4);
            if (!inserted) {
                ERRR("Insert with large key failed!");
            }
            if (!kvidxExists(i, largeKey)) {
                ERRR("Large key not found!");
            }
            kvidxRemove(i, largeKey);
        }

        TEST("LMDB zero-length data...") {
            bool inserted = kvidxInsert(i, 1, 1, 1, NULL, 0);
            if (!inserted) {
                ERRR("Insert with NULL data failed!");
            }

            const uint8_t *data;
            size_t len;
            bool got = kvidxGet(i, 1, NULL, NULL, &data, &len);
            if (!got) {
                ERRR("Get with NULL data failed!");
            }
            if (len != 0) {
                ERR("Expected len=0, got %zu", len);
            }
        }

        TEST("LMDB large data...") {
            size_t largeSize = 64 * 1024; /* 64KB */
            uint8_t *largeData = malloc(largeSize);
            memset(largeData, 0xAB, largeSize);

            bool inserted = kvidxInsert(i, 2, 1, 1, largeData, largeSize);
            if (!inserted) {
                free(largeData);
                ERRR("Insert with large data failed!");
            }

            const uint8_t *gotData;
            size_t gotLen;
            bool got = kvidxGet(i, 2, NULL, NULL, &gotData, &gotLen);
            if (!got) {
                free(largeData);
                ERRR("Get with large data failed!");
            }
            if (gotLen != largeSize) {
                free(largeData);
                ERR("Large data size mismatch: got %zu", gotLen);
            }
            if (memcmp(gotData, largeData, largeSize) != 0) {
                free(largeData);
                ERRR("Large data content mismatch!");
            }
            free(largeData);
        }

        TEST("LMDB empty database operations...") {
            /* Create fresh instance */
            kvidxClose(i);
            removeDir(dirname);

            i->interface = kvidxInterfaceLmdb;
            kvidxOpen(i, dirname, NULL);

            uint64_t maxKey;
            if (kvidxMaxKey(i, &maxKey)) {
                ERRR("MaxKey should fail on empty DB!");
            }

            uint64_t minKey;
            if (kvidxGetMinKey(i, &minKey) != KVIDX_ERROR_NOT_FOUND) {
                ERRR("GetMinKey should return NOT_FOUND on empty DB!");
            }

            uint64_t count;
            kvidxGetKeyCount(i, &count);
            if (count != 0) {
                ERRR("KeyCount should be 0 on empty DB!");
            }
        }

        kvidxClose(i);
        removeDir(dirname);
    }

    /* ================================================================
     * Summary
     * ================================================================ */
    printf("\n=== LMDB Adapter Test Results ===\n");
    if (err == 0) {
        printf("All tests passed!\n");
    } else {
        printf("FAILED: %u tests failed\n", err);
    }

    return err ? 1 : 0;
}
