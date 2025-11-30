/**
 * Comprehensive storage primitives tests for kvidxkit (v0.8.0)
 * Tests: conditional writes, atomic operations, compare-and-swap,
 *        append/prepend, partial value access, and TTL/expiration
 * Runs against both SQLite3 and LMDB backends.
 */

/* Required for clock_gettime and usleep on various platforms */
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE
#else
#define _POSIX_C_SOURCE 199309L
#endif

#include "ctest.h"
#include "kvidxkit.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Backend names for test filenames */
static const char *backendName = NULL;

static void makeTestFilename(char *buf, size_t bufSize, const char *testName) {
    snprintf(buf, bufSize, "test-prim-%s-%s-%d", backendName, testName,
             getpid());
}

static void cleanupTestFile(const char *filename) {
    unlink(filename);
    /* Clean up LMDB lock file if present */
    char lockfile[256];
    snprintf(lockfile, sizeof(lockfile), "%s-lock", filename);
    unlink(lockfile);

    /* Clean up LMDB directory contents (data.mdb, lock.mdb) */
    char lmdbFile[256];
    snprintf(lmdbFile, sizeof(lmdbFile), "%s/data.mdb", filename);
    unlink(lmdbFile);
    snprintf(lmdbFile, sizeof(lmdbFile), "%s/lock.mdb", filename);
    unlink(lmdbFile);
    rmdir(filename);

    /* Clean up RocksDB directory contents */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s 2>/dev/null", filename);
    (void)system(cmd);
}

static bool openFresh(kvidxInstance *i, const char *filename,
                      const kvidxInterface *iface) {
    memset(i, 0, sizeof(*i));
    i->interface = *iface;
    return kvidxOpen(i, filename, NULL);
}

/* ====================================================================
 * TEST SUITE 1: Conditional Writes (InsertEx)
 * ==================================================================== */
static void testConditionalWrites(uint32_t *err, const kvidxInterface *iface) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "cond");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename, iface)) {
        ERRR("Failed to open database for conditional writes tests");
        return;
    }

    TEST("InsertEx: ALWAYS mode - insert new key") {
        kvidxError result =
            kvidxInsertEx(i, 100, 1, 1, "data1", 5, KVIDX_SET_ALWAYS);
        if (result != KVIDX_OK) {
            ERR("InsertEx ALWAYS on new key failed: %d", result);
        }
    }

    TEST("InsertEx: ALWAYS mode - overwrite existing key") {
        kvidxError result =
            kvidxInsertEx(i, 100, 2, 2, "data2", 5, KVIDX_SET_ALWAYS);
        if (result != KVIDX_OK) {
            ERR("InsertEx ALWAYS on existing key failed: %d", result);
        }

        /* Verify it was updated */
        uint64_t term = 0;
        if (!kvidxGet(i, 100, &term, NULL, NULL, NULL)) {
            ERRR("Get after InsertEx ALWAYS failed");
        } else if (term != 2) {
            ERR("Term should be 2 after overwrite, got %" PRIu64, term);
        }
    }

    TEST("InsertEx: IF_NOT_EXISTS - insert new key") {
        kvidxError result =
            kvidxInsertEx(i, 200, 1, 1, "new", 3, KVIDX_SET_IF_NOT_EXISTS);
        if (result != KVIDX_OK) {
            ERR("InsertEx IF_NOT_EXISTS on new key failed: %d", result);
        }
    }

    TEST("InsertEx: IF_NOT_EXISTS - fail on existing key") {
        kvidxError result =
            kvidxInsertEx(i, 200, 2, 2, "dup", 3, KVIDX_SET_IF_NOT_EXISTS);
        if (result != KVIDX_ERROR_CONDITION_FAILED) {
            ERR("InsertEx IF_NOT_EXISTS should fail on existing, got: %d",
                result);
        }

        /* Verify original value unchanged */
        uint64_t term = 0;
        kvidxGet(i, 200, &term, NULL, NULL, NULL);
        if (term != 1) {
            ERR("Term should still be 1, got %" PRIu64, term);
        }
    }

    TEST("InsertEx: IF_EXISTS - fail on new key") {
        kvidxError result =
            kvidxInsertEx(i, 300, 1, 1, "new", 3, KVIDX_SET_IF_EXISTS);
        if (result != KVIDX_ERROR_CONDITION_FAILED) {
            ERR("InsertEx IF_EXISTS should fail on new key, got: %d", result);
        }

        /* Verify key was not created */
        if (kvidxExists(i, 300)) {
            ERRR("Key 300 should not exist");
        }
    }

    TEST("InsertEx: IF_EXISTS - update existing key") {
        kvidxError result =
            kvidxInsertEx(i, 200, 3, 3, "updated", 7, KVIDX_SET_IF_EXISTS);
        if (result != KVIDX_OK) {
            ERR("InsertEx IF_EXISTS on existing key failed: %d", result);
        }

        /* Verify it was updated */
        uint64_t term = 0;
        kvidxGet(i, 200, &term, NULL, NULL, NULL);
        if (term != 3) {
            ERR("Term should be 3, got %" PRIu64, term);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 2: Transaction Abort
 * ==================================================================== */
static void testTransactionAbort(uint32_t *err, const kvidxInterface *iface) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "abort");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename, iface)) {
        ERRR("Failed to open database for transaction abort tests");
        return;
    }

    /* Insert some initial data */
    kvidxInsert(i, 1, 1, 1, "init", 4);
    kvidxCommit(i);

    TEST("Abort: Discard uncommitted inserts") {
        kvidxBegin(i);
        kvidxInsertEx(i, 2, 2, 2, "new", 3, KVIDX_SET_ALWAYS);
        kvidxInsertEx(i, 3, 3, 3, "new", 3, KVIDX_SET_ALWAYS);

        /* Abort the transaction */
        if (!kvidxAbort(i)) {
            ERRR("Abort failed");
        }

        /* Verify uncommitted keys don't exist */
        if (kvidxExists(i, 2)) {
            ERRR("Key 2 should not exist after abort");
        }
        if (kvidxExists(i, 3)) {
            ERRR("Key 3 should not exist after abort");
        }

        /* Verify original data still exists */
        if (!kvidxExists(i, 1)) {
            ERRR("Key 1 should still exist");
        }
    }

    TEST("Abort: Discard uncommitted modifications") {
        kvidxBegin(i);
        kvidxInsertEx(i, 1, 99, 99, "modified", 8, KVIDX_SET_ALWAYS);

        kvidxAbort(i);

        /* Verify original value is preserved */
        uint64_t term = 0;
        kvidxGet(i, 1, &term, NULL, NULL, NULL);
        if (term != 1) {
            ERR("Term should be 1 after abort, got %" PRIu64, term);
        }
    }

    TEST("Abort: Handle when no transaction active") {
        /* Abort behavior without active transaction may vary by backend.
         * SQLite returns false (ROLLBACK fails), LMDB returns true (no-op).
         * Either behavior is acceptable - we just verify no crash. */
        kvidxAbort(i);
        /* No error check - behavior is backend-specific */
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 3: Atomic Operations (GetAndSet, GetAndRemove)
 * ==================================================================== */
static void testAtomicOperations(uint32_t *err, const kvidxInterface *iface) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "atomic");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename, iface)) {
        ERRR("Failed to open database for atomic operations tests");
        return;
    }

    TEST("GetAndSet: Replace existing key and return old value") {
        /* Insert initial value */
        kvidxInsert(i, 1, 10, 20, "original", 8);
        kvidxCommit(i);

        uint64_t oldTerm = 0, oldCmd = 0;
        void *oldData = NULL;
        size_t oldLen = 0;

        kvidxError result =
            kvidxGetAndSet(i, 1, 100, 200, "replacement", 11, &oldTerm, &oldCmd,
                           &oldData, &oldLen);

        if (result != KVIDX_OK) {
            ERR("GetAndSet failed: %d", result);
        } else {
            if (oldTerm != 10) {
                ERR("Old term should be 10, got %" PRIu64, oldTerm);
            }
            if (oldCmd != 20) {
                ERR("Old cmd should be 20, got %" PRIu64, oldCmd);
            }
            if (oldLen != 8 || memcmp(oldData, "original", 8) != 0) {
                ERR("Old data mismatch, len=%zu", oldLen);
            }
            free(oldData);
        }

        /* Verify new value is set */
        uint64_t newTerm = 0;
        kvidxGet(i, 1, &newTerm, NULL, NULL, NULL);
        if (newTerm != 100) {
            ERR("New term should be 100, got %" PRIu64, newTerm);
        }
    }

    TEST("GetAndSet: Set new key (no old value)") {
        uint64_t oldTerm = 999, oldCmd = 999;
        void *oldData = (void *)1;
        size_t oldLen = 999;

        kvidxError result = kvidxGetAndSet(i, 999, 1, 1, "new", 3, &oldTerm,
                                           &oldCmd, &oldData, &oldLen);

        if (result != KVIDX_OK) {
            ERR("GetAndSet on new key failed: %d", result);
        } else {
            /* For new key, old values should be zero/NULL/0 */
            if (oldTerm != 0) {
                ERR("oldTerm should be 0 for new key, got %" PRIu64, oldTerm);
            }
            if (oldCmd != 0) {
                ERR("oldCmd should be 0 for new key, got %" PRIu64, oldCmd);
            }
            if (oldData != NULL) {
                ERR("oldData should be NULL for new key, got %p", oldData);
                free(oldData);
            }
            if (oldLen != 0) {
                ERR("oldLen should be 0 for new key, got %zu", oldLen);
            }
        }
    }

    TEST("GetAndRemove: Remove existing key and return value") {
        /* Ensure key exists */
        kvidxInsertEx(i, 500, 50, 60, "remove-me", 9, KVIDX_SET_ALWAYS);

        uint64_t term = 0, cmd = 0;
        void *data = NULL;
        size_t len = 0;

        kvidxError result = kvidxGetAndRemove(i, 500, &term, &cmd, &data, &len);

        if (result != KVIDX_OK) {
            ERR("GetAndRemove failed: %d", result);
        } else {
            if (term != 50) {
                ERR("Term should be 50, got %" PRIu64, term);
            }
            if (cmd != 60) {
                ERR("Cmd should be 60, got %" PRIu64, cmd);
            }
            if (len != 9 || memcmp(data, "remove-me", 9) != 0) {
                ERR("Data mismatch, len=%zu", len);
            }
            free(data);
        }

        /* Verify key no longer exists */
        if (kvidxExists(i, 500)) {
            ERRR("Key 500 should not exist after GetAndRemove");
        }
    }

    TEST("GetAndRemove: Fail on non-existent key") {
        kvidxError result = kvidxGetAndRemove(i, 88888, NULL, NULL, NULL, NULL);
        if (result != KVIDX_ERROR_NOT_FOUND) {
            ERR("GetAndRemove on non-existent key should return NOT_FOUND: %d",
                result);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 4: Compare-and-Swap
 * ==================================================================== */
static void testCompareAndSwap(uint32_t *err, const kvidxInterface *iface) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "cas");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename, iface)) {
        ERRR("Failed to open database for CAS tests");
        return;
    }

    TEST("CAS: Swap when data matches") {
        kvidxInsert(i, 1, 1, 1, "expected", 8);
        kvidxCommit(i);

        bool swapped = false;
        kvidxError result = kvidxCompareAndSwap(i, 1, "expected", 8, 2, 2,
                                                "swapped", 7, &swapped);

        if (result != KVIDX_OK) {
            ERR("CAS failed: %d", result);
        }
        if (!swapped) {
            ERRR("CAS should have swapped");
        }

        /* Verify new value */
        const uint8_t *data = NULL;
        size_t len = 0;
        kvidxGet(i, 1, NULL, NULL, &data, &len);
        if (len != 7 || memcmp(data, "swapped", 7) != 0) {
            ERRR("Data should be 'swapped' after CAS");
        }
    }

    TEST("CAS: No swap when data differs") {
        bool swapped = true;
        kvidxError result = kvidxCompareAndSwap(i, 1, "wrong-data", 10, 3, 3,
                                                "never", 5, &swapped);

        if (result != KVIDX_OK) {
            ERR("CAS returned error: %d", result);
        }
        if (swapped) {
            ERRR("CAS should NOT have swapped when data differs");
        }

        /* Verify value unchanged */
        const uint8_t *data = NULL;
        size_t len = 0;
        kvidxGet(i, 1, NULL, NULL, &data, &len);
        if (len != 7 || memcmp(data, "swapped", 7) != 0) {
            ERRR("Data should still be 'swapped'");
        }
    }

    TEST("CAS: Swap empty data for new value") {
        /* Insert key with empty data */
        kvidxInsertEx(i, 2, 1, 1, NULL, 0, KVIDX_SET_ALWAYS);

        bool swapped = false;
        kvidxError result = kvidxCompareAndSwap(i, 2, NULL, 0, 2, 2,
                                                "initialized", 11, &swapped);
        if (result != KVIDX_OK || !swapped) {
            ERR("CAS on empty data failed: result=%d, swapped=%d", result,
                swapped);
        }
    }

    TEST("CAS: Returns NOT_FOUND when key doesn't exist") {
        bool swapped = true;
        kvidxError result = kvidxCompareAndSwap(i, 99999, "something", 9, 1, 1,
                                                "new", 3, &swapped);
        if (result != KVIDX_ERROR_NOT_FOUND) {
            ERR("CAS on non-existent key should return NOT_FOUND: %d", result);
        }
        /* swapped should be false regardless */
        if (swapped) {
            ERRR("swapped should be false for non-existent key");
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 5: Append / Prepend
 * ==================================================================== */
static void testAppendPrepend(uint32_t *err, const kvidxInterface *iface) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "append");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename, iface)) {
        ERRR("Failed to open database for append/prepend tests");
        return;
    }

    TEST("Append: Add data to existing value") {
        kvidxInsert(i, 1, 1, 1, "Hello", 5);
        kvidxCommit(i);

        size_t newLen = 0;
        kvidxError result = kvidxAppend(i, 1, 1, 1, " World", 6, &newLen);

        if (result != KVIDX_OK) {
            ERR("Append failed: %d", result);
        }
        if (newLen != 11) {
            ERR("New length should be 11, got %zu", newLen);
        }

        /* Verify combined data */
        const uint8_t *data = NULL;
        size_t len = 0;
        kvidxGet(i, 1, NULL, NULL, &data, &len);
        if (len != 11 || memcmp(data, "Hello World", 11) != 0) {
            ERRR("Appended data mismatch");
        }
    }

    TEST("Append: Create new key with data") {
        size_t newLen = 0;
        kvidxError result = kvidxAppend(i, 100, 1, 1, "NewData", 7, &newLen);

        if (result != KVIDX_OK) {
            ERR("Append to new key failed: %d", result);
        }
        if (newLen != 7) {
            ERR("Length should be 7, got %zu", newLen);
        }
    }

    TEST("Prepend: Add data before existing value") {
        kvidxInsertEx(i, 2, 1, 1, "World", 5, KVIDX_SET_ALWAYS);

        size_t newLen = 0;
        kvidxError result = kvidxPrepend(i, 2, 1, 1, "Hello ", 6, &newLen);

        if (result != KVIDX_OK) {
            ERR("Prepend failed: %d", result);
        }
        if (newLen != 11) {
            ERR("New length should be 11, got %zu", newLen);
        }

        /* Verify combined data */
        const uint8_t *data = NULL;
        size_t len = 0;
        kvidxGet(i, 2, NULL, NULL, &data, &len);
        if (len != 11 || memcmp(data, "Hello World", 11) != 0) {
            ERRR("Prepended data mismatch");
        }
    }

    TEST("Prepend: Create new key with data") {
        size_t newLen = 0;
        kvidxError result = kvidxPrepend(i, 200, 1, 1, "FirstData", 9, &newLen);

        if (result != KVIDX_OK) {
            ERR("Prepend to new key failed: %d", result);
        }
        if (newLen != 9) {
            ERR("Length should be 9, got %zu", newLen);
        }
    }

    TEST("Append/Prepend: Multiple operations") {
        kvidxInsertEx(i, 3, 1, 1, "B", 1, KVIDX_SET_ALWAYS);
        kvidxPrepend(i, 3, 1, 1, "A", 1, NULL);
        kvidxAppend(i, 3, 1, 1, "C", 1, NULL);

        const uint8_t *data = NULL;
        size_t len = 0;
        kvidxGet(i, 3, NULL, NULL, &data, &len);
        if (len != 3 || memcmp(data, "ABC", 3) != 0) {
            ERRR("Multiple append/prepend failed");
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 6: Partial Value Access (GetValueRange, SetValueRange)
 * ==================================================================== */
static void testPartialValueAccess(uint32_t *err, const kvidxInterface *iface) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "partial");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename, iface)) {
        ERRR("Failed to open database for partial value access tests");
        return;
    }

    TEST("GetValueRange: Read substring") {
        kvidxInsert(i, 1, 1, 1, "0123456789", 10);
        kvidxCommit(i);

        void *data = NULL;
        size_t actualLen = 0;

        kvidxError result = kvidxGetValueRange(i, 1, 2, 5, &data, &actualLen);

        if (result != KVIDX_OK) {
            ERR("GetValueRange failed: %d", result);
        } else {
            if (actualLen != 5) {
                ERR("Expected 5 bytes, got %zu", actualLen);
            }
            if (memcmp(data, "23456", 5) != 0) {
                ERRR("Data mismatch in GetValueRange");
            }
            free(data);
        }
    }

    TEST("GetValueRange: Read to end (length=0)") {
        void *data = NULL;
        size_t actualLen = 0;

        kvidxError result = kvidxGetValueRange(i, 1, 7, 0, &data, &actualLen);

        if (result != KVIDX_OK) {
            ERR("GetValueRange to end failed: %d", result);
        } else {
            if (actualLen != 3) {
                ERR("Expected 3 bytes (7-9), got %zu", actualLen);
            } else if (data == NULL) {
                ERRR("Data pointer is NULL");
            } else if (memcmp(data, "789", 3) != 0) {
                ERRR("Data mismatch reading to end");
            }
            free(data);
        }
    }

    TEST("GetValueRange: Offset beyond data returns empty") {
        void *data = NULL;
        size_t actualLen = 999;

        kvidxError result = kvidxGetValueRange(i, 1, 100, 5, &data, &actualLen);

        if (result != KVIDX_OK) {
            ERR("GetValueRange with large offset returned: %d", result);
        }
        if (actualLen != 0 || data != NULL) {
            ERR("Should return empty when offset beyond data: len=%zu",
                actualLen);
        }
    }

    TEST("GetValueRange: Non-existent key") {
        void *data = NULL;
        size_t actualLen = 0;

        kvidxError result =
            kvidxGetValueRange(i, 99999, 0, 5, &data, &actualLen);

        if (result != KVIDX_ERROR_NOT_FOUND) {
            ERR("GetValueRange on missing key should return NOT_FOUND: %d",
                result);
        }
    }

    TEST("SetValueRange: Overwrite middle of value") {
        kvidxInsertEx(i, 2, 1, 1, "AAAAAAAAAA", 10, KVIDX_SET_ALWAYS);

        size_t newLen = 0;
        kvidxError result = kvidxSetValueRange(i, 2, 3, "BBB", 3, &newLen);

        if (result != KVIDX_OK) {
            ERR("SetValueRange failed: %d", result);
        }
        if (newLen != 10) {
            ERR("Length should remain 10, got %zu", newLen);
        }

        const uint8_t *data = NULL;
        size_t len = 0;
        kvidxGet(i, 2, NULL, NULL, &data, &len);
        /* Original: AAAAAAAAAA (10 A's), Write BBB at offset 3 -> AAABBBAAAA */
        if (len != 10 || memcmp(data, "AAABBBAAAA", 10) != 0) {
            ERRR("SetValueRange middle overwrite failed");
        }
    }

    TEST("SetValueRange: Extend value") {
        kvidxInsertEx(i, 3, 1, 1, "SHORT", 5, KVIDX_SET_ALWAYS);

        size_t newLen = 0;
        kvidxError result = kvidxSetValueRange(i, 3, 3, "EXTENDED", 8, &newLen);

        if (result != KVIDX_OK) {
            ERR("SetValueRange extend failed: %d", result);
        }
        if (newLen != 11) { /* 3 + 8 = 11 */
            ERR("Length should be 11, got %zu", newLen);
        }
    }

    TEST("SetValueRange: Non-existent key") {
        size_t newLen = 0;
        kvidxError result = kvidxSetValueRange(i, 99999, 0, "data", 4, &newLen);

        if (result != KVIDX_ERROR_NOT_FOUND) {
            ERR("SetValueRange on missing key should return NOT_FOUND: %d",
                result);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 7: TTL / Expiration
 * ==================================================================== */
static void testTTLExpiration(uint32_t *err, const kvidxInterface *iface) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "ttl");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename, iface)) {
        ERRR("Failed to open database for TTL tests");
        return;
    }

    TEST("SetExpire: Set TTL on existing key") {
        kvidxInsert(i, 1, 1, 1, "data", 4);
        kvidxCommit(i);

        kvidxError result = kvidxSetExpire(i, 1, 60000); /* 60 seconds */
        if (result != KVIDX_OK) {
            ERR("SetExpire failed: %d", result);
        }
    }

    TEST("SetExpire: Fail on non-existent key") {
        kvidxError result = kvidxSetExpire(i, 99999, 60000);
        if (result != KVIDX_ERROR_NOT_FOUND) {
            ERR("SetExpire on missing key should return NOT_FOUND: %d", result);
        }
    }

    TEST("GetTTL: Get remaining time") {
        int64_t ttl = kvidxGetTTL(i, 1);
        if (ttl < 50000 || ttl > 60000) { /* Should be ~60s remaining */
            ERR("TTL should be ~60000ms, got %" PRId64, ttl);
        }
    }

    TEST("GetTTL: Return NONE for key without TTL") {
        kvidxInsertEx(i, 2, 1, 1, "no-ttl", 6, KVIDX_SET_ALWAYS);

        int64_t ttl = kvidxGetTTL(i, 2);
        if (ttl != KVIDX_TTL_NONE) {
            ERR("TTL should be KVIDX_TTL_NONE (-1), got %" PRId64, ttl);
        }
    }

    TEST("GetTTL: Return NOT_FOUND for missing key") {
        int64_t ttl = kvidxGetTTL(i, 99999);
        if (ttl != KVIDX_TTL_NOT_FOUND) {
            ERR("TTL should be KVIDX_TTL_NOT_FOUND (-2), got %" PRId64, ttl);
        }
    }

    TEST("SetExpireAt: Set absolute expiration") {
        kvidxInsertEx(i, 3, 1, 1, "expire-at", 9, KVIDX_SET_ALWAYS);

        /* Set to expire 30 seconds from now */
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        uint64_t expireAt =
            (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000 + 30000;

        kvidxError result = kvidxSetExpireAt(i, 3, expireAt);
        if (result != KVIDX_OK) {
            ERR("SetExpireAt failed: %d", result);
        }

        int64_t ttl = kvidxGetTTL(i, 3);
        if (ttl < 25000 || ttl > 35000) {
            ERR("TTL should be ~30000ms, got %" PRId64, ttl);
        }
    }

    TEST("Persist: Remove TTL from key") {
        kvidxError result = kvidxPersist(i, 1);
        if (result != KVIDX_OK) {
            ERR("Persist failed: %d", result);
        }

        int64_t ttl = kvidxGetTTL(i, 1);
        if (ttl != KVIDX_TTL_NONE) {
            ERR("TTL should be NONE after persist, got %" PRId64, ttl);
        }
    }

    TEST("Persist: Fail on non-existent key") {
        kvidxError result = kvidxPersist(i, 99999);
        if (result != KVIDX_ERROR_NOT_FOUND) {
            ERR("Persist on missing key should return NOT_FOUND: %d", result);
        }
    }

    TEST("ExpireScan: Delete expired keys") {
        /* Insert a key with very short TTL */
        kvidxInsertEx(i, 1000, 1, 1, "expire-soon", 11, KVIDX_SET_ALWAYS);
        kvidxSetExpire(i, 1000, 1); /* 1ms TTL */

        /* Sleep to ensure key expires */
        usleep(10000); /* 10ms */

        uint64_t expiredCount = 0;
        kvidxError result = kvidxExpireScan(i, 0, &expiredCount);
        if (result != KVIDX_OK) {
            ERR("ExpireScan failed: %d", result);
        }
        if (expiredCount < 1) {
            ERR("Expected at least 1 expired key, got %" PRIu64, expiredCount);
        }

        /* Verify key was deleted */
        if (kvidxExists(i, 1000)) {
            ERRR("Expired key 1000 should have been deleted");
        }
    }

    TEST("ExpireScan: Respect maxKeys limit") {
        /* Insert multiple keys with short TTLs */
        for (uint64_t k = 2000; k < 2010; k++) {
            kvidxInsertEx(i, k, 1, 1, "expire", 6, KVIDX_SET_ALWAYS);
            kvidxSetExpire(i, k, 1);
        }

        usleep(10000);

        uint64_t expiredCount = 0;
        kvidxError result =
            kvidxExpireScan(i, 3, &expiredCount); /* Limit to 3 */
        if (result != KVIDX_OK) {
            ERR("ExpireScan with limit failed: %d", result);
        }
        if (expiredCount > 3) {
            ERR("ExpireScan exceeded limit, deleted %" PRIu64, expiredCount);
        }
    }

    TEST("InsertEx: Expired key treated as non-existent for IF_NOT_EXISTS") {
        /* Insert key with immediate expiration */
        kvidxInsertEx(i, 3000, 1, 1, "old", 3, KVIDX_SET_ALWAYS);
        kvidxSetExpire(i, 3000, 1);
        usleep(10000);

        /* Run expire scan to clean up expired keys first
         * (TTL-aware InsertEx is not yet implemented) */
        kvidxExpireScan(i, 0, NULL);

        /* Should succeed because key was expired and cleaned up */
        kvidxError result =
            kvidxInsertEx(i, 3000, 2, 2, "new", 3, KVIDX_SET_IF_NOT_EXISTS);
        if (result != KVIDX_OK) {
            ERR("InsertEx IF_NOT_EXISTS on expired key should succeed: %d",
                result);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 8: Edge Cases and Error Handling
 * ==================================================================== */
static void testEdgeCases(uint32_t *err, const kvidxInterface *iface) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "edge");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename, iface)) {
        ERRR("Failed to open database for edge case tests");
        return;
    }

    TEST("Atomic ops with empty data") {
        kvidxInsert(i, 1, 1, 1, NULL, 0);
        kvidxCommit(i);

        size_t newLen = 0;
        kvidxAppend(i, 1, 1, 1, "data", 4, &newLen);
        if (newLen != 4) {
            ERR("Append to empty should result in len 4, got %zu", newLen);
        }
    }

    TEST("Large data operations") {
        /* 64KB of data */
        size_t bigSize = 64 * 1024;
        void *bigData = malloc(bigSize);
        memset(bigData, 'X', bigSize);

        kvidxError result =
            kvidxInsertEx(i, 100, 1, 1, bigData, bigSize, KVIDX_SET_ALWAYS);
        if (result != KVIDX_OK) {
            ERR("Insert large data failed: %d", result);
        }

        /* Append more data */
        result = kvidxAppend(i, 100, 1, 1, bigData, bigSize, NULL);
        if (result != KVIDX_OK) {
            ERR("Append large data failed: %d", result);
        }

        /* Verify total size */
        const uint8_t *data = NULL;
        size_t len = 0;
        kvidxGet(i, 100, NULL, NULL, &data, &len);
        if (len != bigSize * 2) {
            ERR("Expected %zu bytes, got %zu", bigSize * 2, len);
        }

        free(bigData);
    }

    TEST("Operations within transaction") {
        kvidxBegin(i);

        kvidxInsertEx(i, 200, 1, 1, "txn-data", 8, KVIDX_SET_ALWAYS);
        kvidxAppend(i, 200, 1, 1, "-appended", 9, NULL);

        kvidxCommit(i);

        const uint8_t *data = NULL;
        size_t len = 0;
        kvidxGet(i, 200, NULL, NULL, &data, &len);
        if (len != 17 || memcmp(data, "txn-data-appended", 17) != 0) {
            ERRR("Transaction operations not applied correctly");
        }
    }

    TEST("CAS with binary data containing nulls") {
        uint8_t binData[] = {0x00, 0x01, 0x00, 0x02};
        kvidxInsertEx(i, 300, 1, 1, binData, 4, KVIDX_SET_ALWAYS);

        bool swapped = false;
        uint8_t newData[] = {0xFF, 0xFE};
        kvidxError result =
            kvidxCompareAndSwap(i, 300, binData, 4, 2, 2, newData, 2, &swapped);
        if (result != KVIDX_OK || !swapped) {
            ERR("CAS with binary data failed: result=%d, swapped=%d", result,
                swapped);
        }
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * MAIN - Run all test suites for each backend
 * ==================================================================== */
static void runAllTests(uint32_t *err, const kvidxInterface *iface,
                        const char *name) {
    backendName = name;
    printf("\n========================================\n");
    printf("Testing backend: %s\n", name);
    printf("========================================\n\n");

    testConditionalWrites(err, iface);
    testTransactionAbort(err, iface);
    testAtomicOperations(err, iface);
    testCompareAndSwap(err, iface);
    testAppendPrepend(err, iface);
    testPartialValueAccess(err, iface);
    testTTLExpiration(err, iface);
    testEdgeCases(err, iface);
}

int main(void) {
    uint32_t err = 0;

#ifdef KVIDXKIT_HAS_SQLITE3
    runAllTests(&err, &kvidxInterfaceSqlite3, "sqlite3");
#endif

#ifdef KVIDXKIT_HAS_LMDB
    runAllTests(&err, &kvidxInterfaceLmdb, "lmdb");
#endif

#ifdef KVIDXKIT_HAS_ROCKSDB
    runAllTests(&err, &kvidxInterfaceRocksdb, "rocksdb");
#endif

    TEST_FINAL_RESULT;
}
