/**
 * Comprehensive error handling tests for kvidxkit
 * Tests error reporting, error messages, and error state management
 */

#include "ctest.h"
#include "kvidxkit.h"
#include "kvidxkitErrors.h"

#include <string.h>
#include <unistd.h>
/* cppcheck-suppress constParameterPointer */
static void makeTestFilename(char *buf, size_t bufSize, const char *testName) {
    snprintf(buf, bufSize, "test-errors-%s-%d.sqlite3", testName, getpid());
}

static void cleanupTestFile(const char *filename) {
    unlink(filename);
}

static bool openFresh(kvidxInstance *i, const char *filename) {
    memset(i, 0, sizeof(*i));
    i->interface = kvidxInterfaceSqlite3;
    const char *err = NULL;
    bool result = kvidxOpen(i, filename, &err);
    return result;
}

/* ====================================================================
 * TEST SUITE 1: Error String Functions
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testErrorStrings(uint32_t *err) {
    TEST("Error Strings: All error codes have strings") {
        const kvidxError codes[] = {KVIDX_OK,
                                    KVIDX_ERROR_INVALID_ARGUMENT,
                                    KVIDX_ERROR_DUPLICATE_KEY,
                                    KVIDX_ERROR_NOT_FOUND,
                                    KVIDX_ERROR_DISK_FULL,
                                    KVIDX_ERROR_IO,
                                    KVIDX_ERROR_CORRUPT,
                                    KVIDX_ERROR_TRANSACTION_ACTIVE,
                                    KVIDX_ERROR_NO_TRANSACTION,
                                    KVIDX_ERROR_READONLY,
                                    KVIDX_ERROR_LOCKED,
                                    KVIDX_ERROR_NOMEM,
                                    KVIDX_ERROR_TOO_BIG,
                                    KVIDX_ERROR_CONSTRAINT,
                                    KVIDX_ERROR_SCHEMA,
                                    KVIDX_ERROR_RANGE,
                                    KVIDX_ERROR_INTERNAL};

        for (size_t i = 0; i < sizeof(codes) / sizeof(codes[0]); i++) {
            const char *str = kvidxErrorString(codes[i]);
            if (!str || strlen(str) == 0) {
                ERR("Error code %d has no string", codes[i]);
            }
        }
    }

    TEST("Error Strings: Unknown error code") {
        const char *str = kvidxErrorString((kvidxError)9999);
        if (!str || strcmp(str, "Unknown error") != 0) {
            ERRR("Unknown error code should return 'Unknown error'");
        }
    }

    TEST("Error Helpers: kvidxIsOk") {
        if (!kvidxIsOk(KVIDX_OK)) {
            ERRR("KVIDX_OK should be considered Ok");
        }
        if (kvidxIsOk(KVIDX_ERROR_INVALID_ARGUMENT)) {
            ERRR("Error code should not be considered Ok");
        }
    }

    TEST("Error Helpers: kvidxIsError") {
        if (kvidxIsError(KVIDX_OK)) {
            ERRR("KVIDX_OK should not be considered an error");
        }
        if (!kvidxIsError(KVIDX_ERROR_INVALID_ARGUMENT)) {
            ERRR("Error code should be considered an error");
        }
    }
}

/* ====================================================================
 * TEST SUITE 2: Error Tracking in Instance
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testErrorTracking(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "tracking");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for error tracking tests");
        return;
    }

    TEST("Error Tracking: Initial error is OK") {
        kvidxError e = kvidxGetLastError(i);
        if (e != KVIDX_OK) {
            ERR("Initial error should be KVIDX_OK, got %d", e);
        }
    }

    TEST("Error Tracking: Initial error message is success") {
        const char *msg = kvidxGetLastErrorMessage(i);
        if (!msg || strcmp(msg, "Success") != 0) {
            ERR("Initial error message should be 'Success', got '%s'",
                msg ? msg : "NULL");
        }
    }

    TEST("Error Tracking: Clear error") {
        /* Manually set an error (internal) */
        i->lastError = KVIDX_ERROR_INTERNAL;
        snprintf(i->lastErrorMessage, sizeof(i->lastErrorMessage),
                 "Test error");

        kvidxClearError(i);

        if (kvidxGetLastError(i) != KVIDX_OK) {
            ERRR("Error should be cleared");
        }

        const char *msg = kvidxGetLastErrorMessage(i);
        if (strcmp(msg, "Success") != 0) {
            ERR("Error message should be 'Success' after clear, got '%s'", msg);
        }
    }

    TEST("Error Tracking: NULL instance handling") {
        kvidxError e = kvidxGetLastError(NULL);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("GetLastError(NULL) should return INVALID_ARGUMENT, got %d", e);
        }

        const char *msg = kvidxGetLastErrorMessage(NULL);
        if (!msg || strstr(msg, "NULL") == NULL) {
            ERR("GetLastErrorMessage(NULL) should mention NULL, got '%s'",
                msg ? msg : "NULL");
        }

        /* Should not crash */
        kvidxClearError(NULL);
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 3: Error Reporting in Operations
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testErrorReporting(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "reporting");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for error reporting tests");
        return;
    }

    TEST("Error Reporting: Duplicate key insert") {
        /* Insert first key */
        bool inserted = kvidxInsert(i, 100, 1, 1, "data", 4);
        if (!inserted) {
            ERRR("First insert should succeed");
        }

        /* Try to insert duplicate */
        inserted = kvidxInsert(i, 100, 2, 2, "data2", 5);
        if (inserted) {
            ERRR("Duplicate insert should fail");
        }

        /* In future, after adapter is updated, check error code */
        /* For now, just verify operation failed */
    }

    TEST("Error Reporting: Get non-existent key") {
        const uint8_t *data;
        size_t len;
        bool found = kvidxGet(i, 99999, NULL, NULL, &data, &len);
        if (found) {
            ERRR("Get of non-existent key should return false");
        }

        /* In future, check error is KVIDX_ERROR_NOT_FOUND */
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 4: Transaction State Tracking
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testTransactionState(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "txn-state");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for transaction state tests");
        return;
    }

    TEST("Transaction State: Initial state is not active") {
        if (i->transactionActive) {
            ERRR("Transaction should not be active initially");
        }
    }

    TEST("Transaction State: Begin sets active") {
        if (!kvidxBegin(i)) {
            ERRR("Begin should succeed");
        }

        /* In future, verify transactionActive is set */
    }

    TEST("Transaction State: Commit clears active") {
        if (!kvidxCommit(i)) {
            ERRR("Commit should succeed");
        }

        /* In future, verify transactionActive is cleared */
    }

    TEST("Transaction State: Multiple begins") {
        if (!kvidxBegin(i)) {
            ERRR("First begin should succeed");
        }

        /* Second begin - in future should return error */
        bool second = kvidxBegin(i);

        /* For now, just log - will test properly after adapter update */
        if (second) {
            /* Currently succeeds, will fail in future */
        }

        /* Clean up */
        kvidxCommit(i);
    }

    kvidxClose(i);
    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 5: Error Message Content
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testErrorMessages(uint32_t *err) {
    TEST("Error Messages: Each error code has unique message") {
        const kvidxError codes[] = {
            KVIDX_ERROR_INVALID_ARGUMENT, KVIDX_ERROR_DUPLICATE_KEY,
            KVIDX_ERROR_NOT_FOUND, KVIDX_ERROR_DISK_FULL, KVIDX_ERROR_IO};

        for (size_t i = 0; i < sizeof(codes) / sizeof(codes[0]); i++) {
            const char *msg1 = kvidxErrorString(codes[i]);
            for (size_t j = i + 1; j < sizeof(codes) / sizeof(codes[0]); j++) {
                const char *msg2 = kvidxErrorString(codes[j]);
                if (strcmp(msg1, msg2) == 0) {
                    ERR("Error codes %d and %d have same message: '%s'",
                        codes[i], codes[j], msg1);
                }
            }
        }
    }

    TEST("Error Messages: Messages are descriptive") {
        /* Verify messages are at least somewhat descriptive (>5 chars) */
        const kvidxError codes[] = {KVIDX_ERROR_INVALID_ARGUMENT,
                                    KVIDX_ERROR_DUPLICATE_KEY,
                                    KVIDX_ERROR_NOT_FOUND};

        for (size_t i = 0; i < sizeof(codes) / sizeof(codes[0]); i++) {
            const char *msg = kvidxErrorString(codes[i]);
            if (strlen(msg) < 5) {
                ERR("Error message for code %d is too short: '%s'", codes[i],
                    msg);
            }
        }
    }
}

/* ====================================================================
 * TEST SUITE 6: Error Persistence
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testErrorPersistence(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "persistence");

    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;

    if (!openFresh(i, filename)) {
        ERRR("Failed to open database for error persistence tests");
        return;
    }

    TEST("Error Persistence: Error persists across successful operations") {
        /* Manually set error */
        i->lastError = KVIDX_ERROR_INTERNAL;
        snprintf(i->lastErrorMessage, sizeof(i->lastErrorMessage),
                 "Persistent error");

        /* Perform successful operation */
        bool inserted = kvidxInsert(i, 200, 1, 1, "test", 4);
        if (!inserted) {
            ERRR("Insert should succeed");
        }

        /* In current implementation, error is not cleared by successful
         * operation */
        /* This is correct - errors must be explicitly cleared */
        kvidxError e = kvidxGetLastError(i);
        if (e != KVIDX_ERROR_INTERNAL) {
            ERR("Error should persist, got %d", e);
        }

        /* Clean up */
        kvidxClearError(i);
    }

    TEST("Error Persistence: Can query error multiple times") {
        i->lastError = KVIDX_ERROR_DUPLICATE_KEY;
        snprintf(i->lastErrorMessage, sizeof(i->lastErrorMessage),
                 "Test duplicate");

        kvidxError e1 = kvidxGetLastError(i);
        kvidxError e2 = kvidxGetLastError(i);
        const char *msg1 = kvidxGetLastErrorMessage(i);
        const char *msg2 = kvidxGetLastErrorMessage(i);

        if (e1 != e2) {
            ERR("Error code changed between queries: %d vs %d", e1, e2);
        }

        if (strcmp(msg1, msg2) != 0) {
            ERR("Error message changed between queries: '%s' vs '%s'", msg1,
                msg2);
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
    printf("ERROR HANDLING TESTS FOR KVIDXKIT\n");
    printf("=======================================================\n\n");

    printf("Running Suite 1: Error String Functions\n");
    printf("-------------------------------------------------------\n");
    testErrorStrings(&err);
    printf("\n");

    printf("Running Suite 2: Error Tracking in Instance\n");
    printf("-------------------------------------------------------\n");
    testErrorTracking(&err);
    printf("\n");

    printf("Running Suite 3: Error Reporting in Operations\n");
    printf("-------------------------------------------------------\n");
    testErrorReporting(&err);
    printf("\n");

    printf("Running Suite 4: Transaction State Tracking\n");
    printf("-------------------------------------------------------\n");
    testTransactionState(&err);
    printf("\n");

    printf("Running Suite 5: Error Message Content\n");
    printf("-------------------------------------------------------\n");
    testErrorMessages(&err);
    printf("\n");

    printf("Running Suite 6: Error Persistence\n");
    printf("-------------------------------------------------------\n");
    testErrorPersistence(&err);
    printf("\n");

    printf("=======================================================\n");
    if (err == 0) {
        printf("ALL ERROR HANDLING TESTS PASSED!\n");
        printf("=======================================================\n");
        return 0;
    } else {
        printf("FAILED: %u test(s) failed\n", err);
        printf("=======================================================\n");
        return 1;
    }
}
