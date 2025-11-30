/**
 * Comprehensive configuration tests for kvidxkit
 * Tests configuration API, defaults, and SQLite PRAGMA application
 */

#include "ctest.h"
#include "kvidxkit.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
/* cppcheck-suppress constParameterPointer */
static void makeTestFilename(char *buf, size_t bufSize, const char *testName) {
    snprintf(buf, bufSize, "test-config-%s-%d.sqlite3", testName, getpid());
}

static void cleanupTestFile(const char *filename) {
    unlink(filename);
}

/* ====================================================================
 * TEST SUITE 1: Default Configuration
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testDefaultConfiguration(uint32_t *err) {
    TEST("Default Configuration: Get default config values") {
        kvidxConfig config = kvidxConfigDefault();

        if (config.cacheSizeBytes != 32 * 1024 * 1024) {
            ERR("Expected 32MB cache, got %zu bytes", config.cacheSizeBytes);
        }

        if (config.journalMode != KVIDX_JOURNAL_WAL) {
            ERR("Expected WAL journal mode, got %d", config.journalMode);
        }

        if (config.syncMode != KVIDX_SYNC_NORMAL) {
            ERR("Expected NORMAL sync mode, got %d", config.syncMode);
        }

        if (!config.enableRecursiveTriggers) {
            ERRR("Expected recursive triggers enabled");
        }

        if (config.enableForeignKeys) {
            ERRR("Expected foreign keys disabled");
        }

        if (config.readOnly) {
            ERRR("Expected read-only disabled");
        }

        if (config.busyTimeoutMs != 5000) {
            ERR("Expected 5000ms busy timeout, got %d", config.busyTimeoutMs);
        }

        if (config.mmapSizeBytes != 0) {
            ERR("Expected mmap disabled, got %d", config.mmapSizeBytes);
        }

        if (config.pageSize != 0) {
            ERR("Expected default page size, got %d", config.pageSize);
        }
    }
}

/* ====================================================================
 * TEST SUITE 2: Opening with Configuration
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testOpenWithConfig(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "open");

    TEST("Open with Config: Open with NULL config (use defaults)") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        bool opened = kvidxOpenWithConfig(i, filename, NULL, &errMsg);

        if (!opened) {
            ERR("Failed to open with NULL config: %s",
                errMsg ? errMsg : "unknown");
        } else {
            /* Verify config was initialized */
            kvidxConfig config;
            kvidxError e = kvidxGetConfig(i, &config);
            if (e != KVIDX_OK) {
                ERR("Failed to get config: %d", e);
            }

            if (config.journalMode != KVIDX_JOURNAL_WAL) {
                ERR("Expected WAL journal mode, got %d", config.journalMode);
            }

            kvidxClose(i);
        }
    }

    cleanupTestFile(filename);
    makeTestFilename(filename, sizeof(filename), "custom");

    TEST("Open with Config: Open with custom configuration") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        kvidxConfig config = kvidxConfigDefault();
        config.cacheSizeBytes = 64 * 1024 * 1024; /* 64 MB */
        config.journalMode = KVIDX_JOURNAL_WAL;
        config.syncMode = KVIDX_SYNC_FULL;
        config.busyTimeoutMs = 10000;

        const char *errMsg = NULL;
        bool opened = kvidxOpenWithConfig(i, filename, &config, &errMsg);

        if (!opened) {
            ERR("Failed to open with custom config: %s",
                errMsg ? errMsg : "unknown");
        } else {
            /* Verify config was stored */
            kvidxConfig storedConfig;
            kvidxError e = kvidxGetConfig(i, &storedConfig);
            if (e != KVIDX_OK) {
                ERR("Failed to get config: %d", e);
            }

            if (storedConfig.cacheSizeBytes != config.cacheSizeBytes) {
                ERR("Cache size mismatch: expected %zu, got %zu",
                    config.cacheSizeBytes, storedConfig.cacheSizeBytes);
            }

            if (storedConfig.syncMode != config.syncMode) {
                ERR("Sync mode mismatch: expected %d, got %d", config.syncMode,
                    storedConfig.syncMode);
            }

            kvidxClose(i);
        }
    }

    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 3: Journal Modes
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testJournalModes(uint32_t *err) {
    char filename[128];
    kvidxJournalMode modes[] = {KVIDX_JOURNAL_DELETE,  KVIDX_JOURNAL_TRUNCATE,
                                KVIDX_JOURNAL_PERSIST, KVIDX_JOURNAL_MEMORY,
                                KVIDX_JOURNAL_WAL,     KVIDX_JOURNAL_OFF};
    const char *modeNames[] = {"DELETE", "TRUNCATE", "PERSIST",
                               "MEMORY", "WAL",      "OFF"};

    for (size_t m = 0; m < sizeof(modes) / sizeof(modes[0]); m++) {
        TEST_DESC("Journal Modes: Open with %s mode", modeNames[m]) {
            snprintf(filename, sizeof(filename),
                     "test-config-journal-%s-%d.sqlite3", modeNames[m],
                     getpid());

            kvidxInstance inst = {0};
            kvidxInstance *i = &inst;
            i->interface = kvidxInterfaceSqlite3;

            kvidxConfig config = kvidxConfigDefault();
            config.journalMode = modes[m];

            const char *errMsg = NULL;
            bool opened = kvidxOpenWithConfig(i, filename, &config, &errMsg);

            if (!opened) {
                ERR("Failed to open with %s journal mode: %s", modeNames[m],
                    errMsg ? errMsg : "unknown");
            } else {
                /* Try basic operations */
                bool inserted = kvidxInsert(i, 1, 1, 1, "test", 4);
                if (!inserted) {
                    ERR("Failed to insert with %s journal mode", modeNames[m]);
                }

                bool exists = kvidxExists(i, 1);
                if (!exists) {
                    ERR("Key not found after insert with %s journal mode",
                        modeNames[m]);
                }

                kvidxClose(i);
            }

            cleanupTestFile(filename);
        }
    }
}

/* ====================================================================
 * TEST SUITE 4: Sync Modes
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testSyncModes(uint32_t *err) {
    char filename[128];
    kvidxSyncMode modes[] = {KVIDX_SYNC_OFF, KVIDX_SYNC_NORMAL, KVIDX_SYNC_FULL,
                             KVIDX_SYNC_EXTRA};
    const char *modeNames[] = {"OFF", "NORMAL", "FULL", "EXTRA"};

    for (size_t m = 0; m < sizeof(modes) / sizeof(modes[0]); m++) {
        TEST_DESC("Sync Modes: Open with %s mode", modeNames[m]) {
            snprintf(filename, sizeof(filename),
                     "test-config-sync-%s-%d.sqlite3", modeNames[m], getpid());

            kvidxInstance inst = {0};
            kvidxInstance *i = &inst;
            i->interface = kvidxInterfaceSqlite3;

            kvidxConfig config = kvidxConfigDefault();
            config.syncMode = modes[m];

            const char *errMsg = NULL;
            bool opened = kvidxOpenWithConfig(i, filename, &config, &errMsg);

            if (!opened) {
                ERR("Failed to open with %s sync mode: %s", modeNames[m],
                    errMsg ? errMsg : "unknown");
            } else {
                /* Try basic operations */
                bool inserted = kvidxInsert(i, 100 + m, 1, 1, "sync", 4);
                if (!inserted) {
                    ERR("Failed to insert with %s sync mode", modeNames[m]);
                }

                kvidxClose(i);
            }

            cleanupTestFile(filename);
        }
    }
}

/* ====================================================================
 * TEST SUITE 5: Cache Size Configuration
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testCacheSizes(uint32_t *err) {
    char filename[128];
    size_t cacheSizes[] = {
        1 * 1024 * 1024,  /* 1 MB */
        16 * 1024 * 1024, /* 16 MB */
        64 * 1024 * 1024, /* 64 MB */
        128 * 1024 * 1024 /* 128 MB */
    };

    for (size_t s = 0; s < sizeof(cacheSizes) / sizeof(cacheSizes[0]); s++) {
        TEST_DESC("Cache Size: Open with %zu MB cache",
                  cacheSizes[s] / (1024 * 1024)) {
            snprintf(filename, sizeof(filename),
                     "test-config-cache-%zu-%d.sqlite3", cacheSizes[s],
                     getpid());

            kvidxInstance inst = {0};
            kvidxInstance *i = &inst;
            i->interface = kvidxInterfaceSqlite3;

            kvidxConfig config = kvidxConfigDefault();
            config.cacheSizeBytes = cacheSizes[s];

            const char *errMsg = NULL;
            bool opened = kvidxOpenWithConfig(i, filename, &config, &errMsg);

            if (!opened) {
                ERR("Failed to open with %zu byte cache: %s", cacheSizes[s],
                    errMsg ? errMsg : "unknown");
            } else {
                /* Insert some data */
                for (int j = 0; j < 100; j++) {
                    if (!kvidxInsert(i, j, 1, 1, "cache", 5)) {
                        ERR("Failed to insert key %d", j);
                        break;
                    }
                }

                kvidxClose(i);
            }

            cleanupTestFile(filename);
        }
    }
}

/* ====================================================================
 * TEST SUITE 6: Update Configuration
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testUpdateConfig(uint32_t *err) {
    char filename[128];
    makeTestFilename(filename, sizeof(filename), "update");

    TEST("Update Config: Change configuration on open database") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        /* Open with defaults */
        const char *errMsg = NULL;
        bool opened = kvidxOpen(i, filename, &errMsg);
        if (!opened) {
            ERR("Failed to open database: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Update configuration */
            kvidxConfig newConfig = kvidxConfigDefault();
            newConfig.cacheSizeBytes = 128 * 1024 * 1024; /* 128 MB */
            newConfig.syncMode = KVIDX_SYNC_FULL;

            kvidxError e = kvidxUpdateConfig(i, &newConfig);
            if (e != KVIDX_OK) {
                ERR("Failed to update config: %d", e);
            }

            /* Verify config was updated */
            kvidxConfig storedConfig;
            e = kvidxGetConfig(i, &storedConfig);
            if (e != KVIDX_OK) {
                ERR("Failed to get config: %d", e);
            }

            if (storedConfig.cacheSizeBytes != newConfig.cacheSizeBytes) {
                ERR("Cache size not updated: expected %zu, got %zu",
                    newConfig.cacheSizeBytes, storedConfig.cacheSizeBytes);
            }

            kvidxClose(i);
        }
    }

    cleanupTestFile(filename);
}

/* ====================================================================
 * TEST SUITE 7: Error Handling
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testConfigErrors(uint32_t *err) {
    TEST("Config Errors: NULL instance") {
        kvidxError e = kvidxGetConfig(NULL, NULL);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL instance, got %d", e);
        }

        e = kvidxUpdateConfig(NULL, NULL);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL instance, got %d", e);
        }
    }

    TEST("Config Errors: NULL config parameter") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;

        kvidxError e = kvidxGetConfig(i, NULL);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL config, got %d", e);
        }

        e = kvidxUpdateConfig(i, NULL);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL config, got %d", e);
        }
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
    printf("CONFIGURATION TESTS FOR KVIDXKIT\n");
    printf("=======================================================\n\n");

    printf("Running Suite 1: Default Configuration\n");
    printf("-------------------------------------------------------\n");
    testDefaultConfiguration(&err);
    printf("\n");

    printf("Running Suite 2: Opening with Configuration\n");
    printf("-------------------------------------------------------\n");
    testOpenWithConfig(&err);
    printf("\n");

    printf("Running Suite 3: Journal Modes\n");
    printf("-------------------------------------------------------\n");
    testJournalModes(&err);
    printf("\n");

    printf("Running Suite 4: Sync Modes\n");
    printf("-------------------------------------------------------\n");
    testSyncModes(&err);
    printf("\n");

    printf("Running Suite 5: Cache Size Configuration\n");
    printf("-------------------------------------------------------\n");
    testCacheSizes(&err);
    printf("\n");

    printf("Running Suite 6: Update Configuration\n");
    printf("-------------------------------------------------------\n");
    testUpdateConfig(&err);
    printf("\n");

    printf("Running Suite 7: Error Handling\n");
    printf("-------------------------------------------------------\n");
    testConfigErrors(&err);
    printf("\n");

    printf("=======================================================\n");
    if (err == 0) {
        printf("ALL CONFIGURATION TESTS PASSED!\n");
        printf("=======================================================\n");
        return 0;
    } else {
        printf("FAILED: %u test(s) failed\n", err);
        printf("=======================================================\n");
        return 1;
    }
}
