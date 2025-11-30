/**
 * Comprehensive export/import tests for kvidxkit
 * Tests binary, JSON, and CSV export formats, plus import with validation
 */

#include "ctest.h"
#include "kvidxkit.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
/* cppcheck-suppress constParameterPointer */
static void makeTestFilename(char *buf, size_t bufSize, const char *testName,
                             const char *ext) {
    snprintf(buf, bufSize, "test-export-%s-%d.%s", testName, getpid(), ext);
}

static void cleanupTestFile(const char *filename) {
    unlink(filename);
}

/* Helper to get file size */
static size_t getFileSize(const char *filename) {
    struct stat st;
    if (stat(filename, &st) == 0) {
        return st.st_size;
    }
    return 0;
}

/* Helper to populate test database */
static bool populateTestData(kvidxInstance *i, uint64_t count) {
    if (!kvidxBegin(i)) {
        return false;
    }

    for (uint64_t key = 1; key <= count; key++) {
        char data[64];
        snprintf(data, sizeof(data), "test-data-%" PRIu64, key);
        if (!kvidxInsert(i, key, key % 10, key % 5, data, strlen(data))) {
            return false;
        }
    }

    if (!kvidxCommit(i)) {
        return false;
    }

    return true;
}

/* ====================================================================
 * TEST SUITE 1: Binary Export/Import
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testBinaryExportImport(uint32_t *err) {
    char dbFile[128], exportFile[128];
    makeTestFilename(dbFile, sizeof(dbFile), "binary-db", "sqlite3");
    makeTestFilename(exportFile, sizeof(exportFile), "binary", "bin");

    TEST("Binary Export/Import: Export 100 entries") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, dbFile, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Populate database */
            if (!populateTestData(i, 100)) {
                ERRR("Failed to populate test data");
            }

            /* Export to binary */
            kvidxExportOptions options = kvidxExportOptionsDefault();
            options.format = KVIDX_EXPORT_BINARY;

            kvidxError e = kvidxExport(i, exportFile, &options, NULL, NULL);
            if (e != KVIDX_OK) {
                ERR("Export failed: %d - %s", e, kvidxGetLastErrorMessage(i));
            }

            /* Verify file was created */
            size_t fileSize = getFileSize(exportFile);
            if (fileSize == 0) {
                ERRR("Export file not created or is empty");
            }

            kvidxClose(i);
        }
    }

    TEST("Binary Export/Import: Import and verify") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        char importDb[128];
        makeTestFilename(importDb, sizeof(importDb), "binary-import",
                         "sqlite3");

        const char *errMsg = NULL;
        if (!kvidxOpen(i, importDb, &errMsg)) {
            ERR("Failed to open import db: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Import from binary */
            kvidxImportOptions options = kvidxImportOptionsDefault();
            options.format = KVIDX_EXPORT_BINARY;

            kvidxError e = kvidxImport(i, exportFile, &options, NULL, NULL);
            if (e != KVIDX_OK) {
                ERR("Import failed: %d - %s", e, kvidxGetLastErrorMessage(i));
            }

            /* Verify count */
            uint64_t count = 0;
            e = kvidxGetKeyCount(i, &count);
            if (e != KVIDX_OK || count != 100) {
                ERR("Expected 100 keys after import, got %" PRIu64, count);
            }

            /* Verify some data */
            uint64_t term, cmd;
            const uint8_t *data = NULL;
            size_t dataLen = 0;

            if (!kvidxGet(i, 50, &term, &cmd, &data, &dataLen)) {
                ERRR("Failed to get key 50");
            } else {
                if (term != 50 % 10 || cmd != 50 % 5) {
                    ERR("Data mismatch: term=%" PRIu64 " cmd=%" PRIu64, term,
                        cmd);
                }
            }

            kvidxClose(i);
        }

        cleanupTestFile(importDb);
    }

    cleanupTestFile(dbFile);
    cleanupTestFile(exportFile);
}

/* ====================================================================
 * TEST SUITE 2: JSON Export
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testJsonExport(uint32_t *err) {
    char dbFile[128], exportFile[128];
    makeTestFilename(dbFile, sizeof(dbFile), "json-db", "sqlite3");
    makeTestFilename(exportFile, sizeof(exportFile), "json", "json");

    TEST("JSON Export: Export with metadata") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, dbFile, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Populate database */
            if (!populateTestData(i, 20)) {
                ERRR("Failed to populate test data");
            }

            /* Export to JSON with metadata */
            kvidxExportOptions options = kvidxExportOptionsDefault();
            options.format = KVIDX_EXPORT_JSON;
            options.includeMetadata = true;
            options.prettyPrint = true;

            kvidxError e = kvidxExport(i, exportFile, &options, NULL, NULL);
            if (e != KVIDX_OK) {
                ERR("JSON export failed: %d - %s", e,
                    kvidxGetLastErrorMessage(i));
            }

            /* Verify file created and has content */
            size_t fileSize = getFileSize(exportFile);
            if (fileSize == 0) {
                ERRR("JSON export file not created or is empty");
            }

            kvidxClose(i);
        }
    }

    TEST("JSON Export: Compact format") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        char compactFile[128];
        makeTestFilename(compactFile, sizeof(compactFile), "json-compact",
                         "json");

        const char *errMsg = NULL;
        if (!kvidxOpen(i, dbFile, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Export to JSON without pretty print */
            kvidxExportOptions options = kvidxExportOptionsDefault();
            options.format = KVIDX_EXPORT_JSON;
            options.includeMetadata = false;
            options.prettyPrint = false;

            kvidxError e = kvidxExport(i, compactFile, &options, NULL, NULL);
            if (e != KVIDX_OK) {
                ERR("Compact JSON export failed: %d", e);
            }

            /* Compact should be smaller than pretty-printed */
            size_t prettySize = getFileSize(exportFile);
            size_t compactSize = getFileSize(compactFile);

            if (compactSize == 0) {
                ERRR("Compact JSON file not created");
            }

            if (compactSize >= prettySize) {
                ERR("Compact JSON should be smaller: compact=%zu pretty=%zu",
                    compactSize, prettySize);
            }

            kvidxClose(i);
        }

        cleanupTestFile(compactFile);
    }

    cleanupTestFile(dbFile);
    cleanupTestFile(exportFile);
}

/* ====================================================================
 * TEST SUITE 3: CSV Export
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testCsvExport(uint32_t *err) {
    char dbFile[128], exportFile[128];
    makeTestFilename(dbFile, sizeof(dbFile), "csv-db", "sqlite3");
    makeTestFilename(exportFile, sizeof(exportFile), "csv", "csv");

    TEST("CSV Export: With metadata") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, dbFile, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Populate database */
            if (!populateTestData(i, 15)) {
                ERRR("Failed to populate test data");
            }

            /* Export to CSV with metadata */
            kvidxExportOptions options = kvidxExportOptionsDefault();
            options.format = KVIDX_EXPORT_CSV;
            options.includeMetadata = true;

            kvidxError e = kvidxExport(i, exportFile, &options, NULL, NULL);
            if (e != KVIDX_OK) {
                ERR("CSV export failed: %d - %s", e,
                    kvidxGetLastErrorMessage(i));
            }

            /* Verify file created */
            size_t fileSize = getFileSize(exportFile);
            if (fileSize == 0) {
                ERRR("CSV export file not created or is empty");
            }

            kvidxClose(i);
        }
    }

    TEST("CSV Export: Data only (no metadata)") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        char simpleFile[128];
        makeTestFilename(simpleFile, sizeof(simpleFile), "csv-simple", "csv");

        const char *errMsg = NULL;
        if (!kvidxOpen(i, dbFile, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Export to CSV without metadata */
            kvidxExportOptions options = kvidxExportOptionsDefault();
            options.format = KVIDX_EXPORT_CSV;
            options.includeMetadata = false;

            kvidxError e = kvidxExport(i, simpleFile, &options, NULL, NULL);
            if (e != KVIDX_OK) {
                ERR("Simple CSV export failed: %d", e);
            }

            kvidxClose(i);
        }

        cleanupTestFile(simpleFile);
    }

    cleanupTestFile(dbFile);
    cleanupTestFile(exportFile);
}

/* ====================================================================
 * TEST SUITE 4: Range Export
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testRangeExport(uint32_t *err) {
    char dbFile[128], exportFile[128];
    makeTestFilename(dbFile, sizeof(dbFile), "range-db", "sqlite3");
    makeTestFilename(exportFile, sizeof(exportFile), "range", "bin");

    TEST("Range Export: Export subset of keys") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        const char *errMsg = NULL;
        if (!kvidxOpen(i, dbFile, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Populate 1-100 */
            if (!populateTestData(i, 100)) {
                ERRR("Failed to populate test data");
            }

            /* Export only keys 25-75 */
            kvidxExportOptions options = kvidxExportOptionsDefault();
            options.format = KVIDX_EXPORT_BINARY;
            options.startKey = 25;
            options.endKey = 75;

            kvidxError e = kvidxExport(i, exportFile, &options, NULL, NULL);
            if (e != KVIDX_OK) {
                ERR("Range export failed: %d", e);
            }

            kvidxClose(i);

            /* Import into new database and verify count */
            char importDb[128];
            makeTestFilename(importDb, sizeof(importDb), "range-import",
                             "sqlite3");

            kvidxInstance inst2 = {0};
            i = &inst2;
            i->interface = kvidxInterfaceSqlite3;

            if (!kvidxOpen(i, importDb, &errMsg)) {
                ERR("Failed to open import db: %s",
                    errMsg ? errMsg : "unknown");
            } else {
                kvidxImportOptions importOpts = kvidxImportOptionsDefault();
                e = kvidxImport(i, exportFile, &importOpts, NULL, NULL);
                if (e != KVIDX_OK) {
                    ERR("Range import failed: %d", e);
                }

                /* Verify count is 51 (25-75 inclusive) */
                uint64_t count = 0;
                e = kvidxGetKeyCount(i, &count);
                if (e != KVIDX_OK || count != 51) {
                    ERR("Expected 51 keys in range export, got %" PRIu64,
                        count);
                }

                /* Verify min and max */
                uint64_t minKey = 0;
                e = kvidxGetMinKey(i, &minKey);
                if (e != KVIDX_OK || minKey != 25) {
                    ERR("Expected min key 25, got %" PRIu64, minKey);
                }

                uint64_t maxKey = 0;
                if (!kvidxMaxKey(i, &maxKey) || maxKey != 75) {
                    ERR("Expected max key 75, got %" PRIu64, maxKey);
                }

                kvidxClose(i);
            }

            cleanupTestFile(importDb);
        }
    }

    cleanupTestFile(dbFile);
    cleanupTestFile(exportFile);
}

/* ====================================================================
 * TEST SUITE 5: Import Options
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testImportOptions(uint32_t *err) {
    char dbFile[128], exportFile[128];
    makeTestFilename(dbFile, sizeof(dbFile), "import-opts-db", "sqlite3");
    makeTestFilename(exportFile, sizeof(exportFile), "import-opts", "bin");

    /* Setup: Create export file */
    kvidxInstance inst = {0};
    kvidxInstance *i = &inst;
    i->interface = kvidxInterfaceSqlite3;

    const char *errMsg = NULL;
    if (kvidxOpen(i, dbFile, &errMsg)) {
        populateTestData(i, 50);
        kvidxExportOptions exportOpts = kvidxExportOptionsDefault();
        kvidxExport(i, exportFile, &exportOpts, NULL, NULL);
        kvidxClose(i);
    }

    TEST("Import Options: Clear before import") {
        kvidxInstance inst2 = {0};
        i = &inst2;
        i->interface = kvidxInterfaceSqlite3;

        char importDb[128];
        makeTestFilename(importDb, sizeof(importDb), "clear-import", "sqlite3");

        if (!kvidxOpen(i, importDb, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Add some initial data */
            kvidxInsert(i, 1000, 1, 1, "old", 3);
            kvidxInsert(i, 1001, 1, 1, "data", 4);

            /* Import with clearBeforeImport */
            kvidxImportOptions options = kvidxImportOptionsDefault();
            options.clearBeforeImport = true;

            kvidxError e = kvidxImport(i, exportFile, &options, NULL, NULL);
            if (e != KVIDX_OK) {
                ERR("Import with clear failed: %d", e);
            }

            /* Verify old data is gone */
            if (kvidxExists(i, 1000)) {
                ERRR("Old data should have been cleared");
            }

            /* Verify new data exists */
            uint64_t count = 0;
            kvidxGetKeyCount(i, &count);
            if (count != 50) {
                ERR("Expected 50 keys after import with clear, got %" PRIu64,
                    count);
            }

            kvidxClose(i);
        }

        cleanupTestFile(importDb);
    }

    TEST("Import Options: Skip duplicates") {
        kvidxInstance inst3 = {0};
        i = &inst3;
        i->interface = kvidxInterfaceSqlite3;

        char dupDb[128];
        makeTestFilename(dupDb, sizeof(dupDb), "dup-import", "sqlite3");

        if (!kvidxOpen(i, dupDb, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Add some overlapping keys */
            kvidxInsert(i, 1, 999, 999, "existing", 8);
            kvidxInsert(i, 25, 999, 999, "existing", 8);

            /* Import with skipDuplicates */
            kvidxImportOptions options = kvidxImportOptionsDefault();
            options.skipDuplicates = true;

            kvidxError e = kvidxImport(i, exportFile, &options, NULL, NULL);
            if (e != KVIDX_OK) {
                ERR("Import with skip duplicates failed: %d", e);
            }

            /* Verify data - should have original + new (minus duplicates) */
            uint64_t count = 0;
            kvidxGetKeyCount(i, &count);
            if (count != 50) { /* All 50 keys (2 existing + 48 new) */
                ERR("Expected 50 total keys, got %" PRIu64, count);
            }

            /* Verify existing keys weren't overwritten */
            uint64_t term, cmd;
            const uint8_t *data = NULL;
            size_t dataLen = 0;
            kvidxGet(i, 1, &term, &cmd, &data, &dataLen);
            if (term != 999 || cmd != 999) {
                ERR("Existing key should not be overwritten: term=%" PRIu64,
                    term);
            }

            kvidxClose(i);
        }

        cleanupTestFile(dupDb);
    }

    cleanupTestFile(dbFile);
    cleanupTestFile(exportFile);
}

/* ====================================================================
 * TEST SUITE 6: Error Handling
 * ==================================================================== */
/* cppcheck-suppress constParameterPointer */
static void testExportImportErrors(uint32_t *err) {
    TEST("Export/Import Errors: NULL parameters") {
        kvidxInstance inst = {0};
        inst.interface = kvidxInterfaceSqlite3;

        kvidxError e;

        /* NULL filename */
        e = kvidxExport(&inst, NULL, NULL, NULL, NULL);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL filename, got %d", e);
        }

        e = kvidxImport(&inst, NULL, NULL, NULL, NULL);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL filename, got %d", e);
        }

        /* NULL instance */
        e = kvidxExport(NULL, "test.bin", NULL, NULL, NULL);
        if (e != KVIDX_ERROR_INVALID_ARGUMENT) {
            ERR("Expected INVALID_ARGUMENT for NULL instance, got %d", e);
        }
    }

    TEST("Export/Import Errors: Invalid file paths") {
        kvidxInstance inst = {0};
        kvidxInstance *i = &inst;
        i->interface = kvidxInterfaceSqlite3;

        char dbFile[128];
        makeTestFilename(dbFile, sizeof(dbFile), "error-db", "sqlite3");

        const char *errMsg = NULL;
        if (!kvidxOpen(i, dbFile, &errMsg)) {
            ERR("Failed to open: %s", errMsg ? errMsg : "unknown");
        } else {
            /* Try to export to invalid path */
            kvidxExportOptions options = kvidxExportOptionsDefault();
            kvidxError e = kvidxExport(i, "/nonexistent/dir/file.bin", &options,
                                       NULL, NULL);
            if (e != KVIDX_ERROR_IO) {
                ERR("Expected IO error for invalid export path, got %d", e);
            }

            /* Try to import from nonexistent file */
            kvidxImportOptions importOpts = kvidxImportOptionsDefault();
            e = kvidxImport(i, "/nonexistent/file.bin", &importOpts, NULL,
                            NULL);
            if (e != KVIDX_ERROR_IO) {
                ERR("Expected IO error for nonexistent import file, got %d", e);
            }

            kvidxClose(i);
        }

        cleanupTestFile(dbFile);
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
    printf("EXPORT/IMPORT TESTS FOR KVIDXKIT\n");
    printf("=======================================================\n\n");

    printf("Running Suite 1: Binary Export/Import\n");
    printf("-------------------------------------------------------\n");
    testBinaryExportImport(&err);
    printf("\n");

    printf("Running Suite 2: JSON Export\n");
    printf("-------------------------------------------------------\n");
    testJsonExport(&err);
    printf("\n");

    printf("Running Suite 3: CSV Export\n");
    printf("-------------------------------------------------------\n");
    testCsvExport(&err);
    printf("\n");

    printf("Running Suite 4: Range Export\n");
    printf("-------------------------------------------------------\n");
    testRangeExport(&err);
    printf("\n");

    printf("Running Suite 5: Import Options\n");
    printf("-------------------------------------------------------\n");
    testImportOptions(&err);
    printf("\n");

    printf("Running Suite 6: Error Handling\n");
    printf("-------------------------------------------------------\n");
    testExportImportErrors(&err);
    printf("\n");

    printf("=======================================================\n");
    if (err == 0) {
        printf("ALL EXPORT/IMPORT TESTS PASSED!\n");
        printf("=======================================================\n");
        return 0;
    } else {
        printf("FAILED: %u test(s) failed\n", err);
        printf("=======================================================\n");
        return 1;
    }
}
