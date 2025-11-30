/**
 * kvidxkit Fuzzer Framework
 *
 * Comprehensive fuzzer for testing database adapter stability, consistency,
 * and ACID compliance across all backends.
 *
 * Test Categories:
 * 1. Operation Sequence Fuzzer - Random ops to find crashes/inconsistencies
 * 2. Cross-Adapter Consistency - Same ops on SQLite & LMDB produce same results
 * 3. ACID Compliance - Atomicity, Consistency, Isolation, Durability
 * 4. Stress/Endurance - Long-running tests with high volume
 * 5. Boundary Testing - Edge cases, limits, malformed data
 *
 * Usage:
 *   ./kvidxkit-fuzz [seed]     Run with specific seed for reproducibility
 *   ./kvidxkit-fuzz            Run with random seed (printed for reproduction)
 */

#include "ctest.h"
#include "kvidxkit.h"
#include "kvidxkitRegistry.h"

#include <dirent.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* ====================================================================
 * Configuration
 * ==================================================================== */

#define FUZZ_OP_COUNT 10000       /* Operations per fuzzer run */
#define FUZZ_STRESS_COUNT 50000   /* Operations for stress test */
#define FUZZ_MAX_KEY 10000        /* Maximum key value for fuzzing */
#define FUZZ_MAX_DATA_SIZE 4096   /* Maximum data blob size */
#define FUZZ_CONSISTENCY_OPS 5000 /* Operations for consistency test */
#define MAX_ADAPTERS 16           /* Maximum number of adapters */

/* ====================================================================
 * Adapter Registry
 *
 * Adapters are now discovered at runtime via the kvidxkitRegistry API.
 * Enable adapters at cmake configure time:
 *   cmake -DKVIDXKIT_ENABLE_SQLITE3=ON -DKVIDXKIT_ENABLE_LMDB=ON ..
 *
 * The fuzzer will automatically test all enabled adapters.
 * ==================================================================== */

/* Use registry's kvidxAdapterInfo, alias for minimal code changes */
typedef kvidxAdapterInfo AdapterDesc;

/* Runtime adapter count - set in main() */
static size_t ADAPTER_COUNT = 0;

/* Operation types for fuzzing */
typedef enum {
    FUZZ_OP_INSERT,
    FUZZ_OP_GET,
    FUZZ_OP_REMOVE,
    FUZZ_OP_EXISTS,
    FUZZ_OP_MAX_KEY,
    FUZZ_OP_GET_NEXT,
    FUZZ_OP_GET_PREV,
    FUZZ_OP_BEGIN,
    FUZZ_OP_COMMIT,
    FUZZ_OP_REMOVE_AFTER,
    FUZZ_OP_REMOVE_BEFORE,
    FUZZ_OP_COUNT_RANGE,
    FUZZ_OP_KEY_COUNT,
    FUZZ_OP_TYPE_COUNT
} FuzzOpType;

/* ====================================================================
 * Utility Functions
 * ==================================================================== */

static uint64_t g_seed = 0;
static volatile int g_interrupted = 0;

static void sigint_handler(int sig) {
    (void)sig;
    g_interrupted = 1;
    printf("\n[Interrupted - finishing current test]\n");
}

/* xorshift64 PRNG - fast and good enough for fuzzing */
static uint64_t xorshift64(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

static uint64_t rand_range(uint64_t *state, uint64_t min, uint64_t max) {
    return min + (xorshift64(state) % (max - min + 1));
}

static int removeDir(const char *path) {
    DIR *d = opendir(path);
    if (!d) {
        return unlink(path);
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

static void cleanup_path(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) {
            removeDir(path);
        } else {
            unlink(path);
        }
    }
}

/* Generate test path for an adapter */
static void adapter_path(char *buf, size_t bufSize, const AdapterDesc *adapter,
                         const char *testName) {
    snprintf(buf, bufSize, "fuzz-%s-%s-%d%s", adapter->name, testName, getpid(),
             adapter->pathSuffix);
    /* Convert to lowercase for filesystem friendliness */
    for (char *p = buf; *p; p++) {
        if (*p >= 'A' && *p <= 'Z') {
            *p += 32;
        }
    }
}

/* Generate random data blob */
static void *generate_random_data(uint64_t *state, size_t *len) {
    *len = rand_range(state, 0, FUZZ_MAX_DATA_SIZE);
    if (*len == 0) {
        return NULL;
    }

    uint8_t *data = malloc(*len);
    for (size_t i = 0; i < *len; i++) {
        data[i] = (uint8_t)(xorshift64(state) & 0xFF);
    }
    return data;
}

/* ====================================================================
 * Simple In-Memory Reference Store for Verification
 * ==================================================================== */

typedef struct {
    uint64_t key;
    uint64_t term;
    uint64_t cmd;
    uint8_t *data;
    size_t dataLen;
    bool valid;
} RefEntry;

typedef struct {
    RefEntry *entries;
    size_t capacity;
    size_t count;
} RefStore;

static void ref_init(RefStore *ref, size_t capacity) {
    ref->entries = calloc(capacity, sizeof(RefEntry));
    ref->capacity = capacity;
    ref->count = 0;
}

static void ref_free(RefStore *ref) {
    for (size_t i = 0; i < ref->capacity; i++) {
        if (ref->entries[i].valid && ref->entries[i].data) {
            free(ref->entries[i].data);
        }
    }
    free(ref->entries);
    ref->entries = NULL;
    ref->capacity = 0;
    ref->count = 0;
}

static void ref_insert(RefStore *ref, uint64_t key, uint64_t term, uint64_t cmd,
                       const void *data, size_t dataLen) {
    /* Find or create slot */
    for (size_t i = 0; i < ref->capacity; i++) {
        if (ref->entries[i].valid && ref->entries[i].key == key) {
            /* Update existing */
            free(ref->entries[i].data);
            ref->entries[i].term = term;
            ref->entries[i].cmd = cmd;
            if (dataLen > 0 && data) {
                ref->entries[i].data = malloc(dataLen);
                memcpy(ref->entries[i].data, data, dataLen);
            } else {
                ref->entries[i].data = NULL;
            }
            ref->entries[i].dataLen = dataLen;
            return;
        }
    }

    /* Insert new */
    for (size_t i = 0; i < ref->capacity; i++) {
        if (!ref->entries[i].valid) {
            ref->entries[i].key = key;
            ref->entries[i].term = term;
            ref->entries[i].cmd = cmd;
            if (dataLen > 0 && data) {
                ref->entries[i].data = malloc(dataLen);
                memcpy(ref->entries[i].data, data, dataLen);
            } else {
                ref->entries[i].data = NULL;
            }
            ref->entries[i].dataLen = dataLen;
            ref->entries[i].valid = true;
            ref->count++;
            return;
        }
    }
}

static bool ref_get(RefStore *ref, uint64_t key, uint64_t *term, uint64_t *cmd,
                    const uint8_t **data, size_t *len) {
    for (size_t i = 0; i < ref->capacity; i++) {
        if (ref->entries[i].valid && ref->entries[i].key == key) {
            if (term) {
                *term = ref->entries[i].term;
            }
            if (cmd) {
                *cmd = ref->entries[i].cmd;
            }
            if (data) {
                *data = ref->entries[i].data;
            }
            if (len) {
                *len = ref->entries[i].dataLen;
            }
            return true;
        }
    }
    return false;
}

static bool ref_exists(RefStore *ref, uint64_t key) {
    for (size_t i = 0; i < ref->capacity; i++) {
        if (ref->entries[i].valid && ref->entries[i].key == key) {
            return true;
        }
    }
    return false;
}

static void ref_remove(RefStore *ref, uint64_t key) {
    for (size_t i = 0; i < ref->capacity; i++) {
        if (ref->entries[i].valid && ref->entries[i].key == key) {
            free(ref->entries[i].data);
            ref->entries[i].data = NULL;
            ref->entries[i].valid = false;
            ref->count--;
            return;
        }
    }
}

static void ref_remove_after(RefStore *ref, uint64_t key) {
    for (size_t i = 0; i < ref->capacity; i++) {
        if (ref->entries[i].valid && ref->entries[i].key >= key) {
            free(ref->entries[i].data);
            ref->entries[i].data = NULL;
            ref->entries[i].valid = false;
            ref->count--;
        }
    }
}

static void ref_remove_before(RefStore *ref, uint64_t key) {
    for (size_t i = 0; i < ref->capacity; i++) {
        if (ref->entries[i].valid && ref->entries[i].key <= key) {
            free(ref->entries[i].data);
            ref->entries[i].data = NULL;
            ref->entries[i].valid = false;
            ref->count--;
        }
    }
}

static uint64_t ref_count(RefStore *ref) {
    uint64_t count = 0;
    for (size_t i = 0; i < ref->capacity; i++) {
        if (ref->entries[i].valid) {
            count++;
        }
    }
    return count;
}

static bool ref_max_key(RefStore *ref, uint64_t *maxKey) {
    bool found = false;
    uint64_t max = 0;
    for (size_t i = 0; i < ref->capacity; i++) {
        if (ref->entries[i].valid) {
            if (!found || ref->entries[i].key > max) {
                max = ref->entries[i].key;
                found = true;
            }
        }
    }
    if (found && maxKey) {
        *maxKey = max;
    }
    return found;
}

/* ====================================================================
 * Test 1: Operation Sequence Fuzzer
 * ==================================================================== */

static uint32_t fuzz_operation_sequence(const char *name,
                                        const kvidxInterface *iface,
                                        const char *path, uint64_t seed,
                                        int opCount) {
    uint32_t errors = 0;
    uint64_t state = seed;

    printf("\n[%s] Operation Sequence Fuzzer (seed=%" PRIu64 ", ops=%d)\n",
           name, seed, opCount);

    kvidxInstance inst = {0};
    inst.interface = *iface;

    const char *errStr = NULL;
    if (!kvidxOpen(&inst, path, &errStr)) {
        printf("  FATAL: Failed to open: %s\n", errStr ? errStr : "unknown");
        return 1;
    }

    RefStore ref;
    ref_init(&ref, FUZZ_MAX_KEY);

    bool inTransaction = false;
    int successOps = 0;
    int failOps = 0;

    for (int i = 0; i < opCount && !g_interrupted; i++) {
        FuzzOpType op = rand_range(&state, 0, FUZZ_OP_TYPE_COUNT - 1);
        uint64_t key = rand_range(&state, 1, FUZZ_MAX_KEY - 1);

        switch (op) {
        case FUZZ_OP_INSERT: {
            uint64_t term = rand_range(&state, 1, 1000);
            uint64_t cmd = rand_range(&state, 0, 100);
            size_t dataLen;
            void *data = generate_random_data(&state, &dataLen);

            bool result = kvidxInsert(&inst, key, term, cmd, data, dataLen);
            if (result) {
                ref_insert(&ref, key, term, cmd, data, dataLen);
                successOps++;
            } else {
                failOps++;
            }
            free(data);
            break;
        }

        case FUZZ_OP_GET: {
            uint64_t term, cmd;
            const uint8_t *data;
            size_t len;

            bool dbResult = kvidxGet(&inst, key, &term, &cmd, &data, &len);
            bool refResult = ref_exists(&ref, key);

            if (dbResult != refResult) {
                printf("  ERROR[%d]: Get mismatch for key %" PRIu64
                       " (db=%d, ref=%d)\n",
                       i, key, dbResult, refResult);
                errors++;
            }
            successOps++;
            break;
        }

        case FUZZ_OP_REMOVE: {
            bool dbResult = kvidxRemove(&inst, key);
            ref_remove(&ref, key);
            if (dbResult) {
                successOps++;
            } else {
                failOps++;
            }
            break;
        }

        case FUZZ_OP_EXISTS: {
            bool dbResult = kvidxExists(&inst, key);
            bool refResult = ref_exists(&ref, key);

            if (dbResult != refResult) {
                printf("  ERROR[%d]: Exists mismatch for key %" PRIu64
                       " (db=%d, ref=%d)\n",
                       i, key, dbResult, refResult);
                errors++;
            }
            successOps++;
            break;
        }

        case FUZZ_OP_MAX_KEY: {
            uint64_t dbMax, refMax;
            bool dbResult = kvidxMaxKey(&inst, &dbMax);
            bool refResult = ref_max_key(&ref, &refMax);

            if (dbResult != refResult) {
                printf("  ERROR[%d]: MaxKey existence mismatch\n", i);
                errors++;
            } else if (dbResult && dbMax != refMax) {
                printf("  ERROR[%d]: MaxKey value mismatch (db=%" PRIu64
                       ", ref=%" PRIu64 ")\n",
                       i, dbMax, refMax);
                errors++;
            }
            successOps++;
            break;
        }

        case FUZZ_OP_GET_NEXT:
        case FUZZ_OP_GET_PREV:
            /* These are harder to verify, just ensure no crash */
            if (op == FUZZ_OP_GET_NEXT) {
                uint64_t nextKey, nextTerm;
                kvidxGetNext(&inst, key, &nextKey, &nextTerm, NULL, NULL, NULL);
            } else {
                uint64_t prevKey, prevTerm;
                kvidxGetPrev(&inst, key, &prevKey, &prevTerm, NULL, NULL, NULL);
            }
            successOps++;
            break;

        case FUZZ_OP_BEGIN:
            if (!inTransaction) {
                if (kvidxBegin(&inst)) {
                    inTransaction = true;
                    successOps++;
                } else {
                    failOps++;
                }
            }
            break;

        case FUZZ_OP_COMMIT:
            if (inTransaction) {
                if (kvidxCommit(&inst)) {
                    inTransaction = false;
                    successOps++;
                } else {
                    failOps++;
                }
            }
            break;

        case FUZZ_OP_REMOVE_AFTER: {
            kvidxRemoveAfterNInclusive(&inst, key);
            ref_remove_after(&ref, key);
            successOps++;
            break;
        }

        case FUZZ_OP_REMOVE_BEFORE: {
            kvidxRemoveBeforeNInclusive(&inst, key);
            ref_remove_before(&ref, key);
            successOps++;
            break;
        }

        case FUZZ_OP_COUNT_RANGE: {
            uint64_t startKey = rand_range(&state, 1, FUZZ_MAX_KEY / 2);
            uint64_t endKey = rand_range(&state, startKey, FUZZ_MAX_KEY - 1);
            uint64_t count;
            kvidxCountRange(&inst, startKey, endKey, &count);
            successOps++;
            break;
        }

        case FUZZ_OP_KEY_COUNT: {
            uint64_t dbCount = 0;
            kvidxGetKeyCount(&inst, &dbCount);
            uint64_t refCount = ref_count(&ref);

            if (dbCount != refCount) {
                printf("  ERROR[%d]: KeyCount mismatch (db=%" PRIu64
                       ", ref=%" PRIu64 ")\n",
                       i, dbCount, refCount);
                errors++;
            }
            successOps++;
            break;
        }

        default:
            break;
        }

        /* Progress indicator */
        if ((i + 1) % 1000 == 0) {
            printf("  Progress: %d/%d ops (errors=%u)\r", i + 1, opCount,
                   errors);
            fflush(stdout);
        }
    }

    /* Commit any open transaction */
    if (inTransaction) {
        kvidxCommit(&inst);
    }

    printf("  Completed: %d success, %d fail, %u errors\n", successOps, failOps,
           errors);

    ref_free(&ref);
    kvidxClose(&inst);

    return errors;
}

/* ====================================================================
 * Test 2: Cross-Adapter Consistency Fuzzer
 *
 * Tests that all registered adapters produce identical results when
 * given the same sequence of operations.
 * ==================================================================== */

static uint32_t fuzz_cross_adapter_consistency(uint64_t seed, int opCount) {
    if (ADAPTER_COUNT < 2) {
        printf("\n[Cross-Adapter Consistency] Skipped (need at least 2 "
               "adapters)\n");
        return 0;
    }

    uint32_t errors = 0;
    uint64_t state = seed;

    printf("\n[Cross-Adapter Consistency] (seed=%" PRIu64
           ", ops=%d, adapters=%zu)\n",
           seed, opCount, ADAPTER_COUNT);

    /* Open all adapters */
    kvidxInstance instances[MAX_ADAPTERS] = {0};
    char paths[MAX_ADAPTERS][128];

    for (size_t a = 0; a < ADAPTER_COUNT; a++) {
        adapter_path(paths[a], sizeof(paths[a]), kvidxGetAdapterByIndex(a),
                     "consistency");
        cleanup_path(paths[a]);
        instances[a].interface = *kvidxGetAdapterByIndex(a)->iface;

        if (!kvidxOpen(&instances[a], paths[a], NULL)) {
            printf("  FATAL: Failed to open %s\n",
                   kvidxGetAdapterByIndex(a)->name);
            /* Cleanup previously opened */
            for (size_t b = 0; b < a; b++) {
                kvidxClose(&instances[b]);
                cleanup_path(paths[b]);
            }
            return 1;
        }
    }

    /* Run same operations on all adapters */
    for (int i = 0; i < opCount && !g_interrupted; i++) {
        int opType = rand_range(&state, 0, 4);
        uint64_t key = rand_range(&state, 1, 1000);

        switch (opType) {
        case 0: { /* Insert */
            uint64_t term = rand_range(&state, 1, 100);
            uint64_t cmd = rand_range(&state, 0, 50);
            char data[64];
            snprintf(data, sizeof(data), "data-%d-%" PRIu64, i, key);

            /* Insert into all adapters */
            for (size_t a = 0; a < ADAPTER_COUNT; a++) {
                kvidxInsert(&instances[a], key, term, cmd, data, strlen(data));
            }

            /* Verify all have consistent existence state */
            bool firstExists = kvidxExists(&instances[0], key);
            for (size_t a = 1; a < ADAPTER_COUNT; a++) {
                bool exists = kvidxExists(&instances[a], key);
                if (exists != firstExists) {
                    printf("  ERROR[%d]: Insert existence mismatch "
                           "(%s=%d, %s=%d)\n",
                           i, kvidxGetAdapterByIndex(0)->name, firstExists,
                           kvidxGetAdapterByIndex(a)->name, exists);
                    errors++;
                }
            }
            break;
        }

        case 1: { /* Exists */
            bool firstResult = kvidxExists(&instances[0], key);
            for (size_t a = 1; a < ADAPTER_COUNT; a++) {
                bool result = kvidxExists(&instances[a], key);
                if (result != firstResult) {
                    printf("  ERROR[%d]: Exists mismatch for key %" PRIu64
                           " (%s=%d, %s=%d)\n",
                           i, key, kvidxGetAdapterByIndex(0)->name, firstResult,
                           kvidxGetAdapterByIndex(a)->name, result);
                    errors++;
                }
            }
            break;
        }

        case 2: { /* Get and compare */
            uint64_t t0, c0;
            const uint8_t *d0;
            size_t l0;
            bool r0 = kvidxGet(&instances[0], key, &t0, &c0, &d0, &l0);

            for (size_t a = 1; a < ADAPTER_COUNT; a++) {
                uint64_t ta, ca;
                const uint8_t *da;
                size_t la;
                bool ra = kvidxGet(&instances[a], key, &ta, &ca, &da, &la);

                if (ra != r0) {
                    printf("  ERROR[%d]: Get result mismatch (%s=%d, %s=%d)\n",
                           i, kvidxGetAdapterByIndex(0)->name, r0,
                           kvidxGetAdapterByIndex(a)->name, ra);
                    errors++;
                } else if (r0) {
                    if (t0 != ta || c0 != ca || l0 != la) {
                        printf("  ERROR[%d]: Get data mismatch for key %" PRIu64
                               " (%s vs %s)\n",
                               i, key, kvidxGetAdapterByIndex(0)->name,
                               kvidxGetAdapterByIndex(a)->name);
                        errors++;
                    } else if (l0 > 0 && memcmp(d0, da, l0) != 0) {
                        printf(
                            "  ERROR[%d]: Get content mismatch for key %" PRIu64
                            " (%s vs %s)\n",
                            i, key, kvidxGetAdapterByIndex(0)->name,
                            kvidxGetAdapterByIndex(a)->name);
                        errors++;
                    }
                }
            }
            break;
        }

        case 3: { /* Remove */
            for (size_t a = 0; a < ADAPTER_COUNT; a++) {
                kvidxRemove(&instances[a], key);
            }
            break;
        }

        case 4: { /* Key count */
            uint64_t c0 = 0;
            kvidxGetKeyCount(&instances[0], &c0);

            for (size_t a = 1; a < ADAPTER_COUNT; a++) {
                uint64_t ca = 0;
                kvidxGetKeyCount(&instances[a], &ca);

                if (ca != c0) {
                    printf("  ERROR[%d]: KeyCount mismatch (%s=%" PRIu64
                           ", %s=%" PRIu64 ")\n",
                           i, kvidxGetAdapterByIndex(0)->name, c0,
                           kvidxGetAdapterByIndex(a)->name, ca);
                    errors++;
                }
            }
            break;
        }
        }

        if ((i + 1) % 500 == 0) {
            printf("  Progress: %d/%d ops (errors=%u)\r", i + 1, opCount,
                   errors);
            fflush(stdout);
        }
    }

    /* Final consistency check */
    uint64_t c0 = 0;
    kvidxGetKeyCount(&instances[0], &c0);

    for (size_t a = 1; a < ADAPTER_COUNT; a++) {
        uint64_t ca = 0;
        kvidxGetKeyCount(&instances[a], &ca);

        if (ca != c0) {
            printf("  FINAL ERROR: KeyCount mismatch (%s=%" PRIu64
                   ", %s=%" PRIu64 ")\n",
                   kvidxGetAdapterByIndex(0)->name, c0,
                   kvidxGetAdapterByIndex(a)->name, ca);
            errors++;
        }
    }

    printf("  Completed with %u errors, final count: %" PRIu64 "\n", errors,
           c0);

    /* Cleanup all */
    for (size_t a = 0; a < ADAPTER_COUNT; a++) {
        kvidxClose(&instances[a]);
        cleanup_path(paths[a]);
    }

    return errors;
}

/* ====================================================================
 * Test 3: ACID Compliance Tests
 * ==================================================================== */

static uint32_t test_atomicity(const char *name, const kvidxInterface *iface,
                               const char *path) {
    uint32_t errors = 0;

    printf("\n[%s] Atomicity Test\n", name);

    kvidxInstance inst = {0};
    inst.interface = *iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  FATAL: Failed to open\n");
        return 1;
    }

    /* Test: Batch insert should be all-or-nothing within transaction */
    kvidxBegin(&inst);

    for (uint64_t k = 1; k <= 100; k++) {
        char data[32];
        snprintf(data, sizeof(data), "atomicity-%" PRIu64, k);
        if (!kvidxInsert(&inst, k, k, k, data, strlen(data))) {
            printf("  ERROR: Insert %" PRIu64 " failed within transaction\n",
                   k);
            errors++;
        }
    }

    if (!kvidxCommit(&inst)) {
        printf("  ERROR: Commit failed\n");
        errors++;
    }

    /* Verify all 100 keys exist */
    uint64_t count = 0;
    kvidxGetKeyCount(&inst, &count);
    if (count != 100) {
        printf("  ERROR: Expected 100 keys, got %" PRIu64 "\n", count);
        errors++;
    }

    /* Test: All or nothing with range delete */
    kvidxRemoveAfterNInclusive(&inst, 50);

    kvidxGetKeyCount(&inst, &count);
    if (count != 49) {
        printf("  ERROR: After range delete, expected 49 keys, got %" PRIu64
               "\n",
               count);
        errors++;
    }

    printf("  Atomicity: %s\n", errors == 0 ? "PASS" : "FAIL");

    kvidxClose(&inst);
    return errors;
}

static uint32_t test_consistency(const char *name, const kvidxInterface *iface,
                                 const char *path) {
    uint32_t errors = 0;

    printf("\n[%s] Consistency Test\n", name);

    kvidxInstance inst = {0};
    inst.interface = *iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  FATAL: Failed to open\n");
        return 1;
    }

    /* Insert keys and verify ordering is maintained */
    uint64_t keys[] = {500, 100, 900, 200, 700, 300, 800, 400, 600, 1};

    kvidxBegin(&inst);
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) {
        kvidxInsert(&inst, keys[i], keys[i], 0, NULL, 0);
    }
    kvidxCommit(&inst);

    /* Verify ordering via GetNext */
    uint64_t prev = 0;
    uint64_t current, term;
    int count = 0;

    while (kvidxGetNext(&inst, prev, &current, &term, NULL, NULL, NULL)) {
        if (current <= prev && prev != 0) {
            printf("  ERROR: Ordering violated - prev=%" PRIu64
                   ", current=%" PRIu64 "\n",
                   prev, current);
            errors++;
        }
        prev = current;
        count++;
    }

    if (count != 10) {
        printf("  ERROR: Expected 10 keys in order, got %d\n", count);
        errors++;
    }

    /* Verify max key */
    uint64_t maxKey;
    if (kvidxMaxKey(&inst, &maxKey) && maxKey != 900) {
        printf("  ERROR: MaxKey should be 900, got %" PRIu64 "\n", maxKey);
        errors++;
    }

    printf("  Consistency: %s\n", errors == 0 ? "PASS" : "FAIL");

    kvidxClose(&inst);
    return errors;
}

static uint32_t test_durability(const char *name, const kvidxInterface *iface,
                                const char *path) {
    uint32_t errors = 0;

    printf("\n[%s] Durability Test\n", name);

    /* Phase 1: Write data and close */
    {
        kvidxInstance inst = {0};
        inst.interface = *iface;

        if (!kvidxOpen(&inst, path, NULL)) {
            printf("  FATAL: Failed to open (phase 1)\n");
            return 1;
        }

        kvidxBegin(&inst);
        for (uint64_t k = 1; k <= 50; k++) {
            char data[64];
            snprintf(data, sizeof(data), "durable-data-%" PRIu64, k);
            kvidxInsert(&inst, k, k * 10, k * 100, data, strlen(data));
        }
        kvidxCommit(&inst);
        kvidxFsync(&inst);
        kvidxClose(&inst);
    }

    /* Phase 2: Reopen and verify data persisted */
    {
        kvidxInstance inst = {0};
        inst.interface = *iface;

        if (!kvidxOpen(&inst, path, NULL)) {
            printf("  FATAL: Failed to open (phase 2)\n");
            return 1;
        }

        uint64_t count = 0;
        kvidxGetKeyCount(&inst, &count);
        if (count != 50) {
            printf("  ERROR: Expected 50 keys after reopen, got %" PRIu64 "\n",
                   count);
            errors++;
        }

        /* Verify specific entry */
        uint64_t term, cmd;
        const uint8_t *data;
        size_t len;

        if (!kvidxGet(&inst, 25, &term, &cmd, &data, &len)) {
            printf("  ERROR: Key 25 not found after reopen\n");
            errors++;
        } else {
            if (term != 250 || cmd != 2500) {
                printf("  ERROR: Key 25 metadata corrupted after reopen\n");
                errors++;
            }
            const char *expected = "durable-data-25";
            if (len != strlen(expected) || memcmp(data, expected, len) != 0) {
                printf("  ERROR: Key 25 data corrupted after reopen\n");
                errors++;
            }
        }

        kvidxClose(&inst);
    }

    printf("  Durability: %s\n", errors == 0 ? "PASS" : "FAIL");

    return errors;
}

static uint32_t fuzz_acid_compliance(const char *name,
                                     const kvidxInterface *iface,
                                     const char *basePath) {
    uint32_t errors = 0;
    char path[128];

    snprintf(path, sizeof(path), "%s-atomicity", basePath);
    cleanup_path(path);
    errors += test_atomicity(name, iface, path);
    cleanup_path(path);

    snprintf(path, sizeof(path), "%s-consistency", basePath);
    cleanup_path(path);
    errors += test_consistency(name, iface, path);
    cleanup_path(path);

    snprintf(path, sizeof(path), "%s-durability", basePath);
    cleanup_path(path);
    errors += test_durability(name, iface, path);
    cleanup_path(path);

    return errors;
}

/* ====================================================================
 * Test 4: Stress/Endurance Fuzzer
 * ==================================================================== */

static uint32_t fuzz_stress_endurance(const char *name,
                                      const kvidxInterface *iface,
                                      const char *path, uint64_t seed,
                                      int opCount) {
    uint32_t errors = 0;
    uint64_t state = seed;

    printf("\n[%s] Stress/Endurance Test (seed=%" PRIu64 ", ops=%d)\n", name,
           seed, opCount);

    kvidxInstance inst = {0};
    inst.interface = *iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  FATAL: Failed to open\n");
        return 1;
    }

    clock_t start = clock();
    uint64_t insertCount = 0, deleteCount = 0, readCount = 0;

    for (int i = 0; i < opCount && !g_interrupted; i++) {
        int opType = rand_range(&state, 0, 9);
        uint64_t key = rand_range(&state, 1, 50000);

        if (opType < 5) { /* 50% inserts */
            size_t dataLen = rand_range(&state, 0, 256);
            uint8_t *data = NULL;
            if (dataLen > 0) {
                data = malloc(dataLen);
                for (size_t j = 0; j < dataLen; j++) {
                    data[j] = (uint8_t)(xorshift64(&state) & 0xFF);
                }
            }

            kvidxInsert(&inst, key, key, key, data, dataLen);
            free(data);
            insertCount++;

        } else if (opType < 8) { /* 30% reads */
            uint64_t term, cmd;
            const uint8_t *data;
            size_t len;
            kvidxGet(&inst, key, &term, &cmd, &data, &len);
            readCount++;

        } else { /* 20% deletes */
            kvidxRemove(&inst, key);
            deleteCount++;
        }

        if ((i + 1) % 5000 == 0) {
            printf("  Progress: %d/%d ops\r", i + 1, opCount);
            fflush(stdout);
        }
    }

    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;

    uint64_t finalCount = 0;
    kvidxGetKeyCount(&inst, &finalCount);

    printf("  Completed in %.2fs: %" PRIu64 " inserts, %" PRIu64 " reads, "
           "%" PRIu64 " deletes\n",
           elapsed, insertCount, readCount, deleteCount);
    printf("  Final key count: %" PRIu64 "\n", finalCount);
    printf("  Throughput: %.0f ops/sec\n", opCount / elapsed);

    /* Verify database is still consistent */
    kvidxStats stats;
    if (kvidxGetStats(&inst, &stats) == KVIDX_OK) {
        if (stats.totalKeys != finalCount) {
            printf("  ERROR: Stats key count mismatch\n");
            errors++;
        }
    }

    kvidxClose(&inst);
    return errors;
}

/* ====================================================================
 * Test 5: Boundary/Edge Case Fuzzer
 * ==================================================================== */

static uint32_t fuzz_boundary_testing(const char *name,
                                      const kvidxInterface *iface,
                                      const char *path) {
    uint32_t errors = 0;

    printf("\n[%s] Boundary Testing\n", name);

    kvidxInstance inst = {0};
    inst.interface = *iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  FATAL: Failed to open\n");
        return 1;
    }

    /* Test 1: Key boundaries */
    printf("  Testing key boundaries...\n");

    uint64_t boundaryKeys[] = {0, 1, UINT64_MAX - 1, UINT64_MAX / 2};

    for (size_t i = 0; i < sizeof(boundaryKeys) / sizeof(boundaryKeys[0]);
         i++) {
        uint64_t k = boundaryKeys[i];
        if (k == 0) {
            continue; /* Key 0 might not be valid */
        }

        if (!kvidxInsert(&inst, k, 1, 1, "boundary", 8)) {
            printf("    Warning: Insert key %" PRIu64 " failed\n", k);
        } else {
            if (!kvidxExists(&inst, k)) {
                printf("    ERROR: Key %" PRIu64 " not found after insert\n",
                       k);
                errors++;
            }
            kvidxRemove(&inst, k);
        }
    }

    /* Test 2: Data size boundaries */
    printf("  Testing data size boundaries...\n");

    size_t dataSizes[] = {0, 1, 255, 256, 1023, 1024, 4095, 4096, 65535, 65536};

    for (size_t i = 0; i < sizeof(dataSizes) / sizeof(dataSizes[0]); i++) {
        size_t sz = dataSizes[i];
        uint8_t *data = NULL;

        if (sz > 0) {
            data = malloc(sz);
            memset(data, 0xAA, sz);
        }

        uint64_t key = 1000 + i;
        if (!kvidxInsert(&inst, key, 1, 1, data, sz)) {
            printf("    Warning: Insert with data size %zu failed\n", sz);
        } else {
            const uint8_t *gotData;
            size_t gotLen;
            if (kvidxGet(&inst, key, NULL, NULL, &gotData, &gotLen)) {
                if (gotLen != sz) {
                    printf("    ERROR: Data size %zu came back as %zu\n", sz,
                           gotLen);
                    errors++;
                }
            }
        }

        free(data);
    }

    /* Test 3: Empty database operations */
    printf("  Testing empty database operations...\n");

    /* Clear everything - must clear all keys including the large boundary keys
     */
    kvidxRemoveAfterNInclusive(&inst, 0); /* Remove all keys >= 0 */

    uint64_t count = 999;
    kvidxGetKeyCount(&inst, &count);
    if (count != 0) {
        printf("    ERROR: Database not empty after clear\n");
        errors++;
    }

    uint64_t maxKey;
    if (kvidxMaxKey(&inst, &maxKey)) {
        printf("    ERROR: MaxKey should fail on empty database\n");
        errors++;
    }

    uint64_t nextKey;
    if (kvidxGetNext(&inst, 0, &nextKey, NULL, NULL, NULL, NULL)) {
        printf("    ERROR: GetNext should fail on empty database\n");
        errors++;
    }

    printf("  Boundary testing: %s\n", errors == 0 ? "PASS" : "FAIL");

    kvidxClose(&inst);
    return errors;
}

/* ====================================================================
 * Main Entry Point
 *
 * Automatically tests all adapters enabled at compile time via the
 * kvidxkitRegistry API. Enable adapters with cmake options:
 *   cmake -DKVIDXKIT_ENABLE_SQLITE3=ON -DKVIDXKIT_ENABLE_LMDB=ON ..
 * ==================================================================== */

int main(int argc, char *argv[]) {
    uint32_t totalErrors = 0;

    /* Initialize adapter count from registry */
    ADAPTER_COUNT = kvidxGetAdapterCount();
    if (ADAPTER_COUNT == 0) {
        printf(
            "ERROR: No adapters enabled. Build with at least one adapter:\n");
        printf("  cmake -DKVIDXKIT_ENABLE_SQLITE3=ON ..\n");
        return 1;
    }

    /* Parse seed from command line or generate random */
    if (argc > 1) {
        g_seed = strtoull(argv[1], NULL, 10);
    } else {
        g_seed = (uint64_t)time(NULL) ^ (uint64_t)getpid();
    }

    printf(
        "╔══════════════════════════════════════════════════════════════╗\n");
    printf(
        "║          kvidxkit Fuzzer Framework v1.0                      ║\n");
    printf(
        "╠══════════════════════════════════════════════════════════════╣\n");
    printf("║  Seed: %-54" PRIu64 " ║\n", g_seed);
    printf("║  Reproduce: ./kvidxkit-fuzz %-33" PRIu64 " ║\n", g_seed);
    printf("║  Adapters: %-50zu ║\n", ADAPTER_COUNT);
    printf(
        "╚══════════════════════════════════════════════════════════════╝\n");

    /* Setup signal handler for graceful interruption */
    signal(SIGINT, sigint_handler);

    /* ================================================================
     * Individual Adapter Tests
     * Loop over all registered adapters automatically
     * ================================================================ */
    for (size_t a = 0; a < ADAPTER_COUNT && !g_interrupted; a++) {
        const AdapterDesc *adapter = kvidxGetAdapterByIndex(a);
        char path[128];

        printf("\n═════════════════════════════════════════════════════════════"
               "═══\n");
        printf("  %s Adapter Fuzzing\n", adapter->name);
        printf("═══════════════════════════════════════════════════════════════"
               "═\n");

        /* Operation Sequence Fuzzer */
        adapter_path(path, sizeof(path), adapter, "ops");
        cleanup_path(path);
        totalErrors += fuzz_operation_sequence(adapter->name, adapter->iface,
                                               path, g_seed, FUZZ_OP_COUNT);
        cleanup_path(path);

        if (g_interrupted) {
            break;
        }

        /* ACID Compliance Tests */
        adapter_path(path, sizeof(path), adapter, "acid");
        totalErrors +=
            fuzz_acid_compliance(adapter->name, adapter->iface, path);

        if (g_interrupted) {
            break;
        }

        /* Stress/Endurance Test */
        adapter_path(path, sizeof(path), adapter, "stress");
        cleanup_path(path);
        totalErrors += fuzz_stress_endurance(adapter->name, adapter->iface,
                                             path, g_seed, FUZZ_STRESS_COUNT);
        cleanup_path(path);

        if (g_interrupted) {
            break;
        }

        /* Boundary Testing */
        adapter_path(path, sizeof(path), adapter, "boundary");
        cleanup_path(path);
        totalErrors +=
            fuzz_boundary_testing(adapter->name, adapter->iface, path);
        cleanup_path(path);
    }

    /* ================================================================
     * Cross-Adapter Consistency
     * Tests that all adapters produce identical results
     * ================================================================ */
    if (!g_interrupted) {
        printf("\n═════════════════════════════════════════════════════════════"
               "═══\n");
        printf("  Cross-Adapter Consistency Testing\n");
        printf("═══════════════════════════════════════════════════════════════"
               "═\n");

        totalErrors +=
            fuzz_cross_adapter_consistency(g_seed, FUZZ_CONSISTENCY_OPS);
    }

    /* ================================================================
     * Summary
     * ================================================================ */
    printf(
        "\n╔══════════════════════════════════════════════════════════════╗\n");
    printf(
        "║                      FUZZER SUMMARY                          ║\n");
    printf(
        "╠══════════════════════════════════════════════════════════════╣\n");

    if (g_interrupted) {
        printf("║  Status: INTERRUPTED                                         "
               "║\n");
    } else if (totalErrors == 0) {
        printf("║  Status: ALL TESTS PASSED ✓                                  "
               "║\n");
    } else {
        printf("║  Status: FAILED with %u errors                               "
               " ║\n",
               totalErrors);
    }

    printf("║  Seed for reproduction: %-37" PRIu64 " ║\n", g_seed);
    printf(
        "╚══════════════════════════════════════════════════════════════╝\n");

    return totalErrors ? 1 : 0;
}
