/**
 * kvidxkit Performance Benchmark Framework
 *
 * Comprehensive benchmarks for comparing adapter performance across all
 * backends. Uses the same extensible adapter registry pattern as the fuzzer.
 *
 * Benchmark Categories:
 * 1. Sequential Operations - Insert/Read in key order
 * 2. Random Operations - Insert/Read with random keys
 * 3. Mixed Workloads - Configurable read/write ratios
 * 4. Batch Operations - Bulk insert performance
 * 5. Range Operations - Range queries and deletes
 * 6. Concurrent Patterns - Simulated concurrent access patterns
 *
 * Usage:
 *   ./kvidxkit-bench              Run all benchmarks
 *   ./kvidxkit-bench quick        Run quick benchmarks (fewer operations)
 *   ./kvidxkit-bench <count>      Run with custom operation count
 */

#include "kvidxkit.h"
#include "kvidxkitRegistry.h"

#include <dirent.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

/* ====================================================================
 * Configuration
 * ==================================================================== */

#define BENCH_DEFAULT_COUNT 100000 /* Default operations per benchmark */
#define BENCH_QUICK_COUNT 10000    /* Quick mode operation count */
#define BENCH_BATCH_SIZE 1000      /* Entries per batch */
#define BENCH_DATA_SIZE 64         /* Default data blob size */
#define BENCH_LARGE_DATA_SIZE 4096 /* Large data blob size */
#define MAX_ADAPTERS 16            /* Maximum number of adapters */

/* ====================================================================
 * Adapter Registry
 *
 * Adapters are now discovered at runtime via the kvidxkitRegistry API.
 * Enable adapters at cmake configure time:
 *   cmake -DKVIDXKIT_ENABLE_SQLITE3=ON -DKVIDXKIT_ENABLE_LMDB=ON ..
 *
 * The benchmark will automatically test all enabled adapters.
 * ==================================================================== */

/* Use registry's kvidxAdapterInfo, alias for minimal code changes */
typedef kvidxAdapterInfo AdapterDesc;

/* Runtime adapter count - set in main() */
static size_t ADAPTER_COUNT = 0;

/* ====================================================================
 * Timing Utilities
 * ==================================================================== */

typedef struct {
    struct timeval start;
    struct timeval end;
} BenchTimer;

static void timer_start(BenchTimer *t) {
    gettimeofday(&t->start, NULL);
}

static double timer_stop(BenchTimer *t) {
    gettimeofday(&t->end, NULL);
    double start_sec = t->start.tv_sec + t->start.tv_usec / 1000000.0;
    double end_sec = t->end.tv_sec + t->end.tv_usec / 1000000.0;
    return end_sec - start_sec;
}

/* ====================================================================
 * Benchmark Results
 * ==================================================================== */

typedef struct {
    const char *name;     /* Benchmark name */
    uint64_t operations;  /* Number of operations */
    double elapsed;       /* Time in seconds */
    double opsPerSec;     /* Operations per second */
    double latencyUs;     /* Average latency in microseconds */
    double throughputMBs; /* Data throughput in MB/s (if applicable) */
    uint64_t dataBytes;   /* Total data bytes processed */
} BenchResult;

typedef struct {
    const char *adapterName;
    BenchResult results[32]; /* Up to 32 benchmark results */
    size_t resultCount;
} AdapterResults;

static AdapterResults g_results[MAX_ADAPTERS];
static size_t g_resultCount = 0;

static void record_result(const char *adapter, const char *bench, uint64_t ops,
                          double elapsed, uint64_t dataBytes) {
    /* Find or create adapter results */
    AdapterResults *ar = NULL;
    for (size_t i = 0; i < g_resultCount; i++) {
        if (strcmp(g_results[i].adapterName, adapter) == 0) {
            ar = &g_results[i];
            break;
        }
    }
    if (!ar) {
        ar = &g_results[g_resultCount++];
        ar->adapterName = adapter;
        ar->resultCount = 0;
    }

    /* Record result */
    BenchResult *r = &ar->results[ar->resultCount++];
    r->name = bench;
    r->operations = ops;
    r->elapsed = elapsed;
    r->opsPerSec = ops / elapsed;
    r->latencyUs = (elapsed * 1000000.0) / ops;
    r->dataBytes = dataBytes;
    r->throughputMBs =
        dataBytes > 0 ? (dataBytes / (1024.0 * 1024.0)) / elapsed : 0;
}

/* ====================================================================
 * Utility Functions
 * ==================================================================== */

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

static void adapter_path(char *buf, size_t bufSize, const AdapterDesc *adapter,
                         const char *benchName) {
    snprintf(buf, bufSize, "bench-%s-%s-%d%s", adapter->name, benchName,
             getpid(), adapter->pathSuffix);
    /* Convert to lowercase */
    for (char *p = buf; *p; p++) {
        if (*p >= 'A' && *p <= 'Z') {
            *p += 32;
        }
    }
}

/* xorshift64 PRNG for reproducible random sequences */
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

/* Generate test data of specified size */
static void generate_data(uint8_t *buf, size_t len, uint64_t seed) {
    uint64_t state = seed;
    for (size_t i = 0; i < len; i++) {
        buf[i] = (uint8_t)(xorshift64(&state) & 0xFF);
    }
}

/* ====================================================================
 * Benchmark 1: Sequential Insert
 * ==================================================================== */

static void bench_sequential_insert(const AdapterDesc *adapter,
                                    uint64_t count) {
    char path[128];
    adapter_path(path, sizeof(path), adapter, "seq-insert");
    cleanup_path(path);

    kvidxInstance inst = {0};
    inst.interface = *adapter->iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  [%s] FAILED: Could not open\n", adapter->name);
        return;
    }

    uint8_t data[BENCH_DATA_SIZE];
    generate_data(data, sizeof(data), 12345);

    BenchTimer timer;
    timer_start(&timer);

    kvidxBegin(&inst);
    for (uint64_t i = 1; i <= count; i++) {
        kvidxInsert(&inst, i, i, 0, data, sizeof(data));

        /* Commit periodically to avoid massive transactions */
        if (i % 10000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    double elapsed = timer_stop(&timer);

    kvidxClose(&inst);
    cleanup_path(path);

    record_result(adapter->name, "Sequential Insert", count, elapsed,
                  count * sizeof(data));
}

/* ====================================================================
 * Benchmark 2: Sequential Read
 * ==================================================================== */

static void bench_sequential_read(const AdapterDesc *adapter, uint64_t count) {
    char path[128];
    adapter_path(path, sizeof(path), adapter, "seq-read");
    cleanup_path(path);

    kvidxInstance inst = {0};
    inst.interface = *adapter->iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  [%s] FAILED: Could not open\n", adapter->name);
        return;
    }

    /* Setup: insert data first */
    uint8_t data[BENCH_DATA_SIZE];
    generate_data(data, sizeof(data), 12345);

    kvidxBegin(&inst);
    for (uint64_t i = 1; i <= count; i++) {
        kvidxInsert(&inst, i, i, 0, data, sizeof(data));
        if (i % 10000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    /* Benchmark: sequential reads */
    BenchTimer timer;
    timer_start(&timer);

    uint64_t term, cmd;
    const uint8_t *readData;
    size_t readLen;

    for (uint64_t i = 1; i <= count; i++) {
        kvidxGet(&inst, i, &term, &cmd, &readData, &readLen);
    }

    double elapsed = timer_stop(&timer);

    kvidxClose(&inst);
    cleanup_path(path);

    record_result(adapter->name, "Sequential Read", count, elapsed,
                  count * sizeof(data));
}

/* ====================================================================
 * Benchmark 3: Random Insert
 * ==================================================================== */

static void bench_random_insert(const AdapterDesc *adapter, uint64_t count) {
    char path[128];
    adapter_path(path, sizeof(path), adapter, "rand-insert");
    cleanup_path(path);

    kvidxInstance inst = {0};
    inst.interface = *adapter->iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  [%s] FAILED: Could not open\n", adapter->name);
        return;
    }

    uint8_t data[BENCH_DATA_SIZE];
    generate_data(data, sizeof(data), 12345);

    /* Pre-generate random keys to exclude from timing */
    uint64_t *keys = malloc(count * sizeof(uint64_t));
    uint64_t state = 54321;
    for (uint64_t i = 0; i < count; i++) {
        keys[i] = rand_range(&state, 1, count * 10);
    }

    BenchTimer timer;
    timer_start(&timer);

    kvidxBegin(&inst);
    for (uint64_t i = 0; i < count; i++) {
        kvidxInsert(&inst, keys[i], keys[i], 0, data, sizeof(data));

        if ((i + 1) % 10000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    double elapsed = timer_stop(&timer);

    free(keys);
    kvidxClose(&inst);
    cleanup_path(path);

    record_result(adapter->name, "Random Insert", count, elapsed,
                  count * sizeof(data));
}

/* ====================================================================
 * Benchmark 4: Random Read
 * ==================================================================== */

static void bench_random_read(const AdapterDesc *adapter, uint64_t count) {
    char path[128];
    adapter_path(path, sizeof(path), adapter, "rand-read");
    cleanup_path(path);

    kvidxInstance inst = {0};
    inst.interface = *adapter->iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  [%s] FAILED: Could not open\n", adapter->name);
        return;
    }

    /* Setup: insert sequential data */
    uint8_t data[BENCH_DATA_SIZE];
    generate_data(data, sizeof(data), 12345);

    kvidxBegin(&inst);
    for (uint64_t i = 1; i <= count; i++) {
        kvidxInsert(&inst, i, i, 0, data, sizeof(data));
        if (i % 10000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    /* Pre-generate random keys */
    uint64_t *keys = malloc(count * sizeof(uint64_t));
    uint64_t state = 98765;
    for (uint64_t i = 0; i < count; i++) {
        keys[i] = rand_range(&state, 1, count);
    }

    /* Benchmark: random reads */
    BenchTimer timer;
    timer_start(&timer);

    uint64_t term, cmd;
    const uint8_t *readData;
    size_t readLen;

    for (uint64_t i = 0; i < count; i++) {
        kvidxGet(&inst, keys[i], &term, &cmd, &readData, &readLen);
    }

    double elapsed = timer_stop(&timer);

    free(keys);
    kvidxClose(&inst);
    cleanup_path(path);

    record_result(adapter->name, "Random Read", count, elapsed,
                  count * sizeof(data));
}

/* ====================================================================
 * Benchmark 5: Mixed Workload (80% Read, 20% Write)
 * ==================================================================== */

static void bench_mixed_workload(const AdapterDesc *adapter, uint64_t count) {
    char path[128];
    adapter_path(path, sizeof(path), adapter, "mixed");
    cleanup_path(path);

    kvidxInstance inst = {0};
    inst.interface = *adapter->iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  [%s] FAILED: Could not open\n", adapter->name);
        return;
    }

    /* Setup: pre-populate with half the keys */
    uint8_t data[BENCH_DATA_SIZE];
    generate_data(data, sizeof(data), 12345);

    uint64_t prePopulate = count / 2;
    kvidxBegin(&inst);
    for (uint64_t i = 1; i <= prePopulate; i++) {
        kvidxInsert(&inst, i, i, 0, data, sizeof(data));
        if (i % 10000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    /* Benchmark: 80% reads, 20% writes */
    uint64_t state = 11111;
    uint64_t nextKey = prePopulate + 1;
    uint64_t reads = 0, writes = 0;

    BenchTimer timer;
    timer_start(&timer);

    kvidxBegin(&inst);
    for (uint64_t i = 0; i < count; i++) {
        int op = rand_range(&state, 0, 99);

        if (op < 80) {
            /* Read existing key */
            uint64_t key =
                rand_range(&state, 1, nextKey - 1 > 0 ? nextKey - 1 : 1);
            uint64_t term, cmd;
            const uint8_t *readData;
            size_t readLen;
            kvidxGet(&inst, key, &term, &cmd, &readData, &readLen);
            reads++;
        } else {
            /* Write new key */
            kvidxInsert(&inst, nextKey, nextKey, 0, data, sizeof(data));
            nextKey++;
            writes++;
        }

        /* Commit periodically */
        if ((i + 1) % 1000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    double elapsed = timer_stop(&timer);

    kvidxClose(&inst);
    cleanup_path(path);

    record_result(adapter->name, "Mixed 80/20 R/W", count, elapsed,
                  (reads + writes) * sizeof(data));
}

/* ====================================================================
 * Benchmark 6: Batch Insert
 * ==================================================================== */

static void bench_batch_insert(const AdapterDesc *adapter, uint64_t count) {
    char path[128];
    adapter_path(path, sizeof(path), adapter, "batch");
    cleanup_path(path);

    kvidxInstance inst = {0};
    inst.interface = *adapter->iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  [%s] FAILED: Could not open\n", adapter->name);
        return;
    }

    uint8_t data[BENCH_DATA_SIZE];
    generate_data(data, sizeof(data), 12345);

    /* Prepare batch entries */
    size_t batchSize = BENCH_BATCH_SIZE;
    kvidxEntry *entries = malloc(batchSize * sizeof(kvidxEntry));

    BenchTimer timer;
    timer_start(&timer);

    uint64_t key = 1;
    uint64_t batches = count / batchSize;

    for (uint64_t b = 0; b < batches; b++) {
        /* Prepare batch */
        for (size_t i = 0; i < batchSize; i++) {
            entries[i].key = key++;
            entries[i].term = entries[i].key;
            entries[i].cmd = 0;
            entries[i].data = data;
            entries[i].dataLen = sizeof(data);
        }

        /* Insert batch */
        size_t inserted;
        kvidxInsertBatch(&inst, entries, batchSize, &inserted);
    }

    double elapsed = timer_stop(&timer);

    free(entries);
    kvidxClose(&inst);
    cleanup_path(path);

    uint64_t totalOps = batches * batchSize;
    record_result(adapter->name, "Batch Insert", totalOps, elapsed,
                  totalOps * sizeof(data));
}

/* ====================================================================
 * Benchmark 7: Range Count Query
 * ==================================================================== */

static void bench_range_count(const AdapterDesc *adapter, uint64_t count) {
    char path[128];
    adapter_path(path, sizeof(path), adapter, "range-count");
    cleanup_path(path);

    kvidxInstance inst = {0};
    inst.interface = *adapter->iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  [%s] FAILED: Could not open\n", adapter->name);
        return;
    }

    /* Setup: insert data */
    uint8_t data[BENCH_DATA_SIZE];
    generate_data(data, sizeof(data), 12345);

    kvidxBegin(&inst);
    for (uint64_t i = 1; i <= count; i++) {
        kvidxInsert(&inst, i, i, 0, data, sizeof(data));
        if (i % 10000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    /* Force flush for RocksDB so approximate_sizes works.
     * This is done via a fsync operation which triggers flush. */
    kvidxFsync(&inst);

    /* Benchmark: random range counts */
    uint64_t state = 77777;
    uint64_t queries = count / 10;

    BenchTimer timer;
    timer_start(&timer);

    for (uint64_t i = 0; i < queries; i++) {
        uint64_t start = rand_range(&state, 1, count / 2);
        uint64_t end = rand_range(&state, count / 2, count);
        uint64_t rangeCount;
        kvidxCountRange(&inst, start, end, &rangeCount);
    }

    double elapsed = timer_stop(&timer);

    kvidxClose(&inst);
    cleanup_path(path);

    record_result(adapter->name, "Range Count Query", queries, elapsed, 0);
}

/* ====================================================================
 * Benchmark 8: Iterator Scan
 * ==================================================================== */

static void bench_iterator_scan(const AdapterDesc *adapter, uint64_t count) {
    char path[128];
    adapter_path(path, sizeof(path), adapter, "iterator");
    cleanup_path(path);

    kvidxInstance inst = {0};
    inst.interface = *adapter->iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  [%s] FAILED: Could not open\n", adapter->name);
        return;
    }

    /* Setup: insert data */
    uint8_t data[BENCH_DATA_SIZE];
    generate_data(data, sizeof(data), 12345);

    kvidxBegin(&inst);
    for (uint64_t i = 1; i <= count; i++) {
        kvidxInsert(&inst, i, i, 0, data, sizeof(data));
        if (i % 10000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    /* Benchmark: full forward scan using getNext */
    BenchTimer timer;
    timer_start(&timer);

    uint64_t key = 0;
    uint64_t nextKey, term, cmd;
    const uint8_t *readData;
    size_t readLen;
    uint64_t scanned = 0;

    while (
        kvidxGetNext(&inst, key, &nextKey, &term, &cmd, &readData, &readLen)) {
        key = nextKey;
        scanned++;
    }

    double elapsed = timer_stop(&timer);

    kvidxClose(&inst);
    cleanup_path(path);

    record_result(adapter->name, "Iterator Scan", scanned, elapsed,
                  scanned * sizeof(data));
}

/* ====================================================================
 * Benchmark 9: Large Data Blobs
 * ==================================================================== */

static void bench_large_data(const AdapterDesc *adapter, uint64_t count) {
    /* Use fewer operations for large data */
    uint64_t actualCount = count / 10;
    if (actualCount < 1000) {
        actualCount = 1000;
    }

    char path[128];
    adapter_path(path, sizeof(path), adapter, "large-data");
    cleanup_path(path);

    kvidxInstance inst = {0};
    inst.interface = *adapter->iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  [%s] FAILED: Could not open\n", adapter->name);
        return;
    }

    uint8_t *data = malloc(BENCH_LARGE_DATA_SIZE);
    generate_data(data, BENCH_LARGE_DATA_SIZE, 12345);

    BenchTimer timer;
    timer_start(&timer);

    kvidxBegin(&inst);
    for (uint64_t i = 1; i <= actualCount; i++) {
        kvidxInsert(&inst, i, i, 0, data, BENCH_LARGE_DATA_SIZE);
        if (i % 1000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    double elapsed = timer_stop(&timer);

    free(data);
    kvidxClose(&inst);
    cleanup_path(path);

    record_result(adapter->name, "Large Data (4KB)", actualCount, elapsed,
                  actualCount * BENCH_LARGE_DATA_SIZE);
}

/* ====================================================================
 * Benchmark 10: Delete Performance
 * ==================================================================== */

static void bench_delete(const AdapterDesc *adapter, uint64_t count) {
    char path[128];
    adapter_path(path, sizeof(path), adapter, "delete");
    cleanup_path(path);

    kvidxInstance inst = {0};
    inst.interface = *adapter->iface;

    if (!kvidxOpen(&inst, path, NULL)) {
        printf("  [%s] FAILED: Could not open\n", adapter->name);
        return;
    }

    /* Setup: insert data */
    uint8_t data[BENCH_DATA_SIZE];
    generate_data(data, sizeof(data), 12345);

    kvidxBegin(&inst);
    for (uint64_t i = 1; i <= count; i++) {
        kvidxInsert(&inst, i, i, 0, data, sizeof(data));
        if (i % 10000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    /* Benchmark: delete all keys */
    BenchTimer timer;
    timer_start(&timer);

    kvidxBegin(&inst);
    for (uint64_t i = 1; i <= count; i++) {
        kvidxRemove(&inst, i);

        /* Commit periodically */
        if (i % 10000 == 0) {
            kvidxCommit(&inst);
            kvidxBegin(&inst);
        }
    }
    kvidxCommit(&inst);

    double elapsed = timer_stop(&timer);

    kvidxClose(&inst);
    cleanup_path(path);

    record_result(adapter->name, "Delete", count, elapsed, 0);
}

/* ====================================================================
 * Results Printing
 * ==================================================================== */

#define BOX_WIDTH 80
#define COL_WIDTH 23

static void print_box_top(void) {
    printf("╔");
    for (int i = 0; i < BOX_WIDTH - 2; i++) {
        printf("═");
    }
    printf("╗\n");
}

static void print_box_mid(void) {
    printf("╠");
    for (int i = 0; i < BOX_WIDTH - 2; i++) {
        printf("═");
    }
    printf("╣\n");
}

static void print_box_bottom(void) {
    printf("╚");
    for (int i = 0; i < BOX_WIDTH - 2; i++) {
        printf("═");
    }
    printf("╝\n");
}

static void print_box_line(const char *text) {
    int len = (int)strlen(text);
    int pad_left = (BOX_WIDTH - 2 - len) / 2;
    int pad_right = BOX_WIDTH - 2 - len - pad_left;
    printf("║%*s%s%*s║\n", pad_left, "", text, pad_right, "");
}

static void print_header(void) {
    printf("\n");
    printf("%-20s", "Benchmark");
    for (size_t a = 0; a < ADAPTER_COUNT; a++) {
        printf(" │ %-*s", COL_WIDTH - 1, kvidxGetAdapterByIndex(a)->name);
    }
    printf("\n");

    printf("─────────────────────");
    for (size_t a = 0; a < ADAPTER_COUNT; a++) {
        printf("┼");
        for (int i = 0; i < COL_WIDTH; i++) {
            printf("─");
        }
    }
    printf("\n");
}

static void print_results_table(void) {
    printf("\n");
    print_box_top();
    print_box_line("BENCHMARK RESULTS (ops/sec)");
    print_box_bottom();

    print_header();

    /* Find all unique benchmark names */
    const char *benchNames[32];
    size_t benchCount = 0;

    if (g_resultCount > 0) {
        for (size_t i = 0; i < g_results[0].resultCount; i++) {
            benchNames[benchCount++] = g_results[0].results[i].name;
        }
    }

    /* Print each benchmark row */
    for (size_t b = 0; b < benchCount; b++) {
        printf("%-20s", benchNames[b]);

        for (size_t a = 0; a < ADAPTER_COUNT; a++) {
            /* Find this adapter's result */
            bool found = false;
            for (size_t r = 0; r < g_resultCount; r++) {
                if (strcmp(g_results[r].adapterName,
                           kvidxGetAdapterByIndex(a)->name) == 0) {
                    for (size_t i = 0; i < g_results[r].resultCount; i++) {
                        if (strcmp(g_results[r].results[i].name,
                                   benchNames[b]) == 0) {
                            double ops = g_results[r].results[i].opsPerSec;
                            if (ops >= 1000000) {
                                printf(" │ %18.2fM", ops / 1000000.0);
                            } else if (ops >= 1000) {
                                printf(" │ %18.2fK", ops / 1000.0);
                            } else {
                                printf(" │ %19.0f", ops);
                            }
                            found = true;
                            break;
                        }
                    }
                }
            }
            if (!found) {
                printf(" │ %19s", "N/A");
            }
        }
        printf("\n");
    }

    printf("\n");
}

static void print_throughput_table(void) {
    print_box_top();
    print_box_line("THROUGHPUT RESULTS (MB/s)");
    print_box_bottom();

    print_header();

    /* Find benchmarks with throughput data */
    const char *benchNames[32];
    size_t benchCount = 0;

    if (g_resultCount > 0) {
        for (size_t i = 0; i < g_results[0].resultCount; i++) {
            if (g_results[0].results[i].dataBytes > 0) {
                benchNames[benchCount++] = g_results[0].results[i].name;
            }
        }
    }

    for (size_t b = 0; b < benchCount; b++) {
        printf("%-20s", benchNames[b]);

        for (size_t a = 0; a < ADAPTER_COUNT; a++) {
            bool found = false;
            for (size_t r = 0; r < g_resultCount; r++) {
                if (strcmp(g_results[r].adapterName,
                           kvidxGetAdapterByIndex(a)->name) == 0) {
                    for (size_t i = 0; i < g_results[r].resultCount; i++) {
                        if (strcmp(g_results[r].results[i].name,
                                   benchNames[b]) == 0) {
                            printf(" │ %19.2f",
                                   g_results[r].results[i].throughputMBs);
                            found = true;
                            break;
                        }
                    }
                }
            }
            if (!found) {
                printf(" │ %19s", "N/A");
            }
        }
        printf("\n");
    }

    printf("\n");
}

static void print_latency_table(void) {
    print_box_top();
    print_box_line("LATENCY RESULTS (microseconds)");
    print_box_bottom();

    print_header();

    const char *benchNames[32];
    size_t benchCount = 0;

    if (g_resultCount > 0) {
        for (size_t i = 0; i < g_results[0].resultCount; i++) {
            benchNames[benchCount++] = g_results[0].results[i].name;
        }
    }

    for (size_t b = 0; b < benchCount; b++) {
        printf("%-20s", benchNames[b]);

        for (size_t a = 0; a < ADAPTER_COUNT; a++) {
            bool found = false;
            for (size_t r = 0; r < g_resultCount; r++) {
                if (strcmp(g_results[r].adapterName,
                           kvidxGetAdapterByIndex(a)->name) == 0) {
                    for (size_t i = 0; i < g_results[r].resultCount; i++) {
                        if (strcmp(g_results[r].results[i].name,
                                   benchNames[b]) == 0) {
                            printf(" │ %19.2f",
                                   g_results[r].results[i].latencyUs);
                            found = true;
                            break;
                        }
                    }
                }
            }
            if (!found) {
                printf(" │ %19s", "N/A");
            }
        }
        printf("\n");
    }

    printf("\n");
}

/* ====================================================================
 * Winner Report - Summary of best performers
 * ==================================================================== */

#define TIE_MARGIN 0.10 /* 10% margin for considering results a "tie" */

typedef struct {
    const char *adapter;
    double value;
} RankedResult;

static int compare_ranked_desc(const void *a, const void *b) {
    const RankedResult *ra = (const RankedResult *)a;
    const RankedResult *rb = (const RankedResult *)b;
    if (ra->value > rb->value) {
        return -1;
    }
    if (ra->value < rb->value) {
        return 1;
    }
    return 0;
}

static void print_winner_report(void) {
    print_box_top();
    print_box_line("WINNER REPORT - Best Adapter by Operation");
    print_box_mid();
    print_box_line("(~) = within 10% of leader, considered statistical tie");
    print_box_bottom();

    /* Find all unique benchmark names */
    const char *benchNames[32];
    size_t benchCount = 0;

    if (g_resultCount > 0) {
        for (size_t i = 0; i < g_results[0].resultCount; i++) {
            benchNames[benchCount++] = g_results[0].results[i].name;
        }
    }

    printf("\n");
    printf("%-20s │ %-12s │ %-12s │ %s\n", "Operation", "Winner", "Performance",
           "Ranking (slowdown factor)");
    printf("─────────────────────┼──────────────┼──────────────┼");
    for (int i = 0; i < 38; i++) {
        printf("─");
    }
    printf("\n");

    /* Track wins - both clear wins and ties */
    typedef struct {
        const char *adapter;
        int clearWins;
        int tiedWins;
    } WinCount;
    WinCount wins[MAX_ADAPTERS] = {0};
    size_t winCount = 0;

    /* For each benchmark, find rankings */
    for (size_t b = 0; b < benchCount; b++) {
        RankedResult rankings[MAX_ADAPTERS];
        size_t rankCount = 0;

        /* Gather all adapter results for this benchmark */
        for (size_t r = 0; r < g_resultCount; r++) {
            for (size_t i = 0; i < g_results[r].resultCount; i++) {
                if (strcmp(g_results[r].results[i].name, benchNames[b]) == 0) {
                    rankings[rankCount].adapter = g_results[r].adapterName;
                    rankings[rankCount].value =
                        g_results[r].results[i].opsPerSec;
                    rankCount++;
                    break;
                }
            }
        }

        /* Sort by performance (highest first) */
        qsort(rankings, rankCount, sizeof(RankedResult), compare_ranked_desc);

        /* Print winner line */
        if (rankCount > 0) {
            char perfStr[32];
            double ops = rankings[0].value;
            if (ops >= 1000000) {
                snprintf(perfStr, sizeof(perfStr), "%.2fM/s", ops / 1000000.0);
            } else if (ops >= 1000) {
                snprintf(perfStr, sizeof(perfStr), "%.2fK/s", ops / 1000.0);
            } else {
                snprintf(perfStr, sizeof(perfStr), "%.0f/s", ops);
            }

            /* Check for ties (within TIE_MARGIN of leader) */
            size_t tieCount = 1;
            for (size_t i = 1; i < rankCount; i++) {
                double ratio = rankings[0].value / rankings[i].value;
                if (ratio <= (1.0 + TIE_MARGIN)) {
                    tieCount++;
                } else {
                    break;
                }
            }

            /* Build winner string */
            char winnerStr[32];
            if (tieCount > 1) {
                /* It's a tie - show all tied adapters */
                char tiedNames[64] = "";
                for (size_t i = 0; i < tieCount; i++) {
                    if (i > 0) {
                        strncat(tiedNames, "~",
                                sizeof(tiedNames) - strlen(tiedNames) - 1);
                    }
                    strncat(tiedNames, rankings[i].adapter,
                            sizeof(tiedNames) - strlen(tiedNames) - 1);
                }
                snprintf(winnerStr, sizeof(winnerStr), "~TIE~");
            } else {
                snprintf(winnerStr, sizeof(winnerStr), "%s",
                         rankings[0].adapter);
            }

            /* Build ranking string */
            char rankStr[128] = "";
            for (size_t i = 0; i < rankCount; i++) {
                char entry[48];
                double ratio =
                    (i == 0) ? 1.0 : rankings[0].value / rankings[i].value;
                bool isTied = (ratio <= (1.0 + TIE_MARGIN));

                if (i == 0) {
                    snprintf(entry, sizeof(entry), "%s", rankings[i].adapter);
                } else if (isTied) {
                    snprintf(entry, sizeof(entry), " ~ %s",
                             rankings[i].adapter);
                } else {
                    snprintf(entry, sizeof(entry), " > %s(%.1fx)",
                             rankings[i].adapter, ratio);
                }
                strncat(rankStr, entry, sizeof(rankStr) - strlen(rankStr) - 1);
            }

            printf("%-20s │ %-12s │ %12s │ %s\n", benchNames[b], winnerStr,
                   perfStr, rankStr);

            /* Record wins */
            if (tieCount > 1) {
                /* Tied - give partial credit to all tied adapters */
                for (size_t i = 0; i < tieCount; i++) {
                    bool found = false;
                    for (size_t w = 0; w < winCount; w++) {
                        if (strcmp(wins[w].adapter, rankings[i].adapter) == 0) {
                            wins[w].tiedWins++;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        wins[winCount].adapter = rankings[i].adapter;
                        wins[winCount].tiedWins = 1;
                        winCount++;
                    }
                }
            } else {
                /* Clear winner */
                bool found = false;
                for (size_t w = 0; w < winCount; w++) {
                    if (strcmp(wins[w].adapter, rankings[0].adapter) == 0) {
                        wins[w].clearWins++;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    wins[winCount].adapter = rankings[0].adapter;
                    wins[winCount].clearWins = 1;
                    winCount++;
                }
            }
        }
    }

    printf("\n");

    /* Summary: count wins per adapter */
    print_box_top();
    print_box_line("OVERALL SCORE BY ADAPTER");
    print_box_mid();
    print_box_line("Clear wins = 1 point, Tied wins = 0.5 points each");
    print_box_bottom();

    printf("\n");

    /* Calculate scores and sort */
    typedef struct {
        const char *adapter;
        double score;
        int clear;
        int tied;
    } ScoredAdapter;
    ScoredAdapter scores[MAX_ADAPTERS];

    for (size_t i = 0; i < winCount; i++) {
        scores[i].adapter = wins[i].adapter;
        scores[i].clear = wins[i].clearWins;
        scores[i].tied = wins[i].tiedWins;
        scores[i].score = wins[i].clearWins + (wins[i].tiedWins * 0.5);
    }

    /* Sort by score (descending) */
    for (size_t i = 0; i < winCount; i++) {
        for (size_t j = i + 1; j < winCount; j++) {
            if (scores[j].score > scores[i].score) {
                ScoredAdapter tmp = scores[i];
                scores[i] = scores[j];
                scores[j] = tmp;
            }
        }
    }

    /* Print scores with visual bar */
    for (size_t i = 0; i < winCount; i++) {
        printf("  %-10s ", scores[i].adapter);

        /* Visual bar based on score */
        int fullBlocks = (int)scores[i].score;
        bool halfBlock = (scores[i].score - fullBlocks) >= 0.5;

        for (int j = 0; j < fullBlocks; j++) {
            printf("█");
        }
        if (halfBlock) {
            printf("▌");
            fullBlocks++;
        }
        for (int j = fullBlocks; j < (int)benchCount; j++) {
            printf("░");
        }

        printf(" %.1f pts", scores[i].score);
        if (scores[i].clear > 0 || scores[i].tied > 0) {
            printf(" (%d clear", scores[i].clear);
            if (scores[i].tied > 0) {
                printf(" + %d tied", scores[i].tied);
            }
            printf(")");
        }
        if (i == 0 && winCount > 1 && scores[0].score > scores[1].score) {
            printf(" <- OVERALL WINNER");
        } else if (i == 0 && winCount > 1 &&
                   scores[0].score == scores[1].score) {
            printf(" <- TIE FOR FIRST");
        }
        printf("\n");
    }

    printf("\n");
}

/* ====================================================================
 * Main Entry Point
 * ==================================================================== */

int main(int argc, char *argv[]) {
    uint64_t count = BENCH_DEFAULT_COUNT;

    /* Initialize adapter count from registry */
    ADAPTER_COUNT = kvidxGetAdapterCount();
    if (ADAPTER_COUNT == 0) {
        printf(
            "ERROR: No adapters enabled. Build with at least one adapter:\n");
        printf("  cmake -DKVIDXKIT_ENABLE_SQLITE3=ON ..\n");
        return 1;
    }

    /* Parse arguments */
    if (argc > 1) {
        if (strcmp(argv[1], "quick") == 0) {
            count = BENCH_QUICK_COUNT;
        } else {
            count = strtoull(argv[1], NULL, 10);
            if (count == 0) {
                count = BENCH_DEFAULT_COUNT;
            }
        }
    }

    print_box_top();
    print_box_line("kvidxkit Performance Benchmark Framework v1.0");
    print_box_mid();

    char infoLine[80];
    snprintf(infoLine, sizeof(infoLine),
             "Operations: %" PRIu64
             "  |  Adapters: %zu  |  Data size: %d bytes",
             count, ADAPTER_COUNT, BENCH_DATA_SIZE);
    print_box_line(infoLine);

    print_box_bottom();

    /* Run benchmarks for each adapter */
    for (size_t a = 0; a < ADAPTER_COUNT; a++) {
        const AdapterDesc *adapter = kvidxGetAdapterByIndex(a);

        printf("\n═════════════════════════════════════════════════════════════"
               "═══════════════════\n");
        printf("  Benchmarking: %s\n", adapter->name);
        printf("═══════════════════════════════════════════════════════════════"
               "═════════════════\n");

        printf("  [1/10] Sequential Insert...\n");
        bench_sequential_insert(adapter, count);

        printf("  [2/10] Sequential Read...\n");
        bench_sequential_read(adapter, count);

        printf("  [3/10] Random Insert...\n");
        bench_random_insert(adapter, count);

        printf("  [4/10] Random Read...\n");
        bench_random_read(adapter, count);

        printf("  [5/10] Mixed Workload (80/20)...\n");
        bench_mixed_workload(adapter, count);

        printf("  [6/10] Batch Insert...\n");
        bench_batch_insert(adapter, count);

        printf("  [7/10] Range Count Query...\n");
        bench_range_count(adapter, count);

        printf("  [8/10] Iterator Scan...\n");
        bench_iterator_scan(adapter, count);

        printf("  [9/10] Large Data (4KB blobs)...\n");
        bench_large_data(adapter, count);

        printf("  [10/10] Delete...\n");
        bench_delete(adapter, count);

        printf("  Done.\n");
    }

    /* Print results */
    print_results_table();
    print_throughput_table();
    print_latency_table();
    print_winner_report();

    /* Summary */
    print_box_top();
    print_box_line("BENCHMARK COMPLETE");
    print_box_bottom();

    return 0;
}
