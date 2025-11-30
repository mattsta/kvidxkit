/**
 * kvidxkit Example: Time-Series Data Storage
 *
 * Demonstrates using kvidxkit for storing and querying time-series data.
 * Keys are timestamps, enabling efficient time-range queries.
 *
 * Build:
 *   gcc -o timeseries main.c -I../../src -L../../build/src -lkvidxkit-static
 * -lsqlite3
 *
 * Run:
 *   ./timeseries
 */

#include "kvidxkit.h"
#include <inttypes.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    kvidxInstance db;
} TimeSeriesDB;

typedef struct {
    float temperature;
    float humidity;
    float pressure;
} SensorReading;

// Get current time in milliseconds
uint64_t current_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// Initialize time series database
bool tsdb_init(TimeSeriesDB *db, const char *path) {
    memset(db, 0, sizeof(*db));
    db->db.interface = kvidxInterfaceSqlite3;

    kvidxConfig config = kvidxConfigDefault();
    config.journalMode = KVIDX_JOURNAL_WAL;
    config.syncMode = KVIDX_SYNC_NORMAL;

    const char *err = NULL;
    if (!kvidxOpenWithConfig(&db->db, path, &config, &err)) {
        fprintf(stderr, "Failed to open TSDB: %s\n", err);
        return false;
    }
    return true;
}

// Close database
void tsdb_close(TimeSeriesDB *db) {
    kvidxClose(&db->db);
}

// Insert single data point
bool tsdb_insert(TimeSeriesDB *db, uint64_t timestamp, uint64_t sensorId,
                 const SensorReading *reading) {
    kvidxBegin(&db->db);
    bool ok =
        kvidxInsert(&db->db, timestamp, sensorId, 0, reading, sizeof(*reading));
    kvidxCommit(&db->db);
    return ok;
}

// Batch insert data points
bool tsdb_insert_batch(TimeSeriesDB *db, const kvidxEntry *entries,
                       size_t count) {
    size_t inserted = 0;
    return kvidxInsertBatch(&db->db, entries, count, &inserted);
}

// Query time range
typedef void (*TSDBCallback)(uint64_t timestamp, uint64_t sensorId,
                             const SensorReading *reading, void *userData);

size_t tsdb_query_range(TimeSeriesDB *db, uint64_t startTime, uint64_t endTime,
                        TSDBCallback callback, void *userData) {
    size_t count = 0;
    kvidxIterator *it =
        kvidxIteratorCreate(&db->db, startTime, endTime, KVIDX_ITER_FORWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t timestamp, sensorId;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &timestamp, &sensorId, NULL, &data, &len);

        if (len == sizeof(SensorReading)) {
            callback(timestamp, sensorId, (const SensorReading *)data,
                     userData);
            count++;
        }
    }

    kvidxIteratorDestroy(it);
    return count;
}

// Count data points in range
uint64_t tsdb_count_range(TimeSeriesDB *db, uint64_t startTime,
                          uint64_t endTime) {
    uint64_t count = 0;
    kvidxCountRange(&db->db, startTime, endTime, &count);
    return count;
}

// Delete old data (retention policy)
uint64_t tsdb_apply_retention(TimeSeriesDB *db, uint64_t cutoffTime) {
    uint64_t deleted = 0;
    kvidxRemoveRange(&db->db, 0, cutoffTime, true, true, &deleted);
    return deleted;
}

// Get latest data point
bool tsdb_get_latest(TimeSeriesDB *db, uint64_t *timestamp, uint64_t *sensorId,
                     SensorReading *reading) {
    uint64_t maxKey = 0;
    if (!kvidxMaxKey(&db->db, &maxKey)) {
        return false;
    }

    uint64_t term;
    const uint8_t *data;
    size_t len;
    if (kvidxGet(&db->db, maxKey, &term, NULL, &data, &len)) {
        *timestamp = maxKey;
        *sensorId = term;
        if (len == sizeof(SensorReading)) {
            memcpy(reading, data, sizeof(*reading));
        }
        return true;
    }
    return false;
}

// Get statistics
void tsdb_stats(TimeSeriesDB *db) {
    kvidxStats stats;
    kvidxGetStats(&db->db, &stats);

    printf("Time-Series DB Statistics:\n");
    printf("  Total data points: %" PRIu64 "\n", stats.totalKeys);
    if (stats.totalKeys > 0) {
        printf("  Time range: %" PRIu64 " to %" PRIu64 "\n", stats.minKey,
               stats.maxKey);
        printf("  Data size: %" PRIu64 " bytes\n", stats.totalDataBytes);
    }
    printf("  Database file: %" PRIu64 " bytes\n", stats.databaseFileSize);
}

// Aggregation helper
typedef struct {
    size_t count;
    float sum_temp;
    float sum_humidity;
    float sum_pressure;
    float min_temp;
    float max_temp;
} AggregationResult;

void aggregation_callback(uint64_t timestamp, uint64_t sensorId,
                          const SensorReading *reading, void *userData) {
    AggregationResult *result = (AggregationResult *)userData;
    (void)timestamp;
    (void)sensorId;

    result->count++;
    result->sum_temp += reading->temperature;
    result->sum_humidity += reading->humidity;
    result->sum_pressure += reading->pressure;

    if (reading->temperature < result->min_temp) {
        result->min_temp = reading->temperature;
    }
    if (reading->temperature > result->max_temp) {
        result->max_temp = reading->temperature;
    }
}

// Print callback
void print_callback(uint64_t timestamp, uint64_t sensorId,
                    const SensorReading *reading, void *userData) {
    (void)userData;
    printf("  [%" PRIu64 "] Sensor %" PRIu64
           ": temp=%.1f°C humidity=%.1f%% pressure=%.1fhPa\n",
           timestamp, sensorId, reading->temperature, reading->humidity,
           reading->pressure);
}

int main(void) {
    printf("=== kvidxkit Time-Series Example ===\n\n");

    TimeSeriesDB db;
    if (!tsdb_init(&db, "timeseries.db")) {
        return 1;
    }

    // Generate sample data
    printf("1. Generating sensor data:\n");

    uint64_t baseTime = current_time_ms() - 60000; // Start 60 seconds ago
    kvidxEntry entries[100];
    SensorReading readings[100];

    for (int i = 0; i < 100; i++) {
        uint64_t sensorId = (i % 3) + 1; // 3 sensors

        // Simulate sensor readings with some variation
        readings[i].temperature =
            20.0f + 5.0f * sinf(i * 0.1f) + (rand() % 10) / 10.0f;
        readings[i].humidity =
            50.0f + 10.0f * cosf(i * 0.15f) + (rand() % 10) / 10.0f;
        readings[i].pressure = 1013.0f + 5.0f * sinf(i * 0.05f);

        entries[i].key = baseTime + i * 600; // 600ms apart
        entries[i].term = sensorId;
        entries[i].cmd = 0;
        entries[i].data = &readings[i];
        entries[i].dataLen = sizeof(SensorReading);
    }

    // Batch insert
    tsdb_insert_batch(&db, entries, 100);
    printf("   Inserted 100 data points from 3 sensors\n");

    // Show statistics
    printf("\n2. Database statistics:\n");
    tsdb_stats(&db);

    // Query recent data
    printf("\n3. Last 10 data points:\n");
    uint64_t endTime = baseTime + 100 * 600;
    uint64_t startTime = endTime - 10 * 600;
    tsdb_query_range(&db, startTime, endTime, print_callback, NULL);

    // Aggregate query
    printf("\n4. Aggregation for all data:\n");
    AggregationResult agg = {0, 0, 0, 0, 1000, -1000};
    tsdb_query_range(&db, 0, UINT64_MAX, aggregation_callback, &agg);

    printf("   Data points: %zu\n", agg.count);
    printf("   Avg temperature: %.2f°C\n", agg.sum_temp / agg.count);
    printf("   Min/Max temperature: %.2f°C / %.2f°C\n", agg.min_temp,
           agg.max_temp);
    printf("   Avg humidity: %.2f%%\n", agg.sum_humidity / agg.count);
    printf("   Avg pressure: %.2fhPa\n", agg.sum_pressure / agg.count);

    // Count in time window
    printf("\n5. Counting data points:\n");
    uint64_t count = tsdb_count_range(&db, baseTime, baseTime + 30 * 600);
    printf("   First 30 seconds: %" PRIu64 " points\n", count);

    count = tsdb_count_range(&db, baseTime + 30 * 600, baseTime + 60 * 600);
    printf("   Second 30 seconds: %" PRIu64 " points\n", count);

    // Get latest reading
    printf("\n6. Latest reading:\n");
    uint64_t latestTime, latestSensor;
    SensorReading latestReading;
    if (tsdb_get_latest(&db, &latestTime, &latestSensor, &latestReading)) {
        printf("   Timestamp: %" PRIu64 "\n", latestTime);
        printf("   Sensor: %" PRIu64 "\n", latestSensor);
        printf("   Temperature: %.2f°C\n", latestReading.temperature);
    }

    // Apply retention policy (delete old data)
    printf("\n7. Applying retention policy (delete first 50 points):\n");
    uint64_t cutoff = baseTime + 50 * 600;
    uint64_t deleted = tsdb_apply_retention(&db, cutoff);
    printf("   Deleted %" PRIu64 " old data points\n", deleted);

    // Show updated statistics
    printf("\n8. Statistics after retention:\n");
    tsdb_stats(&db);

    // Export data
    printf("\n9. Exporting remaining data to JSON:\n");
    kvidxExportOptions exportOpts = {.format = KVIDX_EXPORT_JSON,
                                     .prettyPrint = true};
    kvidxExport(&db.db, "timeseries_export.json", &exportOpts, NULL, NULL);
    printf("   Exported to timeseries_export.json\n");

    tsdb_close(&db);

    // Cleanup
    remove("timeseries.db");
    remove("timeseries.db-wal");
    remove("timeseries.db-shm");
    remove("timeseries_export.json");

    printf("\n=== Example Complete ===\n");
    return 0;
}
