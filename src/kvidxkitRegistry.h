/**
 * kvidxkit Adapter Registry
 *
 * Provides runtime discovery of enabled adapters. Use these functions
 * to enumerate available adapters without compile-time knowledge of
 * which adapters are enabled.
 *
 * Example usage:
 *   size_t count = kvidxGetAdapterCount();
 *   for (size_t i = 0; i < count; i++) {
 *       const kvidxAdapterInfo *info = kvidxGetAdapterByIndex(i);
 *       printf("Adapter: %s\n", info->name);
 *   }
 */
#pragma once

#include <stdbool.h>
#include <stddef.h>

/* Forward declaration to avoid circular include */
struct kvidxInterface;

/**
 * Adapter descriptor for runtime discovery.
 */
typedef struct kvidxAdapterInfo {
    const char *name;
    const struct kvidxInterface *iface;
    const char *pathSuffix;
    bool isDirectory;
} kvidxAdapterInfo;

/**
 * Get count of enabled adapters.
 *
 * @return Number of adapters compiled into the library (0 if none)
 */
size_t kvidxGetAdapterCount(void);

/**
 * Get adapter info by index.
 *
 * @param index Zero-based index (0 to kvidxGetAdapterCount()-1)
 * @return Pointer to adapter info, or NULL if index out of range
 */
const kvidxAdapterInfo *kvidxGetAdapterByIndex(size_t index);

/**
 * Get adapter info by name (case-insensitive).
 *
 * @param name Adapter name (e.g., "sqlite3", "LMDB", "RocksDB")
 * @return Pointer to adapter info, or NULL if not found
 */
const kvidxAdapterInfo *kvidxGetAdapterByName(const char *name);

/**
 * Check if a specific adapter is available.
 *
 * @param name Adapter name (case-insensitive)
 * @return true if adapter is compiled into the library
 */
bool kvidxHasAdapter(const char *name);
