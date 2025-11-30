/**
 * kvidxkit Adapter Registry Implementation
 *
 * Provides runtime discovery of enabled adapters via a compile-time
 * populated static array.
 */
#include "kvidxkitRegistry.h"
#include "kvidxkit.h"
#include <string.h>
#include <strings.h> /* For strcasecmp */

/* ====================================================================
 * Static Adapter Registry
 * ====================================================================
 * This array is populated at compile time based on which adapters
 * are enabled via KVIDXKIT_HAS_* defines.
 */
static const kvidxAdapterInfo g_adapters[] = {
#ifdef KVIDXKIT_HAS_SQLITE3
    {.name = "SQLite3",
     .iface = &kvidxInterfaceSqlite3,
     .pathSuffix = ".sqlite3",
     .isDirectory = false},
#endif
#ifdef KVIDXKIT_HAS_LMDB
    {.name = "LMDB",
     .iface = &kvidxInterfaceLmdb,
     .pathSuffix = "",
     .isDirectory = true},
#endif
#ifdef KVIDXKIT_HAS_ROCKSDB
    {.name = "RocksDB",
     .iface = &kvidxInterfaceRocksdb,
     .pathSuffix = "",
     .isDirectory = true},
#endif
};

#define ADAPTER_COUNT (sizeof(g_adapters) / sizeof(g_adapters[0]))

/* ====================================================================
 * Public API
 * ==================================================================== */

size_t kvidxGetAdapterCount(void) {
    return ADAPTER_COUNT;
}

const kvidxAdapterInfo *kvidxGetAdapterByIndex(size_t index) {
    if (index >= ADAPTER_COUNT) {
        return NULL;
    }
    return &g_adapters[index];
}

const kvidxAdapterInfo *kvidxGetAdapterByName(const char *name) {
    if (!name) {
        return NULL;
    }
    for (size_t i = 0; i < ADAPTER_COUNT; i++) {
        if (strcasecmp(g_adapters[i].name, name) == 0) {
            return &g_adapters[i];
        }
    }
    return NULL;
}

bool kvidxHasAdapter(const char *name) {
    return kvidxGetAdapterByName(name) != NULL;
}
