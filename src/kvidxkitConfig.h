#pragma once

#include <stdbool.h>
#include <stddef.h>

__BEGIN_DECLS

/**
 * Journal mode configuration
 * Controls how SQLite manages the rollback journal
 */
typedef enum {
    KVIDX_JOURNAL_DELETE,   /**< Delete journal after each commit (default for
                               non-WAL) */
    KVIDX_JOURNAL_TRUNCATE, /**< Truncate journal to zero length instead of
                               deleting */
    KVIDX_JOURNAL_PERSIST,  /**< Keep journal file, zero header */
    KVIDX_JOURNAL_MEMORY,   /**< Store journal in memory (fast, unsafe) */
    KVIDX_JOURNAL_WAL,      /**< Write-Ahead Log mode (recommended) */
    KVIDX_JOURNAL_OFF       /**< No journal (dangerous, fast) */
} kvidxJournalMode;

/**
 * Synchronization mode configuration
 * Controls how aggressively SQLite syncs to disk
 */
typedef enum {
    KVIDX_SYNC_OFF,    /**< No sync (fast, unsafe - data loss on crash) */
    KVIDX_SYNC_NORMAL, /**< Sync at critical moments (balanced, default) */
    KVIDX_SYNC_FULL,   /**< Full sync guarantee (safe, slow) */
    KVIDX_SYNC_EXTRA   /**< Extra safety checks (paranoid) */
} kvidxSyncMode;

/**
 * Configuration structure for opening/managing database
 */
typedef struct {
    size_t cacheSizeBytes; /**< SQLite cache size (default: 32 MB) */
    const char *vfsName;   /**< VFS name (default: NULL for default VFS) */
    kvidxJournalMode journalMode; /**< Journal mode (default: WAL) */
    kvidxSyncMode syncMode;       /**< Sync mode (default: NORMAL) */
    bool enableRecursiveTriggers; /**< Enable recursive triggers (default: true)
                                   */
    bool enableForeignKeys; /**< Enable foreign key constraints (default: false)
                             */
    bool readOnly;          /**< Open in read-only mode (default: false) */
    int busyTimeoutMs;      /**< Busy timeout in milliseconds (default: 5000) */
    int mmapSizeBytes; /**< Memory-map I/O size, 0 to disable (default: 0) */
    int pageSize; /**< Page size in bytes, must be power of 2 (default: 4096,
                     0=default) */
} kvidxConfig;

__END_DECLS
