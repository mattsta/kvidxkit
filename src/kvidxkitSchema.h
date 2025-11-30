#pragma once

#include "kvidxkitErrors.h"
#include "kvidxkitTableDesc.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Forward declaration for sqlite3 */
struct sqlite3;

__BEGIN_DECLS

/* ============================================================================
 * Migration Definition
 * ============================================================================
 * Migrations define schema changes. Each migration has a version number,
 * description, and SQL to apply (and optionally to rollback).
 */

typedef struct kvidxMigration {
    uint32_t version;        /* Migration version number */
    const char *description; /* Human-readable description */
    const char *upSQL;       /* SQL to apply migration */
    const char *downSQL;     /* SQL to rollback (optional, can be NULL) */
} kvidxMigration;

/* ============================================================================
 * Schema API
 * ============================================================================
 */

/**
 * Initialize schema versioning table
 *
 * Creates the _kvidx_schema table if it doesn't exist. This is called
 * automatically by other schema functions, but can be called explicitly.
 *
 * @param db SQLite database handle
 * @return KVIDX_OK on success
 */
kvidxError kvidxSchemaInit(struct sqlite3 *db);

/**
 * Get current schema version
 *
 * @param db SQLite database handle
 * @param version Receives current version (0 if no migrations applied)
 * @return KVIDX_OK on success
 */
kvidxError kvidxSchemaVersion(struct sqlite3 *db, uint32_t *version);

/**
 * Check if migrations are needed
 *
 * @param db SQLite database handle
 * @param targetVersion Desired schema version
 * @return true if current version < targetVersion
 */
bool kvidxSchemaNeedsMigration(struct sqlite3 *db, uint32_t targetVersion);

/**
 * Apply migrations to reach target version
 *
 * Migrations are applied in order from current version to target version.
 * Each migration is wrapped in a transaction.
 *
 * @param db SQLite database handle
 * @param migrations Array of migrations
 * @param count Number of migrations in array
 * @param targetVersion Target schema version
 * @return KVIDX_OK on success, error code on failure
 */
kvidxError kvidxSchemaApply(struct sqlite3 *db,
                            const kvidxMigration *migrations, size_t count,
                            uint32_t targetVersion);

/**
 * Apply migrations with progress callback
 *
 * @param db SQLite database handle
 * @param migrations Array of migrations
 * @param count Number of migrations in array
 * @param targetVersion Target schema version
 * @param callback Called after each migration (can be NULL)
 * @param userData Passed to callback
 * @return KVIDX_OK on success
 */
typedef void (*kvidxMigrationCallback)(uint32_t version,
                                       const char *description, bool success,
                                       void *userData);

kvidxError kvidxSchemaApplyWithCallback(struct sqlite3 *db,
                                        const kvidxMigration *migrations,
                                        size_t count, uint32_t targetVersion,
                                        kvidxMigrationCallback callback,
                                        void *userData);

/**
 * Create tables from definitions
 *
 * Convenience function to create tables and indexes from kvidxTableDef array.
 *
 * @param db SQLite database handle
 * @param tables Array of table definitions
 * @param count Number of tables
 * @return KVIDX_OK on success
 */
kvidxError kvidxSchemaCreateTables(struct sqlite3 *db,
                                   const kvidxTableDef *tables, size_t count);

/**
 * Get list of applied migration versions
 *
 * @param db SQLite database handle
 * @param versions Array to receive versions (caller allocates)
 * @param maxCount Maximum versions to retrieve
 * @param actualCount Receives actual count of versions
 * @return KVIDX_OK on success
 */
kvidxError kvidxSchemaGetAppliedVersions(struct sqlite3 *db, uint32_t *versions,
                                         size_t maxCount, size_t *actualCount);

__END_DECLS
