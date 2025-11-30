#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

__BEGIN_DECLS

/**
 * Export/Import format types
 */
typedef enum {
    KVIDX_EXPORT_BINARY, /**< Compact binary format (fastest) */
    KVIDX_EXPORT_JSON,   /**< JSON format (human-readable) */
    KVIDX_EXPORT_CSV     /**< CSV format (spreadsheet-compatible) */
} kvidxExportFormat;

/**
 * Export options structure
 */
typedef struct {
    kvidxExportFormat format; /**< Output format */
    uint64_t startKey;        /**< Start of key range (0 = from beginning) */
    uint64_t endKey;          /**< End of key range (UINT64_MAX = to end) */
    bool includeMetadata;     /**< Include term/cmd metadata (CSV/JSON only) */
    bool prettyPrint; /**< Pretty-print JSON (ignored for other formats) */
} kvidxExportOptions;

/**
 * Import options structure
 */
typedef struct {
    kvidxExportFormat format; /**< Input format (BINARY = auto-detect) */
    bool validateData;        /**< Validate data during import */
    bool skipDuplicates;      /**< Skip duplicate keys instead of failing */
    bool clearBeforeImport;   /**< Clear database before importing */
} kvidxImportOptions;

/**
 * Progress callback for export/import operations
 *
 * @param current Current item being processed
 * @param total Total items to process (0 if unknown)
 * @param userData User-provided context
 * @return true to continue, false to abort
 */
typedef bool (*kvidxProgressCallback)(uint64_t current, uint64_t total,
                                      void *userData);

/**
 * Get default export options
 *
 * @return Export options with default values
 */
kvidxExportOptions kvidxExportOptionsDefault(void);

/**
 * Get default import options
 *
 * @return Import options with default values
 */
kvidxImportOptions kvidxImportOptionsDefault(void);

__END_DECLS
