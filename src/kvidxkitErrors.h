#pragma once

#include <stdbool.h>
#include <stdint.h>

__BEGIN_DECLS

/**
 * Error codes for kvidxkit operations
 */
typedef enum {
    /** Operation completed successfully */
    KVIDX_OK = 0,

    /** Invalid argument provided (NULL pointer, out of range, etc.) */
    KVIDX_ERROR_INVALID_ARGUMENT = 1,

    /** Attempted to insert duplicate key */
    KVIDX_ERROR_DUPLICATE_KEY = 2,

    /** Requested key not found */
    KVIDX_ERROR_NOT_FOUND = 3,

    /** Disk full or write failed */
    KVIDX_ERROR_DISK_FULL = 4,

    /** I/O error (permissions, file corruption, etc.) */
    KVIDX_ERROR_IO = 5,

    /** Database corruption detected */
    KVIDX_ERROR_CORRUPT = 6,

    /** Transaction already active */
    KVIDX_ERROR_TRANSACTION_ACTIVE = 7,

    /** No active transaction */
    KVIDX_ERROR_NO_TRANSACTION = 8,

    /** Database is read-only */
    KVIDX_ERROR_READONLY = 9,

    /** Database is locked by another process */
    KVIDX_ERROR_LOCKED = 10,

    /** Out of memory */
    KVIDX_ERROR_NOMEM = 11,

    /** Operation would exceed size limits */
    KVIDX_ERROR_TOO_BIG = 12,

    /** Constraint violation (foreign key, check, etc.) */
    KVIDX_ERROR_CONSTRAINT = 13,

    /** Type mismatch or schema error */
    KVIDX_ERROR_SCHEMA = 14,

    /** Range error (start > end, etc.) */
    KVIDX_ERROR_RANGE = 15,

    /** Operation not supported by this backend */
    KVIDX_ERROR_NOT_SUPPORTED = 16,

    /** Operation cancelled by user */
    KVIDX_ERROR_CANCELLED = 17,

    /** Internal error (should not happen) */
    KVIDX_ERROR_INTERNAL = 99,

    /** Condition not met for conditional operation (v0.8.0) */
    KVIDX_ERROR_CONDITION_FAILED = 100,

    /** Key has expired (v0.8.0) */
    KVIDX_ERROR_EXPIRED = 101
} kvidxError;

/* ====================================================================
 * Conditional Write Modes (v0.8.0)
 * ==================================================================== */

/**
 * Write condition modes for conditional insert/update operations
 */
typedef enum {
    KVIDX_SET_ALWAYS = 0,        /**< Always set (default behavior) */
    KVIDX_SET_IF_NOT_EXISTS = 1, /**< Only set if key does NOT exist (NX) */
    KVIDX_SET_IF_EXISTS = 2      /**< Only set if key DOES exist (XX) */
} kvidxSetCondition;

/* ====================================================================
 * TTL Special Values (v0.8.0)
 * ==================================================================== */

#define KVIDX_TTL_NONE (-1)      /**< Key has no expiration */
#define KVIDX_TTL_NOT_FOUND (-2) /**< Key does not exist */

/**
 * Convert error code to human-readable string
 *
 * @param err Error code
 * @return Static string describing the error (never NULL)
 */
const char *kvidxErrorString(kvidxError err);

/**
 * Check if error code represents success
 *
 * @param err Error code to check
 * @return true if err == KVIDX_OK
 */
static inline bool kvidxIsOk(kvidxError err) {
    return err == KVIDX_OK;
}

/**
 * Check if error code represents an error
 *
 * @param err Error code to check
 * @return true if err != KVIDX_OK
 */
static inline bool kvidxIsError(kvidxError err) {
    return err != KVIDX_OK;
}

__END_DECLS
