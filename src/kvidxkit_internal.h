#pragma once

#include "kvidxkit.h"

/**
 * Internal helper function to set error on instance
 * Used by adapters to report detailed errors
 *
 * @param i Instance handle
 * @param err Error code
 * @param fmt Printf-style format string (NULL to use default error string)
 * @param ... Format arguments
 */
void kvidxSetError(kvidxInstance *i, kvidxError err, const char *fmt, ...);

/**
 * Helper macro to set error and return false
 */
#define KVIDX_SET_ERROR_RETURN(inst, err, ...)                                 \
    do {                                                                       \
        kvidxSetError((inst), (err), ##__VA_ARGS__);                           \
        return false;                                                          \
    } while (0)

/**
 * Helper macro to set OK and return true
 */
#define KVIDX_SET_OK_RETURN(inst)                                              \
    do {                                                                       \
        kvidxSetError((inst), KVIDX_OK, NULL);                                 \
        return true;                                                           \
    } while (0)
