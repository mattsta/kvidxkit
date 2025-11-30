#include "kvidxkitErrors.h"
#include <stddef.h>

const char *kvidxErrorString(kvidxError err) {
    switch (err) {
    case KVIDX_OK:
        return "Success";

    case KVIDX_ERROR_INVALID_ARGUMENT:
        return "Invalid argument";

    case KVIDX_ERROR_DUPLICATE_KEY:
        return "Duplicate key";

    case KVIDX_ERROR_NOT_FOUND:
        return "Key not found";

    case KVIDX_ERROR_DISK_FULL:
        return "Disk full";

    case KVIDX_ERROR_IO:
        return "I/O error";

    case KVIDX_ERROR_CORRUPT:
        return "Database corruption detected";

    case KVIDX_ERROR_TRANSACTION_ACTIVE:
        return "Transaction already active";

    case KVIDX_ERROR_NO_TRANSACTION:
        return "No active transaction";

    case KVIDX_ERROR_READONLY:
        return "Database is read-only";

    case KVIDX_ERROR_LOCKED:
        return "Database is locked";

    case KVIDX_ERROR_NOMEM:
        return "Out of memory";

    case KVIDX_ERROR_TOO_BIG:
        return "Data too large";

    case KVIDX_ERROR_CONSTRAINT:
        return "Constraint violation";

    case KVIDX_ERROR_SCHEMA:
        return "Schema error";

    case KVIDX_ERROR_RANGE:
        return "Invalid range";

    case KVIDX_ERROR_NOT_SUPPORTED:
        return "Operation not supported";

    case KVIDX_ERROR_CANCELLED:
        return "Operation cancelled";

    case KVIDX_ERROR_INTERNAL:
        return "Internal error";

    default:
        return "Unknown error";
    }
}
