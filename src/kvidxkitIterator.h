#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

__BEGIN_DECLS

/* Forward declaration */
struct kvidxInstance;
typedef struct kvidxIterator kvidxIterator;

/**
 * Iterator direction
 */
typedef enum {
    KVIDX_ITER_FORWARD, /* Iterate from start to end (ascending keys) */
    KVIDX_ITER_BACKWARD /* Iterate from end to start (descending keys) */
} kvidxIterDirection;

/**
 * Create iterator for range [startKey, endKey]
 *
 * Use 0 for startKey and UINT64_MAX for endKey for full scan.
 * The iterator is positioned before the first element - call
 * kvidxIteratorNext() to advance to the first entry.
 *
 * @param i Instance handle
 * @param startKey First key in range (inclusive)
 * @param endKey Last key in range (inclusive)
 * @param direction Forward or backward iteration
 * @return Iterator handle, or NULL on error
 *
 * @note Caller must call kvidxIteratorDestroy() when done
 * @note Iterator becomes invalid if database is modified
 */
kvidxIterator *kvidxIteratorCreate(struct kvidxInstance *i, uint64_t startKey,
                                   uint64_t endKey,
                                   kvidxIterDirection direction);

/**
 * Move to next entry in iteration
 *
 * @param it Iterator handle
 * @return true if advanced to next entry, false if no more entries
 */
bool kvidxIteratorNext(kvidxIterator *it);

/**
 * Get current entry (pointers valid until next call)
 *
 * @param it Iterator handle
 * @param key Receives key (can be NULL)
 * @param term Receives term (can be NULL)
 * @param cmd Receives cmd (can be NULL)
 * @param data Receives data pointer (can be NULL)
 * @param len Receives data length (can be NULL)
 * @return true if current entry retrieved, false if invalid
 *
 * @note Data pointer is valid until next iterator operation or iterator
 * destruction
 */
bool kvidxIteratorGet(const kvidxIterator *it, uint64_t *key, uint64_t *term,
                      uint64_t *cmd, const uint8_t **data, size_t *len);

/**
 * Convenience: get current key only
 *
 * @param it Iterator handle
 * @return Current key, or 0 if iterator is invalid
 */
uint64_t kvidxIteratorKey(const kvidxIterator *it);

/**
 * Check if iterator is positioned at a valid entry
 *
 * @param it Iterator handle
 * @return true if valid, false if at end or error
 */
bool kvidxIteratorValid(const kvidxIterator *it);

/**
 * Seek to specific key
 *
 * Positions iterator at the specified key if it exists.
 * If key doesn't exist, positions at next key (forward) or previous key
 * (backward).
 *
 * @param it Iterator handle
 * @param key Key to seek to
 * @return true if positioned successfully, false on error
 */
bool kvidxIteratorSeek(kvidxIterator *it, uint64_t key);

/**
 * Destroy iterator and free resources
 *
 * @param it Iterator handle (can be NULL)
 */
void kvidxIteratorDestroy(kvidxIterator *it);

__END_DECLS
