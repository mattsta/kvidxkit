/**
 * Iterator implementation for kvidxkit
 * Provides convenient iteration over database entries
 */

#include "kvidxkitIterator.h"
#include "kvidxkit.h"
#include <stdlib.h>
#include <string.h>

struct kvidxIterator {
    kvidxInstance *instance;
    uint64_t startKey;
    uint64_t endKey;
    kvidxIterDirection direction;

    /* Current position */
    bool valid;
    uint64_t currentKey;
    uint64_t currentTerm;
    uint64_t currentCmd;
    const uint8_t *currentData;
    size_t currentDataLen;

    /* State tracking */
    bool initialized; /* Has Next been called at least once? */
};

kvidxIterator *kvidxIteratorCreate(kvidxInstance *i, uint64_t startKey,
                                   uint64_t endKey,
                                   kvidxIterDirection direction) {
    if (!i) {
        return NULL;
    }

    /* Validate range */
    if (startKey > endKey) {
        return NULL;
    }

    kvidxIterator *it = calloc(1, sizeof(kvidxIterator));
    if (!it) {
        return NULL;
    }

    it->instance = i;
    it->startKey = startKey;
    it->endKey = endKey;
    it->direction = direction;
    it->valid = false;
    it->initialized = false;

    return it;
}

bool kvidxIteratorNext(kvidxIterator *it) {
    if (!it || !it->instance) {
        return false;
    }

    /* First call - position at start */
    if (!it->initialized) {
        it->initialized = true;

        if (it->direction == KVIDX_ITER_FORWARD) {
            /* Find first key >= startKey */
            if (it->startKey == 0) {
                /* Start from minimum key */
                uint64_t minKey = 0;
                if (!kvidxMaxKey(it->instance, &minKey)) {
                    /* Empty database */
                    it->valid = false;
                    return false;
                }

                /* Get minimum by iterating from 0 */
                bool found = kvidxGetNext(
                    it->instance, 0, &it->currentKey, &it->currentTerm,
                    &it->currentCmd, &it->currentData, &it->currentDataLen);

                if (!found || it->currentKey > it->endKey) {
                    it->valid = false;
                    return false;
                }

                it->valid = true;
                return true;
            } else {
                /* Try exact match first */
                if (kvidxGet(it->instance, it->startKey, &it->currentTerm,
                             &it->currentCmd, &it->currentData,
                             &it->currentDataLen)) {
                    it->currentKey = it->startKey;

                    if (it->currentKey > it->endKey) {
                        it->valid = false;
                        return false;
                    }

                    it->valid = true;
                    return true;
                }

                /* No exact match - get next key after startKey */
                bool found = kvidxGetNext(it->instance, it->startKey - 1,
                                          &it->currentKey, &it->currentTerm,
                                          &it->currentCmd, &it->currentData,
                                          &it->currentDataLen);

                if (!found || it->currentKey > it->endKey) {
                    it->valid = false;
                    return false;
                }

                it->valid = true;
                return true;
            }
        } else {
            /* BACKWARD - start from endKey */
            if (it->endKey == UINT64_MAX) {
                /* Start from maximum key */
                uint64_t maxKey;
                if (!kvidxMaxKey(it->instance, &maxKey)) {
                    /* Empty database */
                    it->valid = false;
                    return false;
                }

                /* Get the max key entry */
                if (!kvidxGet(it->instance, maxKey, &it->currentTerm,
                              &it->currentCmd, &it->currentData,
                              &it->currentDataLen)) {
                    it->valid = false;
                    return false;
                }

                it->currentKey = maxKey;

                if (it->currentKey < it->startKey) {
                    it->valid = false;
                    return false;
                }

                it->valid = true;
                return true;
            } else {
                /* Try exact match at endKey */
                if (kvidxGet(it->instance, it->endKey, &it->currentTerm,
                             &it->currentCmd, &it->currentData,
                             &it->currentDataLen)) {
                    it->currentKey = it->endKey;

                    if (it->currentKey < it->startKey) {
                        it->valid = false;
                        return false;
                    }

                    it->valid = true;
                    return true;
                }

                /* No exact match - get previous key before endKey */
                bool found =
                    kvidxGetPrev(it->instance, it->endKey + 1, &it->currentKey,
                                 &it->currentTerm, &it->currentCmd,
                                 &it->currentData, &it->currentDataLen);

                if (!found || it->currentKey < it->startKey) {
                    it->valid = false;
                    return false;
                }

                it->valid = true;
                return true;
            }
        }
    }

    /* Subsequent calls - advance to next entry */
    if (!it->valid) {
        return false;
    }

    if (it->direction == KVIDX_ITER_FORWARD) {
        /* Get next key after current */
        bool found = kvidxGetNext(it->instance, it->currentKey, &it->currentKey,
                                  &it->currentTerm, &it->currentCmd,
                                  &it->currentData, &it->currentDataLen);

        if (!found || it->currentKey > it->endKey) {
            it->valid = false;
            return false;
        }

        return true;
    } else {
        /* Get previous key before current */
        bool found = kvidxGetPrev(it->instance, it->currentKey, &it->currentKey,
                                  &it->currentTerm, &it->currentCmd,
                                  &it->currentData, &it->currentDataLen);

        if (!found || it->currentKey < it->startKey) {
            it->valid = false;
            return false;
        }

        return true;
    }
}

bool kvidxIteratorGet(const kvidxIterator *it, uint64_t *key, uint64_t *term,
                      uint64_t *cmd, const uint8_t **data, size_t *len) {
    if (!it || !it->valid) {
        return false;
    }

    if (key) {
        *key = it->currentKey;
    }
    if (term) {
        *term = it->currentTerm;
    }
    if (cmd) {
        *cmd = it->currentCmd;
    }
    if (data) {
        *data = it->currentData;
    }
    if (len) {
        *len = it->currentDataLen;
    }

    return true;
}

uint64_t kvidxIteratorKey(const kvidxIterator *it) {
    if (!it || !it->valid) {
        return 0;
    }
    return it->currentKey;
}

bool kvidxIteratorValid(const kvidxIterator *it) {
    if (!it) {
        return false;
    }
    return it->valid;
}

bool kvidxIteratorSeek(kvidxIterator *it, uint64_t key) {
    if (!it || !it->instance) {
        return false;
    }

    /* Check if key is in range */
    if (key < it->startKey || key > it->endKey) {
        it->valid = false;
        return false;
    }

    /* Try exact match */
    if (kvidxGet(it->instance, key, &it->currentTerm, &it->currentCmd,
                 &it->currentData, &it->currentDataLen)) {
        it->currentKey = key;
        it->valid = true;
        it->initialized = true;
        return true;
    }

    /* No exact match - position based on direction */
    if (it->direction == KVIDX_ITER_FORWARD) {
        /* Seek to next key after target */
        bool found = kvidxGetNext(it->instance, key - 1, &it->currentKey,
                                  &it->currentTerm, &it->currentCmd,
                                  &it->currentData, &it->currentDataLen);

        if (!found || it->currentKey > it->endKey) {
            it->valid = false;
            return false;
        }

        it->valid = true;
        it->initialized = true;
        return true;
    } else {
        /* Seek to previous key before target */
        bool found = kvidxGetPrev(it->instance, key + 1, &it->currentKey,
                                  &it->currentTerm, &it->currentCmd,
                                  &it->currentData, &it->currentDataLen);

        if (!found || it->currentKey < it->startKey) {
            it->valid = false;
            return false;
        }

        it->valid = true;
        it->initialized = true;
        return true;
    }
}

void kvidxIteratorDestroy(kvidxIterator *it) {
    if (it) {
        free(it);
    }
}
