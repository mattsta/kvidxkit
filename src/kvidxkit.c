#include "kvidxkit.h"
#include "kvidxkitAdapterSqlite3.h"

/* ====================================================================
 * Sqlite3 Implementation
 * ==================================================================== */
const kvidxInterface kvidxInterfaceSqlite3 = {
    .begin = kvidxSqlite3Begin,
    .commit = kvidxSqlite3Commit,
    .get = kvidxSqlite3Get,
    .getPrev = kvidxSqlite3GetPrev,
    .getNext = kvidxSqlite3GetNext,
    .exists = kvidxSqlite3Exists,
    .existsDual = kvidxSqlite3ExistsDual,
    .maxKey = kvidxSqlite3Max,
    .insert = kvidxSqlite3Insert,
    .remove = kvidxSqlite3Remove,
    .removeAfterNInclusive = kvidxSqlite3RemoveAfterNInclusive,
    .fsync = kvidxSqlite3Fsync,
    .open = kvidxSqlite3Open,
    .close = kvidxSqlite3Close};

/* ====================================================================
 * User API
 * ==================================================================== */
/* Lazy debug flag to see where callers hit our API. */
#ifdef KVIDX_BASIC_DEBUG
#include <stdio.h>
#define VERBOSE_TAG()                                                          \
    do {                                                                       \
        printf("%s();\n", __func__);                                           \
    } while (0)
#else
#define VERBOSE_TAG()
#endif

bool kvidxOpen(kvidxInstance *i, const char *filename, const char **err) {
    VERBOSE_TAG();
    return i->interface.open(i, filename, err);
}

bool kvidxClose(kvidxInstance *i) {
    VERBOSE_TAG();
    return i->interface.close(i);
}

bool kvidxBegin(kvidxInstance *i) {
    VERBOSE_TAG();
    return i->interface.begin(i);
}

bool kvidxCommit(kvidxInstance *i) {
    VERBOSE_TAG();
    return i->interface.commit(i);
}

bool kvidxGet(kvidxInstance *i, uint64_t key, uint64_t *term, uint64_t *cmd,
              const uint8_t **data, size_t *len) {
    VERBOSE_TAG();
    return i->interface.get(i, key, term, cmd, data, len);
}

bool kvidxGetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                  uint64_t *prevTerm, uint64_t *cmd, const uint8_t **data,
                  size_t *len) {
    VERBOSE_TAG();
    return i->interface.getPrev(i, nextKey, prevKey, prevTerm, cmd, data, len);
}

bool kvidxGetNext(kvidxInstance *i, uint64_t previousKey, uint64_t *nextKey,
                  uint64_t *nextTerm, uint64_t *cmd, const uint8_t **data,
                  size_t *len) {
    VERBOSE_TAG();
    return i->interface.getNext(i, previousKey, nextKey, nextTerm, cmd, data,
                                len);
}

bool kvidxExists(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    return i->interface.exists(i, key);
}

bool kvidxExistsDual(kvidxInstance *i, uint64_t key, uint64_t term) {
    VERBOSE_TAG();
    return i->interface.existsDual(i, key, term);
}

bool kvidxMaxKey(kvidxInstance *i, uint64_t *key) {
    VERBOSE_TAG();
    return i->interface.maxKey(i, key);
}

bool kvidxInsert(kvidxInstance *i, uint64_t key, uint64_t term, uint64_t cmd,
                 const void *data, size_t dataLen) {
    VERBOSE_TAG();
    return i->interface.insert(i, key, term, cmd, data, dataLen);
}

bool kvidxRemove(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    return i->interface.remove(i, key);
}

bool kvidxRemoveAfterNInclusive(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    return i->interface.removeAfterNInclusive(i, key);
}

bool kvidxFsync(kvidxInstance *i) {
    VERBOSE_TAG();
    return i->interface.fsync(i);
}

bool kvidxApplyToStateMachine(kvidxInstance *i, uint64_t key) {
    VERBOSE_TAG();
    return i->state.applyToStateMachine(i, key);
}
