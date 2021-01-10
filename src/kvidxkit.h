#pragma once

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

__BEGIN_DECLS

/* Pre-declare instance because Interface<->Instance use each other. */
struct kvidxInstance;

typedef struct kvidxInterface {
    /* CACHE LINE 1 */
    bool (*begin)(struct kvidxInstance *i);
    bool (*commit)(struct kvidxInstance *i);

    bool (*get)(struct kvidxInstance *i, uint64_t key, uint64_t *term,
                uint64_t *cmd, const uint8_t **data, size_t *len);

    bool (*getPrev)(struct kvidxInstance *i, uint64_t previousKey,
                    uint64_t *nextKey, uint64_t *nextTerm, uint64_t *cmd,
                    const uint8_t **data, size_t *len);
    bool (*getNext)(struct kvidxInstance *i, uint64_t previousKey,
                    uint64_t *nextKey, uint64_t *nextTerm, uint64_t *cmd,
                    const uint8_t **data, size_t *len);

    bool (*exists)(struct kvidxInstance *i, uint64_t key);
    bool (*existsDual)(struct kvidxInstance *i, uint64_t key, uint64_t term);

    bool (*maxKey)(struct kvidxInstance *i, uint64_t *key);

    /* CACHE LINE 2 */
    bool (*insert)(struct kvidxInstance *i, uint64_t key, uint64_t term,
                   uint64_t cmd, const void *data, size_t dataLen);

    bool (*remove)(struct kvidxInstance *i, uint64_t key);
    bool (*removeAfterNInclusive)(struct kvidxInstance *i, uint64_t key);

    bool (*fsync)(struct kvidxInstance *i);

    bool (*copyStorageForReplication)(struct kvidxInstance *i,
                                      void (*networkWrite)(void *, void *,
                                                           size_t),
                                      void *networkState);
    bool (*copyStorageForReplicationReceive)(struct kvidxInstance *i);

    bool (*copyStorageForBackup)(struct kvidxInstance *i, void *storageTarget);
    bool (*applyToStateMachine)(struct kvidxInstance *i, uint64_t key);

    /* CACHE LINE 3 */
    bool (*open)(struct kvidxInstance *i, const char *filename,
                 const char **err);
    bool (*close)(struct kvidxInstance *i);
} kvidxInterface;

typedef struct kvidxInterfaceStateMachine {
    bool (*applyToStateMachine)(struct kvidxInstance *i, uint64_t key);
    bool (*resurrectStateMachineFromStorage)(struct kvidxInstance *i);
} kvidxInterfaceStateMachine;

typedef struct kvidxInstance {
    void *kvidxdata;
    void *clientdata;
    kvidxInterface interface;
    kvidxInterfaceStateMachine state;
    bool (*customInit)(struct kvidxInstance *i);
} kvidxInstance;

/* Export identifier for interfaces distributed inside kvidxkit itself */
extern const kvidxInterface kvidxInterfaceSqlite3;

/* Open / Close / Management */
bool kvidxOpen(kvidxInstance *i, const char *filename, const char **err);
bool kvidxClose(kvidxInstance *i);
bool kvidxFsync(kvidxInstance *i);

/* Transactional Management */
bool kvidxBegin(struct kvidxInstance *i);
bool kvidxCommit(struct kvidxInstance *i);
/* We don't currently provide an Abort() because transactions are only
 * used to inform the DB it only needs one fsync() for appending
 * many entries at the same time.  We don't need rollback behavior. */

/* Reading */
bool kvidxGet(kvidxInstance *i, uint64_t key, uint64_t *term, uint64_t *cmd,
              const uint8_t **data, size_t *len);
bool kvidxGetPrev(kvidxInstance *i, uint64_t previousKey, uint64_t *nextKey,
                  uint64_t *nextTerm, uint64_t *cmd, const uint8_t **data,
                  size_t *len);
bool kvidxGetNext(kvidxInstance *i, uint64_t previousKey, uint64_t *nextKey,
                  uint64_t *nextTerm, uint64_t *cmd, const uint8_t **data,
                  size_t *len);
bool kvidxExists(kvidxInstance *i, uint64_t key);
bool kvidxExistsDual(kvidxInstance *i, uint64_t key, uint64_t term);
bool kvidxMaxKey(kvidxInstance *i, uint64_t *key);
bool kvidxInsert(kvidxInstance *i, uint64_t key, uint64_t term, uint64_t cmd,
                 const void *data, size_t dataLen);

/* Deleting */
bool kvidxRemove(kvidxInstance *i, uint64_t key);
bool kvidxRemoveAfterNInclusive(kvidxInstance *i, uint64_t key);

__END_DECLS
