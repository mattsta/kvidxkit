#pragma once

#include "kvidxkit.h"
__BEGIN_DECLS

/* Open / Close / Management */
bool kvidxLmdbOpen(kvidxInstance *i, const char *filename, const char **err);
bool kvidxLmdbClose(kvidxInstance *i);
bool kvidxLmdbFsync(kvidxInstance *i);

/* Transactional Control */
bool kvidxLmdbBegin(kvidxInstance *i);
bool kvidxLmdbCommit(kvidxInstance *i);

/* Reading */
bool kvidxLmdbGet(kvidxInstance *i, uint64_t key, uint64_t *term, uint64_t *cmd,
                  const uint8_t **data, size_t *len);
bool kvidxLmdbGetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                      uint64_t *prevTerm, uint64_t *cmd, const uint8_t **data,
                      size_t *len);
bool kvidxLmdbGetNext(kvidxInstance *i, uint64_t previousKey, uint64_t *nextKey,
                      uint64_t *nextTerm, uint64_t *cmd, const uint8_t **data,
                      size_t *len);
bool kvidxLmdbExists(kvidxInstance *i, uint64_t key);
bool kvidxLmdbExistsDual(kvidxInstance *i, uint64_t key, uint64_t term);
bool kvidxLmdbMax(kvidxInstance *i, uint64_t *key);
bool kvidxLmdbInsert(kvidxInstance *i, uint64_t key, uint64_t term,
                     uint64_t cmd, const void *data, size_t dataLen);

/* Deleting */
bool kvidxLmdbRemove(kvidxInstance *i, uint64_t key);
bool kvidxLmdbRemoveAfterNInclusive(kvidxInstance *i, uint64_t key);
bool kvidxLmdbRemoveBeforeNInclusive(kvidxInstance *i, uint64_t key);

/* Statistics (v0.5.0) */
kvidxError kvidxLmdbGetStats(kvidxInstance *i, kvidxStats *stats);
kvidxError kvidxLmdbGetKeyCount(kvidxInstance *i, uint64_t *count);
kvidxError kvidxLmdbGetMinKey(kvidxInstance *i, uint64_t *key);
kvidxError kvidxLmdbGetDataSize(kvidxInstance *i, uint64_t *bytes);

/* Configuration (v0.5.0) */
kvidxError kvidxLmdbApplyConfig(kvidxInstance *i, const kvidxConfig *config);

/* Range Operations (v0.5.0) */
kvidxError kvidxLmdbRemoveRange(kvidxInstance *i, uint64_t startKey,
                                uint64_t endKey, bool startInclusive,
                                bool endInclusive, uint64_t *deletedCount);
kvidxError kvidxLmdbCountRange(kvidxInstance *i, uint64_t startKey,
                               uint64_t endKey, uint64_t *count);
kvidxError kvidxLmdbExistsInRange(kvidxInstance *i, uint64_t startKey,
                                  uint64_t endKey, bool *exists);

/* Export/Import (v0.6.0) */
kvidxError kvidxLmdbExport(kvidxInstance *i, const char *filename,
                           const kvidxExportOptions *options,
                           kvidxProgressCallback callback, void *userData);
kvidxError kvidxLmdbImport(kvidxInstance *i, const char *filename,
                           const kvidxImportOptions *options,
                           kvidxProgressCallback callback, void *userData);

/* Storage Primitives (v0.8.0) */
/* Conditional writes */
kvidxError kvidxLmdbInsertEx(kvidxInstance *i, uint64_t key, uint64_t term,
                             uint64_t cmd, const void *data, size_t dataLen,
                             kvidxSetCondition condition);

/* Transaction abort */
bool kvidxLmdbAbort(kvidxInstance *i);

/* Atomic operations */
kvidxError kvidxLmdbGetAndSet(kvidxInstance *i, uint64_t key, uint64_t term,
                              uint64_t cmd, const void *data, size_t dataLen,
                              uint64_t *oldTerm, uint64_t *oldCmd,
                              void **oldData, size_t *oldDataLen);
kvidxError kvidxLmdbGetAndRemove(kvidxInstance *i, uint64_t key, uint64_t *term,
                                 uint64_t *cmd, void **data, size_t *dataLen);

/* Compare-and-swap */
kvidxError kvidxLmdbCompareAndSwap(kvidxInstance *i, uint64_t key,
                                   const void *expectedData, size_t expectedLen,
                                   uint64_t newTerm, uint64_t newCmd,
                                   const void *newData, size_t newDataLen,
                                   bool *swapped);

/* Append/Prepend */
kvidxError kvidxLmdbAppend(kvidxInstance *i, uint64_t key, uint64_t term,
                           uint64_t cmd, const void *data, size_t dataLen,
                           size_t *newLen);
kvidxError kvidxLmdbPrepend(kvidxInstance *i, uint64_t key, uint64_t term,
                            uint64_t cmd, const void *data, size_t dataLen,
                            size_t *newLen);

/* Partial value access */
kvidxError kvidxLmdbGetValueRange(kvidxInstance *i, uint64_t key, size_t offset,
                                  size_t length, void **data,
                                  size_t *actualLen);
kvidxError kvidxLmdbSetValueRange(kvidxInstance *i, uint64_t key, size_t offset,
                                  const void *data, size_t dataLen,
                                  size_t *newLen);

/* TTL/Expiration */
kvidxError kvidxLmdbSetExpire(kvidxInstance *i, uint64_t key, uint64_t ttlMs);
kvidxError kvidxLmdbSetExpireAt(kvidxInstance *i, uint64_t key,
                                uint64_t timestampMs);
int64_t kvidxLmdbGetTTL(kvidxInstance *i, uint64_t key);
kvidxError kvidxLmdbPersist(kvidxInstance *i, uint64_t key);
kvidxError kvidxLmdbExpireScan(kvidxInstance *i, uint64_t maxKeys,
                               uint64_t *expiredCount);

__END_DECLS
