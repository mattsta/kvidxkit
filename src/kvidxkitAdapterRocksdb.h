#pragma once

#include "kvidxkit.h"
__BEGIN_DECLS

/* Open / Close / Management */
bool kvidxRocksdbOpen(kvidxInstance *i, const char *filename, const char **err);
bool kvidxRocksdbClose(kvidxInstance *i);
bool kvidxRocksdbFsync(kvidxInstance *i);

/* Transactional Control */
bool kvidxRocksdbBegin(kvidxInstance *i);
bool kvidxRocksdbCommit(kvidxInstance *i);

/* Reading */
bool kvidxRocksdbGet(kvidxInstance *i, uint64_t key, uint64_t *term,
                     uint64_t *cmd, const uint8_t **data, size_t *len);
bool kvidxRocksdbGetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                         uint64_t *prevTerm, uint64_t *cmd,
                         const uint8_t **data, size_t *len);
bool kvidxRocksdbGetNext(kvidxInstance *i, uint64_t previousKey,
                         uint64_t *nextKey, uint64_t *nextTerm, uint64_t *cmd,
                         const uint8_t **data, size_t *len);
bool kvidxRocksdbExists(kvidxInstance *i, uint64_t key);
bool kvidxRocksdbExistsDual(kvidxInstance *i, uint64_t key, uint64_t term);
bool kvidxRocksdbMax(kvidxInstance *i, uint64_t *key);
bool kvidxRocksdbInsert(kvidxInstance *i, uint64_t key, uint64_t term,
                        uint64_t cmd, const void *data, size_t dataLen);

/* Deleting */
bool kvidxRocksdbRemove(kvidxInstance *i, uint64_t key);
bool kvidxRocksdbRemoveAfterNInclusive(kvidxInstance *i, uint64_t key);
bool kvidxRocksdbRemoveBeforeNInclusive(kvidxInstance *i, uint64_t key);

/* Statistics (v0.5.0) */
kvidxError kvidxRocksdbGetStats(kvidxInstance *i, kvidxStats *stats);
kvidxError kvidxRocksdbGetKeyCount(kvidxInstance *i, uint64_t *count);
kvidxError kvidxRocksdbGetMinKey(kvidxInstance *i, uint64_t *key);
kvidxError kvidxRocksdbGetDataSize(kvidxInstance *i, uint64_t *bytes);

/* Configuration (v0.5.0) */
kvidxError kvidxRocksdbApplyConfig(kvidxInstance *i, const kvidxConfig *config);

/* Range Operations (v0.5.0) */
kvidxError kvidxRocksdbRemoveRange(kvidxInstance *i, uint64_t startKey,
                                   uint64_t endKey, bool startInclusive,
                                   bool endInclusive, uint64_t *deletedCount);
kvidxError kvidxRocksdbCountRange(kvidxInstance *i, uint64_t startKey,
                                  uint64_t endKey, uint64_t *count);
kvidxError kvidxRocksdbExistsInRange(kvidxInstance *i, uint64_t startKey,
                                     uint64_t endKey, bool *exists);

/* Export/Import (v0.6.0) */
kvidxError kvidxRocksdbExport(kvidxInstance *i, const char *filename,
                              const kvidxExportOptions *options,
                              kvidxProgressCallback callback, void *userData);
kvidxError kvidxRocksdbImport(kvidxInstance *i, const char *filename,
                              const kvidxImportOptions *options,
                              kvidxProgressCallback callback, void *userData);

/* Storage Primitives (v0.8.0) */
/* Conditional writes */
kvidxError kvidxRocksdbInsertEx(kvidxInstance *i, uint64_t key, uint64_t term,
                                uint64_t cmd, const void *data, size_t dataLen,
                                kvidxSetCondition condition);

/* Transaction abort */
bool kvidxRocksdbAbort(kvidxInstance *i);

/* Atomic operations */
kvidxError kvidxRocksdbGetAndSet(kvidxInstance *i, uint64_t key, uint64_t term,
                                 uint64_t cmd, const void *data, size_t dataLen,
                                 uint64_t *oldTerm, uint64_t *oldCmd,
                                 void **oldData, size_t *oldDataLen);
kvidxError kvidxRocksdbGetAndRemove(kvidxInstance *i, uint64_t key,
                                    uint64_t *term, uint64_t *cmd, void **data,
                                    size_t *dataLen);

/* Compare-and-swap */
kvidxError kvidxRocksdbCompareAndSwap(kvidxInstance *i, uint64_t key,
                                      const void *expectedData,
                                      size_t expectedLen, uint64_t newTerm,
                                      uint64_t newCmd, const void *newData,
                                      size_t newDataLen, bool *swapped);

/* Append/Prepend */
kvidxError kvidxRocksdbAppend(kvidxInstance *i, uint64_t key, uint64_t term,
                              uint64_t cmd, const void *data, size_t dataLen,
                              size_t *newLen);
kvidxError kvidxRocksdbPrepend(kvidxInstance *i, uint64_t key, uint64_t term,
                               uint64_t cmd, const void *data, size_t dataLen,
                               size_t *newLen);

/* Partial value access */
kvidxError kvidxRocksdbGetValueRange(kvidxInstance *i, uint64_t key,
                                     size_t offset, size_t length, void **data,
                                     size_t *actualLen);
kvidxError kvidxRocksdbSetValueRange(kvidxInstance *i, uint64_t key,
                                     size_t offset, const void *data,
                                     size_t dataLen, size_t *newLen);

/* TTL/Expiration */
kvidxError kvidxRocksdbSetExpire(kvidxInstance *i, uint64_t key,
                                 uint64_t ttlMs);
kvidxError kvidxRocksdbSetExpireAt(kvidxInstance *i, uint64_t key,
                                   uint64_t timestampMs);
int64_t kvidxRocksdbGetTTL(kvidxInstance *i, uint64_t key);
kvidxError kvidxRocksdbPersist(kvidxInstance *i, uint64_t key);
kvidxError kvidxRocksdbExpireScan(kvidxInstance *i, uint64_t maxKeys,
                                  uint64_t *expiredCount);

__END_DECLS
