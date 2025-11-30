#pragma once

#include "kvidxkit.h"
__BEGIN_DECLS

/* Open / Close / Management */
bool kvidxSqlite3Open(kvidxInstance *i, const char *filename, const char **err);
bool kvidxSqlite3Close(kvidxInstance *i);
bool kvidxSqlite3Fsync(kvidxInstance *i);

/* Transactional Control */
bool kvidxSqlite3Begin(kvidxInstance *i);
bool kvidxSqlite3Commit(kvidxInstance *i);

/* Reading */
bool kvidxSqlite3Get(kvidxInstance *i, uint64_t key, uint64_t *term,
                     uint64_t *cmd, const uint8_t **data, size_t *len);
bool kvidxSqlite3GetPrev(kvidxInstance *i, uint64_t nextKey, uint64_t *prevKey,
                         uint64_t *prevTerm, uint64_t *cmd,
                         const uint8_t **data, size_t *len);
bool kvidxSqlite3GetNext(kvidxInstance *i, uint64_t previousKey,
                         uint64_t *nextKey, uint64_t *nextTerm, uint64_t *cmd,
                         const uint8_t **data, size_t *len);
bool kvidxSqlite3Exists(kvidxInstance *i, uint64_t key);
bool kvidxSqlite3ExistsDual(kvidxInstance *i, uint64_t key, uint64_t term);
bool kvidxSqlite3Max(kvidxInstance *i, uint64_t *key);
bool kvidxSqlite3Insert(kvidxInstance *i, uint64_t key, uint64_t term,
                        uint64_t cmd, const void *data, size_t dataLen);

/* Deleting */
bool kvidxSqlite3Remove(kvidxInstance *i, uint64_t key);
bool kvidxSqlite3RemoveAfterNInclusive(kvidxInstance *i, uint64_t key);
bool kvidxSqlite3RemoveBeforeNInclusive(kvidxInstance *i, uint64_t key);

/* Statistics (v0.5.0) */
kvidxError kvidxSqlite3GetStats(kvidxInstance *i, kvidxStats *stats);
kvidxError kvidxSqlite3GetKeyCount(kvidxInstance *i, uint64_t *count);
kvidxError kvidxSqlite3GetMinKey(kvidxInstance *i, uint64_t *key);
kvidxError kvidxSqlite3GetDataSize(kvidxInstance *i, uint64_t *bytes);

/* Configuration (v0.5.0) */
kvidxError kvidxSqlite3ApplyConfig(kvidxInstance *i, const kvidxConfig *config);

/* Range Operations (v0.5.0) */
kvidxError kvidxSqlite3RemoveRange(kvidxInstance *i, uint64_t startKey,
                                   uint64_t endKey, bool startInclusive,
                                   bool endInclusive, uint64_t *deletedCount);
kvidxError kvidxSqlite3CountRange(kvidxInstance *i, uint64_t startKey,
                                  uint64_t endKey, uint64_t *count);
kvidxError kvidxSqlite3ExistsInRange(kvidxInstance *i, uint64_t startKey,
                                     uint64_t endKey, bool *exists);

/* Export/Import (v0.6.0) */
kvidxError kvidxSqlite3Export(kvidxInstance *i, const char *filename,
                              const kvidxExportOptions *options,
                              kvidxProgressCallback callback, void *userData);
kvidxError kvidxSqlite3Import(kvidxInstance *i, const char *filename,
                              const kvidxImportOptions *options,
                              kvidxProgressCallback callback, void *userData);

/* Storage Primitives (v0.8.0) */
/* Conditional writes */
kvidxError kvidxSqlite3InsertEx(kvidxInstance *i, uint64_t key, uint64_t term,
                                uint64_t cmd, const void *data, size_t dataLen,
                                kvidxSetCondition condition);

/* Transaction abort */
bool kvidxSqlite3Abort(kvidxInstance *i);

/* Atomic operations */
kvidxError kvidxSqlite3GetAndSet(kvidxInstance *i, uint64_t key, uint64_t term,
                                 uint64_t cmd, const void *data, size_t dataLen,
                                 uint64_t *oldTerm, uint64_t *oldCmd,
                                 void **oldData, size_t *oldDataLen);
kvidxError kvidxSqlite3GetAndRemove(kvidxInstance *i, uint64_t key,
                                    uint64_t *term, uint64_t *cmd, void **data,
                                    size_t *dataLen);

/* Compare-and-swap */
kvidxError kvidxSqlite3CompareAndSwap(kvidxInstance *i, uint64_t key,
                                      const void *expectedData,
                                      size_t expectedLen, uint64_t newTerm,
                                      uint64_t newCmd, const void *newData,
                                      size_t newDataLen, bool *swapped);

/* Append/Prepend */
kvidxError kvidxSqlite3Append(kvidxInstance *i, uint64_t key, uint64_t term,
                              uint64_t cmd, const void *data, size_t dataLen,
                              size_t *newLen);
kvidxError kvidxSqlite3Prepend(kvidxInstance *i, uint64_t key, uint64_t term,
                               uint64_t cmd, const void *data, size_t dataLen,
                               size_t *newLen);

/* Partial value access */
kvidxError kvidxSqlite3GetValueRange(kvidxInstance *i, uint64_t key,
                                     size_t offset, size_t length, void **data,
                                     size_t *actualLen);
kvidxError kvidxSqlite3SetValueRange(kvidxInstance *i, uint64_t key,
                                     size_t offset, const void *data,
                                     size_t dataLen, size_t *newLen);

/* TTL/Expiration */
kvidxError kvidxSqlite3SetExpire(kvidxInstance *i, uint64_t key,
                                 uint64_t ttlMs);
kvidxError kvidxSqlite3SetExpireAt(kvidxInstance *i, uint64_t key,
                                   uint64_t timestampMs);
int64_t kvidxSqlite3GetTTL(kvidxInstance *i, uint64_t key);
kvidxError kvidxSqlite3Persist(kvidxInstance *i, uint64_t key);
kvidxError kvidxSqlite3ExpireScan(kvidxInstance *i, uint64_t maxKeys,
                                  uint64_t *expiredCount);

__END_DECLS
