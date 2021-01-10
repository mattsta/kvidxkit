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

__END_DECLS
