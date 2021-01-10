#pragma once

/* REMINDER:
 *  - sqlite3 column binding is 1-INDEXED
 *  - sqlite3 result set binding is 0-INDEXED
 *  - unset params in a statement default to 'NULL' */
#ifdef NDEBUG
#define sbind(what, ...) sqlite3_bind_##what(__VA_ARGS__)
#else
#define sbind(what, stmt, ...)                                                 \
    do {                                                                       \
        int result_ = sqlite3_bind_##what(stmt, __VA_ARGS__);                  \
        if (result_ != SQLITE_OK) {                                            \
            char *ins_ = sqlite3_expanded_sql(stmt);                           \
            printf("Failed to bind param; query so far: %s\n", ins_);          \
            printf("\twith error: %s\n", sqlite3_errmsg(db));                  \
            sqlite3_free(ins_);                                                \
        }                                                                      \
        assert(result_ == SQLITE_OK);                                          \
    } while (0)
#endif

#ifdef NDEBUG
#define sprepare(db, ...) sqlite3_prepare_v2(db, __VA_ARGS__)
#else
#define sprepare(db, stmt, ...)                                                \
    do {                                                                       \
        int result_ = sqlite3_prepare_v2(db, stmt, __VA_ARGS__);               \
        if (result_ != SQLITE_OK) {                                            \
            printf("Failed to prepare query: %s\n", stmt);                     \
            printf("\twith error: %s\n", sqlite3_errmsg(db));                  \
        }                                                                      \
        assert(result_ == SQLITE_OK);                                          \
    } while (0)
#endif

#ifdef NDEBUG
#define sstep_(check, stmt, result) result = sqlite3_step(stmt)
#else
#define sstep_(check, stmt, result)                                            \
    do {                                                                       \
        result = sqlite3_step(stmt);                                           \
        if (result != (check)) {                                               \
            printf("Failed to step:\n");                                       \
            printf("\tfrom query: %s\n", sqlite3_expanded_sql(stmt));          \
            printf("\twith error: %s\n", sqlite3_errmsg(db));                  \
        }                                                                      \
        assert(result == (check));                                             \
    } while (0)
#endif

#define sstep(stmt, result) sstep_(SQLITE_DONE, stmt, result)
#define sstepRow(stmt, result) sstep_(SQLITE_ROW, stmt, result)

#ifdef NDEBUG
#define sfunction(db, ...) sqlite3_create_function(db, __VA_ARGS__)
#else
#define sfunction(db, ...)                                                     \
    do {                                                                       \
        int result_ = sqlite3_create_function(db, __VA_ARGS__);                \
        if (result_ != SQLITE_OK) {                                            \
            printf("Failed to create function:\n");                            \
            printf("\twith error: %s\n", sqlite3_errmsg(db));                  \
        }                                                                      \
        assert(result_ == SQLITE_OK);                                          \
    } while (0)
#endif

#ifdef NDEBUG
#define sclose(db) result = sqlite3_close(db)
#else
#define sclose(db)                                                             \
    do {                                                                       \
        result = sqlite3_close(db);                                            \
        if (result != SQLITE_OK) {                                             \
            printf("Failed to close:\n");                                      \
            printf("\twith error: %s\n", sqlite3_errmsg(db));                  \
        }                                                                      \
        assert(result == SQLITE_OK);                                           \
    } while (0)
#endif

#ifdef NDEBUG
#define sopen(filename, ...) result = sqlite3_open_v2(filename, __VA_ARGS__)
#else
#define sopen(filename, ...)                                                   \
    do {                                                                       \
        result = sqlite3_open_v2(filename, __VA_ARGS__);                       \
        if (result != SQLITE_OK) {                                             \
            printf("Failed to open:\n");                                       \
            printf("\twith error: %s\n", sqlite3_errmsg(db));                  \
        }                                                                      \
        assert(result == SQLITE_OK);                                           \
    } while (0)
#endif
