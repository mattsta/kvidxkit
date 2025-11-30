/**
 * kvidxkit Example: Session Store with TTL
 *
 * Demonstrates using kvidxkit as a session store with automatic expiration.
 * Uses TTL features for session timeout management.
 *
 * Build:
 *   gcc -o session-store main.c -I../../src -L../../build/src -lkvidxkit-static
 * -lsqlite3
 *
 * Run:
 *   ./session-store
 */

#include "kvidxkit.h"
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define SESSION_TTL_MS (30 * 60 * 1000) // 30 minutes default

typedef struct {
    kvidxInstance db;
    uint64_t nextSessionId;
} SessionStore;

// Initialize session store
bool session_store_init(SessionStore *store, const char *path) {
    memset(store, 0, sizeof(*store));
    store->db.interface = kvidxInterfaceSqlite3;
    store->nextSessionId = 1;

    kvidxConfig config = kvidxConfigDefault();
    config.journalMode = KVIDX_JOURNAL_WAL;
    config.syncMode = KVIDX_SYNC_NORMAL;

    const char *err = NULL;
    if (!kvidxOpenWithConfig(&store->db, path, &config, &err)) {
        fprintf(stderr, "Failed to open session store: %s\n", err);
        return false;
    }

    // Find max session ID to continue from
    uint64_t maxKey = 0;
    if (kvidxMaxKey(&store->db, &maxKey)) {
        store->nextSessionId = maxKey + 1;
    }

    return true;
}

// Close session store
void session_store_close(SessionStore *store) {
    kvidxClose(&store->db);
}

// Create new session
uint64_t session_create(SessionStore *store, const char *userData,
                        uint64_t ttlMs) {
    uint64_t sessionId = store->nextSessionId++;

    kvidxBegin(&store->db);
    if (!kvidxInsert(&store->db, sessionId, 0, 0, userData, strlen(userData))) {
        kvidxAbort(&store->db);
        return 0;
    }
    kvidxCommit(&store->db);

    // Set TTL
    if (ttlMs > 0) {
        kvidxSetExpire(&store->db, sessionId, ttlMs);
    }

    return sessionId;
}

// Get session data (returns false if not found or expired)
bool session_get(SessionStore *store, uint64_t sessionId, char *buffer,
                 size_t bufferSize) {
    // Check TTL first
    int64_t ttl = kvidxGetTTL(&store->db, sessionId);
    if (ttl == KVIDX_TTL_NOT_FOUND || ttl <= 0) {
        return false;
    }

    uint64_t term, cmd;
    const uint8_t *data;
    size_t len;

    if (!kvidxGet(&store->db, sessionId, &term, &cmd, &data, &len)) {
        return false;
    }

    size_t copyLen = len < bufferSize - 1 ? len : bufferSize - 1;
    memcpy(buffer, data, copyLen);
    buffer[copyLen] = '\0';
    return true;
}

// Update session data
bool session_update(SessionStore *store, uint64_t sessionId,
                    const char *newData) {
    // Only update if exists
    kvidxError err =
        kvidxInsertXX(&store->db, sessionId, 0, 0, newData, strlen(newData));
    return err == KVIDX_OK;
}

// Refresh session TTL (extend expiration)
bool session_refresh(SessionStore *store, uint64_t sessionId, uint64_t ttlMs) {
    if (!kvidxExists(&store->db, sessionId)) {
        return false;
    }
    return kvidxSetExpire(&store->db, sessionId, ttlMs) == KVIDX_OK;
}

// Get remaining TTL
int64_t session_ttl(SessionStore *store, uint64_t sessionId) {
    return kvidxGetTTL(&store->db, sessionId);
}

// Delete session
bool session_delete(SessionStore *store, uint64_t sessionId) {
    kvidxBegin(&store->db);
    bool ok = kvidxRemove(&store->db, sessionId);
    kvidxCommit(&store->db);
    return ok;
}

// Cleanup expired sessions (call periodically)
uint64_t session_cleanup(SessionStore *store) {
    uint64_t count = 0;
    kvidxExpireScan(&store->db, 0, &count);
    return count;
}

// Get active session count
uint64_t session_count(SessionStore *store) {
    uint64_t count = 0;
    kvidxGetKeyCount(&store->db, &count);
    return count;
}

// List all sessions (for debugging)
void session_list(SessionStore *store) {
    kvidxIterator *it =
        kvidxIteratorCreate(&store->db, 0, UINT64_MAX, KVIDX_ITER_FORWARD);

    printf("Active sessions:\n");
    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, NULL, NULL, &data, &len);

        int64_t ttl = kvidxGetTTL(&store->db, key);
        printf("  Session %" PRIu64 ": data='%.*s' TTL=%" PRId64 "ms\n", key,
               (int)len, data, ttl);
    }

    kvidxIteratorDestroy(it);
}

// Simulate time passing (for demo)
void simulate_time_passing(int seconds) {
    printf("\n[Simulating %d seconds passing...]\n\n", seconds);
    // In real code, TTL is based on system clock
    // For demo, we use very short TTLs
}

int main(void) {
    printf("=== kvidxkit Session Store Example ===\n\n");

    SessionStore store;
    if (!session_store_init(&store, "sessions.db")) {
        return 1;
    }

    // Create sessions with different TTLs
    printf("1. Creating sessions:\n");

    uint64_t session1 =
        session_create(&store, "{\"user\":\"alice\",\"role\":\"admin\"}",
                       5000); // 5 second TTL for demo
    printf("   Created session %" PRIu64 " for alice (5s TTL)\n", session1);

    uint64_t session2 =
        session_create(&store, "{\"user\":\"bob\",\"role\":\"user\"}",
                       10000); // 10 second TTL
    printf("   Created session %" PRIu64 " for bob (10s TTL)\n", session2);

    uint64_t session3 =
        session_create(&store, "{\"user\":\"charlie\",\"role\":\"guest\"}",
                       3000); // 3 second TTL
    printf("   Created session %" PRIu64 " for charlie (3s TTL)\n", session3);

    // List all sessions
    printf("\n2. Current sessions:\n");
    session_list(&store);

    // Get session data
    printf("\n3. Reading session data:\n");
    char buffer[256];
    if (session_get(&store, session1, buffer, sizeof(buffer))) {
        printf("   Session %" PRIu64 " data: %s\n", session1, buffer);
    }

    // Check TTL
    printf("\n4. Checking TTLs:\n");
    printf("   Session %" PRIu64 " TTL: %" PRId64 "ms\n", session1,
           session_ttl(&store, session1));
    printf("   Session %" PRIu64 " TTL: %" PRId64 "ms\n", session2,
           session_ttl(&store, session2));
    printf("   Session %" PRIu64 " TTL: %" PRId64 "ms\n", session3,
           session_ttl(&store, session3));

    // Refresh session
    printf("\n5. Refreshing session %" PRIu64 " (extending to 30s):\n",
           session1);
    session_refresh(&store, session1, 30000);
    printf("   New TTL: %" PRId64 "ms\n", session_ttl(&store, session1));

    // Update session data
    printf("\n6. Updating session %" PRIu64 " data:\n", session1);
    session_update(&store, session1,
                   "{\"user\":\"alice\",\"role\":\"superadmin\"}");
    if (session_get(&store, session1, buffer, sizeof(buffer))) {
        printf("   Updated data: %s\n", buffer);
    }

    // Manually delete a session
    printf("\n7. Deleting session %" PRIu64 ":\n", session2);
    session_delete(&store, session2);
    printf("   Deleted. Remaining sessions: %" PRIu64 "\n",
           session_count(&store));

    // Wait for some sessions to expire (simulated)
    printf("\n8. Waiting for expiration...\n");
    printf("   (In this demo, we'll run cleanup which removes expired "
           "sessions)\n");

    // For demo purposes, set very short TTL on remaining session
    kvidxSetExpire(&store.db, session3, 1); // 1ms TTL

    // Small delay to let it expire
    struct timespec ts = {0, 10000000}; // 10ms
    nanosleep(&ts, NULL);

    // Cleanup expired sessions
    printf("\n9. Running cleanup:\n");
    uint64_t expired = session_cleanup(&store);
    printf("   Cleaned up %" PRIu64 " expired session(s)\n", expired);

    // Final state
    printf("\n10. Final session list:\n");
    session_list(&store);
    printf("    Total active sessions: %" PRIu64 "\n", session_count(&store));

    session_store_close(&store);

    // Cleanup
    remove("sessions.db");
    remove("sessions.db-wal");
    remove("sessions.db-shm");

    printf("\n=== Example Complete ===\n");
    return 0;
}
