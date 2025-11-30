/**
 * kvidxkit Example: Raft Consensus Log Storage
 *
 * Demonstrates using kvidxkit as a Raft consensus log store.
 * The entry structure (key, term, cmd, data) maps naturally to Raft log
 * entries.
 *
 * Build:
 *   gcc -o raft-log main.c -I../../src -L../../build/src -lkvidxkit-static
 * -lsqlite3
 *
 * Run:
 *   ./raft-log
 */

#include "kvidxkit.h"
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    kvidxInstance db;
    uint64_t commitIndex;
    uint64_t lastApplied;
} RaftLog;

// Initialize Raft log
bool raft_log_init(RaftLog *log, const char *path) {
    memset(log, 0, sizeof(*log));
    log->db.interface = kvidxInterfaceSqlite3;

    kvidxConfig config = kvidxConfigDefault();
    config.journalMode = KVIDX_JOURNAL_WAL;
    config.syncMode = KVIDX_SYNC_FULL; // Durability is critical for consensus

    const char *err = NULL;
    if (!kvidxOpenWithConfig(&log->db, path, &config, &err)) {
        fprintf(stderr, "Failed to open log: %s\n", err);
        return false;
    }

    // Recover state
    uint64_t maxKey = 0;
    if (kvidxMaxKey(&log->db, &maxKey)) {
        log->commitIndex = maxKey;
        log->lastApplied = maxKey;
    }

    return true;
}

// Close Raft log
void raft_log_close(RaftLog *log) {
    kvidxClose(&log->db);
}

// Append entry to log (leader operation)
bool raft_log_append(RaftLog *log, uint64_t term, uint64_t cmd,
                     const void *data, size_t len, uint64_t *index) {
    // Get next index
    uint64_t maxKey = 0;
    kvidxMaxKey(&log->db, &maxKey);
    uint64_t newIndex = maxKey + 1;

    // Append entry
    kvidxBegin(&log->db);
    if (!kvidxInsert(&log->db, newIndex, term, cmd, data, len)) {
        kvidxAbort(&log->db);
        return false;
    }
    kvidxCommit(&log->db);

    *index = newIndex;
    return true;
}

// Get log entry at index
bool raft_log_get(RaftLog *log, uint64_t index, uint64_t *term, uint64_t *cmd,
                  const uint8_t **data, size_t *len) {
    return kvidxGet(&log->db, index, term, cmd, data, len);
}

// Get term at index (for AppendEntries consistency check)
bool raft_log_term_at(RaftLog *log, uint64_t index, uint64_t *term) {
    if (index == 0) {
        *term = 0; // Virtual initial entry
        return true;
    }
    return kvidxGet(&log->db, index, term, NULL, NULL, NULL);
}

// Truncate log from index (inclusive) - used on conflict
void raft_log_truncate_from(RaftLog *log, uint64_t index) {
    kvidxBegin(&log->db);
    kvidxRemoveAfterNInclusive(&log->db, index);
    kvidxCommit(&log->db);
}

// Check if log contains entry at index with matching term
bool raft_log_matches(RaftLog *log, uint64_t index, uint64_t term) {
    return kvidxExistsDual(&log->db, index, term);
}

// Get entries from index for replication
size_t raft_log_get_entries(RaftLog *log, uint64_t fromIndex,
                            uint64_t maxEntries,
                            void (*callback)(uint64_t index, uint64_t term,
                                             uint64_t cmd, const uint8_t *data,
                                             size_t len, void *userData),
                            void *userData) {
    size_t count = 0;
    kvidxIterator *it = kvidxIteratorCreate(&log->db, fromIndex, UINT64_MAX,
                                            KVIDX_ITER_FORWARD);

    while (kvidxIteratorNext(it) && kvidxIteratorValid(it) &&
           count < maxEntries) {
        uint64_t key, term, cmd;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, &term, &cmd, &data, &len);
        callback(key, term, cmd, data, len, userData);
        count++;
    }

    kvidxIteratorDestroy(it);
    return count;
}

// Apply committed entries to state machine
void raft_log_apply_committed(RaftLog *log,
                              void (*apply)(uint64_t index, uint64_t cmd,
                                            const uint8_t *data, size_t len)) {
    while (log->lastApplied < log->commitIndex) {
        log->lastApplied++;
        uint64_t term, cmd;
        const uint8_t *data;
        size_t len;
        if (raft_log_get(log, log->lastApplied, &term, &cmd, &data, &len)) {
            apply(log->lastApplied, cmd, data, len);
        }
    }
}

// Commit up to index
void raft_log_commit(RaftLog *log, uint64_t index) {
    if (index > log->commitIndex) {
        log->commitIndex = index;
    }
}

// Get last index and term
void raft_log_last_info(RaftLog *log, uint64_t *lastIndex, uint64_t *lastTerm) {
    uint64_t maxKey = 0;
    if (kvidxMaxKey(&log->db, &maxKey)) {
        *lastIndex = maxKey;
        kvidxGet(&log->db, maxKey, lastTerm, NULL, NULL, NULL);
    } else {
        *lastIndex = 0;
        *lastTerm = 0;
    }
}

// Get log entry count
uint64_t raft_log_length(RaftLog *log) {
    uint64_t count = 0;
    kvidxGetKeyCount(&log->db, &count);
    return count;
}

// Example state machine application callback
void apply_to_state_machine(uint64_t index, uint64_t cmd, const uint8_t *data,
                            size_t len) {
    printf("  Applied entry %" PRIu64 ": cmd=%" PRIu64 " data='%.*s'\n", index,
           cmd, (int)len, data);
}

// Example replication callback
void print_entry(uint64_t index, uint64_t term, uint64_t cmd,
                 const uint8_t *data, size_t len, void *userData) {
    printf("  [%" PRIu64 "] term=%" PRIu64 " cmd=%" PRIu64 " data='%.*s'\n",
           index, term, cmd, (int)len, data);
}

int main(void) {
    printf("=== kvidxkit Raft Log Example ===\n\n");

    RaftLog log;
    if (!raft_log_init(&log, "raft.db")) {
        return 1;
    }

    // Simulate leader appending entries
    printf("1. Appending entries as leader (term 1):\n");
    uint64_t index;

    raft_log_append(&log, 1, 1, "SET x=1", 7, &index);
    printf("   Appended entry %" PRIu64 "\n", index);

    raft_log_append(&log, 1, 2, "SET y=2", 7, &index);
    printf("   Appended entry %" PRIu64 "\n", index);

    raft_log_append(&log, 1, 1, "SET z=3", 7, &index);
    printf("   Appended entry %" PRIu64 "\n", index);

    // New term after election
    printf("\n2. New term after election (term 2):\n");
    raft_log_append(&log, 2, 1, "SET a=10", 8, &index);
    printf("   Appended entry %" PRIu64 "\n", index);

    // Check log state
    uint64_t lastIndex, lastTerm;
    raft_log_last_info(&log, &lastIndex, &lastTerm);
    printf("\n3. Log state: lastIndex=%" PRIu64 " lastTerm=%" PRIu64
           " length=%" PRIu64 "\n",
           lastIndex, lastTerm, raft_log_length(&log));

    // Simulate replication (get entries for follower)
    printf("\n4. Entries for replication (from index 2):\n");
    raft_log_get_entries(&log, 2, 10, print_entry, NULL);

    // Commit and apply
    printf("\n5. Committing up to index %" PRIu64 " and applying:\n",
           lastIndex);
    raft_log_commit(&log, lastIndex);
    raft_log_apply_committed(&log, apply_to_state_machine);

    // Simulate log conflict and truncation
    printf("\n6. Simulating conflict - truncating from index 3:\n");
    raft_log_truncate_from(&log, 3);
    raft_log_last_info(&log, &lastIndex, &lastTerm);
    printf("   After truncation: lastIndex=%" PRIu64 " lastTerm=%" PRIu64 "\n",
           lastIndex, lastTerm);

    // Append new entries after truncation
    printf("\n7. Appending corrected entries (term 3):\n");
    raft_log_append(&log, 3, 1, "CORRECTED", 9, &index);
    printf("   Appended entry %" PRIu64 "\n", index);

    // Final state
    printf("\n8. Final log contents:\n");
    raft_log_get_entries(&log, 1, 100, print_entry, NULL);

    raft_log_close(&log);

    // Cleanup
    remove("raft.db");
    remove("raft.db-wal");
    remove("raft.db-shm");

    printf("\n=== Example Complete ===\n");
    return 0;
}
