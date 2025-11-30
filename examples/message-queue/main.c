/**
 * kvidxkit Example: Simple Message Queue
 *
 * Demonstrates using kvidxkit as a persistent message/job queue.
 * Messages are ordered by ID, supporting FIFO processing.
 *
 * Build:
 *   gcc -o message-queue main.c -I../../src -L../../build/src -lkvidxkit-static
 * -lsqlite3
 *
 * Run:
 *   ./message-queue
 */

#include "kvidxkit.h"
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Message priority levels (stored in cmd field)
typedef enum {
    PRIORITY_LOW = 0,
    PRIORITY_NORMAL = 1,
    PRIORITY_HIGH = 2,
    PRIORITY_CRITICAL = 3
} MessagePriority;

typedef struct {
    kvidxInstance db;
    uint64_t nextMessageId;
} MessageQueue;

// Initialize message queue
bool mq_init(MessageQueue *mq, const char *path) {
    memset(mq, 0, sizeof(*mq));
    mq->db.interface = kvidxInterfaceSqlite3;
    mq->nextMessageId = 1;

    kvidxConfig config = kvidxConfigDefault();
    config.journalMode = KVIDX_JOURNAL_WAL;
    config.syncMode = KVIDX_SYNC_NORMAL;

    const char *err = NULL;
    if (!kvidxOpenWithConfig(&mq->db, path, &config, &err)) {
        fprintf(stderr, "Failed to open queue: %s\n", err);
        return false;
    }

    // Find max message ID
    uint64_t maxKey = 0;
    if (kvidxMaxKey(&mq->db, &maxKey)) {
        mq->nextMessageId = maxKey + 1;
    }

    return true;
}

// Close queue
void mq_close(MessageQueue *mq) {
    kvidxClose(&mq->db);
}

// Enqueue message
uint64_t mq_enqueue(MessageQueue *mq, const char *message,
                    MessagePriority priority, uint64_t ttlMs) {
    uint64_t messageId = mq->nextMessageId++;

    kvidxBegin(&mq->db);
    if (!kvidxInsert(&mq->db, messageId, 0, priority, message,
                     strlen(message))) {
        kvidxAbort(&mq->db);
        return 0;
    }
    kvidxCommit(&mq->db);

    // Set TTL if specified
    if (ttlMs > 0) {
        kvidxSetExpire(&mq->db, messageId, ttlMs);
    }

    return messageId;
}

// Dequeue oldest message (FIFO)
bool mq_dequeue(MessageQueue *mq, uint64_t *messageId, char *buffer,
                size_t bufferSize, MessagePriority *priority) {
    // Find oldest (minimum key)
    uint64_t minKey = 0;
    kvidxError err = kvidxGetMinKey(&mq->db, &minKey);
    if (err != KVIDX_OK) {
        return false; // Queue empty
    }

    // Atomic get and remove
    uint64_t term, cmd;
    void *data = NULL;
    size_t len = 0;

    err = kvidxGetAndRemove(&mq->db, minKey, &term, &cmd, &data, &len);
    if (err != KVIDX_OK) {
        return false;
    }

    // Copy data to buffer
    *messageId = minKey;
    *priority = (MessagePriority)cmd;

    size_t copyLen = len < bufferSize - 1 ? len : bufferSize - 1;
    memcpy(buffer, data, copyLen);
    buffer[copyLen] = '\0';

    free(data); // Must free data from GetAndRemove
    return true;
}

// Peek at oldest message without removing
bool mq_peek(MessageQueue *mq, uint64_t *messageId, char *buffer,
             size_t bufferSize, MessagePriority *priority) {
    uint64_t minKey = 0;
    kvidxError err = kvidxGetMinKey(&mq->db, &minKey);
    if (err != KVIDX_OK) {
        return false;
    }

    uint64_t term, cmd;
    const uint8_t *data;
    size_t len;

    if (!kvidxGet(&mq->db, minKey, &term, &cmd, &data, &len)) {
        return false;
    }

    *messageId = minKey;
    *priority = (MessagePriority)cmd;

    size_t copyLen = len < bufferSize - 1 ? len : bufferSize - 1;
    memcpy(buffer, data, copyLen);
    buffer[copyLen] = '\0';

    return true;
}

// Get queue length
uint64_t mq_length(MessageQueue *mq) {
    uint64_t count = 0;
    kvidxGetKeyCount(&mq->db, &count);
    return count;
}

// Check if queue is empty
bool mq_is_empty(MessageQueue *mq) {
    return mq_length(mq) == 0;
}

// Cleanup expired messages
uint64_t mq_cleanup_expired(MessageQueue *mq) {
    uint64_t count = 0;
    kvidxExpireScan(&mq->db, 0, &count);
    return count;
}

// Delete specific message (cancel)
bool mq_cancel(MessageQueue *mq, uint64_t messageId) {
    kvidxBegin(&mq->db);
    bool ok = kvidxRemove(&mq->db, messageId);
    kvidxCommit(&mq->db);
    return ok;
}

// List all messages (for debugging)
void mq_list(MessageQueue *mq) {
    const char *priorityNames[] = {"LOW", "NORMAL", "HIGH", "CRITICAL"};

    printf("Queue contents:\n");
    kvidxIterator *it =
        kvidxIteratorCreate(&mq->db, 0, UINT64_MAX, KVIDX_ITER_FORWARD);

    int count = 0;
    while (kvidxIteratorNext(it) && kvidxIteratorValid(it)) {
        uint64_t key, cmd;
        const uint8_t *data;
        size_t len;
        kvidxIteratorGet(it, &key, NULL, &cmd, &data, &len);

        int64_t ttl = kvidxGetTTL(&mq->db, key);
        const char *ttlStr = ttl == KVIDX_TTL_NONE ? "none"
                             : ttl <= 0            ? "expired"
                                                   : "";

        printf("  [%" PRIu64 "] priority=%s msg='%.*s'", key,
               priorityNames[cmd], (int)len, data);
        if (ttl > 0) {
            printf(" TTL=%" PRId64 "ms", ttl);
        } else if (ttl != KVIDX_TTL_NONE) {
            printf(" %s", ttlStr);
        }
        printf("\n");
        count++;
    }

    if (count == 0) {
        printf("  (queue is empty)\n");
    }

    kvidxIteratorDestroy(it);
}

// Batch enqueue
size_t mq_enqueue_batch(MessageQueue *mq, const char **messages, size_t count,
                        MessagePriority priority) {
    kvidxEntry *entries = malloc(count * sizeof(kvidxEntry));
    if (!entries) {
        return 0;
    }

    for (size_t i = 0; i < count; i++) {
        entries[i].key = mq->nextMessageId++;
        entries[i].term = 0;
        entries[i].cmd = priority;
        entries[i].data = messages[i];
        entries[i].dataLen = strlen(messages[i]);
    }

    size_t inserted = 0;
    kvidxInsertBatch(&mq->db, entries, count, &inserted);
    free(entries);
    return inserted;
}

// Process messages (worker simulation)
void mq_process(MessageQueue *mq, int maxMessages,
                void (*processor)(uint64_t id, MessagePriority priority,
                                  const char *message)) {
    char buffer[1024];
    uint64_t messageId;
    MessagePriority priority;
    int processed = 0;

    while (processed < maxMessages &&
           mq_dequeue(mq, &messageId, buffer, sizeof(buffer), &priority)) {
        processor(messageId, priority, buffer);
        processed++;
    }
}

// Example processor
void example_processor(uint64_t id, MessagePriority priority,
                       const char *message) {
    const char *priorityNames[] = {"LOW", "NORMAL", "HIGH", "CRITICAL"};
    printf("  Processing [%" PRIu64 "] (%s): %s\n", id, priorityNames[priority],
           message);
}

int main(void) {
    printf("=== kvidxkit Message Queue Example ===\n\n");

    MessageQueue mq;
    if (!mq_init(&mq, "queue.db")) {
        return 1;
    }

    // Enqueue some messages
    printf("1. Enqueueing messages:\n");

    uint64_t id1 = mq_enqueue(&mq, "Process order #1001", PRIORITY_NORMAL, 0);
    printf("   Enqueued message %" PRIu64 "\n", id1);

    uint64_t id2 = mq_enqueue(&mq, "Send email notification", PRIORITY_LOW, 0);
    printf("   Enqueued message %" PRIu64 "\n", id2);

    uint64_t id3 =
        mq_enqueue(&mq, "URGENT: Payment failed", PRIORITY_CRITICAL, 0);
    printf("   Enqueued message %" PRIu64 "\n", id3);

    uint64_t id4 =
        mq_enqueue(&mq, "Generate report", PRIORITY_LOW, 5000); // 5s TTL
    printf("   Enqueued message %" PRIu64 " (with 5s TTL)\n", id4);

    // Batch enqueue
    printf("\n2. Batch enqueueing:\n");
    const char *batchMessages[] = {"Batch job 1", "Batch job 2", "Batch job 3"};
    size_t batchCount =
        mq_enqueue_batch(&mq, batchMessages, 3, PRIORITY_NORMAL);
    printf("   Enqueued %zu messages\n", batchCount);

    // List queue
    printf("\n3. Current queue:\n");
    mq_list(&mq);
    printf("   Queue length: %" PRIu64 "\n", mq_length(&mq));

    // Peek at first message
    printf("\n4. Peeking at first message:\n");
    char buffer[256];
    uint64_t peekId;
    MessagePriority peekPriority;
    if (mq_peek(&mq, &peekId, buffer, sizeof(buffer), &peekPriority)) {
        printf("   Next message [%" PRIu64 "]: %s\n", peekId, buffer);
    }

    // Process some messages
    printf("\n5. Processing 3 messages:\n");
    mq_process(&mq, 3, example_processor);

    // Show remaining queue
    printf("\n6. Remaining queue:\n");
    mq_list(&mq);

    // Cancel a message
    printf("\n7. Cancelling message %" PRIu64 ":\n", id4);
    mq_cancel(&mq, id4);
    printf("   Cancelled. Queue length: %" PRIu64 "\n", mq_length(&mq));

    // Dequeue all remaining
    printf("\n8. Processing remaining messages:\n");
    mq_process(&mq, 100, example_processor);

    // Check if empty
    printf("\n9. Queue status:\n");
    printf("   Is empty: %s\n", mq_is_empty(&mq) ? "yes" : "no");
    printf("   Length: %" PRIu64 "\n", mq_length(&mq));

    // Enqueue with TTL and let it expire
    printf("\n10. Testing message expiration:\n");
    mq_enqueue(&mq, "Expiring message", PRIORITY_NORMAL, 1); // 1ms TTL
    printf("    Enqueued message with 1ms TTL\n");

    // Small delay
    struct timespec ts = {0, 10000000}; // 10ms
    nanosleep(&ts, NULL);

    uint64_t expired = mq_cleanup_expired(&mq);
    printf("    Cleaned up %" PRIu64 " expired message(s)\n", expired);

    mq_close(&mq);

    // Cleanup
    remove("queue.db");
    remove("queue.db-wal");
    remove("queue.db-shm");

    printf("\n=== Example Complete ===\n");
    return 0;
}
