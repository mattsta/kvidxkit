#include "ctest.h"
#include "kvidxkit.h"

#include <string.h> /* memcmp() */
#include <unistd.h> /* getpid() */

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;
    uint32_t err = 0;

    kvidxInstance pre = {0};
    kvidxInstance *i = &pre;

    i->interface = kvidxInterfaceSqlite3;

    char filename[64] = {0};
    snprintf(filename, sizeof(filename), "tester-openclose-%d.sqlite3",
             getpid());

    printf("Testing inside filename: %s\n", filename);
    TEST("open/close...") {
        const char *errStr = NULL;
        bool opened = kvidxOpen(i, filename, &errStr);
        if (!opened) {
            ERR("Failed to open file!  Returned error: %s", errStr);
        }

        bool closed = kvidxClose(i);
        if (!closed) {
            ERRR("Failed to close!");
        }
    }

    printf("Removing test file...\n");
    unlink(filename);

    snprintf(filename, sizeof(filename), "tester-running-%d.sqlite3", getpid());
    printf("Testing inside filename: %s\n", filename);
    kvidxOpen(i, filename, NULL);

    uint64_t startKey = 331;
    uint64_t startTerm = 701;
    uint8_t data[65536] = {3};
    uint64_t cmd = 88;

    TEST("exists with no data...") {
        bool e1 = kvidxExists(i, startKey);
        if (e1) {
            ERRR("Found key when it can't possibly exist!");
        }
    }

    TEST("insert then exists...") {
        if (!kvidxBegin(i)) {
            ERRR("Didn't BEGIN!");
        }

        bool i1 = kvidxInsert(i, startKey, startTerm, cmd, data, sizeof(data));
        bool e2 = kvidxExists(i, startKey);
        bool ed1 = kvidxExistsDual(i, startKey, startTerm);

        if (!kvidxCommit(i)) {
            ERRR("Didn't COMMIT!");
        }

        if (!i1) {
            ERRR("Didn't insert!");
        }

        if (!e2) {
            ERRR("Didn't find key after insert!");
        }

        if (!ed1) {
            ERRR("Didn't find key + term pair after insert!");
        }
    }

    TEST("max key...") {
        uint64_t max;
        bool m1 = kvidxMaxKey(i, &max);

        if (!m1) {
            ERRR("Didn't find max!");
        }

        if (max != startKey) {
            ERRR("Max isn't our inserted start key!");
        }
    }

    TEST("remove then exists...") {
        if (!kvidxBegin(i)) {
            ERRR("Didn't BEGIN!");
        }

        bool r1 = kvidxRemove(i, startKey);
        bool e3 = kvidxExists(i, startKey);

        if (!kvidxCommit(i)) {
            ERRR("Didn't COMMIT!");
        }

        if (!r1) {
            ERRR("Didn't remove!");
        }

        if (e3) {
            ERRR("Found key after remove!");
        }
    }

    TEST("double insert...") {
        if (!kvidxBegin(i)) {
            ERRR("Didn't BEGIN!");
        }

        bool i2 = kvidxInsert(i, startKey, startTerm, cmd, data, sizeof(data));
        bool e4 = kvidxExists(i, startKey);
        bool ed4 = kvidxExistsDual(i, startKey, startTerm);
        bool i3 = kvidxInsert(i, startKey, startTerm, cmd, data, sizeof(data));
        if (!kvidxCommit(i)) {
            ERRR("Didn't COMMIT!");
        }

        if (!i2) {
            ERRR("Failed to insert!");
        }

        if (!e4) {
            ERRR("Failed to find key after insert!");
        }

        if (!ed4) {
            ERRR("Failed to find key + term after insert!");
        }

        if (i3) {
            ERRR("Inserted duplicate key!");
        }
    }

    TEST("remove after (single)...") {
        bool i4 = kvidxRemoveAfterNInclusive(i, startKey);
        bool e5 = kvidxExists(i, startKey);

        if (!i4) {
            ERRR("Failed to remove range to end!");
        }

        if (e5) {
            ERRR("Found after remove!");
        }
    }

    TEST("remove after (multiple)...") {
        bool i5 =
            kvidxInsert(i, startKey + 1, startTerm, cmd, data, sizeof(data));
        bool i6 =
            kvidxInsert(i, startKey + 2, startTerm, cmd, data, sizeof(data));
        bool i7 =
            kvidxInsert(i, startKey + 3, startTerm, cmd, data, sizeof(data));

        TEST("max key (after triple insert)...") {
            uint64_t max;
            bool m2 = kvidxMaxKey(i, &max);

            if (!m2) {
                ERRR("Didn't find max!");
            }

            if (max != startKey + 3) {
                ERRR("Max isn't our startKey+3!");
            }
        }

        if (!(i5 && i6 && i7)) {
            ERRR("Failed to insert new key(s)!");
        }

        bool i4 = kvidxRemoveAfterNInclusive(i, startKey + 1);
        if (!i4) {
            ERRR("Failed to remove range starting at +1!");
        }

        TEST("max key (with no keys)...") {
            uint64_t max = 500;
            bool m3 = kvidxMaxKey(i, &max);

            if (m3) {
                ERR("Found max key when no keys exist: %" PRIu64 "!", max);
            }
        }

        bool e6 = kvidxExists(i, startKey + 1);
        if (e6) {
            ERRR("Found +1 after remove!");
        }

        bool e7 = kvidxExists(i, startKey + 2);
        if (e7) {
            ERRR("Found +2 after remove!");
        }

        bool e8 = kvidxExists(i, startKey + 3);
        if (e8) {
            ERRR("Found +3 after remove!");
        }
    }

    TEST("get...") {
        bool i8 = kvidxInsert(i, startKey + 1, startTerm + 1, cmd, data,
                              sizeof(data));

        if (!i8) {
            ERRR("Didn't insert +1 again!");
        }

        const uint8_t *foundData;
        size_t foundLen;
        uint64_t foundTerm;
        uint64_t foundCmd;
        bool found = kvidxGet(i, startKey + 1, &foundTerm, &foundCmd,
                              &foundData, &foundLen);

        if (!found) {
            ERRR("Didn't find inserted data!");
        }

        if (foundTerm != startTerm + 1) {
            ERRR("Didn't find correct term!");
        }

        if (foundLen != sizeof(data)) {
            ERR("Returned length wrong!  Data size: %zu but found length: "
                "%zu!",
                sizeof(data), foundLen);
        }

        if (foundCmd != cmd) {
            ERRR("Didn't find command!");
        }

        if (memcmp(foundData, data, foundLen) != 0) {
            ERRR("Found data doesn't match original data!");
        }
    }

    TEST("get prev...") {
        const uint8_t *foundData;
        size_t foundLen;
        uint64_t foundKey;
        uint64_t foundTerm;
        uint64_t foundCmd;
        /* By now, only [StartKey+1] exists, so ask for +2 to get prev of +1 */
        bool g1 = kvidxGetPrev(i, startKey + 2, &foundKey, &foundTerm,
                               &foundCmd, &foundData, &foundLen);

        if (!g1) {
            ERRR("Didn't find prev row!");
        }

        if (foundKey != startKey + 1) {
            ERR("Didn't find expected key!  Wanted %" PRIu64 " but got %" PRIu64
                "!",
                startKey + 1, foundKey);
        }

        if (foundTerm != startTerm + 1) {
            ERRR("Didn't find expected term!");
        }

        if (foundLen != sizeof(data)) {
            ERR("Returned length wrong!  Data size: %zu but found length: "
                "%zu!",
                sizeof(data), foundLen);
        }

        if (foundCmd != cmd) {
            ERR("Expected cmd %" PRIu64 " but got instead: %" PRIu64 "", cmd,
                foundCmd);
        }

        if (memcmp(foundData, data, foundLen) != 0) {
            ERRR("Found data doesn't match original data!");
        }
    }

    TEST("get next...") {
        const uint8_t *foundData;
        size_t foundLen;
        uint64_t foundKey;
        uint64_t foundTerm;
        uint64_t foundCmd;
        /* Again, startKey+1 exists, so ask for startKey to get +1 */
        bool g1 = kvidxGetNext(i, startKey, &foundKey, &foundTerm, &foundCmd,
                               &foundData, &foundLen);

        if (!g1) {
            ERRR("Didn't find next row!");
        }

        if (foundKey != startKey + 1) {
            ERR("Didn't find expected key!  Wanted %" PRIu64 " but got %" PRIu64
                "!",
                startKey + 1, foundKey);
        }

        if (foundTerm != startTerm + 1) {
            ERRR("Didn't find expected term!");
        }

        if (foundLen != sizeof(data)) {
            ERR("Returned length wrong!  Data size: %zu but found length: "
                "%zu!",
                sizeof(data), foundLen);
        }

        if (foundCmd != cmd) {
            ERR("Expected cmd %" PRIu64 " but got instead: %" PRIu64 "", cmd,
                foundCmd);
        }

        if (memcmp(foundData, data, foundLen) != 0) {
            ERRR("Found data doesn't match original data!");
        }
    }

    bool f1 = kvidxFsync(i);
    if (!f1) {
        ERRR("Failed to fsync!");
    }

    bool closed2 = kvidxClose(i);
    if (!closed2) {
        ERRR("Failed to final close!");
    }

    printf("Removing test file...\n");
    unlink(filename);

    TEST_FINAL_RESULT;
    return err;
}
