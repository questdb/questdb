/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#include <cstdint>
#include <tuple>
#include <cstring>
#include "util.h"

typedef std::tuple<int64_t, int64_t> slot_val_t;

// scoreboard slot, two fields we sort the slots on
typedef struct scoreboard_slot {
    int64_t timestamp;
    int64_t txn;
    volatile int64_t access_counter{};
//    uint64_t _unused{};

    scoreboard_slot(scoreboard_slot &other) {
        timestamp = other.timestamp;
        txn = other.txn;
    }

    bool operator<(slot_val_t other) const {
        return timestamp < std::get<0>(other) || timestamp == std::get<0>(other) && txn < std::get<1>(other);
    }

    bool operator>(slot_val_t other) const {
        return timestamp > std::get<0>(other) || timestamp == std::get<0>(other) && txn > std::get<1>(other);
    }

    bool operator==(const scoreboard_slot &other) const {
        return timestamp == other.timestamp && txn == other.txn;
    }

} scoreboard_slot_t;

typedef struct {
    uint64_t partition_count;
    volatile int64_t access_counter;
    uint64_t _unused1;
    uint64_t _unused2;
} scoreboard_header_t;

typedef struct {
    scoreboard_header_t header;
    scoreboard_slot_t slot[];
} scoreboard_t;

typedef struct {
    int64_t timestamp;
    int64_t size;
    int64_t name_txn;
    int64_t data_txn;
} txn_slot_t;

// used in template to either increment or decrement atomic values
inline int64_t inc(int64_t val) {
    return val + 1;
}

inline int64_t dec(int64_t val) {
    return val - 1;
}

inline int64_t positive(int64_t a) {
    return a < 0 ? 0 : a;
}

inline int64_t negative(int64_t a) {
    return a > 0 ? 0 : a;
}

template<typename F, typename M>
void cas_loop(volatile int64_t *val, F next, M limiter) {
    int64_t current = *val;
    int64_t prev = current;
    do {
        prev = limiter(prev);
        current = __sync_val_compare_and_swap(val, prev, next(prev));
    } while (current != prev);
}

template<typename F, typename M>
inline void cas_loop(scoreboard_t *scoreboard, F next, M limiter) {
    cas_loop(
            &(scoreboard->header.access_counter),
            next,
            limiter
    );
}

template<typename F>
inline void process_partition_for_read(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn, F next) {
    cas_loop(scoreboard, inc, positive);
    const slot_val_t slot_val(timestamp, txn);
    int64_t index = binary_search(
            scoreboard->slot,
            slot_val,
            0,
            scoreboard->header.partition_count,
            1
    );

    // check if partition exists
    if (scoreboard->slot[index].timestamp == timestamp && scoreboard->slot[index].txn == txn) {
        cas_loop(&(scoreboard->slot[index].access_counter), next, positive);
    }

    cas_loop(scoreboard, dec, positive);
}

inline void lock_partition_for_read(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    process_partition_for_read(
            scoreboard,
            timestamp,
            txn,
            inc
    );
}

inline void unlock_partition_for_read(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    process_partition_for_read(
            scoreboard,
            timestamp,
            txn,
            dec
    );
}

inline void prepare(void *pTxn, uint32_t pTxnCount, scoreboard_t *scoreboard) {
    auto *slots = reinterpret_cast<txn_slot_t *>(pTxn);
    for (uint32_t i = 0; i < pTxnCount; i++) {
        scoreboard->slot[i].timestamp = slots[i].timestamp;
        scoreboard->slot[i].txn = slots[i].name_txn;
    }
}

inline int64_t get_scoreboard_size(int32_t partitionCount) {
    return sizeof(scoreboard_t) + sizeof(scoreboard_slot_t) * partitionCount;
}

inline bool add_partition_unsafe(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    const slot_val_t slot_val(timestamp, txn);
    int64_t index = binary_search(
            scoreboard->slot,
            slot_val,
            0,
            scoreboard->header.partition_count,
            1
    );

    bool ok;
    if (scoreboard->slot[index].timestamp != timestamp || scoreboard->slot[index].txn != txn) {
        // extend the scoreboard
        const int64_t len = (scoreboard->header.partition_count - index);
        if (len > 0) {
            memmove(
                    scoreboard->slot + index + 1,
                    scoreboard->slot + index,
                    len * sizeof(scoreboard_slot_t)
            );
        }
        scoreboard->slot[index].timestamp = timestamp;
        scoreboard->slot[index].txn = txn;
        scoreboard->slot[index].access_counter = 0;
        scoreboard->header.partition_count++;
        ok = true;
    } else {
        ok = false;
    }
    return ok;
}

inline bool remove_partition_unsafe(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    const slot_val_t slot_val(timestamp, txn);
    int64_t index = binary_search(
            scoreboard->slot,
            slot_val,
            0,
            scoreboard->header.partition_count,
            1
    );

    bool ok;
    if (scoreboard->slot[index].timestamp == timestamp && scoreboard->slot[index].txn == txn) {
        // extend the scoreboard
        int64_t len = (scoreboard->header.partition_count - index - 1);
        memmove(
                &(scoreboard->slot[index]),
                &(scoreboard->slot[index+1]),
                len * sizeof(scoreboard_slot_t)
        );
        scoreboard->header.partition_count--;
        ok = true;
    } else {
        ok = false;
    }
    return ok;
}

inline int64_t get_access_counter(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    const slot_val_t slot_val(timestamp, txn);
    int64_t index = binary_search(
            scoreboard->slot,
            slot_val,
            0,
            scoreboard->header.partition_count,
            1
    );

    if (scoreboard->slot[index].timestamp == timestamp && scoreboard->slot[index].txn == txn) {
        return scoreboard->slot[index].access_counter;
    } else {
        return -2;
    }
}


inline bool lock_partition_for_write(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    // single write mode, process/thread that owns the writer would not
    // be resizing scoreboard at the same time as locking partition slot
    const slot_val_t slot_val(timestamp, txn);
    int64_t index = binary_search(
            scoreboard->slot,
            slot_val,
            0,
            scoreboard->header.partition_count,
            1
    );
    if (scoreboard->slot[index].timestamp == timestamp && scoreboard->slot[index].txn == txn) {
        int64_t current = *&(scoreboard->slot[index].access_counter);
        int64_t prev = current > 0 ? 0 : current;
        current = __sync_val_compare_and_swap(&(scoreboard->slot[index].access_counter), prev, prev - 1);
        return current == prev;
    }
    return false;
}

inline void unlock_partition_for_write(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    const slot_val_t slot_val(timestamp, txn);
    int64_t index = binary_search(
            scoreboard->slot,
            slot_val,
            0,
            scoreboard->header.partition_count,
            1
    );
    if (scoreboard->slot[index].timestamp == timestamp && scoreboard->slot[index].txn == txn) {
        // single-writer mode, this does not require CAS
        scoreboard->slot[index].access_counter = 0;
    }
}

extern "C" {
JNIEXPORT jlong JNICALL Java_io_questdb_cairo_ScoreboardWriter_getScoreboardSize
        (JNIEnv *e, jclass cl, jint partitionCount) {
    return get_scoreboard_size(partitionCount);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_ScoreboardWriter_addPartitionUnsafe
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong timestamp, jlong txn) {
    return add_partition_unsafe(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            timestamp,
            txn
    );
}

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_ScoreboardWriter_removePartitionUnsafe
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong timestamp, jlong txn) {
    return remove_partition_unsafe(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            timestamp,
            txn
    );
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_ScoreboardWriter_getAccessCounter
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong timestamp, jlong txn) {
    return get_access_counter(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            timestamp,
            txn
    );
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_ScoreboardWriter_acquireHeaderLock
        (JNIEnv *e, jclass cl, jlong pScoreboard) {
    cas_loop(reinterpret_cast<scoreboard_t *>(pScoreboard), dec, negative);
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_ScoreboardWriter_releaseHeaderLock
        (JNIEnv *e, jclass cl, jlong pScoreboard) {
    cas_loop(reinterpret_cast<scoreboard_t *>(pScoreboard), inc, negative);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_ScoreboardWriter_acquireWriteLock
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong partition, jlong txn) {
    return lock_partition_for_write(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            partition,
            txn
    );
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_ScoreboardWriter_releaseWriteLock
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong partition, jlong txn) {
    unlock_partition_for_write(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            partition,
            txn
    );
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_ScoreboardWriter_acquireReadLock
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong partition, jlong txn) {
    lock_partition_for_read(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            partition,
            txn
    );
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_ScoreboardWriter_releaseReadLock
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong partition, jlong txn) {
    unlock_partition_for_read(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            partition,
            txn
    );
}

JNIEXPORT jint JNICALL Java_io_questdb_cairo_ScoreboardWriter_getPartitionCount
        (JNIEnv *e, jclass cl, jlong pScoreboard) {
    return reinterpret_cast<scoreboard_t*>(pScoreboard)->header.partition_count;
}
}