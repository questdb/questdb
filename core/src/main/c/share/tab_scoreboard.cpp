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

    scoreboard_slot(const scoreboard_slot &other) {
        timestamp = other.timestamp;
        txn = other.txn;
    }

    bool operator<(slot_val_t other) const {
        return timestamp < std::get<0>(other) || timestamp == std::get<0>(other) && txn < std::get<1>(other);
    }

    bool operator>(slot_val_t other) const {
        return timestamp > std::get<0>(other) || timestamp == std::get<0>(other) && txn > std::get<1>(other);
    }

    bool operator==(slot_val_t other) const {
        return timestamp == std::get<0>(other) && txn == std::get<1>(other);
    }

    bool operator==(const scoreboard_slot &other) const {
        return timestamp == other.timestamp && txn == other.txn;
    }

} scoreboard_slot_t;

typedef struct {
    uint64_t partition_count;
    volatile int64_t access_counter;
    volatile int64_t active_reader_counter;
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

inline int64_t entity(int64_t a) {
    return a;
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

int64_t binary_search(const scoreboard_slot_t *slots, int64_t partition_count, int64_t timestamp, int64_t txn) {
    const slot_val_t slot_val(timestamp, txn);
    if (partition_count > 0) {
        int64_t index = binary_search(
                slots,
                slot_val,
                0,
                partition_count - 1,
                1
        );
        return index;
    }
    return -1;
}

inline scoreboard_slot_t *find_slot(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    const int64_t partition_count = scoreboard->header.partition_count;
    scoreboard_slot_t *slots = scoreboard->slot;
    const int64_t index = binary_search(slots, partition_count, timestamp, txn);
    if (index > -1) {
        return &slots[index];
    }
    return nullptr;
}


template<typename F>
inline void process_partition_for_read(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn, F next) {
    cas_loop(scoreboard, inc, positive);
    scoreboard_slot_t *slot = find_slot(scoreboard, timestamp, txn);
    if (slot) {
        cas_loop(&(slot->access_counter), next, positive);
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

inline int64_t get_partition_index(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    cas_loop(scoreboard, inc, positive);
    const int64_t index = binary_search(scoreboard->slot, scoreboard->header.partition_count, timestamp, txn);
    cas_loop(scoreboard, dec, positive);
    return index;
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
    const int64_t partition_count = scoreboard->header.partition_count;
    scoreboard_slot_t *slots = scoreboard->slot;
    int64_t index = binary_search(slots, partition_count, timestamp, txn);

    if (index < 0) {
        index = -index - 1;
        // extend the scoreboard
        const int64_t len = (partition_count - index);
        if (len > 0) {
            memmove(
                    slots + index + 1,
                    slots + index,
                    len * sizeof(scoreboard_slot_t)
            );
        }
        slots[index].timestamp = timestamp;
        slots[index].txn = txn;
        slots[index].access_counter = 0;
        scoreboard->header.partition_count++;
        return true;
    }
    return false;
}

inline bool remove_partition_unsafe(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    const int64_t partition_count = scoreboard->header.partition_count;
    scoreboard_slot_t *slots = scoreboard->slot;
    int64_t index = binary_search(slots, partition_count, timestamp, txn);
    if (index > -1) {
        // extend the scoreboard
        int64_t len = (partition_count - index - 1);
        memmove(&(slots[index]), &(slots[index + 1]), len * sizeof(scoreboard_slot_t));
        scoreboard->header.partition_count = partition_count - 1;
        return true;
    }
    return false;
}

inline int64_t get_access_counter(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    scoreboard_slot_t *slot = find_slot(scoreboard, timestamp, txn);
    if (slot) {
        return __atomic_load_n(&(slot->access_counter), __ATOMIC_RELAXED);
    }
    return -2;
}


inline bool lock_partition_for_write(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    // single write mode, process/thread that owns the writer would not
    // be resizing scoreboard at the same time as locking partition slot
    scoreboard_slot_t *slot = find_slot(scoreboard, timestamp, txn);
    if (slot) {
        int64_t current = slot->access_counter;
        int64_t prev = current > 0 ? 0 : current;
        current = __sync_val_compare_and_swap(&(slot->access_counter), prev, prev - 1);
        return current == prev;
    }
    return false;
}

inline void unlock_partition_for_write(scoreboard_t *scoreboard, int64_t timestamp, int64_t txn) {
    scoreboard_slot_t *slot = find_slot(scoreboard, timestamp, txn);
    if (slot) {
        // single-writer mode, this does not require CAS
        slot->access_counter = 0;
    }
}

inline int64_t get_active_reader_counter(scoreboard_t *scoreboard) {
    return __atomic_load_n(&(scoreboard->header.active_reader_counter), __ATOMIC_RELAXED);
}

extern "C" {
JNIEXPORT jlong JNICALL Java_io_questdb_cairo_Scoreboard_getScoreboardSize
        (JNIEnv *e, jclass cl, jint partitionCount) {
    return get_scoreboard_size(partitionCount);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_Scoreboard_addPartitionUnsafe
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong timestamp, jlong txn) {
    return add_partition_unsafe(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            timestamp,
            txn
    );
}

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_Scoreboard_removePartitionUnsafe
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong timestamp, jlong txn) {
    return remove_partition_unsafe(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            timestamp,
            txn
    );
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_Scoreboard_getAccessCounter
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong timestamp, jlong txn) {
    return get_access_counter(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            timestamp,
            txn
    );
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_Scoreboard_acquireHeaderLock
        (JNIEnv *e, jclass cl, jlong pScoreboard) {
    cas_loop(reinterpret_cast<scoreboard_t *>(pScoreboard), dec, negative);
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_Scoreboard_releaseHeaderLock
        (JNIEnv *e, jclass cl, jlong pScoreboard) {
    cas_loop(reinterpret_cast<scoreboard_t *>(pScoreboard), inc, negative);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_Scoreboard_acquireWriteLock
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong partition, jlong txn) {
    return lock_partition_for_write(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            partition,
            txn
    );
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_Scoreboard_releaseWriteLock
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong partition, jlong txn) {
    unlock_partition_for_write(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            partition,
            txn
    );
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_Scoreboard_acquireReadLock
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong partition, jlong txn) {
    lock_partition_for_read(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            partition,
            txn
    );
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_Scoreboard_releaseReadLock
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong partition, jlong txn) {
    unlock_partition_for_read(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            partition,
            txn
    );
}

JNIEXPORT jint JNICALL Java_io_questdb_cairo_Scoreboard_getPartitionIndex
        (JNIEnv *e, jclass cl, jlong pScoreboard, jlong partition, jlong txn) {
    return get_partition_index(
            reinterpret_cast<scoreboard_t *>(pScoreboard),
            partition,
            txn
    );
}

JNIEXPORT jint JNICALL Java_io_questdb_cairo_Scoreboard_getPartitionCount
        (JNIEnv *e, jclass cl, jlong pScoreboard) {
    return __atomic_load_n(&(reinterpret_cast<scoreboard_t *>(pScoreboard)->header.partition_count), __ATOMIC_RELAXED);
}

JNIEXPORT jint JNICALL Java_io_questdb_cairo_Scoreboard_getHeaderAccessCounter
        (JNIEnv *e, jclass cl, jlong pScoreboard) {
    return __atomic_load_n(&(reinterpret_cast<scoreboard_t *>(pScoreboard)->header.access_counter), __ATOMIC_RELAXED);
}

}