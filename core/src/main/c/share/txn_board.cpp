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
#include "util.h"
#include "jni.h"

#define MAX_TXN_IN_FLIGHT 4096

void set_max_atomic(int64_t *slot, int64_t value) {
    do {
        int64_t current = __atomic_load_n(slot, __ATOMIC_RELAXED);
        if (value <= current || __sync_val_compare_and_swap(slot, current, value) == current) {
            break;
        }
    } while (true);
}

template<typename NEXT>
inline uint64_t atomic_next(volatile uint64_t *val, NEXT next) {
    do {
        uint64_t current = __atomic_load_n(val, __ATOMIC_RELAXED);
        uint64_t n = next(current);
        if (__sync_val_compare_and_swap(val, current, n) == current) {
            return n;
        }
    } while (true);
}

inline uint64_t inc(uint64_t val) {
    return val + 1;
}

inline uint64_t dec(uint64_t val) {
    return val - 1;
}

class txn_scoreboard_t {
    int64_t max = 0;
    // 1-based min txn that is in-use
    // we have to use 1 based txn to rule out possibility of 0 txn
    // 0 is initial value when shared memory is created
    int64_t min = L_MAX;
    uint64_t counts[MAX_TXN_IN_FLIGHT]{};

public:

    inline int64_t get_min() {
        return __atomic_load_n(&min, __ATOMIC_RELAXED);
    }

    inline int64_t get_max() {
        return __atomic_load_n(&max, __ATOMIC_RELAXED);
    }

    inline uint64_t *get_count_ptr(int64_t offset) {
        return &(counts[offset % MAX_TXN_IN_FLIGHT]);
    }

    inline int64_t *get_max_ptr() {
        return &max;
    }

    inline int64_t *get_min_ptr() {
        return &min;
    }

    inline uint64_t get_count(int64_t offset) {
        return __atomic_load_n(get_count_ptr(offset), __ATOMIC_RELAXED);
    }

    inline int64_t get_max_offset() {
        return get_max();
    }

    inline int64_t get_min_offset() {
        return get_min();
    }

    inline void update_min(const int64_t offset) {
        const int64_t min_offset = get_min_offset();
        int64_t o = min_offset;
        while (o < offset && get_count(o) == 0) {
            o++;
        }
        set_max_atomic(get_min_ptr(), o);
    }

    inline void txn_release(int64_t txn) {
        const int64_t max_offset = get_max_offset();
        if (atomic_next(get_count_ptr(txn), dec) == 0 && get_min() == txn) {
            update_min(max_offset);
        }
    }

    inline bool txn_acquire(int64_t txn) {
        if (txn - get_min() >= MAX_TXN_IN_FLIGHT) {
            update_min(txn);
        }

        if (txn - get_min() < MAX_TXN_IN_FLIGHT) {
            atomic_next(get_count_ptr(txn), inc);
            set_max_atomic(&max, txn);
            update_min(txn);
            return true;
        }
        return false;
    }
};

extern "C" {

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_TxnScoreboard_acquireTxn0
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    return reinterpret_cast<txn_scoreboard_t *>(p_txn_scoreboard)->txn_acquire(txn);
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_TxnScoreboard_releaseTxn0
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    reinterpret_cast<txn_scoreboard_t *>(p_txn_scoreboard)->txn_release(txn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getCount
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    return (jlong)(reinterpret_cast<txn_scoreboard_t *>(p_txn_scoreboard))->get_count(txn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getMin
        (JAVA_STATIC, jlong p_txn_scoreboard) {
    return reinterpret_cast<txn_scoreboard_t *>(p_txn_scoreboard)->get_min();
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getScoreboardSize
        (JAVA_STATIC) {
    return sizeof(txn_scoreboard_t);
}
}
