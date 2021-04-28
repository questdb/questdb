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

#define COUNTER_T uint16_t

template<typename T>
void set_max_atomic(T *slot, T value) {
    do {
        T current = __atomic_load_n(slot, __ATOMIC_RELAXED);
        if (value <= current || __sync_val_compare_and_swap(slot, current, value) == current) {
            break;
        }
    } while (true);
}

template<typename NEXT, typename T>
inline T atomic_next(volatile T *val, NEXT next) {
    do {
        T current = __atomic_load_n(val, __ATOMIC_RELAXED);
        T n = next(current);
        if (__sync_val_compare_and_swap(val, current, n) == current) {
            return n;
        }
    } while (true);
}

template<typename T>
class txn_scoreboard_t {
    uint32_t mask = 0;
    uint32_t size = 0;
    int64_t max = 0;
    // 1-based min txn that is in-use
    // we have to use 1 based txn to rule out possibility of 0 txn
    // 0 is initial value when shared memory is created
    int64_t min = L_MAX;
    T counts[];

    inline static T inc(T val) {
        return val + 1;
    }

    inline static T dec(T val) {
        return val - 1;
    }

public:

    inline int64_t get_min() {
        return __atomic_load_n(&min, __ATOMIC_RELAXED);
    }

    inline int64_t get_max() {
        return __atomic_load_n(&max, __ATOMIC_RELAXED);
    }

    inline T *get_count_ptr(int64_t offset) {
        return &(counts[offset & mask]);
    }

    inline T get_count(int64_t offset) {
        return __atomic_load_n(get_count_ptr(offset), __ATOMIC_RELAXED);
    }

    inline void update_min(const int64_t offset) {
        int64_t o = get_min();
        while (o < offset && get_count(o) == 0) {
            o++;
        }
        set_max_atomic(&min, o);
    }

    inline void txn_release(int64_t txn) {
        const int64_t max_offset = get_max();
        if (atomic_next(get_count_ptr(txn), dec) == 0 && get_min() == txn) {
            update_min(max_offset);
        }
    }

    inline bool txn_acquire(int64_t txn) {
        if (txn - get_min() >= size) {
            update_min(txn);
        }

        if (txn - get_min() < size) {
            atomic_next(get_count_ptr(txn), inc);
            set_max_atomic(&max, txn);
            update_min(txn);
            return true;
        }
        return false;
    }

    inline bool is_txn_avalable(int64_t txn) {
        int64_t _min = get_min();
        if (_min == -1 || get_count(txn) == 0) {
            return true;
        }
        update_min(txn);
        _min = get_min();
        return _min == txn && get_count(txn) == 0;
    }

    void init(uint32_t entry_count) {
        mask = entry_count - 1;
        size = entry_count;
    }
};

extern "C" {

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_TxnScoreboard_acquireTxn0
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    return reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->txn_acquire(txn);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_TxnScoreboard_isTxnAvailable
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    return reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->is_txn_avalable(txn);
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_TxnScoreboard_releaseTxn0
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->txn_release(txn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getCount
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    return (jlong) (reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard))->get_count(txn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getMin
        (JAVA_STATIC, jlong p_txn_scoreboard) {
    return reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->get_min();
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getScoreboardSize
        (JAVA_STATIC, jlong entryCount) {
    return sizeof(txn_scoreboard_t<COUNTER_T>) + entryCount * sizeof(COUNTER_T);
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_TxnScoreboard_init
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong entryCount) {
    reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->init(entryCount);
}

}
