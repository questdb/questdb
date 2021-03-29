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
#include <cstring>
#include "jni.h"

#define MAX_TXN_IN_FLIGHT 4096
#define INITIAL_LO -1

typedef struct txn_board_t {
    int64_t lo = INITIAL_LO;
    int64_t hi = 0;
    int64_t min = INITIAL_LO;
    uint64_t counts[MAX_TXN_IN_FLIGHT]{};

    [[nodiscard]] int64_t get_lo() const {
        return lo;
    }

    int64_t get_min() {
        return __atomic_load_n(&min, __ATOMIC_RELAXED);
    }

    int64_t get_hi() {
        return __atomic_load_n(&hi, __ATOMIC_RELAXED);
    }
} txn_board_t;

inline uint64_t inc(uint64_t val) {
    return val + 1;
}

inline uint64_t dec(uint64_t val) {
    return val - 1;
}

template<typename NEXT>
inline uint64_t atomic_next(volatile uint64_t *val, NEXT next) {
    do {
        uint64_t current = *val;
        uint64_t n = next(current);
        if (__sync_val_compare_and_swap(val, current, n) == current) {
            return n;
        }
    } while (true);
}

void set_max_atomic(int64_t *slot, int64_t value) {
    do {
        int64_t current = *slot;
        if (value <= current || __sync_val_compare_and_swap(slot, current, value) == current) {
            break;
        }
    } while (true);
}

int64_t txn_release(txn_board_t *p_board, int64_t txn) {
    const int64_t lo = p_board->get_lo();
    const int64_t hi = p_board->get_hi();
    int64_t offset = txn - lo;
    int64_t max = hi - lo;
    if (atomic_next(&(p_board->counts[offset%MAX_TXN_IN_FLIGHT]), dec) == 0 && (offset%MAX_TXN_IN_FLIGHT) == 0) {
        // find next "lo" value only if we decremented "edge" txn to 0
        while (++offset <= max) {
            if (p_board->counts[offset%MAX_TXN_IN_FLIGHT] > 0) {
                set_max_atomic(&(p_board->min), offset + txn);
                return offset + txn;
            }
        }
    }
    return p_board->get_min();
}

inline bool txn_acquire(txn_board_t *p_board, int64_t txn) {
    // lazy update "lo" only to assign initial value
    __sync_val_compare_and_swap(&(p_board->lo), INITIAL_LO, txn);

    int64_t offset = (txn - p_board->get_lo());
    if ((txn - p_board->get_min()) < MAX_TXN_IN_FLIGHT) {
        atomic_next(&(p_board->counts[offset%MAX_TXN_IN_FLIGHT]), inc);
        // CAS "hi" to this txn
        set_max_atomic(&(p_board->hi), txn);
        return true;
    }
    return false;
}

extern "C" {

JNIEXPORT jint JNICALL Java_io_questdb_cairo_TxnScoreboard_getScoreboardSize
        (JNIEnv *e, jclass cl) {
    return sizeof(txn_board_t);
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_TxnScoreboard_init
        (JNIEnv *e, jclass cl, jlong p_board) {
    auto *p = reinterpret_cast<txn_board_t *>(p_board);
    p->lo = INITIAL_LO;
    p->hi = INITIAL_LO;
    p->min = INITIAL_LO;
    memset(p->counts, 0, MAX_TXN_IN_FLIGHT * sizeof(uint64_t));
}

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_TxnScoreboard_acquire
        (JNIEnv *e, jclass cl, jlong p_board, jlong txn) {
    return txn_acquire(
            reinterpret_cast<txn_board_t *>(p_board),
            txn
    );
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_release
        (JNIEnv *e, jclass cl, jlong p_board, jlong txn) {
    return txn_release(
            reinterpret_cast<txn_board_t *>(p_board),
            txn
    );
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getLo
        (JNIEnv *e, jclass cl, jlong p_board) {
    return reinterpret_cast<txn_board_t *>(p_board)->get_lo();
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getHi
        (JNIEnv *e, jclass cl, jlong p_board) {
    return reinterpret_cast<txn_board_t *>(p_board)->get_hi();
}

}
