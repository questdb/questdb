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
#include "util.h"
#include "jni.h"
#include "fs.h"

#define MAX_TXN_IN_FLIGHT 4096
#define INITIAL_LO -1

typedef struct txn_board_t {
    int64_t base = INITIAL_LO;
    int64_t max = 0;
    int64_t min = L_MAX;
    uint64_t counts[MAX_TXN_IN_FLIGHT]{};

    void init() {
        if (__sync_val_compare_and_swap(&min, 0, L_MAX) == 0) {
            base = INITIAL_LO;
            max = INITIAL_LO;
        }
    }

    inline int64_t get_min() {
        return __atomic_load_n(&min, __ATOMIC_RELAXED);
    }

    inline int64_t get_max() {
        return __atomic_load_n(&max, __ATOMIC_RELAXED);
    }

    inline uint64_t *get_count_ptr(int64_t offset) {
        return &(counts[offset % MAX_TXN_IN_FLIGHT]);
    }

    [[nodiscard]] inline uint64_t get_offset(int64_t txn) const {
        return txn - base;
    }

    inline uint64_t get_max_offset() {
        return get_offset(get_max());
    }

} txn_board_t;

typedef struct txn_local {
    txn_board_t* p_txn_board;
    uint64_t ref_counter;
    int64_t hMapping;
} txn_local_t;

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

void set_min_atomic(int64_t *slot, int64_t value) {
    do {
        int64_t current = *slot;
        if (value >= current || __sync_val_compare_and_swap(slot, current, value) == current) {
            break;
        }
    } while (true);
}

int64_t txn_release(txn_board_t *p_board, int64_t txn) {
    int64_t offset = p_board->get_offset(txn);
    int64_t max_offset = p_board->get_max_offset();
    if (atomic_next(p_board->get_count_ptr(offset), dec) == 0 && p_board->get_min() == txn) {
        // skip thru all unused txn values up
        while (++offset <= max_offset && *p_board->get_count_ptr(offset) == 0);
        // on first non-zero count update the min value
        const int64_t x = offset + p_board->base;
        set_max_atomic(&(p_board->min), x);
        return x - 1;
    }
    return p_board->get_min() - 1;
}

inline bool txn_acquire(txn_board_t *p_board, int64_t txn) {
    // lazy update "base" only to assign initial value
    __sync_val_compare_and_swap(&(p_board->base), INITIAL_LO, txn);
    const int64_t offset = p_board->get_offset(txn);
    if ((txn - p_board->get_min()) < MAX_TXN_IN_FLIGHT) {
        atomic_next(p_board->get_count_ptr(offset), inc);
        // update txn range
        set_max_atomic(&(p_board->max), txn);
        set_min_atomic(&(p_board->min), txn);
        return true;
    }
    return false;
}

extern "C" {

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_TxnScoreboard_acquire0
        (JNIEnv *e, jclass cl, jlong p_board, jlong txn) {
    return txn_acquire(
            reinterpret_cast<txn_local_t *>(p_board)->p_txn_board,
            txn
    );
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_release0
        (JNIEnv *e, jclass cl, jlong p_board, jlong txn) {
    return txn_release(
            reinterpret_cast<txn_local_t *>(p_board)->p_txn_board,
            txn
    );
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getOffset
        (JNIEnv *e, jclass cl, jlong p_board, jlong txn) {
    return reinterpret_cast<txn_local_t *>(p_board)->p_txn_board->get_offset(txn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getCount
        (JNIEnv *e, jclass cl, jlong p_board, jlong txn) {
    auto *p = reinterpret_cast<txn_local_t *>(p_board)->p_txn_board;
    return *p->get_count_ptr(p->get_offset(txn));
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_create0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    uint64_t size = sizeof(txn_board_t);
    int64_t hMapping;
    auto *pBoard = reinterpret_cast<txn_board_t *>(openShm0(reinterpret_cast<char*>(lpszName), size, &hMapping));
    pBoard->init();

    auto *pTxnLocal = reinterpret_cast<txn_local_t*>(malloc(sizeof(txn_local_t)));
    pTxnLocal->p_txn_board = pBoard;
    pTxnLocal->hMapping = hMapping;
    pTxnLocal->ref_counter = 1;
    return reinterpret_cast<jlong>(pTxnLocal);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_close0
        (JNIEnv *e, jclass cl, jlong lpszName, jlong p_board) {
    auto *pTxnLocal = reinterpret_cast<txn_local_t *>(p_board);
    auto refs_remaining = atomic_next(&(pTxnLocal->ref_counter), dec);
    if (refs_remaining == 0) {
        auto *pTxnBoard = pTxnLocal->p_txn_board;
        auto hMapping = pTxnLocal->hMapping;
        free(pTxnLocal);
        return closeShm0(
                reinterpret_cast<char *>(lpszName),
                pTxnBoard,
                sizeof(txn_board_t),
                hMapping
        );
    }
    return refs_remaining;
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_newRef0
        (JNIEnv *e, jclass cl, jlong p_board) {
    auto *pTxnLocal = reinterpret_cast<txn_local_t *>(p_board);
    atomic_next(&(pTxnLocal->ref_counter), inc);
    return p_board;
}

}
