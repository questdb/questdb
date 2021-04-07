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
#include "fs.h"

#define MAX_TXN_IN_FLIGHT 4096
#define INITIAL_LO (-1)

typedef struct txn_scoreboard_t {
    int64_t base = INITIAL_LO;
    int64_t max = 0;
    // 1-based min txn that is in-use
    // we have to use 1 based txn to rule out possibility of 0 txn
    // 0 is initial value when shared memory is created
    int64_t min = L_MAX;
    uint64_t counts[MAX_TXN_IN_FLIGHT]{};

    void init(int64_t txn) {
        if (__sync_val_compare_and_swap(&min, 0, txn + 1) == 0) {
            base = txn;
            max = txn;
        }
    }

    inline int64_t get_min() {
        return __atomic_load_n(&min, __ATOMIC_RELAXED) - 1;
    }

    inline int64_t get_max() {
        return __atomic_load_n(&max, __ATOMIC_RELAXED);
    }

    inline uint64_t *get_count_ptr(int64_t offset) {
        return &(counts[offset % MAX_TXN_IN_FLIGHT]);
    }

    inline uint64_t get_count(int64_t offset) {
        return __atomic_load_n(get_count_ptr(offset), __ATOMIC_RELAXED);
    }

    [[nodiscard]] inline uint64_t txn_to_offswt(int64_t txn) const {
        return txn - base;
    }

    [[nodiscard]] inline int64_t offset_to_txn(int64_t offset) const {
        return offset + base;
    }

    inline uint64_t get_max_offset() {
        return txn_to_offswt(get_max());
    }

    inline uint64_t get_min_offset() {
        return txn_to_offswt(get_min());
    }

} txn_scoreboard_t;

typedef struct txn_local {
    txn_scoreboard_t* p_txn_scoreboard;
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
        uint64_t current = __atomic_load_n(val, __ATOMIC_RELAXED);
        uint64_t n = next(current);
        if (__sync_val_compare_and_swap(val, current, n) == current) {
            return n;
        }
    } while (true);
}

void set_max_atomic(int64_t *slot, int64_t value) {
    do {
        int64_t current = __atomic_load_n(slot, __ATOMIC_RELAXED);
        if (value <= current || __sync_val_compare_and_swap(slot, current, value) == current) {
            break;
        }
    } while (true);
}

void txn_release(txn_scoreboard_t *p_scoreboard, int64_t txn) {
    int64_t offset = p_scoreboard->txn_to_offswt(txn);
    const int64_t max_offset = p_scoreboard->get_max_offset();
    if (atomic_next(p_scoreboard->get_count_ptr(offset), dec) == 0 && p_scoreboard->get_min() == txn) {
        // skip thru all unused txn values up
        while (++offset <= max_offset && p_scoreboard->get_count(offset) == 0);
        // on first non-zero count update the min value
        set_max_atomic(&(p_scoreboard->min), p_scoreboard->offset_to_txn(offset <= max_offset ? offset : max_offset) + 1);
    }
}

inline bool txn_acquire(txn_scoreboard_t *p_scoreboard, int64_t txn) {
    const int64_t offset = p_scoreboard->txn_to_offswt(txn);
    if ((txn - p_scoreboard->get_min()) < MAX_TXN_IN_FLIGHT) {
        atomic_next(p_scoreboard->get_count_ptr(offset), inc);
        // update max - this could be a new txn
        set_max_atomic(&(p_scoreboard->max), txn);
        // update min
        const int64_t min_offset = p_scoreboard->get_min_offset();
        int64_t o = min_offset;
        while (o < offset && p_scoreboard->get_count(o) == 0) {
            o++;
        }
        set_max_atomic(&(p_scoreboard->min), p_scoreboard->offset_to_txn(o) + 1);
        return true;
    }
    return false;
}

extern "C" {

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_TxnScoreboard_acquire0
        (JNIEnv *e, jclass cl, jlong p_local, jlong txn) {
    return txn_acquire(
            reinterpret_cast<txn_local_t *>(p_local)->p_txn_scoreboard,
            txn
    );
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_TxnScoreboard_release0
        (JNIEnv *e, jclass cl, jlong p_local, jlong txn) {
    txn_release(
            reinterpret_cast<txn_local_t *>(p_local)->p_txn_scoreboard,
            txn
    );
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getOffset
        (JNIEnv *e, jclass cl, jlong p_local, jlong txn) {
    return reinterpret_cast<txn_local_t *>(p_local)->p_txn_scoreboard->txn_to_offswt(txn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getCount
        (JNIEnv *e, jclass cl, jlong p_local, jlong txn) {
    auto *p = reinterpret_cast<txn_local_t *>(p_local)->p_txn_scoreboard;
    return p->get_count(p->txn_to_offswt(txn));
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_TxnScoreboard_init
        (JNIEnv *e, jclass cl, jlong p_board, jlong txn) {
    reinterpret_cast<txn_local_t *>(p_board)->p_txn_scoreboard->init(txn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getMin
        (JNIEnv *e, jclass cl, jlong p_local) {
    auto *p = reinterpret_cast<txn_local_t *>(p_local)->p_txn_scoreboard;
    return p->get_min();
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getMax
        (JNIEnv *e, jclass cl, jlong p_local) {
    auto *p = reinterpret_cast<txn_local_t *>(p_local)->p_txn_scoreboard;
    return p->get_max();
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_create0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    uint64_t size = sizeof(txn_scoreboard_t);
    int64_t hMapping;
    auto *pBoard = reinterpret_cast<txn_scoreboard_t *>(openShm0(reinterpret_cast<char *>(lpszName), size, &hMapping));
    auto *pTxnLocal = reinterpret_cast<txn_local_t *>(malloc(sizeof(txn_local_t)));
    pTxnLocal->p_txn_scoreboard = pBoard;
    pTxnLocal->hMapping = hMapping;
    pTxnLocal->ref_counter = 1;
    return reinterpret_cast<jlong>(pTxnLocal);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_close0
        (JNIEnv *e, jclass cl, jlong lpszName, jlong p_local) {
    auto *pTxnLocal = reinterpret_cast<txn_local_t *>(p_local);
    auto refs_remaining = atomic_next(&(pTxnLocal->ref_counter), dec);
    if (refs_remaining == 0) {
        auto *pTxnBoard = pTxnLocal->p_txn_scoreboard;
        auto hMapping = pTxnLocal->hMapping;
        free(pTxnLocal);
        closeShm0(
                reinterpret_cast<char *>(lpszName),
                pTxnBoard,
                sizeof(txn_scoreboard_t),
                hMapping
        );
        return 0;
    }
    return refs_remaining;
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_newRef0
        (JNIEnv *e, jclass cl, jlong p_local) {
    auto *pTxnLocal = reinterpret_cast<txn_local_t *>(p_local);
    atomic_next(&(pTxnLocal->ref_counter), inc);
    return p_local;
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getScoreboardSize
        (JNIEnv *e, jclass cl) {
    return sizeof(txn_scoreboard_t);
}

}
