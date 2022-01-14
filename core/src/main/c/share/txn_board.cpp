/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
#include <atomic>

#define COUNTER_T uint16_t

template<typename T>
void set_max_atomic(std::atomic<T> &slot, T value) {
    do {
        T current = slot.load();
        if (value <= current || slot.compare_exchange_strong(current, value)) {
            break;
        }
    } while (true);
}

template<typename T>
class txn_scoreboard_t {
    uint32_t mask = 0;
    uint32_t size = 0;
    std::atomic<int64_t> max = 0;
    std::atomic<int64_t> min = L_MAX;
    // 1-based min txn that is in-use
    // we have to use 1 based txn to rule out possibility of 0 txn
    // 0 is initial value when shared memory is created
    std::atomic<uint64_t> version = 0;
    std::atomic<T> counts[];

    inline static T inc(T val) {
        return val + 1;
    }

    inline static T dec(T val) {
        return val - 1;
    }

    inline std::atomic<T> &_get_count(const int64_t offset) {
        return counts[offset & mask];
    }

    inline bool increment_count(int64_t txn) {
        uint64_t current_version;
        while ((current_version = version.load(std::memory_order_acquire)) % 2 == 1) {
            // Locked by update_min. Spin wait.
        }

        while (true) {
            auto current_min = min.load(std::memory_order_acquire);
            if (current_min > txn) {
                return false;
            }
            _get_count(txn).fetch_add(1, std::memory_order_acq_rel);
            if (min.load(std::memory_order_acquire) != current_version) {
                // Min has been updated. Roll back and retry.
                _get_count(txn).fetch_add(-1, std::memory_order_acq_rel);
            } else {
                return true;
            }
        }
    }

    inline int64_t update_min() {
        uint64_t current_version;
        while (true) {
            current_version = version.load(std::memory_order_acquire);
            if (current_version % 2 == 0) {
                // Unlocked. Lock it
                if (version.compare_exchange_strong(current_version, current_version + 1, std::memory_order_acq_rel)) {
                    break;
                }
            }
            // Locked, someone else updates it. Wait, this call has to return best possible min
        }

        auto new_min = min.load(std::memory_order_acquire);
        const auto initial_min = new_min;
        const auto next_max = new_min + size;
        while (new_min < next_max) {
            while (new_min < next_max && get_count_unchecked(new_min) == 0) {
                new_min++;
            }

            if (new_min > initial_min) {
                min.store(new_min, std::memory_order_release);
            }
            version.store(current_version + 2, std::memory_order_release);
        }
        return new_min;
    }

public:

    inline int64_t get_clean_min() {
        return update_min();
    }

    inline T get_count(int64_t offset) {
        if (offset < min.load()) {
            // This can be dirty increment below min
            // everything below min is considered 0
            return 0;
        }
        return get_count_unchecked(offset);
    }

    inline T get_count_unchecked(const int64_t offset) {
        return _get_count(offset).load();
    }

    inline void txn_release(int64_t txn) {
        // this is atomic decrement
        _get_count(txn)--;
    }

    inline int32_t txn_acquire(int64_t txn) {
        int64_t _min = min.load(std::memory_order_acquire);
        if (_min == L_MAX) {
            if (min.compare_exchange_weak(_min, txn, std::memory_order_acq_rel)) {
                _min = txn;
            }
        }

        if (txn - _min >= size) {
            // lazy update min when the range is exhausted
            _min = update_min();
        }

        if (txn < _min) {
            return -2;
        }

        if (txn - _min < size) {
            if (!increment_count(txn)) {
                // Race lost, someone updated min to higher value. Roll back the increment.
                return -2;
            }
            set_max_atomic(max, txn);
            return 0;
        }
        return -1;
    }


    void init(uint32_t entry_count) {
        mask = entry_count - 1;
        size = entry_count;
    }
};

extern "C" {

JNIEXPORT jint JNICALL Java_io_questdb_cairo_TxnScoreboard_acquireTxn0
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    return reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->txn_acquire(txn);
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
    return reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->get_clean_min();
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getScoreboardSize
        (JAVA_STATIC, jlong entryCount) {
    return (jlong)sizeof(txn_scoreboard_t<COUNTER_T>) + entryCount * (jlong) sizeof(std::atomic<COUNTER_T>);
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_TxnScoreboard_init
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong entryCount) {
    reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->init(entryCount);
}

}
