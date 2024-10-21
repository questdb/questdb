/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
#include <algorithm>

#define COUNTER_T uint16_t

template<typename T>
class txn_scoreboard_t {
    uint32_t mask = 0;
    uint32_t size = 0;
    std::atomic<int64_t> max{0};
    // The min txn that is in-use. Increases monotonically.
    // Once the scoreboard is initialized, min is guaranteed to be
    // greater than 0.
    std::atomic<int64_t> min{0};
    std::atomic<T> counts[];

    template<typename TT>
    inline static TT set_max_atomic(std::atomic<TT> &slot, TT value) {
        TT current = slot.load();
        while (value > current && !slot.compare_exchange_weak(current, value));
        return std::max(value, current);
    }

    inline std::atomic<T> &get_counter(const int64_t offset) {
        return counts[offset & mask];
    }

    inline bool increment_count(int64_t txn) {
        // Increment txn count
        // but do not allow to use txn below max value
        // Once there is count for txn 100
        // there cannot be increments for txn 99, 98 etc
        // When an attempt to acquire below max happens
        // the attempt is rejected and TableReader should re-read _txn file
        auto current_max = max.load();
        if (txn < current_max || txn - min.load() >= size) {
            return false;
        }
        get_counter(txn)++; // atomic

        current_max = max.load();
        while (txn > current_max && !max.compare_exchange_weak(current_max, txn));

        if (txn < current_max || txn - min.load() >= size) {
            // We cannot increment below max, only max or higher
            // Also incrementing beyond size is not allowed
            // Roll back the increment
            get_counter(txn)--; //atomic
            return false;
        }
        return true;
    }

    inline int64_t update_min(const int64_t offset) {
        return set_max_atomic(min, calculate_min(offset));
    }

    inline int64_t calculate_min(const int64_t &offset) {
        int64_t o = min.load();
        while (o < offset && get_count(o) == 0) {
            o++;
        }
        return o;
    }

public:

    inline int64_t get_clean_min() {
        int64_t val = min;
        return val == L_MIN ? 0 : val;
    }

    inline T get_count(const int64_t &offset) {
        return get_counter(offset);
    }

    inline int64_t txn_release(int64_t txn) {
        auto last_min = min.load();
        if (txn < last_min) {
            return -last_min - 1;
        }
        auto countAfter = get_counter(txn).fetch_sub(1) - 1;
        if (countAfter == 0 && last_min == txn) {
            update_min(max);
        }
        return countAfter;
    }

    // txn must be > 0
    inline int64_t txn_acquire(int64_t txn) {
        int64_t current_min = min.load();
        if (current_min == L_MIN) {
            if (min.compare_exchange_strong(current_min, txn)) {
                current_min = txn;
            }
        }
        if (txn < current_min) {
            return -1;
        }

        if (txn - current_min >= size) {
            while (txn - current_min >= size) {
                // We need to move min closer to txn
                // Updating min directly will create a race condition
                // instead move min size by size
                auto dummy_txn = current_min + size - 1;
                if (increment_count(dummy_txn)) {
                    current_min = update_min(txn);
                    // release dummy txn
                    get_counter(dummy_txn)--;
                } else {
                    // Someone else pushed max, check if the updated min is better than current one
                    current_min = calculate_min(dummy_txn);
                }

                if (current_min != dummy_txn && txn - current_min >= size) {
                    // No luck to move min any farther
                    return -current_min - 2;
                };
            }
            // After pushing min as far as possible, take the clean value for next steps
            current_min = min.load();
        }

        if (txn - current_min < size) {
            if (!increment_count(txn)) {
                // Race lost, someone updated max to higher value
                return -1;
            }
            update_min(txn);
            return 0;
        }
        return -current_min - 2;
    }

    void init(uint32_t entry_count) {
        mask = entry_count - 1;
        size = entry_count;
        int64_t expected = 0;
        // Since txn values are guaranteed to be greater than 0, min can be 0 only on
        // a newly created scoreboard. So, this CAS should only succeed single time.
        min.compare_exchange_strong(expected, L_MIN);
    }

    bool is_range_available(int64_t from, int64_t to) {
        if (to >= min && from <= max) {
            for (int64_t txn = from; txn < to; txn++) {
                if (get_count(txn) > 0) {
                    return false;
                }
            }
        }
        return true;
    }
};

extern "C" {

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_acquireTxn0
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    return reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->txn_acquire(txn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_releaseTxn0
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    return reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->txn_release(txn);
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
    return (jlong) sizeof(txn_scoreboard_t<COUNTER_T>) + entryCount * (jlong) sizeof(std::atomic<COUNTER_T>);
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_TxnScoreboard_init
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong entryCount) {
    reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->init(entryCount);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_cairo_TxnScoreboard_isRangeAvailable0
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong from, jlong to) {
    return reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->is_range_available(from, to);
}

}
