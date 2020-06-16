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

#include <cmath>
#include <algorithm>
#include "rosti.h"

extern "C" {

constexpr jdouble D_MAX = std::numeric_limits<jdouble>::infinity();
constexpr jdouble D_MIN = -std::numeric_limits<jdouble>::infinity();
constexpr jint I_MAX = std::numeric_limits<jint>::max();
constexpr jint I_MIN = std::numeric_limits<jint>::min();
constexpr jlong L_MIN = std::numeric_limits<jlong>::min();
constexpr jlong L_MAX = std::numeric_limits<jlong>::max();
constexpr jdouble D_NAN = std::numeric_limits<jdouble>::quiet_NaN();

// SUM double

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                            jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<int32_t *>(pKeys);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        _mm_prefetch(pd + 8, _MM_HINT_T0);
        const int32_t v = pi[i];
        const jdouble d = pd[i];
        auto res = find(map, v);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = v;
            *reinterpret_cast<jdouble *>(dest + value_offset) = std::isnan(d) ? 0 : d;
            *reinterpret_cast<jlong *>(dest + count_offset) = std::isnan(d) ? 0 : 1;
        } else {
            *reinterpret_cast<jdouble *>(dest + value_offset) += std::isnan(d) ? 0 : d;
            *reinterpret_cast<jlong *>(dest + count_offset) += std::isnan(d) ? 0 : 1;
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumDoubleMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                                 jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto count_offset = map_b->value_offsets_[valueOffset + 1];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto d = *reinterpret_cast<jdouble *>(src + value_offset);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);

            auto res = find(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + res.first;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
                *reinterpret_cast<jdouble *>(dest + value_offset) = d;
                *reinterpret_cast<jlong *>(dest + count_offset) = count;
            } else {
                *reinterpret_cast<jdouble *>(dest + value_offset) += d;
                *reinterpret_cast<jlong *>(dest + count_offset) += count;
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumDoubleWrapUp(
        JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset, jdouble valueAtNull, jlong valueAtNullCount) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto src = slots + (i << shift);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);
            if (PREDICT_FALSE(count == 0)) {
                *reinterpret_cast<jdouble *>(src + value_offset) = D_NAN;
            }
        }
    }

    // populate null value
    if (valueAtNullCount > 0) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jdouble *>(dest + value_offset) = valueAtNull;
            *reinterpret_cast<jlong *>(dest + count_offset) = valueAtNullCount;
        } else {
            *reinterpret_cast<jdouble *>(dest + value_offset) += valueAtNull;
            *reinterpret_cast<jlong *>(dest + count_offset) += valueAtNullCount;
        }
    }
}

// KSUM double

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntKSumDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                             jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<int32_t *>(pKeys);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto c_offset = map->value_offsets_[valueOffset + 1];
    const auto count_offset = map->value_offsets_[valueOffset + 2];

    for (int i = 0; i < count; i++) {
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        _mm_prefetch(pd + 8, _MM_HINT_T0);
        const int32_t v = pi[i];
        const jdouble d = pd[i];
        auto res = find(map, v);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = v;
            *reinterpret_cast<jdouble *>(dest + value_offset) = std::isnan(d) ? 0 : d;
            *reinterpret_cast<jdouble *>(dest + c_offset) = 0.;
            *reinterpret_cast<jlong *>(dest + count_offset) = std::isnan(d) ? 0 : 1;
        } else {
            const jdouble c = *reinterpret_cast<jdouble *>(dest + c_offset);
            const jdouble sum = *reinterpret_cast<jdouble *>(dest + value_offset);
            const jdouble y = std::isnan(d) ? 0 : d - c; // y = d -c
            const jdouble t = sum + y;

            *reinterpret_cast<jdouble *>(dest + c_offset) = t - sum - y;
            *reinterpret_cast<jdouble *>(dest + value_offset) = t;
            *reinterpret_cast<jlong *>(dest + count_offset) += std::isnan(d) ? 0 : 1;
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntDistinct(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong count) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<int32_t *>(pKeys);
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        const int32_t v = pi[i];
        auto res = find(map, v);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = v;
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntCount(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong count,
        jint valueOffset) {

        auto map = reinterpret_cast<rosti_t *>(pRosti);
        const auto *pi = reinterpret_cast<int32_t *>(pKeys);
        const auto value_offset = map->value_offsets_[valueOffset];
        for (int i = 0; i < count; i++) {
            _mm_prefetch(pi + 16, _MM_HINT_T0);
            const int32_t v = pi[i];
            auto res = find(map, v);
            auto dest = map->slots_ + res.first;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = v;
                *reinterpret_cast<jlong *>(dest + value_offset) = 1;
            } else {
                (*reinterpret_cast<jlong *>(dest + value_offset))++;
            }
        }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntCountMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                                  jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        if (ctrl[i] > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto count = *reinterpret_cast<jlong *>(src + value_offset);
            auto res = find(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + res.first;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
                *reinterpret_cast<jlong *>(dest + value_offset) = count;
            } else {
                (*reinterpret_cast<jlong *>(dest + value_offset)) += count;
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntKSumDoubleMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                                  jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto c_offset = map_b->value_offsets_[valueOffset + 1];
    const auto count_offset = map_b->value_offsets_[valueOffset + 2];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        if (ctrl[i] > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto d = *reinterpret_cast<jdouble *>(src + value_offset);
            auto cc = *reinterpret_cast<jdouble *>(src + c_offset);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);

            auto res = find(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + res.first;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
                *reinterpret_cast<jdouble *>(dest + value_offset) = d;
                *reinterpret_cast<jdouble *>(dest + c_offset) = cc;
                *reinterpret_cast<jlong *>(dest + count_offset) = count;
            } else {
                // do not check for nans in merge, because we can't have them in map
                const jdouble sum = *reinterpret_cast<jdouble *>(dest + value_offset);
                const jdouble y = d - cc; // y = d -c
                const jdouble t = sum + y;
                *reinterpret_cast<jdouble *>(dest + c_offset) = t - sum - y;
                *reinterpret_cast<jdouble *>(dest + value_offset) = t;
                *reinterpret_cast<jlong *>(dest + count_offset) += count;
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntKSumDoubleWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset, jdouble valueAtNull, jlong valueAtNullCount) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto c_offset = map->value_offsets_[valueOffset + 1];
    const auto count_offset = map->value_offsets_[valueOffset + 2];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

    // populate null value
    if (valueAtNullCount > 0) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jdouble *>(dest + value_offset) = valueAtNull;
            *reinterpret_cast<jlong *>(dest + count_offset) = valueAtNullCount;
        } else {
            const jdouble c = *reinterpret_cast<jdouble *>(dest + c_offset);
            const jdouble sum = *reinterpret_cast<jdouble *>(dest + value_offset);
            // y = d -c
            *reinterpret_cast<jdouble *>(dest + value_offset) = sum + (valueAtNull - c);
            *reinterpret_cast<jlong *>(dest + count_offset) += 1;
        }
    }

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto src = slots + (i << shift);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);
            if (PREDICT_FALSE(count == 0)) {
                *reinterpret_cast<jdouble *>(src + value_offset) = D_NAN;
            }
        }
    }
}

// NSUM double

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntNSumDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                             jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<int32_t *>(pKeys);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto c_offset = map->value_offsets_[valueOffset + 1];
    const auto count_offset = map->value_offsets_[valueOffset + 2];

    for (int i = 0; i < count; i++) {
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        _mm_prefetch(pd + 8, _MM_HINT_T0);
        const int32_t v = pi[i];
        const jdouble d = pd[i];
        auto res = find(map, v);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = v;
            *reinterpret_cast<jdouble *>(dest + value_offset) = std::isnan(d) ? 0 : d;
            *reinterpret_cast<jdouble *>(dest + c_offset) = 0.;
            *reinterpret_cast<jlong *>(dest + count_offset) = std::isnan(d) ? 0 : 1;
        } else {
            const jdouble sum = *reinterpret_cast<jdouble *>(dest + value_offset);
            const jdouble x = std::isnan(d) ? 0 : d;
            const jdouble t = sum + x;
            if (std::abs(sum) >= x) {
                *reinterpret_cast<jdouble *>(dest + c_offset) += (sum - t) + x;
            } else {
                *reinterpret_cast<jdouble *>(dest + c_offset) += (x - t) + sum;
            }
            *reinterpret_cast<jdouble *>(dest + value_offset) = t;
            *reinterpret_cast<jlong *>(dest + count_offset) += std::isnan(d) ? 0 : 1;
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntNSumDoubleMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                                  jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto c_offset = map_b->value_offsets_[valueOffset + 1];
    const auto count_offset = map_b->value_offsets_[valueOffset + 2];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        if (ctrl[i] > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto d = *reinterpret_cast<jdouble *>(src + value_offset);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);

            auto res = find(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + res.first;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
                *reinterpret_cast<jdouble *>(dest + value_offset) = d;
                *reinterpret_cast<jdouble *>(dest + c_offset) = *reinterpret_cast<jdouble *>(src + c_offset);
                *reinterpret_cast<jlong *>(dest + count_offset) = count;
            } else {
                // do not check for nans in merge, because we can't have them in map
                const jdouble sum = *reinterpret_cast<jdouble *>(dest + value_offset);
                const jdouble t = sum + d;
                if (std::abs(sum) >= d) {
                    *reinterpret_cast<jdouble *>(dest + c_offset) += (sum - t) + d;
                } else {
                    *reinterpret_cast<jdouble *>(dest + c_offset) += (d - t) + sum;
                }
                *reinterpret_cast<jdouble *>(dest + value_offset) = t;
                *reinterpret_cast<jlong *>(dest + count_offset) += count;
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntNSumDoubleWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
        jdouble valueAtNull, jlong valueAtNullCount, jdouble valueAtNullC) {

    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto c_offset = map->value_offsets_[valueOffset + 1];
    const auto count_offset = map->value_offsets_[valueOffset + 2];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto src = slots + (i << shift);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);
            if (PREDICT_FALSE(count == 0)) {
                *reinterpret_cast<jdouble *>(src + value_offset) = D_NAN;
            } else {
                *reinterpret_cast<jdouble *>(src + value_offset) += *reinterpret_cast<jdouble *>(src + c_offset);
            }
        }
    }

    // populate null value
    if (valueAtNullCount > 0) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jdouble *>(dest + value_offset) = valueAtNull;
            *reinterpret_cast<jlong *>(dest + c_offset) = valueAtNullC;
            *reinterpret_cast<jlong *>(dest + count_offset) = valueAtNullCount;
        } else {
            const jdouble sum = *reinterpret_cast<jdouble *>(dest + value_offset);
            const jdouble t = sum + valueAtNull;
            if (std::abs(sum) >= valueAtNull) {
                *reinterpret_cast<jdouble *>(dest + c_offset) += (sum - t) + valueAtNull;
            } else {
                *reinterpret_cast<jdouble *>(dest + c_offset) += (valueAtNull - t) + sum;
            }
            *reinterpret_cast<jdouble *>(dest + value_offset) = t;
            *reinterpret_cast<jlong *>(dest + count_offset) += 1;
        }
    }

}

// MIN double

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                            jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<int32_t *>(pKeys);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        _mm_prefetch(pd + 8, _MM_HINT_T0);
        const int32_t key = pi[i];
        const jdouble d = pd[i];
        auto res = find(map, key);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = key;
            *reinterpret_cast<jdouble *>(pVal) = std::isnan(d) ? D_MAX : d;
        } else {
            const jdouble old = *reinterpret_cast<jdouble *>(pVal);
            *reinterpret_cast<jdouble *>(pVal) = std::min((std::isnan(d) ? D_MAX : d), old);
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinDoubleMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                                 jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto d = *reinterpret_cast<jdouble *>(src + value_offset);
            auto res = find(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + res.first;
            auto pVal = dest + value_offset;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
                *reinterpret_cast<jdouble *>(pVal) = std::isnan(d) ? D_MAX : d;
            } else {
                const jdouble old = *reinterpret_cast<jdouble *>(pVal);
                *reinterpret_cast<jdouble *>(pVal) = std::min((std::isnan(d) ? D_MAX : d), old);
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinDoubleWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                  jdouble valueAtNull) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

    // populate null value only if non-keyed aggregation did something useful
    if (valueAtNull < D_MAX) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jdouble *>(dest + value_offset) = valueAtNull;
        } else {
            *reinterpret_cast<jdouble *>(dest + value_offset) = std::min(valueAtNull,
                                                                         *reinterpret_cast<jdouble *>(dest +
                                                                                                      value_offset));
        }
    }

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto pVal = slots + (i << shift) + value_offset;
            auto value = *reinterpret_cast<jdouble *>(pVal);
            if (PREDICT_FALSE(value == D_MAX)) {
                *reinterpret_cast<jdouble *>(pVal) = std::numeric_limits<jdouble>::quiet_NaN();
            }
        }
    }
}

// MAX double

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                            jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<int32_t *>(pKeys);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        _mm_prefetch(pd + 8, _MM_HINT_T0);
        const int32_t v = pi[i];
        const jdouble d = pd[i];
        auto res = find(map, v);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = v;
            *reinterpret_cast<jdouble *>(pVal) = std::isnan(d) ? D_MIN : d;
        } else {
            const jdouble old = *reinterpret_cast<jdouble *>(pVal);
            *reinterpret_cast<jdouble *>(pVal) = std::max(std::isnan(d) ? D_MIN : d, old);
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxDoubleMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                                 jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto d = *reinterpret_cast<jdouble *>(src + value_offset);
            auto res = find(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto pKey = map_a->slots_ + res.first;
            auto pVal = pKey + value_offset;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(pKey) = key;
                *reinterpret_cast<jdouble *>(pVal) = std::isnan(d) ? D_MIN : d;
            } else {
                const jdouble old = *reinterpret_cast<jdouble *>(pVal);
                *reinterpret_cast<jdouble *>(pVal) = std::max(std::isnan(d) ? D_MIN : d, old);
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxDoubleWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset, jdouble valueAtNull) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

    if (valueAtNull < D_MIN) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jdouble *>(dest + value_offset) = valueAtNull;
        } else {
            *reinterpret_cast<jdouble *>(dest + value_offset) = std::max(valueAtNull,
                                                                         *reinterpret_cast<jdouble *>(dest +
                                                                                                      value_offset));
        }
    }

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto pVal = (slots + (i << shift)) + value_offset;
            auto value = *reinterpret_cast<jdouble *>(pVal);
            if (PREDICT_FALSE(value == D_MIN)) {
                *reinterpret_cast<jdouble *>(pVal) = std::numeric_limits<jdouble>::quiet_NaN();
            }
        }
    }
}

// avg double

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntAvgDoubleWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset, jdouble valueAtNull, jlong valueAtNullCount) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

    // populate null value
    if (valueAtNullCount > 0) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jdouble *>(dest + value_offset) = valueAtNull;
            *reinterpret_cast<jlong *>(dest + count_offset) = valueAtNullCount;
        } else {
            *reinterpret_cast<jdouble *>(dest + value_offset) += valueAtNull;
            *reinterpret_cast<jlong *>(dest + count_offset) += valueAtNullCount;
        }
    }

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto src = slots + (i << shift);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);
            auto pValue = src + value_offset;
            auto d = *reinterpret_cast<jdouble *>(pValue);
            *reinterpret_cast<jdouble *>(pValue) = d / count;
        }
    }
}

// avg int and long

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntAvgLongWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
        jdouble valueAtNull, jlong valueAtNullCount) {

    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

    // populate null value
    if (valueAtNullCount > 0) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jdouble *>(dest + value_offset) = valueAtNull;
            *reinterpret_cast<jlong *>(dest + count_offset) = valueAtNullCount;
        } else {
            *reinterpret_cast<jdouble *>(dest + value_offset) += valueAtNull;
            *reinterpret_cast<jlong *>(dest + count_offset) += valueAtNullCount;
        }
    }

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto src = slots + (i << shift);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);
            auto pValue = src + value_offset;
            auto d = (jdouble) *reinterpret_cast<jlong *>(pValue);
            *reinterpret_cast<jdouble *>(pValue) = d / count;
        }
    }
}

// SUM int

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumInt(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pInt,
                                         jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pk = reinterpret_cast<int32_t *>(pKeys);
    const auto *pi = reinterpret_cast<jint *>(pInt);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pk + 16, _MM_HINT_T0);
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        const int32_t key = pk[i];
        const jint val = pi[i];
        auto res = find(map, key);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = key;
            if (PREDICT_FALSE(val == I_MIN)) {
                *reinterpret_cast<jlong *>(dest + value_offset) = 0;
                *reinterpret_cast<jlong *>(dest + count_offset) = 0;
            } else {
                *reinterpret_cast<jlong *>(dest + value_offset) = val;
                *reinterpret_cast<jlong *>(dest + count_offset) = 1;
            }
        } else if (PREDICT_TRUE(val > I_MIN)) {
            *reinterpret_cast<jlong *>(dest + value_offset) += val;
            *reinterpret_cast<jlong *>(dest + count_offset) += 1;
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumIntMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                              jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto count_offset = map_b->value_offsets_[valueOffset + 1];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto val = *reinterpret_cast<jint *>(src + value_offset);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);

            auto res = find(map_a, key);
            auto dest = map_a->slots_ + res.first;

            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
            }

            // when maps have non-null values, their count is >0 and val is not MIN
            // on other hand
            const jlong old_count = *reinterpret_cast<jlong *>(dest + count_offset);
            if (old_count > 0 && count > 0) {
                *reinterpret_cast<jlong *>(dest + value_offset) += val;
                *reinterpret_cast<jlong *>(dest + count_offset) += count;
            } else {
                *reinterpret_cast<jlong *>(dest + value_offset) = val;
                *reinterpret_cast<jlong *>(dest + count_offset) = count;
            }
        }
    }
}

// MIN int

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinInt(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pInt,
                                         jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pk = reinterpret_cast<int32_t *>(pKeys);
    const auto *pi = reinterpret_cast<jint *>(pInt);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pk + 16, _MM_HINT_T0);
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        const int32_t v = pk[i];
        const jint val = pi[i];
        auto res = find(map, v);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = v;
            if (val != I_MIN) {
                *reinterpret_cast<jint *>(pVal) = val;
            }
        } else if (val != I_MIN) {
            const jint old = *reinterpret_cast<jint *>(pVal);
            *reinterpret_cast<jint *>(pVal) = std::min(val, old);
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinIntMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                              jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto val = *reinterpret_cast<jint *>(src + value_offset);
            auto res = find(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + res.first;
            auto pVal = dest + value_offset;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
                *reinterpret_cast<jint *>(pVal) = val == I_MIN ? I_MAX : val;
            } else {
                if (val != I_MIN) {
                    const jint old = *reinterpret_cast<jint *>(pVal);
                    *reinterpret_cast<jint *>(pVal) = std::min(val, old);
                }
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinIntWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset, jint valueAtNull) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto pVal = slots + (i << shift) + value_offset;
            auto value = *reinterpret_cast<jint *>(pVal);
            if (PREDICT_FALSE(value == I_MAX)) {
                *reinterpret_cast<jint *>(pVal) = I_MIN;
            }
        }
    }

    if (valueAtNull < I_MAX) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jint *>(dest + value_offset) = valueAtNull;
        } else {
            *reinterpret_cast<jint *>(dest + value_offset) = std::min(valueAtNull, *reinterpret_cast<jint *>(dest + value_offset));
        }
    }
}

// MAX int

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxInt(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pInt,
                                         jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pk = reinterpret_cast<int32_t *>(pKeys);
    const auto *pi = reinterpret_cast<jint *>(pInt);
    const auto shift = map->slot_size_shift_;
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pk + 16, _MM_HINT_T0);
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        const int32_t v = pk[i];
        const jint val = pi[i];
        auto res = find(map, v);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = v;
            *reinterpret_cast<jint *>(pVal) = val;
        } else {
            const jint old = *reinterpret_cast<jint *>(pVal);
            *reinterpret_cast<jint *>(pVal) = std::max(val, old);
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxIntMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                              jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto val = *reinterpret_cast<jint *>(src + value_offset);
            auto res = find(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + res.first;
            auto pVal = dest + value_offset;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
                *reinterpret_cast<jint *>(pVal) = val;
            } else {
                const jint old = *reinterpret_cast<jint *>(pVal);
                *reinterpret_cast<jint *>(pVal) = std::max(val, old);
            }
        }
    }
}

// SUM long

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                          jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pk = reinterpret_cast<int32_t *>(pKeys);
    const auto *pl = reinterpret_cast<jlong *>(pLong);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pk + 16, _MM_HINT_T0);
        _mm_prefetch(pl + 8, _MM_HINT_T0);
        const int32_t v = pk[i];
        const jlong val = pl[i];
        auto res = find(map, v);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = v;
            if (PREDICT_FALSE(val == I_MIN)) {
                *reinterpret_cast<jlong *>(dest + value_offset) = 0;
                *reinterpret_cast<jlong *>(dest + count_offset) = 0;
            } else {
                *reinterpret_cast<jlong *>(dest + value_offset) = val;
                *reinterpret_cast<jlong *>(dest + count_offset) = 1;
            }
        } else {
            if (PREDICT_TRUE(val > I_MIN)) {
                *reinterpret_cast<jlong *>(dest + value_offset) += val;
                *reinterpret_cast<jlong *>(dest + count_offset) += 1;
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLongMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                               jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto count_offset = map_b->value_offsets_[valueOffset + 1];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto val = *reinterpret_cast<jlong *>(src + value_offset);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);

            auto res = find(map_a, key);
            auto dest = map_a->slots_ + res.first;

            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
            }

            // when maps have non-null values, their count is >0 and val is not MIN
            // on other hand
            const jlong old_count = *reinterpret_cast<jlong *>(dest + count_offset);
            if (old_count > 0 && count > 0) {
                *reinterpret_cast<jlong *>(dest + value_offset) += val;
                *reinterpret_cast<jlong *>(dest + count_offset) += count;
            } else {
                *reinterpret_cast<jlong *>(dest + value_offset) = val;
                *reinterpret_cast<jlong *>(dest + count_offset) = count;
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLongWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
        jlong valueAtNull, jlong valueAtNullCount) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto src = slots + (i << shift);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);
            if (PREDICT_FALSE(count == 0)) {
                *reinterpret_cast<jlong *>(src + value_offset) = L_MIN;
            }
        }
    }

    // populate null value
    if (valueAtNullCount > 0) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jlong *>(dest + value_offset) = valueAtNull;
            *reinterpret_cast<jlong *>(dest + count_offset) = valueAtNullCount;
        } else {
            *reinterpret_cast<jlong *>(dest + value_offset) += valueAtNull;
            *reinterpret_cast<jlong *>(dest + count_offset) += valueAtNullCount;
        }
    }
}

// MIN long

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                          jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pk = reinterpret_cast<int32_t *>(pKeys);
    const auto *pi = reinterpret_cast<jlong *>(pLong);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pk + 16, _MM_HINT_T0);
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        const int32_t key = pk[i];
        const jlong val = pi[i];
        auto res = find(map, key);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = key;
            if (val != L_MIN) {
                *reinterpret_cast<jlong *>(pVal) = val;
            }
        } else if (val != L_MIN) {
            const jlong old = *reinterpret_cast<jlong *>(pVal);
            *reinterpret_cast<jlong *>(pVal) = std::min(val, old);
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinLongMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                               jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto val = *reinterpret_cast<jlong *>(src + value_offset);
            auto res = find(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + res.first;
            auto pVal = dest + value_offset;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
                *reinterpret_cast<jlong *>(pVal) = val == L_MIN ? L_MAX : val;
            } else {
                if (val != L_MIN) {
                    const jlong old = *reinterpret_cast<jlong *>(pVal);
                    *reinterpret_cast<jlong *>(pVal) = std::min(val, old);
                }
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinLongWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset, jlong valueAtNull) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto pVal = slots + (i << shift) + value_offset;
            auto value = *reinterpret_cast<jlong *>(pVal);
            if (PREDICT_FALSE(value == L_MAX)) {
                *reinterpret_cast<jlong *>(pVal) = L_MIN;
            }
        }
    }

    // populate null value
    if (valueAtNull < L_MAX) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jlong *>(dest + value_offset) = valueAtNull;
        } else {
            *reinterpret_cast<jlong *>(dest + value_offset) = std::min(valueAtNull, *reinterpret_cast<jlong *>(dest + value_offset));
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxLongWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset, jlong valueAtNull) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];

    // populate null value
    if (valueAtNull > L_MIN) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jlong *>(dest + value_offset) = valueAtNull;
        } else {
            *reinterpret_cast<jlong *>(dest + value_offset) = std::max(valueAtNull, *reinterpret_cast<jlong *>(dest + value_offset));
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxIntWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset, jint valueAtNull) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];

    // populate null value
    if (valueAtNull > I_MIN) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            *reinterpret_cast<jint *>(dest + value_offset) = valueAtNull;
        } else {
            *reinterpret_cast<jint *>(dest + value_offset) = std::max(valueAtNull, *reinterpret_cast<jint *>(dest + value_offset));
        }
    }
}

// MAX long

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                          jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pk = reinterpret_cast<int32_t *>(pKeys);
    const auto *pl = reinterpret_cast<jlong *>(pLong);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pk + 16, _MM_HINT_T0);
        _mm_prefetch(pl + 8, _MM_HINT_T0);
        const int32_t v = pk[i];
        const jlong val = pl[i];
        auto res = find(map, v);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = v;
            *reinterpret_cast<jlong *>(pVal) = val;
        } else {
            const jlong old = *reinterpret_cast<jlong *>(pVal);
            *reinterpret_cast<jlong *>(pVal) = std::max(val, old);
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxLongMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                               jint valueOffset) {
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto val = *reinterpret_cast<jlong *>(src + value_offset);
            auto res = find(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + res.first;
            auto pVal = dest + value_offset;
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
                *reinterpret_cast<jlong *>(pVal) = val;
            } else {
                const jlong old = *reinterpret_cast<jlong *>(pVal);
                *reinterpret_cast<jlong *>(pVal) = std::max(val, old);
            }
        }
    }
}

// ---------------------

#define HOUR_MICROS  3600000000L
#define DAY_HOURS  24

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourCount(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong count,
                                        jint valueOffset) {

    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *p_micros = reinterpret_cast<int64_t *>(pKeys);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(p_micros + 16, _MM_HINT_T0);
        auto micro = p_micros[i];
        int32_t hour;
        if (PREDICT_TRUE(micro > -1)) {
            hour = ((micro / HOUR_MICROS) % DAY_HOURS);
        } else {
            hour = DAY_HOURS - 1 + (((micro + 1) / HOUR_MICROS) % DAY_HOURS);
        }
        auto res = find(map, hour);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = hour;
            *reinterpret_cast<jlong *>(dest + value_offset) = 1;
        } else {
            (*reinterpret_cast<jlong *>(dest + value_offset))++;
        }
    }
}

}
