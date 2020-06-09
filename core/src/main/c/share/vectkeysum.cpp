#include <cmath>
#include <algorithm>

using namespace std;

#include "vectkeysum_vanilla.h"
#include "rosti.h"

inline uint64_t hashInt(int32_t v) {
    uint64_t h = v;
    h = (h << 5) - h + ((unsigned char) (v >> 8));
    h = (h << 5) - h + ((unsigned char) (v >> 16));
    h = (h << 5) - h + ((unsigned char) (v >> 24));
    return h;
}

inline bool eqInt(void *p, int32_t key) {
    return *reinterpret_cast<int32_t *>(p) == key;
}

inline uint64_t hashIntMem(void *p) {
    return hashInt(*reinterpret_cast<int32_t *>(p));
}

inline void cpySlot(void *to, void *from, uint64_t size) {
    memcpy(to, from, size);
}

inline std::pair<uint64_t, bool> find(rosti_t *map, const int32_t key) {
    return find_or_prepare_insert<int32_t>(map, key, hashInt, eqInt, hashIntMem, cpySlot);
}

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
Java_io_questdb_std_Rosti_keyedIntSumZero(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys,
                                            jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<int32_t *>(pKeys);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        const int32_t v = pi[i];
        auto res = find(map, v);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = v;
            *reinterpret_cast<jdouble *>(dest + value_offset) = 0;
            *reinterpret_cast<jlong *>(dest + count_offset) = 0;
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
    auto nullKey = reinterpret_cast<int32_t*>(map->slot_initial_values_)[0];
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
Java_io_questdb_std_Rosti_keyedIntKSumDoubleSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
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
Java_io_questdb_std_Rosti_keyedIntNSumDoubleSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
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
        const int32_t v = pi[i];
        const jdouble d = pd[i];
        auto res = find(map, v);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = v;
            *reinterpret_cast<jdouble *>(pVal) = isnan(d) ? D_MAX : d;

        } else {
            const jdouble old = *reinterpret_cast<jdouble *>(pVal);
            *reinterpret_cast<jdouble *>(pVal) = std::min((isnan(d) ? D_MAX : d), old);
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
                *reinterpret_cast<jdouble *>(pVal) = isnan(d) ? D_MAX : d;
            } else {
                const jdouble old = *reinterpret_cast<jdouble *>(pVal);
                *reinterpret_cast<jdouble *>(pVal) = std::min((isnan(d) ? D_MAX : d), old);
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinDoubleSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
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
            *reinterpret_cast<jdouble *>(pVal) = isnan(d) ? D_MIN : d;
        } else {
            const jdouble old = *reinterpret_cast<jdouble *>(pVal);
            *reinterpret_cast<jdouble *>(pVal) = std::max(isnan(d) ? D_MIN : d, old);
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
                *reinterpret_cast<jdouble *>(pVal) = isnan(d) ? D_MIN : d;
            } else {
                const jdouble old = *reinterpret_cast<jdouble *>(pVal);
                *reinterpret_cast<jdouble *>(pVal) = std::max(isnan(d) ? D_MIN : d, old);
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxDoubleSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;

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
Java_io_questdb_std_Rosti_keyedIntAvgDoubleSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
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
            auto pValue = src + value_offset;
            auto d = *reinterpret_cast<jdouble *>(pValue);
            *reinterpret_cast<jdouble *>(pValue) = d / count;
        }
    }
}

// avg long

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntAvgLongSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
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
            auto pValue = src + value_offset;
            auto d = (jdouble) *reinterpret_cast<jlong *>(pValue);
            *reinterpret_cast<jdouble *>(pValue) = d / count;
        }
    }
}

// avg int

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntAvgIntSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
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

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumIntSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
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
Java_io_questdb_std_Rosti_keyedIntMinIntSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
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
Java_io_questdb_std_Rosti_keyedIntSumLongSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
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
Java_io_questdb_std_Rosti_keyedIntMinLongSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
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

}
