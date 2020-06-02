#include <cmath>
#include <algorithm>

#include "vectkeysum_vanilla.h"
#include "rosti.h"

extern "C" {

// SUM double

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                            jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<int32_t *>(pKeys);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto shift = map->slot_size_shift_;
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        _mm_prefetch(pd + 8, _MM_HINT_T0);
        const int32_t v = pi[i];
        const jdouble d = pd[i];
        auto res = find_or_prepare_insert(map, v);
        auto dest = map->slots_ + (res.first << shift);
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = v;
            if (d == d) {
                *reinterpret_cast<jdouble *>(dest + value_offset) = d;
                *reinterpret_cast<jlong *>(dest + count_offset) = 1;
            }
        } else if (d == d) {
            *reinterpret_cast<jdouble *>(dest + value_offset) += d;
            *reinterpret_cast<jlong *>(dest + count_offset) += 1;
        }
    }
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Rosti_getOffset(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
    return (reinterpret_cast<rosti_t *>(pRosti))->value_offsets_[valueOffset];
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
            auto value = *reinterpret_cast<jdouble *>(src + value_offset);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);

            auto res = find_or_prepare_insert(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + (res.first << shift);
            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
                if (value == value) {
                    *reinterpret_cast<jdouble *>(dest + value_offset) = value;
                    *reinterpret_cast<jlong *>(dest + count_offset) = count;
                }
            } else if (value == value) {
                *reinterpret_cast<jdouble *>(dest + value_offset) += value;
                *reinterpret_cast<jlong *>(dest + count_offset) += count;
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumDoubleSetNull(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    const auto capacity = map->capacity_;
    const auto ctrl = map->ctrl_;
    const auto shift = map->slot_size_shift_;
    const auto slots = map->slots_;
    const jdouble nan = std::numeric_limits<jdouble>::quiet_NaN();

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            const auto src = slots + (i << shift);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);
            if (PREDICT_FALSE(count == 0)) {
                *reinterpret_cast<jdouble *>(src + value_offset) = nan;
            }
        }
    }
}

#define D_MAX std::numeric_limits<jdouble>::max()
#define D_MIN std::numeric_limits<jdouble>::min()

// MIN double

using namespace std;

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                            jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<int32_t *>(pKeys);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto shift = map->slot_size_shift_;
    const auto value_offset = map->value_offsets_[valueOffset];
    printf("value_offset=")
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        _mm_prefetch(pd + 8, _MM_HINT_T0);
        const int32_t v = pi[i];
        const jdouble d = pd[i];
        auto res = find_or_prepare_insert(map, v);
        auto pKey = map->slots_ + (res.first << shift);
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
            auto res = find_or_prepare_insert(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto dest = map_a->slots_ + (res.first << shift);
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
    const auto count_offset = map->value_offsets_[valueOffset + 1];
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
    const auto shift = map->slot_size_shift_;
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        _mm_prefetch(pi + 16, _MM_HINT_T0);
        _mm_prefetch(pd + 8, _MM_HINT_T0);
        const int32_t v = pi[i];
        const jdouble d = pd[i];
        auto res = find_or_prepare_insert(map, v);
        auto pKey = map->slots_ + (res.first << shift);
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
            auto res = find_or_prepare_insert(map_a, key);
            // maps must have identical structure to use "shift" from map B on map A
            auto pKey = map_a->slots_ + (res.first << shift);
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

}
