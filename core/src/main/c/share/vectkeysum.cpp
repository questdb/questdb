#include "vectkeysum_vanilla.h"
#include "rosti.h"

extern "C" {

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
//                auto d = *reinterpret_cast<jdouble *>(dest + value_offset);
                *reinterpret_cast<jdouble *>(dest + value_offset) += value;
                *reinterpret_cast<jlong *>(dest + count_offset) += count;
            }
        }
    }
}

}
