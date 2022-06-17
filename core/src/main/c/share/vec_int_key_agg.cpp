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

#include "rosti.h"
#include <functional>

#define HOUR_MICROS  3600000000L
#define DAY_HOURS  24

struct long128_t {
    // little-endian layout
    uint64_t lo;
    int64_t hi;

    long128_t() = default;

    constexpr long128_t(int64_t high, uint64_t low) : lo(low), hi(high) {}
    constexpr long128_t(int64_t v) : lo{static_cast<uint64_t>(v)}, hi{v < 0 ? ~int64_t{0} : 0} {}

    long128_t& operator=(int64_t v);
    constexpr long128_t operator-(long128_t v) const;

    explicit operator double() const;

    long128_t& operator+=(long128_t other);
};

inline long128_t& long128_t::operator=(int64_t v) { return *this = long128_t(v); }

constexpr long128_t operator+(long128_t lhs, long128_t rhs);

inline long128_t& long128_t::operator+=(long128_t other) {
    *this = *this + other;
    return *this;
}

constexpr long128_t signed_add_carry(const long128_t& result, const long128_t& lhs) {
    return (result.lo < lhs.lo) ? long128_t(result.hi + 1, result.lo) : result;
}

constexpr long128_t operator+(long128_t lhs, long128_t rhs) {
    return signed_add_carry(long128_t(lhs.hi + rhs.hi, lhs.lo + rhs.lo), lhs);
}

inline constexpr double cast_positive(long128_t v) {
    return  static_cast<double>(v.lo) + ldexp(static_cast<double>(v.hi), 64);
}

long128_t::operator double() const {
    // if negative and not minimal - cast absolute value
    if (hi < 0 && hi != std::numeric_limits<int64_t>::min() && lo != 0) {
        long128_t abs = operator-(*this);
        return -cast_positive(abs);
    } else {
        return cast_positive(*this);
    }
}

constexpr long128_t long128_t::operator-(long128_t v) const {
    return {~v.hi + (v.lo == 0), ~v.lo + 1};
}
struct long256_t {
    uint64_t l0;
    uint64_t l1;
    uint64_t l2;
    uint64_t l3;

    long256_t(uint64_t v0, uint64_t v1, uint64_t v2, uint64_t v3)
            : l0(v0), l1(v1), l2(v2), l3(v3)
    {
    }

    bool is_null() const {
        return l0 == L_MIN && l1 == L_MIN && l2 == L_MIN && l3 == L_MIN;
    }

    void operator+=(const long256_t& rhs) {
        if (rhs.is_null()) {
            this->l0 = L_MIN;
            this->l1 = L_MIN;
            this->l2 = L_MIN;
            this->l3 = L_MIN;
        } else {
            // The sum will overflow if both top bits are set (x & y) or if one of them
            // is (x | y), and a carry from the lower place happened. If such a carry
            // happens, the top bit will be 1 + 0 + 1 = 0 (& ~sum).
            uint64_t carry = 0;
            uint64_t l0_ = this->l0 + rhs.l0 + carry;
            carry = ((this->l0 & rhs.l0) | ((this->l0 | rhs.l0) & ~l0_)) >> 63;

            uint64_t l1_ = this->l1 + rhs.l1 + carry;
            carry = ((this->l1 & rhs.l1) | ((this->l1 | rhs.l1) & ~l1_)) >> 63;

            uint64_t l2_ = this->l2 + rhs.l2 + carry;
            carry = ((this->l2 & rhs.l2) | ((this->l2 | rhs.l2) & ~l2_)) >> 63;

            uint64_t l3_ = this->l3 + rhs.l3 + carry;
            //carry = ((this->l3 & rhs.l3) | ((this->l3 | rhs.l3) & ~l3_)) >> 63;

            this->l0 = l0_;
            this->l1 = l1_;
            this->l2 = l2_;
            this->l3 = l3_;
        }
    }
};

//__SIZEOF_INT128__ is defined by Clang and GCC when __int128 is supported
//#if defined(__SIZEOF_INT128__)
//typedef __int128 accumulator_t;
//#else
//typedef jlong accumulator_t;
//#endif

typedef long128_t accumulator_t;

typedef int32_t (*to_int_fn)(jlong, int);

inline int32_t int64_to_hour(jlong ptr, int i) {
    const auto p = reinterpret_cast<int64_t *>(ptr);
    MM_PREFETCH_T0(p + i + 64);
    const auto micro = p[i];
    if (PREDICT_TRUE(micro > -1)) {
        return ((micro / HOUR_MICROS) % DAY_HOURS);
    } else {
        return DAY_HOURS - 1 + (((micro + 1) / HOUR_MICROS) % DAY_HOURS);
    }
}

inline int32_t to_int(jlong ptr, int i) {
    const auto p = reinterpret_cast<int32_t *>(ptr);
    MM_PREFETCH_T0(p + i + 64);
    return p[i];
}

static void kIntMaxInt(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pInt, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<jint *>(pInt);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pi + i + 16);
        const int32_t key = to_int(pKeys, i);
        const jint val = pi[i];
        auto res = find(map, key);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = key;
            *reinterpret_cast<jint *>(pVal) = val;
        } else {
            const jint old = *reinterpret_cast<jint *>(pVal);
            *reinterpret_cast<jint *>(pVal) = MAX(val, old);
        }
    }
}

static void kIntMaxLong(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pLong, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pl = reinterpret_cast<jlong *>(pLong);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pl + i + 8);
        const int32_t key = to_int(pKeys, i);
        const jlong val = pl[i];
        auto res = find(map, key);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = key;
            *reinterpret_cast<jlong *>(pVal) = val;
        } else {
            const jlong old = *reinterpret_cast<jlong *>(pVal);
            *reinterpret_cast<jlong *>(pVal) = MAX(val, old);
        }
    }
}

static void kIntMaxDouble(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pDouble, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pd + i + 8);
        const int32_t key = to_int(pKeys, i);
        const jdouble d = pd[i];
        auto res = find(map, key);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = key;
            *reinterpret_cast<jdouble *>(pVal) = std::isnan(d) ? D_MIN : d;
        } else {
            const jdouble old = *reinterpret_cast<jdouble *>(pVal);
            *reinterpret_cast<jdouble *>(pVal) = MAX(std::isnan(d) ? D_MIN : d, old);
        }
    }
}

static void kIntMinInt(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pInt, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<jint *>(pInt);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pi + i + 16);
        const int32_t key = to_int(pKeys, i);
        const jint val = pi[i];
        auto res = find(map, key);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = key;
            if (val != I_MIN) {
                *reinterpret_cast<jint *>(pVal) = val;
            }
        } else if (val != I_MIN) {
            const jint old = *reinterpret_cast<jint *>(pVal);
            *reinterpret_cast<jint *>(pVal) = MIN(val, old);
        }
    }
}

static void kIntMinLong(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pLong, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<jlong *>(pLong);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pi + i + 16);
        const int32_t key = to_int(pKeys, i);
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
            *reinterpret_cast<jlong *>(pVal) = MIN(val, old);
        }
    }
}

static void kIntMinDouble(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pDouble, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pd + i + 8);
        const int32_t key = to_int(pKeys, i);
        const jdouble d = pd[i];
        auto res = find(map, key);
        auto pKey = map->slots_ + res.first;
        auto pVal = pKey + value_offset;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(pKey) = key;
            *reinterpret_cast<jdouble *>(pVal) = std::isnan(d) ? D_MAX : d;
        } else {
            const jdouble old = *reinterpret_cast<jdouble *>(pVal);
            *reinterpret_cast<jdouble *>(pVal) = MIN((std::isnan(d) ? D_MAX : d), old);
        }
    }
}

template <typename T>
static void kIntSumLong(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pLong, jlong count, jint valueOffset) {
    constexpr auto count_idx = sizeof(T) == 8 ? 1 : 2;
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pl = reinterpret_cast<jlong *>(pLong);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + count_idx];
    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pl + i + 8);
        const int32_t key = to_int(pKeys, i);
        const jlong val = pl[i];
        auto res = find(map, key);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = key;
            if (PREDICT_FALSE(val == L_MIN)) {
                *reinterpret_cast<T *>(dest + value_offset) = 0;
                *reinterpret_cast<jlong *>(dest + count_offset) = 0;
            } else {
                *reinterpret_cast<T *>(dest + value_offset) = val;
                *reinterpret_cast<jlong *>(dest + count_offset) = 1;
            }
        } else {
            if (PREDICT_TRUE(val > L_MIN)) {
                *reinterpret_cast<T *>(dest + value_offset) += val;
                *reinterpret_cast<jlong *>(dest + count_offset) += 1;
            }
        }
    }
}

static void kIntSumLong256(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pLong, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);

    const auto *pl = reinterpret_cast<long256_t *>(pLong);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pl + i + 8);
        const int32_t key = to_int(pKeys, i);
        const long256_t& val = pl[i];
        auto res = find(map, key);
        auto dest = map->slots_ + res.first;
        long256_t& dst = *reinterpret_cast<long256_t *>(dest + value_offset);
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = key;
            if (PREDICT_FALSE(val.is_null())) {
                *reinterpret_cast<jlong *>(dest + count_offset) = 0;
                dst = long256_t(0,0,0,0);
            } else {
                dst = val;
                *reinterpret_cast<jlong *>(dest + count_offset) = 1;
            }
        } else {
            if (PREDICT_TRUE(!val.is_null())) {
                dst += val;
                *reinterpret_cast<jlong *>(dest + count_offset) += 1;
            }
        }
    }
}

static void kIntSumLong256Merge(jlong pRostiA, jlong pRostiB, jint valueOffset) {
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
            auto val = *reinterpret_cast<long256_t *>(src + value_offset);
            auto count = *reinterpret_cast<jlong *>(src + count_offset);

            auto res = find(map_a, key);
            auto dest = map_a->slots_ + res.first;

            if (PREDICT_FALSE(res.second)) {
                *reinterpret_cast<int32_t *>(dest) = key;
            }

            // when maps have non-null values, their count is >0 and val is not MIN
            // on other hand
            long256_t& dst = *reinterpret_cast<long256_t *>(dest + value_offset);
            const jlong old_count = *reinterpret_cast<jlong *>(dest + count_offset);
            if (old_count > 0 && count > 0) {
                dst += val;
                *reinterpret_cast<jlong *>(dest + count_offset) += count;
            } else {
                *reinterpret_cast<long256_t *>(dest + value_offset) = val;
                *reinterpret_cast<jlong *>(dest + count_offset) = count;
            }
        }
    }
}

static void kIntSumLong256WrapUp(jlong pRosti, jint valueOffset, jlong n0, jlong n1, jlong n2, jlong n3, jlong valueAtNullCount) {
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
            long256_t& srcv = *reinterpret_cast<long256_t *>(src + value_offset);
            if (PREDICT_FALSE(count == 0)) {
                srcv = long256_t(L_MIN, L_MIN, L_MIN, L_MIN);
            }
        }
    }

    // populate null value
    if (valueAtNullCount > 0) {
        auto nullKey = reinterpret_cast<int32_t *>(map->slot_initial_values_)[0];
        auto res = find(map, nullKey);
        // maps must have identical structure to use "shift" from map B on map A
        auto dest = map->slots_ + res.first;
        long256_t& dst = *reinterpret_cast<long256_t *>(dest + value_offset);
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = nullKey;
            dst = long256_t(n0, n1, n2, n3);
            *reinterpret_cast<jlong *>(dest + count_offset) = valueAtNullCount;
        } else {
            long256_t valueAtNull(n0, n1, n2, n3);
            dst += valueAtNull;
            *reinterpret_cast<jlong *>(dest + count_offset) += valueAtNullCount;
        }
    }
}

static void kIntNSumDouble(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pDouble, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto c_offset = map->value_offsets_[valueOffset + 1];
    const auto count_offset = map->value_offsets_[valueOffset + 2];

    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pd + i + 8);
        const int32_t key = to_int(pKeys, i);
        const jdouble d = pd[i];
        auto res = find(map, key);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = key;
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

static void kIntSumInt(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pInt, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pi = reinterpret_cast<jint *>(pInt);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pi + i + 16);
        const int32_t key = to_int(pKeys, i);
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

static void kIntDistinct(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong count) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    for (int i = 0; i < count; i++) {
        const int32_t key = to_int(pKeys, i);
        auto res = find(map, key);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = key;
        }
    }
}

static void kIntSumDouble(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pDouble, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + 1];
    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pd + i + 8);
        const int32_t key = to_int(pKeys, i);
        const jdouble d = pd[i];
        auto res = find(map, key);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = key;
            *reinterpret_cast<jdouble *>(dest + value_offset) = std::isnan(d) ? 0 : d;
            *reinterpret_cast<jlong *>(dest + count_offset) = std::isnan(d) ? 0 : 1;
        } else {
            *reinterpret_cast<jdouble *>(dest + value_offset) += std::isnan(d) ? 0 : d;
            *reinterpret_cast<jlong *>(dest + count_offset) += std::isnan(d) ? 0 : 1;
        }
    }
}

static void kIntKSumDouble(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong pDouble, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto *pd = reinterpret_cast<jdouble *>(pDouble);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto c_offset = map->value_offsets_[valueOffset + 1];
    const auto count_offset = map->value_offsets_[valueOffset + 2];

    for (int i = 0; i < count; i++) {
        MM_PREFETCH_T0(pd + i + 8);
        const int32_t key = to_int(pKeys, i);
        const jdouble d = pd[i];
        auto res = find(map, key);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = key;
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

static void kIntCount(to_int_fn to_int, jlong pRosti, jlong pKeys, jlong count, jint valueOffset) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    for (int i = 0; i < count; i++) {
        auto const key = to_int(pKeys, i);
        auto res = find(map, key);
        auto dest = map->slots_ + res.first;
        if (PREDICT_FALSE(res.second)) {
            *reinterpret_cast<int32_t *>(dest) = key;
            *reinterpret_cast<jlong *>(dest + value_offset) = 1;
        } else {
            (*reinterpret_cast<jlong *>(dest + value_offset))++;
        }
    }
}

template<typename T>
static void kIntSumLongMerge(jlong pRostiA, jlong pRostiB, jint valueOffset) {
    constexpr auto count_idx = sizeof(T) == 8 ? 1 : 2;
    auto map_a = reinterpret_cast<rosti_t *>(pRostiA);
    auto map_b = reinterpret_cast<rosti_t *>(pRostiB);
    const auto value_offset = map_b->value_offsets_[valueOffset];
    const auto count_offset = map_b->value_offsets_[valueOffset + count_idx];
    const auto capacity = map_b->capacity_;
    const auto ctrl = map_b->ctrl_;
    const auto shift = map_b->slot_size_shift_;
    const auto slots = map_b->slots_;

    for (size_t i = 0; i < capacity; i++) {
        ctrl_t c = ctrl[i];
        if (c > -1) {
            auto src = slots + (i << shift);
            auto key = *reinterpret_cast<int32_t *>(src);
            auto val = *reinterpret_cast<T *>(src + value_offset);
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
                *reinterpret_cast<accumulator_t *>(dest + value_offset) += val;
                *reinterpret_cast<jlong *>(dest + count_offset) += count;
            } else {
                *reinterpret_cast<T *>(dest + value_offset) = val;
                *reinterpret_cast<jlong *>(dest + count_offset) = count;
            }
        }
    }
}

template<typename T>
static void kIntSumLongWrapUp(jlong pRosti, jint valueOffset, jlong valueAtNull, jlong valueAtNullCount) {
    constexpr auto count_idx = sizeof(T) == 8 ? 1 : 2;
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + count_idx];
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

template<typename T>
static void kIntAvgLongWrapUp(jlong pRosti, jint valueOffset, jdouble valueAtNull, jlong valueAtNullCount) {
    constexpr auto count_idx = sizeof(T) == 8 ? 1 : 2;
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    const auto value_offset = map->value_offsets_[valueOffset];
    const auto count_offset = map->value_offsets_[valueOffset + count_idx];
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
            auto sum = *reinterpret_cast<T *>(pValue);
            auto res = static_cast<jdouble>(sum) / count;
            *reinterpret_cast<jdouble *>(pValue) = res;//todo: segfault??
        }
    }
}

extern "C" {

// SUM double

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                            jlong count, jint valueOffset) {
    kIntSumDouble(to_int, pRosti, pKeys, pDouble, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourSumDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                             jlong count, jint valueOffset) {
    kIntSumDouble(int64_to_hour, pRosti, pKeys, pDouble, count, valueOffset);
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
Java_io_questdb_std_Rosti_keyedIntSumDoubleWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                  jdouble valueAtNull, jlong valueAtNullCount) {
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
    kIntKSumDouble(to_int, pRosti, pKeys, pDouble, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourKSumDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                              jlong count, jint valueOffset) {
    kIntKSumDouble(int64_to_hour, pRosti, pKeys, pDouble, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntDistinct(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong count) {
    kIntDistinct(to_int, pRosti, pKeys, count);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourDistinct(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong count) {
    kIntDistinct(int64_to_hour, pRosti, pKeys, count);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntCount(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong count,
                                        jint valueOffset) {
    kIntCount(to_int, pRosti, pKeys, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourCount(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong count,
                                         jint valueOffset) {
    kIntCount(int64_to_hour, pRosti, pKeys, count, valueOffset);
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
Java_io_questdb_std_Rosti_keyedIntKSumDoubleWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                   jdouble valueAtNull, jlong valueAtNullCount) {
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
    kIntNSumDouble(to_int, pRosti, pKeys, pDouble, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourNSumDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                              jlong count, jint valueOffset) {
    kIntNSumDouble(int64_to_hour, pRosti, pKeys, pDouble, count, valueOffset);
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
    kIntMinDouble(to_int, pRosti, pKeys, pDouble, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourMinDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                             jlong count, jint valueOffset) {
    kIntMinDouble(int64_to_hour, pRosti, pKeys, pDouble, count, valueOffset);
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
                *reinterpret_cast<jdouble *>(pVal) = MIN((std::isnan(d) ? D_MAX : d), old);
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
            *reinterpret_cast<jdouble *>(dest + value_offset) = MIN(valueAtNull,
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
    kIntMaxDouble(to_int, pRosti, pKeys, pDouble, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourMaxDouble(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pDouble,
                                             jlong count, jint valueOffset) {
    kIntMaxDouble(int64_to_hour, pRosti, pKeys, pDouble, count, valueOffset);
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
                *reinterpret_cast<jdouble *>(pVal) = MAX(std::isnan(d) ? D_MIN : d, old);
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxDoubleWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                  jdouble valueAtNull) {
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
            *reinterpret_cast<jdouble *>(dest + value_offset) = MAX(valueAtNull,
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
Java_io_questdb_std_Rosti_keyedIntAvgDoubleWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
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
            auto d = *reinterpret_cast<jdouble *>(pValue);
            *reinterpret_cast<jdouble *>(pValue) = d / count;
        }
    }
}

// avg int and long

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntAvgLongWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                jdouble valueAtNull, jlong valueAtNullCount) {

    kIntAvgLongWrapUp<jlong>(pRosti, valueOffset, valueAtNull, valueAtNullCount);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntAvgLongLongWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                jdouble valueAtNull, jlong valueAtNullCount) {

    kIntAvgLongWrapUp<accumulator_t>(pRosti, valueOffset, valueAtNull, valueAtNullCount);
}

// SUM int

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumInt(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pInt,
                                         jlong count, jint valueOffset) {
    kIntSumInt(to_int, pRosti, pKeys, pInt, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourSumInt(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pInt,
                                          jlong count, jint valueOffset) {
    kIntSumInt(int64_to_hour, pRosti, pKeys, pInt, count, valueOffset);
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
    kIntMinInt(to_int, pRosti, pKeys, pInt, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourMinInt(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pInt,
                                          jlong count, jint valueOffset) {
    kIntMinInt(int64_to_hour, pRosti, pKeys, pInt, count, valueOffset);
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
                    *reinterpret_cast<jint *>(pVal) = MIN(val, old);
                }
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinIntWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                               jint valueAtNull) {
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
            *reinterpret_cast<jint *>(dest + value_offset) = MIN(valueAtNull,
                                                                      *reinterpret_cast<jint *>(dest + value_offset));
        }
    }
}

// MAX int

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxInt(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pInt,
                                         jlong count, jint valueOffset) {
    kIntMaxInt(to_int, pRosti, pKeys, pInt, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourMaxInt(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pInt,
                                          jlong count, jint valueOffset) {
    kIntMaxInt(int64_to_hour, pRosti, pKeys, pInt, count, valueOffset);
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
                *reinterpret_cast<jint *>(pVal) = MAX(val, old);
            }
        }
    }
}

// SUM long

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                          jlong count, jint valueOffset) {
    kIntSumLong<jlong>(to_int, pRosti, pKeys, pLong, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourSumLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                           jlong count, jint valueOffset) {
    kIntSumLong<jlong>(int64_to_hour, pRosti, pKeys, pLong, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLongLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                          jlong count, jint valueOffset) {
    kIntSumLong<accumulator_t>(to_int, pRosti, pKeys, pLong, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourSumLongLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                           jlong count, jint valueOffset) {
    kIntSumLong<accumulator_t>(int64_to_hour, pRosti, pKeys, pLong, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLongMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                               jint valueOffset) {
    kIntSumLongMerge<jlong>(pRostiA, pRostiB, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLongLongMerge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                               jint valueOffset) {
    kIntSumLongMerge<accumulator_t>(pRostiA, pRostiB, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLongWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                jlong valueAtNull, jlong valueAtNullCount) {
    kIntSumLongWrapUp<jlong>(pRosti, valueOffset, valueAtNull, valueAtNullCount);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLongLongWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                jlong valueAtNull, jlong valueAtNullCount) {
    kIntSumLongWrapUp<accumulator_t>(pRosti, valueOffset, valueAtNull, valueAtNullCount);
}
// sum long256
JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourSumLong256(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                           jlong count, jint valueOffset) {
    kIntSumLong256(int64_to_hour, pRosti, pKeys, pLong, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLong256(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                          jlong count, jint valueOffset) {
    kIntSumLong256(to_int, pRosti, pKeys, pLong, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLong256Merge(JNIEnv *env, jclass cl, jlong pRostiA, jlong pRostiB,
                                               jint valueOffset) {
    kIntSumLong256Merge(pRostiA, pRostiB, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntSumLong256WrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                jlong v0, jlong v1, jlong v2, jlong v3, jlong valueAtNullCount) {
    kIntSumLong256WrapUp(pRosti, valueOffset, v0, v1, v2, v3, valueAtNullCount);
}

// MIN long

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                          jlong count, jint valueOffset) {
    kIntMinLong(to_int, pRosti, pKeys, pLong, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourMinLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                           jlong count, jint valueOffset) {
    kIntMinLong(int64_to_hour, pRosti, pKeys, pLong, count, valueOffset);
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
                    *reinterpret_cast<jlong *>(pVal) = MIN(val, old);
                }
            }
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMinLongWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                jlong valueAtNull) {
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
            *reinterpret_cast<jlong *>(dest + value_offset) = MIN(valueAtNull,
                                                                       *reinterpret_cast<jlong *>(dest + value_offset));
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxLongWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                                jlong valueAtNull) {
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
            *reinterpret_cast<jlong *>(dest + value_offset) = MAX(valueAtNull,
                                                                       *reinterpret_cast<jlong *>(dest + value_offset));
        }
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxIntWrapUp(JNIEnv *env, jclass cl, jlong pRosti, jint valueOffset,
                                               jint valueAtNull) {
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
            *reinterpret_cast<jint *>(dest + value_offset) = MAX(valueAtNull,
                                                                      *reinterpret_cast<jint *>(dest + value_offset));
        }
    }
}

// MAX long

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedIntMaxLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                          jlong count, jint valueOffset) {
    kIntMaxLong(to_int, pRosti, pKeys, pLong, count, valueOffset);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_keyedHourMaxLong(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong pLong,
                                           jlong count, jint valueOffset) {
    kIntMaxLong(int64_to_hour, pRosti, pKeys, pLong, count, valueOffset);
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
                *reinterpret_cast<jlong *>(pVal) = MAX(val, old);
            }
        }
    }
}

}
