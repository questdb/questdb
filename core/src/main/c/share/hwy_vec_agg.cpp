/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "hwy_vec_agg.cpp"
#include "hwy/foreach_target.h"
#include "hwy/highway.h"

#include "util.h"
#include <cmath>

HWY_BEFORE_NAMESPACE();
namespace questdb_vec {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// ---------------------------------------------------------------------------
// LONG (int64_t) aggregations
// ---------------------------------------------------------------------------

int64_t CountLong(int64_t *pl, int64_t count) {
    if (count == 0) return 0;

    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    auto cnt_a = hn::Zero(d);
    auto cnt_b = hn::Zero(d);
    const auto vec_one = hn::Set(d, int64_t{1});
    const auto vec_lmin = hn::Set(d, L_MIN);

    int64_t i = 0;
    for (; i + 2 * static_cast<int64_t>(N) <= count; i += 2 * static_cast<int64_t>(N)) {
        const auto va = hn::LoadU(d, pl + i);
        const auto vb = hn::LoadU(d, pl + i + N);
        // IfThenZeroElse(Eq, one) = vpandn: avoids the separate Not that Ne requires
        cnt_a = hn::Add(cnt_a, hn::IfThenZeroElse(hn::Eq(va, vec_lmin), vec_one));
        cnt_b = hn::Add(cnt_b, hn::IfThenZeroElse(hn::Eq(vb, vec_lmin), vec_one));
    }

    auto vec_count = hn::Add(cnt_a, cnt_b);
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        const auto vec = hn::LoadU(d, pl + i);
        vec_count = hn::Add(vec_count, hn::IfThenZeroElse(hn::Eq(vec, vec_lmin), vec_one));
    }

    int64_t result = hn::GetLane(hn::SumOfLanes(d, vec_count));
    for (; i < count; i++) {
        if (pl[i] != L_MIN) result++;
    }
    return result;
}

int64_t SumLong(int64_t *pl, int64_t count) {
    if (count == 0) return L_MIN;

    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    auto sum_a = hn::Zero(d);
    auto sum_b = hn::Zero(d);
    auto nancnt_a = hn::Zero(d);
    auto nancnt_b = hn::Zero(d);
    const auto vec_one = hn::Set(d, int64_t{1});
    const auto vec_lmin = hn::Set(d, L_MIN);

    int64_t i = 0;
    for (; i + 2 * static_cast<int64_t>(N) <= count; i += 2 * static_cast<int64_t>(N)) {
        __builtin_prefetch(pl + i + 64 * static_cast<int64_t>(N), 0, 2);
        const auto va = hn::LoadU(d, pl + i);
        const auto vb = hn::LoadU(d, pl + i + N);
        const auto null_a = hn::Eq(va, vec_lmin);
        const auto null_b = hn::Eq(vb, vec_lmin);
        // IfThenZeroElse maps to vpandn — avoids separate Not instruction
        sum_a = hn::Add(sum_a, hn::IfThenZeroElse(null_a, va));
        sum_b = hn::Add(sum_b, hn::IfThenZeroElse(null_b, vb));
        nancnt_a = hn::Add(nancnt_a, hn::IfThenElseZero(null_a, vec_one));
        nancnt_b = hn::Add(nancnt_b, hn::IfThenElseZero(null_b, vec_one));
    }

    auto vec_sum = hn::Add(sum_a, sum_b);
    auto vec_nancount = hn::Add(nancnt_a, nancnt_b);
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        const auto vec = hn::LoadU(d, pl + i);
        const auto is_null = hn::Eq(vec, vec_lmin);
        vec_sum = hn::Add(vec_sum, hn::IfThenZeroElse(is_null, vec));
        vec_nancount = hn::Add(vec_nancount, hn::IfThenElseZero(is_null, vec_one));
    }

    int64_t sum = 0;
    int64_t n = 0;
    for (; i < count; i++) {
        const int64_t x = pl[i];
        if (x != L_MIN) {
            sum += x;
        } else {
            n++;
        }
    }

    if (hn::GetLane(hn::SumOfLanes(d, vec_nancount)) + n < count) {
        return hn::GetLane(hn::SumOfLanes(d, vec_sum)) + sum;
    }
    return L_MIN;
}

double SumLongAcc(int64_t *pl, int64_t count, int64_t *accCount) {
    if (count == 0) {
        *accCount = 0;
        return NAN;
    }

    const hn::ScalableTag<int64_t> d;
    const hn::ScalableTag<double> dd;
    const size_t N = hn::Lanes(d);
    auto vec_sum = hn::Zero(dd);
    auto vec_nancount = hn::Zero(d);
    const auto vec_one = hn::Set(d, int64_t{1});
    const auto vec_lmin = hn::Set(d, L_MIN);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        __builtin_prefetch(pl + i + 64 * static_cast<int64_t>(N), 0, 2);
        const auto vec = hn::LoadU(d, pl + i);
        const auto is_null = hn::Eq(vec, vec_lmin);
        const auto dvec = hn::ConvertTo(dd, vec);
        vec_sum = hn::Add(vec_sum, hn::IfThenZeroElse(hn::RebindMask(dd, is_null), dvec));
        vec_nancount = hn::Add(vec_nancount, hn::IfThenElseZero(is_null, vec_one));
    }

    __builtin_prefetch(pl, 0, 3);
    double sum = hn::GetLane(hn::SumOfLanes(dd, vec_sum));
    int64_t nans = hn::GetLane(hn::SumOfLanes(d, vec_nancount));
    for (; i < count; i++) {
        const int64_t l = pl[i];
        if (PREDICT_TRUE(l != L_MIN)) {
            sum += static_cast<double>(l);
        } else {
            nans++;
        }
    }

    *accCount = count - nans;
    return nans < count ? sum : NAN;
}

int64_t MinLong(int64_t *pl, int64_t count) {
    if (count == 0) return L_MIN;

    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    auto vec_min = hn::Set(d, L_MIN);
    const auto vec_lmin = hn::Set(d, L_MIN);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        __builtin_prefetch(pl + i + 64 * static_cast<int64_t>(N), 0, 2);
        auto vec = hn::LoadU(d, pl + i);
        // Where vecMin is L_MIN (uninitialized), take from vec
        const auto min_is_null = hn::Eq(vec_min, vec_lmin);
        vec_min = hn::IfThenElse(min_is_null, vec, vec_min);
        // Where vec is L_MIN (null input), take from vecMin
        const auto vec_is_null = hn::Eq(vec, vec_lmin);
        vec = hn::IfThenElse(vec_is_null, vec_min, vec);
        // Now both are either both L_MIN or both non-L_MIN
        vec_min = hn::Min(vec, vec_min);
    }

    // Horizontal min with NULL awareness
    HWY_ALIGN int64_t lanes[HWY_MAX_LANES_D(decltype(d))];
    hn::Store(vec_min, d, lanes);
    int64_t min_val = L_MIN;
    for (size_t j = 0; j < N; j++) {
        const int64_t n = lanes[j];
        if (n != L_MIN && (n < min_val || min_val == L_MIN)) {
            min_val = n;
        }
    }

    for (; i < count; i++) {
        const int64_t x = pl[i];
        if (x != L_MIN && (x < min_val || min_val == L_MIN)) {
            min_val = x;
        }
    }
    return min_val;
}

int64_t MaxLong(int64_t *pl, int64_t count) {
    if (count == 0) return L_MIN;

    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    auto max_a = hn::Set(d, L_MIN);
    auto max_b = hn::Set(d, L_MIN);

    int64_t i = 0;
    for (; i + 2 * static_cast<int64_t>(N) <= count; i += 2 * static_cast<int64_t>(N)) {
        __builtin_prefetch(pl + i + 64 * static_cast<int64_t>(N), 0, 2);
        max_a = hn::Max(max_a, hn::LoadU(d, pl + i));
        max_b = hn::Max(max_b, hn::LoadU(d, pl + i + N));
    }

    auto vec_max = hn::Max(max_a, max_b);
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        vec_max = hn::Max(vec_max, hn::LoadU(d, pl + i));
    }

    int64_t max_val = hn::GetLane(hn::MaxOfLanes(d, vec_max));
    for (; i < count; i++) {
        const int64_t x = pl[i];
        if (x > max_val) max_val = x;
    }
    return max_val;
}

// ---------------------------------------------------------------------------
// INT (int32_t) aggregations
// ---------------------------------------------------------------------------

int64_t CountInt(int32_t *pi, int64_t count) {
    if (count == 0) return 0;

    const hn::ScalableTag<int32_t> d;
    const size_t N = hn::Lanes(d);
    auto cnt_a = hn::Zero(d);
    auto cnt_b = hn::Zero(d);
    const auto vec_one = hn::Set(d, int32_t{1});
    const auto vec_imin = hn::Set(d, I_MIN);

    int64_t i = 0;
    for (; i + 2 * static_cast<int64_t>(N) <= count; i += 2 * static_cast<int64_t>(N)) {
        const auto va = hn::LoadU(d, pi + i);
        const auto vb = hn::LoadU(d, pi + i + N);
        cnt_a = hn::Add(cnt_a, hn::IfThenZeroElse(hn::Eq(va, vec_imin), vec_one));
        cnt_b = hn::Add(cnt_b, hn::IfThenZeroElse(hn::Eq(vb, vec_imin), vec_one));
    }

    auto vec_count = hn::Add(cnt_a, cnt_b);
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        const auto vec = hn::LoadU(d, pi + i);
        vec_count = hn::Add(vec_count, hn::IfThenZeroElse(hn::Eq(vec, vec_imin), vec_one));
    }

    // Widen to int64 for final sum to avoid overflow
    const hn::RepartitionToWide<decltype(d)> d64;
    int64_t result = hn::GetLane(hn::SumOfLanes(d64, hn::PromoteLowerTo(d64, vec_count)))
                   + hn::GetLane(hn::SumOfLanes(d64, hn::PromoteUpperTo(d64, vec_count)));

    for (; i < count; i++) {
        if (PREDICT_TRUE(pi[i] != I_MIN)) result++;
    }
    return result;
}

int64_t SumInt(int32_t *pi, int64_t count) {
    if (count == 0) return L_MIN;

    const hn::ScalableTag<int32_t> d32;
    const hn::RepartitionToWide<decltype(d32)> d64;
    const size_t N = hn::Lanes(d32);
    auto acc64 = hn::Zero(d64);
    const auto vec_imin = hn::Set(d32, I_MIN);
    bool hasData = false;

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        __builtin_prefetch(pi + i + 64 * static_cast<int64_t>(N), 0, 2);
        const auto vec = hn::LoadU(d32, pi + i);
        const auto is_null = hn::Eq(vec, vec_imin);
        hasData = hasData || !hn::AllTrue(d32, is_null);
        const auto masked = hn::IfThenZeroElse(is_null, vec);
        acc64 = hn::Add(acc64, hn::PromoteLowerTo(d64, masked));
        acc64 = hn::Add(acc64, hn::PromoteUpperTo(d64, masked));
    }

    int64_t result = hn::GetLane(hn::SumOfLanes(d64, acc64));
    for (; i < count; i++) {
        const int32_t v = pi[i];
        if (PREDICT_TRUE(v != I_MIN)) {
            result += v;
            hasData = true;
        }
    }
    return hasData ? result : L_MIN;
}

double SumIntAcc(int32_t *pi, int64_t count, int64_t *accCount) {
    if (count == 0) {
        *accCount = 0;
        return NAN;
    }

    const hn::ScalableTag<int32_t> d32;
    const hn::RepartitionToWide<decltype(d32)> d64;
    const size_t N = hn::Lanes(d32);
    auto acc64 = hn::Zero(d64);
    auto nancount32 = hn::Zero(d32);
    const auto vec_imin = hn::Set(d32, I_MIN);
    const auto vec_one32 = hn::Set(d32, int32_t{1});

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        __builtin_prefetch(pi + i + 64 * static_cast<int64_t>(N), 0, 2);
        const auto vec = hn::LoadU(d32, pi + i);
        const auto is_null = hn::Eq(vec, vec_imin);
        const auto masked = hn::IfThenZeroElse(is_null, vec);
        acc64 = hn::Add(acc64, hn::PromoteLowerTo(d64, masked));
        acc64 = hn::Add(acc64, hn::PromoteUpperTo(d64, masked));
        nancount32 = hn::Add(nancount32, hn::IfThenElseZero(is_null, vec_one32));
    }

    __builtin_prefetch(pi, 0, 3);
    double sum = static_cast<double>(hn::GetLane(hn::SumOfLanes(d64, acc64)));
    int64_t nans = hn::GetLane(hn::SumOfLanes(d64, hn::PromoteLowerTo(d64, nancount32)))
                 + hn::GetLane(hn::SumOfLanes(d64, hn::PromoteUpperTo(d64, nancount32)));

    for (; i < count; i++) {
        const int32_t v = pi[i];
        if (PREDICT_TRUE(v != I_MIN)) {
            sum += static_cast<double>(v);
        } else {
            nans++;
        }
    }

    *accCount = count - nans;
    return nans < count ? sum : NAN;
}

int32_t MinInt(int32_t *pi, int64_t count) {
    if (count == 0) return I_MIN;

    const hn::ScalableTag<int32_t> d;
    const size_t N = hn::Lanes(d);
    auto vec_min = hn::Set(d, I_MIN);
    const auto vec_imin = hn::Set(d, I_MIN);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        __builtin_prefetch(pi + i + 64 * static_cast<int64_t>(N), 0, 2);
        auto vec = hn::LoadU(d, pi + i);
        const auto min_is_null = hn::Eq(vec_min, vec_imin);
        vec_min = hn::IfThenElse(min_is_null, vec, vec_min);
        const auto vec_is_null = hn::Eq(vec, vec_imin);
        vec = hn::IfThenElse(vec_is_null, vec_min, vec);
        vec_min = hn::Min(vec, vec_min);
    }

    HWY_ALIGN int32_t lanes[HWY_MAX_LANES_D(decltype(d))];
    hn::Store(vec_min, d, lanes);
    int32_t min_val = I_MIN;
    for (size_t j = 0; j < N; j++) {
        const int32_t n = lanes[j];
        if (n != I_MIN && (n < min_val || min_val == I_MIN)) {
            min_val = n;
        }
    }

    for (; i < count; i++) {
        const int32_t x = pi[i];
        if (x != I_MIN && (x < min_val || min_val == I_MIN)) {
            min_val = x;
        }
    }
    return min_val;
}

int32_t MaxInt(int32_t *pi, int64_t count) {
    if (count == 0) return I_MIN;

    const hn::ScalableTag<int32_t> d;
    const size_t N = hn::Lanes(d);
    auto max_a = hn::Set(d, I_MIN);
    auto max_b = hn::Set(d, I_MIN);

    int64_t i = 0;
    for (; i + 2 * static_cast<int64_t>(N) <= count; i += 2 * static_cast<int64_t>(N)) {
        __builtin_prefetch(pi + i + 64 * static_cast<int64_t>(N), 0, 2);
        max_a = hn::Max(max_a, hn::LoadU(d, pi + i));
        max_b = hn::Max(max_b, hn::LoadU(d, pi + i + N));
    }

    auto vec_max = hn::Max(max_a, max_b);
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        vec_max = hn::Max(vec_max, hn::LoadU(d, pi + i));
    }

    int32_t max_val = hn::GetLane(hn::MaxOfLanes(d, vec_max));
    for (; i < count; i++) {
        const int32_t x = pi[i];
        if (x > max_val) max_val = x;
    }
    return max_val;
}

// ---------------------------------------------------------------------------
// SHORT (int16_t) aggregations
// ---------------------------------------------------------------------------

int64_t SumShort(int16_t *ps, int64_t count) {
    if (count == 0) return L_MIN;

    const hn::ScalableTag<int16_t> d16;
    const hn::RepartitionToWide<decltype(d16)> d32;
    const size_t N = hn::Lanes(d16);

    // WidenMulPairwiseAdd(d32, v, ones) computes a[2i]*1 + a[2i+1]*1 in int32,
    // mapping to a single vpmaddwd on x86. Each int32 lane accumulates the sum
    // of two int16 values per iteration, so range per iter is [-65536, 65534].
    // Over CHUNK_ITERS=16384: max ~1.07B, well within int32 range.
    const auto ones = hn::Set(d16, 1);
    constexpr int64_t CHUNK_ITERS = 16384;
    int64_t total = 0;

    int64_t i = 0;
    while (i + static_cast<int64_t>(N) <= count) {
        auto acc = hn::Zero(d32);
        const int64_t chunk_end = i + CHUNK_ITERS * static_cast<int64_t>(N);

        for (; i + static_cast<int64_t>(N) <= count && i < chunk_end; i += static_cast<int64_t>(N)) {
            const auto v = hn::LoadU(d16, ps + i);
            acc = hn::Add(acc, hn::WidenMulPairwiseAdd(d32, v, ones));
        }

        // Flush int32 accumulator to scalar int64. Widen before horizontal
        // reduction: SumOfLanes(d32, acc) would overflow int32 when all lanes
        // are near 1.07B (8 lanes × 1.07B ≈ 8.59B > INT32_MAX).
        const hn::ScalableTag<int64_t> d64;
        total += hn::GetLane(hn::SumOfLanes(d64, hn::PromoteLowerTo(d64, acc)))
               + hn::GetLane(hn::SumOfLanes(d64, hn::PromoteUpperTo(d64, acc)));
    }

    for (; i < count; i++) {
        total += ps[i];
    }
    return total;
}

int32_t MinShort(int16_t *ps, int64_t count) {
    if (count == 0) return I_MIN;

    const hn::ScalableTag<int16_t> d;
    const size_t N = hn::Lanes(d);
    auto vec_min = hn::Set(d, static_cast<int16_t>(S_MAX));

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        __builtin_prefetch(ps + i + 32 * static_cast<int64_t>(N), 0, 2);
        const auto v = hn::LoadU(d, ps + i);
        vec_min = hn::Min(vec_min, v);
    }

    int32_t min_val = hn::GetLane(hn::MinOfLanes(d, vec_min));
    for (; i < count; i++) {
        if (ps[i] < min_val) min_val = ps[i];
    }
    return min_val;
}

int32_t MaxShort(int16_t *ps, int64_t count) {
    if (count == 0) return I_MIN;

    const hn::ScalableTag<int16_t> d;
    const size_t N = hn::Lanes(d);
    auto vec_max = hn::Set(d, static_cast<int16_t>(S_MIN));

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        __builtin_prefetch(ps + i + 32 * static_cast<int64_t>(N), 0, 2);
        const auto v = hn::LoadU(d, ps + i);
        vec_max = hn::Max(vec_max, v);
    }

    int32_t max_val = hn::GetLane(hn::MaxOfLanes(d, vec_max));
    for (; i < count; i++) {
        if (ps[i] > max_val) max_val = ps[i];
    }
    return max_val;
}

// ---------------------------------------------------------------------------
// DOUBLE aggregations
// ---------------------------------------------------------------------------

int64_t CountDouble(double *d_ptr, int64_t count) {
    if (count == 0) return 0;

    const hn::ScalableTag<double> d;
    const hn::ScalableTag<int64_t> di;
    const size_t N = hn::Lanes(d);
    auto valid_a = hn::Zero(di);
    auto valid_b = hn::Zero(di);
    const auto vec_one = hn::Set(di, int64_t{1});

    int64_t i = 0;
    for (; i + 2 * static_cast<int64_t>(N) <= count; i += 2 * static_cast<int64_t>(N)) {
        const auto va = hn::LoadU(d, d_ptr + i);
        const auto vb = hn::LoadU(d, d_ptr + i + N);
        // IfThenZeroElse(IsNaN, one) = one where NOT NaN → counts valid elements
        valid_a = hn::Add(valid_a, hn::IfThenZeroElse(hn::RebindMask(di, hn::IsNaN(va)), vec_one));
        valid_b = hn::Add(valid_b, hn::IfThenZeroElse(hn::RebindMask(di, hn::IsNaN(vb)), vec_one));
    }

    auto vec_valid = hn::Add(valid_a, valid_b);
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        const auto vec = hn::LoadU(d, d_ptr + i);
        vec_valid = hn::Add(vec_valid, hn::IfThenZeroElse(hn::RebindMask(di, hn::IsNaN(vec)), vec_one));
    }

    int64_t result = hn::GetLane(hn::SumOfLanes(di, vec_valid));
    for (; i < count; i++) {
        if (PREDICT_TRUE(!std::isnan(d_ptr[i]))) result++;
    }
    return result;
}

double SumDouble(double *d_ptr, int64_t count) {
    if (count == 0) return NAN;

    const hn::ScalableTag<double> d;
    const hn::ScalableTag<int64_t> di;
    const size_t N = hn::Lanes(d);
    auto sum_a = hn::Zero(d);
    auto sum_b = hn::Zero(d);
    auto nancnt_a = hn::Zero(di);
    auto nancnt_b = hn::Zero(di);
    const auto vec_one = hn::Set(di, int64_t{1});

    int64_t i = 0;
    for (; i + 2 * static_cast<int64_t>(N) <= count; i += 2 * static_cast<int64_t>(N)) {
        __builtin_prefetch(d_ptr + i + 64 * static_cast<int64_t>(N), 0, 2);
        const auto va = hn::LoadU(d, d_ptr + i);
        const auto vb = hn::LoadU(d, d_ptr + i + N);
        const auto nan_a = hn::IsNaN(va);
        const auto nan_b = hn::IsNaN(vb);
        // IfThenZeroElse maps to vpandn — avoids separate Not for the sum accumulation
        sum_a = hn::Add(sum_a, hn::IfThenZeroElse(nan_a, va));
        sum_b = hn::Add(sum_b, hn::IfThenZeroElse(nan_b, vb));
        nancnt_a = hn::Add(nancnt_a, hn::IfThenElseZero(hn::RebindMask(di, nan_a), vec_one));
        nancnt_b = hn::Add(nancnt_b, hn::IfThenElseZero(hn::RebindMask(di, nan_b), vec_one));
    }

    auto vec_sum = hn::Add(sum_a, sum_b);
    auto vec_nancount = hn::Add(nancnt_a, nancnt_b);
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        const auto vec = hn::LoadU(d, d_ptr + i);
        const auto nan_mask = hn::IsNaN(vec);
        vec_sum = hn::Add(vec_sum, hn::IfThenZeroElse(nan_mask, vec));
        vec_nancount = hn::Add(vec_nancount, hn::IfThenElseZero(hn::RebindMask(di, nan_mask), vec_one));
    }

    __builtin_prefetch(d_ptr, 0, 3);
    double sum = hn::GetLane(hn::SumOfLanes(d, vec_sum));
    int64_t n = hn::GetLane(hn::SumOfLanes(di, vec_nancount));
    for (; i < count; i++) {
        const double x = d_ptr[i];
        if (PREDICT_TRUE(!std::isnan(x))) {
            sum += x;
        } else {
            n++;
        }
    }
    return n < count ? sum : NAN;
}

double SumDoubleAcc(double *d_ptr, int64_t count, int64_t *accCount) {
    if (count == 0) {
        *accCount = 0;
        return NAN;
    }

    const hn::ScalableTag<double> d;
    const hn::ScalableTag<int64_t> di;
    const size_t N = hn::Lanes(d);
    auto sum_a = hn::Zero(d);
    auto sum_b = hn::Zero(d);
    auto nancnt_a = hn::Zero(di);
    auto nancnt_b = hn::Zero(di);
    const auto vec_one = hn::Set(di, int64_t{1});

    int64_t i = 0;
    for (; i + 2 * static_cast<int64_t>(N) <= count; i += 2 * static_cast<int64_t>(N)) {
        __builtin_prefetch(d_ptr + i + 64 * static_cast<int64_t>(N), 0, 2);
        const auto va = hn::LoadU(d, d_ptr + i);
        const auto vb = hn::LoadU(d, d_ptr + i + N);
        const auto nan_a = hn::IsNaN(va);
        const auto nan_b = hn::IsNaN(vb);
        sum_a = hn::Add(sum_a, hn::IfThenZeroElse(nan_a, va));
        sum_b = hn::Add(sum_b, hn::IfThenZeroElse(nan_b, vb));
        nancnt_a = hn::Add(nancnt_a, hn::IfThenElseZero(hn::RebindMask(di, nan_a), vec_one));
        nancnt_b = hn::Add(nancnt_b, hn::IfThenElseZero(hn::RebindMask(di, nan_b), vec_one));
    }

    auto vec_sum = hn::Add(sum_a, sum_b);
    auto vec_nancount = hn::Add(nancnt_a, nancnt_b);
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        const auto vec = hn::LoadU(d, d_ptr + i);
        const auto nan_mask = hn::IsNaN(vec);
        vec_sum = hn::Add(vec_sum, hn::IfThenZeroElse(nan_mask, vec));
        vec_nancount = hn::Add(vec_nancount, hn::IfThenElseZero(hn::RebindMask(di, nan_mask), vec_one));
    }

    __builtin_prefetch(d_ptr, 0, 3);
    double sum = hn::GetLane(hn::SumOfLanes(d, vec_sum));
    int64_t nans = hn::GetLane(hn::SumOfLanes(di, vec_nancount));
    for (; i < count; i++) {
        const double v = d_ptr[i];
        if (PREDICT_TRUE(!std::isnan(v))) {
            sum += v;
        } else {
            nans++;
        }
    }

    *accCount = count - nans;
    return nans < count ? sum : NAN;
}

double SumDoubleKahan(double *d_ptr, int64_t count) {
    if (count == 0) return NAN;

    const hn::ScalableTag<double> d;
    const hn::ScalableTag<int64_t> di;
    const size_t N = hn::Lanes(d);
    auto sum_a = hn::Zero(d);
    auto sum_b = hn::Zero(d);
    auto c_a = hn::Zero(d);
    auto c_b = hn::Zero(d);
    auto nancnt_a = hn::Zero(di);
    auto nancnt_b = hn::Zero(di);
    const auto vec_one = hn::Set(di, int64_t{1});

    int64_t i = 0;
    for (; i + 2 * static_cast<int64_t>(N) <= count; i += 2 * static_cast<int64_t>(N)) {
        __builtin_prefetch(d_ptr + i + 64 * static_cast<int64_t>(N), 0, 2);
        const auto va = hn::LoadU(d, d_ptr + i);
        const auto vb = hn::LoadU(d, d_ptr + i + N);
        const auto nan_a = hn::IsNaN(va);
        const auto nan_b = hn::IsNaN(vb);
        nancnt_a = hn::Add(nancnt_a, hn::IfThenElseZero(hn::RebindMask(di, nan_a), vec_one));
        nancnt_b = hn::Add(nancnt_b, hn::IfThenElseZero(hn::RebindMask(di, nan_b), vec_one));
        const auto y_a = hn::IfThenZeroElse(nan_a, hn::Sub(va, c_a));
        const auto y_b = hn::IfThenZeroElse(nan_b, hn::Sub(vb, c_b));
        const auto t_a = hn::Add(sum_a, y_a);
        const auto t_b = hn::Add(sum_b, y_b);
        c_a = hn::Sub(hn::Sub(t_a, sum_a), y_a);
        c_b = hn::Sub(hn::Sub(t_b, sum_b), y_b);
        sum_a = t_a;
        sum_b = t_b;
    }

    auto vec_sum = hn::Add(sum_a, sum_b);
    auto vec_c = hn::Add(c_a, c_b);
    auto vec_nancount = hn::Add(nancnt_a, nancnt_b);
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        const auto input_vec = hn::LoadU(d, d_ptr + i);
        const auto nan_mask = hn::IsNaN(input_vec);
        vec_nancount = hn::Add(vec_nancount, hn::IfThenElseZero(hn::RebindMask(di, nan_mask), vec_one));
        const auto y_vec = hn::IfThenZeroElse(nan_mask, hn::Sub(input_vec, vec_c));
        const auto t_vec = hn::Add(vec_sum, y_vec);
        vec_c = hn::Sub(hn::Sub(t_vec, vec_sum), y_vec);
        vec_sum = t_vec;
    }

    __builtin_prefetch(d_ptr, 0, 3);
    double sum = hn::GetLane(hn::SumOfLanes(d, vec_sum));
    double c = hn::GetLane(hn::SumOfLanes(d, vec_c));
    int64_t nans = hn::GetLane(hn::SumOfLanes(di, vec_nancount));
    for (; i < count; i++) {
        const double x = d_ptr[i];
        if (!std::isnan(x)) {
            const auto y = x - c;
            const auto t = sum + y;
            c = (t - sum) - y;
            sum = t;
        } else {
            nans++;
        }
    }
    return nans < count ? sum : NAN;
}

double SumDoubleNeumaier(double *d_ptr, int64_t count) {
    if (count == 0) return NAN;

    const hn::ScalableTag<double> d;
    const hn::ScalableTag<int64_t> di;
    const size_t N = hn::Lanes(d);
    auto sum_a = hn::Zero(d);
    auto sum_b = hn::Zero(d);
    auto c_a = hn::Zero(d);
    auto c_b = hn::Zero(d);
    auto nancnt_a = hn::Zero(di);
    auto nancnt_b = hn::Zero(di);
    const auto vec_one = hn::Set(di, int64_t{1});

    int64_t i = 0;
    for (; i + 2 * static_cast<int64_t>(N) <= count; i += 2 * static_cast<int64_t>(N)) {
        __builtin_prefetch(d_ptr + i + 64 * static_cast<int64_t>(N), 0, 2);
        auto va = hn::LoadU(d, d_ptr + i);
        auto vb = hn::LoadU(d, d_ptr + i + N);
        const auto nan_a = hn::IsNaN(va);
        const auto nan_b = hn::IsNaN(vb);
        nancnt_a = hn::Add(nancnt_a, hn::IfThenElseZero(hn::RebindMask(di, nan_a), vec_one));
        nancnt_b = hn::Add(nancnt_b, hn::IfThenElseZero(hn::RebindMask(di, nan_b), vec_one));
        va = hn::IfThenZeroElse(nan_a, va);
        vb = hn::IfThenZeroElse(nan_b, vb);
        const auto t_a = hn::Add(sum_a, va);
        const auto t_b = hn::Add(sum_b, vb);
        const auto cmp_a = hn::Ge(hn::Abs(sum_a), hn::Abs(va));
        const auto cmp_b = hn::Ge(hn::Abs(sum_b), hn::Abs(vb));
        const auto big_a = hn::IfThenElse(cmp_a, sum_a, va);
        const auto big_b = hn::IfThenElse(cmp_b, sum_b, vb);
        const auto small_a = hn::IfThenElse(cmp_a, va, sum_a);
        const auto small_b = hn::IfThenElse(cmp_b, vb, sum_b);
        c_a = hn::Add(c_a, hn::Add(hn::Sub(big_a, t_a), small_a));
        c_b = hn::Add(c_b, hn::Add(hn::Sub(big_b, t_b), small_b));
        sum_a = t_a;
        sum_b = t_b;
    }

    auto vec_sum = hn::Add(sum_a, sum_b);
    auto vec_c = hn::Add(c_a, c_b);
    auto vec_nancount = hn::Add(nancnt_a, nancnt_b);
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        auto input_vec = hn::LoadU(d, d_ptr + i);
        const auto nan_mask = hn::IsNaN(input_vec);
        vec_nancount = hn::Add(vec_nancount, hn::IfThenElseZero(hn::RebindMask(di, nan_mask), vec_one));
        input_vec = hn::IfThenZeroElse(nan_mask, input_vec);
        const auto t_vec = hn::Add(vec_sum, input_vec);
        const auto cmp = hn::Ge(hn::Abs(vec_sum), hn::Abs(input_vec));
        const auto big = hn::IfThenElse(cmp, vec_sum, input_vec);
        const auto small = hn::IfThenElse(cmp, input_vec, vec_sum);
        vec_c = hn::Add(vec_c, hn::Add(hn::Sub(big, t_vec), small));
        vec_sum = t_vec;
    }

    __builtin_prefetch(d_ptr, 0, 3);
    double sum = hn::GetLane(hn::SumOfLanes(d, vec_sum));
    double c = hn::GetLane(hn::SumOfLanes(d, vec_c));
    int64_t nans = hn::GetLane(hn::SumOfLanes(di, vec_nancount));
    for (; i < count; i++) {
        const double input = d_ptr[i];
        if (PREDICT_FALSE(std::isnan(input))) {
            nans++;
        } else {
            const auto t = sum + input;
            if (std::abs(sum) >= std::abs(input)) {
                c += (sum - t) + input;
            } else {
                c += (input - t) + sum;
            }
            sum = t;
        }
    }
    return nans < count ? sum + c : D_NAN;
}

double MinDouble(double *d_ptr, int64_t count) {
    if (count == 0) return NAN;

    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto vec_min = hn::Set(d, D_MAX);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        __builtin_prefetch(d_ptr + i + 64 * static_cast<int64_t>(N), 0, 2);
        const auto vec = hn::LoadU(d, d_ptr + i);
        const auto nan_mask = hn::IsNaN(vec);
        vec_min = hn::IfThenElse(nan_mask, vec_min, hn::Min(vec_min, vec));
    }

    double min_val = hn::GetLane(hn::MinOfLanes(d, vec_min));
    for (; i < count; i++) {
        const double x = d_ptr[i];
        if (!std::isnan(x) && x < min_val) min_val = x;
    }
    return min_val < D_MAX ? min_val : NAN;
}

double MaxDouble(double *d_ptr, int64_t count) {
    if (count == 0) return NAN;

    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto vec_max = hn::Set(d, D_MIN);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        __builtin_prefetch(d_ptr + i + 64 * static_cast<int64_t>(N), 0, 2);
        const auto vec = hn::LoadU(d, d_ptr + i);
        const auto nan_mask = hn::IsNaN(vec);
        vec_max = hn::IfThenElse(nan_mask, vec_max, hn::Max(vec_max, vec));
    }

    double max_val = hn::GetLane(hn::MaxOfLanes(d, vec_max));
    for (; i < count; i++) {
        const double x = d_ptr[i];
        if (!std::isnan(x) && x > max_val) max_val = x;
    }
    return max_val > D_MIN ? max_val : NAN;
}

// ---------------------------------------------------------------------------
// Instruction set detection
// ---------------------------------------------------------------------------

int32_t GetSupportedInstructionSet() {
#if HWY_ARCH_X86
    const int64_t targets = hwy::SupportedTargets();
    if (targets & HWY_AVX3) return 10;
    if (targets & HWY_AVX2) return 8;
    if (targets & HWY_SSE4) return 5;
    if (targets & HWY_SSE2) return 2;
    return 0;
#elif HWY_ARCH_ARM
    const int64_t targets = hwy::SupportedTargets();
    if (targets & HWY_SVE) return 11;
    if (targets & HWY_NEON) return 1;
    return 0;
#else
    return 0;
#endif
}

}  // namespace HWY_NAMESPACE
}  // namespace questdb_vec
HWY_AFTER_NAMESPACE();

// ---------------------------------------------------------------------------
// Dispatch tables and JNI wrappers (compiled once)
// ---------------------------------------------------------------------------

#if HWY_ONCE

namespace questdb_vec {

HWY_EXPORT(CountDouble);
HWY_EXPORT(SumDouble);
HWY_EXPORT(SumDoubleAcc);
HWY_EXPORT(SumDoubleKahan);
HWY_EXPORT(SumDoubleNeumaier);
HWY_EXPORT(MinDouble);
HWY_EXPORT(MaxDouble);

HWY_EXPORT(CountInt);
HWY_EXPORT(SumInt);
HWY_EXPORT(SumIntAcc);
HWY_EXPORT(MinInt);
HWY_EXPORT(MaxInt);

HWY_EXPORT(CountLong);
HWY_EXPORT(SumLong);
HWY_EXPORT(SumLongAcc);
HWY_EXPORT(MinLong);
HWY_EXPORT(MaxLong);

HWY_EXPORT(SumShort);
HWY_EXPORT(MinShort);
HWY_EXPORT(MaxShort);

HWY_EXPORT(GetSupportedInstructionSet);

}  // namespace questdb_vec

using namespace questdb_vec;

extern "C" {

// DOUBLE
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_countDouble(JNIEnv *, jclass, jlong pDouble, jlong count) {
    return HWY_DYNAMIC_DISPATCH(CountDouble)((double *) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDouble(JNIEnv *, jclass, jlong pDouble, jlong count) {
    return HWY_DYNAMIC_DISPATCH(SumDouble)((double *) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDoubleAcc(JNIEnv *, jclass, jlong pDouble, jlong count, jlong pAccCount) {
    return HWY_DYNAMIC_DISPATCH(SumDoubleAcc)((double *) pDouble, count, (int64_t *) pAccCount);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDoubleKahan(JNIEnv *, jclass, jlong pDouble, jlong count) {
    return HWY_DYNAMIC_DISPATCH(SumDoubleKahan)((double *) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDoubleNeumaier(JNIEnv *, jclass, jlong pDouble, jlong count) {
    return HWY_DYNAMIC_DISPATCH(SumDoubleNeumaier)((double *) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_minDouble(JNIEnv *, jclass, jlong pDouble, jlong count) {
    return HWY_DYNAMIC_DISPATCH(MinDouble)((double *) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_maxDouble(JNIEnv *, jclass, jlong pDouble, jlong count) {
    return HWY_DYNAMIC_DISPATCH(MaxDouble)((double *) pDouble, count);
}

// INT
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_countInt(JNIEnv *, jclass, jlong pInt, jlong count) {
    return HWY_DYNAMIC_DISPATCH(CountInt)((int32_t *) pInt, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumInt(JNIEnv *, jclass, jlong pInt, jlong count) {
    return HWY_DYNAMIC_DISPATCH(SumInt)((int32_t *) pInt, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumIntAcc(JNIEnv *, jclass, jlong pInt, jlong count, jlong pAccCount) {
    return HWY_DYNAMIC_DISPATCH(SumIntAcc)((int32_t *) pInt, count, (int64_t *) pAccCount);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_minInt(JNIEnv *, jclass, jlong pInt, jlong count) {
    return HWY_DYNAMIC_DISPATCH(MinInt)((int32_t *) pInt, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_maxInt(JNIEnv *, jclass, jlong pInt, jlong count) {
    return HWY_DYNAMIC_DISPATCH(MaxInt)((int32_t *) pInt, count);
}

// LONG
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_countLong(JNIEnv *, jclass, jlong pLong, jlong count) {
    return HWY_DYNAMIC_DISPATCH(CountLong)((int64_t *) pLong, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumLong(JNIEnv *, jclass, jlong pLong, jlong count) {
    return HWY_DYNAMIC_DISPATCH(SumLong)((int64_t *) pLong, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumLongAcc(JNIEnv *, jclass, jlong pLong, jlong count, jlong pAccCount) {
    return HWY_DYNAMIC_DISPATCH(SumLongAcc)((int64_t *) pLong, count, (int64_t *) pAccCount);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_minLong(JNIEnv *, jclass, jlong pLong, jlong count) {
    return HWY_DYNAMIC_DISPATCH(MinLong)((int64_t *) pLong, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_maxLong(JNIEnv *, jclass, jlong pLong, jlong count) {
    return HWY_DYNAMIC_DISPATCH(MaxLong)((int64_t *) pLong, count);
}

// SHORT
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumShort(JNIEnv *, jclass, jlong pShort, jlong count) {
    return HWY_DYNAMIC_DISPATCH(SumShort)((int16_t *) pShort, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_minShort(JNIEnv *, jclass, jlong pShort, jlong count) {
    return HWY_DYNAMIC_DISPATCH(MinShort)((int16_t *) pShort, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_maxShort(JNIEnv *, jclass, jlong pShort, jlong count) {
    return HWY_DYNAMIC_DISPATCH(MaxShort)((int16_t *) pShort, count);
}

// Instruction set
JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_getSupportedInstructionSet(JNIEnv *, jclass) {
    return HWY_DYNAMIC_DISPATCH(GetSupportedInstructionSet)();
}

}  // extern "C"

#endif  // HWY_ONCE
