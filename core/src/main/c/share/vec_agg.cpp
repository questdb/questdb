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

#include "vec_agg.h"
#include "util.h"
#include <immintrin.h>

#define MAX_VECTOR_SIZE 512

#if INSTRSET >= 10

#define COUNT_DOUBLE F_AVX512(countDouble)
#define SUM_DOUBLE F_AVX512(sumDouble)
#define SUM_DOUBLE_ACC F_AVX512(sumDoubleAcc)
#define SUM_DOUBLE_KAHAN F_AVX512(sumDoubleKahan)
#define SUM_DOUBLE_NEUMAIER F_AVX512(sumDoubleNeumaier)
#define MIN_DOUBLE F_AVX512(minDouble)
#define MAX_DOUBLE F_AVX512(maxDouble)

#define SUM_SHORT F_AVX512(sumShort)
#define MIN_SHORT F_AVX512(minShort)
#define MAX_SHORT F_AVX512(maxShort)

#define COUNT_INT F_AVX512(countInt)
#define SUM_INT F_AVX512(sumInt)
#define SUM_INT_ACC F_AVX512(sumIntAcc)
#define MIN_INT F_AVX512(minInt)
#define MAX_INT F_AVX512(maxInt)

#define COUNT_LONG F_AVX512(countLong)
#define SUM_LONG F_AVX512(sumLong)
#define SUM_LONG_ACC F_AVX512(sumLongAcc)
#define MIN_LONG F_AVX512(minLong)
#define MAX_LONG F_AVX512(maxLong)

#elif INSTRSET >= 8

#define COUNT_DOUBLE F_AVX2(countDouble)
#define SUM_DOUBLE F_AVX2(sumDouble)
#define SUM_DOUBLE_ACC F_AVX2(sumDoubleAcc)
#define SUM_DOUBLE_KAHAN F_AVX2(sumDoubleKahan)
#define SUM_DOUBLE_NEUMAIER F_AVX2(sumDoubleNeumaier)
#define MIN_DOUBLE F_AVX2(minDouble)
#define MAX_DOUBLE F_AVX2(maxDouble)

#define SUM_SHORT F_AVX2(sumShort)
#define MIN_SHORT F_AVX2(minShort)
#define MAX_SHORT F_AVX2(maxShort)

#define COUNT_INT F_AVX2(countInt)
#define SUM_INT F_AVX2(sumInt)
#define SUM_INT_ACC F_AVX2(sumIntAcc)
#define MIN_INT F_AVX2(minInt)
#define MAX_INT F_AVX2(maxInt)

#define COUNT_LONG F_AVX2(countLong)
#define SUM_LONG F_AVX2(sumLong)
#define SUM_LONG_ACC F_AVX2(sumLongAcc)
#define MIN_LONG F_AVX2(minLong)
#define MAX_LONG F_AVX2(maxLong)

#elif INSTRSET >= 5

#define COUNT_DOUBLE F_SSE41(countDouble)
#define SUM_DOUBLE F_SSE41(sumDouble)
#define SUM_DOUBLE_ACC F_SSE41(sumDoubleAcc)
#define SUM_DOUBLE_KAHAN F_SSE41(sumDoubleKahan)
#define SUM_DOUBLE_NEUMAIER F_SSE41(sumDoubleNeumaier)
#define MIN_DOUBLE F_SSE41(minDouble)
#define MAX_DOUBLE F_SSE41(maxDouble)

#define SUM_SHORT F_SSE41(sumShort)
#define MIN_SHORT F_SSE41(minShort)
#define MAX_SHORT F_SSE41(maxShort)

#define COUNT_INT F_SSE41(countInt)
#define SUM_INT F_SSE41(sumInt)
#define SUM_INT_ACC F_SSE41(sumIntAcc)
#define MIN_INT F_SSE41(minInt)
#define MAX_INT F_SSE41(maxInt)

#define COUNT_LONG F_SSE41(countLong)
#define SUM_LONG F_SSE41(sumLong)
#define SUM_LONG_ACC F_SSE41(sumLongAcc)
#define MIN_LONG F_SSE41(minLong)
#define MAX_LONG F_SSE41(maxLong)

#elif INSTRSET >= 2

#define COUNT_DOUBLE F_SSE2(countDouble)
#define SUM_DOUBLE F_SSE2(sumDouble)
#define SUM_DOUBLE_ACC F_SSE2(sumDoubleAcc)
#define SUM_DOUBLE_KAHAN F_SSE2(sumDoubleKahan)
#define SUM_DOUBLE_NEUMAIER F_SSE2(sumDoubleNeumaier)
#define MIN_DOUBLE F_SSE2(minDouble)
#define MAX_DOUBLE F_SSE2(maxDouble)

#define SUM_SHORT F_SSE2(sumShort)
#define MIN_SHORT F_SSE2(minShort)
#define MAX_SHORT F_SSE2(maxShort)

#define COUNT_INT F_SSE2(countInt)
#define SUM_INT F_SSE2(sumInt)
#define SUM_INT_ACC F_SSE2(sumIntAcc)
#define MIN_INT F_SSE2(minInt)
#define MAX_INT F_SSE2(maxInt)

#define COUNT_LONG F_SSE2(countLong)
#define SUM_LONG F_SSE2(sumLong)
#define SUM_LONG_ACC F_SSE2(sumLongAcc)
#define MIN_LONG F_SSE2(minLong)
#define MAX_LONG F_SSE2(maxLong)

#else

#endif

#ifdef SUM_LONG

int64_t COUNT_LONG(int64_t *pl, int64_t count) {
    if (count == 0) {
        return 0;
    }

    const int step = 8;
    Vec8q vec;
    Vec8qb bVec;
    Vec8q veccount = 0;

    int i;
    for (i = 0; i < count - 7; i += step) {
        vec.load(pl + i);
        bVec = vec != L_MIN;
        veccount = if_add(bVec, veccount, 1);
    }

    int64_t result = horizontal_add(veccount);

    for (; i < count; i++) {
        int64_t x = *(pl + i);
        if (x != L_MIN) {
            result++;
        }
    }

    return result;
}

int64_t SUM_LONG(int64_t *pl, int64_t count) {
    if (count == 0) {
        return L_MIN;
    }

    Vec8q vec;
    const int step = 8;
    Vec8q vecsum = 0.;
    Vec8qb bVec;
    Vec8q nancount = 0;
    int i;
    for (i = 0; i < count - 7; i += step) {
        _mm_prefetch(pl + i + 63 * step, _MM_HINT_T1);
        vec.load(pl + i);
        bVec = vec == L_MIN;
        vecsum = if_add(!bVec, vecsum, vec);
        nancount = if_add(bVec, nancount, 1);
    }

    int64_t sum = 0;
    int n = 0;
    for (; i < count; i++) {
        int64_t x = *(pl + i);
        if (x != L_MIN) {
            sum += x;
        } else {
            n++;
        }
    }

    if (horizontal_add(nancount) + n < count) {
        return horizontal_add(vecsum) + sum;
    }

    return L_MIN;
}

double SUM_LONG_ACC(int64_t *pl, int64_t count, int64_t *accCount) {
    if (count == 0) {
        *accCount = 0;
        return NAN;
    }

    Vec8q vec;
    Vec8d dVec;
    const int step = 8;
    Vec8d vecsum = 0.;
    Vec8qb bVec;
    Vec8q nancount = 0;
    int i;
    for (i = 0; i < count - 7; i += step) {
        _mm_prefetch(pl + i + 63 * step, _MM_HINT_T1);
        vec.load(pl + i);
        bVec = vec == L_MIN;
        dVec = to_double(vec);
        vecsum = if_add(!bVec, vecsum, dVec);
        nancount = if_add(bVec, nancount, 1);
    }

    _mm_prefetch(pl, _MM_HINT_T0);
    double sum = horizontal_add(vecsum);
    int64_t nans = horizontal_add(nancount);
    for (; i < count; i++) {
        int64_t l = *(pl + i);
        if (PREDICT_TRUE(l != L_MIN)) {
            sum += (double) l;
        } else {
            nans++;
        }
    }

    *accCount = count - nans;
    return nans < count ? sum : NAN;
}

int64_t MIN_LONG(int64_t *pl, int64_t count) {
    if (count == 0) {
        return L_MIN;
    }

    Vec8q vec;
    const int step = 8;
    Vec8q vecMin = L_MIN;
    int i;
    for (i = 0; i < count - 7; i += step) {
        _mm_prefetch(pl + i + 63 * step, _MM_HINT_T1);
        vec.load(pl + i);
        vecMin = select(vecMin == L_MIN, vec, vecMin);
        vec = select(vec == L_MIN, vecMin, vec);
        // at this point vec and vecMin elements are either both == L_MIN or != L_MIN,
        // so we can safely apply the min function
        vecMin = min(vec, vecMin);
    }

    int64_t min = L_MIN;
    int j;
    for (j = 0; j < step; j++) {
        int64_t n = vecMin[j];
        if (n != L_MIN && (n < min || min == L_MIN)) {
            min = n;
        }
    }

    for (; i < count; i++) {
        const int64_t x = *(pl + i);
        if (x != L_MIN && (x < min || min == L_MIN)) {
            min = x;
        }
    }

    return min;
}

int64_t MAX_LONG(int64_t *pl, int64_t count) {
    if (count == 0) {
        return L_MIN;
    }

    const int step = 8;
    Vec8q vec;
    Vec8q vecMax = L_MIN;
    int i;
    for (i = 0; i < count - 7; i += step) {
        _mm_prefetch(pl + i + 63 * step, _MM_HINT_T1);
        vec.load(pl + i);
        vecMax = max(vecMax, vec);
    }

    int64_t max = horizontal_max(vecMax);
    for (; i < count; i++) {
        const int64_t x = *(pl + i);
        if (x > max) {
            max = x;
        }
    }
    return max;
}

#endif

#ifdef SUM_INT

int64_t COUNT_INT(int32_t *pi, int64_t count) {
    if (count == 0) {
        return 0;
    }

    const int32_t step = 16;
    const auto remainder = (int32_t) (count % step);
    const auto *lim = pi + count;
    const auto *vec_lim = lim - remainder;

    Vec16i vec;
    Vec16i veccount = 0;
    Vec16ib bVec;

    for (; pi < vec_lim; pi += step) {
        vec.load(pi);
        bVec = vec != I_MIN;
        veccount = if_add(bVec, veccount, 1);
    }

    int64_t result = horizontal_add_x(veccount);
    for (; pi < lim; pi++) {
        int32_t v = *pi;
        if (PREDICT_TRUE(v != I_MIN)) {
            ++result;
        }
    }

    return result;
}

int64_t SUM_INT(int32_t *pi, int64_t count) {
    if (count == 0) {
        return L_MIN;
    }

    const int32_t step = 16;
    const auto remainder = (int32_t) (count % step);
    const auto *lim = pi + count;
    const auto *vec_lim = lim - remainder;

    Vec16i vec;
    Vec16ib bVec;
    int64_t result = 0;
    bool hasData = false;
    for (; pi < vec_lim; pi += step) {
        _mm_prefetch(pi + 63 * step, _MM_HINT_T1);
        vec.load(pi);
        bVec = vec != I_MIN;
        hasData = hasData || horizontal_count(bVec) > 0;
        result += horizontal_add_x(select(bVec, vec, 0));
    }

    for (; pi < lim; pi++) {
        int32_t v = *pi;
        if (PREDICT_TRUE(v != I_MIN)) {
            result += v;
            hasData = true;
        }
    }

    return hasData > 0 ? result : L_MIN;
}

double SUM_INT_ACC(int32_t *pi, int64_t count, int64_t *accCount) {
    if (count == 0) {
        *accCount = 0;
        return NAN;
    }

    const int32_t step = 16;
    const auto remainder = (int32_t) (count % step);
    const auto *lim = pi + count;
    const auto *vec_lim = lim - remainder;

    Vec16i vec;
    Vec16ib bVec;
    Vec16i nancount = 0;
    double sum = 0;
    for (; pi < vec_lim; pi += step) {
        _mm_prefetch(pi + 63 * step, _MM_HINT_T1);
        vec.load(pi);
        bVec = vec != I_MIN;
        sum += (double) horizontal_add_x(select(bVec, vec, 0));
        nancount = if_add(!bVec, nancount, 1);
    }

    _mm_prefetch(pi, _MM_HINT_T0);
    int64_t nans = horizontal_add_x(nancount);
    for (; pi < lim; pi++) {
        int32_t v = *pi;
        if (PREDICT_TRUE(v != I_MIN)) {
            sum += (double) v;
        } else {
            nans++;
        }
    }

    *accCount = count - nans;
    return nans < count ? sum : NAN;
}

int32_t MIN_INT(int32_t *pi, int64_t count) {
    if (count == 0) {
        return I_MIN;
    }

    Vec16i vec;
    const int step = 16;
    Vec16i vecMin = I_MIN;
    int i;
    for (i = 0; i < count - 15; i += step) {
        _mm_prefetch(pi + i + 63 * step, _MM_HINT_T1);
        vec.load(pi + i);
        vecMin = select(vecMin == I_MIN, vec, vecMin);
        vec = select(vec == I_MIN, vecMin, vec);
        // at this point vec and vecMin elements are either both == I_MIN or != I_MIN,
        // so we can safely apply the min function
        vecMin = min(vec, vecMin);
    }

    int32_t min = I_MIN;
    int j;
    for (j = 0; j < step; j++) {
        int32_t n = vecMin[j];
        if (n != I_MIN && (n < min || min == I_MIN)) {
            min = n;
        }
    }

    for (; i < count; i++) {
        const int32_t x = *(pi + i);
        if (x != I_MIN && (x < min || min == I_MIN)) {
            min = x;
        }
    }

    return min;
}

int32_t MAX_INT(int32_t *pi, int64_t count) {
    if (count == 0) {
        return I_MIN;
    }

    const int step = 16;
    Vec16i vec;
    Vec16i vecMax = I_MIN;
    int i;
    for (i = 0; i < count - 15; i += step) {
        _mm_prefetch(pi + i + 63 * step, _MM_HINT_T1);
        vec.load(pi + i);
        vecMax = max(vecMax, vec);
    }

    int32_t max = horizontal_max(vecMax);
    for (; i < count; i++) {
        const int32_t x = *(pi + i);
        if (x > max) {
            max = x;
        }
    }
    return max;
}

#endif

#ifdef SUM_SHORT

int64_t SUM_SHORT(int16_t *ps, int64_t count) {
    if (count == 0) {
        return L_MIN;
    }

#if INSTRSET >= 8
    // Faster AVX2 version: uses raw intrinsics since _mm256_madd_epi16 (vpmaddwd) is not available in VCL
    const int32_t step = 32;
    const auto remainder = (int32_t) (count % step);
    const auto *lim = ps + count;
    const auto *vec_lim = lim - remainder;

    const __m256i ones = _mm256_set1_epi16(1);
    __m256i acc0 = _mm256_setzero_si256();
    __m256i acc1 = _mm256_setzero_si256();
    for (; ps < vec_lim; ps += step) {
        _mm_prefetch(ps + 31 * step, _MM_HINT_T1);
        __m256i vec0 = _mm256_loadu_si256((__m256i const*) ps);
        __m256i vec1 = _mm256_loadu_si256((__m256i const*) (ps + 16));
        // multiply the loaded vectors by 1 (no-op) and extend them to 32-bit;
        // then add them to the accumulators
        acc0 = _mm256_add_epi32(acc0, _mm256_madd_epi16(vec0, ones));
        acc1 = _mm256_add_epi32(acc1, _mm256_madd_epi16(vec1, ones));
    }

    _mm_prefetch(ps, _MM_HINT_T0);
    // Horizontal sum of both accumulators to int64
    __m256i combined = _mm256_add_epi32(acc0, acc1);
    __m128i lo = _mm256_castsi256_si128(combined);
    __m128i hi = _mm256_extracti128_si256(combined, 1);
    __m128i sum128 = _mm_add_epi32(lo, hi);
    int64_t sum = (int64_t) _mm_extract_epi32(sum128, 0) +
                  (int64_t) _mm_extract_epi32(sum128, 1) +
                  (int64_t) _mm_extract_epi32(sum128, 2) +
                  (int64_t) _mm_extract_epi32(sum128, 3);
#else
    const int32_t step = 16;
    const auto remainder = (int32_t) (count % step);
    const auto *lim = ps + count;
    const auto *vec_lim = lim - remainder;

    Vec16s vec;
    Vec8i acc0 = 0;
    Vec8i acc1 = 0;
    for (; ps < vec_lim; ps += step) {
        _mm_prefetch(ps + 63 * step, _MM_HINT_T1);
        vec.load(ps);
        acc0 += extend_low(vec);
        acc1 += extend_high(vec);
    }

    _mm_prefetch(ps, _MM_HINT_T0);
    int64_t sum = horizontal_add_x(acc0) + horizontal_add_x(acc1);
#endif
    for (; ps < lim; ps++) {
        int16_t v = *ps;
        sum += v;
    }

    return sum;
}

int32_t MIN_SHORT(int16_t *ps, int64_t count) {
    if (count == 0) {
        return I_MIN;
    }

    const int step = 32;
    const auto remainder = (int32_t) (count % step);
    const auto *lim = ps + count;
    const auto *vec_lim = lim - remainder;

    // instrset >=10 means AVX512BW/DQ/VL support, so it's ok to use Vec32s
    Vec32s vec;
    Vec32s vec_min = S_MAX;
    for (; ps < vec_lim; ps += step) {
        _mm_prefetch(ps + 31 * step, _MM_HINT_T1);
        vec.load(ps);
        vec_min = min(vec_min, vec);
    }

    int32_t min = horizontal_min(vec_min);
    for (; ps < lim; ps++) {
        int16_t x = *ps;
        if (x < min) {
            min = x;
        }
    }
    return min;
}

int32_t MAX_SHORT(int16_t *ps, int64_t count) {
    if (count == 0) {
        return I_MIN;
    }

    const int step = 32;
    const auto remainder = (int32_t) (count % step);
    const auto *lim = ps + count;
    const auto *vec_lim = lim - remainder;

    // instrset >=10 means AVX512BW/DQ/VL support, so it's ok to use Vec32s
    Vec32s vec;
    Vec32s vec_max = S_MIN;
    for (; ps < vec_lim; ps += step) {
        _mm_prefetch(ps + 31 * step, _MM_HINT_T1);
        vec.load(ps);
        vec_max = max(vec_max, vec);
    }

    int32_t max = horizontal_max(vec_max);
    for (; ps < lim; ps++) {
        int16_t x = *ps;
        if (x > max) {
            max = x;
        }
    }
    return max;
}

#endif

#ifdef SUM_DOUBLE

int64_t COUNT_DOUBLE(double *d, int64_t count) {
    if (count == 0) {
        return 0;
    }

    Vec8d vec;
    const int step = 8;
    Vec8db bVec;
    Vec8q nancount = 0;
    int i;
    for (i = 0; i < count - 7; i += step) {
        vec.load(d + i);
        bVec = is_nan(vec);
        nancount = if_add(bVec, nancount, 1);
    }

    int64_t n = horizontal_add(nancount);
    for (; i < count; i++) {
        double x = *(d + i);
        if (PREDICT_FALSE(std::isnan(x))) {
            n++;
        }
    }
    return count - n;
}

double SUM_DOUBLE(double *d, int64_t count) {
    if (count == 0) {
        return NAN;
    }

    Vec8d vec;
    const int step = 8;
    Vec8d vecsum = 0.;
    Vec8db bVec;
    Vec8q nancount = 0;
    int i;
    for (i = 0; i < count - 7; i += step) {
        _mm_prefetch(d + i + 63 * step, _MM_HINT_T1);
        vec.load(d + i);
        bVec = is_nan(vec);
        vecsum += select(bVec, 0, vec);
        nancount = if_add(bVec, nancount, 1);
    }

    _mm_prefetch(d, _MM_HINT_T0);
    double sum = horizontal_add(vecsum);
    int64_t n = horizontal_add(nancount);
    for (; i < count; i++) {
        double x = *(d + i);
        if (PREDICT_TRUE(!std::isnan(x))) {
            sum += x;
        } else {
            n++;
        }
    }

    return n < count ? sum : NAN;
}

double SUM_DOUBLE_ACC(double *d, int64_t count, int64_t *accCount) {
    if (count == 0) {
        *accCount = 0;
        return NAN;
    }

    Vec8d vec;
    const int step = 8;
    Vec8d vecsum = 0.;
    Vec8db bVec;
    Vec8q nancount = 0;
    int i;
    for (i = 0; i < count - 7; i += step) {
        _mm_prefetch(d + i + 63 * step, _MM_HINT_T1);
        vec.load(d + i);
        bVec = is_nan(vec);
        vecsum += select(bVec, 0, vec);
        nancount = if_add(bVec, nancount, 1);
    }

    _mm_prefetch(d, _MM_HINT_T0);
    double sum = horizontal_add(vecsum);
    int64_t nans = horizontal_add(nancount);
    for (; i < count; i++) {
        double v = *(d + i);
        if (PREDICT_TRUE(!std::isnan(v))) {
            sum += v;
        } else {
            nans++;
        }
    }

    *accCount = count - nans;
    return nans < count ? sum : NAN;
}

double SUM_DOUBLE_KAHAN(double *d, int64_t count) {
    if (count == 0) {
        return NAN;
    }

    Vec8d inputVec;
    const int step = 8;
    const auto *lim = d + count;
    const auto remainder = (int32_t) (count % step);
    const auto *lim_vec = lim - remainder;
    Vec8d sumVec = 0.;
    Vec8d yVec;
    Vec8d cVec = 0.;
    Vec8db bVec;
    Vec8q nancount = 0;
    Vec8d tVec;
    for (; d < lim_vec; d += step) {
        _mm_prefetch(d + 63 * step, _MM_HINT_T1);
        inputVec.load(d);
        bVec = is_nan(inputVec);
        nancount = if_add(bVec, nancount, 1);
        yVec = select(bVec, 0, inputVec - cVec);
        tVec = sumVec + yVec;
        cVec = (tVec - sumVec) - yVec;
        sumVec = tVec;
    }

    _mm_prefetch(d, _MM_HINT_T0);
    double sum = horizontal_add(sumVec);
    double c = horizontal_add(cVec);
    int64_t nans = horizontal_add(nancount);
    for (; d < lim; d++) {
        double x = *d;
        if (std::isfinite(x)) {
            auto y = x - c;
            auto t = sum + y;
            c = (t - sum) - y;
            sum = t;
        } else {
            nans++;
        }
    }

    return nans < count ? sum : NAN;
}

double SUM_DOUBLE_NEUMAIER(double *d, int64_t count) {
    if (count == 0) {
        return NAN;
    }

    Vec8d inputVec;
    const int step = 8;
    const auto *lim = d + count;
    const auto remainder = (int32_t) (count % step);
    const auto *lim_vec = lim - remainder;
    Vec8d sumVec = 0.;
    Vec8d cVec = 0.;
    Vec8db bVec;
    Vec8q nancount = 0;
    Vec8d tVec;
    for (; d < lim_vec; d += step) {
        _mm_prefetch(d + 63 * step, _MM_HINT_T1);
        inputVec.load(d);
        bVec = is_nan(inputVec);
        nancount = if_add(bVec, nancount, 1);
        inputVec = select(bVec, 0, inputVec);
        tVec = sumVec + inputVec;
        bVec = abs(sumVec) >= abs(inputVec);
        cVec += (select(bVec, sumVec, inputVec) - tVec) + select(bVec, inputVec, sumVec);
        sumVec = tVec;
    }

    _mm_prefetch(d, _MM_HINT_T0);
    double sum = horizontal_add(sumVec);
    double c = horizontal_add(cVec);
    int64_t nans = horizontal_add(nancount);
    for (; d < lim; d++) {
        double input = *d;
        if (PREDICT_FALSE(std::isnan(input))) {
            nans++;
        } else {
            auto t = sum + input;
            if (abs(sum) >= abs(input)) {
                c += (sum - t) + input;
            } else {
                c += (input - t) + sum;
            }
            sum = t;
        }
    }

    return nans < count ? sum + c : D_NAN;
}

double MIN_DOUBLE(double *d, int64_t count) {
    if (count == 0) {
        return NAN;
    }

    Vec8d vec;
    const int step = 8;
    const double *lim = d + count;
    const double *lim_vec = lim - step + 1;
    Vec8d vecMin = D_MAX;
    Vec8db bVec;
    for (; d < lim_vec; d += step) {
        _mm_prefetch(d + 63 * step, _MM_HINT_T1);
        vec.load(d);
        bVec = is_nan(vec);
        vecMin = select(bVec, vecMin, min(vecMin, vec));
    }

    double min = horizontal_min(vecMin);
    for (; d < lim; d++) {
        double x = *d;
        if (!std::isnan(x) && x < min) {
            min = x;
        }
    }

    return min < D_MAX ? min : NAN;
}

double MAX_DOUBLE(double *d, int64_t count) {
    if (count == 0) {
        return NAN;
    }

    const int step = 8;
    const double *lim = d + count;
    const double *lim_vec = lim - step + 1;

    Vec8d vec;
    Vec8d vecMax = D_MIN;
    Vec8db bVec;
    for (; d < lim_vec; d += step) {
        _mm_prefetch(d + 63 * step, _MM_HINT_T1);
        vec.load(d);
        bVec = is_nan(vec);
        vecMax = select(bVec, vecMax, max(vecMax, vec));
    }

    double max = horizontal_max(vecMax);
    for (; d < lim; d++) {
        double x = *d;
        if (!std::isnan(x) && x > max) {
            max = x;
        }
    }

    return max > D_MIN ? max : NAN;
}

#endif

#if INSTRSET < 5

// Dispatchers
DOUBLE_LONG_DISPATCHER(countDouble)
DOUBLE_DISPATCHER(sumDouble)
DOUBLE_ACC_DISPATCHER(sumDoubleAcc)
DOUBLE_DISPATCHER(sumDoubleKahan)
DOUBLE_DISPATCHER(sumDoubleNeumaier)
DOUBLE_DISPATCHER(minDouble)
DOUBLE_DISPATCHER(maxDouble)

SHORT_LONG_DISPATCHER(sumShort)
SHORT_INT_DISPATCHER(minShort)
SHORT_INT_DISPATCHER(maxShort)

INT_LONG_DISPATCHER(countInt)
INT_LONG_DISPATCHER(sumInt)
INT_LONG_ACC_DISPATCHER(sumIntAcc)
INT_INT_DISPATCHER(minInt)
INT_INT_DISPATCHER(maxInt)

LONG_LONG_DISPATCHER(countLong)
LONG_LONG_DISPATCHER(sumLong)
LONG_LONG_ACC_DISPATCHER(sumLongAcc)
LONG_LONG_DISPATCHER(minLong)
LONG_LONG_DISPATCHER(maxLong)

extern "C" {

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_getSupportedInstructionSet(JNIEnv *env, jclass cl) {
    return instrset_detect();
}

}
#endif  // INSTRSET == 2
