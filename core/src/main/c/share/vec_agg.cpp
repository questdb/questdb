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
#include "vec_agg.h"
#include "util.h"

#define MAX_VECTOR_SIZE 512

#if INSTRSET >= 10

#define SUM_DOUBLE F_AVX512(sumDouble)
#define SUM_DOUBLE_KAHAN F_AVX512(sumDoubleKahan)
#define SUM_DOUBLE_NEUMAIER F_AVX512(sumDoubleNeumaier)
#define AVG_DOUBLE F_AVX512(avgDouble)
#define MIN_DOUBLE F_AVX512(minDouble)
#define MAX_DOUBLE F_AVX512(maxDouble)

#define SUM_INT F_AVX512(sumInt)
#define AVG_INT F_AVX512(avgInt)
#define MIN_INT F_AVX512(minInt)
#define MAX_INT F_AVX512(maxInt)

#define SUM_LONG F_AVX512(sumLong)
#define AVG_LONG F_AVX512(avgLong)
#define MIN_LONG F_AVX512(minLong)
#define MAX_LONG F_AVX512(maxLong)

#define HAS_NULL F_AVX512(hasNull)

#elif INSTRSET >= 8

#define SUM_DOUBLE F_AVX2(sumDouble)
#define SUM_DOUBLE_KAHAN F_AVX2(sumDoubleKahan)
#define SUM_DOUBLE_NEUMAIER F_AVX2(sumDoubleNeumaier)
#define AVG_DOUBLE F_AVX2(avgDouble)
#define MIN_DOUBLE F_AVX2(minDouble)
#define MAX_DOUBLE F_AVX2(maxDouble)

#define SUM_INT F_AVX2(sumInt)
#define AVG_INT F_AVX2(avgInt)
#define MIN_INT F_AVX2(minInt)
#define MAX_INT F_AVX2(maxInt)

#define SUM_LONG F_AVX2(sumLong)
#define AVG_LONG F_AVX2(avgLong)
#define MIN_LONG F_AVX2(minLong)
#define MAX_LONG F_AVX2(maxLong)

#define HAS_NULL F_AVX2(hasNull)

#elif INSTRSET >= 5

#define SUM_DOUBLE F_SSE41(sumDouble)
#define SUM_DOUBLE_KAHAN F_SSE41(sumDoubleKahan)
#define SUM_DOUBLE_NEUMAIER F_SSE41(sumDoubleNeumaier)
#define AVG_DOUBLE F_SSE41(avgDouble)
#define MIN_DOUBLE F_SSE41(minDouble)
#define MAX_DOUBLE F_SSE41(maxDouble)

#define SUM_INT F_SSE41(sumInt)
#define AVG_INT F_SSE41(avgInt)
#define MIN_INT F_SSE41(minInt)
#define MAX_INT F_SSE41(maxInt)

#define SUM_LONG F_SSE41(sumLong)
#define AVG_LONG F_SSE41(avgLong)
#define MIN_LONG F_SSE41(minLong)
#define MAX_LONG F_SSE41(maxLong)

#define HAS_NULL F_SSE41(hasNull)

#elif INSTRSET >= 2

#define SUM_DOUBLE F_SSE2(sumDouble)
#define SUM_DOUBLE_KAHAN F_SSE2(sumDoubleKahan)
#define SUM_DOUBLE_NEUMAIER F_SSE2(sumDoubleNeumaier)
#define AVG_DOUBLE F_SSE2(avgDouble)
#define MIN_DOUBLE F_SSE2(minDouble)
#define MAX_DOUBLE F_SSE2(maxDouble)

#define SUM_INT F_SSE2(sumInt)
#define AVG_INT F_SSE2(avgInt)
#define MIN_INT F_SSE2(minInt)
#define MAX_INT F_SSE2(maxInt)

#define SUM_LONG F_SSE2(sumLong)
#define AVG_LONG F_SSE2(avgLong)
#define MIN_LONG F_SSE2(minLong)
#define MAX_LONG F_SSE2(maxLong)

#define HAS_NULL F_SSE2(hasNull)

#else

#endif

#ifdef HAS_NULL

bool HAS_NULL(int32_t *pi, int64_t count) {
    const int32_t step = 16;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *vec_lim = pi + count - remainder;

    Vec16i vec;
    for (; pi < vec_lim; pi += step) {
        _mm_prefetch(pi + 63 * step, _MM_HINT_T1);
        vec.load(pi);
        if (horizontal_find_first(vec == INT_MIN) > -1) {
            return true;
        }
    }

    if (remainder > 0) {
        vec.load_partial(remainder, pi);
        return horizontal_find_first(vec == INT_MIN) > -1;
    }
    return false;
}

#endif


#ifdef SUM_LONG

int64_t SUM_LONG(int64_t *pl, int64_t count) {
    Vec8q vec;
    const int step = 8;
    Vec8q vecsum = 0.;
    Vec8qb bVec;
    Vec8q nancount = 0;
    int i;
    for (i = 0; i < count - 7; i += step) {
        _mm_prefetch(pl + i + 63 * step, _MM_HINT_T1);
        vec.load(pl + i);
        bVec = vec == LLONG_MIN;
        vecsum = if_add(!bVec, vecsum, vec);
        nancount = if_add(bVec, nancount, 1);
    }

    int64_t sum = 0;
    int n = 0;
    for (; i < count; i++) {
        int64_t x = *(pl + i);
        if (x != LLONG_MIN) {
            sum += x;
        } else {
            n++;
        }
    }

    if (horizontal_add(nancount) + n < count) {
        return horizontal_add(vecsum) + sum;
    }

    return LLONG_MIN;
}

int64_t MIN_LONG(int64_t *pl, int64_t count) {
    Vec8q vec;
    const int step = 8;
    Vec8q vecMin = LLONG_MAX;
    Vec8qb bVec;
    int i;
    for (i = 0; i < count - 7; i += step) {
        _mm_prefetch(pl + i + 63 * step, _MM_HINT_T1);
        vec.load(pl + i);
        bVec = vec == LLONG_MIN;
        vecMin = select(bVec, vecMin, min(vecMin, vec));
    }

    int64_t min = horizontal_min(vecMin);
    for (; i < count; i++) {
        int64_t x = *(pl + i);
        if (x != LLONG_MIN && x < min) {
            min = x;
        }
    }

    if (min < LLONG_MAX) {
        return min;
    }

    return LLONG_MIN;
}

int64_t MAX_LONG(int64_t *pl, int64_t count) {
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
        int64_t x = *(pl + i);
        if (x > max) {
            max = x;
        }
    }
    return max;
}

double AVG_LONG(int64_t *pl, int64_t count) {
    Vec8q vec;
    const int step = 8;
    Vec8q vecsum = 0.;
    Vec8qb bVec;
    Vec8q nancount = 0;
    int i;
    for (i = 0; i < count - 7; i += step) {
        _mm_prefetch(pl + i + 63 * step, _MM_HINT_T1);
        vec.load(pl + i);
        bVec = vec == LLONG_MIN;
        vecsum = if_add(!bVec, vecsum, vec);
        nancount = if_add(bVec, nancount, 1);
    }

    int64_t sum = 0;
    int n = 0;
    for (; i < count; i++) {
        int64_t x = *(pl + i);
        if (x != LLONG_MIN) {
            sum += x;
        } else {
            n++;
        }
    }

    const int64_t nans = horizontal_add(nancount) + n;
    if (nans < count) {
        return (double) (horizontal_add(vecsum) + sum) / (double) (count - nans);
    }

    return NAN;
}

#endif

#ifdef SUM_INT

int64_t SUM_INT(int32_t *pi, int64_t count) {
    const int32_t step = 16;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = pi + count;
    const auto *vec_lim = lim - remainder;

    Vec16i vec;
    Vec16ib bVec;
    int64_t result = 0;
    bool hasData = false;
    for (; pi < vec_lim; pi += step) {
        _mm_prefetch(pi + 63 * step, _MM_HINT_T1);
        vec.load(pi);
        bVec = vec != INT_MIN;
        hasData = hasData || horizontal_count(bVec) > 0;
        result += horizontal_add_x(select(bVec, vec, 0));
    }

    if (pi < lim) {
        for (; pi < lim; pi++) {
            int32_t v = *pi;
            if (PREDICT_TRUE(v != INT_MIN)) {
                result += v;
                hasData = true;
            }
        }
    }

    return hasData > 0 ? result : LLONG_MIN;
}

int32_t MIN_INT(int32_t *pi, int64_t count) {
    Vec16i vec;
    const int step = 16;
    Vec16i vecMin = INT_MAX;
    Vec16ib bVec;
    int i;
    for (i = 0; i < count - 15; i += step) {
        _mm_prefetch(pi + i + 63 * step, _MM_HINT_T1);
        vec.load(pi + i);
        bVec = vec == INT_MIN;
        vecMin = select(bVec, vecMin, min(vecMin, vec));
    }

    int32_t min = horizontal_min(vecMin);
    for (; i < count; i++) {
        int32_t x = *(pi + i);
        if (x != INT_MIN && x < min) {
            min = x;
        }
    }

    if (min < INT_MAX) {
        return min;
    }

    return INT_MIN;
}

int32_t MAX_INT(int32_t *pi, int64_t count) {
    const int step = 16;
    Vec16i vec;
    Vec16i vecMax = INT_MIN;
    int i;
    for (i = 0; i < count - 7; i += step) {
        _mm_prefetch(pi + i + 63 * step, _MM_HINT_T1);
        vec.load(pi + i);
        vecMax = max(vecMax, vec);
    }

    int32_t max = horizontal_max(vecMax);
    for (; i < count; i++) {
        int32_t x = *(pi + i);
        if (x > max) {
            max = x;
        }
    }
    return max;
}

double AVG_INT(int32_t *pi, int64_t count) {
    const int32_t step = 16;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = pi + count;
    const auto *vec_lim = lim - remainder;

    Vec16i vec;
    Vec16ib bVec;
    int64_t sum = 0;
    int64_t sumCount = 0;
    for (; pi < vec_lim; pi += step) {
        _mm_prefetch(pi + 63 * step, _MM_HINT_T1);
        vec.load(pi);
        bVec = vec != INT_MIN;
        sumCount += horizontal_count(bVec);
        sum += horizontal_add_x(select(bVec, vec, 0));
    }

    if (pi < lim) {
        for (; pi < lim; pi++) {
            int v = *pi;
            if (v != INT_MIN) {
                sum += v;
                sumCount++;
            }
        }
    }
    return (double_t) sum / sumCount;
}

#endif

#ifdef SUM_DOUBLE

double SUM_DOUBLE(double *d, int64_t count) {
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
        if (PREDICT_TRUE(std::isfinite(x))) {
            sum += x;
        } else {
            n++;
        }
    }

    if (n < count) {
        return sum;
    }
    return NAN;
}

double SUM_DOUBLE_KAHAN(double *d, int64_t count) {
//    return sumDoubleKahan_Vanilla(d, count);
    Vec8d inputVec;
    const int step = 8;
    const auto *lim = d + count;
    const auto remainder = (int32_t) (count - (count / step) * step);
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
            c = (t - sum) -y;
            sum = t;
        } else {
            nans++;
        }
    }

    if (nans < count) {
        return sum;
    }

    return NAN;
}

double SUM_DOUBLE_NEUMAIER(double *d, int64_t count) {
//    return sumDoubleNeumaier_Vanilla(d, count);
    Vec8d inputVec;
    const int step = 8;
    const auto *lim = d + count;
    const auto remainder = (int32_t) (count - (count / step) * step);
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

    if (nans < count) {
        return sum + c;
    }

    return D_NAN;
}

double AVG_DOUBLE(double *d, int64_t count) {
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

    double sum = horizontal_add(vecsum);
    int64_t n = horizontal_add(nancount);
    for (; i < count; i++) {
        double x = *(d + i);
        if (PREDICT_TRUE(std::isfinite(x))) {
            sum += x;
        } else {
            n++;
        }
    }

    if (n < count) {
        return sum / (double) (count - n);
    }

    return D_NAN;
}

double MIN_DOUBLE(double *d, int64_t count) {
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
        if (std::isfinite(x) && x < min) {
            min = x;
        }
    }

    if (min < D_MAX) {
        return min;
    }

    return NAN;
}

double MAX_DOUBLE(double *d, int64_t count) {
    Vec8d vec;
    const int step = 8;
    const double *lim = d + count;
    const double *lim_vec = lim - step + 1;
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
        if (std::isfinite(x) && x > max) {
            max = x;
        }
    }

    if (max > D_MIN) {
        return max;
    }

    return NAN;
}

#endif

#if INSTRSET < 5

// Dispatchers
DOUBLE_DISPATCHER(sumDouble)
DOUBLE_DISPATCHER(sumDoubleKahan)
DOUBLE_DISPATCHER(sumDoubleNeumaier)
DOUBLE_DISPATCHER(avgDouble)
DOUBLE_DISPATCHER(minDouble)
DOUBLE_DISPATCHER(maxDouble)

INT_LONG_DISPATCHER(sumInt)
INT_BOOL_DISPATCHER(hasNull)
INT_DOUBLE_DISPATCHER(avgInt)
INT_INT_DISPATCHER(minInt)
INT_INT_DISPATCHER(maxInt)

LONG_LONG_DISPATCHER(sumLong)
LONG_DOUBLE_DISPATCHER(avgLong)
LONG_LONG_DISPATCHER(minLong)
LONG_LONG_DISPATCHER(maxLong)

extern "C" {
JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_getSupportedInstructionSet(JNIEnv *env, jclass cl) {
    return instrset_detect();
}

}
#endif  // INSTRSET == 2
