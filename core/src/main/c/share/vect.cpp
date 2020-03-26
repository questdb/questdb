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

#include <cfloat>
#include <cmath>
#include "vect.h"

#define MAX_VECTOR_SIZE 512

// Define function name depending on which instruction set we compile for
#if INSTRSET >= 10

#define SUM_DOUBLE F_AVX512(sumDouble)
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

bool HAS_NULL(int *pi,  int64_t count) {
    const int step = 16;
    const int remainder = (int) (count - (count / step) * step);
    const int *vec_lim = pi + count - remainder;

    Vec16i vec;
    for (; pi < vec_lim; pi += step) {
        vec.load(pi);
        if (horizontal_find_first(vec == INT_MIN)) {
           return true;
        }
    }

    if (remainder > 0) {
        vec.load_partial(remainder, pi);
        if (horizontal_find_first(vec == INT_MIN)) {
            return true;
        }
    }
    return false;
}

#endif


#ifdef SUM_LONG

int64_t SUM_LONG(int64_t *pl, int64_t count) {
    const int32_t step = 8;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = pl + count;
    const auto *vec_lim = lim - remainder;

    Vec8q vec;
    Vec8qb bVec;
    bool hasData = false;
    int64_t result = 0;
    for (; pl < vec_lim; pl += step) {
        vec.load(pl);
        bVec = vec != LLONG_MIN;
        hasData = hasData || horizontal_count(bVec) > 0;
        result += horizontal_add(select(bVec, vec, 0));
    }

    if (pl < lim) {
        for (; pl < lim; pl++) {
            int64_t v = *pl;
            if (v != LLONG_MIN) {
                result += v;
                hasData = true;
            }
        }
    }

    return hasData > 0 ? result : LLONG_MIN;
}

int64_t MIN_LONG(int64_t *pl, int64_t count) {
    const int32_t step = 8;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = pl + count;
    const auto *vec_lim = lim - remainder;

    Vec8q vec;
    int64_t min = LLONG_MAX;
    int64_t v;
    for (; pl < vec_lim; pl += step) {
        vec.load(pl);
        v = horizontal_min1(select(vec != LLONG_MIN, vec, LLONG_MAX));
        if (v < min) {
            min = v;
        }
    }

    if (pl < lim) {
        for (; pl < lim; pl++) {
            v = *pl;
            if (v != LLONG_MIN && v < min) {
                min = v;
            }
        }
    }
    return min == LLONG_MAX ? LLONG_MIN : min;
}

int64_t MAX_LONG(int64_t *pl, int64_t count) {
    const int32_t step = 8;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = pl + count;
    const auto *vec_lim = lim - remainder;

    Vec8q vec;
    int64_t max = LLONG_MIN;
    int64_t v;
    for (; pl < vec_lim; pl += step) {
        vec.load(pl);
        v = horizontal_max(vec);
        if (v > max) {
            max = v;
        }
    }

    if (pl < lim) {
        for (; pl < lim; pl++) {
            v = *pl;
            if (v > max) {
                max = v;
            }
        }
    }
    return max;
}

double AVG_LONG(int64_t *pl, int64_t count) {
    const int32_t step = 8;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = pl + count;
    const auto *vec_lim = lim - remainder;

    Vec8q vec;
    Vec8qb bVec;
    int64_t sum = 0;
    int64_t sumCount = 0;
    for (; pl < vec_lim; pl += step) {
        vec.load(pl);
        bVec = vec != LLONG_MIN;
        sumCount += horizontal_count(bVec);
        sum += horizontal_add(select(bVec, vec, 0));
    }

    if (pl < lim) {
        for (; pl < lim; pl++) {
            int64_t v = *pl;
            if (v != LLONG_MIN) {
                sum += v;
                sumCount++;
            }
        }
    }
    return (double) sum / sumCount;
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
        vec.load(pi);
        bVec = vec != INT_MIN;
        hasData = hasData || horizontal_count(bVec) > 0;
        result += horizontal_add_x(select(bVec, vec, 0));
    }

    if (pi < lim) {
        for (; pi < lim; pi++) {
            int32_t v = *pi;
            if (v != INT_MIN) {
                result += v;
                hasData = true;
            }
        }
    }

    return hasData > 0 ? result : LLONG_MIN;
}

int32_t MIN_INT(int32_t *pi, int64_t count) {
    const int32_t step = 16;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = pi + count;
    const auto *vec_lim = lim - remainder;

    Vec16i vec;
    int32_t min = INT_MAX;
    int32_t v;
    for (; pi < vec_lim; pi += step) {
        vec.load(pi);
        v = horizontal_min1(select(vec != INT_MIN, vec, min));
        if (v < min) {
            min = v;
        }
    }

    if (pi < lim) {
        for (; pi < lim; pi++) {
            v = *pi;
            if (v != INT_MIN && v < min) {
                min = v;
            }
        }
    }
    return min == INT_MAX ? INT_MIN : min;
}

int32_t MAX_INT(int32_t *pi, int64_t count) {
    const int32_t step = 16;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = pi + count;
    const auto *vec_lim = lim - remainder;

    Vec16i vec;
    int32_t max = INT_MIN;
    int32_t v;
    for (; pi < vec_lim; pi += step) {
        vec.load(pi);
        // we can use "max" directly because our "null" is INT_MIN
        v = horizontal_max(vec);
        if (v > max) {
            max = v;
        }
    }

    if (pi < lim) {
        for (; pi < lim; pi++) {
            v = *pi;
            if (v > max) {
                max = v;
            }
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
    const int32_t step = 8;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = d + count;
    const auto *vec_lim = lim - remainder;

    double *pd = d;
    Vec8d vec;
    Vec8db bVec;
    double sum = 0;
    long sumCount = 0;
    for (; pd < vec_lim; pd += step) {
        vec.load(pd);
        bVec = is_nan(vec);
        sumCount += step - horizontal_count(bVec);
        sum += horizontal_add(select(bVec, 0.0, vec));
    }

    if (pd < lim) {
        for (; pd < lim; pd++) {
            double x = *pd;
            if (x == x) {
                sum += x;
                sumCount++;
            }
        }
    }
    return sumCount > 0 ? sum : NAN;
}

double AVG_DOUBLE(double *d, int64_t count) {
    const int32_t step = 8;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = d + count;
    const auto *vec_lim = lim - remainder;

    double *pd = d;
    Vec8d vec;
    Vec8db bVec;
    double sum = 0;
    long sumCount = 0;
    for (; pd < vec_lim; pd += step) {
        vec.load(pd);
        bVec = is_nan(vec);
        sumCount += step - horizontal_count(bVec);
        sum += horizontal_add(select(bVec, 0.0, vec));
    }

    if (pd < lim) {
        for (; pd < lim; pd++) {
            double x = *pd;
            if (x == x) {
                sum += x;
                sumCount++;
            }
        }
    }
    return sum / sumCount;
}

double MIN_DOUBLE(double *d, int64_t count) {
    const int32_t step = 8;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = d + count;
    const auto *vec_lim = lim - remainder;

    Vec8d vec;
    double min = INFINITY;
    double x;
    for (; d < vec_lim; d += step) {
        vec.load(d);
        x = horizontal_min(select(is_nan(vec), INFINITY, vec));
        if (x < min) {
            min = x;
        }
    }

    if (d < lim) {
        for (; d < lim; d++) {
            x = *d;
            if (x < min) {
                min = x;
            }
        }
    }
    return min == INFINITY ? NAN : min;
}

double MAX_DOUBLE(double *d, int64_t count) {
    const int32_t step = 8;
    const auto remainder = (int32_t) (count - (count / step) * step);
    const auto *lim = d + count;
    const auto *vec_lim = lim - remainder;

    Vec8d vec;
    double x;
    double max = -INFINITY;
    for (; d < vec_lim; d += step) {
        vec.load(d);
        x = horizontal_max(select(is_nan(vec), -INFINITY, vec));
        if (x > max) {
            max = x;
        }
    }

    if (d < lim) {
        for (; d < lim; d++) {
            x = *d;
            if (x > max) {
                max = x;
            }
        }
    }
    return max == -INFINITY ? NAN : max;
}

#endif

#if INSTRSET < 5

// Dispatchers
DOUBLE_DISPATCHER(sumDouble)
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

#endif  // INSTRSET == 2

