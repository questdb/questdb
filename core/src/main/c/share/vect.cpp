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

#define HAS_NULL F_AVX512(hasNull)

#elif INSTRSET >= 8

#define SUM_DOUBLE F_AVX2(sumDouble)
#define AVG_DOUBLE F_AVX2(avgDouble)
#define MIN_DOUBLE F_AVX2(minDouble)
#define MAX_DOUBLE F_AVX2(maxDouble)

#define SUM_INT F_AVX2(sumInt)

#define HAS_NULL F_AVX2(hasNull)

#elif INSTRSET >= 5

#define SUM_DOUBLE F_SSE41(sumDouble)
#define AVG_DOUBLE F_SSE41(avgDouble)
#define MIN_DOUBLE F_SSE41(minDouble)
#define MAX_DOUBLE F_SSE41(maxDouble)

#define SUM_INT F_SSE41(sumInt)

#define HAS_NULL F_SSE41(hasNull)

#elif INSTRSET >= 2

#define SUM_DOUBLE F_SSE2(sumDouble)
#define AVG_DOUBLE F_SSE2(avgDouble)
#define MIN_DOUBLE F_SSE2(minDouble)
#define MAX_DOUBLE F_SSE2(maxDouble)

#define SUM_INT F_SSE2(sumInt)

#define HAS_NULL F_SSE2(hasNull)

#else

#endif

#ifdef HAS_NULL

bool HAS_NULL(int *pi,  int64_t count) {
    const int step = 4;
    const int remainder = (int) (count - (count / step) * step);
    const int *vec_lim = pi + count - remainder;

    Vec4i vec;
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


#ifdef SUM_INT

long SUM_INT(int *pi, long count) {
    const int step = 4;
    const int remainder = (int) (count - (count / step) * step);
    const int *vec_lim = pi + count - remainder;

    Vec4i vec, d;
    int64_t result = 0;
    for (; pi < vec_lim; pi += step) {
        vec.load(pi);
        d = select(vec != INT_MIN, vec, 0);
        result += horizontal_add_x(d);
    }

    if (remainder > 0) {
        vec.load_partial(remainder, pi);
        d = select(vec != INT_MIN, vec, 0);
        result += horizontal_add_x(d);
    }
    return result;
}

#endif

#ifdef SUM_DOUBLE

double SUM_DOUBLE(double *d, long count) {
    const int step = 8;
    const int remainder = (int) (count - (count / step) * step);
    const double *vec_lim = d + count - remainder;

    double *pd = d;
    Vec8d vec, v;
    double result = 0;
    for (; pd < vec_lim; pd += step) {
        vec.load(pd);
        v = select(is_nan(vec), 0.0, vec);
        result += horizontal_add(v);
    }

    if (remainder > 0) {
        vec.load_partial(remainder, pd);
        v = select(is_nan(vec), 0.0, vec);
        result += horizontal_add(v);
    }
    return result;
}

double AVG_DOUBLE(double *d, long count) {
    const int step = 8;
    const long remainder = count - (count / step) * step;
    const double *vec_lim = d + count - remainder;

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

    if (remainder > 0) {
        vec.load_partial(remainder, pd);
        bVec = is_nan(vec);
        sumCount += step - horizontal_count(bVec);
        sum += horizontal_add(select(bVec, 0.0, vec));
    }
    return sum / sumCount;
}

double MIN_DOUBLE(double *d, long count) {
    const int step = 8;
    const long remainder = count - (count / step) * step;
    const double *lim = d + count;
    const double *vec_lim = lim - remainder;

    double *pd = d;
    Vec8d vec;
    double min = LDBL_MAX;
    for (; pd < vec_lim; pd += step) {
        vec.load(pd);
        double x = horizontal_min1(vec);
        if (x < min) {
            min = x;
        }
    }

    if (pd < lim) {
        for (; pd < lim; pd++) {
            double x = *pd;
            if (x < min) {
                min = x;
            }
        }
    }
    return min;
}

double MAX_DOUBLE(double *d, long count) {
    const int step = 8;
    const long remainder = count - (count / step) * step;
    const double *lim = d + count;
    const double *vec_lim = lim - remainder;

    double *pd = d;
    Vec8d vec;
    double max = LDBL_MIN;
    for (; pd < vec_lim; pd += step) {
        vec.load(pd);
        double x = horizontal_max1(vec);
        if (x > max) {
            max = x;
        }
    }

    if (pd < lim) {
        for (; pd < lim; pd++) {
            double x = *pd;
            if (x > max) {
                max = x;
            }
        }
    }
    return max;
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

#endif  // INSTRSET == 2

