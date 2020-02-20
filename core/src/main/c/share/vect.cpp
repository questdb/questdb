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
#include "vect.h"

#define MAX_VECTOR_SIZE 512

// Define function name depending on which instruction set we compile for
#if INSTRSET >= 10

#define SUM_DOUBLE F_AVX512(sumDouble)
#define AVG_DOUBLE F_AVX512(avgDouble)
#define MIN_DOUBLE F_AVX512(minDouble)
#define MAX_DOUBLE F_AVX512(maxDouble)

#elif INSTRSET >= 8

#define SUM_DOUBLE F_AVX2(sumDouble)
#define AVG_DOUBLE F_AVX2(avgDouble)
#define MIN_DOUBLE F_AVX2(minDouble)
#define MAX_DOUBLE F_AVX2(maxDouble)

#elif INSTRSET >= 5

#define SUM_DOUBLE F_SSE41(sumDouble)
#define AVG_DOUBLE F_SSE41(avgDouble)
#define MIN_DOUBLE F_SSE41(minDouble)
#define MAX_DOUBLE F_SSE41(maxDouble)

#elif INSTRSET >= 2

#define SUM_DOUBLE F_SSE2(sumDouble)
#define AVG_DOUBLE F_SSE2(avgDouble)
#define MIN_DOUBLE F_SSE2(minDouble)
#define MAX_DOUBLE F_SSE2(maxDouble)

#else

#endif

#ifdef SUM_DOUBLE

double SUM_DOUBLE(double *d, long count) {
    const int step = 8;
    const long remainder = count - (count / step) * step;
    const double *vec_lim = d + count - remainder;

    double *pd = d;
    Vec8d vec;
    double result = 0;
    for (; pd < vec_lim; pd += step) {
        vec.load(pd);
        double s = horizontal_add(vec);
        if (s != s) {
            result += sum_nan_as_zero(pd, step);
        } else {
            result += s;
        }
    }

    if (remainder > 0) {
        result += sum_nan_as_zero(pd, remainder);
    }
    return result;
}

/*
double SUM_DOUBLE_NOT_NULL(double *d, long count) {
    const int step = 8;
    const long remainder = count - (count / step) * step;
    const double *lim = d + count;
    const double *vec_lim = lim - remainder;

    double *pd = d;
    Vec8d vec1;
    double result = 0;
    for (; pd < vec_lim; pd += step) {
        vec1.load(pd);
        result += horizontal_add(vec1);
    }

    if (pd < lim) {
        for (; pd < lim; pd++) {
            result += *pd;
        }
    }
    return result;
}
*/

double AVG_DOUBLE(double *d, long count) {
    const int step = 8;
    const long remainder = count - (count / step) * step;
    const double *vec_lim = d + count - remainder;

    double *pd = d;
    Vec8d vec;
    double sum = 0;
    long sumCount = 0;
    for (; pd < vec_lim; pd += step) {
        vec.load(pd);
        double s = horizontal_add(vec);
        if (s != s) {
            auto v = avg_skip_nan(pd, step);
            sum += v.sum;
            sumCount += v.count;
        } else {
            sum += s;
            sumCount += step;
        }
    }

    if (remainder > 0) {
        auto v = avg_skip_nan(pd, remainder);
        sum += v.sum;
        sumCount += v.count;
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

#if INSTRSET < 4

// Dispatchers
DISPATCHER(sumDouble)
DISPATCHER(avgDouble)
DISPATCHER(minDouble)
DISPATCHER(maxDouble)

#endif  // INSTRSET == 2

