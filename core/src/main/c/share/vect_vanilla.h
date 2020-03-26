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

#ifndef VECT_VANILLA_H
#define VECT_VANILLA_H

#include <climits>

static inline long sum_nan_as_zero(int *pi, long count) {
    const int *lim = pi + count;
    long sum = 0;
    for (; pi < lim; pi++) {
        const int i = *pi;
        if (i != INT_MAX) {
            sum += i;
        }
    }
    return sum;
}

static inline double sum_nan_as_zero(double *pd, long count) {
    const double *lim = pd + count;
    double sum = 0;
    for (; pd < lim; pd++) {
        const double d = *pd;
        if (d == d) {
            sum += d;
        }
    }
    return sum;
}

typedef struct _AVG_DOUBLE {
    double sum;
    long count;
} AVG_DOUBLE;

static inline AVG_DOUBLE avg_skip_nan(double *pd, long count) {
    const double *lim = pd + count;
    double sum = 0;
    long sumCount = 0;
    for (; pd < lim; pd++) {
        const double d = *pd;
        if (d == d) {
            sum += d;
            sumCount++;
        }
    }

    AVG_DOUBLE ad;
    ad.sum = sum;
    ad.count = sumCount;
    return ad;
}

double sumDouble_Vanilla(double *d, long count);

double avgDouble_Vanilla(double *d, long count);

double minDouble_Vanilla(double *d, long count);

double maxDouble_Vanilla(double *d, long count);

long sumInt_Vanilla(int *pi, long count);

bool hasNull_Vanilla(int32_t *pi, int64_t count);

#endif //VECT_VANILLA_H
