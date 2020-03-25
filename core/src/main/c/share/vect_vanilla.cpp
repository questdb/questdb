#include <cfloat>

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

#include "vect_vanilla.h"

double sumDouble_Vanilla(double *d, long count) {
    return sum_nan_as_zero(d, count);
}

long sumInt_Vanilla(int *pi, long count) {
    return sum_nan_as_zero(pi, count);
}

double avgDouble_Vanilla(double *d, long count) {
    auto v = avg_skip_nan(d, count);
    return v.sum / v.count;
}

double minDouble_Vanilla(double *d, long count) {
    const double *ext = d + count;
    double min = LDBL_MAX;
    double *pd = d;
    for (; pd < ext; pd++) {
        double x = *pd;
        if (x < min) {
            min = x;
        }
    }
    return min;
}

double maxDouble_Vanilla(double *d, long count) {
    const double *ext = d + count;
    double max = LDBL_MIN;
    double *pd = d;
    for (; pd < ext; pd++) {
        double x = *pd;
        if (x > max) {
            max = x;
        }
    }
    return max;
}

bool hasNull_Vanilla(int *pi, long count) {
    const int *lim = pi + count;
    for (; pi < lim; pi++) {
        const int i = *pi;
        if (i == -1) {
            return true;
        }
    }
    return false;
}
