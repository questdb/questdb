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
#include <cstdint>
#include <cmath>
#include "util.h"
#include "vec_agg_vanilla.h"

int64_t sumInt_Vanilla(int32_t *pi, int64_t count) {
    int32_t *pi1 = pi;
    const int32_t *lim = pi1 + count;
    int64_t sum = 0;
    bool hasData = false;
    for (; pi1 < lim; pi1++) {
        const int32_t i = *pi1;
        if (i != I_MIN) {
            sum += i;
            hasData = true;
        }
    }
    return hasData ? sum : L_MIN;
}

int32_t minInt_Vanilla(int32_t *pi, int64_t count) {
    const int32_t *lim = pi + count;
    int32_t min = I_MAX;
    bool hasData = false;
    for (; pi < lim; pi++) {
        const int32_t i = *pi;
        if (i != I_MIN && i < min) {
            min = i;
            hasData = true;
        }
    }
    return hasData ? min : I_MIN;
}

int32_t maxInt_Vanilla(int32_t *pi, int64_t count) {
    int32_t *pi1 = pi;
    const int32_t *lim = pi1 + count;
    int32_t max = I_MIN;
    for (; pi1 < lim; pi1++) {
        const int32_t i = *pi1;
        if (i > max) {
            max = i;
        }
    }
    return max;
}

double avgInt_Vanilla(int32_t *pi, int64_t count) {
    int32_t *pd = pi;
    const int32_t *lim = pd + count;
    int64_t sum = 0;
    int64_t sumCount = 0;
    for (; pd < lim; pd++) {
        const int32_t i = *pd;
        if (i != I_MIN) {
            sum += i;
            sumCount++;
        }
    }
    return (double) sum / sumCount;
}

double sumDouble_Vanilla(double *d, int64_t count) {
    const double *lim = d + count;
    double sum = 0;
    bool hasData = false;
    for (; d < lim; d++) {
        const double v = *d;
        if (v == v) {
            sum += v;
            hasData = true;
        }
    }
    return hasData ? sum : NAN;
}

double sumDoubleKahan_Vanilla(double *d, int64_t count) {
    const double *lim = d + count;
    double sum = 0;
    double c = 0;
    bool hasData = false;
    for (; d < lim; d++) {
        const double input = *d;
        if (input == input) {
            double y = input - c;
            double t = sum + y;
            c = (t - sum) - y;
            sum = t;
            hasData = true;
        }
    }
    return hasData ? sum : NAN;
}

double sumDoubleNeumaier_Vanilla(double *d, int64_t count) {
    const double *lim = d + count;
    double sum = 0;
    double c = 0;
    bool hasData = false;
    for (; d < lim; d++) {
        const double input = *d;
        if (input == input) {
            double t = sum + input;
            if (std::abs(sum) >= std::abs(input)) {
                c += (sum - t) + input;
            } else {
                c += (input - t) + sum;
            }
            sum = t;
            hasData = true;
        }
    }
    return hasData ? sum + c : NAN;
}

double avgDouble_Vanilla(double *d, int64_t count) {
    double *pd = d;
    const double *lim = pd + count;
    double sum = 0;
    int64_t sumCount = 0;
    for (; pd < lim; pd++) {
        const double d1 = *pd;
        if (d1 == d1) {
            sum += d1;
            sumCount++;
        }
    }
    return sum / sumCount;
}

double minDouble_Vanilla(double *d, int64_t count) {
    const double *ext = d + count;
    double min = LDBL_MAX;
    double *pd = d;
    bool hasData = false;
    for (; pd < ext; pd++) {
        double x = *pd;
        if (x < min) {
            min = x;
            hasData = true;
        }
    }
    return hasData ? min : NAN;
}

double maxDouble_Vanilla(double *d, int64_t count) {
    const double *ext = d + count;
    double max = LDBL_MIN;
    double *pd = d;
    bool hasData = false;
    for (; pd < ext; pd++) {
        double x = *pd;
        if (x > max) {
            max = x;
            hasData = true;
        }
    }
    return hasData ? max : NAN;
}

int64_t sumLong_Vanilla(int64_t *pl, int64_t count) {
    const int64_t *lim = pl + count;
    int64_t sum = 0;
    bool hasData = false;
    for (; pl < lim; pl++) {
        const int64_t l = *pl;
        if (l != L_MIN) {
            sum += l;
            hasData = true;
        }
    }
    return hasData ? sum : L_MIN;
}

int64_t minLong_Vanilla(int64_t *pl, int64_t count) {
    const int64_t *lim = pl + count;
    int64_t min = L_MAX;
    for (; pl < lim; pl++) {
        int64_t l = *pl;
        if (l != L_MIN && l < min) {
            min = l;
        }
    }
    // all null?
    return min == L_MAX ? L_MIN : min;
}

int64_t maxLong_Vanilla(int64_t *pl, int64_t count) {
    const int64_t *lim = pl + count;
    int64_t max = L_MIN;
    for (; pl < lim; pl++) {
        const int64_t l = *pl;
        if (l > max) {
            max = l;
        }
    }
    return max;
}

bool hasNull_Vanilla(int32_t *pi, int64_t count) {
    const int32_t *lim = pi + count;
    for (; pi < lim; pi++) {
        const int32_t i = *pi;
        if (i == I_MIN) {
            return true;
        }
    }
    return false;
}

double avgLong_Vanilla(int64_t *pl, int64_t count) {
    int64_t *p = pl;
    const int64_t *lim = p + count;
    int64_t sum = 0;
    int64_t sumCount = 0;
    for (; p < lim; p++) {
        const int64_t l = *p;
        if (l != L_MIN) {
            sum += l;
            sumCount++;
        }
    }
    return (double) sum / sumCount;
}
