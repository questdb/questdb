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

#include <cfloat>
#include <cstdint>
#include <cmath>
#include "util.h"
#include "vec_agg_vanilla.h"

int64_t countInt_Vanilla(int32_t *pi, int64_t count) {
    if (count == 0) {
        return 0;
    }
    int32_t *pi1 = pi;
    const int32_t *lim = pi1 + count;
    int64_t cnt = 0;
    for (; pi1 < lim; pi1++) {
        const int32_t i = *pi1;
        cnt += (i != I_MIN);
    }
    return cnt;
}

int64_t sumInt_Vanilla(int32_t *pi, int64_t count) {
    if (count == 0) {
        return L_MIN;
    }
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

double sumIntAcc_Vanilla(int32_t *pi, int64_t count, int64_t *accCount) {
    const int32_t *lim = pi + count;
    double sum = 0;
    int64_t c = 0;
    for (; pi < lim; pi++) {
        const int32_t i = *pi;
        if (i != I_MIN) {
            sum += (double) i;
            ++c;
        }
    }
    *accCount = c;
    return c > 0 ? sum : NAN;
}

int32_t minInt_Vanilla(int32_t *pi, int64_t count) {
    if (count == 0) {
        return I_MIN;
    }
    const int32_t *lim = pi + count;
    int32_t min = I_MIN;
    for (; pi < lim; pi++) {
        const int32_t i = *pi;
        if (i != I_MIN && (i < min || min == I_MIN)) {
            min = i;
        }
    }
    return min;
}

int32_t maxInt_Vanilla(int32_t *pi, int64_t count) {
    if (count == 0) {
        return I_MIN;
    }
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

int64_t countDouble_Vanilla(double *d, int64_t count) {
    if (count == 0) {
        return 0;
    }
    const double *lim = d + count;
    int64_t cnt = 0;
    for (; d < lim; d++) {
        const double v = *d;
        if (!std::isnan(v)) {
            cnt += 1;
        }
    }
    return cnt;
}

double sumDouble_Vanilla(double *d, int64_t count) {
    if (count == 0) {
        return NAN;
    }
    const double *lim = d + count;
    double sum = 0;
    bool hasData = false;
    for (; d < lim; d++) {
        const double v = *d;
        if (!std::isnan(v)) {
            sum += v;
            hasData = true;
        }
    }
    return hasData ? sum : NAN;
}

double sumDoubleAcc_Vanilla(double *d, int64_t count, int64_t *accCount) {
    const double *lim = d + count;
    double sum = 0;
    int64_t c = 0;
    for (; d < lim; d++) {
        double v = *d;
        if (!std::isnan(v)) {
            sum += v;
            ++c;
        }
    }
    *accCount = c;
    return c > 0 ? sum : NAN;
}

double sumDoubleKahan_Vanilla(double *d, int64_t count) {
    if (count == 0) {
        return NAN;
    }
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
    if (count == 0) {
        return NAN;
    }
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

double minDouble_Vanilla(double *d, int64_t count) {
    if (count == 0) {
        return NAN;
    }
    const double *ext = d + count;
    double min = D_MAX;
    double *pd = d;
    for (; pd < ext; pd++) {
        double x = *pd;
        if (!std::isnan(x) && x < min) {
            min = x;
        }
    }
    if (min < D_MAX) {
        return min;
    }
    return NAN;
}

double maxDouble_Vanilla(double *d, int64_t count) {
    if (count == 0) {
        return NAN;
    }
    const double *ext = d + count;
    double max = D_MIN;
    double *pd = d;
    for (; pd < ext; pd++) {
        double x = *pd;
        if (!std::isnan(x) && x > max) {
            max = x;
        }
    }
    if (max > D_MIN) {
        return max;
    }
    return NAN;
}

int64_t countLong_Vanilla(int64_t *pl, int64_t count) {
    if (count == 0) {
        return 0;
    }
    const int64_t *lim = pl + count;
    int64_t cnt = 0;
    for (; pl < lim; pl++) {
        const int64_t l = *pl;
        cnt += (l != L_MIN);
    }
    return cnt;
}

int64_t sumLong_Vanilla(int64_t *pl, int64_t count) {
    if (count == 0) {
        return L_MIN;
    }
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

double sumLongAcc_Vanilla(int64_t *pl, int64_t count, int64_t *accCount) {
    const int64_t *lim = pl + count;
    double sum = 0;
    int64_t c = 0;
    for (; pl < lim; pl++) {
        const int64_t l = *pl;
        if (l != L_MIN) {
            sum += (double) l;
            ++c;
        }
    }
    *accCount = c;
    return c > 0 ? sum : NAN;
}

int64_t minLong_Vanilla(int64_t *pl, int64_t count) {
    if (count == 0) {
        return L_MIN;
    }
    const int64_t *lim = pl + count;
    int64_t min = L_MIN;
    for (; pl < lim; pl++) {
        const int64_t l = *pl;
        if (l != L_MIN && (l < min || min == L_MIN)) {
            min = l;
        }
    }
    return min;
}

int64_t maxLong_Vanilla(int64_t *pl, int64_t count) {
    if (count == 0) {
        return L_MIN;
    }
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

int64_t sumShort_Vanilla(int16_t *ps, int64_t count) {
    if (count == 0) {
        return L_MIN;
    }
    const int16_t *lim = ps + count;
    int64_t sum = 0;
    for (; ps < lim; ps++) {
        const int16_t s = *ps;
        sum += s;
    }
    return sum;
}

int32_t minShort_Vanilla(int16_t *ps, int64_t count) {
    if (count == 0) {
        return I_MIN;
    }
    const int16_t *lim = ps + count;
    int32_t min = I_MAX;
    for (; ps < lim; ps++) {
        const int16_t s = *ps;
        if (s < min) {
            min = s;
        }
    }
    return min;
}

int32_t maxShort_Vanilla(int16_t *ps, int64_t count) {
    if (count == 0) {
        return I_MIN;
    }
    const int16_t *lim = ps + count;
    int32_t max = I_MIN;
    for (; ps < lim; ps++) {
        const int16_t s = *ps;
        if (s > max) {
            max = s;
        }
    }
    return max;
}
