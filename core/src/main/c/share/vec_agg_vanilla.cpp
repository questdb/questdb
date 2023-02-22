/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
    const double *lim = d + count;
    int64_t cnt = 0;
    for (; d < lim; d++) {
        const double v = *d;
        if (v == v) {
            cnt += 1;
        }
    }
    return cnt;
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

int64_t countLong_Vanilla(int64_t *pl, int64_t count) {
    const int64_t *lim = pl + count;
    int64_t cnt = 0;
    for (; pl < lim; pl++) {
        const int64_t l = *pl;
        cnt += (l != L_MIN);
    }
    return cnt;
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

extern "C" {

JNIEXPORT jdouble JNICALL
Java_io_questdb_std_Vect_avgIntAcc(JNIEnv *env, jclass cl, jlong pi, jlong count, jlong pCount) {
    auto *ppi = reinterpret_cast<int32_t *>(pi);
    double_t avg = 0;
    double_t c = 1;
    for (uint32_t i = 0; i < count; i++) {
        int32_t v = ppi[i];
        if (v != I_MIN) {
            avg += (v - avg) / c;
            ++c;
        }
    }
    *(reinterpret_cast<jlong *>(pCount)) = ((jlong) c - 1);
    return avg;
}

JNIEXPORT jdouble JNICALL
Java_io_questdb_std_Vect_avgLongAcc(JNIEnv *env, jclass cl, jlong pi, jlong count, jlong pCount) {
    auto *ppi = reinterpret_cast<int64_t *>(pi);
    double_t avg = 0;
    double_t c = 1;
    for (uint32_t i = 0; i < count; i++) {
        int64_t v = ppi[i];
        if (v != L_MIN) {
            avg += (v - avg) / c;
            ++c;
        }
    }
    *(reinterpret_cast<jlong *>(pCount)) = ((jlong) c - 1);
    return avg;
}

JNIEXPORT jdouble JNICALL
Java_io_questdb_std_Vect_avgDoubleAcc(JNIEnv *env, jclass cl, jlong pi, jlong count, jlong pCount) {
    auto *ppi = reinterpret_cast<double_t *>(pi);
    double_t avg = 0;
    double_t c = 1;
    for (uint32_t i = 0; i < count; i++) {
        double_t v = ppi[i];
        if (v == v) {
            avg += (v - avg) / c;
            ++c;
        }
    }
    *(reinterpret_cast<jlong *>(pCount)) = ((jlong) c - 1);
    return avg;
}
};
