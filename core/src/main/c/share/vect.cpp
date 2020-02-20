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

#include <cstdio>
#include "vcl/vectorclass.h"

#define MAX_VECTOR_SIZE 512

// Define function type
// Change this to fit your purpose. Should not contain vector types:
typedef double SumDoubleType(double *, long);

// function prototypes for each version
SumDoubleType sumDouble_SSE2, sumDouble_SSE41, sumDouble_AVX2, sumDouble_AVX512, sumDouble_dispatch;

// Define function name depending on which instruction set we compile for
#if INSTRSET >= 10                   // AVX512VL
#define SUM_DOUBLE sumDouble_AVX512
#elif INSTRSET >= 8                    // AVX2
#define SUM_DOUBLE sumDouble_AVX2
#elif INSTRSET >= 5                    // SSE4.1
#define SUM_DOUBLE sumDouble_SSE41
#elif INSTRSET >= 2
#define SUM_DOUBLE sumDouble_SSE2           // SSE2
#else

#endif

#ifdef SUM_DOUBLE

// Dispatched version of the function. Compile this once for each instruction set:
double SUM_DOUBLE(double *d, long count) {
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

#endif

#if INSTRSET < 4

double sumDouble_Vanilla(double *d, long count) {
    const double *ext = d + count;
    double result = 0;
    double *pd = d;
    for (; pd < ext; pd++) {
        result += *pd;
    }
    return result;
}

// make dispatcher in only the lowest of the compiled versions
// This function pointer initially points to the dispatcher.
// After the first call it points to the selected version:

SumDoubleType * sumDouble_pointer = &sumDouble_dispatch;

// Dispatcher
double sumDouble_dispatch(double * d, long size) {
    const int iset = instrset_detect();         // Detect supported instruction set
    if (iset >= 10) {
        sumDouble_pointer = &sumDouble_AVX512;  // AVX512 version
    }
    else if (iset >=  8) {
        sumDouble_pointer = &sumDouble_AVX2;    // AVX2 version
    }
    else if (iset >=  5) {
        sumDouble_pointer = &sumDouble_SSE41;   // SSE4.1 version
    }
    else if (iset >=  2) {
        sumDouble_pointer = &sumDouble_SSE2;    // SSE2 version
    }
    else {
        sumDouble_pointer = &sumDouble_Vanilla; // vanilla version
    }
    // continue in dispatched version of the function
    return (*sumDouble_pointer)(d, size);
}

// Entry to dispatched function call
inline double sumDouble(double *d, long size) {
    return (*sumDouble_pointer)(d, size);
}

extern "C" {

#include <jni.h>

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong size) {
    return sumDouble((double *) pDouble, size);
}

}

#endif  // INSTRSET == 2

