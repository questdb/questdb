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

#ifndef VECT_H
#define VECT_H

#include <jni.h>
#include "vcl/vectorclass.h"
#include "vec_agg_vanilla.h"
#include "vec_dispatch.h"

typedef double DoubleVecFuncType(double *, int64_t);

#define DOUBLE_DISPATCHER(func) \
\
DoubleVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
DoubleVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
double F_DISPATCH(func) (double *d, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(d, count); \
} \
\
inline double func(double *d, int64_t count) { \
    return (*POINTER_NAME(func))(d, count); \
}\
\
extern "C" { \
JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pDouble, jlong size) { \
    return func((double *) pDouble, size); \
}\
\
}

typedef double DoubleAccVecFuncType(double *, int64_t, int64_t *);

#define DOUBLE_ACC_DISPATCHER(func) \
\
DoubleAccVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
DoubleAccVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
double F_DISPATCH(func) (double *d, int64_t count, int64_t *accCount) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(d, count, accCount); \
} \
\
inline double func(double *d, int64_t count, int64_t *accCount) { \
    return (*POINTER_NAME(func))(d, count, accCount); \
}\
\
extern "C" { \
JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pDouble, jlong size, jlong pAccCount) { \
    return func((double *) pDouble, size, (int64_t *) pAccCount); \
}\
\
}

typedef int64_t DoubleLongVecFuncType(double *, int64_t);

#define DOUBLE_LONG_DISPATCHER(func) \
\
DoubleLongVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
DoubleLongVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int64_t F_DISPATCH(func) (double *d, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(d, count); \
} \
\
inline int64_t func(double *d, int64_t count) { \
    return (*POINTER_NAME(func))(d, count); \
}\
\
extern "C" { \
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pDouble, jlong size) { \
    return func((double *) pDouble, size); \
}\
\
}

typedef int64_t IntLongVecFuncType(int32_t *, int64_t);

#define INT_LONG_DISPATCHER(func) \
\
IntLongVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
IntLongVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int64_t F_DISPATCH(func) (int32_t *pi, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pi, count); \
} \
\
inline int64_t func(int32_t *i, int64_t count) { \
    return (*POINTER_NAME(func))(i, count); \
}\
\
extern "C" { \
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pInt, jlong count) { \
    return func((int32_t *) pInt, count); \
}\
\
}

typedef double IntLongAccVecFuncType(int32_t *, int64_t, int64_t *);

#define INT_LONG_ACC_DISPATCHER(func) \
\
IntLongAccVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
IntLongAccVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
double F_DISPATCH(func) (int32_t *pi, int64_t count, int64_t *accCount) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pi, count, accCount); \
} \
\
inline double func(int32_t *pi, int64_t count, int64_t *accCount) { \
    return (*POINTER_NAME(func))(pi, count, accCount); \
}\
\
extern "C" { \
JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pInt, jlong count, jlong pAccCount) { \
    return func((int32_t *) pInt, count, (int64_t *) pAccCount); \
}\
\
}

typedef double IntDoubleVecFuncType(int32_t *, int64_t);

#define INT_DOUBLE_DISPATCHER(func) \
\
IntDoubleVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
IntDoubleVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
double F_DISPATCH(func) (int32_t *pi, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pi, count); \
} \
\
inline double func(int32_t *i, int64_t count) { \
    return (*POINTER_NAME(func))(i, count); \
}\
\
extern "C" { \
JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pInt, jlong count) { \
    return func((int32_t *) pInt, count); \
}\
\
}

typedef int32_t IntIntVecFuncType(int32_t *, int64_t);

#define INT_INT_DISPATCHER(func) \
\
IntIntVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
IntIntVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int32_t F_DISPATCH(func) (int32_t *pi, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pi, count); \
} \
\
inline int32_t func(int32_t *i, int64_t count) { \
    return (*POINTER_NAME(func))(i, count); \
}\
\
extern "C" { \
JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pInt, jlong count) { \
    return func((int32_t *) pInt, count); \
}\
\
}

typedef int64_t LongLongVecFuncType(int64_t *, int64_t);

#define LONG_LONG_DISPATCHER(func) \
\
LongLongVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
LongLongVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int64_t F_DISPATCH(func) (int64_t *pi, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pi, count); \
} \
\
inline int64_t func(int64_t *pl, int64_t count) { \
    return (*POINTER_NAME(func))(pl, count); \
}\
\
extern "C" { \
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pLong, jlong count) { \
    return func((int64_t *) pLong, count); \
}\
\
}

typedef double LongLongAccVecFuncType(int64_t *, int64_t, int64_t *);

#define LONG_LONG_ACC_DISPATCHER(func) \
\
LongLongAccVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
LongLongAccVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
double F_DISPATCH(func) (int64_t *pl, int64_t count, int64_t *accCount) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pl, count, accCount); \
} \
\
inline double func(int64_t *pl, int64_t count, int64_t *accCount) { \
    return (*POINTER_NAME(func))(pl, count, accCount); \
}\
\
extern "C" { \
JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pLong, jlong count, jlong pAccCount) { \
    return func((int64_t *) pLong, count, (int64_t *) pAccCount); \
}\
\
}

typedef int64_t LongShortVecFuncType(int16_t *, int64_t);

#define LONG_SHORT_DISPATCHER(func) \
\
LongShortVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
LongShortVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int64_t F_DISPATCH(func) (int16_t *pi, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pi, count); \
} \
\
inline int64_t func(int16_t *pl, int64_t count) { \
    return (*POINTER_NAME(func))(pl, count); \
}\
\
extern "C" { \
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pLong, jlong count) { \
    return func((int16_t *) pLong, count); \
}\
\
}

typedef double LongDoubleVecFuncType(int64_t *, int64_t);

#define LONG_DOUBLE_DISPATCHER(func) \
\
LongDoubleVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
LongDoubleVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
double F_DISPATCH(func) (int64_t *pi, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pi, count); \
} \
\
inline double func(int64_t *pl, int64_t count) { \
    return (*POINTER_NAME(func))(pl, count); \
}\
\
extern "C" { \
JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pLong, jlong count) { \
    return func((int64_t *) pLong, count); \
}\
\
}

typedef bool IntBoolVectFuncType(int32_t *, int64_t);

#define INT_BOOL_DISPATCHER(func) \
\
IntBoolVectFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
IntBoolVectFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
bool F_DISPATCH(func) (int32_t *pi, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pi, count); \
} \
\
inline bool func(int32_t *i, int64_t count) { \
    return (*POINTER_NAME(func))(i, count); \
}\
\
extern "C" { \
JNIEXPORT jboolean JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pInt, jlong count) { \
    return func((int32_t *) pInt, count); \
}\
\
}

typedef int64_t ShortLongVecFuncType(int16_t *, int64_t);

#define SHORT_LONG_DISPATCHER(func) \
\
ShortLongVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
ShortLongVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int64_t F_DISPATCH(func) (int16_t *ps, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(ps, count); \
} \
\
inline int64_t func(int16_t *ps, int64_t count) { \
    return (*POINTER_NAME(func))(ps, count); \
}\
\
extern "C" { \
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pShort, jlong count) { \
    return func((int16_t *) pShort, count); \
}\
\
}

typedef int32_t ShortIntVecFuncType(int16_t *, int64_t);

#define SHORT_INT_DISPATCHER(func) \
\
ShortIntVecFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
ShortIntVecFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int32_t F_DISPATCH(func) (int16_t *ps, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(ps, count); \
} \
\
inline int32_t func(int16_t *ps, int64_t count) { \
    return (*POINTER_NAME(func))(ps, count); \
}\
\
extern "C" { \
JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pShort, jlong count) { \
    return func((int16_t *) pShort, count); \
}\
\
}

// Bitmap-null dispatcher macros.
// Signature: (data_ptr, bitmap_ptr, bitOffset, count) -> result
// The bitmap_ptr and bitOffset identify null bits: bit=1 means null.

typedef int64_t ShortLongBitmapNullFuncType(int16_t *, uint8_t *, int64_t, int64_t);

#define SHORT_LONG_BITMAP_NULL_DISPATCHER(func) \
\
ShortLongBitmapNullFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
ShortLongBitmapNullFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int64_t F_DISPATCH(func) (int16_t *ps, uint8_t *bitmap, int64_t bitOffset, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(ps, bitmap, bitOffset, count); \
} \
\
inline int64_t func(int16_t *ps, uint8_t *bitmap, int64_t bitOffset, int64_t count) { \
    return (*POINTER_NAME(func))(ps, bitmap, bitOffset, count); \
}\
\
extern "C" { \
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pShort, jlong pBitmap, jlong bitOffset, jlong count) { \
    return func((int16_t *) pShort, (uint8_t *) pBitmap, bitOffset, count); \
}\
}

typedef int32_t ShortIntBitmapNullFuncType(int16_t *, uint8_t *, int64_t, int64_t);

#define SHORT_INT_BITMAP_NULL_DISPATCHER(func) \
\
ShortIntBitmapNullFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
ShortIntBitmapNullFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int32_t F_DISPATCH(func) (int16_t *ps, uint8_t *bitmap, int64_t bitOffset, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(ps, bitmap, bitOffset, count); \
} \
\
inline int32_t func(int16_t *ps, uint8_t *bitmap, int64_t bitOffset, int64_t count) { \
    return (*POINTER_NAME(func))(ps, bitmap, bitOffset, count); \
}\
\
extern "C" { \
JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pShort, jlong pBitmap, jlong bitOffset, jlong count) { \
    return func((int16_t *) pShort, (uint8_t *) pBitmap, bitOffset, count); \
}\
}

typedef int64_t IntLongBitmapNullFuncType(int32_t *, uint8_t *, int64_t, int64_t);

#define INT_LONG_BITMAP_NULL_DISPATCHER(func) \
\
IntLongBitmapNullFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
IntLongBitmapNullFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int64_t F_DISPATCH(func) (int32_t *pi, uint8_t *bitmap, int64_t bitOffset, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pi, bitmap, bitOffset, count); \
} \
\
inline int64_t func(int32_t *pi, uint8_t *bitmap, int64_t bitOffset, int64_t count) { \
    return (*POINTER_NAME(func))(pi, bitmap, bitOffset, count); \
}\
\
extern "C" { \
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pInt, jlong pBitmap, jlong bitOffset, jlong count) { \
    return func((int32_t *) pInt, (uint8_t *) pBitmap, bitOffset, count); \
}\
}

typedef int64_t LongLongBitmapNullFuncType(int64_t *, uint8_t *, int64_t, int64_t);

#define LONG_LONG_BITMAP_NULL_DISPATCHER(func) \
\
LongLongBitmapNullFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
LongLongBitmapNullFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int64_t F_DISPATCH(func) (int64_t *pl, uint8_t *bitmap, int64_t bitOffset, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(pl, bitmap, bitOffset, count); \
} \
\
inline int64_t func(int64_t *pl, uint8_t *bitmap, int64_t bitOffset, int64_t count) { \
    return (*POINTER_NAME(func))(pl, bitmap, bitOffset, count); \
}\
\
extern "C" { \
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pLong, jlong pBitmap, jlong bitOffset, jlong count) { \
    return func((int64_t *) pLong, (uint8_t *) pBitmap, bitOffset, count); \
}\
}

typedef int64_t BitmapCountFuncType(uint8_t *, int64_t, int64_t);

#define BITMAP_COUNT_DISPATCHER(func) \
\
BitmapCountFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
BitmapCountFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
int64_t F_DISPATCH(func) (uint8_t *bitmap, int64_t bitOffset, int64_t count) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else if (iset >= 2) { \
        POINTER_NAME(func) = &F_SSE2(func); \
    } else { \
        POINTER_NAME(func) = &F_VANILLA(func); \
    }\
    return (*POINTER_NAME(func))(bitmap, bitOffset, count); \
} \
\
inline int64_t func(uint8_t *bitmap, int64_t bitOffset, int64_t count) { \
    return (*POINTER_NAME(func))(bitmap, bitOffset, count); \
}\
\
extern "C" { \
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pBitmap, jlong bitOffset, jlong count) { \
    return func((uint8_t *) pBitmap, bitOffset, count); \
}\
}

#endif //VECT_H
