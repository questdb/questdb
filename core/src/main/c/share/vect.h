//
// Created by blues on 20/02/2020.
//

#ifndef VECT_H
#define VECT_H

#include <jni.h>
#include "vcl/vectorclass.h"
#include "vect_vanilla.h"

#define POINTER_NAME(func) func ## _pointer
#define F_AVX512(func) func ## _AVX512
#define F_AVX2(func) func ## _AVX2
#define F_SSE41(func) func ## _SSE41
#define F_SSE2(func) func ## _SSE2
#define F_VANILLA(func) func ## _Vanilla
#define F_DISPATCH(func) func ## _dispatch

typedef double DoubleVectFuncType(double *, long);

#define DOUBLE_DISPATCHER(func) \
\
DoubleVectFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
DoubleVectFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
double F_DISPATCH(func) (double *d, long count) { \
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
inline double func(double *d, long count) { \
return (*POINTER_NAME(func))(d, count); \
}\
\
extern "C" { \
JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pDouble, jlong size) { \
    return func((double *) pDouble, size); \
}\
\
}

typedef long IntLongVectFuncType(int *, long);

#define INT_LONG_DISPATCHER(func) \
\
IntLongVectFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
IntLongVectFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
long F_DISPATCH(func) (int *pi, long count) { \
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
inline long func(int *i, long count) { \
return (*POINTER_NAME(func))(i, count); \
}\
\
extern "C" { \
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pInt, jlong count) { \
    return func((int *) pInt, count); \
}\
\
}

typedef bool IntBoolVectFuncType(int *, int64_t);

#define INT_BOOL_DISPATCHER(func) \
\
IntBoolVectFuncType F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
IntBoolVectFuncType *POINTER_NAME(func) = &func ## _dispatch; \
\
bool F_DISPATCH(func) (int *pi, int64_t count) { \
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
inline bool func(int *i, int64_t count) { \
return (*POINTER_NAME(func))(i, count); \
}\
\
extern "C" { \
JNIEXPORT jboolean JNICALL Java_io_questdb_std_Vect_ ## func(JNIEnv *env, jclass cl, jlong pInt, jlong count) { \
    return func((int *) pInt, count); \
}\
\
}

#endif //VECT_H
