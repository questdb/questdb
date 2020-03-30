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

#endif //VECT_H
