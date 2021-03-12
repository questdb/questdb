//
// Created by blues on 16/06/2020.
//

#ifndef VEC_TS_AGG_H
#define VEC_TS_AGG_H

#include "vec_dispatch.h"
#include "rosti.h"

typedef void RostiCount(rosti_t *map, int64_t *p_micros, int64_t count, int32_t valueOffset);

#define ROSTI_DISPATCHER(func) \
\
RostiCount F_SSE2(func), F_SSE41(func), F_AVX2(func), F_AVX512(func), F_DISPATCH(func); \
\
RostiCount *POINTER_NAME(func) = &func ## _dispatch; \
\
void F_DISPATCH(func) (rosti_t *map, int64_t *p_micros, int64_t count, int32_t valueOffset) { \
    const int iset = instrset_detect();  \
    if (iset >= 10) { \
        POINTER_NAME(func) = &F_AVX512(func); \
    } else if (iset >= 8) { \
        POINTER_NAME(func) = &F_AVX2(func); \
    } else if (iset >= 5) { \
        POINTER_NAME(func) = &F_SSE41(func); \
    } else {\
        POINTER_NAME(func) = &F_SSE2(func); \
    }\
    (*POINTER_NAME(func))(map, p_micros, count, valueOffset); \
} \
\
void func(rosti_t *map, int64_t *p_micros, int64_t count, int32_t valueOffset) { \
    (*POINTER_NAME(func))(map, p_micros, count, valueOffset); \
}\
extern "C" { \
JNIEXPORT void JNICALL Java_io_questdb_std_Rosti_ ## func(JNIEnv *env, jclass cl, jlong pRosti, jlong pKeys, jlong count, jint valueOffset) { \
    auto map = reinterpret_cast<rosti_t *>(pRosti); \
    auto *p_micros = reinterpret_cast<int64_t *>(pKeys); \
    func(map, p_micros, count, valueOffset); \
}\
\
}

#endif //VEC_TS_AGG_H
