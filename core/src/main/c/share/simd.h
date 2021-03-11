//
// Created by alpel on 05/03/2021.
//

#ifndef QDB_SIMD_H
#define QDB_SIMD_H

#include "func_dispatcher.h"

#ifdef ENABLE_MULTIVERSION
#define __SIMD_MULTIVERSION__ __attribute__((target_clones("avx2","avx","avx512f","default")))
#else
#define __SIMD_MULTIVERSION__
#endif

#ifdef ENABLE_ASMLIB
#include "asmlib/asmlib.h"
#define __MEMCPY A_memcpy
#else
#ifdef ENABLE_MANUAL_MEMCPY
#define __MEMCPY man_memcpy
#else
#define __MEMCPY std::memcpy
#endif
#endif

#define POINTER_NAME(func) func ## _pointer
#define MAN_F_AVX512(func) func ## 10
#define MAN_F_AVX2(func) func ## 8
#define MAN_F_SSE41(func) func ## 5
#define MAN_F_SSE2(func) func ## 2
#define MAN_F_VANILLA(func) func
#define MAN_F_DISPATCH(func) func ## _dispatch

#define MAN_DISPATCHER(func, T) \
\
decltype(std::declval<func>()) MAN_F_SSE2(func), MAN_F_SSE41(func), MAN_F_AVX2(func), MAN_F_AVX512(func); \
\
//rettype MAN_F_DISPATCH(func)##args{ \
//    const int iset = instrset_detect();  \
//    if (iset >= 10) { \
//        POINTER_NAME(func) = &MAN_F_AVX512(func); \
//    } else if (iset >= 8) { \
//        POINTER_NAME(func) = &MAN_F_AVX2(func); \
//    } else if (iset >= 5) { \
//        POINTER_NAME(func) = &MAN_F_SSE41(func); \
//    } else if (iset >= 2) { \
//        POINTER_NAME(func) = &MAN_F_SSE2(func); \
//    } else { \
//        POINTER_NAME(func) = &MAN_F_VANILLA(func); \
//    }\
//    return (*POINTER_NAME(func))##parms; \
//}                                                  \
//\
//FuncType##func *POINTER_NAME(func) = &MAN_F_DISPATCH(func); \
//\
//inline\
//rettype func_mv(JNIEnv *env, jclass cl, ##args) { \
//    return (*POINTER_NAME(func))##parms; \
//}\
}


#endif //QDB_SIMD_H
