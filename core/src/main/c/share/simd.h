//
// Created by alpel on 05/03/2021.
//

#ifndef ZLIB_SIMD_H
#define ZLIB_SIMD_H

#ifdef ENABLE_MULTIVERSION
#define __SIMD_MULTIVERSION__ __attribute__((target_clones("avx2","avx","sse4.1","default")))
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
#define __MEMCPY memcpy
#endif
#endif





#endif //ZLIB_SIMD_H
