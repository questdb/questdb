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

#ifndef QUESTDB_DISPATCHER_H
#define QUESTDB_DISPATCHER_H

#include "vec_dispatch.h"

#ifdef __aarch64__

#define DECLARE_DISPATCHER(FUNCNAME)
#define DECLARE_DISPATCHER_TYPE(FUNCNAME, ...)  void FUNCNAME(__VA_ARGS__);

#else // __aarch64__

#include "vcl/vectorclass.h"

#define DECLARE_DISPATCHER(FUNCNAME) TF_##FUNCNAME *FUNCNAME = dispatch_to_ptr(&F_AVX512(FUNCNAME), &F_AVX2(FUNCNAME), &F_SSE41(FUNCNAME), &F_VANILLA(FUNCNAME))

#define DECLARE_DISPATCHER_TYPE(FUNCNAME, ...) \
typedef void TF_ ## FUNCNAME(__VA_ARGS__);\
TF_ ## FUNCNAME F_AVX512(FUNCNAME), F_AVX2(FUNCNAME), F_SSE41(FUNCNAME), F_VANILLA(FUNCNAME)

template<typename T>
T *dispatch_to_ptr(T *avx512, T *avx2, T *sse4, T *vanilla) {
    const int iset = instrset_detect();
    if (iset == 10) return avx512;
    if (iset >= 8) return avx2;
    if (iset >= 5) return sse4;
    return vanilla;
}

#if INSTRSET == 10
#define MULTI_VERSION_NAME F_AVX512
#elif INSTRSET >= 8
#define MULTI_VERSION_NAME F_AVX2
#elif INSTRSET >= 5
#define MULTI_VERSION_NAME F_SSE41
#else
#define MULTI_VERSION_NAME F_VANILLA
#endif

#endif

#endif //QUESTDB_DISPATCHER_H
