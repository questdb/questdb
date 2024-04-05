/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

#include "vec_agg.h"

#define MAX_VECTOR_SIZE 512

#if INSTRSET >= 10

//#define HOUR_COUNT F_AVX512(keyedHourCount)

#elif INSTRSET >= 8

//#define HOUR_COUNT F_AVX2(keyedHourCount)

#elif INSTRSET >= 5

//#define HOUR_COUNT F_SSE41(keyedHourCount)

#elif INSTRSET >= 2

//#define HOUR_COUNT F_SSE2(keyedHourCount)

#else

#endif


#if INSTRSET < 5

// Dispatchers
//ROSTI_DISPATCHER(keyedHourCount)

#define HOUR_MICROS  3600000000L
#define DAY_HOURS  24

extern "C" {



}

#endif
