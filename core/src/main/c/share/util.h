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

#ifndef UTIL_H
#define UTIL_H
#include <cmath>

#if (defined(__GNUC__) && !defined(__clang__))
#define ATTRIBUTE_NEVER_INLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#define ATTRIBUTE_NEVER_INLINE __declspec(noinline)
#else
#define ATTRIBUTE_NEVER_INLINE
#endif

#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define PREDICT_TRUE(x) (__builtin_expect(false || (x), true))

constexpr jdouble D_MAX = std::numeric_limits<jdouble>::infinity();
constexpr jdouble D_MIN = -std::numeric_limits<jdouble>::infinity();
constexpr jint I_MAX = std::numeric_limits<jint>::max();
constexpr jint I_MIN = std::numeric_limits<jint>::min();
constexpr jlong L_MIN = std::numeric_limits<jlong>::min();
constexpr jlong L_MAX = std::numeric_limits<jlong>::max();
constexpr jdouble D_NAN = std::numeric_limits<jdouble>::quiet_NaN();

#endif //UTIL_H
