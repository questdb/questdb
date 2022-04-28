/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

#include <jni.h>
#include <string_view>
#include <cstdint>

#if UINTPTR_MAX == 0xffffffffffffffff
#    define TARGET_64
#elif UINTPTR_MAX == 0xffffffff
#    define TARGET_32
#else
#    error Can not determine if compiling for 32-bit or 64-bit target.
#endif

extern "C" {

JNIEXPORT jint JNICALL Java_io_questdb_cutlass_msgpack_BinView_blobHashCode(JNIEnv *env, jclass cl, jlong lo, jlong hi) {
    const auto begin = reinterpret_cast<const char*>(lo);
    const auto end = reinterpret_cast<const char*>(hi);
    const size_t count = end - begin;
    const std::string_view sv{begin, count};
    const size_t hash = std::hash<std::string_view>{}(sv);
    
#ifdef TARGET_32
    static_assert(sizeof(jint) == sizeof(size_t));
    return static_cast<jint>(hash);
#else  // TARGET_64
    jint upper = static_cast<jint>(hash >> 32);
    jint lower = static_cast<jint>(hash);
    return upper ^ lower;
#endif
}

}
