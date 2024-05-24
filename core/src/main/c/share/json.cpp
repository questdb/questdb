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

#include <jni.h>
#include <simdjson.h>
#include <utility>
#include "byte_sink.h"

static_assert(
        simdjson::SIMDJSON_VERSION_MAJOR == 3 &&
        simdjson::SIMDJSON_VERSION_MINOR == 9 &&
        simdjson::SIMDJSON_VERSION_REVISION == 1,
        "You've upgraded the simdjson dependency. "
        "Ensure that the error codes in JsonException are up to date, "
        "then update this expected version static assert.");

static bool bubble_error(JNIEnv* env, simdjson::error_code error) {
    if (error == simdjson::error_code::SUCCESS) {
        return true;
    }
    const auto exc_class = env->FindClass("io/questdb/std/json/JsonException");
    if (!exc_class) {
        return false;
    }
    const auto ctor = env->GetMethodID(exc_class, "<init>", "(I)V");
    if (!ctor) {
        return false;
    }
    const auto int_error = static_cast<jint>(error);
    jobject exc = env->NewObject(exc_class, ctor, int_error);
    if (!exc) {
        return false;
    }
    const auto as_throwable = static_cast<jthrowable>(exc);
    env->Throw(as_throwable);
    return false;
}

template <typename T>
bool bubble_error(JNIEnv* env, const simdjson::simdjson_result<T>& res) {
    return bubble_error(env, res.error());
}

static simdjson::padded_string_view maybe_copied_string_view(
        const char* s,
        size_t len,
        size_t capacity,
        std::string& temp_buffer
) {
    const size_t min_required_capacity = len + simdjson::SIMDJSON_PADDING;
    if (capacity >= min_required_capacity) {
        return simdjson::padded_string_view(s, len, capacity);
    }
    temp_buffer.reserve(min_required_capacity);
    temp_buffer.append(s, len);
    return simdjson::padded_string_view(temp_buffer);
}

// To make the compiler happy when compiling `value_at_path`
// with a lambda that is supposed to return `void`.
struct token_void {};

using json_value = simdjson::simdjson_result<simdjson::ondemand::value>;

template <typename F>
auto value_at_path(
        JNIEnv* env,
        const char *json_chars,
        size_t json_len,
        size_t json_capacity,
        const char *path_chars,
        size_t path_len,
        F&& extractor
) -> decltype(std::forward<F>(extractor)(json_value{})) {
    std::string temp_buffer;
    const simdjson::padded_string_view json_buf = maybe_copied_string_view(
            json_chars, json_len, json_capacity, temp_buffer);
    const std::string_view path{path_chars, path_len};
    simdjson::ondemand::parser parser;
    auto doc = parser.iterate(json_buf);
    auto res = doc.at_path(path);
    if (!bubble_error(env, res)) {
        return {};
    }
    return std::forward<F>(extractor)(res);
}

extern "C" {

JNIEXPORT jint JNICALL
Java_io_questdb_std_json_Json_getSimdJsonPadding(
        JNIEnv */*env*/,
        jclass /*cl*/
) {
    return simdjson::SIMDJSON_PADDING;
}

JNIEXPORT jstring JNICALL
Java_io_questdb_std_json_JsonException_errorMessage(
        JNIEnv* env,
        jclass /*cl*/,
        jint code
) {
    auto msg = simdjson::error_message(static_cast<simdjson::error_code>(code));
    return env->NewStringUTF(msg);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_json_Json_validate(
        JNIEnv* env,
        jclass /*cl*/,
        const char* jsonChars,
        size_t jsonLen,
        size_t jsonCapacity
) {
    std::string tempBuffer;
    const simdjson::padded_string_view jsonBuf = maybe_copied_string_view(
            jsonChars, jsonLen, jsonCapacity, tempBuffer);
    simdjson::dom::parser parser;
    auto dom = parser.parse(jsonBuf);
    bubble_error(env, dom);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_json_Json_queryPathString(
        JNIEnv* env,
        jclass /*cl*/,
        const char* json_chars,
        size_t json_len,
        size_t json_capacity,
        const char* path_chars,
        size_t path_len,
        questdb_byte_sink_t* dest_sink
) {
    value_at_path(
            env, json_chars, json_len, json_capacity, path_chars, path_len,
            [env, dest_sink](json_value res) -> token_void {
                auto str_res = res.get_string();
                if (!bubble_error(env, str_res)) {
                    return {};
                }
                const auto str = str_res.value_unsafe();
                const auto dest = questdb_byte_sink_book(dest_sink, str.length());
                if (dest == nullptr) {
                    bubble_error(env, simdjson::error_code::MEMALLOC);
                }
                memcpy(dest, str.data(), str.length());
                dest_sink->ptr += str.length();
                return {};
            });
}

JNIEXPORT jboolean JNICALL
Java_io_questdb_std_json_Json_queryPathBoolean(
        JNIEnv* env,
        jclass /*cl*/,
        const char* json_chars,
        size_t json_len,
        size_t json_capacity,
        const char* path_chars,
        size_t path_len
) {
    return value_at_path(
            env, json_chars, json_len, json_capacity, path_chars, path_len,
            [env](json_value res) -> jboolean {
                auto bool_res = res.get_bool();
                if (!bubble_error(env, bool_res)) {
                    return false;
                }
                return bool_res.value_unsafe();
            });
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_json_Json_queryPathLong(
        JNIEnv* env,
        jclass /*cl*/,
        const char* json_chars,
        size_t json_len,
        size_t json_capacity,
        const char* path_chars,
        size_t path_len
) {
    return value_at_path(
            env, json_chars, json_len, json_capacity, path_chars, path_len,
            [env](json_value res) -> jlong {
                auto int_res = res.get_int64();
                if (!bubble_error(env, int_res)) {
                    return {};
                }
                return int_res.value_unsafe();
            });
}

JNIEXPORT jdouble JNICALL
Java_io_questdb_std_json_Json_queryPathDouble(
        JNIEnv* env,
        jclass /*cl*/,
        const char* json_chars,
        size_t json_len,
        size_t json_capacity,
        const char* path_chars,
        size_t path_len
) {
    return value_at_path(
            env, json_chars, json_len, json_capacity, path_chars, path_len,
            [env](json_value res) -> jdouble {
                auto double_res = res.get_double();
                if (!bubble_error(env, double_res)) {
                    return {};
                }
                return double_res.value_unsafe();
            });
}

} // extern "C"