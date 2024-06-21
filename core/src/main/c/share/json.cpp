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

#if defined(SIMDJSON_THREADS_ENABLED)
#error "SIMDJSON_THREADS_ENABLED must not be defined"
#endif

#include <simdjson.h>
#include <limits>
#include <cmath>
#include "byte_sink.h"

static_assert(
        simdjson::SIMDJSON_VERSION_MAJOR == 3 &&
        simdjson::SIMDJSON_VERSION_MINOR == 9 &&
        simdjson::SIMDJSON_VERSION_REVISION == 4,
        "You've upgraded the simdjson dependency. "
        "Ensure that the error codes in JsonError are up to date, "
        "then update this expected version static assert.");

// See `JsonResult.java` for the `Unsafe` access to the fields.
PACK(class json_result {
     public:
         simdjson::error_code error;
         simdjson::ondemand::json_type type;

         void from(simdjson::simdjson_result<simdjson::ondemand::value> &res) {
             error = res.error();
             if (error != simdjson::error_code::SUCCESS) {
                 type = static_cast<simdjson::ondemand::json_type>(0);
                 return;
             }
             type = res.type().value_unsafe();
         }

         template<typename T>
         bool set_error(simdjson::simdjson_result<T> &res) {
             error = res.error();
             return error == simdjson::error_code::SUCCESS;
         }
     });

// Assertions to ensure that the offsets/size used `JsonResult.java` are correct.
static_assert(sizeof(simdjson::error_code) == 4, "Unexpected size of simdjson::error_code");
static_assert(sizeof(simdjson::ondemand::json_type) == 4, "Unexpected size of simdjson::ondemand::json_type");
static_assert(sizeof(json_result) == 8, "Unexpected size of json_result");

constexpr std::byte BYTE_0x80 = std::byte(0x80); // 10000000
constexpr std::byte BYTE_0xC0 = std::byte(0xC0); // 11000000
constexpr std::byte BYTE_0xE0 = std::byte(0xE0); // 11100000
constexpr std::byte BYTE_0xF0 = std::byte(0xF0); // 11110000
constexpr std::byte BYTE_0xF8 = std::byte(0xF8); // 11111000

static size_t utf8_char_size(std::byte first_byte) {
    size_t char_size = 0;
    if (first_byte < BYTE_0x80) {
        char_size = 1; // 1-byte character (ASCII)
    } else if ((first_byte & BYTE_0xE0) == BYTE_0xC0) {
        char_size = 2; // 2-byte character
    } else if ((first_byte & BYTE_0xF0) == BYTE_0xE0) {
        char_size = 3; // 3-byte character
    } else if ((first_byte & BYTE_0xF8) == BYTE_0xF0) {
        char_size = 4; // 4-byte character
    }
    return char_size;
}

std::byte *utf8_find_last_char_start(std::byte *end) {
    std::byte *last_utf8_start = end - 1;
    while ((*last_utf8_start & BYTE_0xC0) == BYTE_0x80) {
        --last_utf8_start;
    }
    return last_utf8_start;
}

// Copy `src` to `dest` up to `max_dest_len` bytes.
// If `src` is longer than `max_dest_len`, copy up to the last UTF-8 character that fits.
// This function guarantees that there are no broken UTF-8 characters in the output.
static void trimmed_utf8_copy(questdb_byte_sink_t &dest, std::string_view src, size_t max_dest_len) {
    if (max_dest_len == 0) {
        return;
    }
    const auto copy_len = std::min(src.length(), max_dest_len);
    std::memcpy(dest.ptr, src.data(), copy_len);
    std::byte *end_ptr = dest.ptr + copy_len;
    if (max_dest_len < src.length()) {
        std::byte *last_utf8_start = utf8_find_last_char_start(end_ptr);
        const size_t char_size = utf8_char_size(*last_utf8_start);
        if (end_ptr < last_utf8_start + char_size) {
            end_ptr = last_utf8_start;
        }
    }
    dest.ptr = end_ptr;
}

// To make the compiler happy when compiling `value_at_path`
// with a lambda that is supposed to return `void`.
struct token_void {
};

template<typename T>
struct logical_null;

template<>
struct logical_null<token_void> {
    static token_void value() { return {}; }
};

template<>
struct logical_null<jshort> {
    static jshort value() { return std::numeric_limits<jshort>::min(); }
};

template<>
struct logical_null<jint> {
    static jint value() { return std::numeric_limits<jint>::min(); }
};

template<>
struct logical_null<jlong> {
    static jlong value() { return std::numeric_limits<jlong>::min(); }
};

template<>
struct logical_null<jfloat> {
    // TODO: Is the C++ bit representation for quiet NaN the same as the Java one?
    static jfloat value() { return std::numeric_limits<float>::quiet_NaN(); }
};

template<>
struct logical_null<jdouble> {
    // TODO: Is the C++ bit representation for quiet NaN the same as the Java one?
    static jdouble value() { return std::numeric_limits<double>::quiet_NaN(); }
};

using json_value = simdjson::simdjson_result<simdjson::ondemand::value>;

auto get_int64(simdjson::ondemand::json_type ty, json_value &res) {
    if (ty == simdjson::ondemand::json_type::string) {
        return res.get_int64_in_string();
    }
    return res.get_int64();
}

auto get_double(simdjson::ondemand::json_type ty, json_value &res) {
    if (ty == simdjson::ondemand::json_type::string) {
        return res.get_double_in_string();
    }
    return res.get_double();
}

template<typename F>
auto value_at_pointer(
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result,
        F &&extractor
) -> decltype(std::forward<F>(extractor)(json_value{})) {
    const simdjson::padded_string_view json_buf{json_chars, json_len, json_len + tail_padding};
    const std::string_view pointer{pointer_chars, pointer_len};
    auto doc = parser->iterate(json_buf);
    auto res = doc.at_pointer(pointer);
    result->from(res);
    if (result->type == simdjson::ondemand::json_type::null) {
        return logical_null<decltype(std::forward<F>(extractor)(json_value{}))>::value();
    }
    return std::forward<F>(extractor)(res);
}

extern "C" {

JNIEXPORT jint JNICALL
Java_io_questdb_std_json_JsonParser_getSimdJsonPadding(
        JNIEnv */*env*/,
        jclass /*cl*/
) {
    return simdjson::SIMDJSON_PADDING;
}

JNIEXPORT jstring JNICALL
Java_io_questdb_std_json_JsonError_errorMessage(
        JNIEnv *env,
        jclass /*cl*/,
        jint code
) {
    auto msg = simdjson::error_message(static_cast<simdjson::error_code>(code));
    return env->NewStringUTF(msg);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_json_JsonParser_convertJsonPathToPointer(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        const char *json_chars,
        size_t json_len,
        questdb_byte_sink_t *dest_sink
) {
    const std::string_view json_path{json_chars, json_len};
    const auto json_pointer = simdjson::ondemand::json_path_to_pointer_conversion(json_path);
    auto dest = questdb_byte_sink_book(dest_sink, json_pointer.size());
    std::memcpy(dest, json_pointer.data(), json_pointer.size());
    dest_sink->ptr += json_pointer.size();
}

JNIEXPORT simdjson::ondemand::parser *JNICALL
Java_io_questdb_std_json_JsonParser_create(
        JNIEnv * /*env*/,
        jclass /*cl*/
) {
    return new simdjson::ondemand::parser();
}

JNIEXPORT void JNICALL
Java_io_questdb_std_json_JsonParser_destroy(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser
) {
    delete parser;
}

JNIEXPORT void JNICALL
Java_io_questdb_std_json_JsonParser_queryPointer(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result,
        questdb_byte_sink_t *dest_sink,
        int32_t max_size,
        const char *default_chars,
        size_t default_len
) {
    value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result, dest_sink, max_size, default_chars, default_len](json_value res) -> token_void {
                if (res.error() != simdjson::error_code::SUCCESS) {
                    if (default_chars != nullptr) {
                        const auto max_size_st = static_cast<size_t>(max_size);
                        trimmed_utf8_copy(*dest_sink, {default_chars, default_len}, max_size_st);
                    }
                    return logical_null<token_void>::value();
                }
                switch (result->type) {
                    case simdjson::ondemand::json_type::string: {
                        auto str_res = res.get_string();
                        const auto str = str_res.value_unsafe();
                        const auto max_size_st = static_cast<size_t>(max_size);
                        trimmed_utf8_copy(*dest_sink, str, max_size_st);
                    }
                        return {};
                    case simdjson::ondemand::json_type::array:
                    case simdjson::ondemand::json_type::object:
                    case simdjson::ondemand::json_type::number:
                    case simdjson::ondemand::json_type::boolean: {
                        const auto max_size_st = static_cast<size_t>(max_size);
                        auto raw_res = res.raw_json();
                        if (!result->set_error(raw_res)) {
                            return logical_null<token_void>::value();
                        }
                        auto raw = raw_res.value_unsafe();
                        trimmed_utf8_copy(*dest_sink, raw, max_size_st);
                    }
                        return {};
                    case simdjson::ondemand::json_type::null:
                        return logical_null<token_void>::value();
                }
                return logical_null<token_void>::value();
            });
}

JNIEXPORT void JNICALL
Java_io_questdb_std_json_JsonParser_queryPointerString(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result,
        questdb_byte_sink_t *dest_sink,
        int32_t max_size,
        const char *default_chars,
        size_t default_len
) {
    value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result, dest_sink, max_size, default_chars, default_len](json_value res) -> token_void {
                if (res.error() != simdjson::error_code::SUCCESS) {
                    if (default_chars != nullptr) {
                        const auto max_size_st = static_cast<size_t>(max_size);
                        trimmed_utf8_copy(*dest_sink, {default_chars, default_len}, max_size_st);
                    }
                    return logical_null<token_void>::value();
                }
                auto str_res = res.get_string();
                if (!result->set_error(str_res)) {
                    return logical_null<token_void>::value();
                }
                const auto str = str_res.value_unsafe();
                const auto max_size_st = static_cast<size_t>(max_size);
                trimmed_utf8_copy(*dest_sink, str, max_size_st);
                return {};
            });
}

JNIEXPORT jboolean JNICALL
Java_io_questdb_std_json_JsonParser_queryPointerBoolean(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result,
        jboolean default_value
) {
    // Can't reuse `value_at_pointer` because booleans don't have a value to represent nulls.
    const simdjson::padded_string_view json_buf{json_chars, json_len, json_len + tail_padding};
    const std::string_view pointer{pointer_chars, pointer_len};
    auto doc = parser->iterate(json_buf);
    auto res = doc.at_pointer(pointer);
    result->from(res);
    auto bool_res = res.get_bool();
    if (!result->set_error(bool_res)) {
        return default_value;
    }
    return bool_res.value_unsafe();
}

JNIEXPORT jshort JNICALL
Java_io_questdb_std_json_JsonParser_queryPointerShort(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result,
        jshort default_value
) {
    return value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result, default_value](json_value res) -> jshort {
                auto int_res = get_int64(result->type, res);
                if (!result->set_error(int_res)) {
                    return default_value;
                }
                const auto int_res64 = int_res.value_unsafe();
                const auto int_res16 = static_cast<int16_t>(int_res64);
                if (static_cast<int64_t>(int_res16) != int_res64) {
                    result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                    return default_value;
                }
                return int_res16;
            });
}

JNIEXPORT jint JNICALL
Java_io_questdb_std_json_JsonParser_queryPointerInt(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result,
        jint default_value
) {
    return value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result, default_value](json_value res) -> jint {
                auto int_res = get_int64(result->type, res);
                if (!result->set_error(int_res)) {
                    return default_value;
                }
                const auto int_res64 = int_res.value_unsafe();
                const auto int_res32 = static_cast<int32_t>(int_res64);
                if (static_cast<int64_t>(int_res32) != int_res64) {
                    result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                    return default_value;
                }
                return int_res32;
            });
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_json_JsonParser_queryPointerLong(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result,
        jlong default_value
) {
    return value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result, default_value](json_value res) -> jlong {
                auto int_res = get_int64(result->type, res);
                if (!result->set_error(int_res)) {
                    return default_value;
                }
                return int_res.value_unsafe();
            });
}

JNIEXPORT jdouble JNICALL
Java_io_questdb_std_json_JsonParser_queryPointerDouble(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result,
        jdouble default_value
) {
    return value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result, default_value](json_value res) -> jdouble {
                auto double_res = get_double(result->type, res);
                if (!result->set_error(double_res)) {
                    return default_value;
                }
                return double_res.value_unsafe();
            });
}

JNIEXPORT jfloat JNICALL
Java_io_questdb_std_json_JsonParser_queryPointerFloat(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result,
        jfloat default_value
) {
    return value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result, default_value](json_value res) -> jfloat {
                auto double_res = get_double(result->type, res);
                if (!result->set_error(double_res)) {
                    return default_value;
                }
                const auto value = double_res.value_unsafe();
                if (std::isnan(value)) {
                    return logical_null<jfloat>::value();
                }

                if (value < -std::numeric_limits<float>::max() || value > std::numeric_limits<float>::max()) {
                    result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                    return default_value;
                }
                return static_cast<float>(value);
            });
}

} // extern "C"
