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

#include <jni.h>
#include <simdjson.h>
#include <limits>
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
         simdjson::ondemand::number_type number_type;

         template<class T>
         bool from(simdjson::simdjson_result<T> &res) {
             error = res.error();
             if (error != simdjson::error_code::SUCCESS) {
                 type = static_cast<simdjson::ondemand::json_type>(0);
                 number_type = static_cast<simdjson::ondemand::number_type>(0);
                 return false;
             }
             type = res.type().value_unsafe();
             if (type == simdjson::ondemand::json_type::number) {
                 number_type = res.get_number_type().value_unsafe();
             } else {
                 number_type = static_cast<simdjson::ondemand::number_type>(0);
             }
             return true;
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
static_assert(sizeof(simdjson::ondemand::number_type) == 4, "Unexpected size of simdjson::ondemand::number_type");
static_assert(sizeof(json_result) == 12, "Unexpected size of json_result");

static_assert(sizeof(size_t) == 8);
static_assert(sizeof(jlong) == 8);

constexpr std::byte BYTE_0x80 = std::byte(0x80); // 10000000
constexpr std::byte BYTE_0xC0 = std::byte(0xC0); // 11000000
constexpr std::byte BYTE_0xE0 = std::byte(0xE0); // 11100000
constexpr std::byte BYTE_0xF0 = std::byte(0xF0); // 11110000
constexpr std::byte BYTE_0xF8 = std::byte(0xF8); // 11111000

std::string_view trim(std::string_view str) {
    const char *first = str.begin();
    const char *const end = str.end();

    // Trim leading whitespace.
    for (;; ++first) {
        if (first == end) {
            return {};
        }
        if (!std::isspace(*first)) {
            break;
        }
    }

    // Trim trailing whitespace.
    const char *last = end - 1;
    for (; last > first && isspace(*last); --last);

    auto len = static_cast<size_t>(std::distance(first, last)) + 1;
    return {first, len};
}

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

inline bool copy_and_detect_multibyte_codepoint(std::byte *dest, const char *src, size_t len) {
    bool multibyte = false;
    std::size_t index = 0;
    for (; index < len; ++index) {
        const auto byte = static_cast<std::byte>(src[index]);
        if (byte >= std::byte{0x80}) {
            multibyte = true;
            break;
        }
        dest[index] = byte;
    }
    if (multibyte) {
        std::memcpy(dest + index, src + index, len - index);
    }
    return multibyte;
}

// Copy `src` to `dest` up to `max_dest_len` bytes.
// If `src` is longer than `max_dest_len`, copy up to the last UTF-8 character that fits.
// This function guarantees that there are no broken UTF-8 characters in the output.
static void
truncated_utf8_copy(
        questdb_byte_sink_t &dest,
        size_t max_dest_len,
        std::string_view src,
        bool src_is_ascii
) {
    if (max_dest_len == 0) [[unlikely]] {
        return;
    }
    const auto copy_len = std::min(src.length(), max_dest_len);
    if (src_is_ascii || !dest.ascii) [[likely]] {
        // Take the fast-path if either the input is guaranteed ascii,
        // or if the dest is already known to contain multibyte utf-8 characters.
        std::memcpy(dest.ptr, src.data(), copy_len);
    } else {
        dest.ascii = !copy_and_detect_multibyte_codepoint(dest.ptr, src.data(), copy_len);
    }
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
struct default_value;

template<>
struct default_value<token_void> {
    static constexpr token_void value = {};
};

template<>
struct default_value<jboolean> {
    static constexpr jboolean value = false;
};

template<>
struct default_value<jshort> {
    static constexpr jshort value = 0;
};

template<>
struct default_value<jint> {
    static constexpr jint value = std::numeric_limits<jint>::min();
};

template<>
struct default_value<jlong> {
    static constexpr jlong value = std::numeric_limits<jlong>::min();
};

template<>
struct default_value<jdouble> {
    // This should have the bit representation of 0x7ff8000000000000L which
    // Java uses at the sole representation of NaN.
    // See runtime check in `Java_io_questdb_std_json_SimdJsonParser_getSimdJsonPadding`.
    static constexpr jdouble value = std::numeric_limits<double>::quiet_NaN();
};

using simdjson_value = simdjson::simdjson_result<simdjson::ondemand::value>;

template<typename T, typename V, typename Int64ExtractorT, typename DoubleExtractorT, typename Uint64ExtractorT>
auto extract_numeric(
        json_result &result,
        V &res,
        Int64ExtractorT int64_extractor,
        Uint64ExtractorT uint64_extractor,
        DoubleExtractorT double_extractor
) -> T {
    switch (result.type) {
        case simdjson::ondemand::json_type::null:
            return default_value<T>::value;
        case simdjson::ondemand::json_type::boolean:
            return static_cast<T>(res.get_bool().value_unsafe());
        case simdjson::ondemand::json_type::number:
            switch (result.number_type) {
                case simdjson::ondemand::number_type::signed_integer:
                    return int64_extractor(res.get_int64().value_unsafe());
                case simdjson::ondemand::number_type::unsigned_integer:
                    return uint64_extractor(res.get_uint64().value_unsafe());
                case simdjson::ondemand::number_type::floating_point_number:
                case simdjson::ondemand::number_type::big_integer:
                    return double_extractor(res.get_double().value_unsafe());
            }
        case simdjson::ondemand::json_type::string: {
            auto maybe_int64 = res.get_int64_in_string();
            if (maybe_int64.error() == simdjson::error_code::SUCCESS) {
                return int64_extractor(maybe_int64.value_unsafe());
            }
            auto maybe_uint64 = res.get_uint64_in_string();
            if (maybe_uint64.error() == simdjson::error_code::SUCCESS) {
                return uint64_extractor(maybe_uint64.value_unsafe());
            }
            auto maybe_double = res.get_double_in_string();
            if (maybe_double.error() == simdjson::error_code::SUCCESS) {
                return double_extractor(maybe_double.value_unsafe());
            }
            return default_value<T>::value;
        }
        default:
            return default_value<T>::value;
    }
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
) -> decltype(std::forward<F>(extractor)(simdjson_value{})) {
    const simdjson::padded_string_view json_buf{json_chars, json_len, json_len + tail_padding};
    const std::string_view pointer{pointer_chars, pointer_len};
    auto doc = parser->iterate(json_buf);
    if ((doc.error() == simdjson::error_code::SUCCESS) && doc.is_scalar().value_unsafe()) {
        if (result->from(doc)) {
            return std::forward<F>(extractor)(std::move(doc));
        } else {
            return default_value<decltype(std::forward<F>(extractor)(simdjson_value{}))>::value;
        }
    }
    auto res = doc.at_pointer(pointer);
    if (!result->from(res)) {
        return default_value<decltype(std::forward<F>(extractor)(simdjson_value{}))>::value;
    }
    return std::forward<F>(extractor)(res);
}

inline simdjson::simdjson_result<std::string_view> get_raw_json(
        simdjson::simdjson_result<simdjson::ondemand::document> &doc
) {
    if (doc.error()) { return doc.error(); }
    return doc.value_unsafe().raw_json();
}

inline simdjson::simdjson_result<std::string_view> get_raw_json(
        simdjson::simdjson_result<simdjson::ondemand::value> &val
) {
    return val.raw_json();
}

template<typename V>
void extract_raw_json(
        V &res,
        bool json_is_ascii,
        questdb_byte_sink_t *dest_sink,
        int32_t max_size,
        json_result &result) {
    const auto max_size_st = static_cast<size_t>(max_size);
    auto raw_res = get_raw_json(res);
    if (!result.set_error(raw_res)) [[unlikely]] {
        return;
    }
    auto raw = trim(raw_res.value_unsafe());
    truncated_utf8_copy(*dest_sink, max_size_st, raw, json_is_ascii);
}

template<typename V>
jlong extract_value(
        V &res,
        questdb_byte_sink_t *dest_sink,
        int32_t max_size,
        json_result &result
) {
    if (!result.from(res)) [[unlikely]]{
        return 0;
    }
    // we are extracting value for either DATE, TIMESTAMP or IPV4 type
    // therefore the result is always ASCII
    switch (result.type) {
        case simdjson::ondemand::json_type::array:
        case simdjson::ondemand::json_type::object:
            extract_raw_json(res, true, dest_sink, max_size, result);
            return 0;
        case simdjson::ondemand::json_type::number: {
            switch (result.number_type) {
                case simdjson::ondemand::number_type::floating_point_number:
                    // this is how java converts double to long bits.
                    union {
                        jlong l;
                        double d;
                    } u;
                    u.d = (double) res.get_double().value_unsafe();
                    return u.l;
                case simdjson::ondemand::number_type::signed_integer:
                    return res.get_int64().value_unsafe();
                case simdjson::ondemand::number_type::unsigned_integer:
                    return static_cast<jlong>(res.get_uint64().value_unsafe());
                case simdjson::ondemand::number_type::big_integer:
                    extract_raw_json(res, true, dest_sink, max_size, result);
                    return 0;
            }
        }
        case simdjson::ondemand::json_type::string:
            truncated_utf8_copy(
                    *dest_sink,
                    static_cast<size_t>(max_size),
                    res.get_string().value_unsafe(),
                    true
            );
            return 0;
        case simdjson::ondemand::json_type::boolean:
            return res.get_bool().value_unsafe() ? 1 : 0;
        case simdjson::ondemand::json_type::null:
            return 0;
    }

    // unreachable
    return 0;
}

extern "C" {

JNIEXPORT jint JNICALL
Java_io_questdb_std_json_SimdJsonParser_getSimdJsonPadding(
        JNIEnv */*env*/,
        jclass /*cl*/
) {
    return simdjson::SIMDJSON_PADDING;
}

JNIEXPORT jstring JNICALL
Java_io_questdb_std_json_SimdJsonError_errorMessage(
        JNIEnv *env,
        jclass /*cl*/,
        jint code
) {
    auto msg = simdjson::error_message(static_cast<simdjson::error_code>(code));
    return env->NewStringUTF(msg);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_json_SimdJsonParser_convertJsonPathToPointer(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        const char *json_chars,
        size_t json_len,
        questdb_byte_sink_t *dest_sink
) {
    std::string_view json_path{json_chars, json_len};
    if (!json_path.empty() && json_path[0] == '$') {
        json_path.remove_prefix(1);
    }
    if (json_path.empty()) {
        return;
    }
    const auto json_pointer = simdjson::ondemand::json_path_to_pointer_conversion(json_path);
    auto dest = questdb_byte_sink_book(dest_sink, json_pointer.size());
    std::memcpy(dest, json_pointer.data(), json_pointer.size());
    dest_sink->ptr += json_pointer.size();
}

JNIEXPORT simdjson::ondemand::parser *JNICALL
Java_io_questdb_std_json_SimdJsonParser_create(
        JNIEnv * /*env*/,
        jclass /*cl*/
) {
    return new simdjson::ondemand::parser();
}

JNIEXPORT void JNICALL
Java_io_questdb_std_json_SimdJsonParser_destroy(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser
) {
    delete parser;
}

JNIEXPORT void JNICALL
Java_io_questdb_std_json_SimdJsonParser_queryPointerUtf8(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        jboolean json_is_ascii,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result,
        questdb_byte_sink_t *dest_sink,
        int32_t max_size
) {
    value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result, dest_sink, max_size, json_is_ascii](auto res) -> token_void {
                switch (result->type) {
                    case simdjson::ondemand::json_type::string:
                        truncated_utf8_copy(
                                *dest_sink,
                                static_cast<size_t>(max_size),
                                res.get_string().value_unsafe(),
                                json_is_ascii
                        );
                        return {};
                    case simdjson::ondemand::json_type::array:
                    case simdjson::ondemand::json_type::object:
                    case simdjson::ondemand::json_type::number:
                    case simdjson::ondemand::json_type::boolean:
                        extract_raw_json(res, json_is_ascii, dest_sink, max_size, *result);
                        return {};
                    case simdjson::ondemand::json_type::null:
                        return default_value<token_void>::value;
                }
                return default_value<token_void>::value;
            });
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_json_SimdJsonParser_queryPointerValue(
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
        int32_t max_size
) {
    // this method is used to query utf8 representation of either TIMESTAMP, DATE or IPV4.
    // all these values are ASCII
    const simdjson::padded_string_view json_buf{json_chars, json_len, json_len + tail_padding};
    const std::string_view pointer{pointer_chars, pointer_len};
    auto doc = parser->iterate(json_buf);
    if ((doc.error() == simdjson::error_code::SUCCESS) && doc.is_scalar().value_unsafe()) {
        return extract_value(doc, dest_sink, max_size, *result);
    }
    auto res = doc.at_pointer(pointer);
    return extract_value(res, dest_sink, max_size, *result);
}

JNIEXPORT jboolean JNICALL
Java_io_questdb_std_json_SimdJsonParser_queryPointerBoolean(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result
) {
    // we are extracting boolean value, it is definitely ASCII
    return value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result](auto res) -> jboolean {
                if (result->from(res) && result->type == simdjson::ondemand::json_type::boolean) [[likely]] {
                    return res.get_bool().value_unsafe();
                }
                return false;
            });
}

JNIEXPORT jshort JNICALL
Java_io_questdb_std_json_SimdJsonParser_queryPointerShort(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result
) {
    return value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result](auto res) -> jshort {
                return extract_numeric<jshort>(
                        *result, res,
                        [&result](int64_t value) -> jshort {
                            auto short_res = static_cast<jshort>(value);
                            if (value != short_res) [[unlikely]] {
                                result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                                return default_value<jshort>::value;
                            }
                            return short_res;
                        },
                        [&result](uint64_t value) -> jshort {
                            result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                            return default_value<jshort>::value;
                        },
                        [&result](double value) -> jshort {
                            if (value < std::numeric_limits<short>::min() ||
                                value > std::numeric_limits<short>::max()) [[unlikely]] {
                                result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                                return default_value<jshort>::value;
                            }
                            return static_cast<jshort>(value);
                        });
            });
}

JNIEXPORT jint JNICALL
Java_io_questdb_std_json_SimdJsonParser_queryPointerInt(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result
) {
    return value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result](auto res) -> jint {
                return extract_numeric<jint>(
                        *result, res,
                        [&result](int64_t value) -> jint {
                            auto int_res = static_cast<jint>(value);
                            if (value != int_res) [[unlikely]] {
                                result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                                return default_value<jint>::value;
                            }
                            return int_res;
                        },
                        [&result](uint64_t value) -> jint {
                            result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                            return default_value<jint>::value;
                        },
                        [&result](double value) -> jint {
                            if (value < std::numeric_limits<jint>::min() ||
                                value > std::numeric_limits<jint>::max()) [[unlikely]] {
                                result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                                return default_value<jint>::value;
                            }
                            return static_cast<jint>(value);
                        });
            });
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_json_SimdJsonParser_queryPointerLong(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result
) {
    return value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result](auto res) -> jlong {
                return extract_numeric<jlong>(
                        *result, res,
                        [](int64_t value) -> jlong {
                            return value;
                        },
                        [&result](uint64_t value) -> jlong {
                            result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                            return default_value<jlong>::value;
                        },
                        [&result](double value) -> jlong {
                            if (
                                    (value < static_cast<double>(std::numeric_limits<jlong>::min())) ||
                                    (value > static_cast<double>(std::numeric_limits<jlong>::max()))
                                    ) [[unlikely]] {
                                result->error = simdjson::error_code::NUMBER_OUT_OF_RANGE;
                                return default_value<jlong>::value;
                            }
                            return static_cast<jlong>(value);
                        });
            });
}

JNIEXPORT jdouble JNICALL
Java_io_questdb_std_json_SimdJsonParser_queryPointerDouble(
        JNIEnv * /*env*/,
        jclass /*cl*/,
        simdjson::ondemand::parser *parser,
        const char *json_chars,
        size_t json_len,
        size_t tail_padding,
        const char *pointer_chars,
        size_t pointer_len,
        json_result *result
) {
    return value_at_pointer(
            parser, json_chars, json_len, tail_padding, pointer_chars, pointer_len, result,
            [result](auto res) -> jdouble {
                return extract_numeric<jdouble>(
                        *result, res,
                        [](int64_t value) -> jdouble {
                            return static_cast<jdouble>(value);
                        },
                        [](uint64_t value) -> jdouble {
                            return static_cast<jdouble>(value);
                        },
                        [](double value) -> jdouble {
                            return value;
                        });
            });
}

} // extern "C"
