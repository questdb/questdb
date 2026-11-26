/*+*****************************************************************************
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
#ifndef CONVERTERS_H
#define CONVERTERS_H

#include <cassert>
#include <type_traits>
#include <cmath>
#include <cstdint>
#include "jni.h"
#include "column_type.h"

/**
 * ColumnType enum, matching the Java definitions.
 */


/**
 * Packs the column types into an int64_t, so we can use a single switch over both enum values.
 * @param a the src column type
 * @param b the dst column type
 * @return packed value
 */
constexpr int64_t pack_column_types(ColumnType a, ColumnType b) {
    return static_cast<int64_t>(a) << 32 | (static_cast<int64_t>(b) & 0xffffffffL);
}

enum class ConversionError {
    NONE = 0,
    UNSUPPORTED_CAST = 1,
};

#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch"

/**
 * Whether the fixed-to-fixed conversion table converts values of type t: exactly the types with an
 * EnumTypeMap specialisation below. The switch names every tag and -Wswitch is an error here, so a
 * new tag must be classified before the build passes.
 */
constexpr bool is_fixed_convertible(ColumnType t) {
    switch (t) {
        case ColumnType::BOOLEAN:
        case ColumnType::BYTE:
        case ColumnType::SHORT:
        case ColumnType::INT:
        case ColumnType::LONG:
        case ColumnType::FLOAT:
        case ColumnType::DOUBLE:
        case ColumnType::TIMESTAMP_MICRO:
        case ColumnType::TIMESTAMP_NANO:
        case ColumnType::DATE:
            return true;
        case ColumnType::UNDEFINED:
        case ColumnType::CHAR:
        case ColumnType::STRING:
        case ColumnType::SYMBOL:
        case ColumnType::LONG256:
        case ColumnType::GEOBYTE:
        case ColumnType::GEOSHORT:
        case ColumnType::GEOINT:
        case ColumnType::GEOLONG:
        case ColumnType::BINARY:
        case ColumnType::UUID:
        case ColumnType::CURSOR:
        case ColumnType::VAR_ARG:
        case ColumnType::RECORD:
        case ColumnType::GEOHASH:
        case ColumnType::LONG128:
        case ColumnType::IPV4:
        case ColumnType::VARCHAR:
        case ColumnType::ARRAY:
        case ColumnType::DECIMAL8:
        case ColumnType::DECIMAL16:
        case ColumnType::DECIMAL32:
        case ColumnType::DECIMAL64:
        case ColumnType::DECIMAL128:
        case ColumnType::DECIMAL256:
        case ColumnType::DECIMAL:
        case ColumnType::REGCLASS:
        case ColumnType::REGPROCEDURE:
        case ColumnType::ARRAY_STRING:
        case ColumnType::PARAMETER:
        case ColumnType::INTERVAL:
        case ColumnType::VARCHAR_SLICE:
        case ColumnType::NULL_:
            return false;
    }
    return false;
}

#pragma GCC diagnostic pop

/**
 * The storage type and NULL of a type the conversion table converts. The primary template has
 * no definition: a table row for a type without a specialisation stops the build, instead of
 * reading the values as bytes.
 */
template <ColumnType C> struct EnumTypeMap;

template<>
struct EnumTypeMap<ColumnType::BOOLEAN> {
    static_assert(is_fixed_convertible(ColumnType::BOOLEAN), "EnumTypeMap specialisation for a type is_fixed_convertible does not list");
    using type = bool;
    static constexpr type null_value = false;
    static constexpr bool has_null = false;
};

template<>
struct EnumTypeMap<ColumnType::BYTE> {
    static_assert(is_fixed_convertible(ColumnType::BYTE), "EnumTypeMap specialisation for a type is_fixed_convertible does not list");
    using type = int8_t;
    static constexpr type null_value = 0;
    static constexpr bool has_null = false;
};

template<>
struct EnumTypeMap<ColumnType::SHORT> {
    static_assert(is_fixed_convertible(ColumnType::SHORT), "EnumTypeMap specialisation for a type is_fixed_convertible does not list");
    using type = int16_t;
    static constexpr type null_value = 0;
    static constexpr bool has_null = false;
};

template<>
struct EnumTypeMap<ColumnType::INT> {
    static_assert(is_fixed_convertible(ColumnType::INT), "EnumTypeMap specialisation for a type is_fixed_convertible does not list");
    using type = int32_t;
    static constexpr type null_value =  static_cast<int32_t>(0x80000000);
    static constexpr bool has_null = true;
};

template<>
struct EnumTypeMap<ColumnType::LONG> {
    static_assert(is_fixed_convertible(ColumnType::LONG), "EnumTypeMap specialisation for a type is_fixed_convertible does not list");
    using type = int64_t;
    static constexpr type null_value = static_cast<int64_t>(0x8000000000000000LL);
    static constexpr bool has_null = true;
};

template<>
struct EnumTypeMap<ColumnType::FLOAT> {
    static_assert(is_fixed_convertible(ColumnType::FLOAT), "EnumTypeMap specialisation for a type is_fixed_convertible does not list");
    using type = float;
    static constexpr type null_value = std::numeric_limits<float>::quiet_NaN();
    static constexpr bool has_null = true;
};

template<>
struct EnumTypeMap<ColumnType::DOUBLE> {
    static_assert(is_fixed_convertible(ColumnType::DOUBLE), "EnumTypeMap specialisation for a type is_fixed_convertible does not list");
    using type = double;
    static constexpr type null_value = std::numeric_limits<double>::quiet_NaN();
    static constexpr bool has_null = true;
};

template <>
struct EnumTypeMap<ColumnType::TIMESTAMP_MICRO> {
    static_assert(is_fixed_convertible(ColumnType::TIMESTAMP_MICRO), "EnumTypeMap specialisation for a type is_fixed_convertible does not list");
  using type = int64_t;
  static constexpr type null_value = static_cast<int64_t>(0x8000000000000000LL);
  static constexpr bool has_null = true;
};

template <>
struct EnumTypeMap<ColumnType::TIMESTAMP_NANO> {
    static_assert(is_fixed_convertible(ColumnType::TIMESTAMP_NANO), "EnumTypeMap specialisation for a type is_fixed_convertible does not list");
  using type = int64_t;
  static constexpr type null_value = static_cast<int64_t>(0x8000000000000000LL);
  static constexpr bool has_null = true;
};

template<>
struct EnumTypeMap<ColumnType::DATE> {
    static_assert(is_fixed_convertible(ColumnType::DATE), "EnumTypeMap specialisation for a type is_fixed_convertible does not list");
    using type = int64_t;
    static constexpr type null_value =  static_cast<int64_t>(0x8000000000000000LL);
    static constexpr bool has_null = true;
};

/**
 * Convert between fixed numeric types.
 * Expected to align with SQL CAST behaviour.
 * @tparam src the source type
 * @tparam dst the destination type
 * @param srcMem the source type mmap column
 * @param dstMem the destination type mmap column
 * @param rowCount the number of rows
 * @return
 */
template<ColumnType src, ColumnType dst>
jlong convert_from_type_to_type(void *srcBuff, void *dstBuff, size_t rowCount) {
    using T1 = typename EnumTypeMap<src>::type;
    using T2 = typename EnumTypeMap<dst>::type;

    constexpr T1 srcSentinel = EnumTypeMap<src>::null_value;
    constexpr T2 dstSentinel = EnumTypeMap<dst>::null_value;

    constexpr bool srcNullable = EnumTypeMap<src>::has_null;
    constexpr bool dstNullable = EnumTypeMap<dst>::has_null;

    static_assert(dstNullable == true || (dstNullable == false && dstSentinel == static_cast<T2>(0)));

    T1* srcMem = reinterpret_cast<T1 *>(srcBuff);
    T2* dstMem = reinterpret_cast<T2 *>(dstBuff);

    for (size_t i = 0; i < rowCount; i++) {
        if constexpr (srcNullable) {
            if constexpr ((std::is_same<T1, float>() || std::is_same<T1, double>())) {
                // Float point conversions have undefined behaviour
                // when converting floating point types (float, double)
                // to integers.
                // The conversion can result to different results for every run, e.g. can be different
                // even on the same platform. To avoid it, check the ranges
                if constexpr (!std::is_same<T2, bool>()) {
                    if (std::isnan(srcMem[i]) || srcMem[i] > std::numeric_limits<T2>::max() ||
                        srcMem[i] < std::numeric_limits<T2>::lowest()) {
                        dstMem[i] = dstSentinel;
                        continue;
                    }
                } else {
                    if (std::isnan(srcMem[i])) {
                        dstMem[i] = dstSentinel;
                        continue;
                    }
                }
            } else {
                if (srcMem[i] == srcSentinel) {
                    dstMem[i] = dstSentinel;
                    continue;
                }
            }
        }

        dstMem[i] = static_cast<T2>(srcMem[i]);
    }

    return static_cast<jlong>(ConversionError::NONE);
}

#endif //CONVERTERS_H


