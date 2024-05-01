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
#ifndef CONVERTERS_H
#define CONVERTERS_H

#include <cassert>
#include <type_traits>
#include <cmath>
#include <__algorithm/clamp.h>

/**
 * ColumnType enum, matching the Java definitions.
 */
enum class ColumnType : int {
    UNDEFINED = 0,
    BOOLEAN = 1,
    BYTE = 2,
    SHORT = 3,
    CHAR = 4,
    INT = 5,
    LONG = 6,
    DATE = 7,
    TIMESTAMP = 8,
    FLOAT = 9,
    DOUBLE = 10,
    STRING = 11,
    SYMBOL = 12,
    LONG256 = 13,
    GEOBYTE = 14,
    GEOSHORT = 15,
    GEOINT = 16,
    GEOLONG = 17,
    BINARY = 18,
    UUID = 19,
    CURSOR = 20,
    VAR_ARG = 21,
    RECORD = 22,
    GEOHASH = 23,
    LONG128 = 24,
    IPV4 = 25,
    VARCHAR = 26,
    REGCLASS = 27,
    REGPROCEDURE = 28,
    ARRAY_STRING = 29,
    PARAMETER = 30,
    NULL_ = 31
};

/**
 * Packs the column types into an int64_t, so we can use a single switch over both enum values.
 * @param a the src column type
 * @param b the dst column type
 * @return packed value
 */
constexpr int64_t pack_column_types(ColumnType a, ColumnType b) {
    return static_cast<int64_t>(a) << 32 | static_cast<int64_t>(b) & 0xffffffffL;
}

enum class ConversionError {
    NONE = 0,
    UNSUPPORTED_CAST = 1,
};

/**
 * Convert between fixed numeric types.
 * Expected behaviour:
 *      For nullable types, any over/under flow should be converted to a null.
 *      For non-nullable types, any overflow will be left as is i.e 100,000 -> short will be -31072.
 * @tparam T1 the source type
 * @tparam T2 the destination type
 * @param srcMem the source type mmap column
 * @param dstMem the destination type mmap column
 * @param srcNullable whether source is nullable
 * @param srcSentinel the source null sentinel
 * @param dstNullable whether destination is nullable
 * @param dstSentinel the destination null sentinel
 * @param rowCount the number of rows
 * @return
 */
template<typename T1, typename T2>
ConversionError
convert_fixed_to_fixed_numeric(T1 *srcMem, T2 *dstMem, bool srcNullable, T1 srcSentinel, bool dstNullable,
                               T2 dstSentinel, size_t rowCount) {
    // if dst is nullable, then we have a sentinel
    // else the sentinel must be 0
    // i.e INT(NULL) -> BYTE(0)
    assert(dstNullable == true || dstNullable == false && dstSentinel == 0);

    for (int i = 0; i < rowCount; i++) {
        // if source is nullable and its a null, convert it to a null (or 0 if dst is non nullable)
        if constexpr (srcNullable) {
            if (srcMem[i] == srcSentinel) {
                dstMem[i] = dstSentinel;
                continue;
            }
        }

        // if both are nullable, and src is outside range of dst, then map it to a null
        if constexpr (srcNullable && dstNullable) {
            assert(srcMem[i] != srcSentinel);
            if (not_in_range<T1, T2>(srcMem[i])) {
                dstMem[i] = dstSentinel;
                continue;
            }
        }

        // otherwise, we cast and copy
        dstMem[i] = static_cast<T2>(srcMem[i]);
    }

    return ConversionError::NONE;
}

/**
 * Checks if the T1 value is within the range of the T2's min and max bounds.
 * @tparam T1
 * @tparam T2
 * @param value
 * @return
 */
template<typename T1, typename T2>
constexpr bool not_in_range(T1 value) {
    return std::clamp<T1>(value, std::numeric_limits<T2>::min(), std::numeric_limits<T2>::max()) != value;
}

template<ColumnType C, typename T>
constexpr bool is_matching_type() {
    if constexpr (C == ColumnType::BYTE && std::is_same<T, int8_t>()) {
        return true;
    }
    if constexpr (C == ColumnType::SHORT && std::is_same<T, int16_t>()) {
        return true;
    }
    if constexpr (C == ColumnType::INT && std::is_same<T, int32_t>()) {
        return true;
    }
    if constexpr (C == ColumnType::LONG && std::is_same<T, int64_t>()) {
        return true;
    }
    if constexpr (C == ColumnType::FLOAT && std::is_same<T, float>()) {
        return true;
    }
    if constexpr (C == ColumnType::DOUBLE && std::is_same<T, double>()) {
        return true;
    }
    if constexpr (C == ColumnType::DATE && std::is_same<T, int64_t>()) {
        return true;
    }
    if constexpr (C == ColumnType::TIMESTAMP && std::is_same<T, int64_t>()) {
        return true;
    }
    return false;
}

template<ColumnType C>
constexpr
auto get_null_sentinel() {
    if constexpr (C == ColumnType::INT) {
        return INT32_MIN;
    } else if (C == ColumnType::LONG) {
        return INT64_MIN;
    } else if (C == ColumnType::DATE) {
        return INT64_MIN;
    } else if (C == ColumnType::TIMESTAMP) {
        return INT64_MIN;
    } else if (C == ColumnType::FLOAT) {
        return std::nanf;
    } else if (C == ColumnType::DOUBLE) {
        return std::nan;
    } else {
        return 0;
    }
}

template<ColumnType C>
constexpr
bool is_nullable() {
    if constexpr (C == ColumnType::INT) {
        return true;
    } else if (C == ColumnType::LONG) {
        return true;
    } else if (C == ColumnType::DATE) {
        return true;
    } else if (C == ColumnType::TIMESTAMP) {
        return true;
    } else if (C == ColumnType::FLOAT) {
        return true;
    } else if (C == ColumnType::DOUBLE) {
        return true;
    } else {
        return false;
    }
}

#endif //CONVERTERS_H


