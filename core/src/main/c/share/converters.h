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
    return static_cast<int64_t>(a) << 32 | (static_cast<int64_t>(b) & 0xffffffffL);
}

enum class ConversionError {
    NONE = 0,
    UNSUPPORTED_CAST = 1,
};

/**
 * Ensures that the Java type aligns with the C++ type.
 * @tparam C
 * @tparam T
 * @return
 */
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
    if constexpr (C == ColumnType::TIMESTAMP && std::is_same<T, int64_t>()) {
        return true;
    }
    return false;
}

constexpr static int32_t FLOAT_NULL_SENTINEL = 0x7fc00000;
constexpr static int64_t DOUBLE_NULL_SENTINEL = 0x7ff8000000000000L;


template<ColumnType C, typename T>
constexpr
T get_null_sentinel() {
    if constexpr (C == ColumnType::INT) {
        return static_cast<T>(0x80000000);
    } else if (C == ColumnType::LONG) {
        return static_cast<T>(0x8000000000000000L);
    } else if (C == ColumnType::TIMESTAMP) {
        return static_cast<T>(0x8000000000000000L);
    } else if (C == ColumnType::FLOAT) {
        // ReSharper disable once CppCStyleCast
        return *((float *) (&FLOAT_NULL_SENTINEL)); // INTENTIONAL
    } else if (C == ColumnType::DOUBLE) {
        // ReSharper disable once CppCStyleCast
        return *((double *) (&DOUBLE_NULL_SENTINEL)); // INTENTIONAL
    } else {
        return static_cast<T>(0);
    }
}

template<ColumnType C>
constexpr bool is_nullable() {
    if constexpr (C == ColumnType::INT) {
        return true;
    } else if (C == ColumnType::LONG) {
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

/**
 * Convert between fixed numeric types.
 * Expected to align with SQL CAST behaviour.
 * @tparam T1 the source type
 * @tparam T2 the destination type
 * @param srcMem the source type mmap column
 * @param dstMem the destination type mmap column
 * @param srcSentinel the source null sentinel
 * @param dstSentinel the destination null sentinel
 * @param rowCount the number of rows
 * @return
 */
template<typename T1, typename T2, bool srcNullable, bool dstNullable>
auto convert_fixed_to_fixed_numeric(T1 *srcMem, T2 *dstMem, T1 srcSentinel,
                                    T2 dstSentinel, size_t rowCount) -> ConversionError {
    // if dst is nullable, then we have a sentinel
    // else the sentinel must be 0
    // i.e INT(NULL) -> BYTE(0)
    assert(dstNullable == true || (dstNullable == false && dstSentinel == 0));

    for (size_t i = 0; i < rowCount; i++) {
        if constexpr (srcNullable) {
            if constexpr (std::is_same<T1, float>() || std::is_same<T1, double>()) {
                if (std::isnan(srcMem[i])) {
                    dstMem[i] = dstSentinel;
                    continue;
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


    return ConversionError::NONE;
}

/**
 * Convert between fixed numeric types.
 * Doesn't handle converting null sentinels.
 * @tparam T1 the source type
 * @tparam T2 the destination type
 * @param srcMem the source type mmap column
 * @param dstMem the destination type mmap column
 * @param rowCount the number of rows
 * @return
 */
template<typename T1, typename T2>
auto convert_fixed_to_fixed_numeric_fast(T1 *srcMem, T2 *dstMem, size_t rowCount) -> ConversionError {
    if constexpr(std::is_same<T1, T2>())
    {
        for (size_t i = 0; i < rowCount; i++) {
            dstMem[i] = srcMem[i];
        }
    } else
    {
        for (size_t i = 0; i < rowCount; i++) {
            dstMem[i] = static_cast<T2>(srcMem[i]);
        }
    }

    return ConversionError::NONE;
}


#endif //CONVERTERS_H


