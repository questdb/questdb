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

#include "converters.h"
#include "simd.h"

// Used to clean up noise in the switch statement
#define macro_dispatch_fixed_to_fixed(a, b) case pack_column_types(a, b): return convert_from_type_to_type<a, b>(src, dst, row_count)

void convert_us_to_ms(int64_t *dest, const int64_t *src, const int64_t count) {
    constexpr int64_t srcSentinel = EnumTypeMap<ColumnType::TIMESTAMP_MICRO>::null_value;
    constexpr int64_t dstSentinel = EnumTypeMap<ColumnType::DATE>::null_value;
    for(int64_t i = 0; i < count; i++) {
        const bool isnull = (src[i] == srcSentinel);
        dest[i] = (!isnull) * src[i] / 1000;
        dest[i] += (isnull) * dstSentinel;
    }
}

void convert_us_to_ns(int64_t *dest, const int64_t *src, const int64_t count) {
  constexpr int64_t srcSentinel = EnumTypeMap<ColumnType::TIMESTAMP_MICRO>::null_value;
  constexpr int64_t dstSentinel = EnumTypeMap<ColumnType::TIMESTAMP_NANO>::null_value;
  for (int64_t i = 0; i < count; i++) {
    const bool isnull = (src[i] == srcSentinel);
    dest[i] = (!isnull) * src[i] * 1000;
    dest[i] += (isnull)*dstSentinel;
  }
}

void convert_ms_to_us(int64_t *dest, const int64_t *src, const int64_t count) {
    constexpr int64_t srcSentinel = EnumTypeMap<ColumnType::DATE>::null_value;
    constexpr int64_t dstSentinel = EnumTypeMap<ColumnType::TIMESTAMP_MICRO>::null_value;
    for(int64_t i = 0; i < count; i++) {
        const bool isnull = (src[i] == srcSentinel);
        dest[i] = (!isnull) * src[i] * 1000;
        dest[i] += (isnull) * dstSentinel;
    }
}

void convert_ms_to_ns(int64_t *dest, const int64_t *src, const int64_t count) {
  constexpr int64_t srcSentinel = EnumTypeMap<ColumnType::DATE>::null_value;
  constexpr int64_t dstSentinel =
      EnumTypeMap<ColumnType::TIMESTAMP_NANO>::null_value;
  for (int64_t i = 0; i < count; i++) {
    const bool isnull = (src[i] == srcSentinel);
    dest[i] = (!isnull) * src[i] * 1000000;
    dest[i] += (isnull)*dstSentinel;
  }
}

void convert_ns_to_us(int64_t *dest, const int64_t *src, const int64_t count) {
  constexpr int64_t srcSentinel = EnumTypeMap<ColumnType::TIMESTAMP_NANO>::null_value;
  constexpr int64_t dstSentinel = EnumTypeMap<ColumnType::TIMESTAMP_MICRO>::null_value;
  for (int64_t i = 0; i < count; i++) {
    const bool isnull = (src[i] == srcSentinel);
    dest[i] = (!isnull) * src[i] / 1000;
    dest[i] += (isnull)*dstSentinel;
  }
}

void convert_ns_to_ms(int64_t *dest, const int64_t *src, const int64_t count) {
  constexpr int64_t srcSentinel = EnumTypeMap<ColumnType::TIMESTAMP_NANO>::null_value;
  constexpr int64_t dstSentinel =
      EnumTypeMap<ColumnType::DATE>::null_value;
  for (int64_t i = 0; i < count; i++) {
    const bool isnull = (src[i] == srcSentinel);
    dest[i] = (!isnull) * src[i] / 1000000;
    dest[i] += (isnull)*dstSentinel;
  }
}

// extern "C"
extern "C" {
JNIEXPORT jlong
JNICALL
Java_io_questdb_griffin_ConvertersNative_fixedToFixed
(
    JNIEnv */*env*/,
    jclass /*cl*/,
    jlong srcMem,
    jlong srcType,
    jlong dstMem,
    jlong dstType,
    jlong rowCount
) {
    const auto srcColumnType = static_cast<ColumnType>(srcType);
    const auto dstColumnType = static_cast<ColumnType>(dstType);

    auto src = reinterpret_cast<void*>(srcMem);
    auto dst = reinterpret_cast<void*>(dstMem);
    auto row_count = static_cast<size_t>(rowCount);

    switch (pack_column_types(srcColumnType, dstColumnType)) {
        // BOOL
        macro_dispatch_fixed_to_fixed(ColumnType::BOOLEAN, ColumnType::BYTE);
        macro_dispatch_fixed_to_fixed(ColumnType::BOOLEAN, ColumnType::SHORT);
        macro_dispatch_fixed_to_fixed(ColumnType::BOOLEAN, ColumnType::INT);
        macro_dispatch_fixed_to_fixed(ColumnType::BOOLEAN, ColumnType::LONG);
        macro_dispatch_fixed_to_fixed(ColumnType::BOOLEAN, ColumnType::FLOAT);
        macro_dispatch_fixed_to_fixed(ColumnType::BOOLEAN, ColumnType::DOUBLE);
        macro_dispatch_fixed_to_fixed(ColumnType::BOOLEAN, ColumnType::TIMESTAMP_MICRO);
        macro_dispatch_fixed_to_fixed(ColumnType::BOOLEAN, ColumnType::TIMESTAMP_NANO);
        macro_dispatch_fixed_to_fixed(ColumnType::BOOLEAN, ColumnType::DATE);
        // BYTE
        macro_dispatch_fixed_to_fixed(ColumnType::BYTE, ColumnType::SHORT);
        macro_dispatch_fixed_to_fixed(ColumnType::BYTE, ColumnType::INT);
        macro_dispatch_fixed_to_fixed(ColumnType::BYTE, ColumnType::LONG);
        macro_dispatch_fixed_to_fixed(ColumnType::BYTE, ColumnType::FLOAT);
        macro_dispatch_fixed_to_fixed(ColumnType::BYTE, ColumnType::DOUBLE);
        macro_dispatch_fixed_to_fixed(ColumnType::BYTE,
                                      ColumnType::TIMESTAMP_MICRO);
        macro_dispatch_fixed_to_fixed(ColumnType::BYTE,
                                      ColumnType::TIMESTAMP_NANO);
        macro_dispatch_fixed_to_fixed(ColumnType::BYTE, ColumnType::BOOLEAN);
        macro_dispatch_fixed_to_fixed(ColumnType::BYTE, ColumnType::DATE);
        // SHORT
        macro_dispatch_fixed_to_fixed(ColumnType::SHORT, ColumnType::BYTE);
        macro_dispatch_fixed_to_fixed(ColumnType::SHORT, ColumnType::INT);
        macro_dispatch_fixed_to_fixed(ColumnType::SHORT, ColumnType::LONG);
        macro_dispatch_fixed_to_fixed(ColumnType::SHORT, ColumnType::FLOAT);
        macro_dispatch_fixed_to_fixed(ColumnType::SHORT, ColumnType::DOUBLE);
        macro_dispatch_fixed_to_fixed(ColumnType::SHORT,
                                      ColumnType::TIMESTAMP_MICRO);
        macro_dispatch_fixed_to_fixed(ColumnType::SHORT,
                                      ColumnType::TIMESTAMP_NANO);
        macro_dispatch_fixed_to_fixed(ColumnType::SHORT, ColumnType::BOOLEAN);
        macro_dispatch_fixed_to_fixed(ColumnType::SHORT, ColumnType::DATE);
        // INT
        macro_dispatch_fixed_to_fixed(ColumnType::INT, ColumnType::BYTE);
        macro_dispatch_fixed_to_fixed(ColumnType::INT, ColumnType::SHORT);
        macro_dispatch_fixed_to_fixed(ColumnType::INT, ColumnType::LONG);
        macro_dispatch_fixed_to_fixed(ColumnType::INT, ColumnType::FLOAT);
        macro_dispatch_fixed_to_fixed(ColumnType::INT, ColumnType::DOUBLE);
        macro_dispatch_fixed_to_fixed(ColumnType::INT,
                                      ColumnType::TIMESTAMP_MICRO);
        macro_dispatch_fixed_to_fixed(ColumnType::INT,
                                      ColumnType::TIMESTAMP_NANO);
        macro_dispatch_fixed_to_fixed(ColumnType::INT, ColumnType::BOOLEAN);
        macro_dispatch_fixed_to_fixed(ColumnType::INT, ColumnType::DATE);
        // LONG
        macro_dispatch_fixed_to_fixed(ColumnType::LONG, ColumnType::BYTE);
        macro_dispatch_fixed_to_fixed(ColumnType::LONG, ColumnType::SHORT);
        macro_dispatch_fixed_to_fixed(ColumnType::LONG, ColumnType::INT);
        macro_dispatch_fixed_to_fixed(ColumnType::LONG, ColumnType::FLOAT);
        macro_dispatch_fixed_to_fixed(ColumnType::LONG, ColumnType::DOUBLE);
        macro_dispatch_fixed_to_fixed(ColumnType::LONG, ColumnType::BOOLEAN);
        // FLOAT
        macro_dispatch_fixed_to_fixed(ColumnType::FLOAT, ColumnType::BYTE);
        macro_dispatch_fixed_to_fixed(ColumnType::FLOAT, ColumnType::SHORT);
        macro_dispatch_fixed_to_fixed(ColumnType::FLOAT, ColumnType::INT);
        macro_dispatch_fixed_to_fixed(ColumnType::FLOAT, ColumnType::LONG);
        macro_dispatch_fixed_to_fixed(ColumnType::FLOAT, ColumnType::DOUBLE);
        macro_dispatch_fixed_to_fixed(ColumnType::FLOAT,
                                      ColumnType::TIMESTAMP_MICRO);
        macro_dispatch_fixed_to_fixed(ColumnType::FLOAT,
                                      ColumnType::TIMESTAMP_NANO);
        macro_dispatch_fixed_to_fixed(ColumnType::FLOAT, ColumnType::BOOLEAN);
        macro_dispatch_fixed_to_fixed(ColumnType::FLOAT, ColumnType::DATE);
        // DOUBLE
        macro_dispatch_fixed_to_fixed(ColumnType::DOUBLE, ColumnType::BYTE);
        macro_dispatch_fixed_to_fixed(ColumnType::DOUBLE, ColumnType::SHORT);
        macro_dispatch_fixed_to_fixed(ColumnType::DOUBLE, ColumnType::INT);
        macro_dispatch_fixed_to_fixed(ColumnType::DOUBLE, ColumnType::LONG);
        macro_dispatch_fixed_to_fixed(ColumnType::DOUBLE, ColumnType::FLOAT);
        macro_dispatch_fixed_to_fixed(ColumnType::DOUBLE,
                                      ColumnType::TIMESTAMP_MICRO);
        macro_dispatch_fixed_to_fixed(ColumnType::DOUBLE,
                                      ColumnType::TIMESTAMP_NANO);
        macro_dispatch_fixed_to_fixed(ColumnType::DOUBLE, ColumnType::BOOLEAN);
        macro_dispatch_fixed_to_fixed(ColumnType::DOUBLE, ColumnType::DATE);
        // TIMESTAMP
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_MICRO,
                                      ColumnType::BYTE);
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_MICRO,
                                      ColumnType::SHORT);
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_MICRO,
                                      ColumnType::INT);
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_MICRO,
                                      ColumnType::FLOAT);
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_MICRO,
                                      ColumnType::DOUBLE);
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_MICRO,
                                      ColumnType::BOOLEAN);
        // TIMESTAMP_NANO
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_NANO,
                                      ColumnType::BYTE);
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_NANO,
                                      ColumnType::SHORT);
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_NANO,
                                      ColumnType::INT);
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_NANO,
                                      ColumnType::FLOAT);
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_NANO,
                                      ColumnType::DOUBLE);
        macro_dispatch_fixed_to_fixed(ColumnType::TIMESTAMP_NANO,
                                      ColumnType::BOOLEAN);
        // DATE
        macro_dispatch_fixed_to_fixed(ColumnType::DATE, ColumnType::BYTE);
        macro_dispatch_fixed_to_fixed(ColumnType::DATE, ColumnType::SHORT);
        macro_dispatch_fixed_to_fixed(ColumnType::DATE, ColumnType::INT);
        macro_dispatch_fixed_to_fixed(ColumnType::DATE, ColumnType::FLOAT);
        macro_dispatch_fixed_to_fixed(ColumnType::DATE, ColumnType::DOUBLE);
        macro_dispatch_fixed_to_fixed(ColumnType::DATE, ColumnType::BOOLEAN);

        case pack_column_types(ColumnType::TIMESTAMP_MICRO, ColumnType::LONG):
        case pack_column_types(ColumnType::LONG, ColumnType::TIMESTAMP_MICRO):
        case pack_column_types(ColumnType::TIMESTAMP_NANO, ColumnType::LONG):
        case pack_column_types(ColumnType::LONG, ColumnType::TIMESTAMP_NANO):
        case pack_column_types(ColumnType::LONG, ColumnType::DATE):
        case pack_column_types(ColumnType::DATE, ColumnType::LONG):
            // It's same 64 bit values, same null constant, simply mem copy the values.
            __MEMCPY(reinterpret_cast<int64_t *>(dstMem), reinterpret_cast<int64_t *>(srcMem), rowCount * sizeof(int64_t));
            break;
        case pack_column_types(ColumnType::TIMESTAMP_MICRO, ColumnType::DATE):
          convert_us_to_ms(reinterpret_cast<int64_t *>(dstMem),
                           reinterpret_cast<int64_t *>(srcMem), rowCount);
          break;
        case pack_column_types(ColumnType::DATE, ColumnType::TIMESTAMP_MICRO):
          convert_ms_to_us(reinterpret_cast<int64_t *>(dstMem),
                           reinterpret_cast<int64_t *>(srcMem), rowCount);
          break;
        case pack_column_types(ColumnType::TIMESTAMP_MICRO, ColumnType::TIMESTAMP_NANO):
          convert_us_to_ns(reinterpret_cast<int64_t *>(dstMem),
                           reinterpret_cast<int64_t *>(srcMem), rowCount);
          break;
        case pack_column_types(ColumnType::TIMESTAMP_NANO, ColumnType::TIMESTAMP_MICRO):
          convert_ns_to_us(reinterpret_cast<int64_t *>(dstMem),
                           reinterpret_cast<int64_t *>(srcMem), rowCount);
          break;
        case pack_column_types(ColumnType::DATE, ColumnType::TIMESTAMP_NANO):
          convert_ms_to_ns(reinterpret_cast<int64_t *>(dstMem),
                           reinterpret_cast<int64_t *>(srcMem), rowCount);
          break;
        case pack_column_types(ColumnType::TIMESTAMP_NANO, ColumnType::DATE):
          convert_ns_to_ms(reinterpret_cast<int64_t *>(dstMem),
                           reinterpret_cast<int64_t *>(srcMem), rowCount);
          break;
        default:
            return static_cast<jlong>(ConversionError::UNSUPPORTED_CAST);
    }

    return static_cast<jlong>(ConversionError::NONE);
}
}

