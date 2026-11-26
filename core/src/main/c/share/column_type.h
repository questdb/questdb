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
#ifndef COLUMN_TYPE_H
#define COLUMN_TYPE_H

#include <cassert>
#include <type_traits>
#include <cmath>
#include <cstdint>
#include "jni.h"

/**
 * ColumnType enum, matching the tag constants of io.questdb.cairo.ColumnType. ColumnTypeTest parses
 * this file and fails when a number here differs from Java.
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
  TIMESTAMP_MICRO = 8,
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
  ARRAY = 27,
  DECIMAL8 = 28,
  DECIMAL16 = 29,
  DECIMAL32 = 30,
  DECIMAL64 = 31,
  DECIMAL128 = 32,
  DECIMAL256 = 33,
  DECIMAL = 34,
  REGCLASS = 35,
  REGPROCEDURE = 36,
  ARRAY_STRING = 37,
  PARAMETER = 38,
  INTERVAL = 39,
  VARCHAR_SLICE = 40,
  // type-registration: native tag (see utils/type-probe/README.md)
  NULL_ = 41,
  TIMESTAMP_NANO = 1 << 18 | TIMESTAMP_MICRO,
};

/**
 * The var-size layout of a column type's values, for native code that picks a var-size reader. The
 * dedup switches key on this enum instead of on ColumnType, so a new fixed-size tag needs no change
 * there, while a new layout fails the build at each of them (dedup.cpp turns -Wswitch-enum into an
 * error).
 */
enum class VarLayout : int {
    // fixed-size values, or a type no var-size reader serves
    NONE,
    VARCHAR,
    STRING,
    BINARY,
    ARRAY,
    // a SYMBOL column's keys, remapped into one int buffer (dedup of a WAL commit block)
    SYMBOL,
};

#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch"

/**
 * The layout of the exact type value column_type. The switch names every enumerator and -Wswitch is
 * an error here, so a new tag fails the build until it gets a layout. An encoded value (array
 * dimensions, geohash bits) matches no enumerator and gets NONE: a geohash is fixed-size, and
 * callers pass an array column's bare tag.
 */
inline VarLayout var_layout(int32_t column_type) {
    switch (static_cast<ColumnType>(column_type)) {
        case ColumnType::VARCHAR:
            return VarLayout::VARCHAR;
        case ColumnType::STRING:
            return VarLayout::STRING;
        case ColumnType::BINARY:
            return VarLayout::BINARY;
        case ColumnType::ARRAY:
            return VarLayout::ARRAY;
        case ColumnType::SYMBOL:
            return VarLayout::SYMBOL;
        case ColumnType::UNDEFINED:
        case ColumnType::BOOLEAN:
        case ColumnType::BYTE:
        case ColumnType::SHORT:
        case ColumnType::CHAR:
        case ColumnType::INT:
        case ColumnType::LONG:
        case ColumnType::DATE:
        case ColumnType::TIMESTAMP_MICRO:
        case ColumnType::FLOAT:
        case ColumnType::DOUBLE:
        case ColumnType::LONG256:
        case ColumnType::GEOBYTE:
        case ColumnType::GEOSHORT:
        case ColumnType::GEOINT:
        case ColumnType::GEOLONG:
        case ColumnType::UUID:
        case ColumnType::CURSOR:
        case ColumnType::VAR_ARG:
        case ColumnType::RECORD:
        case ColumnType::GEOHASH:
        case ColumnType::LONG128:
        case ColumnType::IPV4:
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
        case ColumnType::TIMESTAMP_NANO:
            return VarLayout::NONE;
    }
    return VarLayout::NONE;
}

#pragma GCC diagnostic pop

#pragma pack (push, 1)
struct VarcharAuxEntryInlined {
    uint8_t header;
    uint8_t chars[9];
    [[maybe_unused]] uint16_t offset_lo;
    [[maybe_unused]] uint32_t offset_hi;
};

struct VarcharAuxEntrySplit {
    uint32_t header;
    uint8_t chars[6];
    uint16_t offset_lo;
    uint32_t offset_hi;
};

struct ArrayAuxEntry {
    uint64_t offset_48;
    int32_t data_size;
    [[maybe_unused]] int32_t reserved2;
};

struct VarcharAuxEntryBoth {
    uint64_t header1;
    uint16_t header2;
    uint16_t offset_lo;
    uint32_t offset_hi;

    [[nodiscard]]
    inline int64_t get_data_offset() const {
        return (static_cast<int64_t>(offset_hi) << 16) | offset_lo;
    }
};

constexpr uint64_t ARRAY_OFFSET_MAX = (1ULL << 48) - 1ULL;
#pragma pack(pop)

#endif //COLUMN_TYPE_H


