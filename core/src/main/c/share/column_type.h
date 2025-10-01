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
#ifndef COLUMN_TYPE_H
#define COLUMN_TYPE_H

#include <cassert>
#include <type_traits>
#include <cmath>
#include <cstdint>
#include "jni.h"

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
  REGCLASS = 28,
  REGPROCEDURE = 29,
  ARRAY_STRING = 30,
  PARAMETER = 31,
  INTERVAL = 32,
  NULL_ = 33,
  TIMESTAMP_NANO = 1 << 18 | TIMESTAMP_MICRO,
};

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


