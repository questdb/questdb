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

#ifndef QUESTDB_UTF8_H
#define QUESTDB_UTF8_H

#include "dispatcher.h"

// Varchar aux entry constants (must match VarcharTypeDriver.java)
constexpr int32_t VARCHAR_AUX_WIDTH_BYTES = 16;
constexpr int32_t VARCHAR_HEADER_FLAGS_WIDTH = 4;
constexpr int32_t VARCHAR_HEADER_FLAG_INLINED = 1;
constexpr int32_t VARCHAR_HEADER_FLAG_ASCII = 2;
constexpr int32_t VARCHAR_HEADER_FLAG_NULL = 4;
constexpr int32_t VARCHAR_INLINED_LENGTH_MASK = (1 << 4) - 1;
constexpr int32_t VARCHAR_DATA_LENGTH_MASK = (1 << 28) - 1;
constexpr int32_t VARCHAR_FULLY_INLINED_STRING_OFFSET = 1;

// NULL_LEN sentinel (must match TableUtils.NULL_LEN)
constexpr int32_t NULL_LEN = -1;

// Single-pass computation of SUM(length(varchar)) and COUNT(non-null).
// Combines aux header reading, code point counting, and accumulation in one pass.
// Uses software prefetching for non-ASCII data vector accesses.
// outSum (double) and outCount (int64) are accumulated (not overwritten).
DECLARE_DISPATCHER_TYPE(varchar_utf8_length_sum,
                        const char *auxAddr,
                        const char *dataAddr,
                        int64_t rowCount,
                        double *outSum,
                        int64_t *outCount);

#endif // QUESTDB_UTF8_H
