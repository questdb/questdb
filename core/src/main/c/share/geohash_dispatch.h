/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

#ifndef QUESTDB_GEOHASH_DISPATCH_H
#define QUESTDB_GEOHASH_DISPATCH_H

#include "util.h"
#include "dispatcher.h"

constexpr int64_t unpack_length(int64_t packed_hash) { return packed_hash >> 60; }

constexpr int64_t unpack_hash(int64_t packed_hash) { return packed_hash & 0x0fffffffffffffffll; }

constexpr int64_t bitmask(uint8_t count, uint8_t shift) { return ((static_cast<int64_t>(1) << count) - 1) << shift; }

DECLARE_DISPATCHER_TYPE(filter_with_prefix,
                        const int64_t *hashes,
                        int64_t *rows,
                        const int64_t rows_count,
                        const int32_t hash_length,
                        const int64_t *prefixes,
                        const int64_t prefixes_count
);

DECLARE_DISPATCHER_TYPE(simd_iota, int64_t *array, const int64_t array_size, const int64_t start);

#endif //QUESTDB_GEOHASH_DISPATCH_H
