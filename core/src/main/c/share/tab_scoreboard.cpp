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

#include <cstdint>

typedef struct {
    int64_t timestamp;
    int64_t txn;
    uint64_t score;
    uint64_t _unused;

} scoreboard_slot_t;

typedef struct {
    int64_t size;
    uint64_t score;
    uint64_t _unused1;
    uint64_t _unused2;
} scoreboard_header_t;

typedef struct {
    scoreboard_header_t header;
    scoreboard_slot_t slot[];
} scoreboard_t;

void reader_open(scoreboard_t* scoreboard) {
//    while (atomic_compare_exchange())
}