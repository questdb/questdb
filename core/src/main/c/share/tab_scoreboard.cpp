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
#include <atomic>
#include <tuple>
#include "util.h"

typedef std::tuple<int64_t,int64_t> slot_val_t;

typedef struct scoreboard_slot {
    int64_t timestamp;
    int64_t txn;
    std::atomic<uint64_t> score{0};
    uint64_t _unused{};

    scoreboard_slot(scoreboard_slot& other){
        timestamp = other.timestamp;
        txn = other.txn;
    }

    bool operator<(slot_val_t other) const {
        return timestamp < std::get<0>(other) && txn < std::get<1>(other);
    }

    bool operator>(slot_val_t other) const {
        return timestamp > std::get<0>(other) && txn > std::get<1>(other);
    }

    bool operator==(const scoreboard_slot& other) const {
        return timestamp == other.timestamp && txn == other.txn;
    }
    
} scoreboard_slot_t;

typedef struct {
    uint64_t size;
    std::atomic<uint64_t> score;
    uint64_t _unused1;
    uint64_t _unused2;
} scoreboard_header_t;

typedef struct {
    scoreboard_header_t header;
    scoreboard_slot_t slot[];
} scoreboard_t;

void reader_open(scoreboard_t* scoreboard, int64_t timestamp, int64_t txn) {

    // try to lock the scoreboard out of re-structuring
    uint64_t header_score = scoreboard->header.score;
    uint64_t expect_header_score;
    do {
        expect_header_score = header_score < 0 ? 0 : header_score;
    } while (!std::atomic_compare_exchange_weak(&(scoreboard->header.score), &header_score, expect_header_score + 1));

    const slot_val_t slot_val(timestamp, txn);

    // todo: is this correct order?
    auto size = scoreboard->header.size;
    int64_t index = binary_search(
            scoreboard->slot,
            slot_val,
            0,
            size,
            1
            );
}