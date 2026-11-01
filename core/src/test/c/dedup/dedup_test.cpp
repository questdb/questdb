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

// Runs the dedup merge of dedup.cpp on existing rows in the column top of a dedup key. The merge then compares
// the null value that dedup_column holds for the key. dedup_column is a packed record, and the null value is
// 16-byte aligned in some records and not in others. The test places the records at both 8-byte offsets mod 16,
// with the key in either slot, for every fixed key width, in the single-key and the multi-key merge, in UPSERT
// and SKIP mode.
//
// CMakeLists.txt compiles this test and dedup.cpp with the alignment sanitizer in trap mode. A read through a
// pointer to a type that needs more alignment than its address has then traps (SIGILL), whatever instructions
// the compiler emits for the read. Without the sanitizer, such a read faults only when the compiler loads it
// with an aligned SSE instruction: GCC 16 does for 32-byte keys, GCC 10 does not for any key width.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include "jni.h"
#include "src/main/c/share/column_type.h"
#include "src/main/c/share/dedup.h"

extern "C" JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_mergeDedupTimestampWithLongIndexIntKeys(
        JNIEnv *e,
        jclass cl,
        jlong srcTimestampAddr,
        jlong mergeDataLo,
        jlong mergeDataHi,
        jlong sortedTimestampsAddr,
        jlong mergeOOOLo,
        jlong mergeOOOHi,
        jlong tempIndexAddr,
        jint dedupKeyCount,
        jlong dedupColBuffs,
        jboolean skipMode
);

namespace {

// dedup_column offsets, as DedupColumnCommitAddresses.java writes them
constexpr size_t RECORD_BYTES = 136;
constexpr size_t COL_TYPE_OFFSET = 0;
constexpr size_t VAL_SIZE_OFFSET = 4;
constexpr size_t COL_TOP_OFFSET = 8;
constexpr size_t COL_DATA_OFFSET = 16;
constexpr size_t O3_DATA_OFFSET = 40;
constexpr size_t NULL_VALUE_OFFSET = 104;
static_assert(sizeof(dedup_column) == RECORD_BYTES, "dedup_column changed, update the offsets of this test");

// marks a merged row taken from the existing rows rather than from the new rows
constexpr uint64_t FROM_COLUMN = 1ull << 63;
constexpr int ROWS = 4;
constexpr int MAX_KEY_BYTES = 32;

struct key_type {
    const char *name;
    ColumnType type;
    int32_t size;
};

int failure_count = 0;

void put_null(const key_type &key, uint8_t *dst) {
    std::memset(dst, 0, key.size);
    if (key.type == ColumnType::INT) {
        const int32_t null_value = INT32_MIN;
        std::memcpy(dst, &null_value, sizeof(null_value));
    } else if (key.size >= 8) {
        // LONG, UUID and LONG256 are null when all their longs are LONG_NULL
        const int64_t null_value = INT64_MIN;
        for (int i = 0; i < key.size; i += 8) {
            std::memcpy(dst + i, &null_value, sizeof(null_value));
        }
    }
    // BYTE and SHORT are null when 0
}

void put_key(const key_type &key, uint8_t *dst, int64_t value) {
    std::memset(dst, 0, key.size);
    // little-endian: the low bytes of the value, the low long of a wider key
    std::memcpy(dst, &value, key.size < 8 ? key.size : 8);
}

void put_record(uint8_t *record, const key_type &key, int64_t column_top, const void *data, const void *o3_data) {
    const auto type = static_cast<int32_t>(key.type);
    std::memcpy(record + COL_TYPE_OFFSET, &type, sizeof(type));
    std::memcpy(record + VAL_SIZE_OFFSET, &key.size, sizeof(key.size));
    std::memcpy(record + COL_TOP_OFFSET, &column_top, sizeof(column_top));
    std::memcpy(record + COL_DATA_OFFSET, &data, sizeof(data));
    std::memcpy(record + O3_DATA_OFFSET, &o3_data, sizeof(o3_data));
    put_null(key, record + NULL_VALUE_OFFSET);
}

void assert_merge(const key_type &key, int key_count, int key_slot, size_t record_shift, bool skip_mode) {
    // print the case before running it: a SIGILL right after the line is the sanitizer trapping in that case
    std::printf("%s key, key_count=%d, key_slot=%d, record_shift=%zu, skip_mode=%d\n",
                key.name, key_count, key_slot, record_shift, skip_mode);
    std::fflush(stdout);

    // existing rows: ts 10, 20, 20, 30; rows 0 and 1 are in the column top of the key, rows 2 and 3 hold key 1
    const uint64_t src_timestamps[ROWS] = {10, 20, 20, 30};
    alignas(MAX_KEY_BYTES) uint8_t key_data[ROWS * MAX_KEY_BYTES];
    for (int row = 0; row < ROWS; row++) {
        put_null(key, key_data + row * key.size);
    }
    put_key(key, key_data + 2 * key.size, 1);
    put_key(key, key_data + 3 * key.size, 1);

    // new rows, sorted by timestamp and key: 15; 20 with a null key; 20 with key 1; 20 with key 2
    const uint64_t o3_index[2 * ROWS] = {15, 0, 20, 1, 20, 2, 20, 3};
    alignas(MAX_KEY_BYTES) uint8_t key_o3[ROWS * MAX_KEY_BYTES];
    for (int row = 0; row < ROWS; row++) {
        put_null(key, key_o3 + row * key.size);
    }
    put_key(key, key_o3 + 2 * key.size, 1);
    put_key(key, key_o3 + 3 * key.size, 2);

    // the other key is equal on all rows, so the multi-key merge compares the column-top key in either slot
    const key_type other_key{"INT", ColumnType::INT, 4};
    const int32_t other_key_data[ROWS] = {7, 7, 7, 7};
    const int32_t other_key_o3[ROWS] = {7, 7, 7, 7};

    alignas(16) uint8_t records[2 * RECORD_BYTES + 16] = {};
    uint8_t *records_addr = records + record_shift;
    if (key_count == 2) {
        put_record(records_addr + (1 - key_slot) * RECORD_BYTES, other_key, 0, other_key_data, other_key_o3);
    }
    put_record(records_addr + key_slot * RECORD_BYTES, key, 2, key_data, key_o3);

    uint64_t merged[2 * (ROWS + ROWS)] = {};
    const jlong merged_count = Java_io_questdb_std_Vect_mergeDedupTimestampWithLongIndexIntKeys(
            nullptr,
            nullptr,
            reinterpret_cast<jlong>(src_timestamps),
            0,
            ROWS - 1,
            reinterpret_cast<jlong>(o3_index),
            0,
            ROWS - 1,
            reinterpret_cast<jlong>(merged),
            key_count,
            reinterpret_cast<jlong>(records_addr),
            skip_mode
    );

    // existing row 1 matches new row 1 on the null key and existing row 2 matches new row 2 on key 1:
    // UPSERT takes the new rows, SKIP keeps the existing ones
    const uint64_t expected[] = {
            10, 0 | FROM_COLUMN,
            15, 0,
            20, skip_mode ? (1 | FROM_COLUMN) : 1,
            20, skip_mode ? (2 | FROM_COLUMN) : 2,
            20, 3,
            30, 3 | FROM_COLUMN,
    };
    constexpr jlong expected_count = sizeof(expected) / sizeof(expected[0]) / 2;
    if (merged_count != expected_count || std::memcmp(merged, expected, sizeof(expected)) != 0) {
        std::printf("FAILED: merged %lld rows, expected %lld\n",
                    static_cast<long long>(merged_count), static_cast<long long>(expected_count));
        for (jlong i = 0; i < merged_count && i < ROWS + ROWS; i++) {
            std::printf("  ts=%llu row=%llu %s\n",
                        static_cast<unsigned long long>(merged[2 * i]),
                        static_cast<unsigned long long>(merged[2 * i + 1] & ~FROM_COLUMN),
                        (merged[2 * i + 1] & FROM_COLUMN) != 0 ? "existing" : "new");
        }
        failure_count++;
    }
}

} // namespace

int main() {
    const key_type key_types[] = {
            {"BYTE", ColumnType::BYTE, 1},
            {"SHORT", ColumnType::SHORT, 2},
            {"INT", ColumnType::INT, 4},
            {"LONG", ColumnType::LONG, 8},
            {"UUID", ColumnType::UUID, 16},
            {"LONG256", ColumnType::LONG256, 32},
    };
    int case_count = 0;
    for (const key_type &key: key_types) {
        for (int key_count = 1; key_count < 3; key_count++) {
            for (int key_slot = 0; key_slot < key_count; key_slot++) {
                for (size_t record_shift: {0, 8}) {
                    for (bool skip_mode: {false, true}) {
                        assert_merge(key, key_count, key_slot, record_shift, skip_mode);
                        case_count++;
                    }
                }
            }
        }
    }
    if (failure_count > 0) {
        std::printf("dedup merge: %d of %d cases FAILED\n", failure_count, case_count);
        return 1;
    }
    std::printf("dedup merge: all %d cases passed\n", case_count);
    return 0;
}
