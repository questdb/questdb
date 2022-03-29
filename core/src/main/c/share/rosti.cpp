/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

#include "rosti.h"
#include <jni.h>

rosti_t *alloc_rosti(const int32_t *column_types, const int32_t column_count, const uint64_t map_capacity) {
    int32_t slot_key_size = 0;
    auto value_offsets = reinterpret_cast<int32_t *>(malloc(sizeof(int32_t) * (column_count + 1)));
    value_offsets[0] = 0;
    for (int32_t i = 0; i < column_count; i++) {
        switch (column_types[i]) {
            case 1: // BOOL
            case 2: // BYTE
                slot_key_size += 1;
                break;
            case 3: // SHORT
            case 4: // CHAR
                slot_key_size += 2;
                break;
            case 5: // INT
            case 9: // FLOAT
            case 12: // SYMBOL - store as INT
                slot_key_size += 4;
                break;
            case 6: // LONG (64 bit)
            case 7: // DATE
            case 8: // TIMESTAMP
            case 10: // DOUBLE
            case 11: // STRING - store reference only
                slot_key_size += 8;
                break;
            case 13: // LONG256
                slot_key_size += 64;
                break;
        }
        value_offsets[i + 1] = slot_key_size;
    }
    auto map = reinterpret_cast<rosti_t *>(malloc(sizeof(rosti_t)));
    map->slot_size_ = ceil_pow_2(slot_key_size);
    map->slot_size_shift_ = bit_scan_forward(map->slot_size_);
    map->capacity_ = map_capacity;
    map->size_ = 0;
    map->value_offsets_ = value_offsets;
    initialize_slots(map);
    return map;
}

extern "C" {

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Rosti_alloc(JNIEnv *env, jclass cl, jlong pKeyTypes, jint keyTypeCount, jlong capacity) {
    return reinterpret_cast<jlong>(alloc_rosti(reinterpret_cast<int32_t *>(pKeyTypes), keyTypeCount, capacity));
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_free0(JNIEnv *env, jclass cl, jlong pRosti) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    // initial values contains main arena pointer
    free(map->slot_initial_values_);
    free(map->value_offsets_);
    free(map);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_clear(JNIEnv *env, jclass cl, jlong pRosti) {
    clear(reinterpret_cast<rosti_t *>(pRosti));
}

}

