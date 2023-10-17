/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

#include "byte_sink.h"
#include <stdlib.h>
#include <jni.h>

static questdb_byte_sink_t* create(uint64_t capacity) {
    questdb_byte_sink_t* sink = (questdb_byte_sink_t*) malloc(sizeof(questdb_byte_sink_t));
    if (sink == NULL) {
        return NULL;
    }
    sink->buf = (uint8_t*) malloc(capacity);
    if (sink->buf == NULL) {
        free(sink);
        return NULL;
    }
    sink->capacity = capacity;
    sink->pos = 0;
    return sink;
}

static void destroy(questdb_byte_sink_t* sink) {
    free(sink->buf);
    free(sink);
}

static uint64_t remaining(uint64_t capacity, uint64_t pos) {
    return capacity - pos;
}

uint8_t* questdb_byte_sink_book(questdb_byte_sink_t* sink, uint64_t min_len) {
    const uint64_t pos = sink->pos;
    int64_t capacity = sink->capacity;

    if (remaining(capacity, pos) >= min_len) {
        return sink->buf + pos;
    }

    do {
        capacity *= 2;
    } while (remaining(capacity, pos) < min_len);

    sink->buf = (uint8_t*) realloc(sink->buf, capacity);
    if (sink->buf == NULL) {
        return NULL;
    }
    sink->capacity = capacity;
    return sink->buf + pos;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_str_DirectByteCharSink_create(
        JNIEnv *env,
        jclass cl,
        jlong capacity) {
    return (jlong) create(capacity);
}

JNIEXPORT void JNICALL Java_io_questdb_std_str_DirectByteCharSink_destroy(
        JNIEnv *env,
        jclass cl,
        jlong impl) {
    destroy((questdb_byte_sink_t*)impl);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_str_DirectByteCharSink_book(
        JNIEnv *env,
        jclass cl,
        jlong impl,
        jlong len) {
    return (jlong) questdb_byte_sink_book((questdb_byte_sink_t*)impl, (uint64_t)len);
}
