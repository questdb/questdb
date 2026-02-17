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

#ifndef QUESTDB_BYTE_SINK_H
#define QUESTDB_BYTE_SINK_H

#include <cstddef>

#ifdef __GNUC__
#define PACK( __Declaration__ ) __Declaration__ __attribute__((__packed__))
#endif

#ifdef _MSC_VER
#define PACK( __Declaration__ ) __pragma( pack(push, 1) ) __Declaration__ __pragma( pack(pop))
#endif

/**
 * A growable byte buffer.
 *
 * To get current capacity `hi - lo`
 * To get the current used size `ptr - lo`
 * To get the current available size `hi - ptr`
 */
// N.B.: PACK here guarantees a predictable layout of the struct.
// We use this from Java Unsafe to access the fields.
PACK(struct questdb_byte_sink_t {
    // Pointer to the first writable byte.
    std::byte* ptr;

    // Start of the allocated buffer.
    std::byte* lo;

    // End of the allocated buffer.
    std::byte* hi;

    // Set to `true` if a `realloc` fails due to exceeding 2GiB.
    bool overflow;

    char _padding1[3];  // pad out boolean so it can be accessed via `Unsafe#getInt`.

    // This field is only used when processing a UTF-8 buffer.
    // The field can be safely ignored when processing binary data.
    // It is set from `true` (default) to `false` once the sink contains non-7-bit-ASCII bytes.
    bool ascii;
});

typedef struct questdb_byte_sink_t questdb_byte_sink_t;

// Ensure the sink has at least len bytes available.
// Returns a pointer to the start of the available bytes.
// or NULL if a memory allocation failed.
std::byte* questdb_byte_sink_book(questdb_byte_sink_t* sink, size_t min_len);

#endif // QUESTDB_BYTE_SINK_H
