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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRootBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointMutationArena;
import io.questdb.cairo.lv.LiveViewCheckpointOutputKeyDomain;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRootBuilder;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * Native copies of literal partition keys, for the checkpoint APIs that take a key as an
 * {@code (address, length)} pair valid for the call.
 * <p>
 * An instance holds one copy and reuses its memory: {@link #of(byte[])} rewrites it, so an
 * address it hands out is valid until the next {@link #of(byte[])} or {@link #close()}. Use
 * it in try-with-resources where a case feeds many keys or measures allocations. The static
 * forms copy the key into memory of their own for one call and free it before they return,
 * for a case that stages or probes a key once.
 */
final class LiveViewCheckpointTestKeys implements QuietCloseable {
    private long address;
    private long capacity;
    private int length;

    static boolean contains(@NotNull LiveViewCheckpointOutputKeyDomain domain, byte @NotNull [] key) {
        final long address = copy(key);
        try {
            return domain.contains(address, key.length);
        } finally {
            free(address, key);
        }
    }

    static void domain(@NotNull LiveViewCheckpointMutationArena arena, byte @NotNull [] key) {
        final long address = copy(key);
        try {
            arena.domain(address, key.length);
        } finally {
            free(address, key);
        }
    }

    static boolean find(
            @NotNull LiveViewCheckpointPartitionMapReader reader,
            @NotNull LiveViewCheckpointPageRef root,
            byte @NotNull [] key,
            @NotNull LiveViewCheckpointPartitionMapEntry out
    ) {
        final long address = copy(key);
        try {
            return reader.find(root, address, key.length, out);
        } finally {
            free(address, key);
        }
    }

    static void freeze(
            @NotNull LiveViewCheckpointRangeRingStateBuilder builder,
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            byte @NotNull [] key,
            long scalarWord0,
            long scalarWord1,
            long scalarWord2,
            long scalarWord3,
            long frameSize,
            @NotNull LiveViewCheckpointPartitionMapEntry out
    ) {
        final long address = copy(key);
        try {
            builder.freeze(writer, address, key.length, scalarWord0, scalarWord1, scalarWord2, scalarWord3, frameSize, out);
        } finally {
            free(address, key);
        }
    }

    static LiveViewCheckpointPartitionMapEntry of(
            @NotNull LiveViewCheckpointPartitionMapEntry entry,
            byte @NotNull [] key,
            byte @NotNull [] scalarState,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        final long address = copy(key);
        try {
            return entry.of(address, key.length, scalarState, statePageRefs);
        } finally {
            free(address, key);
        }
    }

    static void put(
            @NotNull LiveViewCheckpointMutationArena arena,
            byte @NotNull [] key,
            byte @NotNull [] scalarState,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        final long address = copy(key);
        try {
            arena.put(address, key.length, scalarState, statePageRefs);
        } finally {
            free(address, key);
        }
    }

    static void put(@NotNull LiveViewCheckpointMutationArena arena, byte @NotNull [] key, byte @NotNull [] scalarState) {
        final long address = copy(key);
        try {
            arena.put(address, key.length, scalarState);
        } finally {
            free(address, key);
        }
    }

    static void putPartition(
            @NotNull LiveViewCheckpointFunctionRootBuilder builder,
            byte @NotNull [] key,
            byte @NotNull [] scalarState,
            @NotNull LiveViewCheckpointStatePageRef[] statePageRefs
    ) {
        final long address = copy(key);
        try {
            builder.putPartition(address, key.length, scalarState, statePageRefs);
        } finally {
            free(address, key);
        }
    }

    static void putPartition(
            @NotNull LiveViewCheckpointWindowRootBuilder builder,
            byte @NotNull [] key,
            byte @NotNull [] scalarState,
            boolean isUnchanged
    ) {
        final long address = copy(key);
        try {
            builder.putPartition(address, key.length, scalarState, isUnchanged);
        } finally {
            free(address, key);
        }
    }

    static void remove(@NotNull LiveViewCheckpointMutationArena arena, byte @NotNull [] key) {
        final long address = copy(key);
        try {
            arena.remove(address, key.length);
        } finally {
            free(address, key);
        }
    }

    static void removePartition(@NotNull LiveViewCheckpointFunctionRootBuilder builder, byte @NotNull [] key) {
        final long address = copy(key);
        try {
            builder.removePartition(address, key.length);
        } finally {
            free(address, key);
        }
    }

    static void removePartition(@NotNull LiveViewCheckpointWindowRootBuilder builder, byte @NotNull [] key) {
        final long address = copy(key);
        try {
            builder.removePartition(address, key.length);
        } finally {
            free(address, key);
        }
    }

    /**
     * Frees the copy. Idempotent; the instance stays usable.
     */
    @Override
    public void close() {
        if (address != 0) {
            Unsafe.free(address, capacity, MemoryTag.NATIVE_DEFAULT);
            address = 0;
            capacity = 0;
        }
        length = 0;
    }

    /**
     * @return the address of the key {@link #of(byte[])} last copied, valid until the next
     * {@link #of(byte[])} or {@link #close()}
     */
    long address() {
        return address;
    }

    int length() {
        return length;
    }

    /**
     * Copies {@code key} over the previous one, growing the memory when it does not fit.
     */
    LiveViewCheckpointTestKeys of(byte @NotNull [] key) {
        final long required = Math.max(1, key.length);
        if (required > capacity) {
            address = address == 0
                    ? Unsafe.malloc(required, MemoryTag.NATIVE_DEFAULT)
                    : Unsafe.realloc(address, capacity, required, MemoryTag.NATIVE_DEFAULT);
            capacity = required;
        }
        Unsafe.copyMemory(key, Unsafe.BYTE_OFFSET, null, address, key.length);
        length = key.length;
        return this;
    }

    private static long copy(byte[] key) {
        final long address = Unsafe.malloc(Math.max(1, key.length), MemoryTag.NATIVE_DEFAULT);
        Unsafe.copyMemory(key, Unsafe.BYTE_OFFSET, null, address, key.length);
        return address;
    }

    private static void free(long address, byte[] key) {
        Unsafe.free(address, Math.max(1, key.length), MemoryTag.NATIVE_DEFAULT);
    }
}
