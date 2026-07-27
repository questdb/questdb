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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.Reopenable;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * A native memory heap-based chain of long values. Used to store row id lists in hash joins.
 * <p>
 * For each long value also stores compressed offset for its parent (previous in the chain) value.
 * A compressed offset contains an offset to the address of the parent value in the heap memory
 * compressed to an int. Value addresses are 4-byte aligned. Compressed offsets are unsigned,
 * with -1 reserved as the end-of-chain sentinel, so the chain end must be tested as
 * {@code == -1}, never as {@code < 0}.
 */
public class LongChain implements Closeable, Mutable, Reopenable {
    private static final long CHAIN_VALUE_SIZE = 12;
    private static final long MAX_HEAP_SIZE_LIMIT = (Integer.toUnsignedLong(-1) - 1) << 2;
    private final Cursor cursor = new Cursor();
    private final long initialHeapSize;
    private final long maxHeapSize;
    private long heapLimit;
    private long heapPos;
    private long heapSize;
    private long heapStart;
    // Per-query native memory tracker bound by the owning cursor before the
    // backing heap is (re)allocated. Null when no per-query limit applies; all
    // Unsafe.{malloc,realloc,free} calls degrade to the global-only overloads.
    @Nullable
    private MemoryTracker memoryTracker;

    public LongChain(long valuePageSize, int valueMaxPages) {
        this(valuePageSize, valueMaxPages, false);
    }

    public LongChain(long valuePageSize, int valueMaxPages, boolean keepClosed) {
        assert valuePageSize >= CHAIN_VALUE_SIZE;
        this.initialHeapSize = valuePageSize;
        this.maxHeapSize = Math.min(valuePageSize * valueMaxPages, MAX_HEAP_SIZE_LIMIT);
        if (!keepClosed) {
            heapSize = initialHeapSize;
            heapStart = heapPos = Unsafe.malloc(heapSize, MemoryTag.NATIVE_DEFAULT, memoryTracker);
            heapLimit = heapStart + heapSize;
        }
    }

    @Override
    public void clear() {
        heapPos = heapStart;
    }

    @Override
    public void close() {
        if (heapStart != 0) {
            heapStart = Unsafe.free(heapStart, heapSize, MemoryTag.NATIVE_DEFAULT, memoryTracker);
            heapLimit = heapPos = 0;
            heapSize = 0;
        }
    }

    public Cursor getCursor(int tailOffset) {
        cursor.of(tailOffset);
        return cursor;
    }

    public int put(long value, int parentOffset) {
        checkCapacity();

        final long appendRawOffset = heapPos - heapStart;
        final int appendOffset = compressOffset(appendRawOffset);
        Unsafe.putLong(heapPos, value);
        Unsafe.putInt(heapPos + 8, parentOffset);
        heapPos += CHAIN_VALUE_SIZE;
        return appendOffset;
    }

    @Override
    public void reopen() {
        if (heapStart == 0) {
            heapSize = initialHeapSize;
            heapStart = heapPos = Unsafe.malloc(heapSize, MemoryTag.NATIVE_DEFAULT, memoryTracker);
            heapLimit = heapStart + heapSize;
        }
    }

    public void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    private static int compressOffset(long rawOffset) {
        return (int) (rawOffset >> 2);
    }

    // Compressed offsets are unsigned: values at or past the 8GB mark have the top bit set.
    private static long uncompressOffset(int offset) {
        return Integer.toUnsignedLong(offset) << 2;
    }

    private void checkCapacity() {
        if (heapPos + CHAIN_VALUE_SIZE > heapLimit) {
            final long required = heapPos - heapStart + CHAIN_VALUE_SIZE;
            if (required > maxHeapSize) {
                throw LimitOverflowException.instance().put("limit of ").put(maxHeapSize).put(" memory exceeded in LongChain");
            }
            // Take required into account rather than trusting the doubling alone. Doubling covers
            // it whenever heapSize >= CHAIN_VALUE_SIZE, but this class - unlike the tree chains,
            // which set their heap size in the constructor either way - leaves heapSize at 0 until
            // reopen(), so a put() on a keepClosed chain would otherwise realloc to 0 and write
            // 12 bytes at address 0. It also covers a sub-block page size, which config validation
            // and the constructor's assert rule out but neither runs in every embedding.
            long newHeapSize = Math.max(heapSize << 1, required);
            // Doubling overshoots a cap that is rarely a power of two, and the throw above has
            // already established that the value we have to fit does fit under the cap. Clamp
            // instead of rejecting, otherwise the largest reachable heap is the largest power of
            // two below the cap and up to half of the configured budget stays unused.
            if (newHeapSize > maxHeapSize) {
                newHeapSize = maxHeapSize;
            }
            long newHeapPos = Unsafe.realloc(heapStart, heapSize, newHeapSize, MemoryTag.NATIVE_DEFAULT, memoryTracker);

            heapSize = newHeapSize;
            long delta = newHeapPos - heapStart;
            heapPos += delta;

            this.heapStart = newHeapPos;
            this.heapLimit = newHeapPos + newHeapSize;
        }
    }

    public class Cursor {
        private int nextOffset;

        public boolean hasNext() {
            return nextOffset != -1;
        }

        public long next() {
            final long rawOffset = uncompressOffset(nextOffset);
            final long value = Unsafe.getLong(heapStart + rawOffset);
            nextOffset = Unsafe.getInt(heapStart + rawOffset + 8);
            return value;
        }

        void of(int startOffset) {
            this.nextOffset = startOffset;
        }
    }
}
