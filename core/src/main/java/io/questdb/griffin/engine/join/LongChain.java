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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.Reopenable;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * A native memory heap-based chain of long values. Used to store row id lists in hash joins.
 * <p>
 * For each long value also stores compressed offset for its parent (previous in the chain) value.
 * A compressed offset contains an offset to the address of the parent value in the heap memory
 * compressed to an int. Value addresses are 4-byte aligned.
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

    public LongChain(long valuePageSize, int valueMaxPages) {
        heapSize = initialHeapSize = valuePageSize;
        heapStart = heapPos = Unsafe.malloc(heapSize, MemoryTag.NATIVE_DEFAULT);
        heapLimit = heapStart + heapSize;
        maxHeapSize = Math.min(valuePageSize * valueMaxPages, MAX_HEAP_SIZE_LIMIT);
    }

    @Override
    public void clear() {
        heapPos = heapStart;
    }

    @Override
    public void close() {
        if (heapStart != 0) {
            heapStart = Unsafe.free(heapStart, heapSize, MemoryTag.NATIVE_DEFAULT);
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
        Unsafe.getUnsafe().putLong(heapPos, value);
        Unsafe.getUnsafe().putInt(heapPos + 8, parentOffset);
        heapPos += CHAIN_VALUE_SIZE;
        return appendOffset;
    }

    @Override
    public void reopen() {
        if (heapStart == 0) {
            heapSize = initialHeapSize;
            heapStart = heapPos = Unsafe.malloc(heapSize, MemoryTag.NATIVE_DEFAULT);
            heapLimit = heapStart + heapSize;
        }
    }

    private static int compressOffset(long rawOffset) {
        return (int) (rawOffset >> 2);
    }

    private static long uncompressOffset(int offset) {
        return ((long) offset) << 2;
    }

    private void checkCapacity() {
        if (heapPos + CHAIN_VALUE_SIZE > heapLimit) {
            final long newHeapSize = heapSize << 1;
            if (newHeapSize > maxHeapSize) {
                throw LimitOverflowException.instance().put("limit of ").put(maxHeapSize).put(" memory exceeded in LongChain");
            }
            long newHeapPos = Unsafe.realloc(heapStart, heapSize, newHeapSize, MemoryTag.NATIVE_DEFAULT);

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
            final long value = Unsafe.getUnsafe().getLong(heapStart + rawOffset);
            nextOffset = Unsafe.getUnsafe().getInt(heapStart + rawOffset + 8);
            return value;
        }

        void of(int startOffset) {
            this.nextOffset = startOffset;
        }
    }
}
