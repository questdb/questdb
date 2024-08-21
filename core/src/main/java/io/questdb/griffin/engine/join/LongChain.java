/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

public class LongChain implements Closeable, Mutable, Reopenable {
    private static final long CHAIN_VALUE_SIZE = 16;
    private static final long MAX_HEAP_SIZE_LIMIT = (Integer.toUnsignedLong(-1) - 1) << 3;
    private final TreeCursor cursor = new TreeCursor();
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

    public TreeCursor getCursor(long tailOffset) {
        cursor.of(tailOffset);
        return cursor;
    }

    public long put(long value, long parentOffset) {
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
        final long appendOffset = heapPos - heapStart;
        if (parentOffset != -1) {
            Unsafe.getUnsafe().putLong(heapStart + parentOffset, appendOffset);
        }
        Unsafe.getUnsafe().putLong(heapPos, -1);
        Unsafe.getUnsafe().putLong(heapPos + 8, value);
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

    public class TreeCursor {
        private long nextOffset;

        public boolean hasNext() {
            return nextOffset != -1;
        }

        public long next() {
            final long next = Unsafe.getUnsafe().getLong(heapStart + nextOffset);
            final long value = Unsafe.getUnsafe().getLong(heapStart + nextOffset + 8);
            this.nextOffset = next;
            return value;
        }

        void of(long startOffset) {
            this.nextOffset = startOffset;
        }
    }
}
