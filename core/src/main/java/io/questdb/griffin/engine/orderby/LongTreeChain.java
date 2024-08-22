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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.AbstractRedBlackTree;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

public class LongTreeChain extends AbstractRedBlackTree implements Reopenable {
    private static final long CHAIN_VALUE_SIZE = 16;
    private final TreeCursor cursor = new TreeCursor();
    private final long initialValueHeapSize;
    private final long maxValueHeapSize;
    private long valueHeapLimit;
    private long valueHeapPos;
    private long valueHeapSize;
    private long valueHeapStart;

    public LongTreeChain(long keyPageSize, int keyMaxPages, long valuePageSize, int valueMaxPages) {
        super(keyPageSize, keyMaxPages);
        try {
            valueHeapSize = initialValueHeapSize = valuePageSize;
            valueHeapStart = valueHeapPos = Unsafe.malloc(valueHeapSize, MemoryTag.NATIVE_TREE_CHAIN);
            valueHeapLimit = valueHeapStart + valueHeapSize;
            maxValueHeapSize = valuePageSize * valueMaxPages;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        super.clear();
        valueHeapPos = valueHeapStart;
    }

    @Override
    public void close() {
        super.close();
        cursor.clear();
        if (valueHeapStart != 0) {
            valueHeapStart = Unsafe.free(valueHeapStart, valueHeapSize, MemoryTag.NATIVE_TREE_CHAIN);
            valueHeapLimit = valueHeapPos = 0;
            valueHeapSize = 0;
        }
    }

    public TreeCursor getCursor() {
        cursor.toTop();
        return cursor;
    }

    public void put(
            Record leftRecord,
            RecordCursor sourceCursor,
            Record rightRecord,
            RecordComparator comparator
    ) {
        if (root == -1) {
            putParent(leftRecord.getRowId());
            return;
        }

        comparator.setLeft(leftRecord);

        long offset = root;
        long parent;
        int cmp;
        do {
            parent = offset;
            final long ref = refOf(offset);
            sourceCursor.recordAt(rightRecord, Unsafe.getUnsafe().getLong(valueHeapStart + ref));
            cmp = comparator.compare(rightRecord);
            if (cmp < 0) {
                offset = leftOf(offset);
            } else if (cmp > 0) {
                offset = rightOf(offset);
            } else {
                long oldChainEnd = lastRefOf(offset);
                long newChainEnd = appendValue(leftRecord.getRowId(), -1);
                Unsafe.getUnsafe().putLong(valueHeapStart + oldChainEnd + Long.BYTES, newChainEnd);
                setLastRef(offset, newChainEnd);
                return;
            }
        } while (offset > -1);

        offset = allocateBlock();
        setParent(offset, parent);

        long chainStart = appendValue(leftRecord.getRowId(), -1L);
        setRef(offset, chainStart);
        setLastRef(offset, chainStart);

        if (cmp < 0) {
            setLeft(parent, offset);
        } else {
            setRight(parent, offset);
        }
        fixInsert(offset);
    }

    @Override
    public void reopen() {
        super.reopen();
        if (valueHeapStart == 0) {
            valueHeapSize = initialValueHeapSize;
            valueHeapStart = valueHeapPos = Unsafe.malloc(valueHeapSize, MemoryTag.NATIVE_TREE_CHAIN);
            valueHeapLimit = valueHeapStart + valueHeapSize;
        }
    }

    private long appendValue(long value, long nextValueOffset) {
        checkCapacity();
        final long offset = valueHeapPos - valueHeapStart;
        Unsafe.getUnsafe().putLong(valueHeapPos, value);
        Unsafe.getUnsafe().putLong(valueHeapPos + 8, nextValueOffset);
        valueHeapPos += CHAIN_VALUE_SIZE;
        return offset;
    }

    private void checkCapacity() {
        if (valueHeapPos + CHAIN_VALUE_SIZE > valueHeapLimit) {
            final long newHeapSize = valueHeapSize << 1;
            if (newHeapSize > maxValueHeapSize) {
                throw LimitOverflowException.instance().put("limit of ").put(maxValueHeapSize).put(" memory exceeded in LongTreeChain");
            }
            long newHeapPos = Unsafe.realloc(valueHeapStart, valueHeapSize, newHeapSize, MemoryTag.NATIVE_TREE_CHAIN);

            valueHeapSize = newHeapSize;
            long delta = newHeapPos - valueHeapStart;
            valueHeapPos += delta;

            this.valueHeapStart = newHeapPos;
            this.valueHeapLimit = newHeapPos + newHeapSize;
        }
    }

    @Override
    protected void putParent(long value) {
        root = allocateBlock();
        long chainStart = appendValue(value, -1L);
        setRef(root, chainStart);
        setLastRef(root, chainStart);
        setParent(root, -1);
    }

    public class TreeCursor {

        private long chainCurrent;
        private long treeCurrent;

        public void clear() {
            treeCurrent = -1;
            chainCurrent = -1;
        }

        public boolean hasNext() {
            if (chainCurrent != -1) {
                return true;
            }

            treeCurrent = successor(treeCurrent);
            if (treeCurrent == -1) {
                return false;
            }

            chainCurrent = refOf(treeCurrent);
            return true;
        }

        public long next() {
            long result = chainCurrent;
            chainCurrent = Unsafe.getUnsafe().getLong(valueHeapStart + chainCurrent + Long.BYTES);
            return Unsafe.getUnsafe().getLong(valueHeapStart + result);
        }

        public void toTop() {
            setup();
        }

        private void setup() {
            long p = root;
            if (p != -1) {
                while (leftOf(p) != -1) {
                    p = leftOf(p);
                }
            }
            chainCurrent = refOf(treeCurrent = p);
        }
    }
}
