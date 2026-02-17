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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.AbstractRedBlackTree;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

/**
 * Values are stored on a heap. Value chain addresses are 4-byte aligned.
 */
public class LongTreeChain extends AbstractRedBlackTree implements Reopenable {
    private static final long CHAIN_VALUE_SIZE = 12;
    private static final long MAX_VALUE_HEAP_SIZE_LIMIT = (Integer.toUnsignedLong(-1) - 1) << 2;
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
            maxValueHeapSize = Math.min(valuePageSize * valueMaxPages, MAX_VALUE_HEAP_SIZE_LIMIT);
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

        int offset = root;
        int parent;
        int cmp;
        do {
            parent = offset;
            final int ref = refOf(offset);
            sourceCursor.recordAt(rightRecord, rowId(ref));
            cmp = comparator.compare(rightRecord);
            if (cmp < 0) {
                offset = leftOf(offset);
            } else if (cmp > 0) {
                offset = rightOf(offset);
            } else {
                final int oldChainEnd = lastRefOf(offset);
                final int newChainEnd = appendNewValue(leftRecord.getRowId());
                setNextValueOffset(oldChainEnd, newChainEnd);
                setLastRef(offset, newChainEnd);
                return;
            }
        } while (offset > -1);

        offset = allocateBlock();
        setParent(offset, parent);

        final int chainStart = appendNewValue(leftRecord.getRowId());
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

    private static int compressValueOffset(long rawOffset) {
        return (int) (rawOffset >> 2);
    }

    private static long uncompressValueOffset(int offset) {
        return ((long) offset) << 2;
    }

    private int appendNewValue(long rowId) {
        checkValueCapacity();
        final int offset = compressValueOffset(valueHeapPos - valueHeapStart);
        Unsafe.getUnsafe().putLong(valueHeapPos, rowId);
        Unsafe.getUnsafe().putInt(valueHeapPos + 8, -1);
        valueHeapPos += CHAIN_VALUE_SIZE;
        return offset;
    }

    private void checkValueCapacity() {
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

    private int nextValueOffset(int valueOffset) {
        return Unsafe.getUnsafe().getInt(valueHeapStart + uncompressValueOffset(valueOffset) + 8);
    }

    private void putParent(long rowId) {
        root = allocateBlock();
        final int chainStart = appendNewValue(rowId);
        setRef(root, chainStart);
        setLastRef(root, chainStart);
        setParent(root, -1);
    }

    private long rowId(int valueOffset) {
        return Unsafe.getUnsafe().getLong(valueHeapStart + uncompressValueOffset(valueOffset));
    }

    private void setNextValueOffset(int valueOffset, int nextValueOffset) {
        Unsafe.getUnsafe().putInt(valueHeapStart + uncompressValueOffset(valueOffset) + 8, nextValueOffset);
    }

    public class TreeCursor {
        private int chainCurrent;
        private int treeCurrent;

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
            int result = chainCurrent;
            chainCurrent = nextValueOffset(chainCurrent);
            return rowId(result);
        }

        public void toTop() {
            setup();
        }

        private void setup() {
            int p = root;
            if (p != -1) {
                while (leftOf(p) != -1) {
                    p = leftOf(p);
                }
            }
            chainCurrent = refOf(treeCurrent = p);
        }
    }
}
