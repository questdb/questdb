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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.AbstractRedBlackTree;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

/**
 * Values are stored on a heap. Value chain addresses are 4-byte aligned.
 */
public class LongTreeChain extends AbstractRedBlackTree implements Reopenable {
    // Marks the end of a node's value chain. A different namespace from the tree's EMPTY block
    // sentinel, which happens to share the same numeric value.
    private static final int CHAIN_END = -1;
    private static final long CHAIN_VALUE_SIZE = 12;
    // Upper bound enforced by the compressed-offset encoding (offsets are 4-byte-aligned and
    // stored as 32-bit ints), independent of any user-supplied byte cap.
    private static final long MAX_VALUE_HEAP_SIZE_LIMIT = (Integer.toUnsignedLong(-1) - 1) << 2;
    private final TreeCursor cursor = new TreeCursor();
    private final long initialValueHeapSize;
    private final long maxValueHeapSize;
    private final String valueHeapConfigKey;
    private long valueHeapLimit;
    private long valueHeapPos;
    private long valueHeapSize;
    private long valueHeapStart;

    public LongTreeChain(
            long keyPageSize,
            long maxKeyHeapBytes,
            long valuePageSize,
            long maxValueHeapBytes,
            String keyHeapConfigKey,
            String valueHeapConfigKey
    ) {
        this(keyPageSize, maxKeyHeapBytes, valuePageSize, maxValueHeapBytes, keyHeapConfigKey, valueHeapConfigKey, true);
    }

    public LongTreeChain(
            long keyPageSize,
            long maxKeyHeapBytes,
            long valuePageSize,
            long maxValueHeapBytes,
            String keyHeapConfigKey,
            String valueHeapConfigKey,
            boolean openOnInit
    ) {
        super(keyPageSize, maxKeyHeapBytes, keyHeapConfigKey, openOnInit);
        try {
            // value page must hold at least one chain entry (config rejects sub-block sizes).
            assert valuePageSize >= CHAIN_VALUE_SIZE;
            valueHeapSize = initialValueHeapSize = valuePageSize;
            maxValueHeapSize = Math.min(Math.max(maxValueHeapBytes, valuePageSize), MAX_VALUE_HEAP_SIZE_LIMIT);
            this.valueHeapConfigKey = valueHeapConfigKey;
            if (openOnInit) {
                valueHeapStart = valueHeapPos = Unsafe.malloc(valueHeapSize, MemoryTag.NATIVE_TREE_CHAIN, memoryTracker);
                valueHeapLimit = valueHeapStart + valueHeapSize;
            }
            // else: valueHeapStart stays 0; first reopen() allocates initial backing
            // under whatever MemoryTracker is bound at that time.
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
            valueHeapStart = Unsafe.free(valueHeapStart, valueHeapSize, MemoryTag.NATIVE_TREE_CHAIN, memoryTracker);
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
        put(leftRecord, sourceCursor, rightRecord, comparator, leftRecord.getRowId());
    }

    /**
     * Inserts a row whose stored rowId is provided explicitly, decoupled from
     * {@code leftRecord.getRowId()}. Callers that index their records by a
     * different key (e.g. a dense rowIndex, not the underlying base rowId)
     * use this overload so {@code sourceCursor.recordAt} sees the right key.
     */
    public void put(
            Record leftRecord,
            RecordCursor sourceCursor,
            Record rightRecord,
            RecordComparator comparator,
            long rowId
    ) {
        if (root == EMPTY) {
            putParent(rowId);
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
                final int newChainEnd = appendNewValue(rowId);
                setNextValueOffset(oldChainEnd, newChainEnd);
                setLastRef(offset, newChainEnd);
                return;
            }
        } while (offset != EMPTY);

        offset = allocateBlock();
        setParent(offset, parent);

        final int chainStart = appendNewValue(rowId);
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
            valueHeapStart = valueHeapPos = Unsafe.malloc(valueHeapSize, MemoryTag.NATIVE_TREE_CHAIN, memoryTracker);
            valueHeapLimit = valueHeapStart + valueHeapSize;
        }
    }

    private int appendNewValue(long rowId) {
        checkValueCapacity();
        final int offset = CompressedOffsets.compressAligned4(valueHeapPos - valueHeapStart);
        Unsafe.putLong(valueHeapPos, rowId);
        Unsafe.putInt(valueHeapPos + 8, CHAIN_END);
        valueHeapPos += CHAIN_VALUE_SIZE;
        return offset;
    }

    private void checkValueCapacity() {
        if (valueHeapStart == 0) {
            // See AbstractRedBlackTree.checkKeyCapacity: the heaps are unallocated before the
            // first reopen() and after close(), and valueHeapSize still carries the configured
            // page size in the never-opened case, so growing from here would book a delta against
            // memory nothing ever charged.
            reopen();
        }
        if (valueHeapPos + CHAIN_VALUE_SIZE > valueHeapLimit) {
            final long required = valueHeapPos - valueHeapStart + CHAIN_VALUE_SIZE;
            // Doubling alone falls short whenever the heap is smaller than one value, which the
            // config floors rule out but they do not run in every embedding.
            long newHeapSize = Math.max(valueHeapSize << 1, required);
            if (newHeapSize > maxValueHeapSize) {
                if (required > maxValueHeapSize) {
                    LimitOverflowException ex = LimitOverflowException.instance();
                    ex.put("limit of ").put(maxValueHeapSize).put(" memory exceeded in LongTreeChain");
                    if (valueHeapConfigKey != null) {
                        ex.put(" (raise ").put(valueHeapConfigKey).put(')');
                    }
                    throw ex;
                }
                // Doubling overshoots a cap that is rarely a power of two, so rejecting here
                // would strand part of the configured budget: the largest reachable heap would be
                // the largest pageSize * 2^k not exceeding the cap. The value we have to fit still
                // fits, so clamp to the cap instead.
                newHeapSize = maxValueHeapSize;
            }
            long newHeapPos = Unsafe.realloc(valueHeapStart, valueHeapSize, newHeapSize, MemoryTag.NATIVE_TREE_CHAIN, memoryTracker);

            valueHeapSize = newHeapSize;
            long delta = newHeapPos - valueHeapStart;
            valueHeapPos += delta;

            this.valueHeapStart = newHeapPos;
            this.valueHeapLimit = newHeapPos + newHeapSize;
        }
    }

    private int nextValueOffset(int valueOffset) {
        assert valueOffset != CHAIN_END;
        return Unsafe.getInt(valueHeapStart + CompressedOffsets.uncompressAligned4(valueOffset) + 8);
    }

    private void putParent(long rowId) {
        root = allocateBlock();
        final int chainStart = appendNewValue(rowId);
        setRef(root, chainStart);
        setLastRef(root, chainStart);
        setParent(root, EMPTY);
    }

    private long rowId(int valueOffset) {
        assert valueOffset != CHAIN_END;
        return Unsafe.getLong(valueHeapStart + CompressedOffsets.uncompressAligned4(valueOffset));
    }

    private void setNextValueOffset(int valueOffset, int nextValueOffset) {
        assert valueOffset != CHAIN_END;
        Unsafe.putInt(valueHeapStart + CompressedOffsets.uncompressAligned4(valueOffset) + 8, nextValueOffset);
    }

    public class TreeCursor {
        private int chainCurrent;
        private int treeCurrent;

        public void clear() {
            treeCurrent = EMPTY;
            chainCurrent = CHAIN_END;
        }

        public boolean hasNext() {
            if (chainCurrent != CHAIN_END) {
                return true;
            }

            treeCurrent = successor(treeCurrent);
            if (treeCurrent == EMPTY) {
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
            if (p != EMPTY) {
                while (leftOf(p) != EMPTY) {
                    p = leftOf(p);
                }
            }
            chainCurrent = refOf(treeCurrent = p);
        }
    }
}
