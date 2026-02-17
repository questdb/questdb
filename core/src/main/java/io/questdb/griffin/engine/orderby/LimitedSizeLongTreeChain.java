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
import io.questdb.cairo.sql.RecordRandomAccess;
import io.questdb.griffin.engine.AbstractRedBlackTree;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.TestOnly;

/**
 * LongTreeChain with a size limit - used to keep only the necessary records
 * instead of whole result set for queries with "limit L | limit L, H"  clause.
 * <pre>
 * 1. "limit L" means we only need to keep:
 * L &gt;= 0 - first L records
 * L &lt; 0  - last L records
 * 2. "limit L, H" means we need to keep:
 * L &lt; 0          - last  L records (but skip last H records, if H &gt;=0 then don't skip anything)
 * L &gt;= 0, H &gt;= 0 - first H records (but skip first L later, if H &lt;= L then return empty set)
 * L &gt;= 0, H &lt; 0  - we can't optimize this case (because it spans from record L-th from the beginning up to
 * H-th from the end, and we don't) and need to revert to default behavior -
 * produce the whole set and skip.
 * </pre>
 * TreeChain stores repeating values (rowids) on value heap as a linked list:
 * <pre>
 * [latest rowid, offset to next] -&gt; [old rowid, offset to next] -&gt; [oldest rowid, -1L]
 * </pre>
 * -1 - marks end of current node's value chain.
 * -2 - marks an unused element on the value chain list for the current tree node
 * but should only happen once. It's meant to limit value chain allocations on delete/insert.
 * <p>
 * Values are stored on a heap. Value chain addresses are 4-byte aligned.
 */
public class LimitedSizeLongTreeChain extends AbstractRedBlackTree implements Reopenable {
    // value marks end of value chain
    private static final int CHAIN_END = -1;
    private static final long CHAIN_VALUE_SIZE = 12;
    // marks value chain entry as unused (belonging to a node on the freelist)
    // it's meant to avoid unnecessary reallocations when removing nodes and adding nodes
    private static final long FREE_SLOT = -2;
    private static final long MAX_VALUE_HEAP_SIZE_LIMIT = (Integer.toUnsignedLong(-1) - 1) << 2;
    // LIFO list of free blocks to reuse, allocated on the value chain
    private final DirectIntList chainFreeList;
    private final LimitedSizeLongTreeChain.TreeCursor cursor = new LimitedSizeLongTreeChain.TreeCursor();
    // LIFO list of nodes to reuse, instead of releasing and reallocating
    private final DirectIntList freeList;
    private final long initialValueHeapSize;
    private final long maxValueHeapSize;
    // number of all values stored in tree (including repeating ones)
    private int currentValues = 0;
    // firstN - keep <first->N> set , otherwise keep <last-N->last> set
    private boolean isFirstN;
    // maximum number of values tree can store (including repeating values)
    private long limit; // -1 means 'almost' unlimited
    private int minMaxNode = -1;
    // for fast filtering out of records in here we store rowId of:
    //  - record with max value for firstN/bottomN query
    //  - record with min value for lastN/topN query
    private long minMaxRowId = -1;
    private long valueHeapLimit;
    private long valueHeapPos;
    private long valueHeapSize;
    private long valueHeapStart;

    public LimitedSizeLongTreeChain(long keyPageSize, int keyMaxPages, long valuePageSize, int valueMaxPages) {
        super(keyPageSize, keyMaxPages);
        try {
            freeList = new DirectIntList(16, MemoryTag.NATIVE_TREE_CHAIN);
            chainFreeList = new DirectIntList(16, MemoryTag.NATIVE_TREE_CHAIN);
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
        minMaxRowId = -1;
        minMaxNode = -1;
        currentValues = 0;
        cursor.clear();
        freeList.clear();
        chainFreeList.clear();
    }

    @Override
    public void close() {
        super.close();
        clear();
        Misc.free(freeList);
        Misc.free(chainFreeList);
        if (valueHeapStart != 0) {
            valueHeapStart = Unsafe.free(valueHeapStart, valueHeapSize, MemoryTag.NATIVE_TREE_CHAIN);
            valueHeapLimit = valueHeapPos = 0;
            valueHeapSize = 0;
        }
    }

    // returns offset of node containing searchRecord; otherwise returns -1
    @TestOnly
    public int find(
            Record searchedRecord,
            RecordCursor sourceCursor,
            Record placeholder,
            RecordComparator comparator
    ) {
        comparator.setLeft(searchedRecord);

        if (root == -1) {
            return -1;
        }

        int p = root;
        int cmp;
        do {
            sourceCursor.recordAt(placeholder, rowId(refOf(p)));
            cmp = comparator.compare(placeholder);
            if (cmp < 0) {
                p = leftOf(p);
            } else if (cmp > 0) {
                p = rightOf(p);
            } else {
                return p;
            }
        } while (p > -1);

        return -1;
    }

    public LimitedSizeLongTreeChain.TreeCursor getCursor() {
        cursor.toTop();
        return cursor;
    }

    @TestOnly
    public void print(Utf16Sink sink) {
        print(sink, null);
    }

    // prints tree in-order, horizontally
    public void print(Utf16Sink sink, ValuePrinter printer) {
        if (root == EMPTY) {
            sink.put("[EMPTY TREE]");
        } else {
            if (printer == null) {
                printer = ValuePrinter::toRowId;
            }
            printTree(sink, root, 0, false, printer);
        }
    }

    /**
     * Inserts record into the tree. If tree is full and record is bigger/smaller than the smallest/biggest
     * record in the tree, then it will be inserted and smallest/biggest record will be removed.
     * <p>
     * <strong>important invariant:</strong>
     * when <code>(maxValues == currentValues)</code> then upon returning from this method the comparator left side must be set
     * to the ownedRecord with the max/min rowId.
     *
     * @param currentRecord record to insert into the tree
     * @param sourceCursor  cursor to get record from
     * @param ownedRecord   record to store data in. This record is owned by the tree and it must not be rewinded externally.
     * @param comparator    comparator to compare records
     */
    public void put(
            Record currentRecord,
            RecordRandomAccess sourceCursor,
            Record ownedRecord,
            RecordComparator comparator
    ) {
        if (limit == 0) {
            return;
        }

        // if maxValues < 0 then there's no limit (unless there's more than 2^64 records, which is unlikely)
        if (limit == currentValues) {
            int cmp = comparator.compare(currentRecord);

            if (isFirstN && cmp <= 0) { // bigger than max for firstN/bottomN
                return;
            } else if (!isFirstN && cmp >= 0) { // smaller than min for lastN/topN
                return;
            } else { // record has to be inserted, so we've to remove current minMax
                removeAndCache(minMaxNode);
            }
        }

        if (root == EMPTY) {
            long currentRecordRowId = currentRecord.getRowId();
            putParent(currentRecordRowId);
            minMaxNode = root;
            minMaxRowId = currentRecordRowId;
            currentValues++;
            prepareComparatorLeftSideIfAtMaxCapacity(sourceCursor, ownedRecord, comparator);
            return;
        }

        // ok, we need to insert new record into already existing tree
        // let's optimize for tree-traversal
        comparator.setLeft(currentRecord);

        int p = root;
        int parent;
        int cmp;
        do {
            parent = p;
            final int r = refOf(p);
            final long rowId = rowId(r);
            sourceCursor.recordAt(ownedRecord, rowId);
            cmp = comparator.compare(ownedRecord);
            if (cmp < 0) {
                p = leftOf(p);
            } else if (cmp > 0) {
                p = rightOf(p);
            } else {
                setRef(p, appendValue(currentRecord.getRowId(), r)); // appends value to chain, minMax shouldn't change
                if (minMaxRowId == -1) {
                    refreshMinMaxNode();
                }
                currentValues++;
                prepareComparatorLeftSideIfAtMaxCapacity(sourceCursor, ownedRecord, comparator);
                return;
            }
        } while (p > -1);

        p = allocateBlock(parent, currentRecord.getRowId());

        if (cmp < 0) {
            setLeft(parent, p);
        } else {
            setRight(parent, p);
        }

        fixInsert(p);
        refreshMinMaxNode();
        currentValues++;
        prepareComparatorLeftSideIfAtMaxCapacity(sourceCursor, ownedRecord, comparator);
    }

    // remove node and put on freelist (if holds only one value in chain)
    public void removeAndCache(int node) {
        if (hasMoreThanOneValue(node)) {
            removeMostRecentChainValue(node); // don't change minMax
        } else {
            int nodeToRemove = super.remove(node);
            clearBlock(nodeToRemove);
            freeList.add(nodeToRemove); // keep node on freelist to minimize allocations

            minMaxRowId = -1; // re-compute after inserting, there's no point doing it now
            minMaxNode = -1;
        }

        currentValues--;
    }

    @Override
    public void reopen() {
        super.reopen();
        freeList.reopen();
        chainFreeList.reopen();
        if (valueHeapStart == 0) {
            valueHeapSize = initialValueHeapSize;
            valueHeapStart = valueHeapPos = Unsafe.malloc(valueHeapSize, MemoryTag.NATIVE_TREE_CHAIN);
            valueHeapLimit = valueHeapStart + valueHeapSize;
        }
    }

    @Override
    public long size() {
        return currentValues;
    }

    public void updateLimits(boolean isFirstN, long limit) {
        this.isFirstN = isFirstN;
        this.limit = limit;
    }

    private static int compressValueOffset(long rawOffset) {
        return (int) (rawOffset >> 2);
    }

    private static long uncompressValueOffset(int offset) {
        return ((long) offset) << 2;
    }

    private int appendValue(long value, int prevValueOffset) {
        checkValueCapacity();
        final int offset = compressValueOffset(valueHeapPos - valueHeapStart);
        Unsafe.getUnsafe().putLong(valueHeapPos, value);
        Unsafe.getUnsafe().putInt(valueHeapPos + 8, prevValueOffset);
        valueHeapPos += CHAIN_VALUE_SIZE;
        return offset;
    }

    private void checkValueCapacity() {
        if (valueHeapPos + CHAIN_VALUE_SIZE > valueHeapLimit) {
            final long newHeapSize = valueHeapSize << 1;
            if (newHeapSize > maxValueHeapSize) {
                throw LimitOverflowException.instance().put("limit of ").put(maxValueHeapSize).put(" memory exceeded in LimitedSizeLongTreeChain");
            }
            long newHeapPos = Unsafe.realloc(valueHeapStart, valueHeapSize, newHeapSize, MemoryTag.NATIVE_TREE_CHAIN);

            valueHeapSize = newHeapSize;
            long delta = newHeapPos - valueHeapStart;
            valueHeapPos += delta;

            this.valueHeapStart = newHeapPos;
            this.valueHeapLimit = newHeapPos + newHeapSize;
        }
    }

    private void clearBlock(int position) {
        setParent(position, -1);
        setLeft(position, -1);
        setRight(position, -1);
        setColor(position, BLACK);
        // assume there's only one value in the chain (otherwise node shouldn't be deleted)
        int refOffset = refOf(position);
        assert nextValueOffset(refOffset) == CHAIN_END;
        setRowId(refOffset, FREE_SLOT);
    }

    private int getChainLength(int chainStart) {
        int counter = 1;
        int nextOffset = nextValueOffset(chainStart);
        while (nextOffset != EMPTY) {
            nextOffset = nextValueOffset(nextOffset);
            counter++;
        }
        return counter;
    }

    private boolean hasMoreThanOneValue(int position) {
        final int ref = refOf(position);
        final int previousOffset = nextValueOffset(ref);
        return previousOffset != CHAIN_END;
    }

    private int nextValueOffset(int valueOffset) {
        return Unsafe.getUnsafe().getInt(valueHeapStart + uncompressValueOffset(valueOffset) + 8);
    }

    private void prepareComparatorLeftSideIfAtMaxCapacity(RecordRandomAccess sourceCursor, Record ownedRecord, RecordComparator comparator) {
        if (currentValues == limit) {
            assert minMaxRowId != -1;
            sourceCursor.recordAt(ownedRecord, minMaxRowId);
            comparator.setLeft(ownedRecord);
        }
    }

    private void putParent(long rowId) {
        root = allocateBlock(-1, rowId);
    }

    private void refreshMinMaxNode() {
        int p;
        if (isFirstN) {
            p = findMaxNode();
        } else { // lastN/topN
            p = findMinNode();
        }
        minMaxNode = p;
        minMaxRowId = rowId(refOf(p));
    }

    private void removeMostRecentChainValue(int node) {
        final int ref = refOf(node);
        final int previousOffset = nextValueOffset(ref);
        setRef(node, previousOffset);

        // clear both rowid slot and next value offset
        setRowId(ref, -1);
        setNextValueOffset(ref, -1);

        chainFreeList.add(ref);
    }

    private long rowId(int valueOffset) {
        return Unsafe.getUnsafe().getLong(valueHeapStart + uncompressValueOffset(valueOffset));
    }

    private void setNextValueOffset(int valueOffset, int nextValueOffset) {
        Unsafe.getUnsafe().putInt(valueHeapStart + uncompressValueOffset(valueOffset) + 8, nextValueOffset);
    }

    private void setRowId(int valueOffset, long rowId) {
        Unsafe.getUnsafe().putLong(valueHeapStart + uncompressValueOffset(valueOffset), rowId);
    }

    // if not empty - reuses most recently deleted node from freelist; otherwise allocates a new node
    protected int allocateBlock(int parent, long recordRowId) {
        if (freeList.size() > 0) {
            int freeNode = freeList.get(freeList.size() - 1);
            freeList.removeLast();

            setParent(freeNode, parent);
            setRowId(refOf(freeNode), recordRowId);

            return freeNode;
        } else {
            int newNode = super.allocateBlock();
            setParent(newNode, parent);

            int chainOffset;
            if (chainFreeList.size() > 0) {
                chainOffset = chainFreeList.get(chainFreeList.size() - 1);
                chainFreeList.removeLast();
                setRowId(chainOffset, recordRowId);
                setNextValueOffset(chainOffset, CHAIN_END);
            } else {
                chainOffset = appendValue(recordRowId, CHAIN_END);
            }
            setRef(newNode, chainOffset);

            return newNode;
        }
    }

    void printTree(Utf16Sink sink, int node, int level, boolean isLeft, ValuePrinter printer) {
        byte color = colorOf(node);
        int valueOffset = refOf(node);
        long rowId = rowId(valueOffset);

        for (int i = 1; i < level; i++) {
            sink.put(' ').put(' ');
        }

        if (level > 0) {
            sink.put(' ');
            sink.put(isLeft ? 'L' : 'R');
            sink.put('-');
        }

        sink.put('[');
        sink.put(color == RED ? "Red" : color == BLACK ? "Black" : "Unkown_Color");
        sink.put(',');
        sink.put(printer.toString(rowId));

        int chainLength = getChainLength(valueOffset);
        if (chainLength > 1) {
            sink.put('(').put(chainLength).put(')');
        }
        sink.put(']');
        sink.put('\n');

        if (leftOf(node) != EMPTY) {
            printTree(sink, leftOf(node), level + 1, true, printer);
        }

        if (rightOf(node) != EMPTY) {
            printTree(sink, rightOf(node), level + 1, false, printer);
        }
    }

    @FunctionalInterface
    public interface ValuePrinter {
        static String toRowId(long rowid) {
            return String.valueOf(rowid);
        }

        String toString(long rowid);
    }

    public class TreeCursor {
        private int chainCurrent;
        private int treeCurrent;

        public void clear() {
            treeCurrent = 0;
            chainCurrent = 0;
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
            treeCurrent = p;
            chainCurrent = refOf(treeCurrent);
        }
    }
}
