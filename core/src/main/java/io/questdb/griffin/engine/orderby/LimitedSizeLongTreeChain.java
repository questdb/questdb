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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.engine.AbstractRedBlackTree;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.CharSink;
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
 * TreeChain stores repeating values (rowids) on valueChain as a linked list:
 * <pre>
 * [latest rowid, offset to next] -&gt; [old rowid, offset to next] -&gt; [oldest rowid, -1L]
 * </pre>
 * -1 - marks end of current node's value chain.
 * -2 - marks an unused element on the value chain list for the current tree node
 * but should only happen once. It's meant to limit value chain allocations on delete/insert.
 */
public class LimitedSizeLongTreeChain extends AbstractRedBlackTree implements Reopenable {
    // value marks end of value chain
    private static final long CHAIN_END = -1;

    // marks value chain entry as unused (belonging to a node on the freelist)
    // it's meant to avoid unnecessary reallocations when removing nodes and adding nodes
    private static final long FREE_SLOT = -2L;
    // LIFO list of free blocks to reuse, allocated on the value chain
    private final LongList chainFreeList;
    private final LimitedSizeLongTreeChain.TreeCursor cursor = new LimitedSizeLongTreeChain.TreeCursor();
    // LIFO list of nodes to reuse, instead of releasing and reallocating
    private final LongList freeList;
    // firstN - keep <first->N> set , otherwise keep <last-N->last> set
    private final boolean isFirstN;
    // maximum number of values tree can store (including repeating values)
    private final long maxValues; //-1 means 'almost' unlimited
    private final MemoryARW valueChain;
    // number of all values stored in tree (including repeating ones)
    private long currentValues = 0;
    private long minMaxNode = -1;
    // for fast filtering out of records in here we store rowId of:
    //  - record with max value for firstN/bottomN query
    //  - record with min value for lastN/topN query
    private long minMaxRowId = -1;

    public LimitedSizeLongTreeChain(long keyPageSize, int keyMaxPages, long valuePageSize, int valueMaxPages, boolean isFirstN, long maxValues) {
        super(keyPageSize, keyMaxPages);
        this.valueChain = Vm.getARWInstance(valuePageSize, valueMaxPages, MemoryTag.NATIVE_TREE_CHAIN);
        this.freeList = new LongList();
        this.chainFreeList = new LongList();
        this.isFirstN = isFirstN;
        this.maxValues = maxValues;
    }

    @Override
    public void clear() {
        super.clear();
        this.valueChain.jumpTo(0);

        minMaxRowId = -1;
        minMaxNode = -1;
        currentValues = 0;
        freeList.clear();
        chainFreeList.clear();
        cursor.clear();
    }

    @Override
    public void close() {
        clear();
        super.close();
        Misc.free(valueChain);
    }

    // returns address of node containing searchRecord; otherwise returns -1
    public long find(
            Record searchedRecord,
            RecordCursor sourceCursor,
            Record placeholder,
            RecordComparator comparator
    ) {
        comparator.setLeft(searchedRecord);

        if (root == -1) {
            return -1L;
        }

        long p = root;
        int cmp;
        do {
            sourceCursor.recordAt(placeholder, valueChain.getLong(refOf(p)));
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
    public void print(CharSink sink) {
        print(sink, null);
    }

    // prints tree in-order, horizontally
    public void print(CharSink sink, ValuePrinter printer) {
        if (root == EMPTY) {
            sink.put("[EMPTY TREE]");
        } else {
            if (printer == null) {
                printer = ValuePrinter::toRowId;
            }
            printTree(sink, root, 0, false, printer);
        }
    }

    public void put(
            Record leftRecord,
            RecordCursor sourceCursor,
            Record rightRecord,
            RecordComparator comparator
    ) {
        if (maxValues == 0) {
            return;
        }

        comparator.setLeft(leftRecord);

        // if maxValues < 0 then there's no limit (unless there's more than 2^64 records, which is unlikely)
        if (maxValues == currentValues) {
            sourceCursor.recordAt(rightRecord, minMaxRowId);
            int cmp = comparator.compare(rightRecord);

            if (isFirstN && cmp >= 0) { // bigger than max for firstN/bottomN
                return;
            } else if (!isFirstN && cmp <= 0) { // smaller than min for lastN/topN
                return;
            } else { // record has to be inserted, so we've to remove current minMax
                removeAndCache(minMaxNode);
            }
        }

        if (root == EMPTY) {
            putParent(leftRecord.getRowId());
            minMaxNode = root;
            minMaxRowId = leftRecord.getRowId();
            currentValues++;
            return;
        }

        long p = root;
        long parent;
        int cmp;
        do {
            parent = p;
            final long r = refOf(p);
            sourceCursor.recordAt(rightRecord, valueChain.getLong(r));
            cmp = comparator.compare(rightRecord);
            if (cmp < 0) {
                p = leftOf(p);
            } else if (cmp > 0) {
                p = rightOf(p);
            } else {
                setRef(p, appendValue(leftRecord.getRowId(), r)); // appends value to chain, minMax shouldn't change
                if (minMaxRowId == -1) {
                    refreshMinMaxNode();
                }
                currentValues++;
                return;
            }
        } while (p > -1);

        p = allocateBlock(parent, leftRecord.getRowId());

        if (cmp < 0) {
            setLeft(parent, p);
        } else {
            setRight(parent, p);
        }

        fixInsert(p);
        refreshMinMaxNode();
        currentValues++;
    }

    // remove node and put on freelist (if holds only one value in chain)
    public void removeAndCache(long node) {
        if (hasMoreThanOneValue(node)) {
            removeMostRecentChainValue(node); // don't change minMax
        } else {
            long nodeToRemove = super.remove(node);
            clearBlock(nodeToRemove);
            freeList.add(nodeToRemove); // keep node on freelist to minimize allocations

            minMaxRowId = -1; // re-compute after inserting, there's no point doing it now
            minMaxNode = -1;
        }

        currentValues--;
    }

    @Override
    public long size() {
        return currentValues;
    }

    private long appendValue(long value, long prevValueOffset) {
        final long offset = valueChain.getAppendOffset();
        valueChain.putLong128(value, prevValueOffset);
        return offset;
    }

    private void clearBlock(long position) {
        setParent(position, -1);
        setLeft(position, -1);
        setRight(position, -1);
        setColor(position, BLACK);
        // assume there's only one value in the chain (otherwise node shouldn't be deleted)
        long refOffset = refOf(position);
        assert valueChain.getLong(refOffset + 8) == CHAIN_END;
        valueChain.putLong(refOffset, FREE_SLOT);
    }

    private int getChainLength(long chainStart) {
        int counter = 1;
        long nextOffset = valueChain.getLong(chainStart + 8);

        while (nextOffset != EMPTY) {
            nextOffset = valueChain.getLong(nextOffset + 8);
            counter++;
        }

        return counter;
    }

    private boolean hasMoreThanOneValue(long position) {
        long ref = refOf(position);
        long previousOffset = valueChain.getLong(ref + 8);
        return previousOffset != CHAIN_END;
    }

    private void refreshMinMaxNode() {
        long p;

        if (isFirstN) {
            p = findMaxNode();
        } else { // lastN/topN
            p = findMinNode();
        }

        minMaxNode = p;
        minMaxRowId = valueChain.getLong(refOf(p));
    }

    private void removeMostRecentChainValue(long node) {
        long ref = refOf(node);
        long previousOffset = valueChain.getLong(ref + 8);
        setRef(node, previousOffset);

        // clear both rowid slot and next value offset
        valueChain.putLong(ref, -1L);
        valueChain.putLong(ref + 8, -1L);

        chainFreeList.add(ref);
    }

    // if not empty - reuses most recently deleted node from freelist; otherwise allocates a new node
    protected long allocateBlock(long parent, long recordRowId) {
        if (freeList.size() > 0) {
            long freeNode = freeList.get(freeList.size() - 1);
            freeList.removeIndex(freeList.size() - 1);

            setParent(freeNode, parent);
            valueChain.putLong(refOf(freeNode), recordRowId);

            return freeNode;
        } else {
            long newNode = super.allocateBlock();
            setParent(newNode, parent);

            long chainOffset;

            if (chainFreeList.size() > 0) {
                chainOffset = chainFreeList.get(chainFreeList.size() - 1);
                chainFreeList.removeIndex(chainFreeList.size() - 1);
                valueChain.putLong(chainOffset, recordRowId);
                valueChain.putLong(chainOffset + 8, CHAIN_END);
            } else {
                chainOffset = appendValue(recordRowId, CHAIN_END);
            }

            setRef(newNode, chainOffset);

            return newNode;
        }
    }

    void printTree(CharSink sink, long node, int level, boolean isLeft, ValuePrinter printer) {
        byte color = colorOf(node);
        long valueOffset = refOf(node);
        long value = valueChain.getLong(valueOffset);

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
        sink.put(printer.toString(value));

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

    @Override
    protected void putParent(long value) {
        root = allocateBlock(-1, value);
    }

    @FunctionalInterface
    public interface ValuePrinter {
        static String toRowId(long rowid) {
            return String.valueOf(rowid);
        }

        String toString(long rowid);
    }

    public class TreeCursor {

        private long chainCurrent;
        private long treeCurrent;

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
            long result = chainCurrent;
            chainCurrent = valueChain.getLong(chainCurrent + 8);
            return valueChain.getLong(result);
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
            treeCurrent = p;
            chainCurrent = refOf(treeCurrent);
        }
    }
}
