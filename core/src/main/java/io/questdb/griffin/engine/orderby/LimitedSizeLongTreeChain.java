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
import io.questdb.cairo.sql.RecordRandomAccess;
import io.questdb.griffin.engine.AbstractRedBlackTree;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
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
    // marks value chain entry as unused (belonging to a node on the freelist)
    // it's meant to avoid unnecessary reallocations when removing nodes and adding nodes
    private static final long FREE_SLOT = -2;
    // LIFO list of free blocks to reuse, allocated on the value chain
    private final DirectIntList chainFreeList;
    private final LimitedSizeLongTreeChain.TreeCursor cursor = new LimitedSizeLongTreeChain.TreeCursor();
    // LIFO list of nodes to reuse, instead of releasing and reallocating
    private final DirectIntList freeList;
    private int comparatorLeftSideValidForFrame = -1;
    // number of all values stored in tree (including repeating ones)
    private int currentValues = 0;
    // firstN - keep <first->N> set , otherwise keep <last-N->last> set
    private boolean isFirstN;
    // maximum number of values tree can store (including repeating values)
    private long limit; // -1 means 'almost' unlimited
    private int minMaxNode = EMPTY;
    // for fast filtering out of records in here we store rowId of:
    //  - record with max value for firstN/bottomN query
    //  - record with min value for lastN/topN query
    private long minMaxRowId = -1;

    public LimitedSizeLongTreeChain(
            long keyPageSize,
            long maxKeyHeapBytes,
            long valuePageSize,
            long maxValueHeapBytes,
            String keyHeapConfigKey,
            String valueHeapConfigKey
    ) {
        this(keyPageSize, maxKeyHeapBytes, valuePageSize, maxValueHeapBytes, keyHeapConfigKey, valueHeapConfigKey, true);
    }

    public LimitedSizeLongTreeChain(
            long keyPageSize,
            long maxKeyHeapBytes,
            long valuePageSize,
            long maxValueHeapBytes,
            String keyHeapConfigKey,
            String valueHeapConfigKey,
            boolean openOnInit
    ) {
        super(
                keyPageSize,
                maxKeyHeapBytes,
                valuePageSize,
                maxValueHeapBytes,
                keyHeapConfigKey,
                valueHeapConfigKey,
                "LimitedSizeLongTreeChain",
                openOnInit
        );
        try {
            // DirectIntList freelists are small (64 bytes apiece) and stay on the global
            // counter only; this PR wires the tree's key and value heaps, not the
            // auxiliary freelists.
            freeList = new DirectIntList(16, MemoryTag.NATIVE_TREE_CHAIN);
            chainFreeList = new DirectIntList(16, MemoryTag.NATIVE_TREE_CHAIN);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        super.clear();
        comparatorLeftSideValidForFrame = -1;
        minMaxRowId = -1;
        minMaxNode = EMPTY;
        currentValues = 0;
        cursor.clear();
        freeList.clear();
        chainFreeList.clear();
    }

    @Override
    public void close() {
        super.close();
        // Reset state inline instead of routing through clear(): a malloc fault in the constructor
        // calls close() with freeList/chainFreeList still null, which clear() would dereference.
        // Misc.free() is null-safe.
        comparatorLeftSideValidForFrame = -1;
        minMaxRowId = -1;
        minMaxNode = EMPTY;
        currentValues = 0;
        cursor.clear();
        Misc.free(freeList);
        Misc.free(chainFreeList);
    }

    // returns offset of node containing searchRecord; otherwise returns EMPTY
    @TestOnly
    public int find(
            Record searchedRecord,
            RecordCursor sourceCursor,
            Record placeholder,
            RecordComparator comparator
    ) {
        comparator.setLeft(searchedRecord);

        if (root == EMPTY) {
            return EMPTY;
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
        } while (p != EMPTY);

        return EMPTY;
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
        // The comparator's left side was pre-set at the end of the previous
        // put() call. For Parquet frames, the record holds flyweight references
        // pointing into decoded row group buffers. Those buffers may have been
        // freed between frames (by releaseParquetBuffers()), so we re-navigate
        // once per frame change to ensure the references are backed by live memory.
        int currentFrameIndex = Rows.toPartitionIndex(currentRecord.getRowId());

        if (limit == currentValues) {
            if (currentFrameIndex != comparatorLeftSideValidForFrame) {
                assert minMaxRowId != -1;
                sourceCursor.recordAt(ownedRecord, minMaxRowId);
                comparator.setLeft(ownedRecord);
                comparatorLeftSideValidForFrame = currentFrameIndex;
            }
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
            prepareComparatorLeftSideIfAtMaxCapacity(sourceCursor, ownedRecord, comparator, currentFrameIndex);
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
                prepareComparatorLeftSideIfAtMaxCapacity(sourceCursor, ownedRecord, comparator, currentFrameIndex);
                return;
            }
        } while (p != EMPTY);

        final long currentRecordRowId = currentRecord.getRowId();
        p = allocateBlock(parent, currentRecordRowId);

        if (cmp < 0) {
            setLeft(parent, p);
        } else {
            setRight(parent, p);
        }

        fixInsert(p);
        if (minMaxNode == EMPTY) {
            // Only reachable when removeAndCache() ran on a node that was not the cached extreme
            // and so could not name a replacement. An emptied tree returns at the root == EMPTY
            // branch above and never arrives here.
            refreshMinMaxNode();
        } else if (parent == minMaxNode && (isFirstN ? cmp > 0 : cmp < 0)) {
            // The extreme has no child on its outer side, so a node inserted there is the only one
            // that can displace it - and it displaces it exactly when the walk ended at the extreme
            // and went outwards. fixInsert() rotates the tree but never reorders the sequence, so
            // this holds after the rotations too. Every other insert leaves the extreme alone.
            minMaxNode = p;
            minMaxRowId = currentRecordRowId;
        }
        currentValues++;
        prepareComparatorLeftSideIfAtMaxCapacity(sourceCursor, ownedRecord, comparator, currentFrameIndex);
    }

    // remove node and put on freelist (if holds only one value in chain)
    public void removeAndCache(int node) {
        // find() documents a -1 return, and a -1 here would walk the value chain off the
        // heap: uncompressAligned4(-1) is ~16GB now that compressed offsets are unsigned.
        assert node != EMPTY;
        if (hasMoreThanOneValue(node)) {
            removeMostRecentChainValue(node); // don't change minMax
        } else {
            // The cache must name a genuine extreme: one has no child on its outer side, which is
            // exactly what lets remove() below take its nodeToRemove == node branch. An interior
            // node here would send remove() down the ref-swapping branch and hand nextExtremeAfter
            // the wrong neighbour, silently returning wrong top-N rows instead of failing.
            assert node != minMaxNode || (isFirstN ? rightOf(node) == EMPTY : leftOf(node) == EMPTY);
            // Name the replacement before remove() restructures anything. The cached extreme has
            // no child on its outer side, so remove() takes its nodeToRemove == node branch and
            // never swaps refs with a successor, and fixDelete() rotates without reordering the
            // sequence - so the node that is next-most-extreme now is still next-most-extreme once
            // the rotations finish. Any other node leaves the cache invalid, as before.
            final int replacement = node == minMaxNode ? nextExtremeAfter(node) : EMPTY;

            int nodeToRemove = super.remove(node);
            clearBlock(nodeToRemove);
            freeList.add(nodeToRemove); // keep node on freelist to minimize allocations

            if (replacement != EMPTY) {
                minMaxNode = replacement;
                minMaxRowId = rowId(refOf(replacement));
            } else {
                minMaxRowId = -1; // re-compute on the next insert
                minMaxNode = EMPTY;
            }
        }

        currentValues--;
    }

    @Override
    public void reopen() {
        // Heaps first, freelists second. A throw from super.reopen() - the value-heap malloc
        // breaching the RSS limit - therefore leaves the freelists closed, which is safe:
        // DirectIntList.size() reads 0 while closed, so allocateBlock()'s freeList.size() > 0
        // guards take the else branch, and the first appendValue() re-enters this method through
        // growValueHeap() and reopens both lists before any add() can run. Keep this order:
        // reopening the lists first would only widen the window in which they are open but the
        // value heap is not.
        super.reopen();
        freeList.reopen();
        chainFreeList.reopen();
    }

    @Override
    public long size() {
        return currentValues;
    }

    public void updateLimits(boolean isFirstN, long limit) {
        // Callers must not flip isFirstN against a populated tree. minMaxNode/minMaxRowId name one
        // end of it, chosen by isFirstN, and nothing here re-derives them for the other end - the
        // top of put() reads minMaxRowId before any refresh can run.
        // A cached factory does flip the direction: computeLimits() re-derives isFirstN from the
        // bind variables on every execution, so a re-bound LIMIT that changes sign reaches this
        // with the opposite value. What keeps the precondition true is clear(), not a fixed
        // direction - LimitedSizeSortedLightRecordCursorFactory.initialize() calls this ahead of
        // cursor.of(), and of() clear()s the chain before the first put(). Every throw in between
        // routes through getCursor()'s catch to Misc.free(cursor) -> chain.close(), which resets
        // the same state. Keep that ordering: moving updateLimits() after of() would leave the
        // cache naming the wrong end of a populated tree and silently emit the wrong top-N rows.
        this.isFirstN = isFirstN;
        this.limit = limit;
    }

    private void clearBlock(int position) {
        setParent(position, EMPTY);
        setLeft(position, EMPTY);
        setRight(position, EMPTY);
        setColor(position, BLACK);
        // assume there's only one value in the chain (otherwise node shouldn't be deleted)
        int refOffset = refOf(position);
        assert nextValueOffset(refOffset) == CHAIN_END;
        setRowId(refOffset, FREE_SLOT);
    }

    private int getChainLength(int chainStart) {
        int counter = 1;
        int nextOffset = nextValueOffset(chainStart);
        // CHAIN_END, not EMPTY: this walks value offsets rather than block offsets. The two
        // sentinels are deliberately the same -1, and the tree relies on that - refOf()/lastRefOf()
        // return the literal -1 for an EMPTY block, which the cursors then read as a chain end.
        while (nextOffset != CHAIN_END) {
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

    // The extreme's neighbour: the rightmost node of its left subtree when keeping the first N
    // (where the cache holds the maximum), the leftmost of its right subtree when keeping the last
    // N. The extreme has no child on its outer side, so when the inner one is missing too the
    // neighbour is simply its parent, and EMPTY there means the tree had a single node left.
    private int nextExtremeAfter(int node) {
        if (isFirstN) {
            int p = leftOf(node);
            if (p == EMPTY) {
                return parentOf(node);
            }
            while (rightOf(p) != EMPTY) {
                p = rightOf(p);
            }
            return p;
        }
        int p = rightOf(node);
        if (p == EMPTY) {
            return parentOf(node);
        }
        while (leftOf(p) != EMPTY) {
            p = leftOf(p);
        }
        return p;
    }

    private void prepareComparatorLeftSideIfAtMaxCapacity(RecordRandomAccess sourceCursor, Record ownedRecord, RecordComparator comparator, int currentFrameIndex) {
        if (currentValues == limit) {
            assert minMaxRowId != -1;
            sourceCursor.recordAt(ownedRecord, minMaxRowId);
            comparator.setLeft(ownedRecord);
            comparatorLeftSideValidForFrame = currentFrameIndex;
        }
    }

    private void putParent(long rowId) {
        root = allocateBlock(EMPTY, rowId);
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
        setNextValueOffset(ref, CHAIN_END);

        chainFreeList.add(ref);
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
            // Sentinels, not 0: 0 is a legal block and value offset, so clearing to it left
            // hasNext() reporting true and next() reading rowId(0) - from address 0 after a
            // close(). LongTreeChain's cursor already cleared to the sentinels.
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
            treeCurrent = p;
            chainCurrent = refOf(treeCurrent);
        }
    }
}
