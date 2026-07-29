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
import io.questdb.griffin.engine.RecordComparator;

/**
 * Values are stored on a heap. Value chain addresses are 4-byte aligned.
 */
public class LongTreeChain extends AbstractRedBlackTree implements Reopenable {
    private final TreeCursor cursor = new TreeCursor();

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
        super(
                keyPageSize,
                maxKeyHeapBytes,
                valuePageSize,
                maxValueHeapBytes,
                keyHeapConfigKey,
                valueHeapConfigKey,
                "LongTreeChain",
                openOnInit
        );
    }

    @Override
    public void close() {
        super.close();
        cursor.clear();
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
                final int newChainEnd = appendValue(rowId, CHAIN_END);
                setNextValueOffset(oldChainEnd, newChainEnd);
                setLastRef(offset, newChainEnd);
                return;
            }
        } while (offset != EMPTY);

        offset = allocateBlock();
        setParent(offset, parent);

        final int chainStart = appendValue(rowId, CHAIN_END);
        setRef(offset, chainStart);
        setLastRef(offset, chainStart);

        if (cmp < 0) {
            setLeft(parent, offset);
        } else {
            setRight(parent, offset);
        }
        fixInsert(offset);
    }

    private void putParent(long rowId) {
        root = allocateBlock();
        final int chainStart = appendValue(rowId, CHAIN_END);
        setRef(root, chainStart);
        setLastRef(root, chainStart);
        setParent(root, EMPTY);
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
