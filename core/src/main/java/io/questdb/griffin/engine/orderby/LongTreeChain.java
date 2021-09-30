/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.engine.AbstractRedBlackTree;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

public class LongTreeChain extends AbstractRedBlackTree {
    private final TreeCursor cursor = new TreeCursor();
    private final MemoryARW valueChain;

    public LongTreeChain(long keyPageSize, int keyMaxPages, long valuePageSize, int valueMaxPages) {
        super(keyPageSize, keyMaxPages);
        this.valueChain = Vm.getARWInstance(valuePageSize, valueMaxPages, MemoryTag.NATIVE_TREE_CHAIN);
    }

    @Override
    public void clear() {
        super.clear();
        this.valueChain.jumpTo(0);
    }

    @Override
    public void close() {
        super.close();
        Misc.free(valueChain);
    }

    private long appendValue(long value, long prevValueOffset) {
        final long offset = valueChain.getAppendOffset();
        valueChain.putLong128(value, prevValueOffset);
        return offset;
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
                setRef(p, appendValue(leftRecord.getRowId(), r));
                return;
            }
        } while (p > -1);

        p = allocateBlock();
        setParent(p, parent);

        setRef(p, appendValue(leftRecord.getRowId(), -1L));

        if (cmp < 0) {
            setLeft(parent, p);
        } else {
            setRight(parent, p);
        }
        fix(p);
    }

    @Override
    protected void putParent(long value) {
        root = allocateBlock();
        setRef(root, appendValue(value, -1L));
        setParent(root, -1);
    }

    public class TreeCursor {

        private long treeCurrent;
        private long chainCurrent;

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
            chainCurrent = refOf(treeCurrent = p);
        }
    }
}
