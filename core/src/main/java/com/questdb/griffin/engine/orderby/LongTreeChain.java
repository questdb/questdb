/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.orderby;

import com.questdb.cairo.VirtualMemory;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.griffin.engine.AbstractRedBlackTree;
import com.questdb.std.Misc;

public class LongTreeChain extends AbstractRedBlackTree {
    private final TreeCursor cursor = new TreeCursor();
    private final VirtualMemory valueChain;

    public LongTreeChain(int keyPageSize, int valuePageSize) {
        super(keyPageSize);
        this.valueChain = new VirtualMemory(valuePageSize);
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

    protected void putParent(long value) {
        root = allocateBlock();
        long r = appendValue(value, -1L);
        setRef(root, r);
        setParent(root, -1);
        setLeft(root, -1);
        setRight(root, -1);
    }

    public TreeCursor getCursor() {
        cursor.toTop();
        return cursor;
    }

    public void put(
            long value,
            RecordCursor sourceCursor,
            Record sourceRecord,
            RecordComparator comparator
    ) {
        if (root == -1) {
            putParent(value);
            return;
        }

        sourceCursor.recordAt(sourceRecord, value);
        comparator.setLeft(sourceRecord);

        long p = root;
        long parent;
        int cmp;
        long r;
        do {
            parent = p;
            r = refOf(p);
            sourceCursor.recordAt(sourceRecord, valueChain.getLong(r));
            cmp = comparator.compare(sourceRecord);
            if (cmp < 0) {
                p = leftOf(p);
            } else if (cmp > 0) {
                p = rightOf(p);
            } else {
                setRef(p, appendValue(value, r));
                return;
            }
        } while (p > -1);

        p = allocateBlock();
        setParent(p, parent);

        r = appendValue(value, -1L);
        setRef(p, r);

        if (cmp < 0) {
            setLeft(parent, p);
        } else {
            setRight(parent, p);
        }
        fix(p);
    }

    private long appendValue(long value, long prevValueOffset) {
        final long offset = valueChain.getAppendOffset();
        valueChain.putLong(value);
        valueChain.putLong(prevValueOffset);
        return offset;
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
