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

package com.questdb.griffin.engine.table;

import com.questdb.griffin.engine.AbstractRedBlackTree;

public class LongTreeSet extends AbstractRedBlackTree {

    private final TreeCursor cursor = new TreeCursor();

    public LongTreeSet(int keyPageSize) {
        super(keyPageSize);
    }

    public TreeCursor getCursor() {
        cursor.toTop();
        return cursor;
    }

    public void put(long value) {
        if (root == -1) {
            putParent(value);
            return;
        }

        long p = root;
        long parent;
        long current;
        do {
            parent = p;
            current = refOf(p);
            if (current > value) {
                p = leftOf(p);
            } else if (current < value) {
                p = rightOf(p);
            } else {
                // duplicate
                return;
            }
        } while (p > -1);

        p = allocateBlock();
        setParent(p, parent);
        setRef(p, value);

        if (current > value) {
            setLeft(parent, p);
        } else {
            setRight(parent, p);
        }
        fix(p);
    }

    public class TreeCursor {

        private long current;

        public boolean hasNext() {
            return current != -1;
        }

        public long next() {
            long address = current;
            current = successor(current);
            return refOf(address);
        }

        public void toTop() {
            long p = root;
            if (p != -1) {
                while (leftOf(p) != -1) {
                    p = leftOf(p);
                }
            }
            current = p;
        }
    }
}
