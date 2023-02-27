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

package io.questdb.griffin.engine.table;

import io.questdb.griffin.engine.AbstractRedBlackTree;

public class LongTreeSet extends AbstractRedBlackTree {

    private final TreeCursor cursor = new TreeCursor();

    public LongTreeSet(int keyPageSize, int keyMaxPages) {
        super(keyPageSize, keyMaxPages);
    }

    public TreeCursor getCursor() {
        cursor.toTop();
        return cursor;
    }

    public boolean put(long value) {
        if (root == -1) {
            putParent(value);
            return true;
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
                return false;
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
        fixInsert(p);
        return true;
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
