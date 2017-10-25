/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.ql.map;

import com.questdb.misc.Rnd;
import com.questdb.std.RedBlackTree;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.TreeSet;

public class RedBlackTreeTest {

    @Test
    public void testAddAndGet() throws Exception {
        Rnd rnd = new Rnd();
        TreeSet<Long> control = new TreeSet<>();
        try (RedBlackTree tree = new RedBlackTree(new RedBlackTree.LongComparator() {
            private long left;

            @Override
            public int compare(long y) {
                return Long.compare(left, y);
            }

            @Override
            public void setLeft(long left) {
                this.left = left;
            }
        }, 1024)) {

            long l;
            for (int i = 0; i < 10000; i++) {
                tree.add(l = rnd.nextLong());
                control.add(l);
            }

            Iterator<Long> controlIterator = control.iterator();
            RedBlackTree.LongIterator iterator = tree.iterator();
            while (iterator.hasNext()) {
                Assert.assertTrue(controlIterator.hasNext());
                Assert.assertEquals(controlIterator.next().longValue(), iterator.next());
            }

            tree.clear();

            Assert.assertFalse(tree.iterator().hasNext());
        }
    }

    @Test
    public void testNonUnique() throws Exception {
        try (RedBlackTree set = new RedBlackTree(new RedBlackTree.LongComparator() {
            private long left;

            @Override
            public int compare(long y) {
                return Long.compare(left, y);
            }

            @Override
            public void setLeft(long left) {
                this.left = left;
            }
        }, 1024)) {

            set.add(200);
            set.add(200);
            set.add(100);
            set.add(100);

            RedBlackTree.LongIterator iterator = set.iterator();
            StringBuilder b = new StringBuilder();
            while (iterator.hasNext()) {
                b.append(iterator.next()).append(',');
            }
            TestUtils.assertEquals("100,100,200,200,", b);
        }
    }
}