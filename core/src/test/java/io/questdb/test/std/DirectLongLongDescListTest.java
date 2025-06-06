/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.std;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongLongDescList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.PriorityQueue;

public class DirectLongLongDescListTest {
    private static final Log LOG = LogFactory.getLog(DirectLongLongDescListTest.class);

    @Test
    public void testFuzz() {
        final int N = 10000;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final PriorityQueue<Long> oracle = new PriorityQueue<>(100, (l1, l2) -> Long.compare(l2, l1));
        try (DirectLongLongDescList queue = new DirectLongLongDescList(100, MemoryTag.NATIVE_DEFAULT)) {
            for (long i = 0; i < N; i++) {
                long v = rnd.nextLong();
                queue.add(v, v);
                oracle.add(v);
            }

            DirectLongLongDescList.Cursor cursor = queue.getCursor();
            for (int i = 0, n = queue.size(); i < n; i++) {
                Long v = oracle.poll();
                Assert.assertNotNull(v);
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals((long) v, cursor.index());
                Assert.assertEquals((long) v, cursor.value());
            }
            Assert.assertFalse(cursor.hasNext());
        }
    }

    @Test
    public void testReopen() {
        try (DirectLongLongDescList queue = new DirectLongLongDescList(10, MemoryTag.NATIVE_DEFAULT)) {
            Assert.assertEquals(10, queue.getCapacity());
            Assert.assertEquals(0, queue.size());
            Assert.assertFalse(queue.getCursor().hasNext());

            queue.add(1, 1);
            Assert.assertEquals(1, queue.size());

            queue.clear();
            Assert.assertEquals(0, queue.size());

            queue.add(1, 1);
            Assert.assertEquals(1, queue.size());

            queue.close();
            Assert.assertEquals(0, queue.size());

            queue.reopen();
            Assert.assertEquals(10, queue.getCapacity());
            Assert.assertEquals(0, queue.size());

            queue.add(1, 1);

            DirectLongLongDescList.Cursor cursor = queue.getCursor();
            cursor.toTop();
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(1, cursor.index());
            Assert.assertEquals(1, cursor.value());
            Assert.assertFalse(cursor.hasNext());
        }
    }

    @Test
    public void testSmoke() {
        try (DirectLongLongDescList queue = new DirectLongLongDescList(10, MemoryTag.NATIVE_DEFAULT)) {
            Assert.assertEquals(10, queue.getCapacity());
            Assert.assertEquals(0, queue.size());
            Assert.assertFalse(queue.getCursor().hasNext());

            for (long i = 0; i < 100; i++) {
                queue.add(i, i);
            }
            Assert.assertEquals(10, queue.getCapacity());
            Assert.assertEquals(10, queue.size());

            DirectLongLongDescList.Cursor cursor = queue.getCursor();
            cursor.toTop();
            for (long i = 99; i > 89; i--) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(i, cursor.index());
                Assert.assertEquals(i, cursor.value());
            }
            Assert.assertFalse(cursor.hasNext());
        }
    }
}
