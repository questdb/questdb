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
import io.questdb.std.ObjList;
import io.questdb.std.ObjSortedList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.PriorityQueue;

public class ObjSortedListTest {
    private static final Log LOG = LogFactory.getLog(ObjSortedListTest.class);

    @Test
    public void testFuzz() {
        final int N = 10000;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final PriorityQueue<Long> oracle = new PriorityQueue<>(N);
        ObjSortedList<Long> queue = new ObjSortedList<>(Long::compare);

        for (long i = 0; i < N; i++) {
            long v = rnd.nextLong();
            queue.add(v);
            oracle.add(v);
        }

        for (int i = 0, n = queue.size(); i < n; i++) {
            Long expected = oracle.poll();
            Long actual = queue.poll();
            Assert.assertNotNull(expected);
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testRemove() {
        ObjSortedList<Integer> queue = new ObjSortedList<>(Integer::compare);

        for (int i = 0; i < 10; i++) {
            queue.add(i);
        }
        Assert.assertEquals(16, queue.getCapacity());
        Assert.assertEquals(10, queue.size());

        // remove existing
        queue.remove(3);
        queue.remove(5);
        queue.remove(9);

        // remove non-existing
        queue.remove(-1);
        queue.remove(42);

        Assert.assertEquals(7, queue.size());

        for (int i = 0; i < 10; i++) {
            if (i != 3 && i != 5 && i != 9) {
                Assert.assertEquals(i, (int) queue.poll());
            }
        }
        Assert.assertEquals(0, queue.size());

        // try to remove more non-existing
        for (int i = 0; i < 10; i++) {
            queue.remove(i);
        }
    }

    @Test
    public void testRemoveDuplicates() {
        ObjSortedList<Integer> queue = new ObjSortedList<>(Integer::compare);

        queue.add(0);
        for (int i = 0; i < 3; i++) {
            queue.add(1);
        }
        queue.add(2);
        Assert.assertEquals(16, queue.getCapacity());
        Assert.assertEquals(5, queue.size());

        queue.remove(1);

        Assert.assertEquals(4, queue.size());

        Assert.assertEquals(0, (int) queue.poll());
        Assert.assertEquals(1, (int) queue.poll());
        Assert.assertEquals(1, (int) queue.poll());
        Assert.assertEquals(2, (int) queue.poll());
        Assert.assertEquals(0, queue.size());
    }

    @Test
    public void testSmoke() {
        ObjSortedList<Integer> queue = new ObjSortedList<>(Integer::compare);
        Assert.assertEquals(16, queue.getCapacity());
        Assert.assertEquals(0, queue.size());

        for (int i = 0; i < 100; i++) {
            queue.add(i);
        }
        Assert.assertEquals(128, queue.getCapacity());
        Assert.assertEquals(100, queue.size());

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(i, (int) queue.get(i));
        }

        ObjList<Integer> sink = new ObjList<>();
        queue.popMany(3, sink);
        Assert.assertEquals(3, sink.size());
        for (int i = 0, n = sink.size(); i < n; i++) {
            Assert.assertEquals(i, (int) sink.getQuick(i));
        }
        Assert.assertEquals(128, queue.getCapacity());
        Assert.assertEquals(97, queue.size());

        sink.clear();
        queue.popMany(100, sink);
        Assert.assertEquals(97, sink.size());
        for (int i = 0, n = sink.size(); i < n; i++) {
            Assert.assertEquals(i + 3, (int) sink.getQuick(i));
        }
        Assert.assertEquals(128, queue.getCapacity());
        Assert.assertEquals(0, queue.size());
    }
}
