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

package io.questdb.test.std;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntLongSortedList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

public class IntLongSortedListTest {
    private static final Log LOG = LogFactory.getLog(IntLongSortedListTest.class);

    @Test
    public void testFuzz() {
        final int N = 10000;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final PriorityQueue<Integer> oracle = new PriorityQueue<>(N);
        IntLongSortedList queue = new IntLongSortedList();

        for (int i = 0; i < N; i++) {
            int v = rnd.nextInt();
            queue.add(v, v);
            oracle.add(v);
        }

        Assert.assertEquals(oracle.size(), queue.size());
        while (queue.hasNext()) {
            Integer expected = oracle.poll();
            long actual = queue.pollValue();
            Assert.assertNotNull(expected);
            Assert.assertEquals((int) expected, (int) actual);
        }
    }

    @Test
    public void testHeapsortProperty() {
        final int M = 500;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        IntLongSortedList heap = new IntLongSortedList();

        // Use distinct longs to avoid tie-order ambiguity
        List<Long> inserted = new ArrayList<>(M);
        for (int i = 0; i < M; i++) {
            long v = rnd.nextLong();
            inserted.add(v);
            heap.add(i, v);
        }

        Assert.assertEquals(M, heap.size());

        List<Long> polled = new ArrayList<>(M);
        while (heap.hasNext()) {
            polled.add(heap.pollValue());
        }

        Assert.assertEquals(M, polled.size());

        // Assert non-decreasing (ascending order)
        for (int i = 1; i < polled.size(); i++) {
            Assert.assertTrue(
                    "value at " + i + " (" + polled.get(i) + ") < value at " + (i - 1) + " (" + polled.get(i - 1) + ")",
                    polled.get(i) >= polled.get(i - 1)
            );
        }

        // Assert all pushed values came back (multiset equality)
        List<Long> sortedInserted = new ArrayList<>(inserted);
        Collections.sort(sortedInserted);
        List<Long> sortedPolled = new ArrayList<>(polled);
        Collections.sort(sortedPolled);
        Assert.assertEquals(sortedInserted, sortedPolled);
    }

    @Test
    public void testPollAndReplace() {
        IntLongSortedList heap = new IntLongSortedList();

        // Add 5 entries with known values
        heap.add(0, 10L);
        heap.add(1, 30L);
        heap.add(2, 20L);
        heap.add(3, 50L);
        heap.add(4, 40L);

        // Root must be the minimum (10)
        Assert.assertEquals(0, heap.peekIndex());

        // Replace root (10) with 35; old value (10) should be returned
        long old = heap.pollAndReplace(99, 35L);
        Assert.assertEquals(10L, old);

        // The heap now contains: {35, 30, 20, 50, 40}
        // Drain via pollValue — must be non-decreasing
        long prev = Long.MIN_VALUE;
        int count = 0;
        while (heap.hasNext()) {
            long v = heap.pollValue();
            Assert.assertTrue("poll not ascending: prev=" + prev + " v=" + v, v >= prev);
            prev = v;
            count++;
        }

        // Verify we got all 5 remaining entries (20, 30, 35, 40, 50)
        Assert.assertEquals(5, count);
        Assert.assertEquals(0, heap.size());
    }

    @Test
    public void testClearAndReuse() {
        IntLongSortedList heap = new IntLongSortedList();
        heap.add(0, 100L);
        heap.add(1, 200L);
        heap.clear();

        Assert.assertFalse(heap.hasNext());
        Assert.assertEquals(0, heap.size());

        heap.add(2, 5L);
        heap.add(3, 15L);
        Assert.assertEquals(2, heap.size());
        Assert.assertEquals(5L, heap.pollValue());
        Assert.assertEquals(15L, heap.pollValue());
        Assert.assertFalse(heap.hasNext());
    }
}
