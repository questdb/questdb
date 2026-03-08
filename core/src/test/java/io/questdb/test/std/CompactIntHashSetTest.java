/*******************************************************************************
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

import io.questdb.std.CompactIntHashSet;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class CompactIntHashSetTest extends AbstractTest {

    @Test
    public void testAddAndContains() {
        CompactIntHashSet set = new CompactIntHashSet(16, 0.5);

        Assert.assertTrue(set.add(10));
        Assert.assertTrue(set.add(20));
        Assert.assertTrue(set.add(30));

        Assert.assertFalse(set.add(10));
        Assert.assertFalse(set.add(20));
        Assert.assertFalse(set.add(30));

        Assert.assertFalse(set.excludes(10));
        Assert.assertFalse(set.excludes(20));
        Assert.assertFalse(set.excludes(30));

        Assert.assertTrue(set.excludes(40));
        Assert.assertTrue(set.excludes(50));

        Assert.assertEquals(3, set.size());
    }

    @Test
    public void testAddAtIndex() {
        CompactIntHashSet set = new CompactIntHashSet(16, 0.5);

        int key = 42;
        int index = set.keyIndex(key);
        Assert.assertTrue(index >= 0);

        set.addAt(index, key);

        Assert.assertFalse(set.excludes(key));
        Assert.assertEquals(1, set.size());

        Assert.assertFalse(set.add(key));
    }

    @Test
    public void testClear() {
        CompactIntHashSet set = new CompactIntHashSet(16, 0.5);

        for (int i = 0; i < 100; i++) {
            set.add(i);
        }
        Assert.assertEquals(100, set.size());

        set.clear();
        Assert.assertEquals(0, set.size());

        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(set.excludes(i));
        }

        Assert.assertTrue(set.add(10));
        Assert.assertEquals(1, set.size());
    }

    @Test
    public void testLargeDataSet() {
        Rnd rnd = new Rnd();
        CompactIntHashSet set = new CompactIntHashSet(16, 0.5);
        final int N = 10000;

        for (int i = 0; i < N; i++) {
            set.add(rnd.nextPositiveInt());
        }

        Assert.assertEquals(N, set.size());

        rnd.reset();
        for (int i = 0; i < N; i++) {
            int value = rnd.nextPositiveInt();
            Assert.assertFalse(set.excludes(value));
        }

        for (int i = 0; i < 1000; i++) {
            int value = rnd.nextPositiveInt();
            Assert.assertTrue(set.excludes(value));
        }
    }

    @Test
    public void testRehashing() {
        CompactIntHashSet set = new CompactIntHashSet(4, 0.5);

        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(set.add(i));
        }

        Assert.assertEquals(100, set.size());

        for (int i = 0; i < 100; i++) {
            Assert.assertFalse(set.excludes(i));
        }
    }

    @Test
    public void testRemove() {
        CompactIntHashSet set = new CompactIntHashSet(16, 0.5);

        for (int i = 0; i < 20; i++) {
            set.add(i);
        }
        Assert.assertEquals(20, set.size());

        for (int i = 0; i < 20; i += 2) {
            Assert.assertTrue(set.remove(i) > -1);
        }
        Assert.assertEquals(10, set.size());

        for (int i = 0; i < 20; i++) {
            if (i % 2 == 0) {
                Assert.assertTrue(set.excludes(i));
            } else {
                Assert.assertFalse(set.excludes(i));
            }
        }

        Assert.assertEquals(-1, set.remove(100));
    }

    @Test
    public void testRemoveWithCollisions() {
        Rnd rnd = new Rnd();
        CompactIntHashSet set = new CompactIntHashSet(16, 0.5);

        final int N = 1000;
        for (int i = 0; i < N; i++) {
            set.add(rnd.nextPositiveInt());
        }

        rnd.reset();

        int removed = 0;
        for (int i = 0; i < N; i++) {
            int value = rnd.nextPositiveInt();
            if (i % 3 == 0) {
                Assert.assertTrue(set.remove(value) > -1);
                removed++;
            }
        }

        Assert.assertEquals(N - removed, set.size());

        rnd.reset();
        for (int i = 0; i < N; i++) {
            int value = rnd.nextPositiveInt();
            if (i % 3 == 0) {
                Assert.assertTrue(set.excludes(value));
            } else {
                Assert.assertFalse(set.excludes(value));
            }
        }
    }

    @Test
    public void testSequentialValues() {
        CompactIntHashSet set = new CompactIntHashSet(16, 0.5);

        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue("conflict while inserting " + i, set.add(i));
        }

        Assert.assertEquals(1000, set.size());

        for (int i = 0; i < 1000; i++) {
            Assert.assertFalse(set.excludes(i));
        }
    }

    @Test
    public void testZeroAndNegativeValues() {
        CompactIntHashSet set = new CompactIntHashSet(16, 0.5);

        Assert.assertTrue(set.add(0));
        Assert.assertFalse(set.excludes(0));

        Assert.assertTrue(set.add(-100));
        Assert.assertTrue(set.add(Integer.MIN_VALUE));

        Assert.assertEquals(3, set.size());

        Assert.assertFalse(set.excludes(0));
        Assert.assertFalse(set.excludes(-100));
        Assert.assertFalse(set.excludes(Integer.MIN_VALUE));
    }
}