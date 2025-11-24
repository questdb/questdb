/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \|_| |_| | |_) |
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

import io.questdb.std.DirectLongHashSet;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class DirectLongHashSetTest {

    @Test
    public void testAddAndContains() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            Assert.assertTrue(set.add(100));
            Assert.assertTrue(set.add(200));
            Assert.assertTrue(set.add(300));

            Assert.assertTrue(set.contains(100));
            Assert.assertTrue(set.contains(200));
            Assert.assertTrue(set.contains(300));
            Assert.assertFalse(set.contains(400));
            Assert.assertEquals(3, set.size());
        }
    }

    @Test
    public void testAddDuplicate() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            Assert.assertTrue(set.add(100));
            Assert.assertFalse(set.add(100));
            Assert.assertEquals(1, set.size());
        }
    }

    @Test
    public void testAddZero() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            Assert.assertTrue(set.add(0));
            Assert.assertTrue(set.contains(0));
            Assert.assertFalse(set.add(0));
            Assert.assertEquals(1, set.size());
        }
    }

    @Test
    public void testBasicOperations() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            Rnd rnd = new Rnd();
            final int N = 1000;

            for (int i = 0; i < N; i++) {
                set.add(rnd.nextPositiveLong());
            }

            Assert.assertEquals(N, set.size());

            rnd.reset();

            for (int i = 0; i < N; i++) {
                Assert.assertTrue(set.keyIndex(rnd.nextPositiveLong()) < 0);
            }

            rnd.reset();

            for (int i = 0; i < N; i++) {
                Assert.assertTrue(set.contains(rnd.nextPositiveLong()));
            }
        }
    }

    @Test
    public void testClear() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            set.add(1);
            set.add(2);
            set.add(0);
            Assert.assertEquals(3, set.size());

            set.clear();
            Assert.assertEquals(0, set.size());
            Assert.assertFalse(set.contains(1));
            Assert.assertFalse(set.contains(2));
            Assert.assertFalse(set.contains(0));
        }
    }

    @Test
    public void testExcludes() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            set.add(100);
            Assert.assertFalse(set.excludes(100));
            Assert.assertTrue(set.excludes(200));
        }
    }

    @Test
    public void testMixedPositiveAndNegativeValues() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            Assert.assertTrue(set.add(1));
            Assert.assertTrue(set.add(2));
            Assert.assertTrue(set.add(3));
            Assert.assertTrue(set.add(-1));
            Assert.assertTrue(set.add(-2));
            Assert.assertTrue(set.add(0));

            Assert.assertEquals(6, set.size());

            // Verify all values are present
            Assert.assertTrue(set.contains(1));
            Assert.assertTrue(set.contains(2));
            Assert.assertTrue(set.contains(3));
            Assert.assertTrue(set.contains(-1));
            Assert.assertTrue(set.contains(-2));
            Assert.assertTrue(set.contains(0));

            // Verify duplicates are rejected
            Assert.assertFalse(set.add(1));
            Assert.assertFalse(set.add(-1));
            Assert.assertFalse(set.add(0));
            Assert.assertEquals(6, set.size());

            // Verify non-existent values
            Assert.assertFalse(set.contains(4));
            Assert.assertFalse(set.contains(-3));
        }
    }

    @Test
    public void testRehash() {
        try (DirectLongHashSet set = new DirectLongHashSet(4)) {
            for (int i = 0; i < 1000; i++) {
                set.add(i);
            }

            Assert.assertEquals(1000, set.size());
            for (int i = 0; i < 1000; i++) {
                Assert.assertTrue(set.contains(i));
            }
        }
    }

    @Test
    public void testRemoveWithCollisions() {
        try (DirectLongHashSet set = new DirectLongHashSet(4)) {
            set.add(1);
            set.add(5);
            set.add(9);
            set.add(13);
            Assert.assertEquals(4, set.size());
            set.remove(5);
            Assert.assertEquals(3, set.size());
            Assert.assertFalse(set.contains(5));
            Assert.assertTrue(set.contains(1));
            Assert.assertTrue(set.contains(9));
            Assert.assertTrue(set.contains(13));
            set.add(17);
            Assert.assertTrue(set.contains(17));
            Assert.assertEquals(4, set.size());
        }
    }

    @Test
    public void testRemoveZero() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            set.add(0);
            set.add(1);
            set.add(2);
            Assert.assertEquals(3, set.size());
            Assert.assertTrue(set.remove(0) >= 0);
            Assert.assertEquals(2, set.size());
            Assert.assertFalse(set.contains(0));
            Assert.assertTrue(set.contains(1));
            Assert.assertTrue(set.contains(2));
            Assert.assertEquals(-1, set.remove(0));
            Assert.assertTrue(set.remove(2) >= 0);
            Assert.assertFalse(set.contains(2));
        }
    }

    @Test
    public void testSentinelValues() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            Assert.assertTrue(set.add(0));
            Assert.assertTrue(set.add(-1));
            Assert.assertTrue(set.add(Long.MIN_VALUE));
            Assert.assertTrue(set.add(Long.MAX_VALUE));
            Assert.assertEquals(4, set.size());
            Assert.assertTrue(set.contains(0));
            Assert.assertTrue(set.contains(-1));
            Assert.assertTrue(set.contains(Long.MIN_VALUE));
            Assert.assertTrue(set.contains(Long.MAX_VALUE));
        }
    }

    @Test
    public void testToSinkWithNegativeValues() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            set.add(-1);
            set.add(-2);
            set.add(1);
            set.add(2);
            set.add(0);

            io.questdb.std.str.StringSink sink = new io.questdb.std.str.StringSink();
            set.toSink(sink);
            Assert.assertEquals("[-2,-1,0,1,2]", sink.toString());
        }
    }

    @Test
    public void testToSinkWithValues() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            set.add(3);
            set.add(1);
            set.add(2);
            StringSink sink = new StringSink();
            set.toSink(sink);
            Assert.assertEquals("[1,2,3]", sink.toString());
        }
    }

    @Test
    public void testToSinkWithZero() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            set.add(10);
            set.add(0);
            set.add(5);
            set.add(-5);

            StringSink sink = new StringSink();
            set.toSink(sink);
            Assert.assertEquals("[-5,0,5,10]", sink.toString());
        }
    }

    @Test
    public void testZeroWithOtherValues() {
        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            set.add(100);
            set.add(0);
            set.add(200);

            Assert.assertEquals(3, set.size());
            Assert.assertTrue(set.contains(0));
            Assert.assertTrue(set.contains(100));
            Assert.assertTrue(set.contains(200));
        }
    }
}
