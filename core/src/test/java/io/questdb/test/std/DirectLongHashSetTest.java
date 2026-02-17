/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \|_| |_| | |_) |
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
import io.questdb.std.DirectLongHashSet;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class DirectLongHashSetTest {
    private static final Log LOG = LogFactory.getLog(DirectLongHashSetTest.class);

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
    public void testFuzz() {
        final int N = 100000;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final long seed0 = rnd.getSeed0();
        final long seed1 = rnd.getSeed1();

        try (DirectLongHashSet set = new DirectLongHashSet(16)) {
            Set<Long> referenceSet = new HashSet<>();
            for (int i = 0; i < N; i++) {
                long val = rnd.nextLong();
                set.add(val);
                referenceSet.add(val);
            }

            Assert.assertEquals(referenceSet.size(), set.size());
            Assert.assertTrue(set.capacity() >= referenceSet.size());

            rnd.reset(seed0, seed1);
            for (int i = 0; i < N; i++) {
                long val = rnd.nextLong();
                Assert.assertTrue(set.contains(val));
            }

            set.clear();
            Assert.assertEquals(0, set.size());
            rnd.reset(seed0, seed1);
            referenceSet.clear();
            for (int i = 0; i < N; i++) {
                long val = rnd.nextLong();
                int keyIndex = set.keyIndex(val);
                boolean wasInSet = referenceSet.contains(val);

                if (wasInSet) {
                    Assert.assertTrue(keyIndex < 0);
                } else {
                    Assert.assertTrue(keyIndex >= 0);
                }

                set.add(val);
                referenceSet.add(val);
            }

            Assert.assertEquals(referenceSet.size(), set.size());
            rnd.reset(seed0, seed1);
            for (int i = 0; i < N; i++) {
                Assert.assertTrue(set.contains(rnd.nextLong()));
            }
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

            StringSink sink = new StringSink();
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
