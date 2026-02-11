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

import io.questdb.std.DirectIntMultiLongHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class DirectIntMultiLongHashMapTest {

    @Test
    public void testBasicOperations() {
        try (DirectIntMultiLongHashMap map = new DirectIntMultiLongHashMap(4, 0.5, Integer.MIN_VALUE, 0, 3, MemoryTag.NATIVE_DEFAULT)) {
            Assert.assertEquals(3, map.getValueCount());
            Assert.assertEquals(0, map.size());
            Assert.assertTrue(map.excludes(1));

            map.put(1, 0, 100L);
            map.put(1, 1, 200L);
            map.put(1, 2, 300L);

            Assert.assertEquals(1, map.size());
            Assert.assertFalse(map.excludes(1));

            Assert.assertEquals(100L, map.get(1, 0));
            Assert.assertEquals(200L, map.get(1, 1));
            Assert.assertEquals(300L, map.get(1, 2));

            Assert.assertEquals(0L, map.get(999, 0));
            Assert.assertTrue(map.excludes(999));
        }
    }

    @Test
    public void testClearAndRestore() {
        try (DirectIntMultiLongHashMap map = new DirectIntMultiLongHashMap(4, 0.5, Integer.MIN_VALUE, 0, 2, MemoryTag.NATIVE_DEFAULT)) {
            map.put(1, 0, 100L);
            map.put(2, 1, 200L);
            Assert.assertEquals(2, map.size());

            map.clear();
            Assert.assertEquals(0, map.size());
            Assert.assertTrue(map.excludes(1));
            Assert.assertTrue(map.excludes(2));

            map.restoreInitialCapacity();
            Assert.assertEquals(0, map.size());
            Assert.assertEquals(8, map.capacity()); // Should match initial capacity
        }
    }

    @Test
    public void testMultipleKeys() {
        try (DirectIntMultiLongHashMap map = new DirectIntMultiLongHashMap(4, 0.5, Integer.MIN_VALUE, 0, 2, MemoryTag.NATIVE_DEFAULT)) {
            final int N = 100;

            for (int i = 0; i < N; i++) {
                map.put(i, 0, (long) i * 10);
                map.put(i, 1, (long) i * 20);
            }

            Assert.assertEquals(N, map.size());

            for (int i = 0; i < N; i++) {
                Assert.assertFalse(map.excludes(i));
                Assert.assertEquals((long) i * 10, map.get(i, 0));
                Assert.assertEquals((long) i * 20, map.get(i, 1));
            }
        }
    }

    @Test
    public void testPartialUpdates() {
        try (DirectIntMultiLongHashMap map = new DirectIntMultiLongHashMap(4, 0.5, Integer.MIN_VALUE, 0, 3, MemoryTag.NATIVE_DEFAULT)) {
            map.put(10, 1, 500L);  // Only set middle value
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(0L, map.get(10, 0));    // Should be 0 (default)
            Assert.assertEquals(500L, map.get(10, 1));  // Should be set value
            Assert.assertEquals(0L, map.get(10, 2));    // Should be 0 (default)
            map.put(10, 0, 100L);
            map.put(10, 2, 900L);

            Assert.assertEquals(1, map.size()); // Size shouldn't change
            Assert.assertEquals(100L, map.get(10, 0));
            Assert.assertEquals(500L, map.get(10, 1));
            Assert.assertEquals(900L, map.get(10, 2));
        }
    }

    @Test
    public void testPutAll() {
        try (DirectIntMultiLongHashMap map = new DirectIntMultiLongHashMap(4, 0.5, Integer.MIN_VALUE, 0, 2, MemoryTag.NATIVE_DEFAULT)) {
            long[] values = {1000L, 2000L};
            map.putAll(5, values);
            Assert.assertEquals(1, map.size());

            Assert.assertEquals(1000L, map.get(5, 0));
            Assert.assertEquals(2000L, map.get(5, 1));
            values[0] = 3000L;
            values[1] = 4000L;
            map.putAll(5, values);
            Assert.assertEquals(1, map.size()); // Size shouldn't change
            Assert.assertEquals(3000L, map.get(5, 0));
            Assert.assertEquals(4000L, map.get(5, 1));
        }
    }

    @Test
    public void testRandomOperations() {
        Rnd rnd = new Rnd();
        try (DirectIntMultiLongHashMap map = new DirectIntMultiLongHashMap(4, 0.5, Integer.MIN_VALUE, 0, 3, MemoryTag.NATIVE_DEFAULT)) {
            final int N = 1000;
            long[] expectedValues = new long[N * 3]; // Store expected values

            for (int i = 0; i < N; i++) {
                int key = rnd.nextInt(N);
                long value0 = rnd.nextLong();
                long value1 = rnd.nextLong();
                long value2 = rnd.nextLong();

                map.put(key, 0, value0);
                map.put(key, 1, value1);
                map.put(key, 2, value2);

                expectedValues[key * 3] = value0;
                expectedValues[key * 3 + 1] = value1;
                expectedValues[key * 3 + 2] = value2;
            }

            rnd.reset();
            for (int i = 0; i < N / 10; i++) {  // Check 10% of the data
                int key = rnd.nextInt(N);
                if (!map.excludes(key)) {
                    Assert.assertEquals(expectedValues[key * 3], map.get(key, 0));
                    Assert.assertEquals(expectedValues[key * 3 + 1], map.get(key, 1));
                    Assert.assertEquals(expectedValues[key * 3 + 2], map.get(key, 2));
                }
            }
        }
    }

    @Test
    public void testRehashing() {
        try (DirectIntMultiLongHashMap map = new DirectIntMultiLongHashMap(4, 0.5, Integer.MIN_VALUE, 0, 2, MemoryTag.NATIVE_DEFAULT)) {
            final int initialCapacity = map.capacity();
            final int N = initialCapacity * 2;

            for (int i = 0; i < N; i++) {
                long[] values = {(long) i * 100, (long) i * 200};
                map.putAll(i, values);
            }

            Assert.assertTrue(map.capacity() > initialCapacity);
            Assert.assertEquals(N, map.size());

            for (int i = 0; i < N; i++) {
                Assert.assertEquals((long) i * 100, map.get(i, 0));
                Assert.assertEquals((long) i * 200, map.get(i, 1));
            }
        }
    }
}