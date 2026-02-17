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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByLongLongHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GroupByLongLongHashMapTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testBasicOperations() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);

            Assert.assertEquals(0, map.size());

            map.of(0).inc(10L);
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(1L, map.get(10L));

            map.inc(10L);
            Assert.assertEquals(2L, map.get(10L));

            map.inc(20L);
            Assert.assertEquals(1L, map.get(20L));
            Assert.assertEquals(2L, map.get(10L));
            Assert.assertEquals(2, map.size());
        }
    }

    @Test
    public void testCollisionHandling() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            long[] keys = new long[20];
            for (int i = 0; i < keys.length; i++) {
                keys[i] = i * 4;
                map.inc(keys[i]);
            }

            Assert.assertEquals(keys.length, map.size());

            for (long key : keys) {
                Assert.assertEquals(1L, map.get(key));
            }
        }
    }

    @Test
    public void testGetNonExistentKey() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0).inc(10L);

            Assert.assertEquals(Numbers.LONG_NULL, map.get(999L));
            Assert.assertEquals(1L, map.get(10L));
        }
    }

    @Test
    public void testIncrement() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0).inc(100L);

            for (int i = 0; i < 10; i++) {
                map.inc(100L);
            }

            Assert.assertEquals(11L, map.get(100L));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testIterateOverKeys() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            long[] expectedKeys = {10L, 20L, 30L};
            int[] expectedCounts = {2, 3, 1};

            for (int i = 0; i < expectedKeys.length; i++) {
                for (int j = 0; j < expectedCounts[i]; j++) {
                    map.inc(expectedKeys[i]);
                }
            }

            int foundKeys = 0;
            for (int i = 0, n = map.capacity(); i < n; i++) {
                long key = map.keyAt(i);
                if (key != Numbers.LONG_NULL) {
                    foundKeys++;
                    long value = map.valueAt(i);

                    boolean keyFound = false;
                    for (int j = 0; j < expectedKeys.length; j++) {
                        if (expectedKeys[j] == key) {
                            Assert.assertEquals(expectedCounts[j], value);
                            keyFound = true;
                            break;
                        }
                    }
                    Assert.assertTrue("Found unexpected key: " + key, keyFound);
                }
            }

            Assert.assertEquals(expectedKeys.length, foundKeys);
        }
    }

    @Test
    public void testLargeValues() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            long largeKey = Long.MAX_VALUE - 1;

            for (int i = 0; i < 1000; i++) {
                map.inc(largeKey);
            }

            Assert.assertEquals(1000L, map.get(largeKey));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testMergeAdd() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map1 = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            GroupByLongLongHashMap map2 = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map1.setAllocator(allocator);
            map2.setAllocator(allocator);

            map1.of(0);
            map1.inc(10L);
            map1.inc(10L);
            map1.inc(20L);

            map2.of(0);
            map2.inc(10L);
            map2.inc(30L);
            map2.inc(30L);

            long mergedPtr = map1.mergeAdd(map2);
            map1.of(mergedPtr);

            Assert.assertEquals(3L, map1.get(10L));
            Assert.assertEquals(1L, map1.get(20L));
            Assert.assertEquals(2L, map1.get(30L));
            Assert.assertEquals(3, map1.size());
        }
    }

    @Test
    public void testMergeAddEmptyMaps() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map1 = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            GroupByLongLongHashMap map2 = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map1.setAllocator(allocator);
            map2.setAllocator(allocator);

            map1.of(0);
            map2.of(0);

            long mergedPtr = map1.mergeAdd(map2);
            map1.of(mergedPtr);

            Assert.assertEquals(0, map1.size());
        }
    }

    @Test
    public void testMergeAddOneEmpty() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map1 = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            GroupByLongLongHashMap map2 = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map1.setAllocator(allocator);
            map2.setAllocator(allocator);

            map1.of(0);
            map1.inc(100L);
            map1.inc(200L);

            map2.of(0);

            long mergedPtr = map1.mergeAdd(map2);
            map1.of(mergedPtr);

            Assert.assertEquals(1L, map1.get(100L));
            Assert.assertEquals(1L, map1.get(200L));
            Assert.assertEquals(2, map1.size());
        }
    }

    @Test
    public void testMultipleKeys() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            long[] keys = {1L, 2L, 3L, 4L, 5L};
            int[] counts = {3, 7, 2, 9, 1};

            for (int i = 0; i < keys.length; i++) {
                for (int j = 0; j < counts[i]; j++) {
                    map.inc(keys[i]);
                }
            }

            Assert.assertEquals(keys.length, map.size());

            for (int i = 0; i < keys.length; i++) {
                Assert.assertEquals(counts[i], map.get(keys[i]));
            }
        }
    }

    @Test
    public void testOfWithDifferentPointers() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);
            map.inc(10L);
            long ptr1 = map.ptr();

            map.of(0);
            map.inc(20L);
            long ptr2 = map.ptr();

            map.of(ptr1);
            Assert.assertEquals(1L, map.get(10L));
            Assert.assertEquals(Numbers.LONG_NULL, map.get(20L));

            map.of(ptr2);
            Assert.assertEquals(Numbers.LONG_NULL, map.get(10L));
            Assert.assertEquals(1L, map.get(20L));
        }
    }

    @Test
    public void testPerformanceWithRandomData() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            Rnd rnd = new Rnd();

            map.of(0);

            final int n = 10000;
            final int keyRange = 1000;

            for (int i = 0; i < n; i++) {
                long key = rnd.nextLong(keyRange);
                map.inc(key);
            }

            Assert.assertTrue("Map should contain reasonable number of entries", map.size() > 0);
            Assert.assertTrue("Map should not exceed key range", map.size() <= keyRange);

            long totalCount = 0;
            for (int i = 0, capacity = map.capacity(); i < capacity; i++) {
                if (map.keyAt(i) != Numbers.LONG_NULL) {
                    totalCount += map.valueAt(i);
                }
            }

            Assert.assertEquals("Total count should equal number of insertions", n, totalCount);
        }
    }

    @Test
    public void testResetPtr() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);
            map.inc(10L);
            map.inc(20L);

            Assert.assertEquals(2, map.size());

            map.resetPtr();
            map.of(0);

            Assert.assertEquals(0, map.size());
        }
    }

    @Test
    public void testResize() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            for (int i = 0; i < 100; i++) {
                map.inc(i);
            }

            Assert.assertEquals(100, map.size());
            Assert.assertTrue(map.capacity() > 4);

            for (int i = 0; i < 100; i++) {
                Assert.assertEquals(1L, map.get(i));
            }
        }
    }

    @Test
    public void testWithNullKeys() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            map.inc(Numbers.LONG_NULL);
            Assert.assertEquals(Numbers.LONG_NULL, map.get(Numbers.LONG_NULL));

            map.inc(10L);
            Assert.assertEquals(1L, map.get(10L));

            Assert.assertEquals(1, map.size());
        }
    }
}