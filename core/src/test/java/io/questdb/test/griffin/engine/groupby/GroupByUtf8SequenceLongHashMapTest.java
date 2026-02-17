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
import io.questdb.griffin.engine.groupby.GroupByUtf8SequenceLongHashMap;
import io.questdb.griffin.engine.groupby.GroupByUtf8Sink;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GroupByUtf8SequenceLongHashMapTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testBasicOperations() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);

            Assert.assertEquals(0, map.size());

            Utf8String key1 = new Utf8String("key1");
            map.of(0).inc(key1);
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(1L, map.get(key1));

            map.inc(key1);
            Assert.assertEquals(2L, map.get(key1));

            Utf8String key2 = new Utf8String("key2");
            map.inc(key2);
            Assert.assertEquals(1L, map.get(key2));
            Assert.assertEquals(2L, map.get(key1));
            Assert.assertEquals(2, map.size());
        }
    }

    @Test
    public void testCollisionHandling() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            Utf8String[] keys = new Utf8String[20];
            for (int i = 0; i < keys.length; i++) {
                keys[i] = new Utf8String("key_" + (i * 4));
                map.inc(keys[i]);
            }

            Assert.assertEquals(keys.length, map.size());

            for (Utf8String key : keys) {
                Assert.assertEquals(1L, map.get(key));
            }
        }
    }

    @Test
    public void testEmptyStringKey() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            Utf8String emptyKey = new Utf8String("");
            Utf8String nonEmptyKey = new Utf8String("non_empty");

            map.inc(emptyKey);
            Assert.assertEquals(1L, map.get(emptyKey));

            map.inc(nonEmptyKey);
            Assert.assertEquals(1L, map.get(nonEmptyKey));

            Assert.assertEquals(2, map.size());
        }
    }

    @Test
    public void testGetNonExistentKey() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);

            Utf8String existingKey = new Utf8String("existing_key");
            Utf8String nonExistentKey = new Utf8String("non_existing_key");

            map.of(0).inc(existingKey);

            Assert.assertEquals(Numbers.LONG_NULL, map.get(nonExistentKey));
            Assert.assertEquals(1L, map.get(existingKey));
        }
    }

    @Test
    public void testIncrement() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);

            Utf8String testKey = new Utf8String("test_key");
            map.of(0).inc(testKey);

            for (int i = 0; i < 10; i++) {
                map.inc(testKey);
            }

            Assert.assertEquals(11L, map.get(testKey));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testIncrementWithDelta() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            Utf8String key1 = new Utf8String("key1");
            Utf8String key2 = new Utf8String("key2");

            map.inc(key1, 5);
            Assert.assertEquals(5L, map.get(key1));

            map.inc(key1, 3);
            Assert.assertEquals(8L, map.get(key1));

            map.inc(key2, 10);
            Assert.assertEquals(10L, map.get(key2));
            Assert.assertEquals(2, map.size());
        }
    }

    @Test
    public void testIterateOverKeys() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8Sink sink = new GroupByUtf8Sink();
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            sink.setAllocator(allocator);
            map.of(0);

            Utf8String[] expectedKeys = {new Utf8String("key1"), new Utf8String("key2"), new Utf8String("key3")};
            int[] expectedCounts = {2, 3, 1};

            for (int i = 0; i < expectedKeys.length; i++) {
                for (int j = 0; j < expectedCounts[i]; j++) {
                    map.inc(expectedKeys[i]);
                }
            }

            int foundKeys = 0;
            for (int i = 0, n = map.capacity(); i < n; i++) {
                long keyPtr = map.keyAt(i);
                if (keyPtr != Numbers.LONG_NULL) {
                    foundKeys++;
                    sink.of(keyPtr);
                    long value = map.valueAt(i);

                    boolean keyFound = false;
                    for (int j = 0; j < expectedKeys.length; j++) {
                        if (Utf8s.equals(expectedKeys[j], sink)) {
                            Assert.assertEquals(expectedCounts[j], value);
                            keyFound = true;
                            break;
                        }
                    }
                    Assert.assertTrue("Found unexpected key: " + sink, keyFound);
                }
            }

            Assert.assertEquals(expectedKeys.length, foundKeys);
        }
    }

    @Test
    public void testLargeValues() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            Utf8String largeKey = new Utf8String("large_key_with_long_content");

            for (int i = 0; i < 1000; i++) {
                map.inc(largeKey);
            }

            Assert.assertEquals(1000L, map.get(largeKey));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testLongKeys() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            Utf8String longKey = new Utf8String("this_is_a_very_long_key_that_should_test_the_buffer_management_capabilities_of_the_hash_map_implementation");
            map.inc(longKey);
            Assert.assertEquals(1L, map.get(longKey));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testMergeAdd() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map1 = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            GroupByUtf8SequenceLongHashMap map2 = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map1.setAllocator(allocator);
            map2.setAllocator(allocator);

            Utf8String key1 = new Utf8String("key1");
            Utf8String key2 = new Utf8String("key2");
            Utf8String key3 = new Utf8String("key3");

            map1.of(0);
            map1.inc(key1);
            map1.inc(key1);
            map1.inc(key2);
            // key1 = 2
            // key2 = 1


            map2.of(0);
            map2.inc(key1);
            map2.inc(key3);
            map2.inc(key3);
            // key1 = 1
            // key3 = 2

            long mergedPtr = map1.mergeAdd(map2);
            map1.of(mergedPtr);

            Assert.assertEquals(3L, map1.get(key1));
            Assert.assertEquals(1L, map1.get(key2));
            Assert.assertEquals(2L, map1.get(key3));
            Assert.assertEquals(3, map1.size());
        }
    }

    @Test
    public void testMergeAddEmptyMaps() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map1 = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            GroupByUtf8SequenceLongHashMap map2 = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
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
            GroupByUtf8SequenceLongHashMap map1 = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            GroupByUtf8SequenceLongHashMap map2 = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map1.setAllocator(allocator);
            map2.setAllocator(allocator);

            Utf8String key100 = new Utf8String("key100");
            Utf8String key200 = new Utf8String("key200");

            map1.of(0);
            map1.inc(key100);
            map1.inc(key200);

            map2.of(0);

            long mergedPtr = map1.mergeAdd(map2);
            map1.of(mergedPtr);

            Assert.assertEquals(1L, map1.get(key100));
            Assert.assertEquals(1L, map1.get(key200));
            Assert.assertEquals(2, map1.size());
        }
    }

    @Test
    public void testMultipleKeys() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            Utf8String[] keys = {
                    new Utf8String("a"),
                    new Utf8String("bb"),
                    new Utf8String("ccc"),
                    new Utf8String("dddd"),
                    new Utf8String("eeeee")
            };
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
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);

            Utf8String key1 = new Utf8String("key1");
            Utf8String key2 = new Utf8String("key2");

            map.of(0);
            map.inc(key1);
            long ptr1 = map.ptr();

            map.of(0);
            map.inc(key2);
            long ptr2 = map.ptr();

            map.of(ptr1);
            Assert.assertEquals(1L, map.get(key1));
            Assert.assertEquals(Numbers.LONG_NULL, map.get(key2));

            map.of(ptr2);
            Assert.assertEquals(Numbers.LONG_NULL, map.get(key1));
            Assert.assertEquals(1L, map.get(key2));
        }
    }

    @Test
    public void testPerformanceWithRandomData() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            Rnd rnd = new Rnd();

            map.of(0);

            final int n = 10000;
            final int keyRange = 1000;

            for (int i = 0; i < n; i++) {
                Utf8String key = new Utf8String("key_" + rnd.nextInt(keyRange));
                map.inc(key);
            }

            Assert.assertTrue("Map should contain reasonable number of entries", map.size() > 0);
            Assert.assertTrue("Map should not exceed key range", map.size() <= keyRange);

            long totalCount = 0;
            for (int i = 0, capacity = map.capacity(); i < capacity; i++) {
                long keyPtr = map.keyAt(i);
                if (keyPtr != Numbers.LONG_NULL) {
                    totalCount += map.valueAt(i);
                }
            }


            Assert.assertEquals("Total count should equal number of insertions", n, totalCount);
        }
    }

    @Test
    public void testPutMethod() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            Utf8String key1 = new Utf8String("key1");

            map.put(key1, 42L);
            Assert.assertEquals(42L, map.get(key1));
            Assert.assertEquals(1, map.size());

            map.put(key1, 100L);
            Assert.assertEquals(100L, map.get(key1));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testResetPtr() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            Utf8String key1 = new Utf8String("key1");
            Utf8String key2 = new Utf8String("key2");

            map.inc(key1);
            map.inc(key2);

            Assert.assertEquals(2, map.size());

            map.resetPtr();
            map.of(0);

            Assert.assertEquals(0, map.size());
        }
    }

    @Test
    public void testResize() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(4, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            for (int i = 0; i < 100; i++) {
                Utf8String key = new Utf8String("key_" + i);
                map.inc(key);
            }

            Assert.assertEquals(100, map.size());
            Assert.assertTrue(map.capacity() > 4);

            for (int i = 0; i < 100; i++) {
                Utf8String key = new Utf8String("key_" + i);
                Assert.assertEquals(1L, map.get(key));
            }
        }
    }

    @Test
    public void testUnicodeKeys() {
        try (GroupByAllocator allocator = new FastGroupByAllocator(16 * 1024, Numbers.SIZE_1GB)) {
            GroupByUtf8SequenceLongHashMap map = new GroupByUtf8SequenceLongHashMap(16, 0.7, Numbers.LONG_NULL, Numbers.LONG_NULL);
            map.setAllocator(allocator);
            map.of(0);

            Utf8String unicodeKey1 = new Utf8String("hello_世界");
            Utf8String unicodeKey2 = new Utf8String("test_🚀");
            Utf8String unicodeKey3 = new Utf8String("café");

            map.inc(unicodeKey1);
            map.inc(unicodeKey2);
            map.inc(unicodeKey3);

            Assert.assertEquals(1L, map.get(unicodeKey1));
            Assert.assertEquals(1L, map.get(unicodeKey2));
            Assert.assertEquals(1L, map.get(unicodeKey3));
            Assert.assertEquals(3, map.size());
        }
    }
}