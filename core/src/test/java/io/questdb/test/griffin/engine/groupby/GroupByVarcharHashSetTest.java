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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByVarcharHashSet;
import io.questdb.std.Numbers;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class GroupByVarcharHashSetTest extends AbstractCairoTest {

    @Test
    public void testAddAndContains() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(16, 0.7);
                set.setAllocator(allocator);
                set.of(0);

                Utf8String key1 = new Utf8String("hello");
                Utf8String key2 = new Utf8String("world");
                Utf8String key3 = new Utf8String("hello"); // duplicate

                Assert.assertTrue(set.add(key1));
                Assert.assertEquals(1, set.size());

                Assert.assertTrue(set.add(key2));
                Assert.assertEquals(2, set.size());

                Assert.assertFalse(set.add(key3));
                Assert.assertEquals(2, set.size());

                Assert.assertTrue(set.keyIndex(key1) < 0);
                Assert.assertTrue(set.keyIndex(key2) < 0);
                Assert.assertTrue(set.keyIndex(new Utf8String("unknown")) >= 0);
            }
        });
    }

    @Test
    public void testCollisionHandling() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                // Use small initial capacity to force collisions
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(4, 0.9);
                set.setAllocator(allocator);
                set.of(0);

                // Add strings that might collide
                String[] keys = new String[20];
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = "key_" + i;
                    Assert.assertTrue("Failed to add key: " + keys[i], set.add(new Utf8String(keys[i])));
                }

                Assert.assertEquals(keys.length, set.size());

                // Verify all keys are findable despite collisions
                for (String key : keys) {
                    long index = set.keyIndex(new Utf8String(key));
                    Assert.assertTrue("Key not found: " + key, index < 0);
                }
            }
        });
    }

    @Test
    public void testEmptySet() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(16, 0.7);
                set.setAllocator(allocator);
                set.of(0);

                Assert.assertEquals(0, set.size());
                Assert.assertEquals(16, set.capacity());

                // Should not find any key
                Assert.assertTrue(set.keyIndex(new Utf8String("anything")) >= 0);
            }
        });
    }

    @Test
    public void testEmptyStrings() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(16, 0.7);
                set.setAllocator(allocator);
                set.of(0);

                Utf8String empty1 = new Utf8String("");
                Utf8String empty2 = new Utf8String("");

                Assert.assertTrue(set.add(empty1));
                Assert.assertEquals(1, set.size());

                Assert.assertFalse(set.add(empty2));
                Assert.assertEquals(1, set.size());

                Assert.assertTrue(set.keyIndex(empty1) < 0);
            }
        });
    }

    @Test
    public void testGrowthAndRehashing() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(4, 0.5);
                set.setAllocator(allocator);
                set.of(0);

                // Add enough elements to trigger rehashing
                String[] keys = {"one", "two", "three", "four", "five", "six", "seven", "eight"};
                for (String key : keys) {
                    Assert.assertTrue(set.add(new Utf8String(key)));
                }

                Assert.assertEquals(keys.length, set.size());
                Assert.assertTrue(set.capacity() > 4); // Should have grown

                // Verify all keys are still findable
                for (String key : keys) {
                    Assert.assertTrue(set.keyIndex(new Utf8String(key)) < 0);
                }
            }
        });
    }

    @Test
    public void testKeyIndexBehavior() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(16, 0.7);
                set.setAllocator(allocator);
                set.of(0);

                Utf8String key = new Utf8String("test");

                // Before adding, keyIndex should return positive
                long index = set.keyIndex(key);
                Assert.assertTrue(index >= 0);

                // Add using the index
                set.addAt(index, key);

                // After adding, keyIndex should return negative
                long index2 = set.keyIndex(key);
                Assert.assertTrue(index2 < 0);
                Assert.assertEquals(-index - 1, index2);
            }
        });
    }

    @Test
    public void testLongStrings() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(16, 0.7);
                set.setAllocator(allocator);
                set.of(0);

                // Create long strings
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 1000; i++) {
                    sb.append("a");
                }
                String longStr = sb.toString();

                Utf8String key1 = new Utf8String(longStr);
                Utf8String key2 = new Utf8String(longStr + "b");
                Utf8String key3 = new Utf8String(longStr); // duplicate

                Assert.assertTrue(set.add(key1));
                Assert.assertTrue(set.add(key2));
                Assert.assertFalse(set.add(key3));

                Assert.assertEquals(2, set.size());
            }
        });
    }

    @Test
    public void testMergeDisjointSets() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet setA = new GroupByVarcharHashSet(16, 0.7);
                setA.setAllocator(allocator);
                setA.of(0);

                GroupByVarcharHashSet setB = new GroupByVarcharHashSet(16, 0.7);
                setB.setAllocator(allocator);
                setB.of(0);

                // Add to set A
                setA.add(new Utf8String("alpha"));
                setA.add(new Utf8String("beta"));
                setA.add(new Utf8String("gamma"));

                // Add to set B (disjoint)
                setB.add(new Utf8String("delta"));
                setB.add(new Utf8String("epsilon"));

                Assert.assertEquals(3, setA.size());
                Assert.assertEquals(2, setB.size());

                // Merge B into A
                setA.merge(setB);

                Assert.assertEquals(5, setA.size());

                // Verify all keys exist
                Assert.assertTrue(setA.keyIndex(new Utf8String("alpha")) < 0);
                Assert.assertTrue(setA.keyIndex(new Utf8String("beta")) < 0);
                Assert.assertTrue(setA.keyIndex(new Utf8String("gamma")) < 0);
                Assert.assertTrue(setA.keyIndex(new Utf8String("delta")) < 0);
                Assert.assertTrue(setA.keyIndex(new Utf8String("epsilon")) < 0);
            }
        });
    }

    @Test
    public void testMergeEmptySet() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet setA = new GroupByVarcharHashSet(16, 0.7);
                setA.setAllocator(allocator);
                setA.of(0);

                GroupByVarcharHashSet setB = new GroupByVarcharHashSet(16, 0.7);
                setB.setAllocator(allocator);
                setB.of(0);

                // Add to set A
                setA.add(new Utf8String("test"));

                // Set B is empty
                Assert.assertEquals(1, setA.size());
                Assert.assertEquals(0, setB.size());

                // Merge empty set
                setA.merge(setB);

                Assert.assertEquals(1, setA.size());
                Assert.assertTrue(setA.keyIndex(new Utf8String("test")) < 0);
            }
        });
    }

    @Test
    public void testMergeOverlappingSets() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet setA = new GroupByVarcharHashSet(16, 0.7);
                setA.setAllocator(allocator);
                setA.of(0);

                GroupByVarcharHashSet setB = new GroupByVarcharHashSet(16, 0.7);
                setB.setAllocator(allocator);
                setB.of(0);

                // Add to set A
                setA.add(new Utf8String("one"));
                setA.add(new Utf8String("two"));
                setA.add(new Utf8String("three"));

                // Add to set B (overlapping)
                setB.add(new Utf8String("two"));
                setB.add(new Utf8String("three"));
                setB.add(new Utf8String("four"));
                setB.add(new Utf8String("five"));

                Assert.assertEquals(3, setA.size());
                Assert.assertEquals(4, setB.size());

                // Merge B into A
                setA.merge(setB);

                // Should have union of both sets
                Assert.assertEquals(5, setA.size());

                // Verify all unique keys exist
                Assert.assertTrue(setA.keyIndex(new Utf8String("one")) < 0);
                Assert.assertTrue(setA.keyIndex(new Utf8String("two")) < 0);
                Assert.assertTrue(setA.keyIndex(new Utf8String("three")) < 0);
                Assert.assertTrue(setA.keyIndex(new Utf8String("four")) < 0);
                Assert.assertTrue(setA.keyIndex(new Utf8String("five")) < 0);
            }
        });
    }

    @Test
    public void testResetPtr() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(16, 0.7);
                set.setAllocator(allocator);
                set.of(0);

                // Add some data
                set.add(new Utf8String("test"));
                Assert.assertEquals(1, set.size());

                // Reset pointer
                set.resetPtr();
                Assert.assertEquals(0, set.size());
                Assert.assertEquals(0, set.capacity());

                // Re-initialize
                set.of(0);
                Assert.assertEquals(0, set.size());
                Assert.assertEquals(16, set.capacity());

                // Should be able to add again
                Assert.assertTrue(set.add(new Utf8String("test")));
            }
        });
    }

    @Test
    public void testUtf8Sequences() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(16, 0.7);
                set.setAllocator(allocator);
                set.of(0);

                // Test with various UTF-8 sequences
                String[] utf8Strings = {
                        "hello",
                        "こんにちは",  // Japanese
                        "你好",        // Chinese
                        "مرحبا",      // Arabic
                        "Здравствуйте", // Russian
                        "🎉🎊",       // Emojis
                        "café",       // Accented characters
                        "naïve"
                };

                for (String str : utf8Strings) {
                    Assert.assertTrue(set.add(new Utf8String(str)));
                }

                Assert.assertEquals(utf8Strings.length, set.size());

                // Verify all can be found
                for (String str : utf8Strings) {
                    Assert.assertTrue(set.keyIndex(new Utf8String(str)) < 0);
                }
            }
        });
    }
}