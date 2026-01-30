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
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.json.JsonLexerTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GroupByVarcharHashSetFuzzTest extends AbstractCairoTest {

    @Test
    public void testFuzzBasic() throws Exception {
        assertMemoryLeak(() -> {
            final int N = 1000;
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final long seed0 = rnd.getSeed0();
            final long seed1 = rnd.getSeed1();

            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(16, 0.6);
                set.setAllocator(allocator);
                set.of(0);

                Set<String> referenceSet = new HashSet<>();
                List<String> addedStrings = new ArrayList<>();

                // Add random strings
                for (int i = 0; i < N; i++) {
                    String str = generateRandomString(rnd, 1, 50);
                    addedStrings.add(str);
                    Utf8String utf8String = new Utf8String(str);
                    boolean added = set.add(utf8String);
                    boolean refAdded = referenceSet.add(str);
                    Assert.assertEquals("Mismatch at string: " + str, refAdded, added);
                }

                Assert.assertEquals(referenceSet.size(), set.size());

                // Verify all strings are findable
                for (String str : referenceSet) {
                    long index = set.keyIndex(new Utf8String(str));
                    Assert.assertTrue("String not found: " + str, index < 0);
                }

                // Reset random and verify we can find all added strings
                rnd.reset(seed0, seed1);
                for (int i = 0; i < N; i++) {
                    String str = generateRandomString(rnd, 1, 50);
                    long index = set.keyIndex(new Utf8String(str));
                    if (referenceSet.contains(str)) {
                        Assert.assertTrue("Should find: " + str, index < 0);
                    } else {
                        Assert.assertTrue("Should not find: " + str, index >= 0);
                    }
                }
            }
        });
    }

    @Test
    public void testFuzzCollisionStress() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);

            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                // Very small initial capacity and high load factor to stress collisions
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(2, 0.95);
                set.setAllocator(allocator);
                set.of(0);

                Set<String> referenceSet = new HashSet<>();

                // Add many strings to force multiple rehashes and collision handling
                for (int i = 0; i < 1000; i++) {
                    String str = "collision_test_" + rnd.nextInt(500); // Limited range to force duplicates
                    boolean added = set.add(new Utf8String(str));
                    boolean refAdded = referenceSet.add(str);
                    Assert.assertEquals(refAdded, added);
                }

                Assert.assertEquals(referenceSet.size(), set.size());

                // Verify integrity after heavy collision stress
                for (String str : referenceSet) {
                    Assert.assertTrue(set.keyIndex(new Utf8String(str)) < 0);
                }
            }
        });
    }

    @Test
    public void testFuzzEmptyAndSingleChar() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);

            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(16, 0.7);
                set.setAllocator(allocator);
                set.of(0);

                Set<String> referenceSet = new HashSet<>();

                // Mix of empty, single char, and normal strings
                for (int i = 0; i < 100; i++) {
                    String str;
                    int choice = rnd.nextInt(3);
                    if (choice == 0) {
                        str = ""; // empty
                    } else if (choice == 1) {
                        str = String.valueOf((char) ('a' + rnd.nextInt(26))); // single char
                    } else {
                        str = generateRandomString(rnd, 2, 20); // normal
                    }

                    boolean added = set.add(new Utf8String(str));
                    boolean refAdded = referenceSet.add(str);
                    Assert.assertEquals(refAdded, added);
                }

                Assert.assertEquals(referenceSet.size(), set.size());

                for (String str : referenceSet) {
                    Assert.assertTrue(set.keyIndex(new Utf8String(str)) < 0);
                }
            }
        });
    }

    @Test
    public void testFuzzLargeDataArea() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);

            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(16, 0.7);
                set.setAllocator(allocator);
                set.of(0);

                Set<String> referenceSet = new HashSet<>();

                // Add strings that will require data area growth
                for (int i = 0; i < 100; i++) {
                    // Large strings to stress data area growth
                    String str = generateRandomString(rnd, 100, 500);
                    set.add(new Utf8String(str));
                    referenceSet.add(str);
                }

                Assert.assertEquals(referenceSet.size(), set.size());

                // Verify all large strings are correctly stored and retrievable
                for (String str : referenceSet) {
                    Assert.assertTrue(set.keyIndex(new Utf8String(str)) < 0);
                }
            }
        });
    }

    @Test
    public void testFuzzMerge() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);

            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet setA = new GroupByVarcharHashSet(16, 0.6);
                setA.setAllocator(allocator);
                setA.of(0);

                GroupByVarcharHashSet setB = new GroupByVarcharHashSet(16, 0.8);
                setB.setAllocator(allocator);
                setB.of(0);

                Set<String> refSetA = new HashSet<>();
                Set<String> refSetB = new HashSet<>();

                // Add to set A
                for (int i = 0; i < 500; i++) {
                    String str = generateRandomString(rnd, 5, 30);
                    if (setA.add(new Utf8String(str))) {
                        refSetA.add(str);
                    }
                }

                // Add to set B (some overlap expected)
                for (int i = 0; i < 500; i++) {
                    String str = generateRandomString(rnd, 5, 30);
                    if (setB.add(new Utf8String(str))) {
                        refSetB.add(str);
                    }
                }

                int sizeBeforeMerge = setA.size();
                Assert.assertEquals(refSetA.size(), sizeBeforeMerge);
                Assert.assertEquals(refSetB.size(), setB.size());

                // Merge B into A
                setA.merge(setB);

                // Calculate expected size
                Set<String> union = new HashSet<>(refSetA);
                union.addAll(refSetB);

                Assert.assertEquals(union.size(), setA.size());

                // Verify all strings from both sets are in merged set
                for (String str : union) {
                    Assert.assertTrue("Missing after merge: " + str,
                            setA.keyIndex(new Utf8String(str)) < 0);
                }
            }
        });
    }

    @Test
    public void testFuzzWithVariableLengths() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);

            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByVarcharHashSet set = new GroupByVarcharHashSet(8, 0.5);
                set.setAllocator(allocator);
                set.of(0);

                Set<String> referenceSet = new HashSet<>();

                // Add strings with varying lengths
                for (int len = 0; len <= 100; len += 10) {
                    for (int i = 0; i < 10; i++) {
                        String str = generateRandomString(rnd, len, len + 1);
                        set.add(new Utf8String(str));
                        referenceSet.add(str);
                    }
                }

                Assert.assertEquals(referenceSet.size(), set.size());

                // Verify all strings
                for (String str : referenceSet) {
                    Assert.assertTrue(set.keyIndex(new Utf8String(str)) < 0);
                }
            }
        });
    }

    private String generateRandomString(Rnd rnd, int minLen, int maxLen) {
        int len = minLen + rnd.nextInt(maxLen - minLen);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            // Mix of ASCII and some UTF-8
            int choice = rnd.nextInt(100);
            if (choice < 70) {
                // ASCII letters and numbers
                sb.append((char) ('a' + rnd.nextInt(26)));
            } else if (choice < 85) {
                // Numbers
                sb.append((char) ('0' + rnd.nextInt(10)));
            } else if (choice < 95) {
                // Special chars
                sb.append("_-+.,".charAt(rnd.nextInt(5)));
            } else {
                // Some UTF-8 chars
                sb.append("αβγδεζηθ".charAt(rnd.nextInt(8)));
            }
        }
        return sb.toString();
    }
}