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
import io.questdb.griffin.engine.groupby.GroupByCharSequenceLongHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class GroupByCharSequenceLongHashMapFuzzTest extends AbstractCairoTest {

    @Test
    public void testFuzzWithLongNullAsNoKeyValue() throws Exception {
        testFuzz(Numbers.LONG_NULL, 0);
    }

    @Test
    public void testFuzzWithZeroAsNoKeyValue() throws Exception {
        testFuzz(0, -1);
    }

    @Test
    public void testIncOperations() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByCharSequenceLongHashMap map = new GroupByCharSequenceLongHashMap(16, 0.7, -1, 0);
                map.setAllocator(allocator);
                map.of(0);

                final int N = 1000;
                final Rnd rnd = TestUtils.generateRandom(LOG);

                Map<String, Long> referenceMap = new java.util.HashMap<>();

                for (int i = 0; i < N; i++) {
                    String key = "fuzz_" + rnd.nextPositiveInt();
                    long delta = rnd.nextPositiveLong() % 100 + 1;

                    map.inc(key, delta);
                    referenceMap.merge(key, delta, Long::sum);
                }

                Assert.assertEquals(referenceMap.size(), map.size());

                for (Map.Entry<String, Long> entry : referenceMap.entrySet()) {
                    Assert.assertEquals((long) entry.getValue(), map.get(entry.getKey()));
                }
            }
        });
    }

    @Test
    public void testMerge() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByCharSequenceLongHashMap mapA = new GroupByCharSequenceLongHashMap(16, 0.5, -1, 0);
                mapA.setAllocator(allocator);
                mapA.of(0);
                GroupByCharSequenceLongHashMap mapB = new GroupByCharSequenceLongHashMap(16, 0.9, -1, 0);
                mapB.setAllocator(allocator);
                mapB.of(0);

                final int N = 1000;

                for (int i = 0; i < N; i++) {
                    String key = "key_" + i;
                    mapA.put(key, i * 2);
                }
                Assert.assertEquals(N, mapA.size());
                Assert.assertTrue(mapA.capacity() >= N);

                for (int i = N; i < 2 * N; i++) {
                    String key = "key_" + i;
                    mapB.put(key, i * 3);
                }
                Assert.assertEquals(N, mapB.size());
                Assert.assertTrue(mapB.capacity() >= N);

                mapA.mergeAdd(mapB);
                Assert.assertEquals(2 * N, mapA.size());
                for (int i = 0; i < N; i++) {
                    String key = "key_" + i;
                    Assert.assertEquals(i * 2, mapA.get(key));
                }
                for (int i = N; i < 2 * N; i++) {
                    String key = "key_" + i;
                    Assert.assertEquals(i * 3, mapA.get(key));
                }
            }
        });
    }

    @Test
    public void testMergeAddWithOverlappingKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByCharSequenceLongHashMap mapA = new GroupByCharSequenceLongHashMap(16, 0.5, -1, 0);
                mapA.setAllocator(allocator);
                mapA.of(0);
                GroupByCharSequenceLongHashMap mapB = new GroupByCharSequenceLongHashMap(16, 0.9, -1, 0);
                mapB.setAllocator(allocator);
                mapB.of(0);

                final int N = 500;

                for (int i = 0; i < N; i++) {
                    String key = "overlap_" + i;
                    mapA.put(key, i * 2);
                }

                for (int i = N / 2; i < N + N / 2; i++) {
                    String key = "overlap_" + i;
                    mapB.put(key, i * 3);
                }

                mapA.mergeAdd(mapB);

                for (int i = 0; i < N / 2; i++) {
                    String key = "overlap_" + i;
                    Assert.assertEquals(i * 2, mapA.get(key));
                }
                for (int i = N / 2; i < N; i++) {
                    String key = "overlap_" + i;
                    Assert.assertEquals(i * 2 + i * 3, mapA.get(key));
                }
                for (int i = N; i < N + N / 2; i++) {
                    String key = "overlap_" + i;
                    Assert.assertEquals(i * 3, mapA.get(key));
                }
            }
        });
    }

    @Test
    public void testWithSpecialCharacters() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByCharSequenceLongHashMap map = new GroupByCharSequenceLongHashMap(16, 0.7, -1, 0);
                map.setAllocator(allocator);
                map.of(0);

                String[] specialKeys = {
                        "key_with_emoji_🚀",
                        "key_with_unicode_ñáéíóú",
                        "key_with_chinese_中文",
                        "key_with_spaces  ",
                        "key\nwith\nnewlines",
                        "key\twith\ttabs",
                        "",  // empty string
                        "key_with_special_!@#$%^&*()"
                };

                for (int i = 0; i < specialKeys.length; i++) {
                    map.put(specialKeys[i], i * 10);
                }

                Assert.assertEquals(specialKeys.length, map.size());

                for (int i = 0; i < specialKeys.length; i++) {
                    Assert.assertEquals(i * 10, map.get(specialKeys[i]));
                }
            }
        });
    }

    @Test
    public void testWithStringSink() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByCharSequenceLongHashMap map = new GroupByCharSequenceLongHashMap(16, 0.7, -1, 0);
                map.setAllocator(allocator);
                map.of(0);

                final int N = 100;
                final StringSink sink = new StringSink();

                for (int i = 0; i < N; i++) {
                    sink.clear();
                    sink.put("generated_key_").put(i).put("_suffix");
                    map.put(sink, i * 5);
                }

                Assert.assertEquals(N, map.size());

                for (int i = 0; i < N; i++) {
                    sink.clear();
                    sink.put("generated_key_").put(i).put("_suffix");
                    Assert.assertEquals(i * 5, map.get(sink));
                }
            }
        });
    }

    private void testFuzz(long noKeyValue, long noEntryValue) throws Exception {
        assertMemoryLeak(() -> {
            final int N = 1000;
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final long seed0 = rnd.getSeed0();
            final long seed1 = rnd.getSeed1();
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByCharSequenceLongHashMap map = new GroupByCharSequenceLongHashMap(16, 0.7, noKeyValue, noEntryValue);
                map.setAllocator(allocator);
                map.of(0);

                Map<String, Long> referenceMap = new java.util.HashMap<>();
                for (int i = 0; i < N; i++) {
                    String key = "random_" + rnd.nextPositiveInt() + "_" + rnd.nextPositiveLong();
                    long value = rnd.nextPositiveLong() + 1;
                    map.put(key, value);
                    referenceMap.put(key, value);
                }

                Assert.assertEquals(referenceMap.size(), map.size());
                Assert.assertTrue(map.capacity() >= referenceMap.size());

                rnd.reset(seed0, seed1);

                for (int i = 0; i < N; i++) {
                    String key = "random_" + rnd.nextPositiveInt() + "_" + rnd.nextPositiveLong();
                    rnd.nextPositiveLong();
                    Assert.assertEquals((long) referenceMap.get(key), map.get(key));
                }

                map.of(0);
                rnd.reset(seed0, seed1);

                referenceMap.clear();
                for (int i = 0; i < N; i++) {
                    String key = "random_" + rnd.nextPositiveInt() + "_" + rnd.nextPositiveLong();
                    long value = rnd.nextPositiveLong() + 1;
                    map.put(key, value);
                    referenceMap.put(key, value);
                }

                Assert.assertEquals(referenceMap.size(), map.size());
                for (Map.Entry<String, Long> entry : referenceMap.entrySet()) {
                    Assert.assertEquals((long) entry.getValue(), map.get(entry.getKey()));
                }
            }
        });
    }
}
