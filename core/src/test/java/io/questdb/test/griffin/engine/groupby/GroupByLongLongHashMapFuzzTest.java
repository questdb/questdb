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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class GroupByLongLongHashMapFuzzTest extends AbstractCairoTest {

    @Test
    public void testFuzzWithLongNullAsNoKeyValue() throws Exception {
        testFuzz(Numbers.LONG_NULL, 0);
    }

    @Test
    public void testFuzzWithZeroAsNoKeyValue() throws Exception {
        testFuzz(0, -1);
    }

    @Test
    public void testMerge() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongLongHashMap mapA = new GroupByLongLongHashMap(16, 0.5, -1, 0);
                mapA.setAllocator(allocator);
                mapA.of(0);
                GroupByLongLongHashMap mapB = new GroupByLongLongHashMap(16, 0.9, -1, 0);
                mapB.setAllocator(allocator);
                mapB.of(0);

                final int N = 1000;

                for (int i = 0; i < N; i++) {
                    mapA.put(i, i * 2);
                }
                Assert.assertEquals(N, mapA.size());
                Assert.assertTrue(mapA.capacity() >= N);

                for (int i = N; i < 2 * N; i++) {
                    mapB.put(i, i * 3);
                }
                Assert.assertEquals(N, mapB.size());
                Assert.assertTrue(mapB.capacity() >= N);

                mapA.mergeAdd(mapB);
                Assert.assertEquals(2 * N, mapA.size());
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i * 2, mapA.get(i));
                }
                for (int i = N; i < 2 * N; i++) {
                    Assert.assertEquals(i * 3, mapA.get(i));
                }
            }
        });
    }

    @Test
    public void testMergeAddWithOverlappingKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongLongHashMap mapA = new GroupByLongLongHashMap(16, 0.5, -1, 0);
                mapA.setAllocator(allocator);
                mapA.of(0);
                GroupByLongLongHashMap mapB = new GroupByLongLongHashMap(16, 0.9, -1, 0);
                mapB.setAllocator(allocator);
                mapB.of(0);

                final int N = 500;

                for (int i = 0; i < N; i++) {
                    mapA.put(i, i * 2);
                }

                for (int i = N / 2; i < N + N / 2; i++) {
                    mapB.put(i, i * 3);
                }

                mapA.mergeAdd(mapB);

                for (int i = 0; i < N / 2; i++) {
                    Assert.assertEquals(i * 2, mapA.get(i));
                }
                for (int i = N / 2; i < N; i++) {
                    Assert.assertEquals(i * 2 + i * 3, mapA.get(i));
                }
                for (int i = N; i < N + N / 2; i++) {
                    Assert.assertEquals(i * 3, mapA.get(i));
                }
            }
        });
    }

    @Test
    public void testIncOperations() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, -1, 0);
                map.setAllocator(allocator);
                map.of(0);

                final int N = 1000;
                final Rnd rnd = TestUtils.generateRandom(LOG);

                Map<Long, Long> referenceMap = new java.util.HashMap<>();

                for (int i = 0; i < N; i++) {
                    long key = rnd.nextPositiveLong() + 1;
                    long delta = rnd.nextPositiveLong() % 100 + 1;

                    map.inc(key, delta);
                    referenceMap.merge(key, delta, Long::sum);
                }

                Assert.assertEquals(referenceMap.size(), map.size());

                for (Map.Entry<Long, Long> entry : referenceMap.entrySet()) {
                    Assert.assertEquals((long) entry.getValue(), map.get(entry.getKey()));
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
                GroupByLongLongHashMap map = new GroupByLongLongHashMap(16, 0.7, noKeyValue, noEntryValue);
                map.setAllocator(allocator);
                map.of(0);

                Map<Long, Long> referenceMap = new java.util.HashMap<>();
                for (int i = 0; i < N; i++) {
                    long key = rnd.nextPositiveLong() + 1;
                    long value = rnd.nextPositiveLong() + 1;
                    map.put(key, value);
                    referenceMap.put(key, value);
                }

                Assert.assertEquals(referenceMap.size(), map.size());
                Assert.assertTrue(map.capacity() >= referenceMap.size());

                rnd.reset(seed0, seed1);

                for (int i = 0; i < N; i++) {
                    long key = rnd.nextPositiveLong() + 1;
                    rnd.nextPositiveLong();
                    Assert.assertEquals((long) referenceMap.get(key), map.get(key));
                }

                map.of(0);
                rnd.reset(seed0, seed1);

                referenceMap.clear();
                for (int i = 0; i < N; i++) {
                    long key = rnd.nextPositiveLong() + 1;
                    long value = rnd.nextPositiveLong() + 1;
                    map.put(key, value);
                    referenceMap.put(key, value);
                }

                Assert.assertEquals(referenceMap.size(), map.size());
                for (Map.Entry<Long, Long> entry : referenceMap.entrySet()) {
                    Assert.assertEquals((long) entry.getValue(), map.get(entry.getKey()));
                }
            }
        });
    }
}