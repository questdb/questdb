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

import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorArena;
import io.questdb.griffin.engine.groupby.GroupByIntHashSet;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class GroupByIntHashSetFuzzTest extends AbstractCairoTest {

    @Test
    public void testFuzzWithIntNullAsNoKeyValue() throws Exception {
        testFuzz(Numbers.INT_NULL);
    }

    @Test
    public void testFuzzWithZeroAsNoKeyValue() throws Exception {
        testFuzz(0);
    }

    @Test
    public void testMerge() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocatorArena(64, Numbers.SIZE_1GB)) {
                GroupByIntHashSet setA = new GroupByIntHashSet(16, 0.5, -1);
                setA.setAllocator(allocator);
                setA.of(0);
                GroupByIntHashSet setB = new GroupByIntHashSet(16, 0.9, -1);
                setB.setAllocator(allocator);
                setB.of(0);

                final int N = 1000;

                for (int i = 0; i < N; i++) {
                    setA.add(i);
                }
                Assert.assertEquals(N, setA.size());
                Assert.assertTrue(setA.capacity() >= N);

                for (int i = N; i < 2 * N; i++) {
                    setB.add(i);
                }
                Assert.assertEquals(N, setB.size());
                Assert.assertTrue(setB.capacity() >= N);

                setA.merge(setB);
                Assert.assertEquals(2 * N, setA.size());
                for (int i = 0; i < 2 * N; i++) {
                    Assert.assertTrue(setA.keyIndex(i) < 0);
                }
            }
        });
    }

    private void testFuzz(int noKeyValue) throws Exception {
        assertMemoryLeak(() -> {
            final int N = 1000;
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final long seed0 = rnd.getSeed0();
            final long seed1 = rnd.getSeed1();
            try (GroupByAllocator allocator = new GroupByAllocatorArena(64, Numbers.SIZE_1GB)) {
                GroupByIntHashSet set = new GroupByIntHashSet(16, 0.7, noKeyValue);
                set.setAllocator(allocator);
                set.of(0);

                Set<Integer> referenceSet = new java.util.HashSet<>();
                for (int i = 0; i < N; i++) {
                    int val = rnd.nextPositiveInt();
                    set.add(val);
                    referenceSet.add(val);
                }

                Assert.assertEquals(referenceSet.size(), set.size());
                Assert.assertTrue(set.capacity() >= referenceSet.size());

                rnd.reset(seed0, seed1);

                for (int i = 0; i < N; i++) {
                    Assert.assertTrue(set.keyIndex(rnd.nextPositiveInt()) < 0);
                }

                set.of(0);
                rnd.reset(seed0, seed1);

                referenceSet.clear();
                for (int i = 0; i < N; i++) {
                    int val = rnd.nextPositiveInt();
                    long index = set.keyIndex(val);
                    Assert.assertTrue(index >= 0 || referenceSet.contains(val));
                    set.addAt(index, val);
                    referenceSet.add(val);
                }
            }
        });
    }
}
