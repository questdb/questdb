/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByIntHashSet;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GroupByIntHashSetTest extends AbstractCairoTest {

    @Test
    public void testMerge() throws Exception {
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        TestUtils.assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocator(config)) {
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

    @Test
    public void testSmokeWithIntNullAsNoKeyValue() throws Exception {
        testSmoke(Numbers.INT_NaN);
    }

    @Test
    public void testSmokeWithZeroAsNoKeyValue() throws Exception {
        testSmoke(0);
    }

    private void testSmoke(int noKeyValue) throws Exception {
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                GroupByIntHashSet set = new GroupByIntHashSet(16, 0.7, noKeyValue);
                set.setAllocator(allocator);
                set.of(0);

                final int N = 1000;

                for (int i = 0; i < N; i++) {
                    set.add(rnd.nextPositiveInt());
                }

                Assert.assertEquals(N, set.size());
                Assert.assertTrue(set.capacity() >= N);

                rnd.reset();

                for (int i = 0; i < N; i++) {
                    Assert.assertTrue(set.keyIndex(rnd.nextPositiveInt()) < 0);
                }

                set.of(0);
                rnd.reset();

                for (int i = 0; i < N; i++) {
                    int val = rnd.nextPositiveInt();
                    int index = set.keyIndex(val);
                    Assert.assertTrue(index >= 0);
                    set.addAt(index, val);
                }
            }
        });
    }
}
