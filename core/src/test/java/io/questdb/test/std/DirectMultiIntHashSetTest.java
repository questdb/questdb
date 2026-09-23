/*+*****************************************************************************
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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.DirectMultiIntHashSet;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class DirectMultiIntHashSetTest {
    @Test
    public void testTupleIdentityAndGrowth() throws Exception {
        assertMemoryLeak(() -> {
            for (int width : new int[]{3, 4, 8}) {
                try (DirectMultiIntHashSet set = new DirectMultiIntHashSet(width, 4, 0.6, 32)) {
                    Rnd rnd = new Rnd();
                    int[] key = new int[width];
                    Set<String> expected = new HashSet<>();
                    set.reopen();
                    for (int i = 0; i < 20_000; i++) {
                        int value = rnd.nextPositiveInt() % 1000;
                        for (int j = 0; j < width; j++) {
                            key[j] = value % (11 + j) == 0 ? Integer.MIN_VALUE : (value + j) % (17 + j);
                        }
                        writeKey(set, key);
                        Assert.assertEquals(expected.add(Arrays.toString(key)), set.add());
                    }
                    Assert.assertEquals(expected.size(), set.size());
                    set.clear();
                    Arrays.fill(key, 0);
                    writeKey(set, key);
                    Assert.assertTrue(set.add());
                    Assert.assertFalse(set.add());
                    Arrays.fill(key, Integer.MIN_VALUE);
                    writeKey(set, key);
                    Assert.assertTrue(set.add());
                    Assert.assertFalse(set.add());
                    Assert.assertEquals(2, set.size());
                }
            }
        });
    }

    @Test
    public void testFailedGrowthPreservesKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectMultiIntHashSet set = new DirectMultiIntHashSet(3, 4, 0.5, 32)) {
                set.reopen();
                for (int i = 0; i < 8; i++) {
                    writeKey(set, i, 0, Integer.MIN_VALUE);
                    Assert.assertTrue(set.add());
                }
                writeKey(set, 8, 0, Integer.MIN_VALUE);
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                try {
                    set.add();
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
                Assert.assertEquals(8, set.size());
                for (int i = 0; i < 8; i++) {
                    writeKey(set, i, 0, Integer.MIN_VALUE);
                    Assert.assertFalse(set.add());
                }
                writeKey(set, 8, 0, Integer.MIN_VALUE);
                Assert.assertTrue(set.add());
                Assert.assertEquals(9, set.size());
            }
        });
    }

    @Test
    public void testLazyAllocationAndTrackerAcrossReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (TestMemoryTracker tracker = new TestMemoryTracker();
                 DirectMultiIntHashSet set = new DirectMultiIntHashSet(3, 4, 0.5, 32)) {
                Assert.assertEquals(0, set.getKeyAddress());
                for (int execution = 0; execution < 3; execution++) {
                    set.setMemoryTracker(tracker);
                    set.reopen();
                    Assert.assertEquals(16, set.capacity());
                    for (int i = 0; i < 1000; i++) {
                        writeKey(set, i, i + 1, i + 2);
                        Assert.assertTrue(set.add());
                    }
                    Assert.assertEquals(12L * (set.capacity() + 1), tracker.getUsed());
                    set.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testResizeLimit() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectMultiIntHashSet set = new DirectMultiIntHashSet(3, 4, 0.5, 0)) {
                set.reopen();
                for (int i = 0; i < 8; i++) {
                    writeKey(set, i, 0, 0);
                    Assert.assertTrue(set.add());
                }
                writeKey(set, 8, 0, 0);
                Assert.assertThrows(LimitOverflowException.class, set::add);
                Assert.assertEquals(8, set.size());
            }
        });
    }

    private static void writeKey(DirectMultiIntHashSet set, int... key) {
        for (int i = 0; i < key.length; i++) {
            Unsafe.putInt(set.getKeyAddress() + (long) i * Integer.BYTES, key[i]);
        }
    }

    private static class TestMemoryTracker extends MemoryTracker {
        @Override
        public void close() {
            if (nativeAddress() != 0) {
                destroyNativeBlock();
            }
        }

        @Override
        public long getQueryId() {
            return 1;
        }

        @Override
        public MemoryTrackerWorkload getWorkload() {
            return MemoryTrackerWorkload.QUERY;
        }
    }
}
