/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.join;

import io.questdb.griffin.engine.join.AsyncAsOfJoinKeyTable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * {@link AsyncAsOfJoinKeyTable} against a map: entries of the current epoch only, through the
 * array, its growth, the switch to hashing and the hash table's growth.
 */
public class AsyncAsOfJoinKeyTableTest extends AbstractCairoTest {

    @Test
    public void testDenseKeys() throws Exception {
        // the parallel way: span slots 0..n, all of them may take the array
        assertTable(5000, 5001, 20, 3000);
    }

    @Test
    public void testEnsureDense() throws Exception {
        // the span scan writes the array directly: what it writes, find() and entry() read back
        assertMemoryLeak(() -> {
            try (AsyncAsOfJoinKeyTable table = new AsyncAsOfJoinKeyTable()) {
                table.of(null, 3001);
                for (int epoch = 0; epoch < 3; epoch++) {
                    table.nextEpoch();
                    final long address = table.ensureDense(3001);
                    Assert.assertNotEquals(0, address);
                    Assert.assertTrue(table.getCapacity() >= 3001);
                    for (int key = epoch; key < 3001; key += 3) {
                        Unsafe.putLong(address + 24L * key, table.tagOf(key));
                        Unsafe.putLong(address + 24L * key + 8, key * 10L + epoch);
                    }
                    for (int key = 0; key < 3001; key++) {
                        final long e = table.find(key);
                        if (key % 3 == epoch) {
                            Assert.assertEquals(key * 10L + epoch, Unsafe.getLong(e + 8));
                        } else {
                            Assert.assertEquals(0, e);
                        }
                    }
                }
                // too many keys for the array
                Assert.assertEquals(0, table.ensureDense(1 << 20));
            }
        });
    }

    @Test
    public void testEpochWrap() throws Exception {
        assertMemoryLeak(() -> {
            try (AsyncAsOfJoinKeyTable table = new AsyncAsOfJoinKeyTable()) {
                table.of(null, 0);
                table.setEpoch(-2);
                table.nextEpoch();
                Unsafe.putLong(table.entry(7) + 8, 70);
                Assert.assertNotEquals(0, table.find(7));
                // -1 -> 0 wraps: every entry is untagged, epoch 0 is never used
                table.nextEpoch();
                Assert.assertEquals(0, table.find(7));
                table.nextEpoch();
                Assert.assertEquals(0, table.find(7));
                Unsafe.putLong(table.entry(7) + 8, 71);
                Assert.assertEquals(71, Unsafe.getLong(table.find(7) + 8));
            }
        });
    }

    @Test
    public void testHashedKeys() throws Exception {
        // keys past the array's limit
        assertTable(2_000_000, 0, 20, 5000);
    }

    @Test
    public void testSparseKeys() throws Exception {
        // the serial way: slave keys met; a few small ones take the array, then the large ones switch it to hashing
        assertTable(200_000, 0, 20, 300);
    }

    private static void assertTable(int keySpace, int denseKeyCount, int epochs, int opsPerEpoch) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (AsyncAsOfJoinKeyTable table = new AsyncAsOfJoinKeyTable()) {
                table.of(null, denseKeyCount);
                for (int e = 0; e < epochs; e++) {
                    table.nextEpoch();
                    final HashMap<Integer, long[]> model = new HashMap<>();
                    // early epochs: small keys only, so that the array is used first
                    final int space = e < epochs / 2 ? Math.min(keySpace, 500) : keySpace;
                    for (int i = 0; i < opsPerEpoch; i++) {
                        final int key = rnd.nextInt(space);
                        if (rnd.nextBoolean()) {
                            final long e0 = table.entry(key);
                            final long[] m = model.computeIfAbsent(key, k -> new long[]{-1, -1});
                            Assert.assertEquals("last row of " + key, m[0], Unsafe.getLong(e0 + 8));
                            Assert.assertEquals("walked-to row of " + key, m[1], Unsafe.getLong(e0 + 16));
                            m[0] = rnd.nextLong();
                            m[1] = rnd.nextLong();
                            Unsafe.putLong(e0 + 8, m[0]);
                            Unsafe.putLong(e0 + 16, m[1]);
                        } else {
                            final long e0 = table.find(key);
                            final long[] m = model.get(key);
                            if (m == null) {
                                Assert.assertEquals("absent key " + key, 0, e0);
                            } else {
                                Assert.assertNotEquals("present key " + key, 0, e0);
                                Assert.assertEquals(m[0], Unsafe.getLong(e0 + 8));
                                Assert.assertEquals(m[1], Unsafe.getLong(e0 + 16));
                            }
                        }
                    }
                    // every key of the epoch, and none of an earlier one
                    for (int key = 0; key < Math.min(space, 20_000); key++) {
                        final long[] m = model.get(key);
                        final long e0 = table.find(key);
                        if (m == null) {
                            Assert.assertEquals("epoch " + e + " key " + key, 0, e0);
                        } else {
                            Assert.assertEquals("epoch " + e + " key " + key, m[0], Unsafe.getLong(e0 + 8));
                        }
                    }
                    for (Integer key : model.keySet()) {
                        Assert.assertEquals(model.get(key)[1], Unsafe.getLong(table.find(key) + 16));
                    }
                }
                if (keySpace > 70_000) {
                    Assert.assertFalse("large keys must switch the table to hashing", table.isDirect());
                    Assert.assertTrue("the hash table holds one epoch's keys: " + table.getCapacity(), table.getCapacity() <= 4 * opsPerEpoch + 1024);
                } else if (denseKeyCount > 0) {
                    Assert.assertTrue(table.isDirect());
                }
            }
        });
    }
}
