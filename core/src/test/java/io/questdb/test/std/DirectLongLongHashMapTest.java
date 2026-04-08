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

package io.questdb.test.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.DirectLongLongHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class DirectLongLongHashMapTest {

    @Test
    public void testAll() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            // populate map
            try (DirectLongLongHashMap map = new DirectLongLongHashMap(4, 0.5, Long.MIN_VALUE, Long.MIN_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                map.put(Integer.MAX_VALUE, Long.MAX_VALUE);
                Assert.assertEquals(1, map.size());
                Assert.assertEquals(Long.MAX_VALUE, map.get(Integer.MAX_VALUE));
                map.clear();

                final int N = 1000;
                for (int i = 0; i < N; i++) {
                    int value = i + 1;
                    map.put(i, value);
                }
                Assert.assertEquals(N, map.size());

                rnd.reset();

                // assert that map contains the values we just added
                for (int i = 0; i < N; i++) {
                    Assert.assertFalse(map.excludes(i));
                    Assert.assertEquals(i + 1, map.get(i));
                }

                Rnd rnd2 = new Rnd();

                rnd.reset();

                rnd2.reset();
                rnd.reset();

                Rnd rnd3 = new Rnd();

                // assert that keys we didn't remove are still there and
                // keys we removed are not
                for (int i = 0; i < N; i++) {
                    int value = rnd.nextInt();
                    Assert.assertFalse(map.excludes(i));

                    long index = map.keyIndex(i);
                    Assert.assertEquals(i + 1, map.valueAt(index));

                    // update value
                    map.putAt(index, value, rnd3.nextInt());
                }

                // assert that update is visible correctly
                rnd3.reset();
                rnd2.reset();
                rnd.reset();

                // assert that keys we didn't remove are still there and
                // keys we removed are not
                for (int i = 0; i < N; i++) {
                    Assert.assertFalse(map.excludes(i));
                    Assert.assertEquals(rnd3.nextInt(), map.get(i));
                }

                map.restoreInitialCapacity();
                Assert.assertEquals(0, map.size());
                Assert.assertEquals(8, map.capacity());

                // test putIfAbsent
                for (int i = 0; i < N; i++) {
                    int value = i + 1;
                    Assert.assertTrue(map.putIfAbsent(i, value));
                    Assert.assertFalse(map.putIfAbsent(i, value));
                }
                Assert.assertEquals(N, map.size());

                // assert that map contains the values we just added
                for (int i = 0; i < N; i++) {
                    Assert.assertFalse(map.excludes(i));
                    Assert.assertEquals(i + 1, map.get(i));
                }
            }
        });
    }

    @Test
    public void testRemoveAt() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectLongLongHashMap map = new DirectLongLongHashMap(4, 0.5, -1, -1, MemoryTag.NATIVE_DEFAULT)) {
                final int N = 100;
                for (int i = 0; i < N; i++) {
                    map.put(i, i + 1);
                }
                Assert.assertEquals(N, map.size());

                // Remove every other entry.
                for (int i = 0; i < N; i += 2) {
                    long index = map.keyIndex(i);
                    Assert.assertTrue(index < 0);
                    map.removeAt(index);
                }
                Assert.assertEquals(N / 2, map.size());

                // Verify removed entries are gone and remaining entries are still there.
                for (int i = 0; i < N; i++) {
                    if (i % 2 == 0) {
                        Assert.assertTrue(map.excludes(i));
                        Assert.assertEquals(-1, map.get(i));
                    } else {
                        Assert.assertFalse(map.excludes(i));
                        Assert.assertEquals(i + 1, map.get(i));
                    }
                }

                // Remove all remaining entries.
                for (int i = 1; i < N; i += 2) {
                    long index = map.keyIndex(i);
                    Assert.assertTrue(index < 0);
                    map.removeAt(index);
                }
                Assert.assertEquals(0, map.size());

                // Verify all entries are gone.
                for (int i = 0; i < N; i++) {
                    Assert.assertTrue(map.excludes(i));
                }
            }
        });
    }

    @Test
    public void testCloseAfterRehashOom() throws Exception {
        assertMemoryLeak(() -> {
            // capacity = ceilPow2(4 / 0.5) = 8, free = 4
            try (DirectLongLongHashMap map = new DirectLongLongHashMap(4, 0.5, Long.MIN_VALUE, Long.MIN_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                // Insert 3 items, leaving free = 1.
                for (int i = 0; i < 3; i++) {
                    map.put(i, i + 1);
                }
                int capacityBeforeRehash = map.capacity();

                // Force OOM so the next malloc (triggered by rehash) fails.
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                try {
                    // 4th insert decrements free to 0 and triggers rehash.
                    map.put(3, 4);
                    Assert.fail("Expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                } finally {
                    Unsafe.setRssMemLimit(0);
                }

                // Map must remain in consistent state: capacity and ptr unchanged.
                Assert.assertEquals(capacityBeforeRehash, map.capacity());

                // The entry that triggered rehash was written before rehash was called.
                Assert.assertEquals(4, map.size());
                for (int i = 0; i < 4; i++) {
                    Assert.assertFalse(map.excludes(i));
                    Assert.assertEquals(i + 1, map.get(i));
                }

                // After lifting OOM, the next insert retries rehash successfully.
                map.put(4, 5);
                Assert.assertTrue(map.capacity() > capacityBeforeRehash);
                Assert.assertEquals(5, map.size());
                Assert.assertEquals(5, map.get(4));
            } finally {
                Unsafe.setRssMemLimit(0);
                // close() must not crash
            }
        });
    }

    @Test
    public void testCloseAfterRestoreInitialCapacityOom() throws Exception {
        assertMemoryLeak(() -> {
            DirectLongLongHashMap map = new DirectLongLongHashMap(4, 0.5, Long.MIN_VALUE, Long.MIN_VALUE, MemoryTag.NATIVE_DEFAULT);
            // Close the map: ptr = 0, capacity = 0.
            map.close();

            // Force OOM so restoreInitialCapacity's malloc fails.
            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
            try {
                map.restoreInitialCapacity();
                Assert.fail("Expected CairoException");
            } catch (CairoException e) {
                Assert.assertTrue(e.isOutOfMemory());
            } finally {
                Unsafe.setRssMemLimit(0);
            }

            // Map must remain in consistent closed state: capacity = 0 when ptr = 0.
            // Without the fix, capacity would be set to initialCapacity while ptr stayed 0,
            // causing a SIGSEGV when _close() iterates over capacity and dereferences ptr.
            Assert.assertEquals(0, map.capacity());

            // close() must not crash
            map.close();
        });
    }

}
