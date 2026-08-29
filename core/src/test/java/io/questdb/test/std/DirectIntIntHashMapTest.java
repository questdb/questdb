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
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class DirectIntIntHashMapTest {

    @Test
    public void testAll() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            // populate map
            try (DirectIntIntHashMap map = new DirectIntIntHashMap(4, 0.5, Integer.MIN_VALUE, Integer.MIN_VALUE, MemoryTag.NATIVE_DEFAULT)) {
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
            }
        });
    }

    @Test
    public void testCloseAfterRehashOom() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectIntIntHashMap map = new DirectIntIntHashMap(4, 0.5, Integer.MIN_VALUE, Integer.MIN_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                // Insert 3 items, leaving free = 1.
                for (int i = 0; i < 3; i++) {
                    map.put(i, i + 1);
                }
                int capacityBeforeRehash = map.capacity();

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
            }
        });
    }

    @Test
    public void testCloseAfterRestoreInitialCapacityOom() throws Exception {
        assertMemoryLeak(() -> {
            DirectIntIntHashMap map = new DirectIntIntHashMap(4, 0.5, Integer.MIN_VALUE, Integer.MIN_VALUE, MemoryTag.NATIVE_DEFAULT);
            map.close();

            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
            try {
                map.restoreInitialCapacity();
                Assert.fail("Expected CairoException");
            } catch (CairoException e) {
                Assert.assertTrue(e.isOutOfMemory());
            } finally {
                Unsafe.setRssMemLimit(0);
            }

            Assert.assertEquals(0, map.capacity());
            map.close();
        });
    }

    @Test
    public void testLazyOpenReopenAndMemoryTracker() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    TestMemoryTracker firstTracker = new TestMemoryTracker();
                    TestMemoryTracker secondTracker = new TestMemoryTracker()
            ) {
                final DirectIntIntHashMap map = new DirectIntIntHashMap(
                        4,
                        0.5,
                        Integer.MIN_VALUE,
                        Integer.MIN_VALUE,
                        MemoryTag.NATIVE_DEFAULT,
                        false
                );
                try {
                    Assert.assertFalse(map.isOpen());
                    Assert.assertEquals(8, map.capacity());
                    Assert.assertEquals(0, map.size());
                    Assert.assertEquals(0, firstTracker.getUsed());

                    map.setMemoryTracker(firstTracker);
                    map.reopen();

                    Assert.assertTrue(map.isOpen());
                    Assert.assertEquals(8, map.capacity());
                    Assert.assertEquals(64, firstTracker.getUsed());

                    for (int i = 0; i < 4; i++) {
                        map.put(i, i + 1);
                    }
                    Assert.assertEquals(16, map.capacity());
                    Assert.assertEquals(128, firstTracker.getUsed());

                    // Reopening a live map is a no-op: it neither clears the values nor charges
                    // the tracker for a second directory.
                    map.reopen();
                    Assert.assertEquals(4, map.size());
                    Assert.assertEquals(3, map.get(2));
                    Assert.assertEquals(128, firstTracker.getUsed());

                    map.close();
                    Assert.assertFalse(map.isOpen());
                    Assert.assertEquals(0, map.capacity());
                    Assert.assertEquals(0, map.size());
                    Assert.assertEquals(0, firstTracker.getUsed());

                    // Exercise the standalone Reopenable contract, which NativeKeyMap deliberately
                    // does not reach after close, and prove the new tracker owns the allocation.
                    map.setMemoryTracker(secondTracker);
                    map.reopen();
                    Assert.assertTrue(map.isOpen());
                    Assert.assertEquals(8, map.capacity());
                    Assert.assertEquals(0, map.size());
                    Assert.assertTrue(map.excludes(2));
                    Assert.assertEquals(0, firstTracker.getUsed());
                    Assert.assertEquals(64, secondTracker.getUsed());

                    map.put(42, 84);
                    Assert.assertEquals(84, map.get(42));
                } finally {
                    map.close();
                    map.setMemoryTracker(null);
                }
                Assert.assertEquals(0, firstTracker.getUsed());
                Assert.assertEquals(0, secondTracker.getUsed());
            }
        });
    }

    private static final class TestMemoryTracker extends MemoryTracker {
        private long nativeAddress;

        private TestMemoryTracker() {
            nativeAddress = Unsafe.malloc(Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
            Vect.memset(nativeAddress, Unsafe.MEMORY_TRACKER_BLOCK_SIZE, 0);
        }

        @Override
        public void close() {
            if (nativeAddress != 0) {
                freeNativeAllocators();
                nativeAddress = Unsafe.free(
                        nativeAddress,
                        Unsafe.MEMORY_TRACKER_BLOCK_SIZE,
                        MemoryTag.NATIVE_MEMORY_TRACKER
                );
            }
        }

        @Override
        public long getLimit() {
            return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
        }

        @Override
        public long getQueryId() {
            return 1;
        }

        @Override
        public long getUsed() {
            return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_USED_OFFSET);
        }

        @Override
        public MemoryTrackerWorkload getWorkload() {
            return MemoryTrackerWorkload.QUERY;
        }

        @Override
        public long nativeAddress() {
            return nativeAddress;
        }
    }
}
